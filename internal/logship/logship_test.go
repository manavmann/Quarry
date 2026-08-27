package logship

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recorder is a scripted Sink: it records every call and answers from a
// queue of errors (nil once the queue is empty).
type recorder struct {
	mu    sync.Mutex
	calls [][]Chunk
	errs  []error
}

func (r *recorder) sink(ctx context.Context, chunks []Chunk) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Copy: the shipper keeps the batch until acked and may resend it.
	cp := make([]Chunk, len(chunks))
	for i, c := range chunks {
		cp[i] = Chunk{Seq: c.Seq, Data: append([]byte(nil), c.Data...)}
	}
	r.calls = append(r.calls, cp)
	if len(r.errs) > 0 {
		err := r.errs[0]
		r.errs = r.errs[1:]
		return err
	}
	return nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

// stream concatenates every delivered chunk in order and checks seqs are
// 1-based and gapless (duplicates from retries are allowed and skipped).
func (r *recorder) stream(t *testing.T) []byte {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []byte
	var next int64 = 1
	for _, call := range r.calls {
		for _, c := range call {
			if c.Seq < next {
				continue // resent
			}
			if c.Seq != next {
				t.Fatalf("seq gap: got %d, want %d", c.Seq, next)
			}
			out = append(out, c.Data...)
			next++
		}
	}
	return out
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !cond() {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", what)
		case <-tick.C:
		}
	}
}

// slow is a Config whose ticker never fires during a test, so only size
// thresholds and Close cause flushes.
var slow = Config{FlushInterval: time.Hour, FlushBytes: 64, Backoff: time.Millisecond}

func TestFlushOnCloseDeliversEverything(t *testing.T) {
	rec := &recorder{}
	s := New(slow, rec.sink)
	if _, err := s.Write([]byte("hello ")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("world\n")); err != nil {
		t.Fatal(err)
	}
	if rec.count() != 0 {
		t.Fatalf("flushed before threshold or close: %d calls", rec.count())
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := rec.stream(t); string(got) != "hello world\n" {
		t.Fatalf("delivered %q", got)
	}
	if rec.count() != 1 || rec.calls[0][0].Seq != 1 {
		t.Fatalf("calls = %+v", rec.calls)
	}
	// Close is idempotent and post-Close writes are dropped, not errors.
	if _, err := s.Write([]byte("late")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(context.Background()); err != nil || rec.count() != 1 {
		t.Fatalf("second Close: %v, calls %d", err, rec.count())
	}
}

func TestSizeThresholdFlushesWithoutWaiting(t *testing.T) {
	rec := &recorder{}
	s := New(slow, rec.sink)
	data := bytes.Repeat([]byte("x"), 64*3+10) // three full chunks and a tail
	if _, err := s.Write(data); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(rec.stream(t)) >= 64*3 }, "three chunks to ship")
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := rec.stream(t)
	if !bytes.Equal(got, data) {
		t.Fatalf("delivered %d bytes, want %d", len(got), len(data))
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, call := range rec.calls {
		for _, c := range call {
			if len(c.Data) > 64 {
				t.Errorf("chunk %d is %d bytes, over FlushBytes", c.Seq, len(c.Data))
			}
		}
	}
}

func TestIntervalFlushes(t *testing.T) {
	rec := &recorder{}
	s := New(Config{FlushInterval: 2 * time.Millisecond, FlushBytes: 1 << 20}, rec.sink)
	defer s.Close(context.Background())
	s.Write([]byte("tick"))
	waitFor(t, func() bool { return rec.count() >= 1 }, "interval flush")
	if got := rec.stream(t); string(got) != "tick" {
		t.Fatalf("delivered %q", got)
	}
}

func TestRetryResendsIdenticalBatch(t *testing.T) {
	rec := &recorder{errs: []error{errors.New("connection refused"), errors.New("502")}}
	s := New(slow, rec.sink)
	s.Write(bytes.Repeat([]byte("a"), 64))
	s.Write(bytes.Repeat([]byte("b"), 64))
	waitFor(t, func() bool { return rec.count() >= 3 }, "two failures then a success")
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	first, second, third := rec.calls[0], rec.calls[1], rec.calls[2]
	rec.mu.Unlock()
	for i, call := range [][]Chunk{second, third} {
		if len(call) != len(first) {
			t.Fatalf("retry %d has %d chunks, first had %d", i+1, len(call), len(first))
		}
		for j := range call {
			if call[j].Seq != first[j].Seq || !bytes.Equal(call[j].Data, first[j].Data) {
				t.Fatalf("retry %d chunk %d differs: seq %d vs %d", i+1, j, call[j].Seq, first[j].Seq)
			}
		}
	}
	if got := rec.stream(t); len(got) != 128 || got[0] != 'a' || got[127] != 'b' {
		t.Fatalf("stream = %d bytes", len(got))
	}
}

func TestStaleDropsAndSignalsOnce(t *testing.T) {
	var stale atomic.Int32
	rec := &recorder{errs: []error{ErrStale}}
	cfg := slow
	cfg.OnStale = func() { stale.Add(1) }
	s := New(cfg, rec.sink)
	s.Write(bytes.Repeat([]byte("a"), 64))
	waitFor(t, func() bool { return stale.Load() == 1 }, "OnStale")
	// Nothing more is sent, whatever is written afterwards.
	s.Write(bytes.Repeat([]byte("b"), 64))
	if err := s.Close(context.Background()); !errors.Is(err, ErrStale) {
		t.Fatalf("Close = %v, want ErrStale", err)
	}
	if rec.count() != 1 || stale.Load() != 1 {
		t.Fatalf("calls %d, OnStale %d", rec.count(), stale.Load())
	}
}

func TestRejectedBatchIsDroppedAndReported(t *testing.T) {
	rec := &recorder{errs: []error{errors.New("400: " + ErrRejected.Error())}}
	rec.errs[0] = errors.Join(ErrRejected, rec.errs[0])
	s := New(slow, rec.sink)
	s.Write([]byte("bad"))
	err := s.Close(context.Background())
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("Close = %v, want ErrRejected", err)
	}
	if rec.count() != 1 {
		t.Fatalf("calls = %d, want 1 (no retry of a rejected batch)", rec.count())
	}
}

func TestCloseContextBoundsFinalFlush(t *testing.T) {
	// The server is down for good: Close must give up when its context
	// ends rather than block, and say what was lost.
	rec := &recorder{}
	rec.errs = make([]error, 100)
	for i := range rec.errs {
		rec.errs[i] = errors.New("down")
	}
	s := New(slow, rec.sink)
	s.Write([]byte("unsent"))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := s.Close(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "6 bytes unsent") {
		t.Fatalf("Close = %v", err)
	}
}

func TestClosedContextDiscards(t *testing.T) {
	rec := &recorder{}
	s := New(slow, rec.sink)
	s.Write([]byte("discard me"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Close(ctx); err == nil {
		t.Fatal("Close with a done ctx should report the unsent bytes")
	}
	if rec.count() != 0 {
		t.Fatalf("sent %d batches under a done ctx", rec.count())
	}
}

func TestBufferCapDropsAndMarks(t *testing.T) {
	// Sink blocks until released so the buffer fills; MaxBuffered is tiny.
	release := make(chan struct{})
	rec := &recorder{}
	var blocked atomic.Bool
	sink := func(ctx context.Context, chunks []Chunk) error {
		if blocked.CompareAndSwap(false, true) {
			<-release
		}
		return rec.sink(ctx, chunks)
	}
	cfg := Config{FlushInterval: time.Hour, FlushBytes: 8, MaxBuffered: 16, Backoff: time.Millisecond}
	s := New(cfg, sink)
	s.Write([]byte("12345678")) // sealed → flush starts and blocks in the sink
	waitFor(t, func() bool { return blocked.Load() }, "sink to block")
	s.Write([]byte("abcdefgh")) // fills the buffer (16 buffered)
	s.Write([]byte("DROPPED"))  // no room
	close(release)
	waitFor(t, func() bool { return rec.count() >= 1 }, "first batch")
	// Room is back: the next write is preceded by a marker.
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.buffered < cfg.MaxBuffered
	}, "buffer to drain")
	s.Write([]byte("Z"))
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := string(rec.stream(t))
	if !strings.HasPrefix(got, "12345678abcdefgh[quarry] log shipper dropped 7 bytes\n") || !strings.HasSuffix(got, "Z") {
		t.Fatalf("stream = %q", got)
	}
}
