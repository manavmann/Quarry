// Package logship is the runner-side log shipper: an io.Writer that the
// executor streams a job's output into, and that ships it to the control
// plane in batches — every FlushInterval, or as soon as FlushBytes are
// buffered. Each chunk gets its seq exactly once, when it is sealed, so a
// retried batch carries the very same (seq, data) pairs and the server's
// INSERT OR IGNORE makes redelivery harmless. Close flushes everything
// still buffered before returning, which is what lets the agent guarantee
// that complete never overtakes the last chunk.
//
// Goroutines: New starts one flush loop, owned by the Shipper and ended
// by Close; Close returns only after it has exited.
package logship

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Defaults for Config's zero values.
const (
	DefaultFlushInterval = 250 * time.Millisecond
	DefaultFlushBytes    = 64 << 10
	DefaultMaxPostBytes  = 512 << 10
	DefaultMaxBuffered   = 16 << 20
	DefaultBackoff       = 200 * time.Millisecond
	maxBackoff           = 10 * time.Second
)

// Chunk is one contiguous piece of an attempt's output. Seq is 1-based
// and gapless within a shipper.
type Chunk struct {
	Seq  int64
	Data []byte
}

// Sink delivers one batch to the server. It returns nil when every chunk
// was stored (or already had been), an error wrapping ErrStale when the
// server answered 409 (this attempt is no longer the running one), an
// error wrapping ErrRejected when the batch can never be accepted (any
// other 4xx), and any other error for a transient failure — the shipper
// then resends the identical batch after a backoff.
type Sink func(ctx context.Context, chunks []Chunk) error

var (
	// ErrStale is the 409: the attempt was superseded. The shipper drops
	// its buffer, calls Config.OnStale once and sends nothing more.
	ErrStale = errors.New("logship: attempt is not the running attempt")
	// ErrRejected marks a batch the server refuses for good; it is dropped
	// and reported by Close.
	ErrRejected = errors.New("logship: batch rejected")
)

// Config tunes a Shipper. Zero values take the defaults above.
type Config struct {
	// FlushInterval is how long buffered output waits before it is sent.
	FlushInterval time.Duration
	// FlushBytes is the chunk size: reaching it seals a chunk and flushes.
	FlushBytes int
	// MaxPostBytes bounds the chunk data in one Sink call.
	MaxPostBytes int
	// MaxBuffered bounds unsent output held in memory while the server is
	// unreachable; beyond it Write drops bytes and notes how many in the
	// stream once room returns.
	MaxBuffered int
	// Backoff is the first retry delay after a transient send failure;
	// it doubles up to 10 s.
	Backoff time.Duration
	// OnStale is called once, from the flush loop, when a send returns
	// ErrStale. The agent uses it to kill the attempt.
	OnStale func()
}

// Shipper buffers and ships one attempt's output. It is safe for
// concurrent Writes.
type Shipper struct {
	cfg  Config
	send Sink

	mu       sync.Mutex
	cur      []byte  // bytes not yet sealed into a chunk
	pending  []Chunk // sealed, unsent, in seq order
	buffered int     // len(cur) + sum(len(pending[i].Data))
	dropped  int64   // bytes dropped since the last marker
	nextSeq  int64
	closed   bool
	stale    bool
	err      error // first permanent error, for Close
	closeCtx context.Context

	kick      chan struct{} // Write → loop: a chunk is ready
	stop      chan struct{} // Close → loop: flush and exit
	done      chan struct{} // loop → Close: exited
	runCtx    context.Context
	cancelRun context.CancelFunc
	staleOnce sync.Once
}

// New starts a Shipper that delivers through send.
func New(cfg Config, send Sink) *Shipper {
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = DefaultFlushInterval
	}
	if cfg.FlushBytes <= 0 {
		cfg.FlushBytes = DefaultFlushBytes
	}
	if cfg.MaxPostBytes < cfg.FlushBytes {
		cfg.MaxPostBytes = max(DefaultMaxPostBytes, cfg.FlushBytes)
	}
	if cfg.MaxBuffered <= 0 {
		cfg.MaxBuffered = DefaultMaxBuffered
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = DefaultBackoff
	}
	cfg.Backoff = min(cfg.Backoff, maxBackoff)
	s := &Shipper{
		cfg: cfg, send: send, nextSeq: 1,
		kick: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
	}
	s.runCtx, s.cancelRun = context.WithCancel(context.Background())
	go s.loop()
	return s
}

// Write buffers p. It never blocks on the network and always reports p as
// written: a job must not fail because its logs cannot be shipped. Output
// past MaxBuffered is dropped (and noted), as is anything written after
// Close or after the attempt went stale.
func (s *Shipper) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.stale {
		return len(p), nil
	}
	room := s.cfg.MaxBuffered - s.buffered
	if s.dropped > 0 && room > 0 {
		// The marker is exempt from the cap: it is what makes the loss
		// visible, and it is written once per episode.
		s.append([]byte(fmt.Sprintf("[quarry] log shipper dropped %d bytes\n", s.dropped)))
		s.dropped = 0
	}
	if room <= 0 {
		s.dropped += int64(len(p))
		return len(p), nil
	}
	if len(p) > room {
		s.dropped += int64(len(p) - room)
		p = p[:room]
	}
	s.append(p)
	return len(p), nil
}

// append copies p into the buffer, sealing chunks as FlushBytes fill.
// Caller holds mu.
func (s *Shipper) append(p []byte) {
	s.cur = append(s.cur, p...)
	s.buffered += len(p)
	sealed := false
	for len(s.cur) >= s.cfg.FlushBytes {
		s.seal(s.cfg.FlushBytes)
		sealed = true
	}
	if sealed {
		select {
		case s.kick <- struct{}{}:
		default:
		}
	}
}

// seal moves the first n bytes of cur into a new pending chunk with the
// next seq. Caller holds mu.
func (s *Shipper) seal(n int) {
	data := make([]byte, n)
	copy(data, s.cur[:n])
	s.cur = s.cur[:copy(s.cur, s.cur[n:])]
	s.pending = append(s.pending, Chunk{Seq: s.nextSeq, Data: data})
	s.nextSeq++
}

// take seals whatever is buffered and returns the first batch of pending
// chunks, up to MaxPostBytes. The chunks stay pending until ack.
func (s *Shipper) take() []Chunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cur) > 0 {
		s.seal(len(s.cur))
	}
	var n, size int
	for n < len(s.pending) && (n == 0 || size+len(s.pending[n].Data) <= s.cfg.MaxPostBytes) {
		size += len(s.pending[n].Data)
		n++
	}
	return s.pending[:n:n]
}

// ack forgets the first n pending chunks once the server has them.
func (s *Shipper) ack(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.pending[:n] {
		s.buffered -= len(c.Data)
	}
	s.pending = s.pending[n:]
}

// loop flushes on the ticker and on kicks until Close, then performs the
// final flush under Close's context and exits.
func (s *Shipper) loop() {
	defer close(s.done)
	defer s.cancelRun()
	t := time.NewTicker(s.cfg.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
		case <-s.runCtx.Done():
			<-s.stop
		case <-t.C:
			s.drain(s.runCtx, false)
			continue
		case <-s.kick:
			s.drain(s.runCtx, false)
			continue
		}
		s.mu.Lock()
		ctx := s.closeCtx
		s.mu.Unlock()
		s.drain(ctx, true)
		return
	}
}

// drain sends batches until nothing is pending, the attempt is stale, or
// ctx ends. Transient failures back off and resend the same batch; while
// not final the wait also yields to Close so the final flush can take
// over under its own context.
func (s *Shipper) drain(ctx context.Context, final bool) {
	backoff := s.cfg.Backoff
	for {
		if ctx.Err() != nil {
			return
		}
		batch := s.take()
		if len(batch) == 0 {
			return
		}
		err := s.send(ctx, batch)
		switch {
		case err == nil:
			s.ack(len(batch))
			backoff = s.cfg.Backoff
		case errors.Is(err, ErrStale):
			s.markStale()
			return
		case errors.Is(err, ErrRejected):
			s.ack(len(batch))
			s.setErr(err)
		case ctx.Err() != nil:
			return
		default:
			timer := time.NewTimer(backoff)
			var stop <-chan struct{}
			if !final {
				stop = s.stop
			}
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-stop:
				timer.Stop()
				return
			case <-timer.C:
			}
			if backoff *= 2; backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// markStale drops everything, stops further sends and fires OnStale once.
func (s *Shipper) markStale() {
	s.mu.Lock()
	s.stale = true
	s.cur, s.pending, s.buffered, s.dropped = nil, nil, 0, 0
	s.mu.Unlock()
	s.cancelRun()
	s.staleOnce.Do(func() {
		if s.cfg.OnStale != nil {
			s.cfg.OnStale()
		}
	})
}

func (s *Shipper) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

// Close stops accepting output, flushes everything still buffered (bounded
// by ctx; a ctx that is already done skips the flush and just discards)
// and waits for the flush loop to exit. It returns ErrStale if the server
// rejected the attempt, the first ErrRejected otherwise, or an error
// wrapping ctx.Err() if unsent output remained when ctx ended. Calling it
// twice is fine; the second call just returns the same result.
func (s *Shipper) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.closeCtx = ctx
		s.mu.Unlock()
		// A deadline interrupts a stuck send. Let an otherwise healthy
		// in-flight acknowledgement finish before the final flush takes over.
		stopCancel := context.AfterFunc(ctx, s.cancelRun)
		defer stopCancel()
		close(s.stop)
	} else {
		s.mu.Unlock()
	}
	<-s.done

	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.stale:
		return ErrStale
	case s.err != nil:
		return s.err
	case s.buffered > 0:
		return fmt.Errorf("logship: %d bytes unsent (%d dropped): %w", s.buffered, s.dropped, ctx.Err())
	case s.dropped > 0:
		return fmt.Errorf("logship: %d bytes dropped at the buffer cap", s.dropped)
	}
	return nil
}
