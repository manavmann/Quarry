package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

type reconnectTransport func(*http.Request) (*http.Response, error)

func (f reconnectTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func reconnectReply(body string) *http.Response {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func TestConnectionRetryBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls []time.Time
		c := NewClient("http://server", "token")
		c.Retries = 1
		c.HTTP = &http.Client{Transport: reconnectTransport(func(r *http.Request) (*http.Response, error) {
			if r.URL.Query().Get("after") != "17" || r.URL.Query().Get("attempt") != "1" {
				t.Fatalf("cursor changed: %s", r.URL)
			}
			calls = append(calls, time.Now())
			if len(calls) <= 10 {
				return nil, errors.New("connection refused")
			}
			return reconnectReply(`{"attempt":1,"chunks":[{"seq":18,"data":"dGFpbA=="}],"next":18}`), nil
		})}
		l, err := c.GetLogs(t.Context(), "job", 1, 17)
		if err != nil || l.Next != 18 || len(calls) != 11 {
			t.Fatalf("logs=%+v calls=%d err=%v", l, len(calls), err)
		}
		delay := 200 * time.Millisecond
		for i := 1; i < len(calls); i++ {
			if got := calls[i].Sub(calls[i-1]); got != delay {
				t.Fatalf("retry %d delay=%s want=%s", i, got, delay)
			}
			delay = min(delay*2, 10*time.Second)
		}
	})
	t.Run("cancellation and initial cap", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c := NewClient("http://server", "token")
			c.Backoff = time.Hour
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls, start := 0, time.Now()
			err := c.retry(ctx, func() error {
				calls++
				if calls == 2 {
					cancel()
				}
				return &connectionError{io.ErrUnexpectedEOF}
			})
			if !errors.Is(err, context.Canceled) || calls != 2 || time.Since(start) != 10*time.Second {
				t.Fatalf("err=%v calls=%d elapsed=%s", err, calls, time.Since(start))
			}
		})
	})
}

type brokenRead struct{}

func (brokenRead) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestDownloadReconnectAfterPartialRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		c := NewClient("http://server", "token")
		c.HTTP = &http.Client{Transport: reconnectTransport(func(*http.Request) (*http.Response, error) {
			calls++
			r := reconnectReply("whole artifact")
			if calls == 1 {
				r.Body = io.NopCloser(io.MultiReader(strings.NewReader("whole"), brokenRead{}))
			}
			return r, nil
		})}
		var out bytes.Buffer
		sum, err := c.DownloadArtifact(t.Context(), "job", 1, "out", &out)
		want := fmt.Sprintf("%x", sha256.Sum256([]byte("whole artifact")))
		if err != nil || out.String() != "whole artifact" || sum != want || calls != 2 {
			t.Fatalf("body=%q sum=%s calls=%d err=%v", out.String(), sum, calls, err)
		}
	})
}

func TestReconnectKeepsPermanentErrorsFinal(t *testing.T) {
	for _, status := range []int{401, 404, 409, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			calls := 0
			c := NewClient("http://server", "token")
			c.HTTP = &http.Client{Transport: reconnectTransport(func(*http.Request) (*http.Response, error) {
				calls++
				r := reconnectReply("{}")
				r.StatusCode = status
				return r, nil
			})}
			_, err := c.GetRun(t.Context(), "r")
			var api *APIError
			if !errors.As(err, &api) || api.Status != status || calls != 1 {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
}
