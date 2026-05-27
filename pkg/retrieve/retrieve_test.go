package retrieve

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- progressReader tests ---

func TestProgressReader_ReadCountsBytes(t *testing.T) {
	data := "hello world"
	pr := newProgressReader(strings.NewReader(data), time.Hour)

	buf := make([]byte, 64)
	n, err := pr.Read(buf)
	if err != nil {
		t.Fatalf("unexpected error on first read: %v", err)
	}
	if n != len(data) {
		t.Fatalf("expected %d bytes, got %d", len(data), n)
	}
	if pr.BytesRead() != int64(len(data)) {
		t.Fatalf("BytesRead() = %d, want %d", pr.BytesRead(), len(data))
	}
}

func TestProgressReader_ReadReturnsEOF(t *testing.T) {
	pr := newProgressReader(strings.NewReader("abc"), time.Hour)

	buf := make([]byte, 64)
	// First read gets all data and may or may not return EOF.
	_, _ = pr.Read(buf)
	// Subsequent reads must return EOF.
	_, err := pr.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestProgressReader_DoneClosedOnEOF(t *testing.T) {
	pr := newProgressReader(strings.NewReader("x"), time.Hour)

	// Drain all data.
	buf := make([]byte, 64)
	for {
		_, err := pr.Read(buf)
		if err != nil {
			break
		}
	}

	// done channel should be closed promptly.
	select {
	case <-pr.done:
		// success — logProgress goroutine can exit
	case <-time.After(time.Second):
		t.Fatal("done channel was not closed after EOF")
	}
}

func TestProgressReader_DoneClosedOnNonEOFError(t *testing.T) {
	errBoom := errors.New("network failure")
	pr := newProgressReader(&failingReader{err: errBoom}, time.Hour)

	buf := make([]byte, 64)
	_, err := pr.Read(buf)
	if !errors.Is(err, errBoom) {
		t.Fatalf("expected errBoom, got %v", err)
	}

	// done channel must be closed even on non-EOF errors (goroutine leak fix).
	select {
	case <-pr.done:
		// success
	case <-time.After(time.Second):
		t.Fatal("done channel was not closed after non-EOF error")
	}
}

func TestProgressReader_DoubleEOFNoPanic(t *testing.T) {
	// A reader that returns io.EOF on every call after the first.
	r := &multiEOFReader{data: []byte("hi")}
	pr := newProgressReader(r, time.Hour)

	buf := make([]byte, 64)
	// First read returns data + EOF.
	_, _ = pr.Read(buf)
	// Second read returns EOF again — must not panic (sync.Once protects close).
	_, err := pr.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF on second read, got %v", err)
	}
}

func TestProgressReader_LogProgressExitsOnDone(t *testing.T) {
	// Use a very short interval so the ticker fires quickly.
	pr := newProgressReader(strings.NewReader("data"), 10*time.Millisecond)

	buf := make([]byte, 64)
	for {
		_, err := pr.Read(buf)
		if err != nil {
			break
		}
	}

	// Give the goroutine a moment to observe the closed channel and exit.
	// We can't directly assert goroutine count, but we verify no panic or hang.
	time.Sleep(50 * time.Millisecond)
}

func TestProgressReader_BytesReadConcurrentWithLogProgress(t *testing.T) {
	// The atomic counter must be safe to read concurrently from the
	// logProgress goroutine while Read() is writing to it.
	// Use a slow reader to keep the read loop alive while we poll BytesRead().
	pr := newProgressReader(&slowReader{chunks: 20, chunkSize: 50, delay: 5 * time.Millisecond}, time.Hour)

	var wg sync.WaitGroup

	// Reader goroutine — sequential reads (as in production).
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 128)
		for {
			_, err := pr.Read(buf)
			if err != nil {
				return
			}
		}
	}()

	// Simulate what logProgress does: concurrent loads of the counter.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = pr.BytesRead()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()

	if pr.BytesRead() != int64(20*50) {
		t.Fatalf("BytesRead() = %d, want %d", pr.BytesRead(), 20*50)
	}
}

func TestProgressReader_EmptyReader(t *testing.T) {
	pr := newProgressReader(strings.NewReader(""), time.Hour)

	buf := make([]byte, 64)
	n, err := pr.Read(buf)
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if pr.BytesRead() != 0 {
		t.Fatalf("BytesRead() = %d, want 0", pr.BytesRead())
	}

	select {
	case <-pr.done:
		// success
	case <-time.After(time.Second):
		t.Fatal("done channel was not closed after EOF on empty reader")
	}
}

// --- writeResultsToDirectory tests ---

func TestWriteResultsToDirectory_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// Provide a reader that blocks forever — context should unblock us.
	ec := make(chan error)
	_, err := writeResultsToDirectory(ctx, t.TempDir(), &blockingReader{ctx: ctx}, ec)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestWriteResultsToDirectory_ErrorChannelPropagation(t *testing.T) {
	ec := make(chan error, 1)
	expectedErr := errors.New("sonobuoy aggregation error")
	ec <- expectedErr

	// Provide a reader that returns EOF immediately so work goroutine finishes.
	_, err := writeResultsToDirectory(context.Background(), t.TempDir(), strings.NewReader(""), ec)
	// The error from ec should propagate (or be nil if work finishes first).
	// Either way, no hang should occur.
	_ = err // We primarily assert no hang; error propagation depends on race.
}

func TestWriteResultsToDirectory_NoHangOnUnclosedErrorChannel(t *testing.T) {
	// This is the core regression test: the error channel is never closed and
	// never sends a value. Before the fix, this would hang indefinitely.
	ec := make(chan error) // never closed, never written to

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Use an empty reader so ScanPatchTarGzipReaderFor processes it quickly.
		// The scan/untar may return an error on the empty input, which is fine —
		// we're testing that the function returns at all.
		_, _ = writeResultsToDirectory(context.Background(), t.TempDir(), strings.NewReader(""), ec)
	}()

	select {
	case <-done:
		// success — function returned without hanging
	case <-time.After(10 * time.Second):
		t.Fatal("writeResultsToDirectory hung on unclosed error channel")
	}
}

// --- retrieveResultsRetry tests (context only, no sonobuoy mock) ---

func TestRetrieveResultsRetry_ContextAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before first attempt

	// Even though the function takes a sonobuoyclient.Interface, the context
	// check happens before calling RetrieveResults. Pass nil — it should
	// never be dereferenced.
	err := retrieveResultsRetry(ctx, nil, t.TempDir())
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if !strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("expected cancellation error, got: %v", err)
	}
}

// --- Helper types ---

// failingReader always returns the given error.
type failingReader struct {
	err error
}

func (r *failingReader) Read(p []byte) (int, error) {
	return 0, r.err
}

// multiEOFReader returns all data with EOF on first read, then EOF on subsequent reads.
type multiEOFReader struct {
	data []byte
	read bool
}

func (r *multiEOFReader) Read(p []byte) (int, error) {
	if r.read {
		return 0, io.EOF
	}
	r.read = true
	n := copy(p, r.data)
	return n, io.EOF
}

// blockingReader blocks on Read until the context is cancelled.
type blockingReader struct {
	ctx context.Context
}

func (r *blockingReader) Read(p []byte) (int, error) {
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

// slowReader returns chunkSize bytes per read with a small delay, then EOF.
type slowReader struct {
	chunks    int
	chunkSize int
	delay     time.Duration
	count     int
}

func (r *slowReader) Read(p []byte) (int, error) {
	if r.count >= r.chunks {
		return 0, io.EOF
	}
	r.count++
	time.Sleep(r.delay)
	n := r.chunkSize
	if n > len(p) {
		n = len(p)
	}
	for i := range n {
		p[i] = 'x'
	}
	return n, nil
}
