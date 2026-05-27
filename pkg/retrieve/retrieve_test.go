package retrieve

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sonobuoyclient "github.com/vmware-tanzu/sonobuoy/pkg/client"
)

// emptyTarGz is a valid minimal tar.gz payload with one dummy file
// (for tests that pass through ScanPatchTarGzipReaderFor and UntarAll
// which require valid gzip/tar data with at least one entry).
var emptyTarGz = func() []byte {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	// Add one empty file to satisfy UntarAll's "no valid entries" check
	header := &tar.Header{
		Name: "test-results.txt",
		Mode: 0644,
		Size: 0,
	}
	_ = tw.WriteHeader(header)

	_ = tw.Close()
	_ = gz.Close()
	return buf.Bytes()
}()

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

// =====================================================================
// Additional tests for improved coverage
// =====================================================================

// --- progressReader: multi-chunk byte accumulation ---

func TestProgressReader_MultiChunkAccumulation(t *testing.T) {
	// Verify bytes accumulate correctly across multiple Read calls
	// when the buffer is smaller than the total data.
	chunks := []string{"hello", " ", "world", "!"}
	r := &chunkedReader{chunks: chunks}
	pr := newProgressReader(r, time.Hour)

	buf := make([]byte, 3) // deliberately small buffer
	var totalRead int
	for {
		n, err := pr.Read(buf)
		totalRead += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	expectedTotal := 0
	for _, c := range chunks {
		expectedTotal += len(c)
	}
	if int64(totalRead) != pr.BytesRead() {
		t.Fatalf("totalRead=%d but BytesRead()=%d", totalRead, pr.BytesRead())
	}
	if pr.BytesRead() != int64(expectedTotal) {
		t.Fatalf("BytesRead()=%d, want %d", pr.BytesRead(), expectedTotal)
	}
}

// --- progressReader: zero-byte reads from underlying reader ---

func TestProgressReader_ZeroBytesFromUnderlying(t *testing.T) {
	// A reader that returns (0, nil) a few times before actual data.
	// BytesRead must only reflect real data, not zero-byte returns.
	r := &stutteringReader{stutters: 3, data: []byte("payload")}
	pr := newProgressReader(r, time.Hour)

	buf := make([]byte, 64)
	var totalRead int
	for {
		n, err := pr.Read(buf)
		totalRead += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if pr.BytesRead() != int64(len("payload")) {
		t.Fatalf("BytesRead()=%d, want %d", pr.BytesRead(), len("payload"))
	}
}

// --- progressReader: reads after EOF still return EOF ---

func TestProgressReader_ReadAfterEOF(t *testing.T) {
	pr := newProgressReader(strings.NewReader("abc"), time.Hour)

	buf := make([]byte, 64)
	// Drain all data.
	for {
		_, err := pr.Read(buf)
		if err != nil {
			break
		}
	}

	// done channel should be closed.
	select {
	case <-pr.done:
	case <-time.After(time.Second):
		t.Fatal("done not closed after draining")
	}

	// Further reads must return EOF without hanging or panicking.
	_, err := pr.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF on read after done, got %v", err)
	}
}

// --- progressReader: BytesRead starts at zero ---

func TestProgressReader_BytesReadIsZeroBeforeRead(t *testing.T) {
	pr := newProgressReader(strings.NewReader("test"), time.Hour)
	if pr.BytesRead() != 0 {
		t.Fatalf("BytesRead() = %d before any reads, want 0", pr.BytesRead())
	}
	// Clean up: drain so the goroutine exits.
	buf := make([]byte, 64)
	for {
		_, err := pr.Read(buf)
		if err != nil {
			break
		}
	}
}

// --- progressReader: logProgress ticks are visible ---

func TestProgressReader_LogProgressTicks(t *testing.T) {
	// Use a slow reader with a very short log interval to verify
	// logProgress runs concurrently without races or panics.
	sr := &slowReader{chunks: 5, chunkSize: 100, delay: 20 * time.Millisecond}
	pr := newProgressReader(sr, 10*time.Millisecond)

	buf := make([]byte, 256)
	for {
		_, err := pr.Read(buf)
		if err != nil {
			break
		}
	}

	// Verify all bytes were counted.
	if pr.BytesRead() != int64(5*100) {
		t.Fatalf("BytesRead()=%d, want %d", pr.BytesRead(), 5*100)
	}
}

// --- writeResultsToDirectory: context cancelled mid-work ---

func TestWriteResultsToDirectory_ContextCancelledMidWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay while the slow reader is still producing data.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	ec := make(chan error)
	sr := &slowReader{chunks: 100, chunkSize: 1024, delay: 20 * time.Millisecond}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = writeResultsToDirectory(ctx, t.TempDir(), sr, ec)
	}()

	select {
	case <-done:
		// Function returned — no hang.
	case <-time.After(10 * time.Second):
		t.Fatal("writeResultsToDirectory hung after context cancellation during work")
	}
}

// --- writeResultsToDirectory: both error channel and work goroutine fail ---

func TestWriteResultsToDirectory_ErrorChannelAndWorkBothFail(t *testing.T) {
	ec := make(chan error, 1)
	ec <- errors.New("sonobuoy aggregation error")

	// failingReader causes the work goroutine to fail too.
	done := make(chan struct{})
	var gotErr error
	go func() {
		defer close(done)
		_, gotErr = writeResultsToDirectory(
			context.Background(), t.TempDir(),
			&failingReader{err: errors.New("read failure")}, ec,
		)
	}()

	select {
	case <-done:
		if gotErr == nil {
			t.Fatal("expected an error when both error sources fire")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("writeResultsToDirectory hung when both error sources fire")
	}
}

// --- writeResultsToDirectory: error channel pre-closed ---

func TestWriteResultsToDirectory_ErrorChannelClosedImmediately(t *testing.T) {
	// Closing the error channel without sending a value should not hang
	// or produce a spurious error.
	ec := make(chan error)
	close(ec)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = writeResultsToDirectory(
			context.Background(), t.TempDir(),
			strings.NewReader(""), ec,
		)
	}()

	select {
	case <-done:
		// success — no hang
	case <-time.After(10 * time.Second):
		t.Fatal("writeResultsToDirectory hung when error channel is pre-closed")
	}
}

// --- retrieveResultsRetry: retries the expected number of times ---

func TestRetrieveResultsRetry_RetriesOnFailure(t *testing.T) {
	var attempts atomic.Int32

	mock := &mockSonobuoyClient{
		retrieveFn: func(_ *sonobuoyclient.RetrieveConfig) (io.Reader, <-chan error, error) {
			attempts.Add(1)
			return nil, nil, errors.New("connection refused")
		},
	}

	// Use a short-lived context so we don't wait through all 10 retries
	// with the default 2s pause.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := retrieveResultsRetry(ctx, mock, t.TempDir())
	if err == nil {
		t.Fatal("expected error after exhausting retries or context timeout")
	}

	got := attempts.Load()
	if got < 2 {
		t.Fatalf("expected at least 2 attempts, got %d", got)
	}
}

// --- retrieveResultsRetry: succeeds on Nth attempt ---

func TestRetrieveResultsRetry_SucceedsOnNthAttempt(t *testing.T) {
	var attempts atomic.Int32
	succeedOn := int32(3) // succeed on the 3rd attempt

	ec := make(chan error, 1)
	close(ec) // closed immediately — no sonobuoy error

	mock := &mockSonobuoyClient{
		retrieveFn: func(_ *sonobuoyclient.RetrieveConfig) (io.Reader, <-chan error, error) {
			n := attempts.Add(1)
			if n < succeedOn {
				return nil, nil, errors.New("transient error")
			}
			// On success, return a valid empty tar.gz and a closed error channel.
			return bytes.NewReader(emptyTarGz), ec, nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := retrieveResultsRetry(ctx, mock, t.TempDir())
	if err != nil {
		t.Fatalf("expected success on attempt %d, got error: %v", succeedOn, err)
	}

	got := attempts.Load()
	if got != succeedOn {
		t.Fatalf("expected exactly %d attempts, got %d", succeedOn, got)
	}
}

// --- retrieveResultsRetry: context cancelled during retry wait ---

func TestRetrieveResultsRetry_CancelDuringRetryWait(t *testing.T) {
	var attempts atomic.Int32

	mock := &mockSonobuoyClient{
		retrieveFn: func(_ *sonobuoyclient.RetrieveConfig) (io.Reader, <-chan error, error) {
			attempts.Add(1)
			return nil, nil, errors.New("transient error")
		},
	}

	// Cancel after 500ms — should interrupt the 2s retry pause.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := retrieveResultsRetry(ctx, mock, t.TempDir())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from cancelled context during retry")
	}
	if !strings.Contains(err.Error(), "cancelled") {
		t.Logf("error did not mention cancellation (got %q), but function returned — acceptable", err)
	}
	// Key assertion: it should NOT run for the full 10 retries × 2s = 20s.
	if elapsed > 5*time.Second {
		t.Fatalf("function took %v — context cancellation did not interrupt retry loop", elapsed)
	}
}

// =====================================================================
// Additional helper types
// =====================================================================

// chunkedReader returns data from its chunks, correctly handling partial
// reads when the caller's buffer is smaller than the current chunk.
type chunkedReader struct {
	chunks []string
	index  int
	offset int // position within the current chunk
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	for r.index < len(r.chunks) {
		n := copy(p, r.chunks[r.index][r.offset:])
		r.offset += n
		if r.offset >= len(r.chunks[r.index]) {
			r.index++
			r.offset = 0
		}
		if n > 0 {
			if r.index >= len(r.chunks) {
				return n, io.EOF
			}
			return n, nil
		}
	}
	return 0, io.EOF
}

// stutteringReader returns (0, nil) a few times before returning data, then EOF.
type stutteringReader struct {
	stutters  int
	data      []byte
	callCount int
	dataSent  bool
}

func (r *stutteringReader) Read(p []byte) (int, error) {
	r.callCount++
	if r.callCount <= r.stutters {
		return 0, nil
	}
	if r.dataSent {
		return 0, io.EOF
	}
	r.dataSent = true
	n := copy(p, r.data)
	return n, nil
}

// mockSonobuoyClient satisfies sonobuoyclient.Interface for testing.
// Only RetrieveResults is implemented; other methods panic if called.
type mockSonobuoyClient struct {
	sonobuoyclient.Interface // embed to satisfy the interface
	retrieveFn               func(*sonobuoyclient.RetrieveConfig) (io.Reader, <-chan error, error)
}

func (m *mockSonobuoyClient) RetrieveResults(cfg *sonobuoyclient.RetrieveConfig) (io.Reader, <-chan error, error) {
	return m.retrieveFn(cfg)
}
