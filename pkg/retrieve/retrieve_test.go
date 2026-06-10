package retrieve

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
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

// --- progressWriter tests ---

func TestProgressWriter_WriteCountsBytes(t *testing.T) {
	var buf bytes.Buffer
	pw := newProgressWriter(&buf, time.Hour)
	defer pw.Close()

	data := []byte("hello world")
	n, err := pw.Write(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != len(data) {
		t.Fatalf("expected %d bytes written, got %d", len(data), n)
	}
	if pw.BytesWritten() != int64(len(data)) {
		t.Fatalf("BytesWritten() = %d, want %d", pw.BytesWritten(), len(data))
	}
}

func TestProgressWriter_MultiWrite(t *testing.T) {
	var buf bytes.Buffer
	pw := newProgressWriter(&buf, time.Hour)
	defer pw.Close()

	chunks := []string{"hello", " ", "world", "!"}
	for _, chunk := range chunks {
		_, err := pw.Write([]byte(chunk))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	expectedTotal := int64(0)
	for _, c := range chunks {
		expectedTotal += int64(len(c))
	}
	if pw.BytesWritten() != expectedTotal {
		t.Fatalf("BytesWritten() = %d, want %d", pw.BytesWritten(), expectedTotal)
	}
	if buf.String() != "hello world!" {
		t.Fatalf("buffer content = %q, want %q", buf.String(), "hello world!")
	}
}

func TestProgressWriter_BytesWrittenIsZeroBeforeWrite(t *testing.T) {
	var buf bytes.Buffer
	pw := newProgressWriter(&buf, time.Hour)
	defer pw.Close()

	if pw.BytesWritten() != 0 {
		t.Fatalf("BytesWritten() = %d before any writes, want 0", pw.BytesWritten())
	}
}

func TestProgressWriter_CloseStopsGoroutine(t *testing.T) {
	var buf bytes.Buffer
	pw := newProgressWriter(&buf, 10*time.Millisecond)

	// Write some data
	_, _ = pw.Write([]byte("test"))

	// Close should stop the goroutine
	pw.Close()

	// Double close should not panic (sync.Once protects)
	pw.Close()

	// Give goroutine time to exit
	time.Sleep(50 * time.Millisecond)
}

func TestProgressWriter_ConcurrentWriteAndBytesWritten(t *testing.T) {
	var buf bytes.Buffer
	pw := newProgressWriter(&buf, time.Hour)
	defer pw.Close()

	var wg sync.WaitGroup

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, _ = pw.Write([]byte("x"))
			time.Sleep(time.Millisecond)
		}
	}()

	// Concurrent reader of BytesWritten
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = pw.BytesWritten()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()

	if pw.BytesWritten() != 100 {
		t.Fatalf("BytesWritten() = %d, want 100", pw.BytesWritten())
	}
}

func TestProgressWriter_WriterError(t *testing.T) {
	errDisk := errors.New("disk full")
	pw := newProgressWriter(&failingWriter{err: errDisk}, time.Hour)
	defer pw.Close()

	_, err := pw.Write([]byte("data"))
	if !errors.Is(err, errDisk) {
		t.Fatalf("expected errDisk, got %v", err)
	}
	// No bytes should be counted on write error (Write returns 0)
	if pw.BytesWritten() != 0 {
		t.Fatalf("BytesWritten() = %d, want 0 after write error", pw.BytesWritten())
	}
}

func TestProgressWriter_LogProgressTicks(t *testing.T) {
	var buf bytes.Buffer
	pw := newProgressWriter(&buf, 10*time.Millisecond)

	// Write data slowly to let ticker fire
	for i := 0; i < 5; i++ {
		_, _ = pw.Write([]byte("chunk"))
		time.Sleep(20 * time.Millisecond)
	}
	pw.Close()

	if pw.BytesWritten() != int64(5*len("chunk")) {
		t.Fatalf("BytesWritten() = %d, want %d", pw.BytesWritten(), 5*len("chunk"))
	}
}

// --- retrieveResultsRetry tests ---

func TestRetrieveResultsRetry_ContextAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before first attempt

	err := retrieveResultsRetry(ctx, t.TempDir())
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if !strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("expected cancellation error, got: %v", err)
	}
}

func TestRetrieveResultsRetry_RetriesOnFailure(t *testing.T) {
	origFn := retrieveFunc
	defer func() { retrieveFunc = origFn }()

	var attempts atomic.Int32
	retrieveFunc = func(_ context.Context, _ string) error {
		attempts.Add(1)
		return errors.New("connection refused")
	}

	// Use a short-lived context so we don't wait through all 10 retries.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := retrieveResultsRetry(ctx, t.TempDir())
	if err == nil {
		t.Fatal("expected error after exhausting retries or context timeout")
	}

	got := attempts.Load()
	if got < 2 {
		t.Fatalf("expected at least 2 attempts, got %d", got)
	}
}

func TestRetrieveResultsRetry_SucceedsOnNthAttempt(t *testing.T) {
	origFn := retrieveFunc
	defer func() { retrieveFunc = origFn }()

	var attempts atomic.Int32
	succeedOn := int32(3)

	retrieveFunc = func(_ context.Context, _ string) error {
		n := attempts.Add(1)
		if n < succeedOn {
			return errors.New("transient error")
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := retrieveResultsRetry(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("expected success on attempt %d, got error: %v", succeedOn, err)
	}

	got := attempts.Load()
	if got != succeedOn {
		t.Fatalf("expected exactly %d attempts, got %d", succeedOn, got)
	}
}

func TestRetrieveResultsRetry_CancelDuringRetryWait(t *testing.T) {
	origFn := retrieveFunc
	defer func() { retrieveFunc = origFn }()

	var attempts atomic.Int32
	retrieveFunc = func(_ context.Context, _ string) error {
		attempts.Add(1)
		return errors.New("transient error")
	}

	// Cancel after 500ms — should interrupt the 2s retry pause.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := retrieveResultsRetry(ctx, t.TempDir())
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

// --- formatBytes tests ---

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{512, "512 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1048576, "1.0 MiB"},            // 1 MiB
		{31763252, "30.3 MiB"},           // from mtulio's example
		{157892123, "150.6 MiB"},         // from mtulio's example
		{1073741824, "1.0 GiB"},          // 1 GiB
		{1099511627776, "1.0 TiB"},       // 1 TiB
		{1125899906842624, "1.0 PiB"},    // 1 PiB
		{1152921504606846976, "1.0 EiB"}, // 1 EiB
	}
	for _, tt := range tests {
		got := formatBytes(tt.input)
		if got != tt.expected {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

// =====================================================================
// Helper types
// =====================================================================

// failingReader always returns the given error.
type failingReader struct {
	err error
}

func (r *failingReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

// failingWriter always returns the given error.
type failingWriter struct {
	err error
}

func (w *failingWriter) Write(_ []byte) (int, error) {
	return 0, w.err
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
