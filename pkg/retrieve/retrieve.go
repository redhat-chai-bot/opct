package retrieve

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	sonobuoyclient "github.com/vmware-tanzu/sonobuoy/pkg/client"
	config2 "github.com/vmware-tanzu/sonobuoy/pkg/config"
	pluginaggregation "github.com/vmware-tanzu/sonobuoy/pkg/plugin/aggregation"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/redhat-openshift-ecosystem/opct/internal/cleaner"
	"github.com/redhat-openshift-ecosystem/opct/pkg"
	opclient "github.com/redhat-openshift-ecosystem/opct/pkg/client"
	"github.com/redhat-openshift-ecosystem/opct/pkg/status"
)

// retrieveFunc is the function used by retrieveResultsRetry for each attempt.
// It is a package-level variable to allow injection in tests.
var retrieveFunc = retrieveResults

func NewCmdRetrieve() *cobra.Command {
	var skipRedact bool

	cmd := &cobra.Command{
		Use:   "retrieve",
		Args:  cobra.MaximumNArgs(1),
		Short: "Collect results from validation environment",
		Long:  `Downloads the results archive from the validation environment`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
			defer cancel()

			if skipRedact {
				log.Warn("═════════════════════════════════════════════════════════════")
				log.Warn("WARNING: --debug-only-skip-redact is enabled")
				log.Warn("WARNING: Sensitive data will NOT be redacted from the archive")
				log.Warn("WARNING: DO NOT share this archive externally")
				log.Warn("WARNING: Archive may contain credentials, tokens, and secrets")
				log.Warn("═════════════════════════════════════════════════════════════")
				cleaner.SkipRedaction = true
			}

			destinationDirectory, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("retrieve finished with errors: %v", err)
			}
			if len(args) == 1 {
				destinationDirectory = args[0]
				finfo, err := os.Stat(destinationDirectory)
				if err != nil {
					return fmt.Errorf("retrieve finished with errors: %v", err)
				}
				if !finfo.IsDir() {
					return fmt.Errorf("retrieve finished with errors: %v", err)
				}
			}

			s := status.NewStatus(&status.StatusInput{Watch: false})
			if err := s.PreRunCheck(); err != nil {
				return fmt.Errorf("retrieve finished with errors: %v", err)
			}

			log.Info("Collecting results...")
			if err := retrieveResultsRetry(ctx, destinationDirectory); err != nil {
				return fmt.Errorf("retrieve finished with errors: %v", err)
			}

			log.Info("Run 'opct report -s ./report <archive>.tar.gz' to review the validation results.")
			return nil
		},
	}

	cmd.Flags().BoolVar(&skipRedact, "debug-only-skip-redact", false,
		"Skip redaction of sensitive data (DEBUG ONLY - NOT RECOMMENDED)")
	_ = cmd.Flags().MarkHidden("debug-only-skip-redact")

	return cmd
}

func retrieveResultsRetry(ctx context.Context, destinationDirectory string) error {
	var err error
	limit := 10
	pause := time.Second * 2
	retries := 1
	for retries <= limit {
		select {
		case <-ctx.Done():
			return fmt.Errorf("retrieval cancelled: %w", ctx.Err())
		default:
		}

		err = retrieveFunc(ctx, destinationDirectory)
		if err != nil {
			log.Error(err)
			if retries+1 < limit {
				log.Warnf("Retrying retrieval %d more times after %d sec", limit-retries, pause/time.Second)
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("retrieval cancelled during retry wait: %w", ctx.Err())
			case <-time.After(pause):
			}
			retries++
			continue
		}
		return nil
	}

	return fmt.Errorf("retrieval retry limit reached")
}

func retrieveResults(ctx context.Context, destinationDirectory string) error {
	// Phase 1: Download archive to temp file
	tmpFile, err := downloadFromPod(ctx)
	if err != nil {
		return fmt.Errorf("error retrieving results from aggregator server: %w", err)
	}
	defer func() { _ = os.Remove(tmpFile) }()

	// Phase 2: Scan/redact from disk
	log.Info("Scanning archive for sensitive data...")
	fin, err := os.Open(tmpFile)
	if err != nil {
		return fmt.Errorf("error opening downloaded archive: %w", err)
	}
	defer func() { _ = fin.Close() }()

	scannedReader, _, err := cleaner.ScanPatchTarGzipReaderFor(fin)
	if err != nil {
		return fmt.Errorf("error scanning results: %w", err)
	}

	// Phase 3: Save scanned archive and extract
	scannedFile, err := os.CreateTemp("", "opct-scanned-*.tar.gz")
	if err != nil {
		return fmt.Errorf("error creating temp file for scanned archive: %w", err)
	}
	scannedPath := scannedFile.Name()
	defer func() { _ = os.Remove(scannedPath) }()

	if _, err := io.Copy(scannedFile, scannedReader); err != nil {
		_ = scannedFile.Close()
		return fmt.Errorf("error writing scanned archive: %w", err)
	}
	if err := scannedFile.Close(); err != nil {
		return fmt.Errorf("error closing scanned archive: %w", err)
	}

	// Reopen for extraction
	scannedIn, err := os.Open(scannedPath)
	if err != nil {
		return fmt.Errorf("error reopening scanned archive: %w", err)
	}
	defer func() { _ = scannedIn.Close() }()

	filesCreated, err := sonobuoyclient.UntarAll(scannedIn, destinationDirectory, "")
	if err != nil {
		return fmt.Errorf("error extracting results: %w", err)
	}

	for _, result := range filesCreated {
		newFile := fmt.Sprintf("%s/opct_%s", filepath.Dir(result), strings.Replace(filepath.Base(result), "sonobuoy_", "", 1))
		log.Debugf("Renaming %s to %s", result, newFile)
		if err := os.Rename(result, newFile); err != nil {
			return fmt.Errorf("error renaming %s to %s: %w", result, newFile, err)
		}
		log.Infof("Results saved to %s", newFile)
	}

	return nil
}

// downloadFromPod downloads the results archive from the sonobuoy aggregator pod
// to a temp file using WebSocket (with SPDY fallback).
func downloadFromPod(ctx context.Context) (string, error) {
	cli, err := opclient.NewClient()
	if err != nil {
		return "", fmt.Errorf("error creating kubernetes client: %w", err)
	}

	podName, err := pluginaggregation.GetAggregatorPodName(cli.KClient, pkg.CertificationNamespace)
	if err != nil {
		return "", fmt.Errorf("failed to get aggregator server's pod: %w", err)
	}

	restClient := cli.KClient.CoreV1().RESTClient()
	req := restClient.Post().
		Resource("pods").
		Name(podName).
		Namespace(pkg.CertificationNamespace).
		SubResource("exec").
		Param("container", config2.AggregatorContainerName)
	req.VersionedParams(&corev1.PodExecOptions{
		Container: config2.AggregatorContainerName,
		Command:   []string{"/sonobuoy", "splat", config2.AggregatorResultsPath},
		Stdin:     false,
		Stdout:    true,
		Stderr:    false,
	}, scheme.ParameterCodec)

	// WebSocket primary, SPDY fallback
	wsExec, err := remotecommand.NewWebSocketExecutor(cli.RestConfig, "POST", req.URL().String())
	if err != nil {
		return "", fmt.Errorf("error creating WebSocket executor: %w", err)
	}
	spdyExec, err := remotecommand.NewSPDYExecutor(cli.RestConfig, "POST", req.URL())
	if err != nil {
		return "", fmt.Errorf("error creating SPDY executor: %w", err)
	}
	exec, err := remotecommand.NewFallbackExecutor(wsExec, spdyExec, httpstream.IsUpgradeFailure)
	if err != nil {
		return "", fmt.Errorf("error creating fallback executor: %w", err)
	}

	tmpFile, err := os.CreateTemp("", "opct-retrieve-*.tar.gz")
	if err != nil {
		return "", fmt.Errorf("error creating temp file: %w", err)
	}

	log.Infof("Downloading archive from aggregator server...")
	log.Debugf("Discovered aggregator server running on pod %s/%s...", pkg.CertificationNamespace, podName)
	startTime := time.Now()

	// Wrap temp file with progress tracking to log download progress every 30s
	pw := newProgressWriter(tmpFile, 30*time.Second)

	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: pw,
		Tty:    false,
	})
	pw.Close()

	if err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		return "", fmt.Errorf("error streaming results from pod: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return "", fmt.Errorf("error closing temp file: %w", err)
	}

	log.Infof("Downloaded %s in %s", formatBytes(pw.BytesWritten()), time.Since(startTime).Round(time.Second))

	return tmpFile.Name(), nil
}

// progressReader wraps an io.Reader and periodically logs the number of bytes read.
type progressReader struct {
	r         io.Reader
	bytes     atomic.Int64
	interval  time.Duration
	done      chan struct{}
	closeOnce sync.Once
}

// newProgressReader creates a progressReader that logs bytes read every interval.
func newProgressReader(r io.Reader, interval time.Duration) *progressReader {
	pr := &progressReader{
		r:        r,
		interval: interval,
		done:     make(chan struct{}),
	}
	go pr.logProgress()
	return pr
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.r.Read(p)
	if n > 0 {
		pr.bytes.Add(int64(n))
	}
	if err != nil {
		pr.closeOnce.Do(func() { close(pr.done) })
	}
	return n, err
}

// BytesRead returns the total number of bytes read so far.
func (pr *progressReader) BytesRead() int64 {
	return pr.bytes.Load()
}

func (pr *progressReader) logProgress() {
	ticker := time.NewTicker(pr.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Infof("Retrieve in progress: %s received so far...", formatBytes(pr.bytes.Load()))
		case <-pr.done:
			return
		}
	}
}

// progressWriter wraps an io.Writer and periodically logs the number of bytes written.
// It mirrors progressReader but for write operations (e.g., streaming download to disk).
type progressWriter struct {
	w         io.Writer
	bytes     atomic.Int64
	interval  time.Duration
	done      chan struct{}
	closeOnce sync.Once
}

// newProgressWriter creates a progressWriter that logs bytes written every interval.
func newProgressWriter(w io.Writer, interval time.Duration) *progressWriter {
	pw := &progressWriter{
		w:        w,
		interval: interval,
		done:     make(chan struct{}),
	}
	go pw.logProgress()
	return pw
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n, err := pw.w.Write(p)
	if n > 0 {
		pw.bytes.Add(int64(n))
	}
	return n, err
}

// BytesWritten returns the total number of bytes written so far.
func (pw *progressWriter) BytesWritten() int64 {
	return pw.bytes.Load()
}

// Close stops the progress logging goroutine.
func (pw *progressWriter) Close() {
	pw.closeOnce.Do(func() { close(pw.done) })
}

func (pw *progressWriter) logProgress() {
	ticker := time.NewTicker(pw.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Infof("Retrieve in progress: %s received so far...", formatBytes(pw.bytes.Load()))
		case <-pw.done:
			return
		}
	}
}

// formatBytes converts bytes to human-readable format (KiB, MiB, GiB).
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
