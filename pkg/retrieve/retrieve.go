package retrieve

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	sonobuoyclient "github.com/vmware-tanzu/sonobuoy/pkg/client"
	config2 "github.com/vmware-tanzu/sonobuoy/pkg/config"
	"golang.org/x/sync/errgroup"

	"github.com/redhat-openshift-ecosystem/opct/internal/cleaner"
	"github.com/redhat-openshift-ecosystem/opct/pkg"
	"github.com/redhat-openshift-ecosystem/opct/pkg/status"
)

func NewCmdRetrieve() *cobra.Command {
	return &cobra.Command{
		Use:   "retrieve",
		Args:  cobra.MaximumNArgs(1),
		Short: "Collect results from validation environment",
		Long:  `Downloads the results archive from the validation environment`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
			defer cancel()

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
			if err := retrieveResultsRetry(ctx, s.GetSonobuoyClient(), destinationDirectory); err != nil {
				return fmt.Errorf("retrieve finished with errors: %v", err)
			}

			log.Info("Use the results command to check the validation test summary or share the results archive with your Red Hat partner.")
			return nil
		},
	}
}

func retrieveResultsRetry(ctx context.Context, sclient sonobuoyclient.Interface, destinationDirectory string) error {
	var err error
	limit := 10 // Retry retrieve 10 times
	pause := time.Second * 2
	retries := 1
	for retries <= limit {
		select {
		case <-ctx.Done():
			return fmt.Errorf("retrieval cancelled: %w", ctx.Err())
		default:
		}

		err = retrieveResults(ctx, sclient, destinationDirectory)
		if err != nil {
			log.Warn(err)
			if retries+1 < limit {
				log.Warnf("Retrying retrieval %d more times", limit-retries)
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("retrieval cancelled during retry wait: %w", ctx.Err())
			case <-time.After(pause):
			}
			retries++
			continue
		}
		return nil // Retrieved results without a problem
	}

	return fmt.Errorf("retrieval retry limit reached")
}

func retrieveResults(ctx context.Context, sclient sonobuoyclient.Interface, destinationDirectory string) error {
	// Get a reader that contains the tar output of the results directory.
	reader, ec, err := sclient.RetrieveResults(&sonobuoyclient.RetrieveConfig{
		Namespace: pkg.CertificationNamespace,
		Path:      config2.AggregatorResultsPath,
	})
	if err != nil {
		return fmt.Errorf("error retrieving results from sonobuoy: %w", err)
	}

	// Wrap reader with progress logging
	pr := newProgressReader(reader, 30*time.Second)

	// Download results into target directory
	results, err := writeResultsToDirectory(ctx, destinationDirectory, pr, ec)
	if err != nil {
		return fmt.Errorf("error retrieving results from sonobuoy: %w", err)
	}

	log.Infof("Download complete: %d bytes received", pr.BytesRead())

	// Log the new files to stdout
	for _, result := range results {
		// Rename the file prepending 'opct_' to the name.
		newFile := fmt.Sprintf("%s/opct_%s", filepath.Dir(result), strings.Replace(filepath.Base(result), "sonobuoy_", "", 1))
		log.Debugf("Renaming %s to %s", result, newFile)
		if err := os.Rename(result, newFile); err != nil {
			return fmt.Errorf("error renaming %s to %s: %w", result, newFile, err)
		}
		log.Infof("Results saved to %s", newFile)
	}

	return nil
}

func writeResultsToDirectory(ctx context.Context, outputDir string, r io.Reader, ec <-chan error) ([]string, error) {
	// Use errgroup.WithContext so that if either goroutine fails,
	// the other is notified via context cancellation.
	eg, egCtx := errgroup.WithContext(ctx)
	var results []string

	// workCtx is cancelled when the scan/extract goroutine finishes,
	// ensuring the error-channel goroutine does not block indefinitely
	// if the sonobuoy error channel is never closed.
	workCtx, workCancel := context.WithCancel(egCtx)

	eg.Go(func() error {
		select {
		case err := <-ec:
			return err
		case <-workCtx.Done():
			// The work goroutine finished; stop waiting for the error channel.
			return nil
		}
	})
	eg.Go(func() error {
		defer workCancel()

		// scanning for sensitive data
		scannedReader, _, err := cleaner.ScanPatchTarGzipReaderFor(r)
		if err != nil {
			return fmt.Errorf("error scanning results: %w", err)
		}

		// This untars the request itself, which is tar'd as just part of the API request, not the sonobuoy logic.
		filesCreated, err := sonobuoyclient.UntarAll(scannedReader, outputDir, "")
		if err != nil {
			return err
		}
		// Only print the filename if not extracting. Allows capturing the filename for scripting.
		results = append(results, filesCreated...)

		return nil
	})

	return results, eg.Wait()
}

// progressReader wraps an io.Reader and periodically logs the number of bytes read.
type progressReader struct {
	r        io.Reader
	bytes    atomic.Int64
	interval time.Duration
	done     chan struct{}
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
	if err == io.EOF {
		close(pr.done)
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
			log.Infof("Retrieve in progress: %d bytes received so far...", pr.bytes.Load())
		case <-pr.done:
			return
		}
	}
}
