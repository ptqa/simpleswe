package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	WorkerResultPathVariable = "SIMPLESWE_WORKER_RESULT_PATH"
	workerResultVersion      = 1
	workerOutcomePullRequest = "pull_request"
	workerOutcomeFailed      = "failed"
	maxWorkerFailureReason   = 32 << 10
	// encoding/json's default HTML escaping expands one input byte to at most six bytes.
	workerResultJSONEscapeMaxBytes   = len(`\u003c`)
	workerFailureResultEnvelopeBytes = len(`{"version":1,"outcome":"failed","reason":""}`)
	// A trailing newline is accepted because external reporters commonly write one.
	maxWorkerResultEncodedBytes = workerFailureResultEnvelopeBytes + maxWorkerFailureReason*workerResultJSONEscapeMaxBytes + len("\n")
	workerReportUsage           = "usage: simpleswe worker report (--pull-request NUMBER | --failure REASON)"
)

type workerResult struct {
	Version           int    `json:"version"`
	Outcome           string `json:"outcome"`
	PullRequestNumber int    `json:"pull_request_number,omitempty"`
	Reason            string `json:"reason,omitempty"`
}

// Report writes the result of one OpenCode invocation to the private path
// selected by its supervising worker.
func Report(args []string) error {
	if len(args) != 2 {
		return errors.New(workerReportUsage)
	}
	result := workerResult{Version: workerResultVersion}
	switch args[0] {
	case "--pull-request":
		number, err := strconv.Atoi(args[1])
		if err != nil {
			return errors.New(workerReportUsage)
		}
		result.Outcome, result.PullRequestNumber = workerOutcomePullRequest, number
	case "--failure":
		result.Outcome, result.Reason = workerOutcomeFailed, args[1]
	default:
		return errors.New(workerReportUsage)
	}
	if err := validateWorkerResult(result); err != nil {
		return fmt.Errorf("invalid worker report: %w", err)
	}
	path := os.Getenv(WorkerResultPathVariable)
	if strings.TrimSpace(path) == "" {
		return errors.New("worker-controlled result path is not configured")
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode worker report: %w", err)
	}
	return writeWorkerReport(path, payload)
}

func validateWorkerResult(result workerResult) error {
	if result.Version != workerResultVersion {
		return fmt.Errorf("worker result version must be %d", workerResultVersion)
	}
	switch result.Outcome {
	case workerOutcomePullRequest:
		if result.PullRequestNumber <= 0 || result.Reason != "" {
			return errors.New("pull request result requires only a positive pull request number")
		}
	case workerOutcomeFailed:
		if result.PullRequestNumber != 0 {
			return errors.New("failed result must not contain a pull request number")
		}
		if strings.TrimSpace(result.Reason) == "" {
			return errors.New("worker failure reason is blank")
		}
		if len(result.Reason) > maxWorkerFailureReason {
			return errors.New("worker failure reason is too long")
		}
		if !utf8.ValidString(result.Reason) {
			return errors.New("worker failure reason is not valid UTF-8")
		}
		if strings.IndexFunc(result.Reason, unicode.IsControl) >= 0 {
			return errors.New("worker failure reason contains control characters")
		}
	default:
		return fmt.Errorf("unsupported worker result outcome %q", result.Outcome)
	}
	return nil
}

func writeWorkerReport(path string, payload []byte) (resultErr error) {
	if len(payload) > maxWorkerResultEncodedBytes {
		return errors.New("worker report is too large")
	}
	directory := filepath.Dir(path)
	root, err := os.OpenRoot(directory)
	if err != nil {
		return fmt.Errorf("open atomic worker report directory: %w", err)
	}
	defer func() {
		if err := root.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close atomic worker report directory: %w", err))
		}
	}()
	resultName := filepath.Base(path)
	temporary, err := os.CreateTemp(directory, "."+resultName+".*")
	if err != nil {
		return fmt.Errorf("create atomic worker report: %w", err)
	}
	temporaryName := filepath.Base(temporary.Name())
	defer func() {
		if err := root.Remove(temporaryName); err != nil && !errors.Is(err, os.ErrNotExist) {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove atomic worker report: %w", err))
		}
	}()
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write worker report: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync worker report: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close worker report: %w", err)
	}
	if err := root.Link(temporaryName, resultName); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("publish atomic worker report: %w", err)
	}
	existing, err := root.ReadFile(resultName)
	if err != nil {
		return fmt.Errorf("read existing worker report: %w", err)
	}
	if !bytes.Equal(existing, payload) {
		return errors.New("worker report conflict: a different result is already recorded")
	}
	return nil
}
