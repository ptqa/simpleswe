//go:build !linux

package worker

import (
	"errors"
)

func isolateRunnerProcess() (func() error, error) {
	return nil, errors.New("runner process isolation is unsupported on this platform")
}
