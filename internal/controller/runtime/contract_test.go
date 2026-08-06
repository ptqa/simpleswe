package runtime

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

// Concrete contract exercised by this package:
//
//	NewBackend(*store.Store, Controller) *Backend
//	NewRuntime(kubernetes.Interface, *store.Store, Controller, *Backend, Options) (*Runtime, error)
//	(*Runtime).Run(context.Context) error
//	(*Runtime).CollectPodLogs(context.Context, *corev1.Pod) error
//
// Backend implements every method consumed by api.NewHandler. Options includes
// Namespace, LogChunkBytes, SecretRetention, PodLogs, Logger, and Clock. Clock
// supplies Now and After so retention is testable without sleeping.

type fakeController struct {
	store *store.Store

	mu         sync.Mutex
	cancelled  []string
	retried    []string
	reconciles int
	events     []workerEventCall
	exhausted  []workerEventCall
	reconcileC chan struct{}
	eventC     chan struct{}
}

type workerEventCall struct {
	job   string
	pod   string
	event protocol.Event
}

func newFakeController(db *store.Store) *fakeController {
	return &fakeController{
		store:      db,
		reconcileC: make(chan struct{}, 16),
		eventC:     make(chan struct{}, 16),
	}
}

func (f *fakeController) CreateTask(ctx context.Context, params store.CreateTaskParams) (store.Task, error) {
	return f.store.CreateTask(ctx, params)
}

func (f *fakeController) Cancel(_ context.Context, taskID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = append(f.cancelled, taskID)
	return nil
}

func (f *fakeController) Retry(ctx context.Context, taskID string) (store.Attempt, error) {
	f.mu.Lock()
	f.retried = append(f.retried, taskID)
	f.mu.Unlock()
	return f.store.CurrentAttempt(ctx, taskID)
}

func (f *fakeController) RetryWithKey(ctx context.Context, taskID, _ string) (store.Attempt, error) {
	return f.Retry(ctx, taskID)
}

func (f *fakeController) Reconcile(context.Context) error {
	f.mu.Lock()
	f.reconciles++
	f.mu.Unlock()
	select {
	case f.reconcileC <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeController) HandleWorkerEvent(_ context.Context, job, pod string, event protocol.Event) error {
	f.mu.Lock()
	f.events = append(f.events, workerEventCall{job: job, pod: pod, event: event})
	f.mu.Unlock()
	select {
	case f.eventC <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeController) WorkerLogsExhausted(_ context.Context, job, pod string) error {
	f.mu.Lock()
	f.exhausted = append(f.exhausted, workerEventCall{job: job, pod: pod})
	f.mu.Unlock()
	return nil
}

func (f *fakeController) snapshot() (cancelled, retried []string, reconciles int, events []workerEventCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cancelled...), append([]string(nil), f.retried...), f.reconciles, append([]workerEventCall(nil), f.events...)
}

type fakePodLogs struct {
	mu      sync.Mutex
	content map[string][]string
	results map[string][]podLogResult
	opens   chan string
	counts  map[string]int
	onOpen  func(string, int)
}

type podLogResult struct {
	content string
	openErr error
	readErr error
}

func (f *fakePodLogs) Open(_ context.Context, _, pod, _ string, _ *time.Time) (io.ReadCloser, error) {
	f.mu.Lock()
	if f.counts == nil {
		f.counts = make(map[string]int)
	}
	f.counts[pod]++
	count := f.counts[pod]
	results := f.results[pod]
	if len(results) > 0 {
		result := results[0]
		f.results[pod] = results[1:]
		onOpen := f.onOpen
		f.mu.Unlock()
		if onOpen != nil {
			onOpen(pod, count)
		}
		if result.openErr != nil {
			return nil, result.openErr
		}
		return &podLogReader{Reader: strings.NewReader(result.content), err: result.readErr}, nil
	}
	values := f.content[pod]
	if len(values) == 0 {
		f.mu.Unlock()
		return io.NopCloser(strings.NewReader("")), nil
	}
	value := values[0]
	f.content[pod] = values[1:]
	if f.opens != nil {
		select {
		case f.opens <- pod:
		default:
		}
	}
	onOpen := f.onOpen
	f.mu.Unlock()
	if onOpen != nil {
		onOpen(pod, count)
	}
	return io.NopCloser(strings.NewReader(value)), nil
}

type podLogReader struct {
	*strings.Reader
	err error
}

func (r *podLogReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF && r.err != nil {
		err, r.err = r.err, nil
	}
	return n, err
}

func (*podLogReader) Close() error { return nil }

type manualClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []clockWaiter
	added   chan struct{}
}

type clockWaiter struct {
	at time.Time
	c  chan time.Time
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{now: now, added: make(chan struct{}, 16)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) After(delay time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(chan time.Time, 1)
	c.waiters = append(c.waiters, clockWaiter{at: c.now.Add(delay), c: result})
	select {
	case c.added <- struct{}{}:
	default:
	}
	return result
}

func (c *manualClock) Step(delta time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delta)
	now := c.now
	var pending []clockWaiter
	for _, waiter := range c.waiters {
		if waiter.at.After(now) {
			pending = append(pending, waiter)
			continue
		}
		waiter.c <- now
		close(waiter.c)
	}
	c.waiters = pending
	c.mu.Unlock()
}

func waitSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func managedPod(namespace, name, job, taskID, attempt, attemptID string, jobUID types.UID) *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			UID:       types.UID(name + "-uid"),
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "simpleswe",
				"simpleswe.dev/task-id":        taskID,
				"simpleswe.dev/attempt":        attempt,
				"simpleswe.dev/attempt-id":     attemptID,
			},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: job, UID: jobUID, Controller: &controller}},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}},
	}
}
