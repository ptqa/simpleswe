package runtime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

const managedSelector = "app.kubernetes.io/managed-by=simpleswe"

// PodLogs opens one container's raw log stream.
type PodLogs interface {
	Open(context.Context, string, string, string, *time.Time) (io.ReadCloser, error)
}

// Clock is the small time surface needed for retention cleanup.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type Options struct {
	Namespace                 string
	LogChunkBytes             int
	MaxLogBytes               int
	SecretRetention           time.Duration
	PodLogs                   PodLogs
	Logger                    *slog.Logger
	Clock                     Clock
	RecoveryInterval          time.Duration
	NotifyPendingPullRequests func(context.Context) error
	ProcessForgeEvents        func(context.Context) error
}

// Runtime connects Kubernetes watches and Pod output to the durable controller.
type Runtime struct {
	kubernetes kubernetes.Interface
	store      *store.Store
	controller Controller
	logEvents  interface {
		WorkerLogsExhausted(context.Context, string, string) error
	}
	backend *Backend
	options Options

	mu       sync.Mutex
	running  bool
	pods     map[string]bool
	cleanups map[string]*cleanupRun
	tasks    sync.WaitGroup
}

type cleanupRun struct {
	jobUID string
	cancel context.CancelFunc
}

func NewRuntime(client kubernetes.Interface, db *store.Store, controller Controller, backend *Backend, options Options) (*Runtime, error) {
	if client == nil || db == nil || controller == nil || backend == nil {
		return nil, errors.New("runtime dependencies must not be nil")
	}
	logEvents, ok := controller.(interface {
		WorkerLogsExhausted(context.Context, string, string) error
	})
	if !ok {
		return nil, errors.New("runtime controller does not handle exhausted worker logs")
	}
	if strings.TrimSpace(options.Namespace) == "" {
		return nil, errors.New("runtime namespace is empty")
	}
	if options.LogChunkBytes <= 0 {
		options.LogChunkBytes = 64 << 10
	}
	if options.MaxLogBytes == 0 {
		options.MaxLogBytes = 64 << 20
	}
	if options.MaxLogBytes < len(store.LogTruncationMarker) {
		return nil, fmt.Errorf("runtime log byte quota must be at least %d", len(store.LogTruncationMarker))
	}
	if options.SecretRetention < 0 {
		return nil, errors.New("runtime Secret retention must not be negative")
	}
	if options.PodLogs == nil {
		options.PodLogs = kubernetesPodLogs{client: client}
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.Clock == nil {
		options.Clock = realClock{}
	}
	if options.RecoveryInterval <= 0 {
		options.RecoveryInterval = 30 * time.Second
	}
	return &Runtime{
		kubernetes: client, store: db, controller: controller, logEvents: logEvents, backend: backend, options: options,
		pods: make(map[string]bool), cleanups: make(map[string]*cleanupRun),
	}, nil
}

// Run watches until ctx is cancelled. Normal cancellation returns nil.
func (r *Runtime) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("runtime context is nil")
	}
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return errors.New("runtime is already running")
	}
	r.running = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.running = false
		r.mu.Unlock()
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 3)
	go func() { results <- r.watchJobs(ctx) }()
	go func() { results <- r.watchPods(ctx) }()
	go func() { results <- r.recoverDurableWork(ctx) }()
	var firstErr error
	for range 3 {
		if err := <-results; err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
	}
	cancel()
	r.tasks.Wait()
	return firstErr
}

func (r *Runtime) recoverDurableWork(ctx context.Context) error {
	delay := jitter(r.options.RecoveryInterval)
	maxDelay := min(8*r.options.RecoveryInterval, 5*time.Minute)
	for ctx.Err() == nil {
		if !sleepContext(ctx, delay) {
			return nil
		}
		err := r.recoverOnce(ctx)
		if err != nil {
			r.options.Logger.ErrorContext(ctx, "recover durable controller work", "error", err)
			delay = min(delay*2, maxDelay)
			continue
		}
		delay = jitter(r.options.RecoveryInterval)
	}
	return nil
}

func (r *Runtime) recoverOnce(ctx context.Context) error {
	var errs []error
	if r.options.ProcessForgeEvents != nil {
		if err := r.options.ProcessForgeEvents(ctx); err != nil {
			errs = append(errs, fmt.Errorf("process forge events: %w", err))
		}
	}
	if err := r.controller.Reconcile(ctx); err != nil {
		errs = append(errs, fmt.Errorf("reconcile tasks: %w", err))
	}
	jobs, err := r.kubernetes.BatchV1().Jobs(r.options.Namespace).List(ctx, metav1.ListOptions{LabelSelector: managedSelector})
	if err != nil {
		errs = append(errs, fmt.Errorf("list recovery Jobs: %w", err))
	} else {
		for i := range jobs.Items {
			r.observeJob(ctx, &jobs.Items[i])
			r.scheduleSecretCleanup(ctx, &jobs.Items[i], false)
		}
	}
	pods, err := r.kubernetes.CoreV1().Pods(r.options.Namespace).List(ctx, metav1.ListOptions{LabelSelector: managedSelector})
	if err != nil {
		errs = append(errs, fmt.Errorf("list recovery Pods: %w", err))
	} else {
		for i := range pods.Items {
			r.observePod(ctx, &pods.Items[i])
			r.startPodLogs(ctx, &pods.Items[i])
		}
	}
	r.recoverSecretCleanups(ctx)
	if r.options.NotifyPendingPullRequests != nil {
		if err := r.options.NotifyPendingPullRequests(ctx); err != nil {
			errs = append(errs, fmt.Errorf("notify pending pull requests: %w", err))
		}
	}
	return errors.Join(errs...)
}

func jitter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}
	spread := max(delay/5, time.Nanosecond)
	// #nosec G404 -- retry jitter is not security-sensitive randomness.
	return delay - spread + time.Duration(rand.Int64N(int64(2*spread)))
}

func (r *Runtime) watchJobs(ctx context.Context) error {
	for ctx.Err() == nil {
		jobs, err := r.kubernetes.BatchV1().Jobs(r.options.Namespace).List(ctx, metav1.ListOptions{LabelSelector: managedSelector})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !waitRetry(ctx) {
				return nil
			}
			continue
		}
		if err := r.controller.Reconcile(ctx); err != nil {
			r.options.Logger.ErrorContext(ctx, "reconcile active task intents", "error", err)
		}
		for i := range jobs.Items {
			r.observeJob(ctx, &jobs.Items[i])
			r.scheduleSecretCleanup(ctx, &jobs.Items[i], false)
		}
		r.recoverSecretCleanups(ctx)
		stream, err := r.kubernetes.BatchV1().Jobs(r.options.Namespace).Watch(ctx, metav1.ListOptions{
			LabelSelector: managedSelector, ResourceVersion: jobs.ResourceVersion,
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !waitRetry(ctx) {
				return nil
			}
			continue
		}
		r.consumeJobWatch(ctx, stream)
	}
	return nil
}

func (r *Runtime) consumeJobWatch(ctx context.Context, stream watch.Interface) {
	defer stream.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case event, open := <-stream.ResultChan():
			if !open || event.Type == watch.Error {
				return
			}
			if event.Type != watch.Added && event.Type != watch.Modified && event.Type != watch.Deleted {
				continue
			}
			job, ok := event.Object.(*batchv1.Job)
			if !ok || job.Labels["app.kubernetes.io/managed-by"] != "simpleswe" {
				continue
			}
			if err := r.controller.Reconcile(ctx); err != nil {
				r.options.Logger.ErrorContext(ctx, "reconcile watched Job", "job", job.Name, "error", err)
			}
			r.observeJob(ctx, job)
			r.scheduleSecretCleanup(ctx, job, event.Type == watch.Deleted)
		}
	}
}

func (r *Runtime) watchPods(ctx context.Context) error {
	for ctx.Err() == nil {
		pods, err := r.kubernetes.CoreV1().Pods(r.options.Namespace).List(ctx, metav1.ListOptions{LabelSelector: managedSelector})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !waitRetry(ctx) {
				return nil
			}
			continue
		}
		for i := range pods.Items {
			r.observePod(ctx, &pods.Items[i])
			r.startPodLogs(ctx, &pods.Items[i])
		}
		stream, err := r.kubernetes.CoreV1().Pods(r.options.Namespace).Watch(ctx, metav1.ListOptions{
			LabelSelector: managedSelector, ResourceVersion: pods.ResourceVersion,
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !waitRetry(ctx) {
				return nil
			}
			continue
		}
		r.consumePodWatch(ctx, stream)
	}
	return nil
}

func (r *Runtime) consumePodWatch(ctx context.Context, stream watch.Interface) {
	defer stream.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case event, open := <-stream.ResultChan():
			if !open || event.Type == watch.Error {
				return
			}
			if event.Type != watch.Added && event.Type != watch.Modified {
				continue
			}
			pod, ok := event.Object.(*corev1.Pod)
			if !ok || pod.Labels["app.kubernetes.io/managed-by"] != "simpleswe" {
				continue
			}
			r.observePod(ctx, pod)
			r.startPodLogs(ctx, pod)
		}
	}
}

func (r *Runtime) startPodLogs(ctx context.Context, pod *corev1.Pod) {
	key := string(pod.UID)
	if key == "" {
		r.options.Logger.ErrorContext(ctx, "ignore Pod without UID", "pod", pod.Name)
		return
	}
	r.mu.Lock()
	if r.pods[key] {
		r.mu.Unlock()
		return
	}
	r.pods[key] = true
	r.tasks.Add(1)
	r.mu.Unlock()
	podCopy := pod.DeepCopy()
	go func() {
		defer r.tasks.Done()
		err := r.CollectPodLogs(ctx, podCopy)
		if err != nil || ctx.Err() != nil {
			r.mu.Lock()
			delete(r.pods, key)
			r.mu.Unlock()
		}
		if err != nil && ctx.Err() == nil {
			r.options.Logger.ErrorContext(ctx, "collect Pod logs", "pod", podCopy.Name, "error", err)
		}
	}()
}

// CollectPodLogs streams and persists raw output while interpreting only exact
// worker protocol lines.
func (r *Runtime) CollectPodLogs(ctx context.Context, pod *corev1.Pod) error {
	if pod == nil {
		return errors.New("Pod is nil")
	}
	if pod.UID == "" {
		return errors.New("Pod UID is empty")
	}
	taskID := pod.Labels["simpleswe.dev/task-id"]
	attemptNumber, err := strconv.Atoi(pod.Labels["simpleswe.dev/attempt"])
	if taskID == "" || err != nil || attemptNumber <= 0 {
		return errors.New("Pod is missing task attempt labels")
	}
	attempt, err := r.store.GetAttemptNumber(ctx, taskID, attemptNumber)
	if err != nil {
		return err
	}
	if pod.Labels["simpleswe.dev/attempt-id"] != attempt.ID {
		return fmt.Errorf("Pod attempt ID label %q does not match %q", pod.Labels["simpleswe.dev/attempt-id"], attempt.ID)
	}
	owner := controllerJobOwner(pod)
	if owner == nil {
		return errors.New("Pod has no controlling Job")
	}
	jobName := owner.Name
	job, err := r.kubernetes.BatchV1().Jobs(pod.Namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get controlling Job %q: %w", jobName, err)
	}
	if job.UID == "" || owner.UID != job.UID {
		return fmt.Errorf("Pod controlling Job UID %q does not match Job %q UID %q", owner.UID, jobName, job.UID)
	}
	for key, value := range map[string]string{
		"app.kubernetes.io/managed-by": "simpleswe", "simpleswe.dev/task-id": taskID,
		"simpleswe.dev/attempt": strconv.Itoa(attempt.Number), "simpleswe.dev/attempt-id": attempt.ID,
	} {
		if job.Labels[key] != value || pod.Labels[key] != value {
			return fmt.Errorf("Pod and Job ownership label %s does not match %q", key, value)
		}
	}
	container := "worker"
	if len(pod.Spec.Containers) > 0 {
		container = pod.Spec.Containers[0].Name
		for _, candidate := range pod.Spec.Containers {
			if candidate.Name == "worker" {
				container = candidate.Name
				break
			}
		}
	}
	podUID := string(pod.UID)
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		cursor, err := r.store.GetPodLogCursor(ctx, podUID)
		if err != nil {
			return err
		}
		if err := r.drainWorkerEvents(ctx, podUID); err != nil {
			return err
		}
		if cursor.Exhausted {
			return nil
		}
		var since *time.Time
		if !cursor.Timestamp.IsZero() {
			value := cursor.Timestamp
			since = &value
		}
		stream, openErr := r.options.PodLogs.Open(ctx, pod.Namespace, pod.Name, container, since)
		cleanEOF := false
		if openErr == nil {
			readErr := r.readPodLogStream(ctx, stream, pod, taskID, attempt.ID, jobName, cursor)
			_ = stream.Close()
			cleanEOF = errors.Is(readErr, io.EOF)
			if !cleanEOF && ctx.Err() == nil {
				r.options.Logger.WarnContext(ctx, "interrupted Pod log stream; retrying", "pod", pod.Name, "error", readErr)
			}
		} else if ctx.Err() == nil {
			r.options.Logger.WarnContext(ctx, "open Pod log stream; retrying", "pod", pod.Name, "error", openErr)
		}
		if cleanEOF {
			terminal, err := r.logsTerminal(ctx, pod.Namespace, pod.Name, container, jobName, taskID, attempt.ID)
			if err != nil {
				r.options.Logger.WarnContext(ctx, "check Pod log terminal state; retrying", "pod", pod.Name, "error", err)
			} else if terminal {
				if err := r.drainWorkerEvents(ctx, podUID); err != nil {
					return err
				}
				if err := r.logEvents.WorkerLogsExhausted(ctx, jobName, pod.Name); err != nil {
					return fmt.Errorf("record exhausted worker logs: %w", err)
				}
				return r.store.MarkPodLogsExhausted(ctx, podUID)
			}
		}
		if !sleepContext(ctx, backoff) {
			return nil
		}
		backoff = min(backoff*2, 5*time.Second)
	}
	return nil
}

func (r *Runtime) parseProtocolLine(ctx context.Context, jobName, podName string, raw []byte) (protocol.Event, bool) {
	line := strings.TrimSuffix(string(raw), "\n")
	line = strings.TrimSuffix(line, "\r")
	parsed, err := protocol.ParseLine(line)
	if err != nil {
		r.options.Logger.ErrorContext(ctx, "malformed worker event", "job", jobName, "pod", podName, "error", err)
		return protocol.Event{}, false
	}
	if parsed.Event == nil {
		return protocol.Event{}, false
	}
	if err := validateDurableWorkerEvent(*parsed.Event); err != nil {
		r.options.Logger.ErrorContext(ctx, "malformed worker event", "job", jobName, "pod", podName, "error", err)
		return protocol.Event{}, false
	}
	return *parsed.Event, true
}

func validateDurableWorkerEvent(event protocol.Event) error {
	if strings.TrimSpace(event.TaskID) == "" {
		return errors.New("worker event task ID is empty")
	}
	switch event.Type {
	case protocol.EventAgentStarted, protocol.EventValidationSucceeded, "worker_failed":
		return nil
	case protocol.EventValidationStarted, protocol.EventValidationResult, protocol.EventValidationFailed:
		if len(event.Command) == 0 {
			return fmt.Errorf("worker event %q command is empty", event.Type)
		}
		return nil
	case protocol.EventBranchPushed:
		return protocol.ValidateEvent(event, event.Branch)
	default:
		return fmt.Errorf("unsupported worker event type %q", event.Type)
	}
}

func (r *Runtime) readPodLogStream(ctx context.Context, stream io.ReadCloser, pod *corev1.Pod, taskID, attemptID, jobName string, cursor store.PodLogCursor) error {
	stopClose := context.AfterFunc(ctx, func() { _ = stream.Close() })
	defer stopClose()
	reader := bufio.NewReaderSize(stream, max(r.options.LogChunkBytes, len(protocol.EventPrefix)))
	ordinals := make(map[string]int)
	untimestamped := cursor.UntimestampedLines
	for {
		line, oversized, err := readBoundedLine(reader, r.options.MaxLogBytes+128)
		if len(line) > 0 {
			raw, timestamp, timestampText := rawKubernetesLogLine(line)
			if oversized {
				raw = append(raw, 0) // Force the durable quota path without retaining the discarded suffix.
			}
			ordinal := 0
			if !timestamp.IsZero() {
				ordinals[timestampText]++
				ordinal = ordinals[timestampText]
			} else {
				untimestamped++
			}
			params := store.AppendPodLogParams{
				TaskID: taskID, AttemptID: attemptID, PodUID: string(pod.UID), JobName: jobName, PodName: pod.Name,
				Timestamp: timestamp, TimestampOrdinal: ordinal, UntimestampedOrdinal: untimestamped, Content: raw,
			}
			if bytes.HasPrefix(raw, []byte(protocol.EventPrefix)) && len(raw) <= 1<<20 {
				if _, ok := r.parseProtocolLine(ctx, jobName, pod.Name, raw); ok {
					identity := timestampText + "/" + strconv.Itoa(ordinal)
					if timestamp.IsZero() {
						identity = "line/" + strconv.Itoa(untimestamped)
					}
					params.WorkerEventID = string(pod.UID) + "/" + identity
					params.WorkerEvent = strings.TrimRight(string(raw), "\r\n")
				}
			} else if bytes.HasPrefix(raw, []byte(protocol.EventPrefix)) {
				r.options.Logger.ErrorContext(ctx, "malformed worker event", "pod", pod.Name, "error", "worker event exceeds 1 MiB")
			}
			if _, appendErr := r.store.AppendPodLog(ctx, params, r.options.MaxLogBytes, r.options.LogChunkBytes); appendErr != nil {
				return appendErr
			}
			if drainErr := r.drainWorkerEvents(ctx, string(pod.UID)); drainErr != nil {
				return drainErr
			}
		}
		if err != nil {
			return err
		}
	}
}

func readBoundedLine(reader *bufio.Reader, limit int) ([]byte, bool, error) {
	line := make([]byte, 0, min(limit, reader.Size()))
	oversized := false
	for {
		fragment, err := reader.ReadSlice('\n')
		remaining := limit - len(line)
		if len(fragment) > remaining {
			oversized = true
		}
		if remaining > 0 {
			line = append(line, fragment[:min(remaining, len(fragment))]...)
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			if len(line) >= limit {
				oversized = true
			}
			continue
		}
		return line, oversized, err
	}
}

func rawKubernetesLogLine(line []byte) ([]byte, time.Time, string) {
	space := bytes.IndexByte(line, ' ')
	if space <= 0 {
		return line, time.Time{}, ""
	}
	timestampText := string(line[:space])
	timestamp, err := time.Parse(time.RFC3339Nano, timestampText)
	if err != nil {
		return line, time.Time{}, ""
	}
	return line[space+1:], timestamp, timestampText
}

func (r *Runtime) drainWorkerEvents(ctx context.Context, podUID string) error {
	events, err := r.store.ListPendingWorkerEvents(ctx, podUID)
	if err != nil {
		return err
	}
	for _, pending := range events {
		parsed, err := protocol.ParseLine(pending.Content)
		if err != nil || parsed.Event == nil {
			return fmt.Errorf("parse durable worker event %q: %w", pending.ID, err)
		}
		if once, ok := r.controller.(interface {
			HandleWorkerEventOnce(context.Context, string, string, string, protocol.Event) error
		}); ok {
			err = once.HandleWorkerEventOnce(ctx, pending.ID, pending.JobName, pending.PodName, *parsed.Event)
		} else {
			err = r.controller.HandleWorkerEvent(ctx, pending.JobName, pending.PodName, *parsed.Event)
		}
		if err != nil {
			return fmt.Errorf("handle durable worker event %q: %w", pending.ID, err)
		}
		if err := r.store.MarkWorkerEventProcessed(ctx, pending.ID); err != nil {
			return fmt.Errorf("mark worker event %q processed: %w", pending.ID, err)
		}
	}
	return nil
}

func (r *Runtime) logsTerminal(ctx context.Context, namespace, podName, container, jobName, taskID, attemptID string) (bool, error) {
	pod, podErr := r.kubernetes.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if podErr == nil {
		r.observePod(ctx, pod)
		if podTerminal(pod, container) {
			return true, nil
		}
	} else if !apierrors.IsNotFound(podErr) {
		return false, podErr
	}
	job, jobErr := r.kubernetes.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
	if jobErr == nil {
		r.observeJob(ctx, job)
		if jobCompleted(job) || jobFailed(job) {
			return true, nil
		}
		return false, nil
	} else if !apierrors.IsNotFound(jobErr) {
		return false, jobErr
	}
	attempt, err := r.store.GetAttempt(ctx, taskID, attemptID)
	if err != nil {
		return false, err
	}
	return terminalAttempt(attempt.State), nil
}

func podTerminal(pod *corev1.Pod, container string) bool {
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return true
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == container && status.State.Terminated != nil {
			return true
		}
	}
	return false
}

func (r *Runtime) attemptForLabels(ctx context.Context, labels map[string]string) (store.Attempt, error) {
	taskID, attemptID := labels["simpleswe.dev/task-id"], labels["simpleswe.dev/attempt-id"]
	number, err := strconv.Atoi(labels["simpleswe.dev/attempt"])
	if taskID == "" || attemptID == "" || err != nil || number <= 0 {
		return store.Attempt{}, errors.New("resource is missing task attempt labels")
	}
	attempt, err := r.store.GetAttemptNumber(ctx, taskID, number)
	if err != nil {
		return store.Attempt{}, err
	}
	if attempt.ID != attemptID {
		return store.Attempt{}, fmt.Errorf("attempt label %q does not match %q", attemptID, attempt.ID)
	}
	return attempt, nil
}

func (r *Runtime) observeJob(ctx context.Context, job *batchv1.Job) {
	if job == nil || job.UID == "" {
		return
	}
	attempt, err := r.attemptForLabels(ctx, job.Labels)
	if err != nil {
		return
	}
	state, reason, message := "pending", "", ""
	var completed *time.Time
	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		reason, message = condition.Reason, condition.Message
		if condition.Type == batchv1.JobComplete {
			state = "succeeded"
			value := condition.LastTransitionTime.Time
			if !value.IsZero() {
				completed = &value
			}
		}
		if condition.Type == batchv1.JobFailed {
			state = "failed"
			value := condition.LastTransitionTime.Time
			if !value.IsZero() {
				completed = &value
			}
		}
	}
	if job.Status.Active > 0 {
		state = "running"
	}
	var started *time.Time
	if job.Status.StartTime != nil {
		value := job.Status.StartTime.Time
		started = &value
	}
	if job.Status.CompletionTime != nil {
		value := job.Status.CompletionTime.Time
		completed = &value
	}
	err = r.store.ObserveKubernetesJob(ctx, store.KubernetesJobObservation{
		TaskID: attempt.TaskID, AttemptID: attempt.ID, AttemptNumber: attempt.Number, Namespace: job.Namespace, Name: job.Name, UID: string(job.UID),
		State: state, Reason: reason, Message: message, StartedAt: started, CompletedAt: completed, SecretName: taskSecretName(job),
	})
	if err != nil {
		r.options.Logger.ErrorContext(ctx, "persist Job observation", "job", job.Name, "error", err)
		return
	}
	if !jobCompleted(job) && !jobFailed(job) {
		_ = r.store.MarkSecretCleanupIneligible(ctx, attempt.ID)
		r.cancelSecretCleanup(attempt.ID, "")
	} else {
		r.cancelSecretCleanup(attempt.ID, string(job.UID))
	}
}

func (r *Runtime) observePod(ctx context.Context, pod *corev1.Pod) {
	if pod == nil || pod.UID == "" {
		return
	}
	attempt, err := r.attemptForLabels(ctx, pod.Labels)
	if err != nil {
		return
	}
	state := strings.ToLower(string(pod.Status.Phase))
	if state == "" {
		state = "unknown"
	}
	if state != "pending" && state != "running" && state != "succeeded" && state != "failed" {
		state = "unknown"
	}
	image := ""
	for _, container := range pod.Spec.Containers {
		if container.Name == "worker" || image == "" {
			image = container.Image
		}
		if container.Name == "worker" {
			break
		}
	}
	states := make(map[string]string)
	var completed *time.Time
	for _, status := range pod.Status.ContainerStatuses {
		value := "waiting"
		if status.State.Running != nil {
			value = "running"
		}
		if status.State.Terminated != nil {
			value = "terminated"
			t := status.State.Terminated.FinishedAt.Time
			if !t.IsZero() && (completed == nil || t.After(*completed)) {
				completed = &t
			}
		}
		states[status.Name] = value
	}
	encoded, err := json.Marshal(states)
	if err != nil {
		r.options.Logger.ErrorContext(ctx, "encode Pod container states", "pod", pod.Name, "error", err)
		return
	}
	var started *time.Time
	if pod.Status.StartTime != nil {
		value := pod.Status.StartTime.Time
		started = &value
	}
	err = r.store.ObserveKubernetesPod(ctx, store.KubernetesPodObservation{
		TaskID: attempt.TaskID, AttemptID: attempt.ID, Namespace: pod.Namespace, Name: pod.Name, UID: string(pod.UID), State: state,
		Reason: pod.Status.Reason, Message: pod.Status.Message, Node: pod.Spec.NodeName, Image: image, ContainerStates: string(encoded), StartedAt: started, CompletedAt: completed,
	})
	if err != nil {
		r.options.Logger.ErrorContext(ctx, "persist Pod observation", "pod", pod.Name, "error", err)
	}
}

func (r *Runtime) recoverSecretCleanups(ctx context.Context) {
	cleanups, err := r.store.ListSecretCleanups(ctx)
	if err != nil {
		r.options.Logger.ErrorContext(ctx, "list task Secret cleanups", "error", err)
		return
	}
	for _, cleanup := range cleanups {
		attempt, getErr := r.store.GetAttempt(ctx, cleanup.TaskID, cleanup.AttemptID)
		if getErr != nil || !terminalAttempt(attempt.State) {
			_ = r.store.MarkSecretCleanupIneligible(ctx, cleanup.AttemptID)
			r.cancelSecretCleanup(cleanup.AttemptID, "")
			continue
		}
		if r.cleanupBlockedByCurrentJob(ctx, cleanup, attempt) {
			continue
		}
		if cleanup.EligibleAt == nil {
			value := r.options.Clock.Now()
			if err := r.store.MarkSecretCleanupEligible(ctx, cleanup, value); err != nil {
				continue
			}
			cleanup.EligibleAt = &value
		}
		r.startSecretCleanup(ctx, cleanup)
	}
}

func validTaskSecret(secret *corev1.Secret, cleanup store.SecretCleanup) bool {
	if secret == nil || secret.Name != cleanup.SecretName || secret.Namespace != cleanup.Namespace {
		return false
	}
	want := map[string]string{"app.kubernetes.io/managed-by": "simpleswe", "simpleswe.dev/task-id": cleanup.TaskID, "simpleswe.dev/attempt": strconv.Itoa(cleanup.AttemptNumber), "simpleswe.dev/attempt-id": cleanup.AttemptID}
	for key, value := range want {
		if secret.Labels[key] != value {
			return false
		}
	}
	return true
}

func terminalAttempt(state task.State) bool {
	return state == task.READY || state == task.FAILED || state == task.CANCELLED
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (r *Runtime) scheduleSecretCleanup(ctx context.Context, job *batchv1.Job, _ bool) {
	attempt, err := r.attemptForLabels(ctx, job.Labels)
	if err != nil {
		return
	}
	attempt, err = r.store.GetAttempt(ctx, attempt.TaskID, attempt.ID)
	if err != nil || !terminalAttempt(attempt.State) {
		_ = r.store.MarkSecretCleanupIneligible(ctx, attempt.ID)
		r.cancelSecretCleanup(attempt.ID, "")
		return
	}
	cleanup := store.SecretCleanup{TaskID: attempt.TaskID, AttemptID: attempt.ID, AttemptNumber: attempt.Number, Namespace: job.Namespace, JobName: job.Name, JobUID: string(job.UID), SecretName: taskSecretName(job)}
	if r.cleanupBlockedByCurrentJob(ctx, cleanup, attempt) {
		return
	}
	cleanup, err = r.store.GetSecretCleanup(ctx, attempt.ID)
	if err != nil || cleanup.JobUID != string(job.UID) || cleanup.SecretName != taskSecretName(job) {
		return
	}
	eligible := r.options.Clock.Now()
	if job.Status.CompletionTime != nil {
		eligible = job.Status.CompletionTime.Time
	}
	if err := r.store.MarkSecretCleanupEligible(ctx, cleanup, eligible); err != nil {
		r.options.Logger.ErrorContext(ctx, "mark task Secret cleanup", "attempt", attempt.ID, "error", err)
		return
	}
	cleanup.EligibleAt = &eligible
	r.startSecretCleanup(ctx, cleanup)
}

func (r *Runtime) cleanupBlockedByCurrentJob(ctx context.Context, cleanup store.SecretCleanup, attempt store.Attempt) bool {
	job, err := r.kubernetes.BatchV1().Jobs(cleanup.Namespace).Get(ctx, cleanup.JobName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		return true
	}
	current, err := r.attemptForLabels(ctx, job.Labels)
	if err != nil || current.ID != attempt.ID || current.TaskID != attempt.TaskID {
		return true
	}
	if string(job.UID) != cleanup.JobUID {
		r.observeJob(ctx, job)
		return true
	}
	if !jobCompleted(job) && !jobFailed(job) {
		_ = r.store.MarkSecretCleanupIneligible(ctx, attempt.ID)
		r.cancelSecretCleanup(attempt.ID, "")
		return true
	}
	return false
}

func (r *Runtime) cancelSecretCleanup(attemptID, keepJobUID string) {
	r.mu.Lock()
	run := r.cleanups[attemptID]
	if run == nil || keepJobUID != "" && run.jobUID == keepJobUID {
		r.mu.Unlock()
		return
	}
	delete(r.cleanups, attemptID)
	r.mu.Unlock()
	run.cancel()
}

func (r *Runtime) startSecretCleanup(ctx context.Context, cleanup store.SecretCleanup) {
	key := cleanup.AttemptID
	r.mu.Lock()
	if r.cleanups[key] != nil {
		r.mu.Unlock()
		return
	}
	cleanupCtx, cancel := context.WithCancel(ctx)
	run := &cleanupRun{jobUID: cleanup.JobUID, cancel: cancel}
	r.cleanups[key] = run
	r.tasks.Add(1)
	r.mu.Unlock()
	delay := r.options.SecretRetention
	if cleanup.EligibleAt != nil {
		delay -= r.options.Clock.Now().Sub(*cleanup.EligibleAt)
		if delay < 0 {
			delay = 0
		}
	}
	go func() {
		defer r.tasks.Done()
		defer cancel()
		defer func() {
			r.mu.Lock()
			if r.cleanups[key] == run {
				delete(r.cleanups, key)
			}
			r.mu.Unlock()
		}()
		select {
		case <-cleanupCtx.Done():
			return
		case <-r.options.Clock.After(delay):
		}
		backoff := 100 * time.Millisecond
		for cleanupCtx.Err() == nil {
			if !r.secretCleanupStillEligible(cleanupCtx, cleanup) {
				return
			}
			secret, err := r.kubernetes.CoreV1().Secrets(cleanup.Namespace).Get(cleanupCtx, cleanup.SecretName, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				_ = r.store.CompleteSecretCleanup(ctx, cleanup)
				return
			}
			if err == nil && !validTaskSecret(secret, cleanup) {
				r.options.Logger.ErrorContext(ctx, "refuse task Secret cleanup with mismatched ownership", "secret", cleanup.SecretName, "attempt", cleanup.AttemptID)
				_ = r.store.CompleteSecretCleanup(ctx, cleanup)
				return
			}
			if err == nil {
				cleanup, err = r.store.BindSecretCleanupUID(cleanupCtx, cleanup, string(secret.UID))
				if err != nil {
					return
				}
				if !r.secretCleanupStillEligible(cleanupCtx, cleanup) {
					return
				}
				options := metav1.DeleteOptions{}
				if secret.UID != "" {
					options.Preconditions = &metav1.Preconditions{UID: &secret.UID}
				}
				err = r.kubernetes.CoreV1().Secrets(cleanup.Namespace).Delete(cleanupCtx, cleanup.SecretName, options)
			}
			if err == nil || apierrors.IsNotFound(err) {
				_ = r.store.CompleteSecretCleanup(ctx, cleanup)
				return
			}
			r.options.Logger.WarnContext(ctx, "delete retained task Secret; retrying", "secret", cleanup.SecretName, "job", cleanup.JobName, "error", err)
			if !sleepContext(cleanupCtx, backoff) {
				return
			}
			backoff = min(backoff*2, 5*time.Second)
		}
	}()
}

func (r *Runtime) secretCleanupStillEligible(ctx context.Context, cleanup store.SecretCleanup) bool {
	attempt, err := r.store.GetAttempt(ctx, cleanup.TaskID, cleanup.AttemptID)
	if err != nil || !terminalAttempt(attempt.State) {
		_ = r.store.MarkSecretCleanupIneligible(ctx, cleanup.AttemptID)
		return false
	}
	current, err := r.store.GetSecretCleanup(ctx, cleanup.AttemptID)
	if err != nil || current.EligibleAt == nil || current.Generation != cleanup.Generation || current.JobUID != cleanup.JobUID || current.SecretName != cleanup.SecretName || cleanup.SecretUID != "" && current.SecretUID != cleanup.SecretUID {
		return false
	}
	return !r.cleanupBlockedByCurrentJob(ctx, current, attempt)
}

func taskSecretName(job *batchv1.Job) string {
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Name == "task-secret" && volume.Secret != nil {
			return volume.Secret.SecretName
		}
	}
	return job.Name + "-task"
}

func jobCompleted(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobFailed(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return job.Status.Failed > 0
}

func controllerJobOwner(pod *corev1.Pod) *metav1.OwnerReference {
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "Job" && owner.Controller != nil && *owner.Controller {
			ownerCopy := owner
			return &ownerCopy
		}
	}
	return nil
}

func waitRetry(ctx context.Context) bool {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

type realClock struct{}

func (realClock) Now() time.Time                             { return time.Now() }
func (realClock) After(delay time.Duration) <-chan time.Time { return time.After(delay) }

type kubernetesPodLogs struct{ client kubernetes.Interface }

func (k kubernetesPodLogs) Open(ctx context.Context, namespace, pod, container string, since *time.Time) (io.ReadCloser, error) {
	options := &corev1.PodLogOptions{Container: container, Follow: true, Timestamps: true}
	if since != nil {
		value := metav1.NewTime(*since)
		options.SinceTime = &value
	}
	return k.client.CoreV1().Pods(namespace).GetLogs(pod, options).Stream(ctx)
}
