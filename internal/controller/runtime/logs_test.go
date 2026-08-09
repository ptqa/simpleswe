package runtime

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestPodLogScannerPersistsBoundedRawChunksAndOnlyDispatchesProtocolEvents(t *testing.T) {
	db, taskRecord, attempt, storePath := backendStore(t)
	controller := newFakeController(db)
	backend := NewBackend(db, controller)
	valid, err := protocol.EncodeEvent(protocol.Event{Type: "agent_started", TaskID: taskRecord.ID})
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	malformed := "@@simpleswe:{not-json}"
	semanticPoison := fmt.Sprintf(`@@simpleswe:{"type":"unknown","task_id":%q}`, taskRecord.ID)
	plain := "agent_started task=" + taskRecord.ID
	var historical strings.Builder
	for i := range 250 {
		fmt.Fprintf(&historical, "historical-%03d-%s\n", i, strings.Repeat("x", 20))
	}
	raw := historical.String() + plain + "\n" + valid + "\n" + malformed + "\n" + semanticPoison + "\n"
	logs := &fakePodLogs{content: map[string][]string{"worker-a1": {raw}, "worker-a1-live": {"live line\n"}}}
	var logOutput bytes.Buffer
	jobUID := types.UID("job-uid-a1")
	job := runtimeJob("simpleswe-workers", "task-job-a1", taskRecord.ID, attempt.ID, jobUID)
	runtime, err := NewRuntime(fake.NewSimpleClientset(job), db, controller, backend, Options{
		Namespace:       "simpleswe-workers",
		LogChunkBytes:   32,
		SecretRetention: time.Hour,
		PodLogs:         logs,
		Logger:          slog.New(slog.NewJSONHandler(&logOutput, nil)),
	})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	pod := managedPod("simpleswe-workers", "worker-a1", "task-job-a1", taskRecord.ID, "1", attempt.ID, jobUID)
	if err := runtime.CollectPodLogs(context.Background(), pod); err != nil {
		t.Fatalf("CollectPodLogs() error = %v", err)
	}

	_, _, _, calls := controller.snapshot()
	if len(calls) != 1 {
		t.Fatalf("HandleWorkerEvent calls = %#v, want only the valid prefixed event", calls)
	}
	if calls[0].job != "task-job-a1" || calls[0].pod != pod.Name || calls[0].event.Type != "agent_started" || calls[0].event.TaskID != taskRecord.ID {
		t.Fatalf("HandleWorkerEvent call = %#v", calls[0])
	}
	if strings.Contains(logOutput.String(), plain) {
		t.Fatalf("ordinary output was interpreted as an event: %s", logOutput.String())
	}
	if !strings.Contains(logOutput.String(), `"msg":"malformed worker event"`) || !strings.Contains(logOutput.String(), "decode worker event") {
		t.Fatalf("malformed prefixed line did not produce an explicit parser error event: %s", logOutput.String())
	}
	if !strings.Contains(logOutput.String(), "unsupported worker event type") {
		t.Fatalf("semantic poison event was queued instead of rejected: %s", logOutput.String())
	}

	replayed, _, err := backend.GetLogs(context.Background(), taskRecord.ID, false, attempt.ID, 10000)
	if err != nil {
		t.Fatalf("GetLogs() error = %v", err)
	}
	if replayed != raw {
		t.Fatalf("replayed raw logs differ\ngot:  %q\nwant: %q", replayed, raw)
	}

	inspection, err := sql.Open("sqlite", storePath)
	if err != nil {
		t.Fatalf("open log inspection connection: %v", err)
	}
	defer inspection.Close()
	var chunks, largest int
	if err := inspection.QueryRowContext(context.Background(),
		"SELECT COUNT(*), COALESCE(MAX(length(content)), 0) FROM log_chunks WHERE attempt_id = ?", attempt.ID,
	).Scan(&chunks, &largest); err != nil {
		t.Fatalf("inspect persisted log chunks: %v", err)
	}
	if chunks < 2 || largest > 32 {
		t.Fatalf("persisted chunks = %d, largest = %d bytes; want multiple chunks of at most 32 bytes", chunks, largest)
	}
}

func TestLogsReplayTailAndFollowWithoutLoadingAllHistory(t *testing.T) {
	db, taskRecord, attempt, _ := backendStore(t)
	controller := newFakeController(db)
	backend := NewBackend(db, controller)
	logs := &fakePodLogs{content: map[string][]string{
		"history": {strings.Repeat("old line that must not be replayed\n", 500) + "last one\nlast two\n"},
		"live":    {"live one\nlive two\n"},
	}}
	jobUID := types.UID("job-uid-history")
	job := runtimeJob("simpleswe-workers", "job-a1", taskRecord.ID, attempt.ID, jobUID)
	runtime, err := NewRuntime(fake.NewSimpleClientset(job), db, controller, backend, Options{
		Namespace:       "simpleswe-workers",
		LogChunkBytes:   64,
		SecretRetention: time.Hour,
		PodLogs:         logs,
	})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	if err := runtime.CollectPodLogs(context.Background(), managedPod("simpleswe-workers", "history", "job-a1", taskRecord.ID, "1", attempt.ID, jobUID)); err != nil {
		t.Fatalf("collect history: %v", err)
	}

	initial, updates, err := backend.GetLogs(context.Background(), taskRecord.ID, true, attempt.ID, 2)
	if err != nil {
		t.Fatalf("GetLogs(follow) error = %v", err)
	}
	if initial != "last one\nlast two\n" {
		t.Fatalf("tail replay = %q, want only the final two lines", initial)
	}
	if strings.Contains(initial, "old line") {
		t.Fatal("tail replay loaded historical lines outside the requested bound")
	}

	if err := runtime.CollectPodLogs(context.Background(), managedPod("simpleswe-workers", "live", "job-a1", taskRecord.ID, "1", attempt.ID, jobUID)); err != nil {
		t.Fatalf("collect live logs: %v", err)
	}
	var followed strings.Builder
	deadline := time.After(2 * time.Second)
	for !strings.Contains(followed.String(), "live two\n") {
		select {
		case update := <-updates:
			followed.WriteString(update)
		case <-deadline:
			t.Fatalf("follow updates = %q, want live lines", followed.String())
		}
	}
	if followed.String() != "live one\nlive two\n" {
		t.Fatalf("follow updates = %q, want only newly persisted raw logs", followed.String())
	}
}

func TestPodLogScannerRejectsMismatchedJobOwnerUID(t *testing.T) {
	db, taskRecord, attempt, _ := backendStore(t)
	controller := newFakeController(db)
	backend := NewBackend(db, controller)
	jobUID := types.UID("real-job")
	job := runtimeJob("simpleswe-workers", "job-a1", taskRecord.ID, attempt.ID, jobUID)
	runtime, err := NewRuntime(fake.NewSimpleClientset(job), db, controller, backend, Options{
		Namespace: "simpleswe-workers", LogChunkBytes: 64, PodLogs: &fakePodLogs{content: map[string][]string{}},
	})
	if err != nil {
		t.Fatalf("NewRuntime(): %v", err)
	}
	pod := managedPod("simpleswe-workers", "spoof", job.Name, taskRecord.ID, "1", attempt.ID, types.UID("other-job"))
	if _, err := db.AppendPodLog(context.Background(), store.AppendPodLogParams{
		TaskID: taskRecord.ID, AttemptID: attempt.ID, PodUID: string(pod.UID), Content: []byte("saved\n"),
	}, 1<<20, 64); err != nil {
		t.Fatalf("seed Pod cursor: %v", err)
	}
	if err := db.MarkLogsExhausted(context.Background(), taskRecord.ID, attempt.ID); err != nil {
		t.Fatalf("mark attempt logs exhausted: %v", err)
	}
	if err := runtime.CollectPodLogs(context.Background(), pod); err == nil {
		t.Fatal("Pod owned by another Job UID was accepted")
	}
	cursor, err := db.GetPodLogCursor(context.Background(), string(pod.UID))
	if err != nil || cursor.Exhausted {
		t.Fatalf("unowned Pod closed cursor = %#v, %v", cursor, err)
	}
}

func TestPodLogRecoveryClosesOwnedCursorAfterAttemptExhaustion(t *testing.T) {
	db, taskRecord, attempt, _ := backendStore(t)
	controller := &failAfterAttemptExhaustionController{
		fakeController: newFakeController(db), taskID: taskRecord.ID, attemptID: attempt.ID,
	}
	jobUID := types.UID("recovery-job-uid")
	job := runtimeJob("simpleswe-workers", "recovery-job", taskRecord.ID, attempt.ID, jobUID)
	logs := &fakePodLogs{content: map[string][]string{"recovery-pod": {
		"2026-08-06T12:00:00Z final line\n",
		"2026-08-06T12:00:00Z final line\n",
	}}}
	r, err := NewRuntime(fake.NewSimpleClientset(job), db, controller, NewBackend(db, controller), Options{
		Namespace: job.Namespace, PodLogs: logs, MaxLogBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	pod := managedPod(job.Namespace, "recovery-pod", job.Name, taskRecord.ID, "1", attempt.ID, jobUID)
	if err := r.CollectPodLogs(context.Background(), pod); err == nil || !strings.Contains(err.Error(), "injected failure after attempt exhaustion") {
		t.Fatalf("first collection error = %v, want injected exhaustion failure", err)
	}
	cursor, err := db.GetPodLogCursor(context.Background(), string(pod.UID))
	if err != nil || cursor.Exhausted {
		t.Fatalf("cursor after injected failure = %#v, %v", cursor, err)
	}
	if err := r.CollectPodLogs(context.Background(), pod); err != nil {
		t.Fatalf("recovery collection: %v", err)
	}
	cursor, err = db.GetPodLogCursor(context.Background(), string(pod.UID))
	if err != nil || !cursor.Exhausted {
		t.Fatalf("recovered cursor = %#v, %v", cursor, err)
	}
	logs.mu.Lock()
	opens := logs.counts[pod.Name]
	logs.mu.Unlock()
	if opens != 1 || controller.exhaustionCalls != 1 {
		t.Fatalf("recovery replayed logs or exhaustion callback: opens/calls = %d/%d, want 1/1", opens, controller.exhaustionCalls)
	}
	got, err := db.ReadLogTail(context.Background(), taskRecord.ID, attempt.ID, 10)
	if err != nil || got != "final line\n" {
		t.Fatalf("recovered logs = %q, %v; want final line once", got, err)
	}
}

func TestPodLogRecoveryDrainsPendingEventsBeforeClosingCursor(t *testing.T) {
	db, taskRecord, attempt, _ := backendStore(t)
	controller := newFakeController(db)
	jobUID := types.UID("pending-recovery-job-uid")
	job := runtimeJob("simpleswe-workers", "pending-recovery-job", taskRecord.ID, attempt.ID, jobUID)
	pod := managedPod(job.Namespace, "pending-recovery-pod", job.Name, taskRecord.ID, "1", attempt.ID, jobUID)
	event, err := protocol.EncodeEvent(protocol.Event{Type: protocol.EventAgentStarted, TaskID: taskRecord.ID})
	if err != nil {
		t.Fatalf("encode pending event: %v", err)
	}
	if _, err := db.AppendPodLog(context.Background(), store.AppendPodLogParams{
		TaskID: taskRecord.ID, AttemptID: attempt.ID, PodUID: string(pod.UID), JobName: job.Name, PodName: pod.Name,
		Content: []byte(event + "\n"), WorkerEventID: string(pod.UID) + "/event", WorkerEvent: event,
	}, 1<<20, 64); err != nil {
		t.Fatalf("seed pending worker event: %v", err)
	}
	if err := db.MarkLogsExhausted(context.Background(), taskRecord.ID, attempt.ID); err != nil {
		t.Fatalf("mark attempt logs exhausted: %v", err)
	}
	logs := &fakePodLogs{content: map[string][]string{pod.Name: {"must not open\n"}}}
	r, err := NewRuntime(fake.NewSimpleClientset(job), db, controller, NewBackend(db, controller), Options{
		Namespace: job.Namespace, PodLogs: logs, MaxLogBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := r.CollectPodLogs(context.Background(), pod); err != nil {
		t.Fatalf("recover pending event: %v", err)
	}
	_, _, _, calls := controller.snapshot()
	if len(calls) != 1 || calls[0].event.Type != protocol.EventAgentStarted {
		t.Fatalf("recovered worker events = %#v, want pending agent_started", calls)
	}
	pending, err := db.ListPendingWorkerEvents(context.Background(), string(pod.UID))
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending worker events after recovery = %#v, %v", pending, err)
	}
	cursor, err := db.GetPodLogCursor(context.Background(), string(pod.UID))
	if err != nil || !cursor.Exhausted {
		t.Fatalf("cursor after pending event recovery = %#v, %v", cursor, err)
	}
	logs.mu.Lock()
	opens := logs.counts[pod.Name]
	logs.mu.Unlock()
	if opens != 0 {
		t.Fatalf("recovery opened Kubernetes logs %d times, want 0", opens)
	}
}

func TestPodLogCollectionResumesFromDurableTimestampWithoutDuplicateEvent(t *testing.T) {
	db, taskRecord, attempt, path := backendStore(t)
	controller := newFakeController(db)
	backend := NewBackend(db, controller)
	jobUID := types.UID("job-uid-resume")
	job := runtimeJob("simpleswe-workers", "job-a1", taskRecord.ID, attempt.ID, jobUID)
	job.Status.Conditions = nil
	t1 := "2026-08-06T12:00:00.000000001Z"
	t2 := "2026-08-06T12:00:00.000000002Z"
	event, _ := protocol.EncodeEvent(protocol.Event{Type: "agent_started", TaskID: taskRecord.ID})
	logs := &fakePodLogs{content: map[string][]string{"worker": {
		t1 + " first\n" + t2 + " " + event + "\n",
		t1 + " first\n" + t2 + " " + event + "\n" + "2026-08-06T12:00:00.000000003Z last\n",
	}}}
	client := fake.NewSimpleClientset(job)
	runtime1, err := NewRuntime(client, db, controller, backend, Options{Namespace: "simpleswe-workers", PodLogs: logs, MaxLogBytes: 1 << 20})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	pod := managedPod("simpleswe-workers", "worker", job.Name, taskRecord.ID, "1", attempt.ID, jobUID)
	pod.UID = "pod-uid-resume"
	pod.Status.Phase = "Succeeded"
	first, err := logs.Open(context.Background(), pod.Namespace, pod.Name, "worker", nil)
	if err != nil {
		t.Fatalf("open first stream: %v", err)
	}
	if err := runtime1.readPodLogStream(context.Background(), first, pod, taskRecord.ID, attempt.ID, job.Name, store.PodLogCursor{}); !errors.Is(err, io.EOF) {
		t.Fatalf("first interrupted stream error = %v, want EOF", err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: "True"}}
	if _, err := client.BatchV1().Jobs(job.Namespace).Update(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("complete Job: %v", err)
	}
	runtime2, err := NewRuntime(client, db, controller, backend, Options{Namespace: "simpleswe-workers", PodLogs: logs, MaxLogBytes: 1 << 20})
	if err != nil {
		t.Fatalf("restart NewRuntime: %v", err)
	}
	if err := runtime2.CollectPodLogs(context.Background(), pod); err != nil {
		t.Fatalf("restart collection: %v", err)
	}
	_, _, _, calls := controller.snapshot()
	if len(calls) != 1 {
		t.Fatalf("worker event calls after replay = %d, want 1", len(calls))
	}
	inspection, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("inspect logs: %v", err)
	}
	defer inspection.Close()
	var content string
	if err := inspection.QueryRow(`SELECT COALESCE(group_concat(CAST(content AS TEXT), ''), '') FROM (SELECT content FROM log_chunks ORDER BY sequence)`).Scan(&content); err != nil {
		t.Fatalf("read logs: %v", err)
	}
	want := "first\n" + event + "\nlast\n"
	if content != want {
		t.Fatalf("durable raw logs = %q, want %q", content, want)
	}
}

func TestPodLogCollectionRetriesEOFWithoutWatchUpdateUntilJobTerminal(t *testing.T) {
	db, taskRecord, attempt, _ := backendStore(t)
	controller := newFakeController(db)
	jobUID := types.UID("retry-job-uid")
	job := runtimeJob("simpleswe-workers", "retry-job", taskRecord.ID, attempt.ID, jobUID)
	job.Status.Conditions = nil
	client := fake.NewSimpleClientset(job)
	logs := &fakePodLogs{content: map[string][]string{"retry-pod": {
		"2026-08-06T12:00:00.000000001Z first\n",
		"2026-08-06T12:00:00.000000002Z second\n",
	}}}
	logs.onOpen = func(_ string, count int) {
		if count != 2 {
			return
		}
		latest, _ := client.BatchV1().Jobs(job.Namespace).Get(context.Background(), job.Name, metav1.GetOptions{})
		latest.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		_, _ = client.BatchV1().Jobs(job.Namespace).Update(context.Background(), latest, metav1.UpdateOptions{})
	}
	r, err := NewRuntime(client, db, controller, NewBackend(db, controller), Options{Namespace: job.Namespace, PodLogs: logs, MaxLogBytes: 1 << 20})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	pod := managedPod(job.Namespace, "retry-pod", job.Name, taskRecord.ID, "1", attempt.ID, jobUID)
	pod.UID = "retry-pod-uid"
	if err := r.CollectPodLogs(context.Background(), pod); err != nil {
		t.Fatalf("CollectPodLogs: %v", err)
	}
	logs.mu.Lock()
	opens := logs.counts[pod.Name]
	logs.mu.Unlock()
	if opens != 2 {
		t.Fatalf("log stream opens = %d, want independent retry after EOF", opens)
	}
	got, err := db.ReadLogTail(context.Background(), taskRecord.ID, attempt.ID, 10)
	if err != nil || got != "first\nsecond\n" {
		t.Fatalf("retried logs = %q, %v", got, err)
	}
}

func TestTerminalPodLogCollectionRetriesFailedOpenOrReadBeforeFinalValidationEvent(t *testing.T) {
	for _, test := range []struct {
		name  string
		first podLogResult
	}{
		{name: "open", first: podLogResult{openErr: errors.New("temporary open failure")}},
		{name: "read", first: podLogResult{readErr: errors.New("temporary read failure")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, taskRecord, attempt, _ := backendStore(t)
			controller := newFakeController(db)
			jobUID := types.UID("terminal-retry-" + test.name)
			job := runtimeJob("simpleswe-workers", "terminal-job-"+test.name, taskRecord.ID, attempt.ID, jobUID)
			event, err := protocol.EncodeEvent(protocol.Event{
				Type: protocol.EventValidationStarted, TaskID: taskRecord.ID, Command: []string{"go", "test", "./..."},
			})
			if err != nil {
				t.Fatalf("encode final validation event: %v", err)
			}
			podName := "terminal-pod-" + test.name
			logs := &fakePodLogs{results: map[string][]podLogResult{podName: {
				test.first,
				{content: "2026-08-06T12:00:00Z " + event + "\n"},
			}}}
			r, err := NewRuntime(fake.NewSimpleClientset(job), db, controller, NewBackend(db, controller), Options{
				Namespace: job.Namespace, PodLogs: logs, MaxLogBytes: 1 << 20,
			})
			if err != nil {
				t.Fatalf("NewRuntime: %v", err)
			}
			pod := managedPod(job.Namespace, podName, job.Name, taskRecord.ID, "1", attempt.ID, jobUID)
			pod.UID = types.UID(podName + "-uid")
			pod.Status.Phase = corev1.PodSucceeded
			if err := r.CollectPodLogs(context.Background(), pod); err != nil {
				t.Fatalf("CollectPodLogs: %v", err)
			}
			logs.mu.Lock()
			opens := logs.counts[podName]
			logs.mu.Unlock()
			if opens != 2 {
				t.Fatalf("log stream opens = %d, want retry then clean EOF", opens)
			}
			_, _, _, calls := controller.snapshot()
			if len(calls) != 1 || calls[0].event.Type != protocol.EventValidationStarted {
				t.Fatalf("final validation events = %#v, want one", calls)
			}
		})
	}
}

func runtimeJob(namespace, name, taskID, attemptID string, uid types.UID) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: uid, Labels: map[string]string{
		"app.kubernetes.io/managed-by": "simpleswe", "simpleswe.dev/task-id": taskID,
		"simpleswe.dev/attempt": "1", "simpleswe.dev/attempt-id": attemptID,
	}}, Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: "True"}}}}
}

type failAfterAttemptExhaustionController struct {
	*fakeController
	taskID, attemptID string
	exhaustionCalls   int
}

func (c *failAfterAttemptExhaustionController) WorkerLogsExhausted(ctx context.Context, _, _ string) error {
	c.exhaustionCalls++
	if err := c.store.MarkLogsExhausted(ctx, c.taskID, c.attemptID); err != nil {
		return fmt.Errorf("mark attempt logs exhausted: %w", err)
	}
	return errors.New("injected failure after attempt exhaustion")
}
