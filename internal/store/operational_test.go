package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPodLogCRUDCheckpointAndChunkReads(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "logs"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	attempt := created.CurrentAttemptID
	if _, err := db.AppendPodLog(ctx, AppendPodLogParams{}, 100, 10); err == nil {
		t.Fatal("Pod log append accepted missing identity")
	}
	if _, err := db.AppendPodLog(ctx, AppendPodLogParams{TaskID: created.ID, AttemptID: "missing", PodUID: "pod"}, 100, 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Pod log for missing attempt = %v, want ErrNotFound", err)
	}

	params := AppendPodLogParams{
		TaskID: created.ID, AttemptID: attempt, PodUID: "pod-1", JobName: "job-1", PodName: "pod-1",
		UntimestampedOrdinal: 2, Content: []byte("first\nsecond\n"), WorkerEventID: "worker-event-1", WorkerEvent: `{"type":"started"}`,
	}
	result, err := db.AppendPodLog(ctx, params, 256, 5)
	if err != nil {
		t.Fatalf("append Pod log: %v", err)
	}
	if result != (AppendPodLogResult{AppendedBytes: len(params.Content)}) {
		t.Fatalf("append Pod log = %#v", result)
	}
	duplicate, err := db.AppendPodLog(ctx, params, 256, 5)
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("duplicate untimestamped Pod log = %#v, %v", duplicate, err)
	}
	other, err := db.CreateTask(ctx, CreateTaskParams{Repository: "other", Prompt: "logs"})
	if err != nil {
		t.Fatalf("create other task: %v", err)
	}
	if _, err := db.AppendPodLog(ctx, AppendPodLogParams{TaskID: other.ID, AttemptID: other.CurrentAttemptID, PodUID: "pod-1", Content: []byte("wrong")}, 256, 5); !errors.Is(err, ErrConflict) {
		t.Fatalf("reuse Pod UID across attempts = %v, want ErrConflict", err)
	}

	cursor, err := db.GetPodLogCursor(ctx, "pod-1")
	if err != nil || !cursor.Timestamp.IsZero() || cursor.UntimestampedLines != 2 || cursor.Exhausted || cursor.Truncated {
		t.Fatalf("Pod cursor = %#v, %v", cursor, err)
	}
	missingCursor, err := db.GetPodLogCursor(ctx, "missing")
	if err != nil || missingCursor != (PodLogCursor{}) {
		t.Fatalf("missing Pod cursor = %#v, %v", missingCursor, err)
	}
	events, err := db.ListPendingWorkerEvents(ctx, "pod-1")
	if err != nil || len(events) != 1 || events[0].ID != "worker-event-1" || events[0].PodUID != "pod-1" || events[0].TaskID != created.ID || events[0].AttemptID != attempt || events[0].JobName != "job-1" || events[0].PodName != "pod-1" || events[0].Content != params.WorkerEvent {
		t.Fatalf("pending worker events = %#v, %v", events, err)
	}
	gotEvent, err := db.GetWorkerLogEvent(ctx, events[0].ID)
	if err != nil || gotEvent != events[0] {
		t.Fatalf("worker event lookup = %#v, %v", gotEvent, err)
	}
	if _, err := db.GetWorkerLogEvent(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing worker event = %v, want ErrNotFound", err)
	}
	podUIDs, err := db.ListPendingWorkerEventPodUIDs(ctx)
	if err != nil || len(podUIDs) != 1 || podUIDs[0] != "pod-1" {
		t.Fatalf("pending worker event Pods = %#v, %v", podUIDs, err)
	}
	pending, err := db.HasPendingWorkerEvents(ctx, created.ID, attempt)
	if err != nil || !pending {
		t.Fatalf("attempt pending worker event = %t, %v", pending, err)
	}
	if err := db.MarkWorkerEventProcessed(ctx, events[0].ID); err != nil {
		t.Fatalf("mark worker event processed: %v", err)
	}
	events, err = db.ListPendingWorkerEvents(ctx, "pod-1")
	if err != nil || len(events) != 0 {
		t.Fatalf("pending events after processing = %#v, %v", events, err)
	}
	pending, err = db.HasPendingWorkerEvents(ctx, created.ID, attempt)
	if err != nil || pending {
		t.Fatalf("processed attempt pending worker event = %t, %v", pending, err)
	}
	if err := db.MarkPodLogsExhausted(ctx, "pod-1", other.ID, other.CurrentAttemptID); !errors.Is(err, ErrConflict) {
		t.Fatalf("mark Pod cursor for another attempt = %v, want ErrConflict", err)
	}
	cursor, err = db.GetPodLogCursor(ctx, "pod-1")
	if err != nil || cursor.Exhausted {
		t.Fatalf("cursor exhausted by another attempt = %#v, %v", cursor, err)
	}
	if err := db.MarkPodLogsExhausted(ctx, "pod-1", created.ID, attempt); err != nil {
		t.Fatalf("mark Pod logs exhausted: %v", err)
	}
	cursor, err = db.GetPodLogCursor(ctx, "pod-1")
	if err != nil || !cursor.Exhausted {
		t.Fatalf("exhausted Pod cursor = %#v, %v", cursor, err)
	}
	if err := db.MarkPodLogsExhausted(ctx, "empty-pod", created.ID, attempt); err != nil {
		t.Fatalf("mark empty Pod logs exhausted: %v", err)
	}
	emptyCursor, err := db.GetPodLogCursor(ctx, "empty-pod")
	if err != nil || !emptyCursor.Exhausted {
		t.Fatalf("empty exhausted Pod cursor = %#v, %v", emptyCursor, err)
	}
}

func TestAppendPodLogOrdersAtomicallyWithLogsExhaustion(t *testing.T) {
	for _, appendFirst := range []bool{true, false} {
		name := "logs exhausted first"
		if appendFirst {
			name = "event append first"
		}
		t.Run(name, func(t *testing.T) { testAppendPodLogBarrierOrder(t, appendFirst) })
	}
}

func testAppendPodLogBarrierOrder(t *testing.T, appendFirst bool) {
	t.Helper()
	ctx := context.Background()
	db := openTestStore(t)
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "atomic log barrier"})
	if err != nil {
		t.Fatal(err)
	}
	params := AppendPodLogParams{
		TaskID: created.ID, AttemptID: created.CurrentAttemptID, PodUID: "barrier-pod", JobName: "job", PodName: "pod",
		Content: []byte("worker event"), WorkerEventID: "barrier-event", WorkerEvent: "worker event",
	}
	appendGate, exhaustedGate := make(chan struct{}), make(chan struct{})
	appendResult, exhaustedResult := make(chan error, 1), make(chan error, 1)
	var started sync.WaitGroup
	started.Add(2)
	go func() {
		started.Done()
		<-appendGate
		_, err := db.AppendPodLog(ctx, params, 256, 64)
		appendResult <- err
	}()
	go func() {
		started.Done()
		<-exhaustedGate
		exhaustedResult <- db.MarkLogsExhausted(ctx, created.ID, created.CurrentAttemptID)
	}()
	started.Wait()
	if appendFirst {
		close(appendGate)
		if err := <-appendResult; err != nil {
			t.Fatalf("append before barrier: %v", err)
		}
		close(exhaustedGate)
	} else {
		close(exhaustedGate)
		if err := <-exhaustedResult; err != nil {
			t.Fatalf("close log barrier: %v", err)
		}
		close(appendGate)
	}
	appendErr, exhaustedErr := error(nil), error(nil)
	if appendFirst {
		exhaustedErr = <-exhaustedResult
	} else {
		appendErr = <-appendResult
	}
	if exhaustedErr != nil {
		t.Fatalf("mark logs exhausted: %v", exhaustedErr)
	}
	pending, err := db.HasPendingWorkerEvents(ctx, created.ID, created.CurrentAttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if appendFirst {
		if !pending {
			t.Fatal("event committed before the barrier is not pending")
		}
	} else if !errors.Is(appendErr, ErrConflict) || pending {
		t.Fatalf("append after barrier error/pending = %v/%t, want ErrConflict/false", appendErr, pending)
	}
	if _, err := db.AppendPodLog(ctx, params, 256, 64); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate append after barrier = %v, want ErrConflict", err)
	}

}

func TestPodLogCursorTailAndChunkReads(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "logs"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	attempt := created.CurrentAttemptID
	if _, err := db.AppendPodLog(ctx, AppendPodLogParams{TaskID: created.ID, AttemptID: attempt, PodUID: "pod", Content: []byte("first\nsecond\n")}, 256, 5); err != nil {
		t.Fatalf("append Pod log: %v", err)
	}
	tail, sequence, err := db.ReadLogTailCursor(ctx, created.ID, attempt, 0)
	if err != nil || tail != "" || sequence == 0 {
		t.Fatalf("zero-line cursor tail = %q, %d, %v", tail, sequence, err)
	}
	tail, sameSequence, err := db.ReadLogTailCursor(ctx, created.ID, attempt, 1)
	if err != nil || tail != "second\n" || sameSequence != sequence {
		t.Fatalf("one-line cursor tail = %q, %d, %v", tail, sameSequence, err)
	}
	if _, _, err := db.ReadLogTailCursor(ctx, created.ID, attempt, -1); err == nil {
		t.Fatal("cursor tail accepted negative lines")
	}
	if _, _, err := db.ReadLogTailCursor(ctx, created.ID, "missing", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cursor tail for missing attempt = %v, want ErrNotFound", err)
	}
	chunks, err := db.ReadLogChunksAfter(ctx, created.ID, attempt, 0, 2)
	if err != nil || len(chunks) != 2 || chunks[0].Sequence != 1 || chunks[0].Content != "first" {
		t.Fatalf("first log chunks = %#v, %v", chunks, err)
	}
	remaining, err := db.ReadLogChunksAfter(ctx, created.ID, attempt, chunks[1].Sequence, 100)
	if err != nil || len(remaining) == 0 {
		t.Fatalf("remaining log chunks = %#v, %v", remaining, err)
	}
}

func TestPodLogTailTrimming(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	trimmedTask, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "trim"})
	if err != nil {
		t.Fatalf("create trimming task: %v", err)
	}
	maxBytes := len(LogTruncationMarker) + 20
	firstContent := []byte(strings.Repeat("x", maxBytes-1))
	if result, err := db.AppendPodLog(ctx, AppendPodLogParams{TaskID: trimmedTask.ID, AttemptID: trimmedTask.CurrentAttemptID, PodUID: "trim-pod", Content: firstContent}, maxBytes, 7); err != nil || result.AppendedBytes != len(firstContent) {
		t.Fatalf("append pre-quota content = %#v, %v", result, err)
	}
	result, err := db.AppendPodLog(ctx, AppendPodLogParams{TaskID: trimmedTask.ID, AttemptID: trimmedTask.CurrentAttemptID, PodUID: "trim-pod", UntimestampedOrdinal: 1, Content: []byte("overflow")}, maxBytes, 7)
	if err != nil || !result.Truncated || result.AppendedBytes != len(LogTruncationMarker) {
		t.Fatalf("trim overflowing tail = %#v, %v", result, err)
	}
	trimmed, err := db.ReadLogTail(ctx, trimmedTask.ID, trimmedTask.CurrentAttemptID, 100)
	if err != nil || len(trimmed) != maxBytes || !strings.HasSuffix(trimmed, LogTruncationMarker) {
		t.Fatalf("trimmed log = %d bytes %q, %v", len(trimmed), trimmed, err)
	}
}

func TestKubernetesObservationsAndSecretCleanupLifecycle(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "observe"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	attempt := created.CurrentAttemptID
	if err := db.ObserveKubernetesJob(ctx, KubernetesJobObservation{}); err == nil {
		t.Fatal("Job observation accepted incomplete identity")
	}
	if err := db.ObserveKubernetesPod(ctx, KubernetesPodObservation{}); err == nil {
		t.Fatal("Pod observation accepted incomplete identity")
	}
	if err := db.ObserveKubernetesJob(ctx, KubernetesJobObservation{TaskID: created.ID, AttemptID: "missing", Namespace: "workers", Name: "job", UID: "uid"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Job observation for missing attempt = %v, want ErrNotFound", err)
	}
	if err := db.RegisterSecretCleanup(ctx, SecretCleanup{}); err == nil {
		t.Fatal("Secret cleanup accepted incomplete identity")
	}
	if err := db.RegisterSecretCleanup(ctx, SecretCleanup{TaskID: created.ID, AttemptID: "missing", AttemptNumber: 1, Namespace: "workers", JobName: "job", SecretName: "secret"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Secret cleanup for missing attempt = %v, want ErrNotFound", err)
	}

	started := time.Date(2026, 8, 6, 10, 0, 0, 123, time.UTC)
	completed := started.Add(time.Minute)
	jobObservation := KubernetesJobObservation{
		TaskID: created.ID, AttemptID: attempt, AttemptNumber: 1, Namespace: "workers", Name: "job-1", UID: "job-uid-1",
		State: "complete", Reason: "Completed", Message: "done", StartedAt: &started, CompletedAt: &completed, SecretName: "task-secret",
	}
	if err := db.ObserveKubernetesJob(ctx, jobObservation); err != nil {
		t.Fatalf("observe Job: %v", err)
	}
	if err := db.ObserveKubernetesPod(ctx, KubernetesPodObservation{
		TaskID: created.ID, AttemptID: attempt, Namespace: "workers", Name: "pod-1", UID: "pod-uid-1",
		State: "succeeded", Reason: "Completed", StartedAt: &started, CompletedAt: &completed,
	}); err != nil {
		t.Fatalf("observe Pod: %v", err)
	}
	job, pod, err := db.AttemptKubernetes(ctx, attempt)
	if err != nil || job.APIVersion != "batch/v1" || pod.APIVersion != "v1" || pod.ContainerStates != "{}" || job.StartedAt == nil || !job.StartedAt.Equal(started) || pod.CompletedAt == nil || !pod.CompletedAt.Equal(completed) {
		t.Fatalf("Kubernetes observations = Job %#v, Pod %#v, %v", job, pod, err)
	}
	emptyJob, emptyPod, err := db.AttemptKubernetes(ctx, "missing")
	if err != nil || emptyJob != (KubernetesJobObservation{}) || emptyPod != (KubernetesPodObservation{}) {
		t.Fatalf("missing Kubernetes observations = %#v, %#v, %v", emptyJob, emptyPod, err)
	}

	cleanup, err := db.GetSecretCleanup(ctx, attempt)
	if err != nil || cleanup.AttemptNumber != 1 || cleanup.JobUID != "job-uid-1" || cleanup.SecretName != "task-secret" || cleanup.Generation != 1 || cleanup.EligibleAt != nil {
		t.Fatalf("initial Secret cleanup = %#v, %v", cleanup, err)
	}
	cleanups, err := db.ListSecretCleanups(ctx)
	if err != nil || len(cleanups) != 1 || cleanups[0].AttemptID != attempt {
		t.Fatalf("list Secret cleanups = %#v, %v", cleanups, err)
	}
	stale := cleanup
	stale.Generation++
	if err := db.MarkSecretCleanupEligible(ctx, stale, completed); !errors.Is(err, ErrConflict) {
		t.Fatalf("mark stale cleanup eligible = %v, want ErrConflict", err)
	}
	eligibleAt := completed.Add(time.Minute)
	if err := db.MarkSecretCleanupEligible(ctx, cleanup, eligibleAt); err != nil {
		t.Fatalf("mark cleanup eligible: %v", err)
	}
	cleanup, err = db.GetSecretCleanup(ctx, attempt)
	if err != nil || cleanup.EligibleAt == nil || !cleanup.EligibleAt.Equal(eligibleAt) {
		t.Fatalf("eligible Secret cleanup = %#v, %v", cleanup, err)
	}
	if _, err := db.BindSecretCleanupUID(ctx, cleanup, ""); err != nil {
		t.Fatalf("bind initially empty Secret UID: %v", err)
	}
	cleanup, err = db.BindSecretCleanupUID(ctx, cleanup, "secret-uid-1")
	if err != nil || cleanup.SecretUID != "secret-uid-1" {
		t.Fatalf("bind Secret UID = %#v, %v", cleanup, err)
	}
	wrongUID := cleanup
	wrongUID.SecretUID = "wrong"
	if err := db.CompleteSecretCleanup(ctx, wrongUID); !errors.Is(err, ErrConflict) {
		t.Fatalf("complete cleanup with wrong Secret UID = %v, want ErrConflict", err)
	}
	if err := db.CompleteSecretCleanup(ctx, cleanup); err != nil {
		t.Fatalf("complete Secret cleanup: %v", err)
	}
	if _, err := db.GetSecretCleanup(ctx, attempt); !errors.Is(err, ErrNotFound) {
		t.Fatalf("completed cleanup lookup = %v, want ErrNotFound", err)
	}
	cleanups, err = db.ListSecretCleanups(ctx)
	if err != nil || len(cleanups) != 0 {
		t.Fatalf("cleanups after completion = %#v, %v", cleanups, err)
	}
}

func TestSecretCleanupIdentityChangeInvalidatesEligibility(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "cleanup"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	registered := SecretCleanup{TaskID: created.ID, AttemptID: created.CurrentAttemptID, AttemptNumber: 1, Namespace: "workers", JobName: "job", SecretName: "secret-a"}
	if err := db.RegisterSecretCleanup(ctx, registered); err != nil {
		t.Fatalf("register cleanup: %v", err)
	}
	if err := db.RegisterSecretCleanup(ctx, registered); err != nil {
		t.Fatalf("replay cleanup registration: %v", err)
	}
	registered.SecretName = "secret-b"
	if err := db.RegisterSecretCleanup(ctx, registered); err != nil {
		t.Fatalf("replace pre-Job cleanup identity: %v", err)
	}
	cleanup, err := db.GetSecretCleanup(ctx, created.CurrentAttemptID)
	if err != nil || cleanup.Generation != 2 || cleanup.SecretName != "secret-b" {
		t.Fatalf("changed registered cleanup = %#v, %v", cleanup, err)
	}
	if err := db.ObserveKubernetesJob(ctx, KubernetesJobObservation{TaskID: created.ID, AttemptID: created.CurrentAttemptID, AttemptNumber: 1, Namespace: "workers", Name: "job", UID: "job-uid", State: "running", SecretName: "secret-b"}); err != nil {
		t.Fatalf("observe cleanup Job: %v", err)
	}
	cleanup, err = db.GetSecretCleanup(ctx, created.CurrentAttemptID)
	if err != nil || cleanup.Generation != 3 || cleanup.JobUID != "job-uid" {
		t.Fatalf("Job-bound cleanup = %#v, %v", cleanup, err)
	}
	if err := db.MarkSecretCleanupEligible(ctx, cleanup, time.Now().UTC()); err != nil {
		t.Fatalf("mark cleanup eligible: %v", err)
	}
	if err := db.MarkSecretCleanupIneligible(ctx, cleanup.AttemptID); err != nil {
		t.Fatalf("mark cleanup ineligible: %v", err)
	}
	invalidated, err := db.GetSecretCleanup(ctx, cleanup.AttemptID)
	if err != nil || invalidated.Generation != cleanup.Generation+1 || invalidated.EligibleAt != nil || invalidated.SecretUID != "" {
		t.Fatalf("invalidated cleanup = %#v, %v", invalidated, err)
	}
	if err := db.MarkSecretCleanupEligible(ctx, cleanup, time.Now().UTC()); !errors.Is(err, ErrConflict) {
		t.Fatalf("old cleanup generation remained usable: %v", err)
	}
	if _, err := db.BindSecretCleanupUID(ctx, invalidated, "secret-uid"); !errors.Is(err, ErrConflict) {
		t.Fatalf("ineligible cleanup accepted Secret UID: %v", err)
	}
}
