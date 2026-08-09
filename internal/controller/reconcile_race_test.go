package controller

import (
	"errors"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stesting "k8s.io/client-go/testing"

	"github.com/simpleswe/simpleswe/internal/kubernetes/jobs"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestMissingJobAfterEnsureRetriesWithoutClosingLogs(t *testing.T) {
	fixture := newFixture(t)
	record := createTask(t, fixture, "recreate Job after Get race", "missing-job-after-ensure")
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	jobName := jobs.Name(record.ID, attempt.Number)
	jobGets := 0
	fixture.kube.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		get := action.(k8stesting.GetAction)
		if get.GetName() != jobName {
			return false, nil, nil
		}
		jobGets++
		if jobGets != 2 {
			return false, nil, nil
		}
		if err := fixture.kube.Tracker().Delete(batchv1.SchemeGroupVersion.WithResource("jobs"), workerNamespace, jobName); err != nil {
			t.Fatalf("delete Job between ensure and Get: %v", err)
		}
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, jobName)
	})

	if err := fixture.controller.Reconcile(fixture.ctx); !apierrors.IsNotFound(err) {
		t.Fatalf("raced Reconcile() error = %v; want transient Job NotFound", err)
	}
	if observations := eventText(listEvents(t, fixture, record.ID)); !strings.Contains(observations, "transient resource reconciliation; retry pending") {
		t.Fatalf("raced reconciliation observations = %q; want transient retry", observations)
	}
	attempt, err = fixture.store.GetAttempt(fixture.ctx, record.ID, attempt.ID)
	if err != nil || attempt.LogsExhausted {
		t.Fatalf("raced attempt = %#v, %v; want logs open", attempt, err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("recreate missing Job: %v", err)
	}
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{}); err != nil {
		t.Fatalf("get recreated Job: %v", err)
	}
	jobsList, jobsErr := fixture.kube.BatchV1().Jobs(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
	secrets, secretsErr := fixture.kube.CoreV1().Secrets(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
	if jobsErr != nil || secretsErr != nil || len(jobsList.Items) != 1 || len(secrets.Items) != 1 {
		t.Fatalf("recreated resources: Jobs=%d Secrets=%d errors=%v/%v; want one each", len(jobsList.Items), len(secrets.Items), jobsErr, secretsErr)
	}
	var jobCreates, secretCreates int
	for _, resource := range createResources(fixture.kube.Actions()) {
		switch resource {
		case "jobs":
			jobCreates++
		case "secrets":
			secretCreates++
		}
	}
	if jobCreates != 2 || secretCreates != 1 {
		t.Fatalf("resource creates: Jobs=%d Secrets=%d; want initial and replacement Job with one Secret", jobCreates, secretCreates)
	}

	event := protocol.Event{Type: protocol.EventAgentStarted, TaskID: record.ID}
	queueDurableWorkerEvent(t, fixture, "event-after-job-recreation", "pod-after-job-recreation", jobName, "worker-pod", event)
	if err := fixture.controller.(*Controller).HandleWorkerEventOnce(fixture.ctx, "event-after-job-recreation", jobName, "worker-pod", event); err != nil {
		t.Fatalf("process event after Job recreation: %v", err)
	}
	if err := fixture.store.MarkWorkerEventProcessed(fixture.ctx, "event-after-job-recreation"); err != nil {
		t.Fatal(err)
	}
	if got := getTask(t, fixture, record.ID).State; got != task.AGENT_RUNNING {
		t.Fatalf("state after recreated Job event = %q; want AGENT_RUNNING", got)
	}
	attempt, err = fixture.store.GetAttempt(fixture.ctx, record.ID, attempt.ID)
	if err != nil || attempt.LogsExhausted {
		t.Fatalf("attempt after recreated Job event = %#v, %v; want logs open", attempt, err)
	}
}

func TestMissingTerminalJobDrainsPendingEventsBeforeClosingLogs(t *testing.T) {
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "drain before missing Job closure", "missing-terminal-job-pending-event")
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	jobName, podName := jobs.Name(record.ID, attempt.Number), "worker-pod-a1"
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{
		Type: protocol.EventWorkerFailed, TaskID: record.ID, Message: "terminal before durable replay", ExitCode: 1,
	}); err != nil {
		t.Fatalf("fail task: %v", err)
	}
	pendingEvent := protocol.Event{Type: protocol.EventAgentStarted, TaskID: record.ID}
	queueDurableWorkerEvent(t, fixture, "terminal-pending-event", "terminal-pod-uid", jobName, podName, pendingEvent)
	if err := fixture.kube.BatchV1().Jobs(workerNamespace).Delete(fixture.ctx, jobName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.kube.CoreV1().Pods(workerNamespace).Delete(fixture.ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("reconcile missing terminal Job with pending event: %v", err)
	}
	attempt, err = fixture.store.GetAttempt(fixture.ctx, record.ID, attempt.ID)
	if err != nil || attempt.LogsExhausted {
		t.Fatalf("attempt before pending drain = %#v, %v; want logs open", attempt, err)
	}
	if err := fixture.controller.(*Controller).HandleWorkerEventOnce(fixture.ctx, "terminal-pending-event", jobName, podName, pendingEvent); err != nil {
		t.Fatalf("process terminal pending event: %v", err)
	}
	if err := fixture.store.MarkWorkerEventProcessed(fixture.ctx, "terminal-pending-event"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("reconcile missing terminal Job after pending drain: %v", err)
	}
	attempt, err = fixture.store.GetAttempt(fixture.ctx, record.ID, attempt.ID)
	if err != nil || !attempt.LogsExhausted {
		t.Fatalf("attempt after pending drain = %#v, %v; want logs exhausted", attempt, err)
	}
}

func TestPermanentResourceReconciliationOrdersWithDurableWorkerEvents(t *testing.T) {
	for _, appendFirst := range []bool{true, false} {
		name := "logs exhausted first"
		if appendFirst {
			name = "worker events appended first"
		}
		t.Run(name, func(t *testing.T) { testPermanentResourceReconciliationOrder(t, appendFirst, name) })
	}
}

func testPermanentResourceReconciliationOrder(t *testing.T, appendFirst bool, name string) {
	t.Helper()
	fixture := newFixture(t)
	record := createTask(t, fixture, "resource reconciliation race", "resource-reconciliation-race-"+name)
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	jobName, podName, podUID := jobs.Name(record.ID, attempt.Number), "worker-pod", "worker-pod-uid"
	events := []protocol.Event{
		{Type: protocol.EventAgentStarted, TaskID: record.ID},
		{Type: protocol.EventValidationStarted, TaskID: record.ID, Command: []string{"go", "test", "./..."}},
	}
	eventIDs := []string{"race-agent-started", "race-validation-started"}
	encoded := make([]string, len(events))
	for i := range events {
		encoded[i], err = protocol.EncodeEvent(events[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	apiStarted, releaseAPI := make(chan struct{}, 1), make(chan struct{})
	fixture.kube.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.(k8stesting.GetAction).GetName() != jobName {
			return false, nil, nil
		}
		apiStarted <- struct{}{}
		<-releaseAPI
		return true, nil, apierrors.NewInvalid(schema.GroupKind{Group: "batch", Kind: "Job"}, jobName, nil)
	})

	reconcileDone := make(chan error, 1)
	go func() {
		reconcileDone <- fixture.controller.(*Controller).reconcileTask(fixture.ctx, record)
	}()
	<-apiStarted

	appendEvents := func() []error {
		appendErrors := make([]error, len(events))
		for i := range events {
			_, appendErrors[i] = fixture.store.AppendPodLog(fixture.ctx, store.AppendPodLogParams{
				TaskID: record.ID, AttemptID: attempt.ID, PodUID: podUID, JobName: jobName, PodName: podName,
				UntimestampedOrdinal: i + 1, Content: []byte(encoded[i]), WorkerEventID: eventIDs[i], WorkerEvent: encoded[i],
			}, 1<<20, 64<<10)
		}
		return appendErrors
	}

	var appendErrors []error
	if appendFirst {
		appendErrors = appendEvents()
	}
	close(releaseAPI)
	reconcileErr := <-reconcileDone
	if !appendFirst {
		appendErrors = appendEvents()
	}
	if reconcileErr == nil || !apierrors.IsInvalid(reconcileErr) {
		t.Fatalf("permanent reconciliation error = %v, want Kubernetes Invalid", reconcileErr)
	}

	pending, err := fixture.store.HasPendingWorkerEvents(fixture.ctx, record.ID, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	current := getTask(t, fixture, record.ID)
	currentAttempt, err := fixture.store.GetAttempt(fixture.ctx, record.ID, attempt.ID)
	if err != nil || !currentAttempt.LogsExhausted {
		t.Fatalf("attempt after permanent reconciliation = %#v, %v; want logs exhausted", currentAttempt, err)
	}
	if appendFirst {
		assertEventsCommittedBeforeExhaustion(t, fixture, record, jobName, podName, events, eventIDs, appendErrors, pending, current)
		return
	}
	assertEventsRejectedAfterExhaustion(t, appendErrors, pending, current)
}

func assertEventsCommittedBeforeExhaustion(t *testing.T, fixture *fixture, record store.Task, jobName, podName string, events []protocol.Event, eventIDs []string, appendErrors []error, pending bool, current store.Task) {
	t.Helper()
	for i, appendErr := range appendErrors {
		if appendErr != nil {
			t.Fatalf("append event %d before exhaustion: %v", i, appendErr)
		}
	}
	if !pending || current.State == task.FAILED {
		t.Fatalf("committed worker events pending/state = %t/%q, want true/non-FAILED", pending, current.State)
	}
	control := fixture.controller.(*Controller)
	for i := range events {
		if err := control.HandleWorkerEventOnce(fixture.ctx, eventIDs[i], jobName, podName, events[i]); err != nil {
			t.Fatalf("recover durable event %d: %v", i, err)
		}
		if err := fixture.store.MarkWorkerEventProcessed(fixture.ctx, eventIDs[i]); err != nil {
			t.Fatal(err)
		}
	}
	if got := getTask(t, fixture, record.ID).State; got != task.VALIDATING {
		t.Fatalf("state after durable agent/validation recovery = %q, want VALIDATING", got)
	}
}

func assertEventsRejectedAfterExhaustion(t *testing.T, appendErrors []error, pending bool, current store.Task) {
	t.Helper()
	for i, appendErr := range appendErrors {
		if !errors.Is(appendErr, store.ErrConflict) {
			t.Fatalf("append event %d after exhaustion = %v, want ErrConflict", i, appendErr)
		}
	}
	if pending || current.State != task.FAILED {
		t.Fatalf("exhaustion-first pending/state = %t/%q, want false/FAILED", pending, current.State)
	}
}
