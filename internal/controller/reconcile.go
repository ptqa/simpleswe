package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/simpleswe/simpleswe/internal/kubernetes/jobs"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
)

var recoveryLifecycle = []task.State{
	task.RECEIVED,
	task.QUEUED,
	task.CREATING_JOB,
	task.JOB_PENDING,
	task.RUNNING,
	task.AGENT_RUNNING,
	task.VALIDATING,
	task.COMMITTING,
	task.PUSHING,
	task.CREATING_PR,
	task.PR_OPEN,
	task.WAITING_CI,
	task.WAITING_REVIEW,
	task.READY,
}

// Reconcile starts from durable active intents, then creates or adopts their
// deterministic Kubernetes resources and reconciles observed status.
func (c *Controller) Reconcile(ctx context.Context) error {
	records, err := c.store.ListActiveTasks(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, record := range records {
		err := c.reconcileTask(ctx, record)
		if err != nil {
			errs = append(errs, fmt.Errorf("reconcile task %s: %w", record.ID, err))
		}
	}
	return errors.Join(errs...)
}

func (c *Controller) reconcileTask(ctx context.Context, record store.Task) error {
	unlock, err := c.locks.lock(ctx, record.ID)
	if err != nil {
		return err
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(ctx, c.providerTimeout)
	defer cancel()
	record, err = c.store.GetTask(ctx, record.ID)
	if err != nil {
		return err
	}
	attempt, err := c.store.CurrentAttempt(ctx, record.ID)
	if err != nil {
		return err
	}
	if record.CancellationRequested {
		return c.reconcileCancellation(ctx, record, attempt)
	}
	repository, err := c.repository(record.Repository)
	if err != nil {
		return err
	}
	if record.State == task.RECEIVED {
		if err := c.transition(ctx, record.ID, task.RECEIVED, task.QUEUED, "recovery queued durable task intent", "system"); err != nil {
			return err
		}
		record.State = task.QUEUED
	}
	if record.State == task.QUEUED {
		if err := c.transition(ctx, record.ID, task.QUEUED, task.CREATING_JOB, "recovery creating deterministic resources", "system"); err != nil {
			return err
		}
		record.State = task.CREATING_JOB
	}
	if workerResourceState(record.State) {
		recovered, err := c.ensureAttemptResources(ctx, record, attempt, repository)
		if err != nil {
			failedState := record.State
			latest, getErr := c.store.GetTask(ctx, record.ID)
			if getErr != nil {
				return errors.Join(err, getErr)
			}
			record = latest
			if errors.Is(err, errResourceCreationBlocked) || record.CancellationRequested || terminal(record.State) || (failedState == task.CREATING_JOB && stateAtOrAfter(record.State, task.JOB_PENDING)) {
				return nil
			}
			reason := failureMessage("kubernetes", jobs.Name(record.ID, attempt.Number), "", nil, -1, err)
			if permanentKubernetesError(err) {
				_ = c.store.MarkLogsExhausted(ctx, record.ID, attempt.ID)
				if transitionErr := c.transition(ctx, record.ID, record.State, task.FAILED, reason, "kubernetes"); transitionErr != nil {
					return errors.Join(err, transitionErr)
				}
				return err
			}
			if observationErr := c.store.RecordObservation(ctx, record.ID, "transient resource reconciliation; retry pending "+reason, "kubernetes"); observationErr != nil {
				return errors.Join(err, observationErr)
			}
			return err
		}
		if len(recovered) > 0 {
			if err := c.store.RecordObservation(ctx, record.ID, "recovery recreated "+strings.Join(recovered, " and "), "kubernetes"); err != nil {
				return err
			}
		} else if record.State == task.CREATING_JOB {
			if err := c.store.RecordObservation(ctx, record.ID, "recovery adopted deterministic Job and Secret", "kubernetes"); err != nil {
				return err
			}
		}
		if record.State == task.CREATING_JOB {
			if err := c.completeAttemptResources(ctx, record.ID, attempt.ID, "recovery worker Job available job="+jobs.Name(record.ID, attempt.Number)); err != nil {
				return err
			}
			record.State = task.JOB_PENDING
		}
	}
	if stateAtOrAfter(record.State, task.VALIDATING) && !stateAtOrAfter(record.State, task.PR_OPEN) {
		if _, err := c.store.GetGitResult(ctx, attempt.ID); err == nil {
			if err := c.resumePullRequestLocked(ctx, record, attempt, "recovery durable branch"); err != nil {
				return err
			}
			record, err = c.store.GetTask(ctx, record.ID)
			if err != nil {
				return err
			}
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	jobName := jobs.Name(record.ID, attempt.Number)
	job, err := c.kubernetes.BatchV1().Jobs(c.config.Controller.Namespace).Get(ctx, jobName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) && !workerResourceState(record.State) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get worker Job %s: %w", jobName, err)
	}
	if err := verifyResourceLabels("Job", job.Labels, record.ID, attempt); err != nil {
		return err
	}
	return c.reconcileJob(ctx, record, attempt, job)
}

func (c *Controller) reconcileJob(ctx context.Context, taskRecord store.Task, attempt store.Attempt, job *batchv1.Job) error {
	if (jobFailed(job) || jobComplete(job)) && !attempt.LogsExhausted {
		exhausted, err := c.markLogsExhaustedWithoutPod(ctx, taskRecord, attempt, job)
		if err != nil {
			return err
		}
		attempt.LogsExhausted = exhausted
	}
	switch {
	case jobFailed(job):
		if terminal(taskRecord.State) || !attempt.LogsExhausted {
			return nil
		}
		return c.finishFailedJob(ctx, taskRecord, attempt, job)
	case jobComplete(job):
		if terminal(taskRecord.State) || !attempt.LogsExhausted {
			return nil
		}
		return c.finishCompletedJob(ctx, taskRecord, attempt, job.Name)
	case job.Status.Active > 0:
		if terminal(taskRecord.State) || stateAtOrAfter(taskRecord.State, task.RUNNING) {
			return nil
		}
		return c.recoverTo(ctx, taskRecord, task.RUNNING, "recovery running job="+job.Name)
	default:
		return nil
	}
}

func (c *Controller) markLogsExhaustedWithoutPod(ctx context.Context, record store.Task, attempt store.Attempt, job *batchv1.Job) (bool, error) {
	selector := fmt.Sprintf("simpleswe.dev/task-id=%s,simpleswe.dev/attempt-id=%s", record.ID, attempt.ID)
	pods, err := c.kubernetes.CoreV1().Pods(job.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return false, fmt.Errorf("list terminal Job Pods: %w", err)
	}
	for i := range pods.Items {
		if podOwnedByJob(&pods.Items[i], job.Name, job.UID) {
			return false, nil
		}
	}
	if err := c.store.MarkLogsExhausted(ctx, record.ID, attempt.ID); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Controller) finishFailedJob(ctx context.Context, record store.Task, attempt store.Attempt, job *batchv1.Job) error {
	pod, exitCode, command := c.failedPod(ctx, job)
	reason := "recovery failed " + failureMessage("kubernetes", job.Name, pod, command, exitCode, nil)
	if record.State != task.VALIDATING {
		return c.transition(ctx, record.ID, record.State, task.FAILED, reason, "kubernetes")
	}
	if err := c.store.MarkValidationComplete(ctx, record.ID, attempt.ID, "failed"); err == nil {
		return c.transition(ctx, record.ID, task.VALIDATING, task.FAILED, reason, "kubernetes")
	} else if !errors.Is(err, store.ErrConflict) {
		return err
	}
	return c.transitionWith(ctx, record.ID, task.VALIDATING, task.FAILED, reason, "kubernetes", &store.ValidationTransition{State: "failed", Error: reason})
}

func (c *Controller) finishCompletedJob(ctx context.Context, record store.Task, attempt store.Attempt, jobName string) error {
	git, gitErr := c.store.GetGitResult(ctx, attempt.ID)
	pr, prErr := c.store.GetPullRequest(ctx, attempt.ID)
	if gitErr == nil && prErr == nil && git.State == "pushed" && git.Branch != "" && git.CommitSHA != "" && pr.State == "open" && pr.HeadBranch == git.Branch && pr.Number > 0 && pr.URL != "" {
		return nil
	}
	if gitErr != nil && !errors.Is(gitErr, store.ErrNotFound) {
		return gitErr
	}
	if prErr != nil && !errors.Is(prErr, store.ErrNotFound) {
		return prErr
	}
	if gitErr == nil && git.State == "pushed" && git.Branch != "" && git.CommitSHA != "" {
		// A durable push makes PR creation resumable. Provider and store failures
		// are retried by reconciliation rather than converted to log-EOF failure.
		return nil
	}
	reason := "indeterminate successful Job after logs exhausted: missing durable branch_pushed Git result and open pull request job=" + jobName
	return c.transition(ctx, record.ID, record.State, task.FAILED, reason, "kubernetes")
}

func (c *Controller) reconcileCancellation(ctx context.Context, record store.Task, attempt store.Attempt) error {
	jobName := jobs.Name(record.ID, attempt.Number)
	job, err := c.kubernetes.BatchV1().Jobs(c.config.Controller.Namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err == nil {
		if err := verifyResourceLabels("Job", job.Labels, record.ID, attempt); err != nil {
			return err
		}
		if err := c.deleteJob(ctx, c.config.Controller.Namespace, jobName, job.UID); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cancel worker Job %s: %w", jobName, err)
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get cancelling Job %s: %w", jobName, err)
	}
	if _, err := c.kubernetes.BatchV1().Jobs(c.config.Controller.Namespace).Get(ctx, jobName, metav1.GetOptions{}); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("confirm cancelling Job %s: %w", jobName, err)
	}
	selector := fmt.Sprintf("simpleswe.dev/task-id=%s,simpleswe.dev/attempt-id=%s", record.ID, attempt.ID)
	pods, err := c.kubernetes.CoreV1().Pods(c.config.Controller.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return fmt.Errorf("list cancelling Pods: %w", err)
	}
	for i := range pods.Items {
		if podOwnedByJob(&pods.Items[i], jobName, "") {
			return nil
		}
	}
	if err := c.store.MarkLogsExhausted(ctx, record.ID, attempt.ID); err != nil {
		return err
	}
	if err := c.transition(ctx, record.ID, record.State, task.CANCELLED, "cancellation confirmed Job and owned Pods absent job="+jobName, "controller"); err != nil {
		return err
	}
	c.logger.InfoContext(ctx, "task cancelled", "task", record.ID, "attempt", attempt.ID, "job", jobName)
	return nil
}

func workerResourceState(state task.State) bool {
	return state == task.CREATING_JOB || state == task.JOB_PENDING || state == task.RUNNING || state == task.AGENT_RUNNING || state == task.VALIDATING
}

func podOwnedByJob(pod *corev1.Pod, jobName string, jobUID types.UID) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.Kind != "Job" || owner.Name != jobName || owner.Controller == nil || !*owner.Controller {
			continue
		}
		return jobUID == "" || owner.UID == jobUID
	}
	return false
}

func (c *Controller) recoverTo(ctx context.Context, record store.Task, target task.State, reason string) error {
	fromIndex := stateIndex(record.State)
	targetIndex := stateIndex(target)
	if fromIndex < 0 || targetIndex < 0 || fromIndex >= targetIndex {
		return nil
	}
	for i := fromIndex + 1; i <= targetIndex; i++ {
		if err := c.transition(ctx, record.ID, recoveryLifecycle[i-1], recoveryLifecycle[i], reason, "kubernetes"); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) failedPod(ctx context.Context, job *batchv1.Job) (string, int, []string) {
	selector := fmt.Sprintf("simpleswe.dev/task-id=%s,simpleswe.dev/attempt-id=%s",
		job.Labels["simpleswe.dev/task-id"], job.Labels["simpleswe.dev/attempt-id"])
	pods, err := c.kubernetes.CoreV1().Pods(job.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil || len(pods.Items) == 0 {
		return "", -1, nil
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !podOwnedByJob(pod, job.Name, job.UID) {
			continue
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Terminated == nil {
				continue
			}
			var command []string
			for _, container := range pod.Spec.Containers {
				if container.Name == status.Name {
					command = append(append([]string(nil), container.Command...), container.Args...)
					break
				}
			}
			return pod.Name, int(status.State.Terminated.ExitCode), command
		}
	}
	return "", -1, nil
}

func jobFailed(job *batchv1.Job) bool {
	if job.Status.Failed > 0 {
		return true
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func jobComplete(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func stateIndex(state task.State) int {
	for i, candidate := range recoveryLifecycle {
		if candidate == state {
			return i
		}
	}
	return -1
}

func stateAtOrAfter(state, target task.State) bool {
	return stateIndex(state) >= stateIndex(target)
}

func terminal(state task.State) bool {
	return state == task.READY || state == task.FAILED || state == task.CANCELLED
}
