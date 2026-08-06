package task

import "fmt"

// State is the durable state of a task.
type State string

const (
	RECEIVED       State = "received"
	QUEUED         State = "queued"
	CREATING_JOB   State = "creating_job"
	JOB_PENDING    State = "job_pending"
	RUNNING        State = "running"
	AGENT_RUNNING  State = "agent_running"
	VALIDATING     State = "validating"
	COMMITTING     State = "committing"
	PUSHING        State = "pushing"
	CREATING_PR    State = "creating_pr"
	PR_OPEN        State = "pr_open"
	WAITING_CI     State = "waiting_ci"
	WAITING_REVIEW State = "waiting_review"
	READY          State = "ready"
	FAILED         State = "failed"
	CANCELLED      State = "cancelled"
)

// Machine validates task transitions. It has no mutable state, so its zero
// value is ready for use.
type Machine struct{}

var lifecycle = []State{
	RECEIVED,
	QUEUED,
	CREATING_JOB,
	JOB_PENDING,
	RUNNING,
	AGENT_RUNNING,
	VALIDATING,
	COMMITTING,
	PUSHING,
	CREATING_PR,
	PR_OPEN,
	WAITING_CI,
	WAITING_REVIEW,
	READY,
}

// Transition returns an error unless from/to are one adjacent lifecycle step,
// or the task is moving to FAILED or CANCELLED from a non-terminal state.
func (Machine) Transition(from, to State) error {
	if !known(from) || !known(to) {
		return fmt.Errorf("unknown task state transition %q -> %q", from, to)
	}
	if from == READY || from == FAILED || from == CANCELLED {
		return fmt.Errorf("terminal task state %q cannot transition", from)
	}
	for i := 1; i < len(lifecycle); i++ {
		if lifecycle[i-1] == from && lifecycle[i] == to {
			return nil
		}
	}
	if to == FAILED || to == CANCELLED {
		return nil
	}
	return fmt.Errorf("invalid task transition %q -> %q", from, to)
}

// Retry validates the explicit aggregate reset used to start a new immutable
// attempt. It is deliberately separate from ordinary lifecycle transitions.
func (Machine) Retry(from, to State) error {
	if from != FAILED || to != QUEUED {
		return fmt.Errorf("invalid task retry %q -> %q", from, to)
	}
	return nil
}

// ForgeFollowUp validates the explicit aggregate reset used to address forge
// feedback on an existing pull request with a new immutable attempt.
func (Machine) ForgeFollowUp(from, to State) error {
	if to == QUEUED {
		switch from {
		case PR_OPEN, WAITING_CI, WAITING_REVIEW, READY:
			return nil
		}
	}
	return fmt.Errorf("invalid forge follow-up %q -> %q", from, to)
}

func known(state State) bool {
	if state == FAILED || state == CANCELLED {
		return true
	}
	for _, candidate := range lifecycle {
		if candidate == state {
			return true
		}
	}
	return false
}
