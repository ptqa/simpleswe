package task

import "testing"

func TestMachineAllowsTheTaskLifecycle(t *testing.T) {
	lifecycle := []State{
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

	var machine Machine
	for i := 1; i < len(lifecycle); i++ {
		if err := machine.Transition(lifecycle[i-1], lifecycle[i]); err != nil {
			t.Fatalf("transition %v -> %v: %v", lifecycle[i-1], lifecycle[i], err)
		}
	}
}

func TestMachineAllowsFailureAndCancellationFromNonTerminalStates(t *testing.T) {
	nonTerminal := []State{
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
	}

	var machine Machine
	for _, from := range nonTerminal {
		for _, to := range []State{FAILED, CANCELLED} {
			if err := machine.Transition(from, to); err != nil {
				t.Errorf("transition %v -> %v: %v", from, to, err)
			}
		}
	}
}

func TestMachineRejectsInvalidAndTerminalTransitions(t *testing.T) {
	cases := []struct {
		name string
		from State
		to   State
	}{
		{name: "skips lifecycle state", from: RECEIVED, to: RUNNING},
		{name: "moves backwards", from: VALIDATING, to: AGENT_RUNNING},
		{name: "skips review", from: WAITING_CI, to: READY},
		{name: "self transition", from: QUEUED, to: QUEUED},
		{name: "ready is terminal", from: READY, to: FAILED},
		{name: "failed is terminal", from: FAILED, to: QUEUED},
		{name: "cancelled is terminal", from: CANCELLED, to: QUEUED},
	}

	var machine Machine
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := machine.Transition(tc.from, tc.to); err == nil {
				t.Fatalf("transition %v -> %v was accepted", tc.from, tc.to)
			}
		})
	}
}

func TestMachineRetryIsTheOnlyFailedToQueuedOperation(t *testing.T) {
	var machine Machine
	if err := machine.Transition(FAILED, QUEUED); err == nil {
		t.Fatal("ordinary failed -> queued transition was accepted")
	}
	if err := machine.Retry(FAILED, QUEUED); err != nil {
		t.Fatalf("retry failed -> queued: %v", err)
	}
	for _, transition := range [][2]State{{READY, QUEUED}, {CANCELLED, QUEUED}, {FAILED, RUNNING}} {
		if err := machine.Retry(transition[0], transition[1]); err == nil {
			t.Errorf("retry %q -> %q was accepted", transition[0], transition[1])
		}
	}
}
