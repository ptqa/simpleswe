package controller

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/kubernetes/jobs"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestReconcileUsesAttemptSnapshottedForgeTargetAfterConfigChanges(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "keep original pull request route", "immutable-forge-route")
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "agent_started", TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_started", TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_result", TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_succeeded", TaskID: created.ID})
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, created.ID)
	if err != nil {
		t.Fatalf("current attempt: %v", err)
	}
	manifest, err := attemptManifest(attempt)
	if err != nil {
		t.Fatal(err)
	}
	var resources attemptResourceSnapshot
	if err := json.Unmarshal(attempt.ResourceSnapshot, &resources); err != nil {
		t.Fatalf("decode resource snapshot: %v", err)
	}
	wantTarget := forge.Target{
		Provider: forge.ProviderBitbucket, BaseURL: "https://api.bitbucket.org",
		Owner: "acme", Repository: "widget", CredentialsSecret: "bitbucket-widget",
	}
	if resources.ForgeTarget == nil || *resources.ForgeTarget != wantTarget {
		t.Fatalf("resource snapshot target = %#v; want %#v", resources.ForgeTarget, wantTarget)
	}
	branch := manifest.TaskBranch
	if err := fixture.store.RecordGitResult(fixture.ctx, store.GitResult{AttemptID: attempt.ID, State: "pushed", Branch: branch, CommitSHA: fullCommitSHA}); err != nil {
		t.Fatalf("record Git result: %v", err)
	}

	changed := fixture.config
	changed.Repositories[0].Bitbucket = config.RepositoryBitbucketConfig{}
	changed.Repositories[0].GitHub = config.RepositoryGitHubConfig{Owner: "new-owner", Repository: "new-repository", CredentialsSecret: "new-github"}
	changed.GitHub.BaseURL = "https://github.enterprise.example/api/v3"
	restarted, err := New(fixture.store, fixture.kube, changed, fixture.notifier, fixture.pullRequests)
	if err != nil {
		t.Fatalf("restart controller: %v", err)
	}
	if err := restarted.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	want := wantTarget
	if len(fixture.pullRequests.findTargets) != 1 || fixture.pullRequests.findTargets[0] != want {
		t.Fatalf("find targets = %#v; want %#v", fixture.pullRequests.findTargets, want)
	}
	if len(fixture.pullRequests.calls) != 1 || fixture.pullRequests.calls[0].target != want {
		t.Fatalf("create calls = %#v; want target %#v", fixture.pullRequests.calls, want)
	}
	if got := getTask(t, fixture, created.ID).State; got != task.PR_OPEN {
		t.Fatalf("reconciled state = %q; want %q", got, task.PR_OPEN)
	}
}

func TestAttemptForgeTargetFallsBackOnlyWhenSnapshotFieldIsAbsent(t *testing.T) {
	fixture := newFixture(t)
	control := fixture.controller.(*Controller)
	record := store.Task{Repository: repositoryURL}
	target, err := control.attemptForgeTarget(record, store.Attempt{ID: "legacy", ResourceSnapshot: []byte(`{"job":{},"secret":{}}`)})
	if err != nil {
		t.Fatalf("legacy target fallback: %v", err)
	}
	if want := (forge.Target{
		Provider: forge.ProviderBitbucket, BaseURL: "https://api.bitbucket.org",
		Owner: "acme", Repository: "widget", CredentialsSecret: "bitbucket-widget",
	}); target != want {
		t.Fatalf("legacy target = %#v; want %#v", target, want)
	}
	for name, snapshot := range map[string]string{
		"null":    `{"forge_target":null}`,
		"partial": `{"forge_target":{"provider":"github","owner":"acme","repository":"widget"}}`,
		"unknown": `{"forge_target":{"provider":"gitlab","base_url":"https://gitlab.example/api","owner":"acme","repository":"widget","credentials_secret_name":"gitlab"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := control.attemptForgeTarget(record, store.Attempt{ID: name, ResourceSnapshot: []byte(snapshot)}); err == nil {
				t.Fatal("invalid snapshotted target was accepted")
			}
		})
	}
}

func TestWorkerTaskJSONStrictlyDecodesWithPreForgeTargetSchema(t *testing.T) {
	fixture := newFixture(t)
	created := createTask(t, fixture, "remain compatible with old workers", "old-worker-schema")
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, created.ID)
	if err != nil {
		t.Fatalf("current attempt: %v", err)
	}
	var resources attemptResourceSnapshot
	if err := json.Unmarshal(attempt.ResourceSnapshot, &resources); err != nil {
		t.Fatalf("decode resource snapshot: %v", err)
	}
	type oldTaskManifest struct {
		TaskID             string               `json:"task_id"`
		Repository         string               `json:"repository,omitempty"`
		CloneURL           string               `json:"clone_url,omitempty"`
		BaseBranch         string               `json:"base_branch,omitempty"`
		TaskBranch         string               `json:"task_branch,omitempty"`
		Prompt             string               `json:"prompt"`
		Slack              protocol.SlackOrigin `json:"slack"`
		OpenCodeCommand    []string             `json:"opencode_command,omitempty"`
		ValidationCommand  []string             `json:"validation_command,omitempty"`
		ValidationCommands [][]string           `json:"validation_commands,omitempty"`
		MaxFixAttempts     int                  `json:"max_fix_attempts"`
	}
	decoder := json.NewDecoder(bytes.NewReader(resources.Secret.Data["task.json"]))
	decoder.DisallowUnknownFields()
	var manifest oldTaskManifest
	if err := decoder.Decode(&manifest); err != nil {
		t.Fatalf("strictly decode task.json with old worker schema: %v", err)
	}
}
