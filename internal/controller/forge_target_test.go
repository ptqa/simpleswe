package controller

import (
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
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventPullRequestPublished, TaskID: created.ID, PullRequestNumber: 42, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA})
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
	changed := fixture.config
	changed.Repositories[0].Bitbucket = config.RepositoryBitbucketConfig{}
	changed.Repositories[0].GitHub = config.RepositoryGitHubConfig{Owner: "new-owner", Repository: "new-repository", CredentialsSecret: "new-github"}
	changed.GitHub.BaseURL = "https://github.enterprise.example/api/v3"
	restarted, err := New(fixture.store, fixture.kube, changed, fixture.pullRequests)
	if err != nil {
		t.Fatalf("restart controller: %v", err)
	}
	fixture.pullRequests.getResult = &forge.PullRequestState{Number: 42, State: "open", HTMLURL: pullRequestURL, Title: "Provider title", SourceOwner: "acme", SourceRepository: "widget", SourceBranch: branch, DestinationBranch: "main", HeadSHA: fullCommitSHA}
	if err := restarted.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: protocol.EventPullRequestReady, TaskID: created.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: fullCommitSHA}); err != nil {
		t.Fatalf("HandleWorkerEvent: %v", err)
	}
	want := wantTarget
	if len(fixture.pullRequests.getTargets) != 1 || fixture.pullRequests.getTargets[0] != want {
		t.Fatalf("get targets = %#v; want %#v", fixture.pullRequests.getTargets, want)
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
			if _, err := control.attemptForgeTarget(record, store.Attempt{ID: name, ResourceSnapshot: []byte(snapshot)}); err == nil || !forge.IsPermanent(err) {
				t.Fatal("invalid snapshotted target was accepted")
			}
		})
	}
}
