package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/task"
)

func TestRecordVerifiedPullRequestIsAtomicIdempotentAndConflictSafe(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "fix it"})
	if err != nil {
		t.Fatal(err)
	}
	git := GitResult{AttemptID: created.CurrentAttemptID, State: "pushed", Branch: "simpleswe/task-1", CommitSHA: "0123456789abcdef0123456789abcdef01234567"}
	pr := PullRequest{AttemptID: created.CurrentAttemptID, State: "open", Number: 42, URL: "https://github.example/acme/widget/pull/42", Title: "Actual provider title", HeadBranch: git.Branch, BaseBranch: "main"}
	setAttemptBranches(t, db, created.CurrentAttemptID, pr.BaseBranch, git.Branch)
	recordCandidateForTest(t, db, git, pr)

	for range 2 {
		if err := db.RecordVerifiedPullRequest(ctx, git, pr); err != nil {
			t.Fatalf("record/replay verified pull request: %v", err)
		}
	}
	gotGit, gitErr := db.GetGitResult(ctx, git.AttemptID)
	gotPR, prErr := db.GetPullRequest(ctx, pr.AttemptID)
	if gitErr != nil || prErr != nil || gotGit != git || gotPR != pr {
		t.Fatalf("durable verified result = %#v / %#v, errors %v / %v", gotGit, gotPR, gitErr, prErr)
	}

	changed := pr
	changed.Number = 43
	if err := db.RecordVerifiedPullRequest(ctx, git, changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed PR replay error = %v, want ErrConflict", err)
	}
	changedGit := git
	changedGit.CommitSHA = "1123456789abcdef0123456789abcdef01234567"
	if err := db.RecordVerifiedPullRequest(ctx, changedGit, pr); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed Git replay error = %v, want ErrConflict", err)
	}
	if after, err := db.GetPullRequest(ctx, pr.AttemptID); err != nil || after != pr {
		t.Fatalf("conflict replaced PR with %#v, error %v", after, err)
	}
}

func TestPullRequestCandidateCanAdvanceSHAAndFinalMustMatchLatest(t *testing.T) {
	db := openTestStore(t)
	created, err := db.CreateTask(t.Context(), CreateTaskParams{Repository: "repo", Prompt: "fix it"})
	if err != nil {
		t.Fatal(err)
	}
	branch := "simpleswe/candidate"
	setAttemptBranches(t, db, created.CurrentAttemptID, "main", branch)
	firstGit := GitResult{AttemptID: created.CurrentAttemptID, State: "candidate", Branch: branch, CommitSHA: strings.Repeat("1", 40)}
	reported := PullRequest{AttemptID: created.CurrentAttemptID, State: "reported", Number: 42, HeadBranch: branch, BaseBranch: "main"}
	for range 2 {
		if err := db.RecordPullRequestCandidate(t.Context(), firstGit, reported); err != nil {
			t.Fatalf("record/replay first candidate: %v", err)
		}
	}
	latestGit := firstGit
	latestGit.CommitSHA = strings.Repeat("2", 40)
	if err := db.RecordPullRequestCandidate(t.Context(), latestGit, reported); err != nil {
		t.Fatalf("replace candidate SHA: %v", err)
	}
	for name, mutate := range map[string]func(*PullRequest){
		"number": func(pr *PullRequest) { pr.Number++ },
		"head":   func(pr *PullRequest) { pr.HeadBranch = "simpleswe/other" },
		"base":   func(pr *PullRequest) { pr.BaseBranch = "release" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := reported
			mutate(&changed)
			changedGit := latestGit
			changedGit.Branch = changed.HeadBranch
			if err := db.RecordPullRequestCandidate(t.Context(), changedGit, changed); !errors.Is(err, ErrConflict) {
				t.Fatalf("changed candidate identity error = %v, want ErrConflict", err)
			}
		})
	}
	providerPR := PullRequest{AttemptID: reported.AttemptID, State: "open", Number: 42, URL: "https://github.example/acme/widget/pull/42", Title: "Provider title", HeadBranch: branch, BaseBranch: "main"}
	stale := firstGit
	stale.State = "pushed"
	if err := db.RecordVerifiedPullRequest(t.Context(), stale, providerPR); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale candidate promotion error = %v, want ErrConflict", err)
	}
	latestGit.State = "pushed"
	if err := db.RecordVerifiedPullRequest(t.Context(), latestGit, providerPR); err != nil {
		t.Fatalf("promote latest candidate: %v", err)
	}
	gotGit, gitErr := db.GetGitResult(t.Context(), latestGit.AttemptID)
	gotPR, prErr := db.GetPullRequest(t.Context(), latestGit.AttemptID)
	if gitErr != nil || prErr != nil || gotGit != latestGit || gotPR != providerPR {
		t.Fatalf("promoted candidate = %#v/%#v, errors %v/%v", gotGit, gotPR, gitErr, prErr)
	}
}

func TestRecordVerifiedPullRequestRequiresCandidate(t *testing.T) {
	db := openTestStore(t)
	created, err := db.CreateTask(t.Context(), CreateTaskParams{Repository: "repo", Prompt: "fix it"})
	if err != nil {
		t.Fatal(err)
	}
	git := GitResult{AttemptID: created.CurrentAttemptID, State: "pushed", Branch: "simpleswe/no-candidate", CommitSHA: strings.Repeat("a", 40)}
	pr := PullRequest{AttemptID: git.AttemptID, State: "open", Number: 42, URL: "https://github.example/acme/widget/pull/42", Title: "Provider title", HeadBranch: git.Branch, BaseBranch: "main"}
	setAttemptBranches(t, db, git.AttemptID, pr.BaseBranch, git.Branch)
	if err := db.RecordVerifiedPullRequest(t.Context(), git, pr); !errors.Is(err, ErrConflict) {
		t.Fatalf("promotion without candidate error = %v, want ErrConflict", err)
	}
}

func TestRecordVerifiedPullRequestNeverPersistsPartialRows(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "fix it"})
	if err != nil {
		t.Fatal(err)
	}
	git := GitResult{AttemptID: created.CurrentAttemptID, State: "pushed", Branch: "simpleswe/task-1", CommitSHA: "0123456789abcdef0123456789abcdef01234567"}
	invalid := PullRequest{AttemptID: created.CurrentAttemptID, State: "open", Number: 0, URL: "https://github.example/pull/0", Title: "invalid", HeadBranch: git.Branch, BaseBranch: "main"}
	if err := db.RecordVerifiedPullRequest(ctx, git, invalid); err == nil {
		t.Fatal("atomic operation accepted an invalid pull request")
	}
	if _, err := db.GetGitResult(ctx, git.AttemptID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed atomic operation persisted Git row: %v", err)
	}
	if _, err := db.GetPullRequest(ctx, git.AttemptID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed atomic operation persisted PR row: %v", err)
	}
}

func TestRecordVerifiedPullRequestRejectsInvalidOrConflictingPriorNumbers(t *testing.T) {
	for _, test := range []struct {
		name    string
		numbers []any
	}{
		{name: "null", numbers: []any{nil}},
		{name: "null mixed with valid", numbers: []any{nil, 42}},
		{name: "non-positive", numbers: []any{0}},
		{name: "conflicting", numbers: []any{42, 43}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db := openTestStore(t)
			created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "fix it"})
			if err != nil {
				t.Fatal(err)
			}
			const branch = "simpleswe/prior-identity"
			setAttemptBranches(t, db, created.CurrentAttemptID, "main", branch)
			for i, number := range test.numbers {
				attemptID := fmt.Sprintf("prior-attempt-%d", i)
				if _, err := db.db.ExecContext(ctx, `INSERT INTO task_attempts (id, task_id, number, immutable, state, base_branch, task_branch, created_at) VALUES (?, ?, ?, 1, ?, 'main', ?, ?)`, attemptID, created.ID, i+2, task.PR_OPEN, branch, stamp(time.Now().UTC())); err != nil {
					t.Fatal(err)
				}
				if _, err := db.db.ExecContext(ctx, `INSERT INTO pull_requests (attempt_id, state, number, url, title, head_branch, base_branch) VALUES (?, 'open', ?, 'https://github.example/acme/widget/pull/42', 'Provider title', ?, 'main')`, attemptID, number, branch); err != nil {
					t.Fatal(err)
				}
			}
			git := GitResult{AttemptID: created.CurrentAttemptID, State: "pushed", Branch: branch, CommitSHA: "0123456789abcdef0123456789abcdef01234567"}
			pullRequest := PullRequest{AttemptID: git.AttemptID, State: "open", Number: 42, URL: "https://github.example/acme/widget/pull/42", Title: "Provider title", HeadBranch: branch, BaseBranch: "main"}
			candidateGit, candidatePR := candidateResult(git, pullRequest)
			if err := db.RecordPullRequestCandidate(ctx, candidateGit, candidatePR); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "invalid or conflicting identity") {
				t.Fatalf("prior identity error = %v, want invalid/conflicting ErrConflict", err)
			}
		})
	}
}

func TestRecordVerifiedPullRequestFollowUpRequiresCopiedSamePullRequestAndBranches(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "fix it"})
	if err != nil {
		t.Fatal(err)
	}
	firstGit := GitResult{AttemptID: created.CurrentAttemptID, State: "pushed", Branch: "simpleswe/task-1", CommitSHA: "0123456789abcdef0123456789abcdef01234567"}
	firstPR := PullRequest{AttemptID: created.CurrentAttemptID, State: "open", Number: 42, URL: "https://github.example/pull/42", Title: "Provider title", HeadBranch: firstGit.Branch, BaseBranch: "main"}
	setAttemptBranches(t, db, created.CurrentAttemptID, firstPR.BaseBranch, firstGit.Branch)
	recordCandidateForTest(t, db, firstGit, firstPR)
	if err := db.RecordVerifiedPullRequest(ctx, firstGit, firstPR); err != nil {
		t.Fatal(err)
	}

	const followUpID = "attempt-follow-up"
	now := stamp(time.Now().UTC())
	if _, err := db.db.ExecContext(ctx, `INSERT INTO task_attempts (id, task_id, number, immutable, state, prompt, base_branch, task_branch, created_at) VALUES (?, ?, 2, 1, ?, 'review', 'main', ?, ?)`, followUpID, created.ID, task.VALIDATING, firstGit.Branch, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `INSERT INTO pull_requests (attempt_id, state, number, url, title, head_branch, base_branch) VALUES (?, 'open', ?, ?, ?, ?, ?)`, followUpID, firstPR.Number, firstPR.URL, firstPR.Title, firstPR.HeadBranch, firstPR.BaseBranch); err != nil {
		t.Fatal(err)
	}
	followGit := firstGit
	followGit.AttemptID, followGit.CommitSHA = followUpID, "1123456789abcdef0123456789abcdef01234567"
	followPR := firstPR
	followPR.AttemptID = followUpID
	recordCandidateForTest(t, db, followGit, followPR)
	if err := db.RecordVerifiedPullRequest(ctx, followGit, followPR); err != nil {
		t.Fatalf("record matching follow-up: %v", err)
	}

	const mismatchedID = "attempt-mismatched-follow-up"
	if _, err := db.db.ExecContext(ctx, `INSERT INTO task_attempts (id, task_id, number, immutable, state, prompt, base_branch, task_branch, created_at) VALUES (?, ?, 3, 1, ?, 'review', 'main', ?, ?)`, mismatchedID, created.ID, task.VALIDATING, firstGit.Branch, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `INSERT INTO pull_requests (attempt_id, state, number, url, title, head_branch, base_branch) VALUES (?, 'open', ?, ?, ?, ?, ?)`, mismatchedID, firstPR.Number, firstPR.URL, firstPR.Title, firstPR.HeadBranch, firstPR.BaseBranch); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*PullRequest){
		"number": func(pr *PullRequest) { pr.Number++ },
		"head":   func(pr *PullRequest) { pr.HeadBranch = "simpleswe/other" },
		"base":   func(pr *PullRequest) { pr.BaseBranch = "release" },
	} {
		t.Run(name, func(t *testing.T) {
			candidateGit, candidatePR := followGit, firstPR
			candidateGit.AttemptID, candidatePR.AttemptID = mismatchedID, mismatchedID
			mutate(&candidatePR)
			candidateGit, candidatePR = candidateResult(candidateGit, candidatePR)
			if err := db.RecordPullRequestCandidate(ctx, candidateGit, candidatePR); !errors.Is(err, ErrConflict) {
				t.Fatalf("mismatched follow-up error = %v, want ErrConflict", err)
			}
			if _, err := db.GetGitResult(ctx, mismatchedID); !errors.Is(err, ErrNotFound) {
				t.Fatalf("mismatched follow-up persisted Git row: %v", err)
			}
		})
	}
}

func setAttemptBranches(t *testing.T, db *Store, attemptID, base, branch string) {
	t.Helper()
	if _, err := db.db.Exec(`UPDATE task_attempts SET base_branch = ?, task_branch = ? WHERE id = ?`, base, branch, attemptID); err != nil {
		t.Fatalf("set attempt branch identity: %v", err)
	}
}

func TestRecordVerifiedPullRequestRequiresCopiedFollowUpAndImmutableBranches(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "fix it"})
	if err != nil {
		t.Fatal(err)
	}
	firstGit := GitResult{AttemptID: created.CurrentAttemptID, State: "pushed", Branch: "simpleswe/task-a1", CommitSHA: "0123456789abcdef0123456789abcdef01234567"}
	firstPR := PullRequest{AttemptID: firstGit.AttemptID, State: "open", Number: 42, URL: "https://github.example/acme/widget/pull/42", Title: "Provider title", HeadBranch: firstGit.Branch, BaseBranch: "main"}
	setAttemptBranches(t, db, firstGit.AttemptID, firstPR.BaseBranch, firstGit.Branch)
	recordCandidateForTest(t, db, firstGit, firstPR)
	if err := db.RecordVerifiedPullRequest(ctx, firstGit, firstPR); err != nil {
		t.Fatal(err)
	}

	now := stamp(time.Now().UTC())
	const missingCopy = "attempt-missing-copy"
	if _, err := db.db.ExecContext(ctx, `INSERT INTO task_attempts (id, task_id, number, immutable, state, prompt, base_branch, task_branch, created_at) VALUES (?, ?, 2, 1, ?, 'review', 'main', ?, ?)`, missingCopy, created.ID, task.VALIDATING, firstGit.Branch, now); err != nil {
		t.Fatal(err)
	}
	followGit, followPR := firstGit, firstPR
	followGit.AttemptID, followGit.CommitSHA = missingCopy, "1123456789abcdef0123456789abcdef01234567"
	followPR.AttemptID = missingCopy
	candidateGit, candidatePR := candidateResult(followGit, followPR)
	if err := db.RecordPullRequestCandidate(ctx, candidateGit, candidatePR); !errors.Is(err, ErrConflict) {
		t.Fatalf("missing copied follow-up error = %v, want ErrConflict", err)
	}

	const mismatch = "attempt-branch-mismatch"
	if _, err := db.db.ExecContext(ctx, `INSERT INTO task_attempts (id, task_id, number, immutable, state, prompt, base_branch, task_branch, created_at) VALUES (?, ?, 3, 1, ?, 'retry', 'main', 'simpleswe/task-a3', ?)`, mismatch, created.ID, task.VALIDATING, now); err != nil {
		t.Fatal(err)
	}
	mismatchGit := GitResult{AttemptID: mismatch, State: "pushed", Branch: "simpleswe/wrong", CommitSHA: "2123456789abcdef0123456789abcdef01234567"}
	mismatchPR := PullRequest{AttemptID: mismatch, State: "open", Number: 43, URL: "https://github.example/acme/widget/pull/43", Title: "Retry", HeadBranch: mismatchGit.Branch, BaseBranch: "main"}
	candidateGit, candidatePR = candidateResult(mismatchGit, mismatchPR)
	if err := db.RecordPullRequestCandidate(ctx, candidateGit, candidatePR); !errors.Is(err, ErrConflict) {
		t.Fatalf("attempt branch mismatch error = %v, want ErrConflict", err)
	}

	const ordinaryRetry = "attempt-ordinary-retry"
	if _, err := db.db.ExecContext(ctx, `INSERT INTO task_attempts (id, task_id, number, immutable, state, prompt, base_branch, task_branch, created_at) VALUES (?, ?, 4, 1, ?, 'retry', 'main', 'simpleswe/task-a4', ?)`, ordinaryRetry, created.ID, task.VALIDATING, now); err != nil {
		t.Fatal(err)
	}
	retryGit := GitResult{AttemptID: ordinaryRetry, State: "pushed", Branch: "simpleswe/task-a4", CommitSHA: "3123456789abcdef0123456789abcdef01234567"}
	retryPR := PullRequest{AttemptID: ordinaryRetry, State: "open", Number: 44, URL: "https://github.example/acme/widget/pull/44", Title: "Retry", HeadBranch: retryGit.Branch, BaseBranch: "main"}
	recordCandidateForTest(t, db, retryGit, retryPR)
	if err := db.RecordVerifiedPullRequest(ctx, retryGit, retryPR); err != nil {
		t.Fatalf("ordinary retry with new deterministic branch: %v", err)
	}
}

func TestRecordVerifiedPullRequestDecodesManifestBranchIdentity(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "fix it"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE task_attempts SET manifest_json = ? WHERE id = ?`, []byte(`{"base_branch":"main","task_branch":"simpleswe/from-manifest"}`), created.CurrentAttemptID); err != nil {
		t.Fatal(err)
	}
	git := GitResult{AttemptID: created.CurrentAttemptID, State: "pushed", Branch: "simpleswe/from-manifest", CommitSHA: "0123456789abcdef0123456789abcdef01234567"}
	pr := PullRequest{AttemptID: git.AttemptID, State: "open", Number: 42, URL: "https://github.example/acme/widget/pull/42", Title: "Provider title", HeadBranch: git.Branch, BaseBranch: "main"}
	recordCandidateForTest(t, db, git, pr)
	if err := db.RecordVerifiedPullRequest(ctx, git, pr); err != nil {
		t.Fatalf("manifest branch identity: %v", err)
	}
}

func TestRecordVerifiedPullRequestConcurrentIdenticalAndConflicting(t *testing.T) {
	for _, conflicting := range []bool{false, true} {
		name := "identical"
		if conflicting {
			name = "conflicting"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := openTestStore(t)
			created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "fix it"})
			if err != nil {
				t.Fatal(err)
			}
			git := GitResult{AttemptID: created.CurrentAttemptID, State: "pushed", Branch: "simpleswe/concurrent", CommitSHA: "0123456789abcdef0123456789abcdef01234567"}
			first := PullRequest{AttemptID: git.AttemptID, State: "open", Number: 42, URL: "https://github.example/acme/widget/pull/42", Title: "Provider title", HeadBranch: git.Branch, BaseBranch: "main"}
			second := first
			if conflicting {
				second.Number, second.URL = 43, "https://github.example/acme/widget/pull/43"
			}
			setAttemptBranches(t, db, git.AttemptID, first.BaseBranch, git.Branch)
			recordCandidateForTest(t, db, git, first)
			results := make(chan error, 2)
			var wg sync.WaitGroup
			for _, pullRequest := range []PullRequest{first, second} {
				wg.Go(func() {
					results <- db.RecordVerifiedPullRequest(ctx, git, pullRequest)
				})
			}
			wg.Wait()
			close(results)
			succeeded, conflicts := 0, 0
			for err := range results {
				switch {
				case err == nil:
					succeeded++
				case errors.Is(err, ErrConflict):
					conflicts++
				default:
					t.Fatalf("concurrent result: %v", err)
				}
			}
			if conflicting && (succeeded != 1 || conflicts != 1) || !conflicting && succeeded != 2 {
				t.Fatalf("concurrent results succeeded/conflicts = %d/%d", succeeded, conflicts)
			}
		})
	}
}

func TestLatestVerifiedPullRequestGitResultStaysWithinExactLineage(t *testing.T) {
	db := openTestStore(t)
	record, original, pullRequest := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(t.Context(), record.ID, original.ID); err != nil {
		t.Fatal(err)
	}
	event := testForgeEvent("latest-lineage", "review_comment")
	if _, err := db.PutForgeEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	followUp, _, err := startForgeEventAttemptForTest(t.Context(), db, event.ID, record.ID, "follow up")
	if err != nil {
		t.Fatal(err)
	}
	latest := GitResult{AttemptID: followUp.ID, State: "pushed", Branch: pullRequest.HeadBranch, CommitSHA: "1123456789abcdef0123456789abcdef01234567"}
	copied := pullRequest
	copied.AttemptID = followUp.ID
	recordCandidateForTest(t, db, latest, copied)
	if err := db.RecordVerifiedPullRequest(t.Context(), latest, copied); err != nil {
		t.Fatal(err)
	}

	got, err := db.LatestVerifiedPullRequestGitResult(t.Context(), record.ID, copied, "")
	if err != nil || got != latest {
		t.Fatalf("latest exact-lineage Git result = %#v, %v; want %#v", got, err, latest)
	}
	prior, err := db.LatestVerifiedPullRequestGitResult(t.Context(), record.ID, copied, followUp.ID)
	if err != nil || prior.AttemptID != original.ID || prior.CommitSHA != "0123456789abcdef0123456789abcdef01234567" {
		t.Fatalf("excluded latest Git result = %#v, %v; want original", prior, err)
	}
	wrong := copied
	wrong.Number++
	if _, err := db.LatestVerifiedPullRequestGitResult(t.Context(), record.ID, wrong, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong-number lineage error = %v, want ErrNotFound", err)
	}
	wrong = copied
	wrong.BaseBranch = "release"
	if _, err := db.LatestVerifiedPullRequestGitResult(t.Context(), record.ID, wrong, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong-base lineage error = %v, want ErrNotFound", err)
	}
}

func candidateResult(git GitResult, pullRequest PullRequest) (GitResult, PullRequest) {
	git.State = "candidate"
	pullRequest.State, pullRequest.URL, pullRequest.Title = "reported", "", ""
	return git, pullRequest
}

func recordCandidateForTest(t *testing.T, db *Store, git GitResult, pullRequest PullRequest) {
	t.Helper()
	candidateGit, candidatePR := candidateResult(git, pullRequest)
	if err := db.RecordPullRequestCandidate(t.Context(), candidateGit, candidatePR); err != nil {
		t.Fatalf("record pull request candidate: %v", err)
	}
}
