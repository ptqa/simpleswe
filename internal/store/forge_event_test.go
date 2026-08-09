package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/task"
)

func TestForgeEventInboxPersistsAcceptedOrderAndDeduplicatesByID(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()
	first := testForgeEvent("forge-event-1", "review_comment")
	stored, err := db.PutForgeEvent(ctx, first)
	if err != nil {
		t.Fatalf("put forge event: %v", err)
	}
	if stored.Status != "pending" || stored.Attempts != 0 || stored.CreatedAt.IsZero() || stored.UpdatedAt.IsZero() {
		t.Fatalf("new forge event = %#v; want pending with timestamps", stored)
	}

	changed := first
	changed.Body = "replacement feedback"
	changed.Author = "other-reviewer"
	duplicate, err := db.PutForgeEvent(ctx, changed)
	if err != nil {
		t.Fatalf("put duplicate forge event: %v", err)
	}
	if duplicate.Body != first.Body || duplicate.Author != first.Author || duplicate.CreatedAt != stored.CreatedAt {
		t.Fatalf("duplicate replaced durable forge event: %#v", duplicate)
	}

	second := testForgeEvent("forge-event-2", "quality_gate_failed")
	second.CommentID, second.CommentKind = 0, ""
	second.Title, second.Body = "go test ./...", "TestWidget failed"
	if _, err := db.PutForgeEvent(ctx, second); err != nil {
		t.Fatalf("put quality-gate event: %v", err)
	}
	if err := db.RecordForgeEventError(ctx, second.ID, errors.New("temporary provider failure")); err != nil {
		t.Fatalf("record forge event error: %v", err)
	}

	incomplete, err := db.ListIncompleteForgeEvents(ctx)
	if err != nil {
		t.Fatalf("list incomplete forge events: %v", err)
	}
	if len(incomplete) != 1 || incomplete[0].ID != first.ID {
		t.Fatalf("due forge events = %#v; want only first event", incomplete)
	}
	deferred, err := db.GetForgeEvent(ctx, second.ID)
	if err != nil || deferred.Attempts != 1 || deferred.LastError != "temporary provider failure" || deferred.NextAttemptAt == nil {
		t.Fatalf("errored forge event = %#v, %v; want one deferred processing attempt", deferred, err)
	}
	got, err := db.GetForgeEvent(ctx, first.ID)
	if err != nil {
		t.Fatalf("get forge event: %v", err)
	}
	if !reflect.DeepEqual(got, stored) {
		t.Fatalf("get forge event = %#v; want %#v", got, stored)
	}
}

func TestForgeEventBatchSchedulingUsesSlidingQuietPeriod(t *testing.T) {
	db := openTestStore(t)
	ctx := t.Context()
	const quietPeriod = 30 * time.Minute

	first := testForgeEvent("batch-first", "review_comment")
	started := time.Now().UTC()
	stored, err := db.PutForgeEventAfter(ctx, first, quietPeriod)
	if err != nil {
		t.Fatalf("put first review event: %v", err)
	}
	assertForgeEventDeadline(t, stored, started.Add(quietPeriod))
	initialDeadline := *stored.NextAttemptAt

	matching := first
	matching.ID = "batch-matching"
	matching.CommentID = 502
	matching.Body = "Also preserve escaped commas"
	matching.URL = "https://bitbucket.example/acme/widget/pull-requests/42#comment-502"
	matchingStored, err := db.PutForgeEventAfter(ctx, matching, quietPeriod)
	if err != nil {
		t.Fatalf("put matching review event: %v", err)
	}
	refreshed, err := db.GetForgeEvent(ctx, first.ID)
	if err != nil {
		t.Fatalf("reload first review event: %v", err)
	}
	if refreshed.NextAttemptAt == nil || !refreshed.NextAttemptAt.After(initialDeadline) {
		t.Fatalf("first review deadline = %v; want sliding deadline after %v", refreshed.NextAttemptAt, initialDeadline)
	}
	if matchingStored.NextAttemptAt == nil || !matchingStored.NextAttemptAt.Equal(*refreshed.NextAttemptAt) {
		t.Fatalf("matching review deadline = %v; want %v", matchingStored.NextAttemptAt, refreshed.NextAttemptAt)
	}
	assertForgeEventDeadline(t, refreshed, time.Now().UTC().Add(quietPeriod))

	differentSHA := first
	differentSHA.ID = "batch-different-sha"
	differentSHA.CommitSHA = "fedcba9876543210fedcba9876543210fedcba98"
	differentSHA.CommentID = 503
	differentSHA.URL = "https://bitbucket.example/acme/widget/pull-requests/42#comment-503"
	if _, err := db.PutForgeEventAfter(ctx, differentSHA, quietPeriod); err != nil {
		t.Fatalf("put different-SHA review event: %v", err)
	}
	unchanged, err := db.GetForgeEvent(ctx, first.ID)
	if err != nil {
		t.Fatalf("reload first review event after different SHA: %v", err)
	}
	if unchanged.NextAttemptAt == nil || !unchanged.NextAttemptAt.Equal(*refreshed.NextAttemptAt) {
		t.Fatalf("different SHA moved first deadline from %v to %v", refreshed.NextAttemptAt, unchanged.NextAttemptAt)
	}

	if _, err := db.PutForgeEventAfter(ctx, first, quietPeriod); err != nil {
		t.Fatalf("put duplicate review event: %v", err)
	}
	unchanged, err = db.GetForgeEvent(ctx, first.ID)
	if err != nil {
		t.Fatalf("reload first review event after duplicate: %v", err)
	}
	if unchanged.NextAttemptAt == nil || !unchanged.NextAttemptAt.Equal(*refreshed.NextAttemptAt) {
		t.Fatalf("duplicate moved first deadline from %v to %v", refreshed.NextAttemptAt, unchanged.NextAttemptAt)
	}

	quality := testForgeEvent("batch-quality", "quality_gate_failed")
	qualityStored, err := db.PutForgeEventAfter(ctx, quality, quietPeriod)
	if err != nil {
		t.Fatalf("put quality-gate event: %v", err)
	}
	if qualityStored.NextAttemptAt != nil && qualityStored.NextAttemptAt.After(time.Now().UTC().Add(100*time.Millisecond)) {
		t.Fatalf("quality-gate deadline = %v; want immediate", qualityStored.NextAttemptAt)
	}
	due, err := db.ListIncompleteForgeEvents(ctx)
	if err != nil {
		t.Fatalf("list immediately due events: %v", err)
	}
	for _, event := range due {
		if event.ID == first.ID {
			t.Fatalf("sliding review event is immediately due: %#v", event)
		}
	}
}

func TestSHALessReviewEventsKeepIndependentDeadlinesAndBatches(t *testing.T) {
	db := openTestStore(t)
	ctx := t.Context()
	record, original, _ := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(ctx, record.ID, original.ID); err != nil {
		t.Fatal(err)
	}
	const quietPeriod = 30 * time.Minute

	first := testForgeEvent("sha-less-first", "review_comment")
	first.Provider, first.CommentKind, first.CommitSHA, first.Branch = "github", "issue_comment", "", ""
	first.URL = "https://github.example/acme/widget/issues/42#issuecomment-501"
	if _, err := db.PutForgeEventAfter(ctx, first, quietPeriod); err != nil {
		t.Fatal(err)
	}
	firstDeadline := time.Now().UTC().Add(10 * time.Minute)
	if _, err := db.db.ExecContext(ctx, `UPDATE forge_events SET next_attempt_at = ? WHERE id = ?`, stamp(firstDeadline), first.ID); err != nil {
		t.Fatal(err)
	}

	second := first
	second.ID, second.CommentID = "sha-less-second", 502
	second.URL = "https://github.example/acme/widget/issues/42#issuecomment-502"
	secondStored, err := db.PutForgeEventAfter(ctx, second, quietPeriod)
	if err != nil {
		t.Fatal(err)
	}
	firstStored, err := db.GetForgeEvent(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if firstStored.NextAttemptAt == nil || !firstStored.NextAttemptAt.Equal(firstDeadline) {
		t.Fatalf("first SHA-less deadline = %v; want unchanged %v", firstStored.NextAttemptAt, firstDeadline)
	}
	assertForgeEventDeadline(t, secondStored, time.Now().UTC().Add(quietPeriod))

	for i, event := range []ForgeEvent{first, second} {
		if _, err := db.db.ExecContext(ctx, `UPDATE forge_events SET next_attempt_at = ? WHERE id = ?`, stamp(time.Now().UTC().Add(-time.Duration(i+1)*time.Minute)), event.ID); err != nil {
			t.Fatal(err)
		}
		seed, err := db.GetForgeEvent(ctx, event.ID)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := db.ListDueForgeEventBatch(ctx, seed)
		if err != nil || len(batch) != 1 || batch[0].ID != event.ID {
			t.Fatalf("SHA-less batch for %q = %#v, %v; want only its seed", event.ID, batch, err)
		}
	}
	if _, err := db.PlanForgeEventAttempt(ctx, []string{first.ID, second.ID}, record.ID, "must not combine SHA-less comments"); !errors.Is(err, ErrConflict) {
		t.Fatalf("combined SHA-less batch error = %v; want ErrConflict", err)
	}
	if _, err := db.PlanForgeEventAttempt(ctx, []string{first.ID}, record.ID, "process one SHA-less comment"); err != nil {
		t.Fatalf("single SHA-less batch plan: %v", err)
	}
}

func TestForgeEventBatchAssociatesManyEventsAndLeavesLaterCommentPending(t *testing.T) {
	db := openTestStore(t)
	ctx := t.Context()
	record, original, _ := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(ctx, record.ID, original.ID); err != nil {
		t.Fatalf("mark original logs exhausted: %v", err)
	}

	first := testForgeEvent("batch-claim-first", "review_comment")
	second := first
	second.ID = "batch-claim-second"
	second.CommentID = 502
	second.Body = "Please also cover the empty input"
	second.URL = "https://bitbucket.example/acme/widget/pull-requests/42#comment-502"
	for _, event := range []ForgeEvent{first, second} {
		stored, err := db.PutForgeEventAfter(ctx, event, 0)
		if err != nil {
			t.Fatalf("put due review event %q: %v", event.ID, err)
		}
		if stored.NextAttemptAt != nil && stored.NextAttemptAt.After(time.Now().UTC().Add(100*time.Millisecond)) {
			t.Fatalf("due review event %q deadline = %v", event.ID, stored.NextAttemptAt)
		}
	}

	attempt, inserted, err := startForgeEventBatchForTest(ctx, db, []string{first.ID, second.ID}, record.ID, "fix all review feedback")
	if err != nil {
		t.Fatalf("start batched forge follow-up: %v", err)
	}
	if !inserted {
		t.Fatal("batched forge follow-up was reported as a replay")
	}
	associated, err := db.ListForgeEventsByAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatalf("list batched forge events: %v", err)
	}
	if len(associated) != 2 {
		t.Fatalf("batched forge events = %#v; want two events", associated)
	}
	for _, event := range associated {
		if event.Status != ForgeEventRunning || event.TaskID != record.ID || event.AttemptID != attempt.ID || event.Attempts != 1 {
			t.Fatalf("associated forge event = %#v; want running on one attempt", event)
		}
	}

	third := first
	third.ID = "batch-claim-later"
	third.CommentID = 503
	third.Body = "A comment after the claim"
	third.URL = "https://bitbucket.example/acme/widget/pull-requests/42#comment-503"
	if _, err := db.PutForgeEventAfter(ctx, third, 0); err != nil {
		t.Fatalf("put post-claim review event: %v", err)
	}
	pending, err := db.GetForgeEvent(ctx, third.ID)
	if err != nil {
		t.Fatalf("get post-claim review event: %v", err)
	}
	if pending.Status != ForgeEventPending || pending.TaskID != "" || pending.AttemptID != "" {
		t.Fatalf("post-claim review event = %#v; want unclaimed pending event", pending)
	}
	associated, err = db.ListForgeEventsByAttempt(ctx, attempt.ID)
	if err != nil || len(associated) != 2 {
		t.Fatalf("batched forge events after later comment = %#v, %v; want original two", associated, err)
	}
	attempts, err := db.ListAttempts(ctx, record.ID)
	if err != nil || len(attempts) != 2 {
		t.Fatalf("task attempts after batched claim = %#v, %v; want one follow-up attempt", attempts, err)
	}
}

func TestForgeEventBatchClaimsOldest32AndLeaves33rdPending(t *testing.T) {
	db := openTestStore(t)
	ctx := t.Context()
	record, original, _ := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(ctx, record.ID, original.ID); err != nil {
		t.Fatalf("mark original logs exhausted: %v", err)
	}

	eventIDs := make([]string, 33)
	for i := range eventIDs {
		event := testForgeEvent(fmt.Sprintf("batch-limit-%02d", i+1), "review_comment")
		event.CommentID = 501 + i
		event.URL = fmt.Sprintf("https://bitbucket.example/acme/widget/pull-requests/42#comment-%d", event.CommentID)
		if _, err := db.PutForgeEventAfter(ctx, event, 0); err != nil {
			t.Fatalf("put review event %d: %v", i+1, err)
		}
		eventIDs[i] = event.ID
	}

	seed, err := db.GetForgeEvent(ctx, eventIDs[0])
	if err != nil {
		t.Fatalf("get batch seed: %v", err)
	}
	batch, err := db.ListDueForgeEventBatch(ctx, seed)
	if err != nil {
		t.Fatalf("list due review batch: %v", err)
	}
	if len(batch) != 32 {
		t.Fatalf("due review batch size = %d; want 32", len(batch))
	}
	claimedIDs := make([]string, len(batch))
	for i, event := range batch {
		claimedIDs[i] = event.ID
	}
	if !reflect.DeepEqual(claimedIDs, eventIDs[:32]) {
		t.Fatalf("claimed event IDs = %#v; want oldest %#v", claimedIDs, eventIDs[:32])
	}
	if _, _, err := startForgeEventBatchForTest(ctx, db, claimedIDs, record.ID, "fix the oldest review batch"); err != nil {
		t.Fatalf("claim oldest review batch: %v", err)
	}
	remaining, err := db.GetForgeEvent(ctx, eventIDs[32])
	if err != nil {
		t.Fatalf("get remaining review event: %v", err)
	}
	if remaining.Status != ForgeEventPending || remaining.TaskID != "" || remaining.AttemptID != "" {
		t.Fatalf("33rd review event = %#v; want pending for the next batch", remaining)
	}
}

func TestStartForgeEventAttemptRejectsBatchSlidAfterPlanning(t *testing.T) {
	db := openTestStore(t)
	record, original, _ := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(t.Context(), record.ID, original.ID); err != nil {
		t.Fatal(err)
	}
	first := testForgeEvent("planned-due-review", "review_comment")
	if _, err := db.PutForgeEventAfter(t.Context(), first, 0); err != nil {
		t.Fatal(err)
	}
	plan, err := db.PlanForgeEventAttempt(t.Context(), []string{first.ID}, record.ID, "fix due review")
	if err != nil {
		t.Fatal(err)
	}
	plan.Attempt.ManifestJSON = []byte(`{"task_id":"planned"}`)
	plan.Attempt.ResourceSnapshot = []byte(`{"job":{"metadata":{"name":"planned"}},"secret":{"metadata":{"name":"planned"}}}`)
	plan.Attempt.ConfigDigest = "sha256:planned"

	delayed := first
	delayed.ID, delayed.CommentID = "new-delayed-review", 502
	if _, err := db.PutForgeEventAfter(t.Context(), delayed, 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.StartForgeEventAttempt(t.Context(), plan); !errors.Is(err, ErrForgeEventNotDue) {
		t.Fatalf("StartForgeEventAttempt() error = %v, want ErrForgeEventNotDue", err)
	}
	for _, id := range []string{first.ID, delayed.ID} {
		stored, err := db.GetForgeEvent(t.Context(), id)
		if err != nil || stored.Status != ForgeEventPending || stored.TaskID != "" || stored.AttemptID != "" || stored.Attempts != 0 {
			t.Fatalf("event %q after refused start = %#v, %v", id, stored, err)
		}
	}
	attempts, err := db.ListAttempts(t.Context(), record.ID)
	if err != nil || len(attempts) != 1 || attempts[0].ID != original.ID {
		t.Fatalf("attempts after refused start = %#v, %v", attempts, err)
	}
}

func TestForgeEventQualityIdentityMayBeCommitOnly(t *testing.T) {
	db := openTestStore(t)
	event := testForgeEvent("branchless-quality", "quality_gate_failed")
	event.PullRequestNumber, event.Branch, event.URL = 0, "", ""
	event.CommentID, event.CommentKind = 0, ""
	if _, err := db.PutForgeEvent(context.Background(), event); err != nil {
		t.Fatalf("put commit-only quality event: %v", err)
	}

	event.ID, event.CommitSHA = "identity-less-quality", ""
	if _, err := db.PutForgeEvent(context.Background(), event); err == nil {
		t.Fatal("PutForgeEvent accepted quality event without PR, branch, or commit identity")
	}
}

func TestForgeEventAcceptsBitbucketChangesRequestSubtype(t *testing.T) {
	db := openTestStore(t)
	event := testForgeEvent("bitbucket:request-changes", "review_comment")
	event.CommentID, event.CommentKind = 0, "changes_request"
	stored, err := db.PutForgeEvent(t.Context(), event)
	if err != nil {
		t.Fatalf("put Bitbucket changes-request event: %v", err)
	}
	if stored.CommentID != 0 || stored.CommentKind != "changes_request" {
		t.Fatalf("stored Bitbucket changes-request comment identity = %d/%q; want 0/changes_request", stored.CommentID, stored.CommentKind)
	}
}

func TestPutForgeEventRejectsMalformedDurableEvents(t *testing.T) {
	db := openTestStore(t)
	tests := map[string]func(*ForgeEvent){
		"unsupported provider":          func(event *ForgeEvent) { event.Provider = "gitlab" },
		"unsupported kind":              func(event *ForgeEvent) { event.Kind = "push" },
		"GitHub Bitbucket comment kind": func(event *ForgeEvent) { event.Provider, event.CommentKind = "github", "comment" },
		"Bitbucket GitHub comment kind": func(event *ForgeEvent) { event.CommentKind = "issue_comment" },
		"Bitbucket empty review subtype": func(event *ForgeEvent) {
			event.CommentID, event.CommentKind = 0, ""
		},
		"Bitbucket comment subtype without ID": func(event *ForgeEvent) {
			event.CommentID, event.CommentKind = 0, "comment"
		},
		"Bitbucket comment ID without subtype": func(event *ForgeEvent) { event.CommentKind = "" },
		"Bitbucket changes request with comment ID": func(event *ForgeEvent) {
			event.CommentKind = "changes_request"
		},
		"GitHub Bitbucket changes request": func(event *ForgeEvent) {
			event.Provider, event.CommentID, event.CommentKind = "github", 0, "changes_request"
		},
		"quality with comment identity": func(event *ForgeEvent) { event.Kind = "quality_gate_failed" },
		"quality without SHA": func(event *ForgeEvent) {
			event.Kind, event.CommentID, event.CommentKind, event.CommitSHA = "quality_gate_failed", 0, "", ""
		},
		"control in owner":   func(event *ForgeEvent) { event.Owner = "acme\x00org" },
		"control in body":    func(event *ForgeEvent) { event.Body = "fix\x00this" },
		"oversized identity": func(event *ForgeEvent) { event.Author = strings.Repeat("a", 128*1024+1) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			event := testForgeEvent("invalid-"+name, "review_comment")
			mutate(&event)
			if _, err := db.PutForgeEvent(t.Context(), event); err == nil {
				t.Fatal("PutForgeEvent accepted malformed durable event")
			}
		})
	}
}

func TestForgeEventTransientFailureDefersOnlyFailedEvent(t *testing.T) {
	db := openTestStore(t)
	deferred := testForgeEvent("deferred-event", "quality_gate_failed")
	deferred.CommentID, deferred.CommentKind = 0, ""
	due := testForgeEvent("due-event", "review_comment")
	if _, err := db.PutForgeEvent(t.Context(), deferred); err != nil {
		t.Fatal(err)
	}
	before := time.Now().UTC()
	if err := db.RecordForgeEventErrorAfter(t.Context(), deferred.ID, errors.New("temporary outage"), 48*time.Hour); err != nil {
		t.Fatal(err)
	}
	after := time.Now().UTC()
	if _, err := db.PutForgeEvent(t.Context(), due); err != nil {
		t.Fatal(err)
	}

	listed, err := db.ListIncompleteForgeEvents(t.Context())
	if err != nil || len(listed) != 1 || listed[0].ID != due.ID {
		t.Fatalf("due forge events = %#v, %v; want only %q", listed, err, due.ID)
	}
	stored, err := db.GetForgeEvent(t.Context(), deferred.ID)
	if err != nil || stored.NextAttemptAt == nil || stored.NextAttemptAt.Before(before.Add(24*time.Hour)) || stored.NextAttemptAt.After(after.Add(24*time.Hour)) {
		t.Fatalf("deferred forge event = %#v, %v", stored, err)
	}
	if _, err := db.db.ExecContext(t.Context(), `UPDATE forge_events SET next_attempt_at = ? WHERE id = ?`, stamp(time.Now().Add(-time.Second)), deferred.ID); err != nil {
		t.Fatal(err)
	}
	listed, err = db.ListIncompleteForgeEvents(t.Context())
	if err != nil || len(listed) != 2 {
		t.Fatalf("due forge events after delay = %#v, %v", listed, err)
	}
}

func TestForgeEventBatchFailureIsExactBoundedAndReplaySafeAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "forge-batch-replay.sqlite")
	db, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	eventIDs := putPendingForgeEventBatch(t, db, "exact-failure", forgeEventBatchSize)
	cause := errors.New(strings.Repeat("permanent-diagnostic-", 300))
	before := time.Now().UTC()
	if err := db.FailForgeEventBatch(t.Context(), eventIDs, cause); err != nil {
		t.Fatal(err)
	}
	failed := readForgeEventBatch(t, db, eventIDs)
	for _, event := range failed {
		if event.Status != ForgeEventFailed || event.Attempts != 1 || event.TaskID != "" || event.AttemptID != "" || event.FailedAt == nil ||
			event.FailedAt.Before(before) || event.NextAttemptAt != nil || len(event.LastError) > forgeEventErrorMaxBytes ||
			event.LastError != failed[0].LastError || !event.FailedAt.Equal(*failed[0].FailedAt) || !event.UpdatedAt.Equal(failed[0].UpdatedAt) {
			t.Fatalf("failed exact batch member = %#v; first=%#v", event, failed[0])
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.FailForgeEventBatch(t.Context(), eventIDs, cause); err != nil {
		t.Fatalf("exact replay after restart: %v", err)
	}
	replayed := readForgeEventBatch(t, db, eventIDs)
	if !reflect.DeepEqual(replayed, failed) {
		t.Fatalf("exact replay rewrote diagnostics: before=%#v after=%#v", failed, replayed)
	}
	if err := db.FailForgeEventBatch(t.Context(), eventIDs[:31], cause); !errors.Is(err, ErrConflict) {
		t.Fatalf("subset replay = %v, want ErrConflict", err)
	}
	if err := db.FailForgeEventBatch(t.Context(), eventIDs, errors.New("different permanent diagnostic")); !errors.Is(err, ErrConflict) {
		t.Fatalf("different replay = %v, want ErrConflict", err)
	}
	if due, err := db.ListIncompleteForgeEvents(t.Context()); err != nil || len(due) != 0 {
		t.Fatalf("failed siblings regrouped after restart: %#v, %v", due, err)
	}
}

func TestForgeEventBatchDeferralIsExactCommonAndReplaySafe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "forge-batch-deferral.sqlite")
	db, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	eventIDs := putPendingForgeEventBatch(t, db, "exact-deferral", forgeEventBatchSize)
	if _, err := db.db.ExecContext(t.Context(), `UPDATE forge_events SET attempts = 2 WHERE id = ?`, eventIDs[17]); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("temporary batch outage")
	if err := db.RecordForgeEventBatchErrorAfter(t.Context(), eventIDs, cause, time.Second); err != nil {
		t.Fatal(err)
	}
	deferred := readForgeEventBatch(t, db, eventIDs)
	for i, event := range deferred {
		wantAttempts := 1
		if i == 17 {
			wantAttempts = 3
		}
		if event.Status != ForgeEventPending || event.Attempts != wantAttempts || event.LastError != cause.Error() || event.NextAttemptAt == nil ||
			!event.NextAttemptAt.Equal(*deferred[0].NextAttemptAt) || !event.UpdatedAt.Equal(deferred[0].UpdatedAt) {
			t.Fatalf("deferred exact batch member %d = %#v; first=%#v", i, event, deferred[0])
		}
	}
	if got := deferred[0].NextAttemptAt.Sub(deferred[0].UpdatedAt); got != forgeEventRetryDelay(2) {
		t.Fatalf("common retry delay = %s, want %s", got, forgeEventRetryDelay(2))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.RecordForgeEventBatchErrorAfter(t.Context(), eventIDs, cause, time.Second); err != nil {
		t.Fatalf("exact deferral replay after restart: %v", err)
	}
	if replayed := readForgeEventBatch(t, db, eventIDs); !reflect.DeepEqual(replayed, deferred) {
		t.Fatalf("deferral replay rewrote diagnostics: before=%#v after=%#v", deferred, replayed)
	}
	if err := db.RecordForgeEventBatchErrorAfter(t.Context(), eventIDs[1:], cause, time.Second); !errors.Is(err, ErrConflict) {
		t.Fatalf("different deferral members = %v, want ErrConflict", err)
	}
	if err := db.RecordForgeEventBatchErrorAfter(t.Context(), eventIDs, cause, time.Minute); !errors.Is(err, ErrConflict) {
		t.Fatalf("different deferral delay = %v, want ErrConflict", err)
	}
	seed, err := db.GetForgeEvent(t.Context(), eventIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if batch, err := db.ListDueForgeEventBatch(t.Context(), seed); err != nil || len(batch) != 0 {
		t.Fatalf("deferred siblings regrouped before common retry: %#v, %v", batch, err)
	}
}

func TestRunningForgeEventBatchDeferralPreservesExactAssociationAndReplaysAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "running-forge-batch-replay.sqlite")
	db, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	record, original, _ := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(t.Context(), record.ID, original.ID); err != nil {
		t.Fatal(err)
	}
	eventIDs := putPendingForgeEventBatch(t, db, "running-deferral", forgeEventBatchSize)
	attempt, _, err := startForgeEventBatchForTest(t.Context(), db, eventIDs, record.ID, "fix exact batch")
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("temporary Kubernetes startup failure")
	if err := db.RecordForgeEventErrorAfter(t.Context(), eventIDs[0], cause, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordForgeEventBatchErrorAfter(t.Context(), eventIDs, cause, time.Second); err != nil {
		t.Fatal(err)
	}
	deferred := readForgeEventBatch(t, db, eventIDs)
	for _, event := range deferred {
		if event.Status != ForgeEventRunning || event.TaskID != record.ID || event.AttemptID != attempt.ID || event.Attempts != 3 ||
			event.LastError != cause.Error() || event.NextAttemptAt == nil || !event.NextAttemptAt.Equal(*deferred[0].NextAttemptAt) || !event.UpdatedAt.Equal(deferred[0].UpdatedAt) {
			t.Fatalf("deferred running batch member = %#v; first=%#v", event, deferred[0])
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.RecordForgeEventBatchErrorAfter(t.Context(), eventIDs, cause, time.Second); err != nil {
		t.Fatalf("running exact replay after restart: %v", err)
	}
	if replayed := readForgeEventBatch(t, db, eventIDs); !reflect.DeepEqual(replayed, deferred) {
		t.Fatalf("running replay changed batch: before=%#v after=%#v", deferred, replayed)
	}
	if _, err := db.db.ExecContext(t.Context(), `UPDATE forge_events SET next_attempt_at = ? WHERE attempt_id = ?`, stamp(time.Now().Add(-time.Second)), attempt.ID); err != nil {
		t.Fatal(err)
	}
	due, err := db.ListIncompleteForgeEvents(t.Context())
	if err != nil || len(due) != forgeEventBatchSize || due[0].AttemptID != attempt.ID {
		t.Fatalf("running batch after deadline = %#v, %v", due, err)
	}
}

func TestRunningForgeEventBatchDeferralIgnoresHistoricalTerminalSiblings(t *testing.T) {
	db := openTestStore(t)
	_, eventIDs := mixedTerminalRunningForgeEventBatch(t, db, "partial-deferral")
	historical := readForgeEventBatch(t, db, eventIDs[:2])
	cause := errors.New("temporary partial batch outage")

	if err := db.RecordForgeEventBatchErrorAfter(t.Context(), eventIDs[2:], cause, time.Second); err != nil {
		t.Fatal(err)
	}
	after := readForgeEventBatch(t, db, eventIDs)
	if !reflect.DeepEqual(after[:2], historical) {
		t.Fatalf("historical terminal siblings changed: before=%#v after=%#v", historical, after[:2])
	}
	for _, event := range after[2:] {
		if event.Status != ForgeEventRunning || event.Attempts != 2 || event.LastError != cause.Error() || event.NextAttemptAt == nil ||
			!event.NextAttemptAt.Equal(*after[2].NextAttemptAt) || !event.UpdatedAt.Equal(after[2].UpdatedAt) {
			t.Fatalf("deferred running sibling = %#v; first=%#v", event, after[2])
		}
	}

	beforeConflict := readForgeEventBatch(t, db, eventIDs)
	if err := db.RecordForgeEventBatchErrorAfter(t.Context(), eventIDs[2:3], cause, time.Second); !errors.Is(err, ErrConflict) {
		t.Fatalf("omitted running member = %v, want ErrConflict", err)
	}
	if err := db.RecordForgeEventBatchErrorAfter(t.Context(), []string{eventIDs[0], eventIDs[2]}, cause, time.Second); !errors.Is(err, ErrConflict) {
		t.Fatalf("selected handled member = %v, want ErrConflict", err)
	}
	if current := readForgeEventBatch(t, db, eventIDs); !reflect.DeepEqual(current, beforeConflict) {
		t.Fatalf("conflicting selections changed events: before=%#v after=%#v", beforeConflict, current)
	}
}

func TestPermanentRunningForgeEventBatchReplayUsesExactFailureFingerprint(t *testing.T) {
	db := openTestStore(t)
	record, eventIDs := mixedTerminalRunningForgeEventBatch(t, db, "partial-permanent")
	historical := readForgeEventBatch(t, db, eventIDs[:2])
	cause := errors.New("permanent partial batch failure")

	if err := db.FailForgeEventBatch(t.Context(), eventIDs[2:], cause); err != nil {
		t.Fatal(err)
	}
	failed := readForgeEventBatch(t, db, eventIDs)
	if !reflect.DeepEqual(failed[:2], historical) {
		t.Fatalf("historical terminal siblings changed: before=%#v after=%#v", historical, failed[:2])
	}
	for _, event := range failed[2:] {
		if event.Status != ForgeEventFailed || event.Attempts != 2 || event.LastError != cause.Error() || event.FailedAt == nil || event.NextAttemptAt != nil ||
			!event.FailedAt.Equal(*failed[2].FailedAt) || !event.UpdatedAt.Equal(failed[2].UpdatedAt) {
			t.Fatalf("newly failed running sibling = %#v; first=%#v", event, failed[2])
		}
	}
	current, err := db.GetTask(t.Context(), record.ID)
	if err != nil || !current.CancellationRequested {
		t.Fatalf("running owner cancellation = %#v, %v", current, err)
	}

	if err := db.FailForgeEventBatch(t.Context(), eventIDs[2:], cause); err != nil {
		t.Fatalf("exact batch replay: %v", err)
	}
	if err := db.FailForgeEvent(t.Context(), eventIDs[3], cause); err != nil {
		t.Fatalf("replay through newly failed sibling: %v", err)
	}
	if replayed := readForgeEventBatch(t, db, eventIDs); !reflect.DeepEqual(replayed, failed) {
		t.Fatalf("exact replay changed events: before=%#v after=%#v", failed, replayed)
	}

	for name, test := range map[string]struct {
		ids   []string
		cause error
	}{
		"omitted fingerprint member": {ids: eventIDs[2:3], cause: cause},
		"unrelated failed member":    {ids: []string{eventIDs[1], eventIDs[2]}, cause: cause},
		"different cause":            {ids: eventIDs[2:], cause: errors.New("different permanent failure")},
	} {
		t.Run(name, func(t *testing.T) {
			if err := db.FailForgeEventBatch(t.Context(), test.ids, test.cause); !errors.Is(err, ErrConflict) {
				t.Fatalf("conflicting replay = %v, want ErrConflict", err)
			}
			if current := readForgeEventBatch(t, db, eventIDs); !reflect.DeepEqual(current, failed) {
				t.Fatalf("conflicting replay changed events: before=%#v after=%#v", failed, current)
			}
		})
	}
}

func TestPermanentRunningForgeEventBatchFailsExactlyWithoutCancellingTerminalTask(t *testing.T) {
	db := openTestStore(t)
	record, original, _ := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(t.Context(), record.ID, original.ID); err != nil {
		t.Fatal(err)
	}
	eventIDs := putPendingForgeEventBatch(t, db, "running-terminal", forgeEventBatchSize)
	attempt, _, err := startForgeEventBatchForTest(t.Context(), db, eventIDs, record.ID, "fix exact batch")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Transition(t.Context(), record.ID, task.QUEUED, task.FAILED, TransitionParams{Reason: "startup failed", Trigger: "kubernetes"}); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("permanent Kubernetes startup failure")
	if err := db.FailForgeEventBatch(t.Context(), eventIDs, cause); err != nil {
		t.Fatal(err)
	}
	failed := readForgeEventBatch(t, db, eventIDs)
	for _, event := range failed {
		if event.Status != ForgeEventFailed || event.TaskID != record.ID || event.AttemptID != attempt.ID || event.Attempts != 2 || event.LastError != cause.Error() || event.FailedAt == nil {
			t.Fatalf("terminal running batch member = %#v", event)
		}
	}
	current, err := db.GetTask(t.Context(), record.ID)
	if err != nil || current.State != task.FAILED || current.CancellationRequested {
		t.Fatalf("terminal task after batch failure = %#v, %v", current, err)
	}
	if err := db.FailForgeEventBatch(t.Context(), eventIDs, cause); err != nil {
		t.Fatalf("terminal exact replay: %v", err)
	}
}

func TestRunningForgeEventBatchOutcomeRejectsNonExactAssociationWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Store, []string, Task, Attempt)
		ids    func([]string) []string
	}{
		{
			name: "mixed handled member",
			mutate: func(t *testing.T, db *Store, ids []string, _ Task, _ Attempt) {
				if err := db.MarkForgeEventHandled(t.Context(), ids[1]); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "mismatched attempt",
			mutate: func(t *testing.T, db *Store, ids []string, _ Task, original Attempt) {
				if _, err := db.db.ExecContext(t.Context(), `UPDATE forge_events SET attempt_id = ? WHERE id = ?`, original.ID, ids[1]); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "mismatched task attempt pair",
			mutate: func(t *testing.T, db *Store, ids []string, _ Task, _ Attempt) {
				other, err := db.CreateTask(t.Context(), CreateTaskParams{Repository: "other", Prompt: "other task"})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.db.ExecContext(t.Context(), `UPDATE forge_events SET attempt_id = ? WHERE id IN (?, ?, ?)`, other.CurrentAttemptID, ids[0], ids[1], ids[2]); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "unselected running sibling",
			ids:  func(ids []string) []string { return ids[:2] },
		},
		{
			name: "missing ID",
			ids: func(ids []string) []string {
				return append(append([]string(nil), ids[:2]...), "missing-running-member")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openTestStore(t)
			record, original, _ := createOpenPullRequestTask(t, db)
			if err := db.MarkLogsExhausted(t.Context(), record.ID, original.ID); err != nil {
				t.Fatal(err)
			}
			eventIDs := putPendingForgeEventBatch(t, db, "running-mismatch-"+strings.ReplaceAll(test.name, " ", "-"), 3)
			attempt, _, err := startForgeEventBatchForTest(t.Context(), db, eventIDs, record.ID, "fix exact batch")
			if err != nil {
				t.Fatal(err)
			}
			if test.mutate != nil {
				test.mutate(t, db, eventIDs, record, original)
			}
			selected := eventIDs
			if test.ids != nil {
				selected = test.ids(eventIDs)
			}
			before := readForgeEventBatch(t, db, eventIDs)
			err = db.RecordForgeEventBatchErrorAfter(t.Context(), selected, errors.New("must remain atomic"), time.Minute)
			if err == nil || !errors.Is(err, ErrConflict) && !errors.Is(err, ErrNotFound) {
				t.Fatalf("non-exact running batch = %v; want conflict or not found", err)
			}
			after := readForgeEventBatch(t, db, eventIDs)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("non-exact outcome mutated batch on attempt %q: before=%#v after=%#v", attempt.ID, before, after)
			}
		})
	}
}

func TestRunningForgeEventBatchOutcomeFailuresRollBackEventsAndCancellation(t *testing.T) {
	for _, test := range []struct {
		name, setup, want string
	}{
		{
			name: "statement failure",
			want: "forced running batch statement failure",
			setup: `CREATE TRIGGER reject_running_batch_update BEFORE UPDATE OF status ON forge_events
				WHEN NEW.status = 'failed' BEGIN SELECT RAISE(FAIL, 'forced running batch statement failure'); END`,
		},
		{
			name: "commit failure",
			want: "commit forge event batch failure",
			setup: `
				CREATE TABLE forced_running_batch_commit_failure (task_id TEXT REFERENCES tasks(id) DEFERRABLE INITIALLY DEFERRED);
				CREATE TRIGGER reject_running_batch_commit AFTER UPDATE OF status ON forge_events WHEN NEW.status = 'failed'
				BEGIN INSERT INTO forced_running_batch_commit_failure(task_id) VALUES ('missing-task'); END;`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openTestStore(t)
			record, original, _ := createOpenPullRequestTask(t, db)
			if err := db.MarkLogsExhausted(t.Context(), record.ID, original.ID); err != nil {
				t.Fatal(err)
			}
			eventIDs := putPendingForgeEventBatch(t, db, "running-rollback-"+strings.ReplaceAll(test.name, " ", "-"), 3)
			if _, _, err := startForgeEventBatchForTest(t.Context(), db, eventIDs, record.ID, "fix exact batch"); err != nil {
				t.Fatal(err)
			}
			before := readForgeEventBatch(t, db, eventIDs)
			if _, err := db.db.ExecContext(t.Context(), test.setup); err != nil {
				t.Fatal(err)
			}
			err := db.FailForgeEventBatch(t.Context(), eventIDs, errors.New("permanent startup failure"))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("running batch %s = %v", test.name, err)
			}
			after := readForgeEventBatch(t, db, eventIDs)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("running batch %s partially changed events: before=%#v after=%#v", test.name, before, after)
			}
			current, err := db.GetTask(t.Context(), record.ID)
			if err != nil || current.CancellationRequested {
				t.Fatalf("running batch %s retained cancellation: %#v, %v", test.name, current, err)
			}
		})
	}
}

func TestForgeEventBatchMismatchAndStatementFailureRollBackEveryMember(t *testing.T) {
	t.Run("mismatched member", func(t *testing.T) {
		db := openTestStore(t)
		eventIDs := putPendingForgeEventBatch(t, db, "mismatch", 3)
		if err := db.MarkForgeEventHandled(t.Context(), eventIDs[1]); err != nil {
			t.Fatal(err)
		}
		if err := db.FailForgeEventBatch(t.Context(), eventIDs, errors.New("must be atomic")); !errors.Is(err, ErrConflict) {
			t.Fatalf("mismatched batch = %v, want ErrConflict", err)
		}
		events := readForgeEventBatch(t, db, eventIDs)
		if events[0].Status != ForgeEventPending || events[0].Attempts != 0 || events[1].Status != ForgeEventHandled || events[2].Status != ForgeEventPending || events[2].Attempts != 0 {
			t.Fatalf("mismatched batch partially changed: %#v", events)
		}
	})

	t.Run("statement failure", func(t *testing.T) {
		db := openTestStore(t)
		eventIDs := putPendingForgeEventBatch(t, db, "statement-rollback", 3)
		if _, err := db.db.ExecContext(t.Context(), `
			CREATE TRIGGER reject_second_batch_member BEFORE UPDATE ON forge_events
			WHEN OLD.id = 'statement-rollback-01'
			BEGIN SELECT RAISE(FAIL, 'forced batch statement failure'); END`); err != nil {
			t.Fatal(err)
		}
		err := db.FailForgeEventBatch(t.Context(), eventIDs, errors.New("must roll back"))
		if err == nil || !strings.Contains(err.Error(), "forced batch statement failure") {
			t.Fatalf("statement failure = %v", err)
		}
		for _, event := range readForgeEventBatch(t, db, eventIDs) {
			if event.Status != ForgeEventPending || event.Attempts != 0 || event.LastError != "" || event.FailedAt != nil {
				t.Fatalf("statement failure partially changed event: %#v", event)
			}
		}
		if _, err := db.db.ExecContext(t.Context(), `DROP TRIGGER reject_second_batch_member`); err != nil {
			t.Fatalf("connection unusable after statement rollback: %v", err)
		}
		if err := db.FailForgeEventBatch(t.Context(), eventIDs, errors.New("must roll back")); err != nil {
			t.Fatalf("retry after statement rollback: %v", err)
		}
	})
}

func TestForgeEventBatchCommitFailureRollsBackAndReusesConnection(t *testing.T) {
	db := openTestStore(t)
	eventIDs := putPendingForgeEventBatch(t, db, "commit-deferral", 3)
	if _, err := db.db.ExecContext(t.Context(), `
		CREATE TABLE forced_batch_commit_failure (
			event_id TEXT REFERENCES forge_events(id) DEFERRABLE INITIALLY DEFERRED
		);
		CREATE TRIGGER reject_batch_commit AFTER UPDATE OF next_attempt_at ON forge_events
		WHEN NEW.next_attempt_at IS NOT NULL
		BEGIN INSERT INTO forced_batch_commit_failure(event_id) VALUES ('missing-event'); END;
	`); err != nil {
		t.Fatal(err)
	}
	err := db.RecordForgeEventBatchErrorAfter(t.Context(), eventIDs, errors.New("temporary"), time.Minute)
	if err == nil || !strings.Contains(err.Error(), "commit forge event batch deferral") {
		t.Fatalf("commit failure = %v", err)
	}
	for _, event := range readForgeEventBatch(t, db, eventIDs) {
		if event.Status != ForgeEventPending || event.Attempts != 0 || event.LastError != "" || event.NextAttemptAt != nil {
			t.Fatalf("commit failure partially deferred event: %#v", event)
		}
	}
	var forcedRows int
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM forced_batch_commit_failure`).Scan(&forcedRows); err != nil || forcedRows != 0 {
		t.Fatalf("deferred constraint rows after rollback = %d, %v", forcedRows, err)
	}
	if _, err := db.db.ExecContext(t.Context(), `DROP TRIGGER reject_batch_commit`); err != nil {
		t.Fatalf("connection unusable after commit rollback: %v", err)
	}
	if err := db.RecordForgeEventBatchErrorAfter(t.Context(), eventIDs, errors.New("temporary"), time.Minute); err != nil {
		t.Fatalf("retry after commit rollback: %v", err)
	}
}

func TestDeferForgeEventYieldsWithoutChangingOutcomeOrDiagnostics(t *testing.T) {
	db := openTestStore(t)
	event := testForgeEvent("yield-pending", "review_comment")
	if _, err := db.PutForgeEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordForgeEventError(t.Context(), event.ID, errors.New("real provider error")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(t.Context(), `UPDATE forge_events SET next_attempt_at = ? WHERE id = ?`, stamp(time.Now().Add(-time.Second)), event.ID); err != nil {
		t.Fatal(err)
	}
	before, err := db.GetForgeEvent(t.Context(), event.ID)
	if err != nil {
		t.Fatal(err)
	}
	err = db.DeferForgeEvent(t.Context(), event.ID, ForgeEventPending)
	after, getErr := db.GetForgeEvent(t.Context(), event.ID)
	if err != nil || getErr != nil || after.Status != ForgeEventPending || after.Attempts != before.Attempts || after.LastError != before.LastError || after.TaskID != before.TaskID || after.AttemptID != before.AttemptID || after.NextAttemptAt == nil || !after.NextAttemptAt.After(time.Now().UTC()) {
		t.Fatalf("deferred event = %#v, errors %v/%v; before %#v", after, err, getErr, before)
	}
	if err := db.MarkForgeEventHandled(t.Context(), event.ID); err != nil {
		t.Fatal(err)
	}
	err = db.DeferForgeEvent(t.Context(), event.ID, ForgeEventPending)
	handled, getErr := db.GetForgeEvent(t.Context(), event.ID)
	if err != nil || getErr != nil || handled.Status != ForgeEventHandled || handled.NextAttemptAt != nil {
		t.Fatalf("status-conditional deferral changed handled event = %#v, errors %v/%v", handled, err, getErr)
	}
}

func TestListIncompleteForgeEventsReturnsOrderedDueBatch(t *testing.T) {
	db := openTestStore(t)
	const batchSize = 32
	for i := range batchSize + 3 {
		event := testForgeEvent(fmt.Sprintf("batched-%02d", i), "review_comment")
		if _, err := db.PutForgeEvent(t.Context(), event); err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			if err := db.RecordForgeEventError(t.Context(), event.ID, errors.New("deferred")); err != nil {
				t.Fatal(err)
			}
		}
	}

	listed, err := db.ListIncompleteForgeEvents(t.Context())
	if err != nil || len(listed) != batchSize {
		t.Fatalf("ListIncompleteForgeEvents() count = %d, %v; want %d", len(listed), err, batchSize)
	}
	for i, event := range listed {
		want := i
		if i > 0 {
			want++
		}
		if event.ID != fmt.Sprintf("batched-%02d", want) {
			t.Fatalf("ListIncompleteForgeEvents()[%d] = %q; want batched-%02d", i, event.ID, want)
		}
	}
}

func TestListIncompleteForgeEventsOrdersByEligibilityBeforeAcceptance(t *testing.T) {
	db := openTestStore(t)
	eligibility := time.Now().UTC()
	for i := range 32 {
		event := testForgeEvent(fmt.Sprintf("eligibility-deferred-%02d", i), "review_comment")
		if _, err := db.PutForgeEvent(t.Context(), event); err != nil {
			t.Fatal(err)
		}
		if _, err := db.db.ExecContext(t.Context(), `UPDATE forge_events SET created_at = ?, next_attempt_at = ? WHERE id = ?`, stamp(eligibility.Add(-time.Hour)), stamp(eligibility.Add(-5*time.Second)), event.ID); err != nil {
			t.Fatal(err)
		}
	}
	unrelated := testForgeEvent("eligibility-unrelated", "quality_gate_failed")
	if _, err := db.PutForgeEvent(t.Context(), unrelated); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(t.Context(), `UPDATE forge_events SET created_at = ?, next_attempt_at = NULL WHERE id = ?`, stamp(eligibility.Add(-10*time.Second)), unrelated.ID); err != nil {
		t.Fatal(err)
	}

	due, err := db.ListIncompleteForgeEvents(t.Context())
	if err != nil || len(due) != 32 || due[0].ID != unrelated.ID {
		t.Fatalf("eligibility-ordered events = %#v, %v; want unrelated event first", due, err)
	}
}

func TestFailForgeEventIsTerminalCancelsOwnerAndPreservesAssociation(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()
	record, original, _ := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(ctx, record.ID, original.ID); err != nil {
		t.Fatalf("mark original logs exhausted: %v", err)
	}
	event := testForgeEvent("permanent-reply-failure", "review_comment")
	if _, err := db.PutForgeEvent(ctx, event); err != nil {
		t.Fatalf("put forge event: %v", err)
	}
	attempt, _, err := startForgeEventAttemptForTest(ctx, db, event.ID, record.ID, "fix review feedback")
	if err != nil {
		t.Fatalf("start forge event: %v", err)
	}
	before := time.Now().UTC()
	if err := db.FailForgeEvent(ctx, event.ID, errors.New("permanent provider rejection")); err != nil {
		t.Fatalf("fail forge event: %v", err)
	}
	failed, err := db.GetForgeEvent(ctx, event.ID)
	if err != nil || failed.Status != ForgeEventFailed || failed.TaskID != record.ID || failed.AttemptID != attempt.ID || failed.Attempts != 2 || failed.FailedAt == nil || failed.FailedAt.Before(before) || failed.UpdatedAt.Before(before) || failed.NextAttemptAt != nil || !strings.Contains(failed.LastError, "permanent provider rejection") {
		t.Fatalf("failed forge event = %#v, %v", failed, err)
	}
	current, err := db.GetTask(ctx, record.ID)
	if err != nil || !current.CancellationRequested {
		t.Fatalf("owned running event cancellation = %#v, %v", current, err)
	}
	if failed.HandledAt != nil {
		t.Fatalf("failed forge event has handled timestamp: %#v", failed)
	}
	incomplete, err := db.ListIncompleteForgeEvents(ctx)
	if err != nil || len(incomplete) != 0 {
		t.Fatalf("incomplete forge events = %#v, %v; want failed excluded", incomplete, err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE forge_events SET status = 'unknown' WHERE id = ?`, event.ID); err == nil {
		t.Fatal("forge_events status CHECK accepted unknown state")
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE tasks SET state = ?, current_attempt_id = ?, cancellation_requested = 0 WHERE id = ?`, task.PR_OPEN, original.ID, record.ID); err != nil {
		t.Fatalf("restore task after terminal event: %v", err)
	}
	next := testForgeEvent("after-permanent-failure", "review_comment")
	if _, err := db.PutForgeEvent(ctx, next); err != nil {
		t.Fatal(err)
	}
	if _, _, err := startForgeEventAttemptForTest(ctx, db, next.ID, record.ID, "fix later feedback"); err != nil {
		t.Fatalf("terminal forge event blocked later event for task: %v", err)
	}
}

func TestFailForgeEventAtomicallyFailsRunningBatchAndReplaysThroughSiblings(t *testing.T) {
	db := openTestStore(t)
	ctx := t.Context()
	record, original, _ := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(ctx, record.ID, original.ID); err != nil {
		t.Fatal(err)
	}
	const batchSize = 32
	eventIDs := make([]string, batchSize)
	for i := range batchSize {
		event := testForgeEvent(fmt.Sprintf("permanent-batch-%02d", i), "review_comment")
		event.CommentID = 1000 + i
		eventIDs[i] = event.ID
		if _, err := db.PutForgeEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	attempt, _, err := startForgeEventBatchForTest(ctx, db, eventIDs, record.ID, "fix every review comment")
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("permanent batch ownership loss")
	before := time.Now().UTC()
	if err := db.FailForgeEvent(ctx, eventIDs[0], cause); err != nil {
		t.Fatal(err)
	}
	after := time.Now().UTC()
	failed, err := db.ListForgeEventsByAttempt(ctx, attempt.ID)
	if err != nil || len(failed) != batchSize {
		t.Fatalf("failed batch = %#v, %v", failed, err)
	}
	for _, event := range failed {
		if event.Status != ForgeEventFailed || event.TaskID != record.ID || event.AttemptID != attempt.ID || event.Attempts != 2 || event.LastError != cause.Error() || event.NextAttemptAt != nil || event.FailedAt == nil || event.FailedAt.Before(before) || event.FailedAt.After(after) || !event.FailedAt.Equal(*failed[0].FailedAt) || !event.UpdatedAt.Equal(failed[0].UpdatedAt) {
			t.Fatalf("batch failure event = %#v; first=%#v", event, failed[0])
		}
	}
	if current, err := db.GetTask(ctx, record.ID); err != nil || !current.CancellationRequested {
		t.Fatalf("batch cancellation = %#v, %v", current, err)
	}

	for _, replayID := range []string{eventIDs[0], eventIDs[17]} {
		if err := db.FailForgeEvent(ctx, replayID, cause); err != nil {
			t.Fatalf("replay through %q: %v", replayID, err)
		}
	}
	if err := db.FailForgeEvent(ctx, eventIDs[31], errors.New("different permanent cause")); !errors.Is(err, ErrConflict) {
		t.Fatalf("different replay cause = %v; want ErrConflict", err)
	}
	replayed, err := db.ListForgeEventsByAttempt(ctx, attempt.ID)
	if err != nil || !reflect.DeepEqual(replayed, failed) {
		t.Fatalf("replay changed batch: before=%#v after=%#v err=%v", failed, replayed, err)
	}
}

func TestFailForgeEventStillFailsWithMissingStaleAndTerminalAssociations(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Store, Task, Attempt, Attempt, ForgeEvent)
	}{
		{
			name: "missing association",
			mutate: func(t *testing.T, db *Store, _ Task, _ Attempt, _ Attempt, event ForgeEvent) {
				if _, err := db.db.ExecContext(t.Context(), `UPDATE forge_events SET task_id = NULL, attempt_id = NULL, status = 'pending' WHERE id = ?`, event.ID); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "stale attempt",
			mutate: func(t *testing.T, db *Store, _ Task, original, _ Attempt, event ForgeEvent) {
				if _, err := db.db.ExecContext(t.Context(), `UPDATE forge_events SET attempt_id = ? WHERE id = ?`, original.ID, event.ID); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "terminal task",
			mutate: func(t *testing.T, db *Store, record Task, _ Attempt, _ Attempt, _ ForgeEvent) {
				if err := db.Transition(t.Context(), record.ID, task.QUEUED, task.FAILED, TransitionParams{Reason: "terminal", Trigger: "system"}); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openTestStore(t)
			record, original, _ := createOpenPullRequestTask(t, db)
			if err := db.MarkLogsExhausted(t.Context(), record.ID, original.ID); err != nil {
				t.Fatal(err)
			}
			event := testForgeEvent("safe-cancellation-"+strings.ReplaceAll(test.name, " ", "-"), "review_comment")
			if _, err := db.PutForgeEvent(t.Context(), event); err != nil {
				t.Fatal(err)
			}
			followUp, _, err := startForgeEventAttemptForTest(t.Context(), db, event.ID, record.ID, "follow up")
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, db, record, original, followUp, event)

			cause := errors.New("permanent association loss")
			err = db.FailForgeEvent(t.Context(), event.ID, cause)
			current, getErr := db.GetTask(t.Context(), record.ID)
			failed, eventErr := db.GetForgeEvent(t.Context(), event.ID)
			if err != nil || getErr != nil || eventErr != nil || current.CancellationRequested || failed.Status != ForgeEventFailed || failed.LastError != cause.Error() || failed.FailedAt == nil {
				t.Fatalf("FailForgeEvent() = %v; task=%#v, %v; event=%#v, %v", err, current, getErr, failed, eventErr)
			}
		})
	}
}

func TestFailForgeEventReplayAndMissingRequireIncompleteEvent(t *testing.T) {
	db := openTestStore(t)
	event := testForgeEvent("permanent-replay", "review_comment")
	if _, err := db.PutForgeEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	firstCause := errors.New("first permanent error")
	if err := db.FailForgeEvent(t.Context(), event.ID, firstCause); err != nil {
		t.Fatal(err)
	}
	first, err := db.GetForgeEvent(t.Context(), event.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FailForgeEvent(t.Context(), event.ID, errors.New("replayed permanent error")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("FailForgeEvent() replay = %v, want ErrNotFound", err)
	}
	replayed, err := db.GetForgeEvent(t.Context(), event.ID)
	if err != nil || replayed.Attempts != first.Attempts || replayed.LastError != first.LastError || replayed.UpdatedAt != first.UpdatedAt || replayed.FailedAt == nil || first.FailedAt == nil || !replayed.FailedAt.Equal(*first.FailedAt) {
		t.Fatalf("replayed failure changed event: first=%#v replayed=%#v err=%v", first, replayed, err)
	}
	if err := db.FailForgeEvent(t.Context(), "missing-permanent-event", firstCause); !errors.Is(err, ErrNotFound) {
		t.Fatalf("FailForgeEvent() missing = %v, want ErrNotFound", err)
	}
}

func TestFailForgeEventRollsBackCancellationWhenFailureUpdateFails(t *testing.T) {
	db := openTestStore(t)
	record, original, _ := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(t.Context(), record.ID, original.ID); err != nil {
		t.Fatal(err)
	}
	event := testForgeEvent("permanent-rollback", "review_comment")
	if _, err := db.PutForgeEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	if _, _, err := startForgeEventAttemptForTest(t.Context(), db, event.ID, record.ID, "follow up"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(t.Context(), `CREATE TRIGGER reject_forge_failure BEFORE UPDATE ON forge_events BEGIN SELECT RAISE(FAIL, 'forced forge failure update'); END`); err != nil {
		t.Fatal(err)
	}

	err := db.FailForgeEvent(t.Context(), event.ID, errors.New("permanent provider rejection"))
	if err == nil || !strings.Contains(err.Error(), "forced forge failure update") {
		t.Fatalf("FailForgeEvent() = %v, want forced second-statement failure", err)
	}
	current, taskErr := db.GetTask(t.Context(), record.ID)
	running, eventErr := db.GetForgeEvent(t.Context(), event.ID)
	if taskErr != nil || eventErr != nil || current.CancellationRequested || running.Status != ForgeEventRunning || running.Attempts != 1 || running.FailedAt != nil {
		t.Fatalf("atomic rollback outcomes: task=%#v event=%#v errors=%v/%v", current, running, taskErr, eventErr)
	}
}

func TestFailForgeEventCommitFailureRollsBackBatchAndCancellation(t *testing.T) {
	db := openTestStore(t)
	record, original, _ := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(t.Context(), record.ID, original.ID); err != nil {
		t.Fatal(err)
	}
	eventIDs := []string{"commit-rollback-a", "commit-rollback-b", "commit-rollback-c"}
	for i, id := range eventIDs {
		event := testForgeEvent(id, "review_comment")
		event.CommentID += i
		if _, err := db.PutForgeEvent(t.Context(), event); err != nil {
			t.Fatal(err)
		}
	}
	attempt, _, err := startForgeEventBatchForTest(t.Context(), db, eventIDs, record.ID, "follow up")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(t.Context(), `CREATE TABLE forced_forge_commit_failure (task_id TEXT REFERENCES tasks(id) DEFERRABLE INITIALLY DEFERRED)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(t.Context(), `CREATE TRIGGER reject_forge_failure_commit AFTER UPDATE OF status ON forge_events WHEN NEW.status = 'failed' BEGIN INSERT INTO forced_forge_commit_failure(task_id) VALUES ('missing-task'); END`); err != nil {
		t.Fatal(err)
	}

	err = db.FailForgeEvent(t.Context(), eventIDs[1], errors.New("permanent provider rejection"))
	if err == nil || !strings.Contains(err.Error(), "commit forge event") {
		t.Fatalf("FailForgeEvent() = %v, want commit failure", err)
	}
	current, taskErr := db.GetTask(t.Context(), record.ID)
	running, eventErr := db.ListForgeEventsByAttempt(t.Context(), attempt.ID)
	if taskErr != nil || eventErr != nil || current.CancellationRequested || len(running) != len(eventIDs) {
		t.Fatalf("commit rollback outcomes after %v: task=%#v events=%#v errors=%v/%v", err, current, running, taskErr, eventErr)
	}
	for _, event := range running {
		if event.Status != ForgeEventRunning || event.Attempts != 1 || event.FailedAt != nil || event.LastError != "" {
			t.Fatalf("commit failure changed running event: %#v", event)
		}
	}
}

func TestStartForgeEventAttemptIsAtomicImmutableAndReplaySafe(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()
	record, originalAttempt, originalPR := createOpenPullRequestTask(t, db)
	event := testForgeEvent("forge-follow-up-1", "review_comment")
	if _, err := db.PutForgeEvent(ctx, event); err != nil {
		t.Fatalf("put forge event: %v", err)
	}

	if originalAttempt.Prompt != "" || originalAttempt.BaseBranch != "" || originalAttempt.TaskBranch != "" {
		t.Fatalf("initial attempt overrides = prompt %q base %q task %q; want fallback values", originalAttempt.Prompt, originalAttempt.BaseBranch, originalAttempt.TaskBranch)
	}
	prompt := "Original task: fix the parser\n\nReview feedback from Ada: preserve quoted commas"
	if _, _, err := startForgeEventAttemptForTest(ctx, db, event.ID, record.ID, prompt); !errors.Is(err, ErrConflict) {
		t.Fatalf("start follow-up before logs exhausted = %v, want ErrConflict", err)
	}
	before, err := db.ListAttempts(ctx, record.ID)
	if err != nil || len(before) != 1 {
		t.Fatalf("attempts after refused follow-up = %#v, %v; want original only", before, err)
	}

	if err := db.MarkLogsExhausted(ctx, record.ID, originalAttempt.ID); err != nil {
		t.Fatalf("mark original logs exhausted: %v", err)
	}
	followUp, inserted, err := startForgeEventAttemptForTest(ctx, db, event.ID, record.ID, prompt)
	if err != nil {
		t.Fatalf("start forge follow-up: %v", err)
	}
	if !inserted || followUp.Number != 2 || followUp.State != task.QUEUED || !followUp.Immutable {
		t.Fatalf("follow-up attempt = %#v, inserted=%t; want immutable queued attempt 2", followUp, inserted)
	}
	if followUp.Prompt != prompt || followUp.BaseBranch != originalPR.BaseBranch || followUp.TaskBranch != originalPR.HeadBranch {
		t.Fatalf("follow-up overrides = prompt %q base %q task %q; want immutable prompt and existing PR base/head %q/%q", followUp.Prompt, followUp.BaseBranch, followUp.TaskBranch, originalPR.BaseBranch, originalPR.HeadBranch)
	}
	if len(followUp.ManifestJSON) == 0 || len(followUp.ResourceSnapshot) == 0 || followUp.ConfigDigest == "" {
		t.Fatalf("follow-up snapshot is incomplete: %#v", followUp)
	}
	current, err := db.GetTask(ctx, record.ID)
	if err != nil || current.State != task.QUEUED || current.CurrentAttemptID != followUp.ID || current.Prompt != record.Prompt {
		t.Fatalf("task after follow-up = %#v, %v; want queued on new attempt with original task prompt", current, err)
	}
	copiedPR, err := db.GetPullRequest(ctx, followUp.ID)
	if err != nil {
		t.Fatalf("get copied pull request: %v", err)
	}
	if copiedPR.State != "open" || copiedPR.Number != originalPR.Number || copiedPR.URL != originalPR.URL || copiedPR.Title != originalPR.Title || copiedPR.HeadBranch != originalPR.HeadBranch || copiedPR.BaseBranch != originalPR.BaseBranch {
		t.Fatalf("copied pull request = %#v; want identity from %#v", copiedPR, originalPR)
	}
	associated, err := db.ListForgeEventsByAttempt(ctx, followUp.ID)
	if err != nil || len(associated) != 1 || associated[0].ID != event.ID || associated[0].TaskID != record.ID || associated[0].AttemptID != followUp.ID || associated[0].Status != "running" || associated[0].Attempts != 1 {
		t.Fatalf("forge event association = %#v, %v", associated, err)
	}

	replayed, inserted, err := startForgeEventAttemptForTest(ctx, db, event.ID, record.ID, "replacement prompt must be ignored")
	if err != nil {
		t.Fatalf("replay forge follow-up: %v", err)
	}
	if inserted || replayed.ID != followUp.ID || replayed.Prompt != prompt || replayed.BaseBranch != originalPR.BaseBranch || replayed.TaskBranch != originalPR.HeadBranch ||
		string(replayed.ManifestJSON) != string(followUp.ManifestJSON) || string(replayed.ResourceSnapshot) != string(followUp.ResourceSnapshot) || replayed.ConfigDigest != followUp.ConfigDigest {
		t.Fatalf("replayed follow-up = %#v, inserted=%t; want immutable attempt %q", replayed, inserted, followUp.ID)
	}
	after, err := db.ListAttempts(ctx, record.ID)
	if err != nil || len(after) != 2 {
		t.Fatalf("attempts after replay = %#v, %v; want exactly two", after, err)
	}

	concurrent := testForgeEvent("forge-follow-up-2", "quality_gate_failed")
	if _, err := db.PutForgeEvent(ctx, concurrent); err != nil {
		t.Fatalf("put concurrent forge event: %v", err)
	}
	if _, _, err := startForgeEventAttemptForTest(ctx, db, concurrent.ID, record.ID, "fix the quality gate"); !errors.Is(err, ErrConflict) {
		t.Fatalf("start concurrent follow-up = %v, want ErrConflict", err)
	}
	after, err = db.ListAttempts(ctx, record.ID)
	if err != nil || len(after) != 2 {
		t.Fatalf("attempts after concurrent refusal = %#v, %v; want exactly two", after, err)
	}

	beforeHandled := time.Now().UTC()
	if err := db.MarkForgeEventHandled(ctx, event.ID); err != nil {
		t.Fatalf("mark forge event handled: %v", err)
	}
	handled, err := db.GetForgeEvent(ctx, event.ID)
	if err != nil || handled.Status != "handled" || handled.HandledAt == nil || handled.HandledAt.Before(beforeHandled) {
		t.Fatalf("handled forge event = %#v, %v", handled, err)
	}
	incomplete, err := db.ListIncompleteForgeEvents(ctx)
	if err != nil || len(incomplete) != 1 || incomplete[0].ID != concurrent.ID {
		t.Fatalf("incomplete events after handling = %#v, %v; want concurrent event only", incomplete, err)
	}
}

func TestStartForgeEventAttemptWaitsForPendingWorkerEvents(t *testing.T) {
	db := openTestStore(t)
	ctx := t.Context()
	record, originalAttempt, originalPR := createOpenPullRequestTask(t, db)
	if _, err := db.AppendPodLog(ctx, AppendPodLogParams{
		TaskID: record.ID, AttemptID: originalAttempt.ID, PodUID: "forge-follow-up-pod", JobName: "job", PodName: "pod",
		Content: []byte("pending event"), WorkerEventID: "forge-follow-up-worker-event", WorkerEvent: "pending event",
	}, 256, 64); err != nil {
		t.Fatalf("append pending worker event: %v", err)
	}
	if err := db.MarkLogsExhausted(ctx, record.ID, originalAttempt.ID); err != nil {
		t.Fatalf("mark original logs exhausted: %v", err)
	}
	event := testForgeEvent("forge-follow-up-after-worker-event", "review_comment")
	if _, err := db.PutForgeEvent(ctx, event); err != nil {
		t.Fatalf("put forge event: %v", err)
	}
	plan, err := db.PlanForgeEventAttempt(ctx, []string{event.ID}, record.ID, "apply durable review feedback")
	if err != nil {
		t.Fatalf("plan forge follow-up: %v", err)
	}
	plan.Attempt.ManifestJSON = []byte(`{"task_id":"planned"}`)
	plan.Attempt.ResourceSnapshot = []byte(`{"job":{"metadata":{"name":"planned"}},"secret":{"metadata":{"name":"planned"}}}`)
	plan.Attempt.ConfigDigest = "sha256:planned"

	if _, _, err := db.StartForgeEventAttempt(ctx, plan); !errors.Is(err, ErrConflict) {
		t.Fatalf("start follow-up with pending worker event = %v, want ErrConflict", err)
	}
	current, err := db.GetTask(ctx, record.ID)
	if err != nil || current.CurrentAttemptID != originalAttempt.ID || current.State != task.PR_OPEN {
		t.Fatalf("task after refused follow-up = %#v, %v", current, err)
	}
	storedEvent, err := db.GetForgeEvent(ctx, event.ID)
	if err != nil || storedEvent.Status != ForgeEventPending || storedEvent.TaskID != "" || storedEvent.AttemptID != "" {
		t.Fatalf("forge event after refused follow-up = %#v, %v", storedEvent, err)
	}

	if err := db.MarkWorkerEventProcessed(ctx, "forge-follow-up-worker-event"); err != nil {
		t.Fatalf("mark worker event processed: %v", err)
	}
	followUp, inserted, err := db.StartForgeEventAttempt(ctx, plan)
	if err != nil || !inserted {
		t.Fatalf("start same follow-up plan after processing event = %#v, %t, %v", followUp, inserted, err)
	}
	copiedPR, err := db.GetPullRequest(ctx, followUp.ID)
	wantPR := originalPR
	wantPR.AttemptID = followUp.ID
	if err != nil || !reflect.DeepEqual(copiedPR, wantPR) {
		t.Fatalf("copied pull request = %#v, %v; want %#v", copiedPR, err, wantPR)
	}
	replayed, inserted, err := db.StartForgeEventAttempt(ctx, plan)
	if err != nil || inserted || !reflect.DeepEqual(replayed, followUp) {
		t.Fatalf("replayed follow-up = %#v, inserted=%t, err=%v; want %#v", replayed, inserted, err, followUp)
	}
}

func TestStartForgeEventAttemptRollsBackEntirePlannedSnapshotOnPreCommitFailure(t *testing.T) {
	db := openTestStore(t)
	record, original, _ := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(t.Context(), record.ID, original.ID); err != nil {
		t.Fatal(err)
	}
	event := testForgeEvent("forge-precommit-rollback", "review_comment")
	if _, err := db.PutForgeEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	plan, err := db.PlanForgeEventAttempt(t.Context(), []string{event.ID}, record.ID, "immutable prompt")
	if err != nil {
		t.Fatal(err)
	}
	plan.Attempt.ManifestJSON = []byte(`{"task_id":"planned"}`)
	plan.Attempt.ResourceSnapshot = []byte(`{"job":{"metadata":{"name":"planned"}},"secret":{"metadata":{"name":"planned"}}}`)
	plan.Attempt.ConfigDigest = "sha256:planned"
	if _, err := db.db.ExecContext(t.Context(), `CREATE TRIGGER reject_forge_transition BEFORE INSERT ON task_events WHEN NEW.trigger = 'webhook' BEGIN SELECT RAISE(FAIL, 'forced pre-commit failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.StartForgeEventAttempt(t.Context(), plan); err == nil || !strings.Contains(err.Error(), "forced pre-commit failure") {
		t.Fatalf("StartForgeEventAttempt error = %v", err)
	}
	current, err := db.GetTask(t.Context(), record.ID)
	if err != nil || current.CurrentAttemptID != original.ID || current.State != task.PR_OPEN {
		t.Fatalf("task after rollback = %#v, %v", current, err)
	}
	stored, err := db.GetForgeEvent(t.Context(), event.ID)
	if err != nil || stored.Status != ForgeEventPending || stored.AttemptID != "" || stored.TaskID != "" {
		t.Fatalf("event after rollback = %#v, %v", stored, err)
	}
	attempts, err := db.ListAttempts(t.Context(), record.ID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts after rollback = %#v, %v", attempts, err)
	}
}

func TestStartForgeEventAttemptRejectsStateOutsideMachineReset(t *testing.T) {
	db := openTestStore(t)
	record, attempt, _ := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(t.Context(), record.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(t.Context(), `UPDATE tasks SET state = ? WHERE id = ?`, task.CREATING_PR, record.ID); err != nil {
		t.Fatal(err)
	}
	event := testForgeEvent("invalid-follow-up-state", "review_comment")
	if _, err := db.PutForgeEvent(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	if _, _, err := startForgeEventAttemptForTest(t.Context(), db, event.ID, record.ID, "fix review"); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "invalid forge follow-up") {
		t.Fatalf("StartForgeEventAttempt invalid state error = %v", err)
	}
	attempts, err := db.ListAttempts(t.Context(), record.ID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts after rejected reset = %#v, %v", attempts, err)
	}
}

func TestRetryTaskOnceRebindsRunningForgeEventBatch(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()
	record, originalAttempt, originalPR := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(ctx, record.ID, originalAttempt.ID); err != nil {
		t.Fatalf("mark original logs exhausted: %v", err)
	}
	first := testForgeEvent("forge-retry-first", "review_comment")
	second := first
	second.ID, second.CommentID = "forge-retry-second", 502
	for _, event := range []ForgeEvent{first, second} {
		if _, err := db.PutForgeEvent(ctx, event); err != nil {
			t.Fatalf("put forge event: %v", err)
		}
	}
	prompt := "Original task: fix the parser; body=preserve quoted commas"
	failed, _, err := startForgeEventBatchForTest(ctx, db, []string{first.ID, second.ID}, record.ID, prompt)
	if err != nil {
		t.Fatalf("start forge follow-up: %v", err)
	}
	if err := db.Transition(ctx, record.ID, task.QUEUED, task.FAILED, TransitionParams{Reason: "follow-up failed", Trigger: "kubernetes"}); err != nil {
		t.Fatalf("fail follow-up: %v", err)
	}
	if err := db.MarkLogsExhausted(ctx, record.ID, failed.ID); err != nil {
		t.Fatalf("mark failed follow-up logs exhausted: %v", err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE task_attempts SET base_branch = task_branch WHERE id = ?`, failed.ID); err != nil {
		t.Fatalf("make failed follow-up branch fields legacy-shaped: %v", err)
	}

	retry, inserted, err := retryTaskOnceForForgeTest(ctx, db, record.ID, "retry-forge-follow-up")
	if err != nil {
		t.Fatalf("retry forge follow-up: %v", err)
	}
	if !inserted || retry.Number != 3 || retry.Prompt != prompt || retry.BaseBranch != originalPR.BaseBranch || retry.TaskBranch != originalPR.HeadBranch {
		t.Fatalf("forge retry = %#v, inserted=%t; want attempt 3 with immutable follow-up overrides", retry, inserted)
	}
	copiedPR, err := db.GetPullRequest(ctx, retry.ID)
	if err != nil {
		t.Fatalf("get retry pull request: %v", err)
	}
	if copiedPR.Number != originalPR.Number || copiedPR.URL != originalPR.URL || copiedPR.HeadBranch != originalPR.HeadBranch {
		t.Fatalf("retry pull request = %#v; want copied open PR %#v", copiedPR, originalPR)
	}
	associated, err := db.ListForgeEventsByAttempt(ctx, retry.ID)
	if err != nil || len(associated) != 2 {
		t.Fatalf("rebound forge events = %#v, %v", associated, err)
	}
	for _, event := range associated {
		if event.Status != ForgeEventRunning || event.AttemptID != retry.ID || event.TaskID != record.ID {
			t.Fatalf("rebound forge event = %#v; want running on retry", event)
		}
	}
	if old, err := db.ListForgeEventsByAttempt(ctx, failed.ID); err != nil || len(old) != 0 {
		t.Fatalf("failed attempt remains associated with forge event: %#v, %v", old, err)
	}
	replayed, inserted, err := retryTaskOnceForForgeTest(ctx, db, record.ID, "retry-forge-follow-up")
	if err != nil || inserted || replayed.ID != retry.ID || replayed.Prompt != prompt {
		t.Fatalf("retry replay = %#v, inserted=%t, err=%v", replayed, inserted, err)
	}
	attempts, err := db.ListAttempts(ctx, record.ID)
	if err != nil || len(attempts) != 3 {
		t.Fatalf("attempts after retry replay = %#v, %v; want three", attempts, err)
	}
}

func TestRetryTaskOnceRollsBackForgeRebindWhenPullRequestCannotBeCopied(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()
	record, original, _ := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(ctx, record.ID, original.ID); err != nil {
		t.Fatalf("mark original logs exhausted: %v", err)
	}
	event := testForgeEvent("forge-retry-rollback", "review_comment")
	if _, err := db.PutForgeEvent(ctx, event); err != nil {
		t.Fatalf("put forge event: %v", err)
	}
	failed, _, err := startForgeEventAttemptForTest(ctx, db, event.ID, record.ID, "preserve this prompt")
	if err != nil {
		t.Fatalf("start follow-up: %v", err)
	}
	if err := db.Transition(ctx, record.ID, task.QUEUED, task.FAILED, TransitionParams{Reason: "failed", Trigger: "kubernetes"}); err != nil {
		t.Fatalf("fail follow-up: %v", err)
	}
	if err := db.MarkLogsExhausted(ctx, record.ID, failed.ID); err != nil {
		t.Fatalf("mark follow-up logs exhausted: %v", err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE pull_requests SET state = 'failed' WHERE attempt_id = ?`, failed.ID); err != nil {
		t.Fatalf("make pull request uncopyable: %v", err)
	}

	if _, _, err := retryTaskOnceForForgeTest(ctx, db, record.ID, "rollback-retry"); !errors.Is(err, ErrConflict) {
		t.Fatalf("retry without open pull request = %v, want ErrConflict", err)
	}
	attempts, err := db.ListAttempts(ctx, record.ID)
	if err != nil || len(attempts) != 2 {
		t.Fatalf("attempts after rolled-back retry = %#v, %v", attempts, err)
	}
	current, err := db.GetTask(ctx, record.ID)
	if err != nil || current.CurrentAttemptID != failed.ID || current.State != task.FAILED {
		t.Fatalf("task after rolled-back retry = %#v, %v", current, err)
	}
	stored, err := db.GetForgeEvent(ctx, event.ID)
	if err != nil || stored.AttemptID != failed.ID || stored.Status != ForgeEventRunning {
		t.Fatalf("event after rolled-back retry = %#v, %v", stored, err)
	}
	if _, err := db.GetRetryIntent(ctx, record.ID, "rollback-retry"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back retry intent = %v, want not found", err)
	}
}

func testForgeEvent(id, kind string) ForgeEvent {
	event := ForgeEvent{
		ID:                id,
		Provider:          "bitbucket",
		Kind:              kind,
		Owner:             "acme",
		Repository:        "widget",
		PullRequestNumber: 42,
		CommitSHA:         "0123456789abcdef0123456789abcdef01234567",
		Branch:            "simpleswe/task-a1",
		CommentID:         501,
		CommentKind:       "comment",
		Title:             "Parser review",
		Body:              "Preserve quoted commas",
		Author:            "Ada",
		URL:               "https://bitbucket.example/acme/widget/pull-requests/42#comment-501",
	}
	if kind == "quality_gate_failed" {
		event.CommentID, event.CommentKind = 0, ""
	}
	return event
}

func putPendingForgeEventBatch(t *testing.T, db *Store, prefix string, size int) []string {
	t.Helper()
	eventIDs := make([]string, size)
	for i := range size {
		event := testForgeEvent(fmt.Sprintf("%s-%02d", prefix, i), "review_comment")
		event.CommentID += i
		event.URL = fmt.Sprintf("https://bitbucket.example/acme/widget/pull-requests/42#comment-%d", event.CommentID)
		if _, err := db.PutForgeEvent(t.Context(), event); err != nil {
			t.Fatalf("put batch member %d: %v", i, err)
		}
		eventIDs[i] = event.ID
	}
	return eventIDs
}

func mixedTerminalRunningForgeEventBatch(t *testing.T, db *Store, prefix string) (Task, []string) {
	t.Helper()
	record, original, _ := createOpenPullRequestTask(t, db)
	if err := db.MarkLogsExhausted(t.Context(), record.ID, original.ID); err != nil {
		t.Fatal(err)
	}
	eventIDs := putPendingForgeEventBatch(t, db, prefix, 4)
	_, _, err := startForgeEventBatchForTest(t.Context(), db, eventIDs, record.ID, "process partial terminal batch")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.MarkForgeEventHandled(t.Context(), eventIDs[0]); err != nil {
		t.Fatal(err)
	}
	historicalAt := stamp(time.Now().UTC().Add(-time.Hour))
	if _, err := db.db.ExecContext(t.Context(), `
		UPDATE forge_events
		SET status = 'failed', attempts = 7, last_error = 'unrelated historical failure',
		    failed_at = ?, updated_at = ?, next_attempt_at = NULL
		WHERE id = ?`, historicalAt, historicalAt, eventIDs[1]); err != nil {
		t.Fatal(err)
	}
	return record, eventIDs
}

func readForgeEventBatch(t *testing.T, db *Store, eventIDs []string) []ForgeEvent {
	t.Helper()
	events := make([]ForgeEvent, len(eventIDs))
	for i, id := range eventIDs {
		event, err := db.GetForgeEvent(t.Context(), id)
		if err != nil {
			t.Fatalf("get batch member %q: %v", id, err)
		}
		events[i] = event
	}
	return events
}

func assertForgeEventDeadline(t *testing.T, event ForgeEvent, want time.Time) {
	t.Helper()
	if event.NextAttemptAt == nil {
		t.Fatalf("forge event deadline is nil; want approximately %v", want)
	}
	if delta := event.NextAttemptAt.Sub(want); delta < -2*time.Second || delta > 2*time.Second {
		t.Fatalf("forge event deadline = %v; want approximately %v", event.NextAttemptAt, want)
	}
}

func startForgeEventBatchForTest(ctx context.Context, db *Store, eventIDs []string, taskID, prompt string) (Attempt, bool, error) {
	plan, err := db.PlanForgeEventAttempt(ctx, eventIDs, taskID, prompt)
	if err != nil {
		return Attempt{}, false, err
	}
	if len(plan.Attempt.ResourceSnapshot) == 0 {
		plan.Attempt.ManifestJSON = []byte(`{"task_id":"` + plan.Attempt.ID + `"}`)
		plan.Attempt.ResourceSnapshot = []byte(`{"job":{"metadata":{"name":"` + plan.Attempt.ID + `"}},"secret":{"metadata":{"name":"` + plan.Attempt.ID + `"}}}`)
		plan.Attempt.ConfigDigest = "sha256:" + plan.Attempt.ID
	}
	return db.StartForgeEventAttempt(ctx, plan)
}

func startForgeEventAttemptForTest(ctx context.Context, db *Store, eventID, taskID, prompt string) (Attempt, bool, error) {
	return startForgeEventBatchForTest(ctx, db, []string{eventID}, taskID, prompt)
}

func retryTaskOnceForForgeTest(ctx context.Context, db *Store, taskID, key string) (Attempt, bool, error) {
	plan, planned, err := db.PlanRetryAttempt(ctx, taskID, key)
	if err != nil || !planned {
		return plan.Attempt, false, err
	}
	plan.Attempt.ManifestJSON = []byte(`{"task_id":"` + plan.Attempt.ID + `"}`)
	plan.Attempt.ResourceSnapshot = []byte(`{"job":{"metadata":{"name":"` + plan.Attempt.ID + `"}},"secret":{"metadata":{"name":"` + plan.Attempt.ID + `"}}}`)
	plan.Attempt.ConfigDigest = "sha256:" + plan.Attempt.ID
	return db.StartPlannedRetryTaskOnce(ctx, taskID, key, plan)
}

func createOpenPullRequestTask(t *testing.T, db *Store) (Task, Attempt, PullRequest) {
	t.Helper()
	ctx := context.Background()
	record, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "fix the parser"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	path := []task.State{
		task.RECEIVED, task.QUEUED, task.CREATING_JOB, task.JOB_PENDING, task.RUNNING,
		task.AGENT_RUNNING, task.VALIDATING, task.COMMITTING, task.PUSHING, task.CREATING_PR,
	}
	for i := 1; i < len(path); i++ {
		if err := db.Transition(ctx, record.ID, path[i-1], path[i], TransitionParams{Reason: "advance", Trigger: "system"}); err != nil {
			t.Fatalf("transition %q -> %q: %v", path[i-1], path[i], err)
		}
	}
	attempt, err := db.CurrentAttempt(ctx, record.ID)
	if err != nil {
		t.Fatalf("current attempt: %v", err)
	}
	branch := "simpleswe/" + record.ID + "-a1"
	if _, err := db.db.ExecContext(ctx, `UPDATE task_attempts SET base_branch = 'main', task_branch = ? WHERE id = ?`, branch, attempt.ID); err != nil {
		t.Fatalf("set attempt branch identity: %v", err)
	}
	git := GitResult{AttemptID: attempt.ID, State: "pushed", Branch: branch, CommitSHA: "0123456789abcdef0123456789abcdef01234567"}
	pullRequest := PullRequest{AttemptID: attempt.ID, State: "open", Number: 42, URL: "https://bitbucket.org/acme/widget/pull-requests/42", Title: "fix the parser", HeadBranch: branch, BaseBranch: "main"}
	recordCandidateForTest(t, db, git, pullRequest)
	if err := db.RecordVerifiedPullRequest(ctx, git, pullRequest); err != nil {
		t.Fatalf("record verified pull request: %v", err)
	}
	if err := db.Transition(ctx, record.ID, task.CREATING_PR, task.PR_OPEN, TransitionParams{Reason: "pull request open", Trigger: "system"}); err != nil {
		t.Fatalf("transition to PR_OPEN: %v", err)
	}
	pullRequest, err = db.GetPullRequest(ctx, attempt.ID)
	if err != nil {
		t.Fatalf("get pull request: %v", err)
	}
	return record, attempt, pullRequest
}
