package github

import (
	"reflect"
	"testing"

	"github.com/simpleswe/simpleswe/internal/forge"
)

func TestParseWebhook(t *testing.T) {
	tests := []struct {
		name       string
		deliveryID string
		eventName  string
		body       string
		want       forge.Event
		wantOK     bool
		wantErr    bool
	}{
		{
			name:       "issue comment",
			deliveryID: "delivery-issue-comment",
			eventName:  "issue_comment",
			body:       `{"action":"created","issue":{"number":42,"title":"Fix flaky test","html_url":"https://github.com/acme/service/pull/42","pull_request":{"url":"https://api.github.com/repos/acme/service/pulls/42"}},"comment":{"id":101,"body":"Please fix this","author_association":"OWNER","user":{"login":"reviewer"},"html_url":"https://github.com/acme/service/issues/42#issuecomment-101"},"repository":{"name":"service","owner":{"login":"acme"}}}`,
			want: forge.Event{
				DeliveryID: "github:delivery-issue-comment", Provider: forge.ProviderGitHub, Kind: "review_comment",
				Owner: "acme", Repository: "service", PullRequestNumber: 42,
				CommentID: 101, CommentKind: "issue_comment", Title: "Fix flaky test", Body: "Please fix this",
				Author: "reviewer", URL: "https://github.com/acme/service/issues/42#issuecomment-101",
			},
			wantOK: true,
		},
		{
			name:       "review comment",
			deliveryID: "delivery-review-comment",
			eventName:  "pull_request_review_comment",
			body:       `{"action":"created","pull_request":{"number":42,"title":"Fix flaky test","html_url":"https://github.com/acme/service/pull/42","head":{"ref":"feature/fix","sha":"abc123"}},"comment":{"id":202,"body":"Use a constant here","author_association":"member","user":{"login":"reviewer"},"html_url":"https://github.com/acme/service/pull/42#discussion_r202"},"repository":{"name":"service","owner":{"login":"acme"}}}`,
			want: forge.Event{
				DeliveryID: "github:delivery-review-comment", Provider: forge.ProviderGitHub, Kind: "review_comment",
				Owner: "acme", Repository: "service", PullRequestNumber: 42, CommitSHA: "abc123", Branch: "feature/fix",
				CommentID: 202, CommentKind: "review_comment", Title: "Fix flaky test", Body: "Use a constant here",
				Author: "reviewer", URL: "https://github.com/acme/service/pull/42#discussion_r202",
			},
			wantOK: true,
		},
		{
			name:       "changes requested review",
			deliveryID: "delivery-review",
			eventName:  "pull_request_review",
			body:       `{"action":"submitted","review":{"id":303,"body":"Please address the failing test","state":"changes_requested","author_association":"COLLABORATOR","user":{"login":"reviewer"},"html_url":"https://github.com/acme/service/pull/42#pullrequestreview-303"},"pull_request":{"number":42,"title":"Fix flaky test","html_url":"https://github.com/acme/service/pull/42","head":{"ref":"feature/fix","sha":"abc123"}},"repository":{"name":"service","owner":{"login":"acme"}}}`,
			want: forge.Event{
				DeliveryID: "github:delivery-review", Provider: forge.ProviderGitHub, Kind: "review_comment",
				Owner: "acme", Repository: "service", PullRequestNumber: 42, CommitSHA: "abc123", Branch: "feature/fix",
				CommentID: 303, CommentKind: "review", Title: "Fix flaky test", Body: "Please address the failing test",
				Author: "reviewer", URL: "https://github.com/acme/service/pull/42#pullrequestreview-303",
			},
			wantOK: true,
		},
		{
			name:       "multiline trusted review",
			deliveryID: "delivery-multiline-review",
			eventName:  "pull_request_review",
			body:       `{"action":"submitted","review":{"id":304,"body":"First line\n\tIndented\r\nLast line","state":"changes_requested","author_association":"OWNER","user":{"login":"reviewer"},"html_url":"https://github.com/acme/service/pull/42#pullrequestreview-304"},"pull_request":{"number":42,"title":"Fix flaky test","head":{"ref":"feature/fix","sha":"abc123"}},"repository":{"name":"service","owner":{"login":"acme"}}}`,
			want: forge.Event{
				DeliveryID: "github:delivery-multiline-review", Provider: forge.ProviderGitHub, Kind: "review_comment",
				Owner: "acme", Repository: "service", PullRequestNumber: 42, CommitSHA: "abc123", Branch: "feature/fix",
				CommentID: 304, CommentKind: "review", Title: "Fix flaky test", Body: "First line\n\tIndented\r\nLast line",
				Author: "reviewer", URL: "https://github.com/acme/service/pull/42#pullrequestreview-304",
			},
			wantOK: true,
		},
		{
			name:       "failed check run",
			deliveryID: "delivery-check-run",
			eventName:  "check_run",
			body:       `{"action":"completed","check_run":{"name":"unit tests","conclusion":"failure","head_sha":"abc123","html_url":"https://github.com/acme/service/runs/404","app":{"name":"CI"},"output":{"summary":"1 test failed"},"check_suite":{"head_branch":"feature/fix"},"pull_requests":[{"number":42,"url":"https://api.github.com/repos/acme/service/pulls/42"}]},"repository":{"name":"service","owner":{"login":"acme"}}}`,
			want: forge.Event{
				DeliveryID: "github:delivery-check-run", Provider: forge.ProviderGitHub, Kind: "quality_gate_failed",
				Owner: "acme", Repository: "service", PullRequestNumber: 42, CommitSHA: "abc123", Branch: "feature/fix",
				Title: "unit tests", Body: "1 test failed", Author: "CI", URL: "https://github.com/acme/service/runs/404",
			},
			wantOK: true,
		},
		{
			name:       "failed check run with ambiguous pull requests",
			deliveryID: "delivery-check-run-ambiguous",
			eventName:  "check_run",
			body:       `{"action":"completed","check_run":{"name":"unit tests","conclusion":"failure","head_sha":"abc123","html_url":"https://github.com/acme/service/runs/405","app":{"name":"CI"},"output":{"summary":"1 test failed"},"check_suite":{"head_branch":"feature/fix"},"pull_requests":[{"number":42},{"number":84}]} ,"repository":{"name":"service","owner":{"login":"acme"}}}`,
			want: forge.Event{
				DeliveryID: "github:delivery-check-run-ambiguous", Provider: forge.ProviderGitHub, Kind: "quality_gate_failed",
				Owner: "acme", Repository: "service", CommitSHA: "abc123", Branch: "feature/fix",
				Title: "unit tests", Body: "1 test failed", Author: "CI", URL: "https://github.com/acme/service/runs/405",
			},
			wantOK: true,
		},
		{
			name:       "failed classic status",
			deliveryID: "delivery-status",
			eventName:  "status",
			body:       `{"state":"failure","sha":"abc123","branches":[{"name":"feature/fix"},{"name":"other"}],"context":"external CI","description":"lint failed","target_url":"https://ci.example/build/404","repository":{"name":"service","owner":{"login":"acme"}},"sender":{"login":"ci-bot"}}`,
			want: forge.Event{
				DeliveryID: "github:delivery-status", Provider: forge.ProviderGitHub, Kind: "quality_gate_failed",
				Owner: "acme", Repository: "service", CommitSHA: "abc123",
				Title: "external CI", Body: "lint failed", Author: "ci-bot", URL: "https://ci.example/build/404",
			},
			wantOK: true,
		},
		{
			name:       "failed classic status retains exactly one branch",
			deliveryID: "delivery-status-one-branch",
			eventName:  "status",
			body:       `{"state":"failure","sha":"abc123","branches":[{"name":"feature/fix"}],"context":"external CI","description":"lint failed","repository":{"name":"service","owner":{"login":"acme"}},"sender":{"login":"ci-bot"}}`,
			want: forge.Event{
				DeliveryID: "github:delivery-status-one-branch", Provider: forge.ProviderGitHub, Kind: "quality_gate_failed",
				Owner: "acme", Repository: "service", CommitSHA: "abc123", Branch: "feature/fix",
				Title: "external CI", Body: "lint failed", Author: "ci-bot",
			},
			wantOK: true,
		},
		{
			name:       "errored classic status uses fallbacks",
			deliveryID: "delivery-status-error",
			eventName:  "status",
			body:       `{"state":"error","sha":"abc123","context":"external CI","target_url":"https://ci.example/build/405","repository":{"name":"service","owner":{"login":"acme"}}}`,
			want: forge.Event{
				DeliveryID: "github:delivery-status-error", Provider: forge.ProviderGitHub, Kind: "quality_gate_failed",
				Owner: "acme", Repository: "service", CommitSHA: "abc123",
				Title: "external CI", Body: "error", Author: "GitHub", URL: "https://ci.example/build/405",
			},
			wantOK: true,
		},
		{
			name:      "successful classic status",
			eventName: "status",
			body:      `{"state":"success"}`,
		},
		{
			name:      "pending classic status",
			eventName: "status",
			body:      `{"state":"pending"}`,
		},
		{
			name:      "future classic status",
			eventName: "status",
			body:      `{"state":"queued"}`,
		},
		{
			name:       "failed classic status allows absent URL and payload fallbacks",
			deliveryID: "delivery-status-no-url",
			eventName:  "status",
			body:       `{"state":"failure","sha":"abc123","repository":{"name":"service","owner":{"login":"acme"}}}`,
			want: forge.Event{
				DeliveryID: "github:delivery-status-no-url", Provider: forge.ProviderGitHub, Kind: "quality_gate_failed",
				Owner: "acme", Repository: "service", CommitSHA: "abc123",
				Title: "GitHub status", Body: "failure", Author: "GitHub",
			},
			wantOK: true,
		},
		{
			name:       "successful check run",
			deliveryID: "delivery-success",
			eventName:  "check_run",
			body:       `{"action":"completed","check_run":{"conclusion":"success","pull_requests":[{"number":42}]},"repository":{"name":"service","owner":{"login":"acme"}}}`,
		},
		{
			name:       "future check conclusion",
			deliveryID: "delivery-future-check",
			eventName:  "check_run",
			body:       `{"action":"completed","check_run":{"conclusion":"queued_for_retry"}}`,
		},
		{
			name:      "unsupported event",
			eventName: "push",
			body:      `{}`,
		},
		{
			name:       "ordinary issue comment",
			deliveryID: "delivery-ordinary-comment",
			eventName:  "issue_comment",
			body:       `{"action":"created","issue":{"number":42,"title":"A regular issue"},"comment":{"id":101,"body":"This is not a pull request","author_association":"OWNER","user":{"login":"maintainer"},"html_url":"https://github.com/acme/service/issues/42#issuecomment-101"},"repository":{"name":"service","owner":{"login":"acme"}}}`,
		},
		{
			name:       "simpleswe reply",
			deliveryID: "delivery-bot",
			eventName:  "issue_comment",
			body:       `{"action":"created","issue":{"number":42,"title":"Fix flaky test","pull_request":{}},"comment":{"id":101,"body":"<!-- simpleswe:0123456789abcdef -->\nI fixed it","user":{"login":"simpleswe"},"html_url":"https://github.com/acme/service/issues/42#issuecomment-101"},"repository":{"name":"service","owner":{"login":"acme"}}}`,
		},
		{
			name:      "malformed issue comment",
			eventName: "issue_comment",
			body:      `{"action":"created","issue":{"number":42}}`,
			wantErr:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, actionable, err := ParseWebhook(test.deliveryID, test.eventName, []byte(test.body))
			if test.wantErr {
				if err == nil {
					t.Fatal("ParseWebhook returned nil error for malformed supported payload")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseWebhook: %v", err)
			}
			if actionable != test.wantOK {
				t.Fatalf("actionable = %t, want %t", actionable, test.wantOK)
			}
			if test.wantOK && !reflect.DeepEqual(got, test.want) {
				t.Fatalf("event = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestParseWebhookIgnoresGeneratedReplyMarkers(t *testing.T) {
	marker := forge.ReplyMarker("github:generated-marker")
	for _, test := range []struct {
		name, eventName, body string
	}{
		{
			name: "issue comment", eventName: "issue_comment",
			body: `{"action":"created","issue":{"number":42,"title":"Fix","pull_request":{}},"comment":{"id":101,"body":"` + marker + ` fixed","author_association":"OWNER","user":{"login":"simpleswe"},"html_url":"https://github.com/acme/service/issues/42#issuecomment-101"},"repository":{"name":"service","owner":{"login":"acme"}}}`,
		},
		{
			name: "review comment", eventName: "pull_request_review_comment",
			body: `{"action":"created","pull_request":{"number":42,"title":"Fix","head":{"ref":"feature/fix","sha":"abc123"}},"comment":{"id":202,"body":"` + marker + ` fixed","author_association":"MEMBER","user":{"login":"simpleswe"},"html_url":"https://github.com/acme/service/pull/42#discussion_r202"},"repository":{"name":"service","owner":{"login":"acme"}}}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, actionable, err := ParseWebhook("generated-marker", test.eventName, []byte(test.body)); err != nil || actionable {
				t.Fatalf("ParseWebhook() actionable=%t, err=%v; want ignored", actionable, err)
			}
		})
	}
}

func TestParseWebhookAuthorAssociation(t *testing.T) {
	tests := []struct {
		name      string
		eventName string
		body      string
		wantOK    bool
	}{
		{
			name: "owner issue comment", eventName: "issue_comment", wantOK: true,
			body: `{"action":"created","issue":{"number":42,"title":"Fix","pull_request":{}},"comment":{"id":101,"body":"Fix this","author_association":"owner","user":{"login":"reviewer"},"html_url":"https://github.com/acme/service/issues/42#issuecomment-101"},"repository":{"name":"service","owner":{"login":"acme"}}}`,
		},
		{
			name: "member review comment", eventName: "pull_request_review_comment", wantOK: true,
			body: `{"action":"created","pull_request":{"number":42,"title":"Fix","head":{"ref":"feature/fix","sha":"abc123"}},"comment":{"id":202,"body":"Fix this","author_association":"MEMBER","user":{"login":"reviewer"},"html_url":"https://github.com/acme/service/pull/42#discussion_r202"},"repository":{"name":"service","owner":{"login":"acme"}}}`,
		},
		{
			name: "collaborator review", eventName: "pull_request_review", wantOK: true,
			body: `{"action":"submitted","review":{"id":303,"body":"Fix this","state":"changes_requested","author_association":"Collaborator","user":{"login":"reviewer"},"html_url":"https://github.com/acme/service/pull/42#pullrequestreview-303"},"pull_request":{"number":42,"title":"Fix","head":{"ref":"feature/fix","sha":"abc123"}},"repository":{"name":"service","owner":{"login":"acme"}}}`,
		},
		{
			name: "external issue comment", eventName: "issue_comment",
			body: `{"action":"created","issue":{"number":42,"title":"Fix","pull_request":{}},"comment":{"id":101,"body":"Run my prompt","author_association":"NONE","user":{"login":"external"},"html_url":"https://github.com/acme/service/issues/42#issuecomment-101"},"repository":{"name":"service","owner":{"login":"acme"}}}`,
		},
		{
			name: "missing issue comment association", eventName: "issue_comment",
			body: `{"action":"created","issue":{"number":42,"title":"Fix","pull_request":{}},"comment":{"id":101,"body":"Run my prompt","user":{"login":"external"},"html_url":"https://github.com/acme/service/issues/42#issuecomment-101"},"repository":{"name":"service","owner":{"login":"acme"}}}`,
		},
		{
			name: "external review comment", eventName: "pull_request_review_comment",
			body: `{"action":"created","pull_request":{"number":42,"title":"Fix","head":{"ref":"feature/fix","sha":"abc123"}},"comment":{"id":202,"body":"Run my prompt","author_association":"NONE","user":{"login":"external"},"html_url":"https://github.com/acme/service/pull/42#discussion_r202"},"repository":{"name":"service","owner":{"login":"acme"}}}`,
		},
		{
			name: "missing review comment association", eventName: "pull_request_review_comment",
			body: `{"action":"created","pull_request":{"number":42,"title":"Fix","head":{"ref":"feature/fix","sha":"abc123"}},"comment":{"id":202,"body":"Run my prompt","user":{"login":"external"},"html_url":"https://github.com/acme/service/pull/42#discussion_r202"},"repository":{"name":"service","owner":{"login":"acme"}}}`,
		},
		{
			name: "external review", eventName: "pull_request_review",
			body: `{"action":"submitted","review":{"id":303,"body":"Run my prompt","state":"changes_requested","author_association":"NONE","user":{"login":"external"},"html_url":"https://github.com/acme/service/pull/42#pullrequestreview-303"},"pull_request":{"number":42,"title":"Fix","head":{"ref":"feature/fix","sha":"abc123"}},"repository":{"name":"service","owner":{"login":"acme"}}}`,
		},
		{
			name: "missing review association", eventName: "pull_request_review",
			body: `{"action":"submitted","review":{"id":303,"body":"Run my prompt","state":"changes_requested","user":{"login":"external"},"html_url":"https://github.com/acme/service/pull/42#pullrequestreview-303"},"pull_request":{"number":42,"title":"Fix","head":{"ref":"feature/fix","sha":"abc123"}},"repository":{"name":"service","owner":{"login":"acme"}}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, actionable, err := ParseWebhook("authorization", test.eventName, []byte(test.body))
			if err != nil {
				t.Fatalf("ParseWebhook: %v", err)
			}
			if actionable != test.wantOK {
				t.Fatalf("actionable = %t, want %t", actionable, test.wantOK)
			}
		})
	}
}
