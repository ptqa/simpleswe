package bitbucket

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/simpleswe/simpleswe/internal/forge"
)

func quotedJSON(value string) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func TestParseWebhook(t *testing.T) {
	tests := []struct {
		name        string
		requestUUID string
		eventKey    string
		body        string
		want        forge.Event
		wantOK      bool
		wantErr     bool
	}{
		{
			name:        "pull request comment",
			requestUUID: "request-comment",
			eventKey:    "pullrequest:comment_created",
			body:        `{"comment":{"id":501,"content":{"raw":"Please fix this"},"user":{"uuid":"{reviewer}","nickname":"reviewer","display_name":"reviewer"},"links":{"html":{"href":"https://bitbucket.org/acme/service/pull-requests/42/_/diff#comment-501"}}},"pullrequest":{"id":42,"title":"Fix flaky test","reviewers":[{"uuid":"{reviewer}"}],"links":{"html":{"href":"https://bitbucket.org/acme/service/pull-requests/42"}},"source":{"branch":{"name":"feature/fix"},"commit":{"hash":"abc123"}}},"repository":{"name":"Service Display Name","slug":"service","workspace":{"slug":"acme"}}}`,
			want: forge.Event{
				DeliveryID: "bitbucket:request-comment", Provider: forge.ProviderBitbucket, Kind: "review_comment",
				Owner: "acme", Repository: "service", PullRequestNumber: 42, CommitSHA: "abc123", Branch: "feature/fix",
				CommentID: 501, CommentKind: "comment", Title: "Fix flaky test", Body: "Please fix this",
				Author: "reviewer", URL: "https://bitbucket.org/acme/service/pull-requests/42/_/diff#comment-501",
			},
			wantOK: true,
		},
		{
			name:        "pull request comment repository name fallback",
			requestUUID: "request-comment-name",
			eventKey:    "pullrequest:comment_created",
			body:        `{"comment":{"id":502,"content":{"raw":"Please fix this"},"user":{"uuid":"{commenter}","nickname":"commenter"},"links":{"html":{"href":"https://bitbucket.org/acme/service/pull-requests/43/_/diff#comment-502"}}},"pullrequest":{"id":43,"title":"Fix test"},"repository":{"name":"service","workspace":{"slug":"acme"}}}`,
			want: forge.Event{
				DeliveryID: "bitbucket:request-comment-name", Provider: forge.ProviderBitbucket, Kind: "review_comment",
				Owner: "acme", Repository: "service", PullRequestNumber: 43, CommentID: 502,
				CommentKind: "comment", Title: "Fix test", Body: "Please fix this", Author: "commenter",
				URL: "https://bitbucket.org/acme/service/pull-requests/43/_/diff#comment-502",
			},
			wantOK: true,
		},
		{
			name:        "pull request changes requested",
			requestUUID: "request-changes",
			eventKey:    "pullrequest:changes_request_created",
			body:        bitbucketChangesRequestPayload,
			want: forge.Event{
				DeliveryID: "bitbucket:request-changes", Provider: forge.ProviderBitbucket, Kind: "review_comment",
				Owner: "acme", Repository: "service", PullRequestNumber: 42, CommitSHA: "abc123", Branch: "feature/fix",
				CommentKind: "changes_request", Title: "Fix flaky test", Body: "Changes requested", Author: "reviewer",
				URL: "https://bitbucket.org/acme/service/pull-requests/42",
			},
			wantOK: true,
		},
		{
			name:        "pull request changes requested requires defining object",
			requestUUID: "request-changes-incomplete",
			eventKey:    "pullrequest:changes_request_created",
			body:        `{"actor":{"nickname":"reviewer"},"repository":{"slug":"service","workspace":{"slug":"acme"}},"pullrequest":{"id":42,"title":"Fix flaky test","links":{"html":{"href":"https://bitbucket.org/acme/service/pull-requests/42"}},"source":{"branch":{"name":"feature/fix"},"commit":{"hash":"abc123"}}}}`,
			wantErr:     true,
		},
		{
			name:        "failed pipeline",
			requestUUID: "request-pipeline",
			eventKey:    "pipeline:build:completed",
			body:        `{"actor":{"display_name":"Bitbucket Pipelines"},"repository":{"name":"Service Display Name","slug":"service","workspace":{"slug":"acme"}},"pipeline":{"uuid":"{pipeline-uuid}","build_number":404,"state":{"name":"COMPLETED","result":{"name":"FAILED"}},"target":{"ref_name":"feature/fix","commit":{"hash":"abc123"}},"links":{"self":{"href":"https://api.bitbucket.org/2.0/repositories/acme/service/pipelines/{pipeline-uuid}"}}}}`,
			want: forge.Event{
				DeliveryID: "bitbucket:request-pipeline", Provider: forge.ProviderBitbucket, Kind: "quality_gate_failed",
				Owner: "acme", Repository: "service", CommitSHA: "abc123", Branch: "feature/fix",
				Title: "Pipeline #404", Body: "FAILED", Author: "Bitbucket Pipelines", URL: "https://api.bitbucket.org/2.0/repositories/acme/service/pipelines/{pipeline-uuid}",
			},
			wantOK: true,
		},
		{
			name:        "failed external commit status",
			requestUUID: "request-status",
			eventKey:    "repo:commit_status_updated",
			body:        `{"actor":{"display_name":"External CI","nickname":"ci-bot"},"repository":{"name":"Service Display Name","slug":"service","workspace":{"slug":"acme"}},"commit_status":{"name":"unit tests","key":"ci/unit","description":"1 test failed","state":"FAILED","url":"https://ci.example/build/404","refname":"feature/fix","links":{"commit":{"href":"https://api.bitbucket.org/2.0/repositories/acme/service/commit/abc123"},"self":{"href":"https://api.bitbucket.org/2.0/repositories/acme/service/commit/abc123/statuses/build/ci%2Funit"}}}}`,
			want: forge.Event{
				DeliveryID: "bitbucket:request-status", Provider: forge.ProviderBitbucket, Kind: "quality_gate_failed",
				Owner: "acme", Repository: "service", CommitSHA: "abc123", Branch: "feature/fix",
				Title: "unit tests", Body: "1 test failed", Author: "External CI", URL: "https://ci.example/build/404",
			},
			wantOK: true,
		},
		{
			name:        "created failed external commit status",
			requestUUID: "request-status-created-failed",
			eventKey:    "repo:commit_status_created",
			body:        `{"actor":{"display_name":"External CI"},"repository":{"slug":"service","workspace":{"slug":"acme"}},"commit_status":{"name":"unit tests","description":"failed","state":"FAILED","links":{"commit":{"href":"https://api.bitbucket.org/2.0/repositories/acme/service/commit/abc123"}}}}`,
			want: forge.Event{
				DeliveryID: "bitbucket:request-status-created-failed", Provider: forge.ProviderBitbucket, Kind: "quality_gate_failed",
				Owner: "acme", Repository: "service", CommitSHA: "abc123", Title: "unit tests", Body: "failed", Author: "External CI",
			},
			wantOK: true,
		},
		{
			name:        "created errored external commit status",
			requestUUID: "request-status-created-error",
			eventKey:    "repo:commit_status_created",
			body:        `{"actor":{"nickname":"ci"},"repository":{"slug":"service","workspace":{"slug":"acme"}},"commit_status":{"key":"ci/unit","state":"ERROR","links":{"commit":{"href":"https://api.bitbucket.org/2.0/repositories/acme/service/commit/abc123"}}}}`,
			want: forge.Event{
				DeliveryID: "bitbucket:request-status-created-error", Provider: forge.ProviderBitbucket, Kind: "quality_gate_failed",
				Owner: "acme", Repository: "service", CommitSHA: "abc123", Title: "ci/unit", Body: "ERROR", Author: "ci",
			},
			wantOK: true,
		},
		{
			name:     "created successful external commit status",
			eventKey: "repo:commit_status_created",
			body:     `{"commit_status":{"state":"SUCCESSFUL"}}`,
		},
		{
			name:        "errored external commit status uses fallbacks",
			requestUUID: "request-status-error",
			eventKey:    "repo:commit_status_updated",
			body:        `{"actor":{"nickname":"ci-bot"},"repository":{"name":"Service Display Name","slug":"service","workspace":{"slug":"acme"}},"commit_status":{"key":"ci/unit","state":"ERROR","refname":"feature/fix","links":{"commit":{"href":"https://api.bitbucket.org/2.0/repositories/acme/service/commit/abc%31%32%33"},"self":{"href":"https://api.bitbucket.org/2.0/repositories/acme/service/commit/abc123/statuses/build/ci%2Funit"}}}}`,
			want: forge.Event{
				DeliveryID: "bitbucket:request-status-error", Provider: forge.ProviderBitbucket, Kind: "quality_gate_failed",
				Owner: "acme", Repository: "service", CommitSHA: "abc123", Branch: "feature/fix",
				Title: "ci/unit", Body: "ERROR", Author: "ci-bot", URL: "https://api.bitbucket.org/2.0/repositories/acme/service/commit/abc123/statuses/build/ci%2Funit",
			},
			wantOK: true,
		},
		{
			name:        "stopped external commit status",
			requestUUID: "request-status-stopped",
			eventKey:    "repo:commit_status_updated",
			body:        `{"actor":{"display_name":"External CI"},"repository":{"name":"Service Display Name","slug":"service","workspace":{"slug":"acme"}},"commit_status":{"name":"unit tests","description":"Stopped by CI","state":"STOPPED","links":{"commit":{"href":"https://api.bitbucket.org/2.0/repositories/acme/service/commit/abc123"}}}}`,
			want: forge.Event{
				DeliveryID: "bitbucket:request-status-stopped", Provider: forge.ProviderBitbucket, Kind: "quality_gate_failed",
				Owner: "acme", Repository: "service", CommitSHA: "abc123",
				Title: "unit tests", Body: "Stopped by CI", Author: "External CI",
			},
			wantOK: true,
		},
		{
			name:     "successful external commit status",
			eventKey: "repo:commit_status_updated",
			body:     `{"commit_status":{"state":"SUCCESSFUL"}}`,
		},
		{
			name:     "in progress external commit status",
			eventKey: "repo:commit_status_updated",
			body:     `{"commit_status":{"state":"INPROGRESS"}}`,
		},
		{
			name:     "future external commit status",
			eventKey: "repo:commit_status_updated",
			body:     `{"commit_status":{"state":"QUEUED"}}`,
		},
		{
			name:        "failed external commit status requires repository identity",
			requestUUID: "request-status-no-slug",
			eventKey:    "repo:commit_status_updated",
			body:        `{"actor":{"display_name":"External CI"},"repository":{"workspace":{"slug":"acme"}},"commit_status":{"name":"unit tests","description":"failed","state":"FAILED","links":{"commit":{"href":"https://api.bitbucket.org/2.0/repositories/acme/service/commit/abc123"}}}}`,
			wantErr:     true,
		},
		{
			name:        "successful pipeline",
			requestUUID: "request-success",
			eventKey:    "pipeline:build:completed",
			body:        `{"repository":{"name":"service","workspace":{"slug":"acme"}},"pipeline":{"state":{"name":"COMPLETED","result":{"name":"SUCCESSFUL"}}}}`,
		},
		{
			name:     "in progress pipeline",
			eventKey: "pipeline:build:completed",
			body:     `{"pipeline":{"state":{"result":{"name":"IN_PROGRESS"}}}}`,
		},
		{
			name:     "future pipeline result",
			eventKey: "pipeline:build:completed",
			body:     `{"pipeline":{"state":{"result":{"name":"PAUSED"}}}}`,
		},
		{
			name:     "unsupported event",
			eventKey: "repo:push",
			body:     `{}`,
		},
		{
			name:        "simpleswe reply",
			requestUUID: "request-bot",
			eventKey:    "pullrequest:comment_created",
			body:        `{"comment":{"id":501,"content":{"raw":"<!-- simpleswe:0123456789abcdef -->\nI fixed it"},"user":{"nickname":"simpleswe"},"links":{"html":{"href":"https://bitbucket.org/acme/service/pull-requests/42/_/diff#comment-501"}}},"pullrequest":{"id":42,"title":"Fix flaky test","links":{"html":{"href":"https://bitbucket.org/acme/service/pull-requests/42"}}},"repository":{"name":"service","workspace":{"slug":"acme"}}}`,
		},
		{
			name:     "malformed pull request comment",
			eventKey: "pullrequest:comment_created",
			body:     `{"comment":{"id":501}}`,
			wantErr:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, actionable, err := ParseWebhook(test.requestUUID, test.eventKey, []byte(test.body))
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

func TestParseWebhookIgnoresGeneratedReplyMarkerFromAssignedReviewer(t *testing.T) {
	body := `{"comment":{"id":501,"content":{"raw":` + quotedJSON(forge.ReplyMarker("bitbucket:generated-marker")+" fixed") + `},"user":{"uuid":"{reviewer}","nickname":"reviewer"},"links":{"html":{"href":"https://bitbucket.org/acme/service/pull-requests/42/_/diff#comment-501"}}},"pullrequest":{"id":42,"title":"Fix","reviewers":[{"uuid":"{reviewer}"}]},"repository":{"slug":"service","workspace":{"slug":"acme"}}}`
	if _, actionable, err := ParseWebhook("generated-marker", "pullrequest:comment_created", []byte(body)); err != nil || actionable {
		t.Fatalf("ParseWebhook() actionable=%t, err=%v; want ignored", actionable, err)
	}
}

const bitbucketChangesRequestPayload = `{"actor":{"nickname":"reviewer","display_name":"Reviewer Display Name"},"repository":{"name":"Service Display Name","slug":"service","workspace":{"slug":"acme"}},"pullrequest":{"id":42,"title":"Fix flaky test","links":{"html":{"href":"https://bitbucket.org/acme/service/pull-requests/42"}},"source":{"branch":{"name":"feature/fix"},"commit":{"hash":"abc123"}}},"changes_request":{"date":"2015-04-06T16:34:59.195330+00:00","user":{"nickname":"reviewer","display_name":"Reviewer Display Name"}}}`
