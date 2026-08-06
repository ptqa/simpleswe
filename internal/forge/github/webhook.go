package github

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/simpleswe/simpleswe/internal/forge"
)

const githubDeliveryPrefix = "github:"

// ParseWebhook normalizes actionable GitHub webhook payloads without keeping
// the provider payload in the returned event.
func ParseWebhook(deliveryID, eventName string, body []byte) (forge.Event, bool, error) {
	switch eventName {
	case "issue_comment":
		var payload githubIssueComment
		if err := decodeWebhook(body, &payload); err != nil {
			return forge.Event{}, false, err
		}
		if payload.Action != "created" {
			return forge.Event{}, false, nil
		}
		if forge.ContainsReplyMarker(payload.Comment.Body) {
			return forge.Event{}, false, nil
		}
		if strings.TrimSpace(payload.Comment.Body) == "" {
			if payload.Comment.ID <= 0 || payload.Comment.HTMLURL == "" {
				return forge.Event{}, false, errors.New("GitHub issue comment is incomplete")
			}
			return forge.Event{}, false, nil
		}
		if payload.Issue.PullRequest == nil {
			return forge.Event{}, false, nil
		}
		event := githubCommentEvent(deliveryID, "issue_comment", payload.Issue.Number, payload.Issue.Title,
			payload.Comment.ID, payload.Comment.Body, payload.Comment.User.Login, payload.Comment.HTMLURL,
			payload.Repository.Owner.Login, payload.Repository.Name)
		return finishGitHubEvent(event, true, trustedAuthorAssociation(payload.Comment.AuthorAssociation))

	case "pull_request_review_comment":
		var payload githubReviewComment
		if err := decodeWebhook(body, &payload); err != nil {
			return forge.Event{}, false, err
		}
		if payload.Action != "created" {
			return forge.Event{}, false, nil
		}
		if forge.ContainsReplyMarker(payload.Comment.Body) {
			return forge.Event{}, false, nil
		}
		if strings.TrimSpace(payload.Comment.Body) == "" {
			if payload.Comment.ID <= 0 || payload.Comment.HTMLURL == "" {
				return forge.Event{}, false, errors.New("GitHub review comment is incomplete")
			}
			return forge.Event{}, false, nil
		}
		event := githubCommentEvent(deliveryID, "review_comment", payload.PullRequest.Number, payload.PullRequest.Title,
			payload.Comment.ID, payload.Comment.Body, payload.Comment.User.Login, payload.Comment.HTMLURL,
			payload.Repository.Owner.Login, payload.Repository.Name)
		event.CommitSHA = payload.PullRequest.Head.SHA
		event.Branch = payload.PullRequest.Head.Ref
		return finishGitHubEvent(event, true, trustedAuthorAssociation(payload.Comment.AuthorAssociation))

	case "pull_request_review":
		var payload githubReview
		if err := decodeWebhook(body, &payload); err != nil {
			return forge.Event{}, false, err
		}
		if payload.Action != "submitted" || strings.ToLower(payload.Review.State) != "changes_requested" || strings.TrimSpace(payload.Review.Body) == "" || forge.ContainsReplyMarker(payload.Review.Body) {
			return forge.Event{}, false, nil
		}
		event := forge.Event{
			DeliveryID:        githubDeliveryID(deliveryID),
			Provider:          forge.ProviderGitHub,
			Kind:              "review_comment",
			Owner:             payload.Repository.Owner.Login,
			Repository:        payload.Repository.Name,
			PullRequestNumber: payload.PullRequest.Number,
			CommitSHA:         payload.PullRequest.Head.SHA,
			Branch:            payload.PullRequest.Head.Ref,
			CommentID:         payload.Review.ID,
			CommentKind:       "review",
			Title:             payload.PullRequest.Title,
			Body:              payload.Review.Body,
			Author:            payload.Review.User.Login,
			URL:               payload.Review.HTMLURL,
		}
		return finishGitHubEvent(event, true, trustedAuthorAssociation(payload.Review.AuthorAssociation))

	case "check_run":
		var payload githubCheckRun
		if err := decodeWebhook(body, &payload); err != nil {
			return forge.Event{}, false, err
		}
		if payload.Action != "completed" {
			return forge.Event{}, false, nil
		}
		if !githubFailedConclusion(payload.CheckRun.Conclusion) {
			return forge.Event{}, false, nil
		}
		var pullRequestNumber int
		if len(payload.CheckRun.PullRequests) == 1 {
			pullRequestNumber = payload.CheckRun.PullRequests[0].Number
		}
		event := forge.Event{
			DeliveryID:        githubDeliveryID(deliveryID),
			Provider:          forge.ProviderGitHub,
			Kind:              "quality_gate_failed",
			Owner:             payload.Repository.Owner.Login,
			Repository:        payload.Repository.Name,
			PullRequestNumber: pullRequestNumber,
			CommitSHA:         payload.CheckRun.HeadSHA,
			Branch:            payload.CheckRun.CheckSuite.HeadBranch,
			Title:             firstNonblank(payload.CheckRun.Name, "GitHub check"),
			Body:              firstNonblank(payload.CheckRun.Output.Summary, payload.CheckRun.Conclusion),
			Author:            firstNonblank(payload.CheckRun.App.Name, "GitHub"),
			URL:               payload.CheckRun.HTMLURL,
		}
		return finishGitHubEvent(event, false, true)

	case "status":
		var payload githubStatus
		if err := decodeWebhook(body, &payload); err != nil {
			return forge.Event{}, false, err
		}
		if state := strings.ToLower(payload.State); state != "failure" && state != "error" {
			return forge.Event{}, false, nil
		}
		var branch string
		if len(payload.Branches) == 1 {
			branch = payload.Branches[0].Name
		}
		event := forge.Event{
			DeliveryID: githubDeliveryID(deliveryID),
			Provider:   forge.ProviderGitHub,
			Kind:       "quality_gate_failed",
			Owner:      payload.Repository.Owner.Login,
			Repository: payload.Repository.Name,
			CommitSHA:  payload.SHA,
			Branch:     branch,
			Title:      firstNonblank(payload.Context, "GitHub status"),
			Body:       firstNonblank(payload.Description, payload.State),
			Author:     firstNonblank(payload.Sender.Login, "GitHub"),
			URL:        payload.TargetURL,
		}
		return finishGitHubEvent(event, false, true)
	default:
		return forge.Event{}, false, nil
	}
}

func githubCommentEvent(deliveryID, commentKind string, pullRequestNumber int, title string, commentID int, body, author, commentURL, owner, repository string) forge.Event {
	return forge.Event{
		DeliveryID:        githubDeliveryID(deliveryID),
		Provider:          forge.ProviderGitHub,
		Kind:              "review_comment",
		Owner:             owner,
		Repository:        repository,
		PullRequestNumber: pullRequestNumber,
		CommentID:         commentID,
		CommentKind:       commentKind,
		Title:             title,
		Body:              body,
		Author:            author,
		URL:               commentURL,
	}
}

func finishGitHubEvent(event forge.Event, comment, authorized bool) (forge.Event, bool, error) {
	if err := validateGitHubEvent(event, comment); err != nil {
		return forge.Event{}, false, err
	}
	if !authorized {
		return forge.Event{}, false, nil
	}
	return event, true, nil
}

func validateGitHubEvent(event forge.Event, comment bool) error {
	if !strings.HasPrefix(event.DeliveryID, githubDeliveryPrefix) {
		return errors.New("GitHub webhook delivery ID is required")
	}
	if err := forge.ValidateNormalizedIdentity("GitHub webhook delivery ID", strings.TrimPrefix(event.DeliveryID, githubDeliveryPrefix), true); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"GitHub webhook owner":      event.Owner,
		"GitHub webhook repository": event.Repository,
		"GitHub webhook author":     event.Author,
		"GitHub webhook URL":        event.URL,
		"GitHub webhook commit SHA": event.CommitSHA,
		"GitHub webhook branch":     event.Branch,
	} {
		required := comment && name != "GitHub webhook commit SHA" && name != "GitHub webhook branch" || !comment && name != "GitHub webhook branch" && name != "GitHub webhook URL"
		if err := forge.ValidateNormalizedIdentity(name, value, required); err != nil {
			return err
		}
	}
	for name, value := range map[string]string{"GitHub webhook title": event.Title, "GitHub webhook body": event.Body} {
		if err := forge.ValidateNormalizedText(name, value, true); err != nil {
			return err
		}
	}
	if event.PullRequestNumber < 0 {
		return errors.New("GitHub webhook pull request number is invalid")
	}
	if comment {
		if event.PullRequestNumber <= 0 || event.CommentID <= 0 || event.CommentKind == "" {
			return errors.New("GitHub webhook comment identity is incomplete")
		}
	}
	if event.URL == "" && !comment {
		return nil
	}
	return validateWebhookURL(event.URL)
}

func validateWebhookURL(value string) error {
	link, err := url.Parse(value)
	if err != nil || link.Host == "" || link.User != nil || (link.Scheme != "http" && link.Scheme != "https") {
		return errors.New("webhook URL is invalid")
	}
	return nil
}

func githubDeliveryID(deliveryID string) string {
	return githubDeliveryPrefix + deliveryID
}

func githubFailedConclusion(conclusion string) bool {
	switch strings.ToLower(conclusion) {
	case "failure", "cancelled", "timed_out", "action_required", "startup_failure", "stale":
		return true
	default:
		return false
	}
}

func trustedAuthorAssociation(association string) bool {
	switch strings.ToUpper(association) {
	case "OWNER", "MEMBER", "COLLABORATOR":
		return true
	default:
		return false
	}
}

func firstNonblank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func decodeWebhook(body []byte, target any) error {
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("decode GitHub webhook: %w", err)
	}
	return nil
}

type githubRepository struct {
	Name  string `json:"name"`
	Owner struct {
		Login string `json:"login"`
	} `json:"owner"`
}

type githubIssueComment struct {
	Action string `json:"action"`
	Issue  struct {
		Number      int    `json:"number"`
		Title       string `json:"title"`
		PullRequest *struct {
			URL string `json:"url"`
		} `json:"pull_request"`
	} `json:"issue"`
	Comment struct {
		ID                int    `json:"id"`
		Body              string `json:"body"`
		AuthorAssociation string `json:"author_association"`
		User              struct {
			Login string `json:"login"`
		} `json:"user"`
		HTMLURL string `json:"html_url"`
	} `json:"comment"`
	Repository githubRepository `json:"repository"`
}

type githubPullRequest struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
	Head   struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"head"`
}

type githubReviewComment struct {
	Action      string            `json:"action"`
	PullRequest githubPullRequest `json:"pull_request"`
	Comment     struct {
		ID                int    `json:"id"`
		Body              string `json:"body"`
		AuthorAssociation string `json:"author_association"`
		User              struct {
			Login string `json:"login"`
		} `json:"user"`
		HTMLURL string `json:"html_url"`
	} `json:"comment"`
	Repository githubRepository `json:"repository"`
}

type githubReview struct {
	Action string `json:"action"`
	Review struct {
		ID                int    `json:"id"`
		Body              string `json:"body"`
		State             string `json:"state"`
		AuthorAssociation string `json:"author_association"`
		User              struct {
			Login string `json:"login"`
		} `json:"user"`
		HTMLURL string `json:"html_url"`
	} `json:"review"`
	PullRequest githubPullRequest `json:"pull_request"`
	Repository  githubRepository  `json:"repository"`
}

type githubCheckRun struct {
	Action   string `json:"action"`
	CheckRun struct {
		Name       string `json:"name"`
		Conclusion string `json:"conclusion"`
		HeadSHA    string `json:"head_sha"`
		HTMLURL    string `json:"html_url"`
		App        struct {
			Name string `json:"name"`
		} `json:"app"`
		Output struct {
			Summary string `json:"summary"`
		} `json:"output"`
		CheckSuite struct {
			HeadBranch string `json:"head_branch"`
		} `json:"check_suite"`
		PullRequests []struct {
			Number int `json:"number"`
		} `json:"pull_requests"`
	} `json:"check_run"`
	Repository githubRepository `json:"repository"`
}

type githubStatus struct {
	State       string `json:"state"`
	SHA         string `json:"sha"`
	Context     string `json:"context"`
	Description string `json:"description"`
	TargetURL   string `json:"target_url"`
	Branches    []struct {
		Name string `json:"name"`
	} `json:"branches"`
	Repository githubRepository `json:"repository"`
	Sender     struct {
		Login string `json:"login"`
	} `json:"sender"`
}
