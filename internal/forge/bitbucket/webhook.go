package bitbucket

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/simpleswe/simpleswe/internal/forge"
)

const bitbucketDeliveryPrefix = "bitbucket:"

// ParseWebhook normalizes actionable Bitbucket webhook payloads without
// keeping the provider payload in the returned event.
func ParseWebhook(deliveryID, eventKey string, body []byte) (forge.Event, bool, error) {
	switch eventKey {
	case "pullrequest:comment_created":
		var payload bitbucketComment
		if err := decodeWebhook(body, &payload); err != nil {
			return forge.Event{}, false, err
		}
		if forge.ContainsReplyMarker(payload.Comment.Content.Raw) {
			return forge.Event{}, false, nil
		}
		if strings.TrimSpace(payload.Comment.Content.Raw) == "" {
			if payload.Comment.ID <= 0 || payload.Comment.Links.HTML.Href == "" || payload.PullRequest.ID <= 0 {
				return forge.Event{}, false, errors.New("Bitbucket pull request comment is incomplete")
			}
			return forge.Event{}, false, nil
		}
		event := forge.Event{
			DeliveryID:        bitbucketDeliveryID(deliveryID),
			Provider:          forge.ProviderBitbucket,
			Kind:              "review_comment",
			Owner:             payload.Repository.Workspace.Slug,
			Repository:        firstNonblank(payload.Repository.Slug, payload.Repository.Name),
			PullRequestNumber: payload.PullRequest.ID,
			CommitSHA:         payload.PullRequest.Source.Commit.Hash,
			Branch:            payload.PullRequest.Source.Branch.Name,
			CommentID:         payload.Comment.ID,
			CommentKind:       "comment",
			Title:             payload.PullRequest.Title,
			Body:              payload.Comment.Content.Raw,
			Author:            firstNonblank(payload.Comment.User.Nickname, payload.Comment.User.DisplayName),
			URL:               payload.Comment.Links.HTML.Href,
		}
		return finishBitbucketEvent(event, true)

	case "pullrequest:changes_request_created":
		var payload bitbucketChangesRequest
		if err := decodeWebhook(body, &payload); err != nil {
			return forge.Event{}, false, err
		}
		if payload.ChangesRequest == nil {
			return forge.Event{}, false, errors.New("Bitbucket pull request changes request is incomplete")
		}
		event := forge.Event{
			DeliveryID:        bitbucketDeliveryID(deliveryID),
			Provider:          forge.ProviderBitbucket,
			Kind:              "review_comment",
			Owner:             payload.Repository.Workspace.Slug,
			Repository:        firstNonblank(payload.Repository.Slug, payload.Repository.Name),
			PullRequestNumber: payload.PullRequest.ID,
			CommitSHA:         payload.PullRequest.Source.Commit.Hash,
			Branch:            payload.PullRequest.Source.Branch.Name,
			CommentID:         0,
			CommentKind:       "changes_request",
			Title:             payload.PullRequest.Title,
			Body:              "Changes requested",
			Author:            firstNonblank(payload.Actor.Nickname, payload.Actor.DisplayName),
			URL:               payload.PullRequest.Links.HTML.Href,
		}
		return finishBitbucketEvent(event, true)

	case "pipeline:build:completed":
		var payload bitbucketPipeline
		if err := decodeWebhook(body, &payload); err != nil {
			return forge.Event{}, false, err
		}
		if !strings.EqualFold(payload.Pipeline.State.Result.Name, "failed") {
			return forge.Event{}, false, nil
		}
		event := forge.Event{
			DeliveryID: bitbucketDeliveryID(deliveryID),
			Provider:   forge.ProviderBitbucket,
			Kind:       "quality_gate_failed",
			Owner:      payload.Repository.Workspace.Slug,
			Repository: firstNonblank(payload.Repository.Slug, payload.Repository.Name),
			CommitSHA:  payload.Pipeline.Target.Commit.Hash,
			Branch:     payload.Pipeline.Target.RefName,
			Title:      pipelineTitle(payload.Pipeline.BuildNumber, payload.Pipeline.UUID),
			Body:       payload.Pipeline.State.Result.Name,
			Author:     firstNonblank(payload.Actor.DisplayName, payload.Actor.Nickname, "Bitbucket Pipelines"),
			URL:        payload.Pipeline.Links.Self.Href,
		}
		return finishBitbucketEvent(event, false)

	case "repo:commit_status_created", "repo:commit_status_updated":
		var payload bitbucketCommitStatus
		if err := decodeWebhook(body, &payload); err != nil {
			return forge.Event{}, false, err
		}
		switch strings.ToLower(payload.CommitStatus.State) {
		case "failed", "error", "stopped":
		default:
			return forge.Event{}, false, nil
		}
		commitSHA, err := commitHashFromLink(payload.CommitStatus.Links.Commit.Href)
		if err != nil {
			return forge.Event{}, false, err
		}
		event := forge.Event{
			DeliveryID: bitbucketDeliveryID(deliveryID),
			Provider:   forge.ProviderBitbucket,
			Kind:       "quality_gate_failed",
			Owner:      payload.Repository.Workspace.Slug,
			Repository: firstNonblank(payload.Repository.Slug, payload.Repository.Name),
			CommitSHA:  commitSHA,
			Branch:     payload.CommitStatus.RefName,
			Title:      firstNonblank(payload.CommitStatus.Name, payload.CommitStatus.Key, "Commit status"),
			Body:       firstNonblank(payload.CommitStatus.Description, payload.CommitStatus.State),
			Author:     firstNonblank(payload.Actor.DisplayName, payload.Actor.Nickname, "Bitbucket"),
			URL:        firstNonblank(payload.CommitStatus.URL, payload.CommitStatus.Links.Self.Href),
		}
		return finishBitbucketEvent(event, false)
	default:
		return forge.Event{}, false, nil
	}
}

func finishBitbucketEvent(event forge.Event, comment bool) (forge.Event, bool, error) {
	if err := validateBitbucketEvent(event, comment); err != nil {
		return forge.Event{}, false, err
	}
	return event, true, nil
}

func validateBitbucketEvent(event forge.Event, comment bool) error {
	if !strings.HasPrefix(event.DeliveryID, bitbucketDeliveryPrefix) {
		return errors.New("Bitbucket webhook delivery ID is required")
	}
	if err := forge.ValidateNormalizedIdentity("Bitbucket webhook delivery ID", strings.TrimPrefix(event.DeliveryID, bitbucketDeliveryPrefix), true); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"Bitbucket webhook owner":      event.Owner,
		"Bitbucket webhook repository": event.Repository,
		"Bitbucket webhook author":     event.Author,
		"Bitbucket webhook URL":        event.URL,
		"Bitbucket webhook commit SHA": event.CommitSHA,
		"Bitbucket webhook branch":     event.Branch,
	} {
		required := comment && name != "Bitbucket webhook commit SHA" && name != "Bitbucket webhook branch" || !comment && name != "Bitbucket webhook branch" && name != "Bitbucket webhook URL"
		if err := forge.ValidateNormalizedIdentity(name, value, required); err != nil {
			return err
		}
	}
	for name, value := range map[string]string{"Bitbucket webhook title": event.Title, "Bitbucket webhook body": event.Body} {
		if err := forge.ValidateNormalizedText(name, value, true); err != nil {
			return err
		}
	}
	if event.PullRequestNumber < 0 {
		return errors.New("Bitbucket webhook pull request number is invalid")
	}
	validCommentIdentity := event.CommentID > 0 && event.CommentKind == "comment" ||
		event.CommentID == 0 && event.CommentKind == "changes_request"
	if comment && (event.PullRequestNumber <= 0 || !validCommentIdentity) {
		return errors.New("Bitbucket webhook comment identity is incomplete")
	}
	if event.URL == "" && !comment {
		return nil
	}
	return validateWebhookURL(event.URL)
}

func bitbucketDeliveryID(deliveryID string) string {
	return bitbucketDeliveryPrefix + deliveryID
}

func firstNonblank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func pipelineTitle(buildNumber int, uuid string) string {
	if buildNumber > 0 {
		return "Pipeline #" + strconv.Itoa(buildNumber)
	}
	return firstNonblank(uuid, "Bitbucket pipeline")
}

func commitHashFromLink(value string) (string, error) {
	if err := validateWebhookURL(value); err != nil {
		return "", fmt.Errorf("Bitbucket commit status commit link: %w", err)
	}
	link, _ := url.Parse(value)
	escapedPath := link.EscapedPath()
	index := strings.LastIndexByte(escapedPath, '/')
	if index < 0 || index == len(escapedPath)-1 {
		return "", errors.New("Bitbucket commit status commit link has no commit hash")
	}
	hash, err := url.PathUnescape(escapedPath[index+1:])
	if err != nil || strings.ContainsAny(hash, `/\\`) {
		return "", errors.New("Bitbucket commit status commit link has an invalid commit hash")
	}
	if err := forge.ValidateNormalizedIdentity("Bitbucket webhook commit SHA", hash, true); err != nil {
		return "", err
	}
	return hash, nil
}

func decodeWebhook(body []byte, target any) error {
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("decode Bitbucket webhook: %w", err)
	}
	return nil
}

func validateWebhookURL(value string) error {
	link, err := url.Parse(value)
	if err != nil || link.Host == "" || link.User != nil || (link.Scheme != "http" && link.Scheme != "https") {
		return errors.New("webhook URL is invalid")
	}
	return nil
}

type bitbucketRepository struct {
	Name      string `json:"name"`
	Slug      string `json:"slug"`
	Workspace struct {
		Slug string `json:"slug"`
	} `json:"workspace"`
}

type bitbucketComment struct {
	Comment struct {
		ID      int `json:"id"`
		Content struct {
			Raw string `json:"raw"`
		} `json:"content"`
		User struct {
			UUID        string `json:"uuid"`
			Nickname    string `json:"nickname"`
			DisplayName string `json:"display_name"`
		} `json:"user"`
		Links struct {
			HTML struct {
				Href string `json:"href"`
			} `json:"html"`
		} `json:"links"`
	} `json:"comment"`
	PullRequest bitbucketPullRequest `json:"pullrequest"`
	Repository  bitbucketRepository  `json:"repository"`
}

type bitbucketChangesRequest struct {
	Actor          bitbucketActor       `json:"actor"`
	Repository     bitbucketRepository  `json:"repository"`
	PullRequest    bitbucketPullRequest `json:"pullrequest"`
	ChangesRequest *struct{}            `json:"changes_request"`
}

type bitbucketPullRequest struct {
	ID     int    `json:"id"`
	Title  string `json:"title"`
	Source struct {
		Branch struct {
			Name string `json:"name"`
		} `json:"branch"`
		Commit struct {
			Hash string `json:"hash"`
		} `json:"commit"`
	} `json:"source"`
	Links struct {
		HTML struct {
			Href string `json:"href"`
		} `json:"html"`
	} `json:"links"`
}

type bitbucketActor struct {
	DisplayName string `json:"display_name"`
	Nickname    string `json:"nickname"`
}

type bitbucketPipeline struct {
	Actor      bitbucketActor      `json:"actor"`
	Repository bitbucketRepository `json:"repository"`
	Pipeline   struct {
		UUID        string `json:"uuid"`
		BuildNumber int    `json:"build_number"`
		State       struct {
			Result struct {
				Name string `json:"name"`
			} `json:"result"`
		} `json:"state"`
		Target struct {
			RefName string `json:"ref_name"`
			Commit  struct {
				Hash string `json:"hash"`
			} `json:"commit"`
		} `json:"target"`
		Links struct {
			Self struct {
				Href string `json:"href"`
			} `json:"self"`
		} `json:"links"`
	} `json:"pipeline"`
}

type bitbucketCommitStatus struct {
	Actor        bitbucketActor      `json:"actor"`
	Repository   bitbucketRepository `json:"repository"`
	CommitStatus struct {
		Name        string `json:"name"`
		Key         string `json:"key"`
		Description string `json:"description"`
		State       string `json:"state"`
		URL         string `json:"url"`
		RefName     string `json:"refname"`
		Links       struct {
			Self struct {
				Href string `json:"href"`
			} `json:"self"`
			Commit struct {
				Href string `json:"href"`
			} `json:"commit"`
		} `json:"links"`
	} `json:"commit_status"`
}
