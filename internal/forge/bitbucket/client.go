package bitbucket

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/simpleswe/simpleswe/internal/forge"
)

const (
	maxResponseBody            = 1 << 20
	maxCommentListResponseBody = 32 << 20
)

// HTTPError preserves provider status so lifecycle reconciliation can retry
// transient failures without treating malformed requests as recoverable.
type HTTPError struct {
	StatusCode      int
	Status          string
	Message         string
	RetryAfterDelay time.Duration
}

func (e *HTTPError) Error() string {
	if e.Message == "" {
		return "Bitbucket returned " + e.Status
	}
	return "Bitbucket returned " + e.Status + ": " + e.Message
}

func (e *HTTPError) HTTPStatusCode() int       { return e.StatusCode }
func (e *HTTPError) RetryDelay() time.Duration { return e.RetryAfterDelay }
func (e *HTTPError) Retryable() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.RetryAfterDelay > 0
}

func (c *Client) httpError(response *http.Response, body []byte) *HTTPError {
	return &HTTPError{
		StatusCode: response.StatusCode, Status: response.Status,
		Message:         c.redact(strings.TrimSpace(string(body))),
		RetryAfterDelay: forge.ParseRetryAfter(response.Header.Get("Retry-After"), time.Now()),
	}
}

// IsPermanent reports only clear client errors. Timeout, conflict, locked,
// too-early, and rate-limit responses remain retryable.
func IsPermanent(err error) bool {
	return forge.IsPermanent(err)
}

// Client is a Bitbucket Cloud pull-request client.
type Client struct {
	baseURL     *url.URL
	username    string
	appPassword string
	httpClient  *http.Client
}

// CreatePullRequestRequest contains the fields Bitbucket needs to create a pull request.
type CreatePullRequestRequest = forge.CreatePullRequestRequest

// PullRequest is the subset of a Bitbucket pull request returned by this client.
type PullRequest = forge.PullRequest

// NewClient creates a Bitbucket Cloud client using app-password Basic Auth.
func NewClient(baseURL, username, appPassword string) (*Client, error) {
	parsedURL, err := url.Parse(baseURL)
	if err != nil || parsedURL.Host == "" || parsedURL.User != nil || parsedURL.RawQuery != "" || parsedURL.Fragment != "" || (parsedURL.Scheme != "https" && !(parsedURL.Scheme == "http" && isLoopback(parsedURL.Hostname()))) {
		return nil, errors.New("Bitbucket base URL must use HTTPS without credentials (HTTP is allowed only for loopback test servers)")
	}
	return &Client{
		baseURL:     parsedURL,
		username:    username,
		appPassword: appPassword,
		httpClient:  http.DefaultClient,
	}, nil
}

// CreatePullRequest creates a pull request in a Bitbucket workspace repository.
func (c *Client) CreatePullRequest(ctx context.Context, workspace, repository string, input CreatePullRequestRequest) (_ PullRequest, resultErr error) {
	endpoint, err := c.endpoint(workspace, repository)
	if err != nil {
		return PullRequest{}, forge.MarkPermanent(err)
	}

	body, err := json.Marshal(pullRequestPayload{
		Title:       input.Title,
		Description: input.Description,
		Source: payloadBranchRef{
			Branch: payloadBranch{Name: input.SourceBranch},
		},
		Destination: payloadBranchRef{
			Branch: payloadBranch{Name: input.DestinationBranch},
		},
		CloseSourceBranch: false,
	})
	if err != nil {
		return PullRequest{}, fmt.Errorf("encode Bitbucket request: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return PullRequest{}, fmt.Errorf("create Bitbucket request: %w", err)
	}
	request.SetBasicAuth(c.username, c.appPassword)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")

	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	response, err := httpClient.Do(request)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return PullRequest{}, contextErr
		}
		return PullRequest{}, fmt.Errorf("Bitbucket request failed: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, response.Body.Close()) }()

	responseBody, err := readBounded(response.Body)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return PullRequest{}, contextErr
		}
		return PullRequest{}, fmt.Errorf("read Bitbucket response: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return PullRequest{}, c.httpError(response, responseBody)
	}

	var decoded pullRequestResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return PullRequest{}, forge.MarkPermanent(fmt.Errorf("decode Bitbucket response: %w", err))
	}
	if decoded.ID <= 0 {
		return PullRequest{}, forge.MarkPermanent(errors.New("decode Bitbucket response: missing pull request id"))
	}
	if decoded.Links.HTML.Href == "" {
		return PullRequest{}, forge.MarkPermanent(errors.New("decode Bitbucket response: missing pull request URL"))
	}
	link, err := url.Parse(decoded.Links.HTML.Href)
	if err != nil || link.Scheme == "" || link.Host == "" {
		return PullRequest{}, forge.MarkPermanent(errors.New("decode Bitbucket response: invalid pull request URL"))
	}

	return PullRequest{ID: decoded.ID, HTMLURL: decoded.Links.HTML.Href}, nil
}

// FindPullRequest finds an open pull request by both source branch and the
// simpleswe task marker embedded in its description.
func (c *Client) FindPullRequest(ctx context.Context, workspace, repository, sourceBranch, destinationBranch, taskMarker string) (_ PullRequest, _ bool, resultErr error) {
	endpoint, err := c.endpoint(workspace, repository)
	if err != nil {
		return PullRequest{}, false, forge.MarkPermanent(err)
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		return PullRequest{}, false, fmt.Errorf("parse Bitbucket endpoint: %w", err)
	}
	query := target.Query()
	query.Set("state", "OPEN")
	query.Set("pagelen", "50")
	query.Set("q", fmt.Sprintf(`source.branch.name=%q AND destination.branch.name=%q`, sourceBranch, destinationBranch))
	target.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return PullRequest{}, false, fmt.Errorf("create Bitbucket lookup request: %w", err)
	}
	request.SetBasicAuth(c.username, c.appPassword)
	request.Header.Set("Accept", "application/json")
	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	response, err := httpClient.Do(request)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return PullRequest{}, false, contextErr
		}
		return PullRequest{}, false, fmt.Errorf("Bitbucket lookup failed: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, response.Body.Close()) }()
	body, err := readBounded(response.Body)
	if err != nil {
		return PullRequest{}, false, fmt.Errorf("read Bitbucket lookup response: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return PullRequest{}, false, c.httpError(response, body)
	}
	var decoded pullRequestListResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return PullRequest{}, false, forge.MarkPermanent(fmt.Errorf("decode Bitbucket lookup response: %w", err))
	}
	marker := "simpleswe task " + taskMarker
	for _, candidate := range decoded.Values {
		if candidate.Source.Branch.Name != sourceBranch || candidate.Destination.Branch.Name != destinationBranch || !strings.Contains(candidate.Description, marker) {
			continue
		}
		if !strings.EqualFold(candidate.State, "open") {
			continue
		}
		sourceWorkspace, sourceRepository, ok := bitbucketRepositoryCoordinates(candidate.Source.Repository)
		if !ok {
			return PullRequest{}, false, forge.MarkPermanent(errors.New("decode Bitbucket lookup response: missing source repository identity"))
		}
		if !strings.EqualFold(sourceWorkspace, workspace) || !strings.EqualFold(sourceRepository, repository) {
			continue
		}
		if candidate.ID <= 0 || candidate.Links.HTML.Href == "" {
			return PullRequest{}, false, forge.MarkPermanent(errors.New("decode Bitbucket lookup response: incomplete pull request"))
		}
		link, err := url.Parse(candidate.Links.HTML.Href)
		if err != nil || link.Scheme == "" || link.Host == "" {
			return PullRequest{}, false, forge.MarkPermanent(errors.New("decode Bitbucket lookup response: invalid pull request URL"))
		}
		return PullRequest{ID: candidate.ID, HTMLURL: candidate.Links.HTML.Href}, true, nil
	}
	return PullRequest{}, false, nil
}

// GetPullRequest reads current provider truth from the configured repository
// endpoint. Response URLs are intentionally ignored.
func (c *Client) GetPullRequest(ctx context.Context, workspace, repository string, number int) (_ forge.PullRequestState, resultErr error) {
	endpoint, err := c.pullRequestEndpoint(workspace, repository, number)
	if err != nil {
		return forge.PullRequestState{}, forge.MarkPermanent(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return forge.PullRequestState{}, forge.MarkPermanent(fmt.Errorf("create Bitbucket pull request request: %w", err))
	}
	request.SetBasicAuth(c.username, c.appPassword)
	request.Header.Set("Accept", "application/json")
	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	response, err := httpClient.Do(request)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return forge.PullRequestState{}, contextErr
		}
		return forge.PullRequestState{}, fmt.Errorf("Bitbucket pull request failed: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, response.Body.Close()) }()
	body, err := readBounded(response.Body)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return forge.PullRequestState{}, contextErr
		}
		return forge.PullRequestState{}, fmt.Errorf("read Bitbucket pull request response: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return forge.PullRequestState{}, c.httpError(response, body)
	}
	var decoded pullRequestResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return forge.PullRequestState{}, forge.MarkPermanent(fmt.Errorf("decode Bitbucket pull request response: %w", err))
	}
	state, err := validatePullRequestState(decoded)
	return state, forge.MarkPermanent(err)
}

func (c *Client) redact(message string) string {
	if c == nil {
		return message
	}
	if c.username != "" {
		message = strings.ReplaceAll(message, c.username, "[REDACTED]")
	}
	if c.appPassword != "" {
		message = strings.ReplaceAll(message, c.appPassword, "[REDACTED]")
	}
	return message
}

func (c *Client) endpoint(workspace, repository string) (string, error) {
	if c == nil || c.baseURL == nil {
		return "", errors.New("invalid Bitbucket base URL")
	}
	if c.baseURL.Scheme == "" || c.baseURL.Host == "" {
		return "", errors.New("Bitbucket base URL must include scheme and host")
	}
	if workspace == "" || repository == "" {
		return "", errors.New("Bitbucket workspace and repository are required")
	}

	target := *c.baseURL
	basePath := strings.TrimRight(c.baseURL.Path, "/")
	escapedBasePath := strings.TrimRight(c.baseURL.EscapedPath(), "/")
	target.Path = basePath + "/2.0/repositories/" + workspace + "/" + repository + "/pullrequests"
	target.RawPath = escapedBasePath + "/2.0/repositories/" + url.PathEscape(workspace) + "/" + url.PathEscape(repository) + "/pullrequests"
	target.RawQuery = ""
	target.Fragment = ""
	target.User = nil
	return target.String(), nil
}

func (c *Client) pullRequestEndpoint(workspace, repository string, number int) (string, error) {
	if number <= 0 {
		return "", errors.New("Bitbucket pull request number must be positive")
	}
	endpoint, err := c.endpoint(workspace, repository)
	if err != nil {
		return "", err
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("parse Bitbucket pull request endpoint: %w", err)
	}
	suffix := "/" + strconv.Itoa(number)
	escapedPath := target.EscapedPath()
	target.Path += suffix
	target.RawPath = escapedPath + suffix
	return target.String(), nil
}

func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func readBounded(reader io.Reader) ([]byte, error) {
	return readBoundedTo(reader, maxResponseBody)
}

func readCommentListBounded(reader io.Reader) ([]byte, error) {
	return readBoundedTo(reader, maxCommentListResponseBody)
}

func readBoundedTo(reader io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("response body exceeds %d bytes", limit)
	}
	return body, nil
}

type pullRequestPayload struct {
	Title             string           `json:"title"`
	Description       string           `json:"description"`
	Source            payloadBranchRef `json:"source"`
	Destination       payloadBranchRef `json:"destination"`
	CloseSourceBranch bool             `json:"close_source_branch"`
}

type payloadBranchRef struct {
	Branch payloadBranch `json:"branch"`
}

type payloadBranch struct {
	Name string `json:"name"`
}

type pullRequestResponse struct {
	ID          int                  `json:"id"`
	State       string               `json:"state"`
	Description string               `json:"description"`
	Source      pullRequestBranchRef `json:"source"`
	Destination pullRequestBranchRef `json:"destination"`
	Links       struct {
		HTML struct {
			Href string `json:"href"`
		} `json:"html"`
	} `json:"links"`
}

type pullRequestListResponse struct {
	Values []pullRequestResponse `json:"values"`
}

type pullRequestBranchRef struct {
	Branch payloadBranch `json:"branch"`
	Commit struct {
		Hash string `json:"hash"`
	} `json:"commit"`
	Repository bitbucketPullRequestRepository `json:"repository"`
}

type bitbucketPullRequestRepository struct {
	Name      string `json:"name"`
	FullName  string `json:"full_name"`
	Workspace struct {
		Slug string `json:"slug"`
	} `json:"workspace"`
}

func bitbucketRepositoryCoordinates(repository bitbucketPullRequestRepository) (string, string, bool) {
	workspace, name, found := strings.Cut(repository.FullName, "/")
	if found && workspace != "" && name != "" && !strings.Contains(name, "/") {
		return workspace, name, true
	}
	if repository.Workspace.Slug != "" && repository.Name != "" {
		return repository.Workspace.Slug, repository.Name, true
	}
	return "", "", false
}

func validatePullRequestState(decoded pullRequestResponse) (forge.PullRequestState, error) {
	workspace, repository, ok := bitbucketRepositoryCoordinates(decoded.Source.Repository)
	state := strings.ToLower(decoded.State)
	if decoded.ID <= 0 || !ok || decoded.Source.Branch.Name == "" || decoded.Destination.Branch.Name == "" {
		return forge.PullRequestState{}, errors.New("decode Bitbucket pull request response: incomplete pull request state or refs")
	}
	if err := forge.ValidateNormalizedIdentity("Bitbucket pull request head SHA", decoded.Source.Commit.Hash, true); err != nil {
		return forge.PullRequestState{}, fmt.Errorf("decode Bitbucket pull request response: %w", err)
	}
	switch state {
	case "open", "merged", "declined", "superseded":
	default:
		return forge.PullRequestState{}, errors.New("decode Bitbucket pull request response: invalid pull request state")
	}
	return forge.PullRequestState{
		Number: decoded.ID, State: state, SourceOwner: workspace, SourceRepository: repository,
		SourceBranch: decoded.Source.Branch.Name, DestinationBranch: decoded.Destination.Branch.Name,
		HeadSHA: decoded.Source.Commit.Hash,
	}, nil
}
