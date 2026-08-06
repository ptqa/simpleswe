package github

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

type HTTPError struct {
	StatusCode         int
	Status             string
	Message            string
	RetryAfter         bool
	RetryAfterDelay    time.Duration
	RateLimitExhausted bool
}

func (e *HTTPError) Error() string {
	if e.Message == "" {
		return "GitHub returned " + e.Status
	}
	return "GitHub returned " + e.Status + ": " + e.Message
}

func (e *HTTPError) HTTPStatusCode() int       { return e.StatusCode }
func (e *HTTPError) RetryDelay() time.Duration { return e.RetryAfterDelay }

func (e *HTTPError) Retryable() bool {
	switch e.StatusCode {
	case http.StatusForbidden, http.StatusTooManyRequests:
		message := strings.ToLower(e.Message)
		return e.RetryAfter || e.RateLimitExhausted || strings.Contains(message, "primary rate limit") || strings.Contains(message, "secondary rate limit") || strings.Contains(message, "rate limit exceeded")
	case http.StatusUnprocessableEntity:
		message := strings.ToLower(e.Message)
		return strings.Contains(message, "endpoint has been spammed") || strings.Contains(message, "spam protection")
	default:
		return false
	}
}

func IsPermanent(err error) bool { return forge.IsPermanent(err) }

type Client struct {
	baseURL    *url.URL
	token      string
	httpClient *http.Client
}

func NewClient(baseURL, token string) (*Client, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopback(parsed.Hostname()))) {
		return nil, errors.New("GitHub base URL must use HTTPS without credentials (HTTP is allowed only for loopback test servers)")
	}
	return &Client{baseURL: parsed, token: token, httpClient: http.DefaultClient}, nil
}

func (c *Client) CreatePullRequest(ctx context.Context, owner, repository string, input forge.CreatePullRequestRequest) (_ forge.PullRequest, resultErr error) {
	endpoint, err := c.endpoint(owner, repository)
	if err != nil {
		return forge.PullRequest{}, forge.MarkPermanent(err)
	}
	body, err := json.Marshal(struct {
		Title string `json:"title"`
		Body  string `json:"body"`
		Head  string `json:"head"`
		Base  string `json:"base"`
	}{input.Title, input.Description, input.SourceBranch, input.DestinationBranch})
	if err != nil {
		return forge.PullRequest{}, fmt.Errorf("encode GitHub request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return forge.PullRequest{}, fmt.Errorf("create GitHub request: %w", err)
	}
	c.setHeaders(request)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.do(request)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return forge.PullRequest{}, fmt.Errorf("GitHub request context: %w", contextErr)
		}
		return forge.PullRequest{}, fmt.Errorf("GitHub request failed: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, response.Body.Close()) }()
	responseBody, err := readBounded(response.Body)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return forge.PullRequest{}, fmt.Errorf("GitHub response context: %w", contextErr)
		}
		return forge.PullRequest{}, fmt.Errorf("read GitHub response: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return forge.PullRequest{}, c.httpError(response, responseBody)
	}
	var decoded pullRequestResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return forge.PullRequest{}, forge.MarkPermanent(fmt.Errorf("decode GitHub response: %w", err))
	}
	pullRequest, err := validatePullRequest(decoded, "decode GitHub response")
	return pullRequest, forge.MarkPermanent(err)
}

func (c *Client) FindPullRequest(ctx context.Context, owner, repository, sourceBranch, destinationBranch, taskMarker string) (_ forge.PullRequest, _ bool, resultErr error) {
	endpoint, err := c.endpoint(owner, repository)
	if err != nil {
		return forge.PullRequest{}, false, forge.MarkPermanent(err)
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		return forge.PullRequest{}, false, fmt.Errorf("parse GitHub endpoint: %w", err)
	}
	query := target.Query()
	query.Set("state", "open")
	query.Set("head", owner+":"+sourceBranch)
	query.Set("base", destinationBranch)
	target.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return forge.PullRequest{}, false, fmt.Errorf("create GitHub lookup request: %w", err)
	}
	c.setHeaders(request)
	response, err := c.do(request)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return forge.PullRequest{}, false, fmt.Errorf("GitHub lookup context: %w", contextErr)
		}
		return forge.PullRequest{}, false, fmt.Errorf("GitHub lookup failed: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, response.Body.Close()) }()
	body, err := readBounded(response.Body)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return forge.PullRequest{}, false, fmt.Errorf("GitHub lookup response context: %w", contextErr)
		}
		return forge.PullRequest{}, false, fmt.Errorf("read GitHub lookup response: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return forge.PullRequest{}, false, c.httpError(response, body)
	}
	var decoded []pullRequestResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return forge.PullRequest{}, false, forge.MarkPermanent(fmt.Errorf("decode GitHub lookup response: %w", err))
	}
	marker := "simpleswe task " + taskMarker
	for _, candidate := range decoded {
		if candidate.Head.Ref != sourceBranch || candidate.Base.Ref != destinationBranch || !strings.Contains(candidate.Body, marker) {
			continue
		}
		state := strings.ToLower(candidate.State)
		if candidate.Merged {
			state = "merged"
		}
		if state != "open" {
			continue
		}
		if candidate.Head.Repository.Owner.Login == "" || candidate.Head.Repository.Name == "" {
			return forge.PullRequest{}, false, forge.MarkPermanent(errors.New("decode GitHub lookup response: missing source repository identity"))
		}
		if !strings.EqualFold(candidate.Head.Repository.Owner.Login, owner) || !strings.EqualFold(candidate.Head.Repository.Name, repository) {
			continue
		}
		pullRequest, err := validatePullRequest(candidate, "decode GitHub lookup response")
		return pullRequest, err == nil, forge.MarkPermanent(err)
	}
	return forge.PullRequest{}, false, nil
}

// GetPullRequest reads current provider truth from the configured repository
// endpoint. Response URLs are intentionally ignored.
func (c *Client) GetPullRequest(ctx context.Context, owner, repository string, number int) (_ forge.PullRequestState, resultErr error) {
	endpoint, err := c.pullRequestEndpoint(owner, repository, number)
	if err != nil {
		return forge.PullRequestState{}, forge.MarkPermanent(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return forge.PullRequestState{}, forge.MarkPermanent(fmt.Errorf("create GitHub pull request request: %w", err))
	}
	c.setHeaders(request)
	response, err := c.do(request)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return forge.PullRequestState{}, fmt.Errorf("GitHub pull request context: %w", contextErr)
		}
		return forge.PullRequestState{}, fmt.Errorf("GitHub pull request failed: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, response.Body.Close()) }()
	body, err := readBounded(response.Body)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return forge.PullRequestState{}, fmt.Errorf("GitHub pull request response context: %w", contextErr)
		}
		return forge.PullRequestState{}, fmt.Errorf("read GitHub pull request response: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return forge.PullRequestState{}, c.httpError(response, body)
	}
	var decoded pullRequestResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return forge.PullRequestState{}, forge.MarkPermanent(fmt.Errorf("decode GitHub pull request response: %w", err))
	}
	state, err := validatePullRequestState(decoded)
	return state, forge.MarkPermanent(err)
}

func (c *Client) endpoint(owner, repository string) (string, error) {
	if c == nil || c.baseURL == nil || c.baseURL.Scheme == "" || c.baseURL.Host == "" {
		return "", errors.New("invalid GitHub base URL")
	}
	if owner == "" || repository == "" {
		return "", errors.New("GitHub owner and repository are required")
	}
	target := *c.baseURL
	basePath := strings.TrimRight(c.baseURL.Path, "/")
	escapedBasePath := strings.TrimRight(c.baseURL.EscapedPath(), "/")
	target.Path = basePath + "/repos/" + owner + "/" + repository + "/pulls"
	target.RawPath = escapedBasePath + "/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(repository) + "/pulls"
	return target.String(), nil
}

func (c *Client) pullRequestEndpoint(owner, repository string, number int) (string, error) {
	if number <= 0 {
		return "", errors.New("GitHub pull request number must be positive")
	}
	endpoint, err := c.endpoint(owner, repository)
	if err != nil {
		return "", err
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("parse GitHub pull request endpoint: %w", err)
	}
	suffix := "/" + strconv.Itoa(number)
	escapedPath := target.EscapedPath()
	target.Path += suffix
	target.RawPath = escapedPath + suffix
	return target.String(), nil
}

func (c *Client) setHeaders(request *http.Request) {
	if c.token != "" {
		request.Header.Set("Authorization", "Bearer "+c.token)
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-Github-Api-Version", "2022-11-28")
}

func (c *Client) do(request *http.Request) (*http.Response, error) {
	client := c.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("send GitHub HTTP request: %w", err)
	}
	return response, nil
}

func (c *Client) redact(message string) string {
	if c != nil && c.token != "" {
		return strings.ReplaceAll(message, c.token, "[REDACTED]")
	}
	return message
}

func (c *Client) httpError(response *http.Response, body []byte) *HTTPError {
	return &HTTPError{
		StatusCode:         response.StatusCode,
		Status:             response.Status,
		Message:            c.redact(strings.TrimSpace(string(body))),
		RetryAfter:         response.Header.Get("Retry-After") != "",
		RetryAfterDelay:    forge.ParseRetryAfter(response.Header.Get("Retry-After"), time.Now()),
		RateLimitExhausted: strings.TrimSpace(response.Header.Get("X-Ratelimit-Remaining")) == "0",
	}
}

func validatePullRequest(decoded pullRequestResponse, prefix string) (forge.PullRequest, error) {
	if decoded.Number <= 0 {
		return forge.PullRequest{}, errors.New(prefix + ": missing pull request id")
	}
	if decoded.HTMLURL == "" {
		return forge.PullRequest{}, errors.New(prefix + ": missing pull request URL")
	}
	link, err := url.Parse(decoded.HTMLURL)
	if err != nil || link.Host == "" || link.User != nil || (link.Scheme != "http" && link.Scheme != "https") {
		return forge.PullRequest{}, errors.New(prefix + ": invalid pull request URL")
	}
	return forge.PullRequest{ID: decoded.Number, HTMLURL: decoded.HTMLURL}, nil
}

func validatePullRequestState(decoded pullRequestResponse) (forge.PullRequestState, error) {
	state := strings.ToLower(decoded.State)
	if decoded.Merged {
		state = "merged"
	}
	if decoded.Number <= 0 || (state != "open" && state != "closed" && state != "merged") ||
		decoded.Head.Ref == "" || decoded.Head.Repository.Owner.Login == "" || decoded.Head.Repository.Name == "" || decoded.Base.Ref == "" {
		return forge.PullRequestState{}, errors.New("decode GitHub pull request response: incomplete pull request state or refs")
	}
	if err := forge.ValidateNormalizedIdentity("GitHub pull request head SHA", decoded.Head.SHA, true); err != nil {
		return forge.PullRequestState{}, fmt.Errorf("decode GitHub pull request response: %w", err)
	}
	return forge.PullRequestState{
		Number: decoded.Number, State: state,
		SourceOwner: decoded.Head.Repository.Owner.Login, SourceRepository: decoded.Head.Repository.Name,
		SourceBranch: decoded.Head.Ref, DestinationBranch: decoded.Base.Ref, HeadSHA: decoded.Head.SHA,
	}, nil
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
		return nil, fmt.Errorf("read bounded response: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("response body exceeds %d bytes", limit)
	}
	return body, nil
}

func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type pullRequestResponse struct {
	Number  int    `json:"number"`
	Body    string `json:"body"`
	HTMLURL string `json:"html_url"`
	State   string `json:"state"`
	Merged  bool   `json:"merged"`
	Head    struct {
		Ref        string `json:"ref"`
		SHA        string `json:"sha"`
		Repository struct {
			Name  string `json:"name"`
			Owner struct {
				Login string `json:"login"`
			} `json:"owner"`
		} `json:"repo"`
	} `json:"head"`
	Base struct {
		Ref string `json:"ref"`
	} `json:"base"`
}
