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
	"strings"

	"github.com/simpleswe/simpleswe/internal/forge"
)

const maxResponseBody = 1 << 20

type HTTPError struct {
	StatusCode         int
	Status             string
	Message            string
	RetryAfter         bool
	RateLimitExhausted bool
}

func (e *HTTPError) Error() string {
	if e.Message == "" {
		return "GitHub returned " + e.Status
	}
	return "GitHub returned " + e.Status + ": " + e.Message
}

func (e *HTTPError) HTTPStatusCode() int { return e.StatusCode }

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
		return forge.PullRequest{}, err
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
		return forge.PullRequest{}, fmt.Errorf("decode GitHub response: %w", err)
	}
	return validatePullRequest(decoded, "decode GitHub response")
}

func (c *Client) FindPullRequest(ctx context.Context, owner, repository, sourceBranch, taskMarker string) (_ forge.PullRequest, _ bool, resultErr error) {
	endpoint, err := c.endpoint(owner, repository)
	if err != nil {
		return forge.PullRequest{}, false, err
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		return forge.PullRequest{}, false, fmt.Errorf("parse GitHub endpoint: %w", err)
	}
	query := target.Query()
	query.Set("state", "open")
	query.Set("head", owner+":"+sourceBranch)
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
		return forge.PullRequest{}, false, fmt.Errorf("decode GitHub lookup response: %w", err)
	}
	marker := "simpleswe task " + taskMarker
	for _, candidate := range decoded {
		if candidate.Head.Ref != sourceBranch || !strings.Contains(candidate.Body, marker) {
			continue
		}
		pullRequest, err := validatePullRequest(candidate, "decode GitHub lookup response")
		return pullRequest, err == nil, err
	}
	return forge.PullRequest{}, false, nil
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

func readBounded(reader io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxResponseBody+1))
	if err != nil {
		return nil, fmt.Errorf("read bounded response: %w", err)
	}
	if len(body) > maxResponseBody {
		return nil, fmt.Errorf("response body exceeds %d bytes", maxResponseBody)
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
	Head    struct {
		Ref string `json:"ref"`
	} `json:"head"`
}
