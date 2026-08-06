package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/simpleswe/simpleswe/internal/forge"
)

const (
	commentPageSize        = 100
	recentCommentPageLimit = 10
)

// ReplyToPullRequest posts a reply using the GitHub route selected by the
// normalized comment kind.
func (c *Client) ReplyToPullRequest(ctx context.Context, owner, repository string, input forge.ReplyRequest) (resultErr error) {
	if err := forge.ValidateReplyRequest(input); err != nil {
		return forge.MarkPermanent(err)
	}
	endpoint, err := c.replyEndpoint(owner, repository, input)
	if err != nil {
		return forge.MarkPermanent(err)
	}
	body, err := json.Marshal(struct {
		Body string `json:"body"`
	}{input.Body})
	if err != nil {
		return fmt.Errorf("encode GitHub reply: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create GitHub reply request: %w", err)
	}
	c.setHeaders(request)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.do(request)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return fmt.Errorf("GitHub reply context: %w", contextErr)
		}
		return fmt.Errorf("GitHub reply failed: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, response.Body.Close()) }()
	responseBody, err := readBounded(response.Body)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return fmt.Errorf("GitHub reply response context: %w", contextErr)
		}
		return fmt.Errorf("read GitHub reply response: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return c.httpError(response, responseBody)
	}
	return nil
}

// PullRequestReplyExists searches only the configured repository endpoint and
// rebuilds a bounded window of recent page numbers rather than following
// response URLs. A duplicate is possible only when more than this window of
// comments arrives during crash recovery; deterministic liveness wins over an
// unbounded retry scan.
func (c *Client) PullRequestReplyExists(ctx context.Context, owner, repository string, input forge.ReplyRequest, marker string) (bool, error) {
	if err := validateReplyRoute(input); err != nil {
		return false, forge.MarkPermanent(err)
	}
	if err := forge.ValidateNormalizedText("reply marker", marker, true); err != nil {
		return false, forge.MarkPermanent(err)
	}
	found, links, err := c.replyMarkerOnPage(ctx, owner, repository, input, marker, 1)
	if err != nil {
		return false, err
	}
	lastPage, err := githubLastPage(links)
	if err != nil {
		return false, fmt.Errorf("validate GitHub reply pagination: %w", err)
	}
	if found {
		return true, nil
	}
	firstRecentPage := max(2, lastPage-recentCommentPageLimit+1)
	for page := lastPage; page >= firstRecentPage; page-- {
		found, _, err := c.replyMarkerOnPage(ctx, owner, repository, input, marker, page)
		if err != nil || found {
			return found, err
		}
	}
	return false, nil
}

func (c *Client) replyMarkerOnPage(ctx context.Context, owner, repository string, input forge.ReplyRequest, marker string, page int) (bool, []string, error) {
	if err := ctx.Err(); err != nil {
		return false, nil, err
	}
	endpoint, err := c.replyListEndpoint(owner, repository, input, page)
	if err != nil {
		return false, nil, forge.MarkPermanent(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false, nil, fmt.Errorf("create GitHub reply lookup request: %w", err)
	}
	c.setHeaders(request)
	response, err := c.do(request)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return false, nil, fmt.Errorf("GitHub reply lookup context: %w", contextErr)
		}
		return false, nil, fmt.Errorf("GitHub reply lookup failed: %w", err)
	}
	body, readErr := readCommentListBounded(response.Body)
	closeErr := response.Body.Close()
	if contextErr := ctx.Err(); contextErr != nil {
		return false, nil, fmt.Errorf("GitHub reply lookup context: %w", contextErr)
	}
	if readErr != nil || closeErr != nil {
		return false, nil, fmt.Errorf("read GitHub reply lookup response: %w", errors.Join(readErr, closeErr))
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return false, nil, c.httpError(response, body)
	}
	var comments []struct {
		Body string `json:"body"`
	}
	if err := json.Unmarshal(body, &comments); err != nil {
		return false, nil, fmt.Errorf("decode GitHub reply lookup response: %w", err)
	}
	for _, comment := range comments {
		if strings.Contains(comment.Body, marker) {
			return true, response.Header.Values("Link"), nil
		}
	}
	return false, response.Header.Values("Link"), nil
}

func githubLastPage(headers []string) (int, error) {
	if len(headers) == 0 {
		return 1, nil
	}
	lastPage, lastLinks, hasNext := 0, 0, false
	for _, header := range headers {
		links, err := splitGitHubLinks(header)
		if err != nil {
			return 0, err
		}
		for _, link := range links {
			target, relations, err := parseGitHubLink(link)
			if err != nil {
				return 0, err
			}
			for _, relation := range relations {
				if relation == "next" {
					hasNext = true
				}
				if relation != "last" {
					continue
				}
				lastLinks++
				values := target.Query()["page"]
				if len(values) != 1 || values[0] == "" || strings.Trim(values[0], "0123456789") != "" {
					return 0, errors.New(`rel="last" must contain exactly one numeric page`)
				}
				page, err := strconv.Atoi(values[0])
				if err != nil || page < 1 {
					return 0, errors.New(`rel="last" page is invalid`)
				}
				if lastPage != 0 && lastPage != page {
					return 0, errors.New(`rel="last" page values are inconsistent`)
				}
				lastPage = page
			}
		}
	}
	if lastLinks > 1 {
		return 0, errors.New(`multiple rel="last" links`)
	}
	if lastPage == 0 {
		if hasNext {
			return 0, errors.New(`rel="next" is present without rel="last"`)
		}
		return 1, nil
	}
	if hasNext && lastPage == 1 {
		return 0, errors.New(`rel="next" is inconsistent with rel="last" page 1`)
	}
	return lastPage, nil
}

func splitGitHubLinks(header string) ([]string, error) {
	var links []string
	start, angle, quoted := 0, false, false
	for index, char := range header {
		switch char {
		case '<':
			if !quoted {
				angle = true
			}
		case '>':
			if !quoted {
				angle = false
			}
		case '"':
			if !angle {
				quoted = !quoted
			}
		case ',':
			if !angle && !quoted {
				links = append(links, strings.TrimSpace(header[start:index]))
				start = index + 1
			}
		}
	}
	if angle || quoted {
		return nil, errors.New("malformed Link header")
	}
	links = append(links, strings.TrimSpace(header[start:]))
	for _, link := range links {
		if link == "" {
			return nil, errors.New("empty Link header entry")
		}
	}
	return links, nil
}

func parseGitHubLink(link string) (*url.URL, []string, error) {
	if !strings.HasPrefix(link, "<") {
		return nil, nil, errors.New("malformed Link header entry")
	}
	end := strings.IndexByte(link, '>')
	if end < 2 {
		return nil, nil, errors.New("malformed Link target")
	}
	target, err := url.Parse(link[1:end])
	if err != nil {
		return nil, nil, fmt.Errorf("parse Link target: %w", err)
	}
	var relations []string
	for _, rawParameter := range strings.Split(link[end+1:], ";") {
		parameter := strings.TrimSpace(rawParameter)
		if parameter == "" {
			continue
		}
		name, value, ok := strings.Cut(parameter, "=")
		if !ok || strings.TrimSpace(name) == "" {
			return nil, nil, errors.New("malformed Link parameter")
		}
		value = strings.TrimSpace(value)
		if strings.HasPrefix(value, `"`) {
			value, err = strconv.Unquote(value)
			if err != nil {
				return nil, nil, errors.New("malformed quoted Link parameter")
			}
		}
		if strings.EqualFold(strings.TrimSpace(name), "rel") {
			relations = append(relations, strings.Fields(value)...)
		}
	}
	return target, relations, nil
}

func (c *Client) replyEndpoint(owner, repository string, input forge.ReplyRequest) (string, error) {
	if err := validateReplyRoute(input); err != nil {
		return "", err
	}
	base, err := c.endpoint(owner, repository)
	if err != nil {
		return "", err
	}
	target, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("parse GitHub reply endpoint: %w", err)
	}
	basePath := strings.TrimSuffix(target.Path, "/pulls")
	baseEscapedPath := strings.TrimSuffix(target.EscapedPath(), "/pulls")
	path := "/issues/" + strconv.Itoa(input.PullRequestNumber) + "/comments"
	escapedPath := path
	if input.CommentKind == "review_comment" {
		path = "/pulls/" + strconv.Itoa(input.PullRequestNumber) + "/comments/" + strconv.Itoa(input.CommentID) + "/replies"
		escapedPath = path
	}
	target.Path = basePath + path
	target.RawPath = baseEscapedPath + escapedPath
	target.RawQuery = ""
	target.Fragment = ""
	return target.String(), nil
}

func validateReplyRoute(input forge.ReplyRequest) error {
	if input.PullRequestNumber <= 0 {
		return errors.New("reply pull request number must be positive")
	}
	if input.CommentKind != "" && input.CommentKind != "review_comment" && input.CommentKind != "issue_comment" && input.CommentKind != "review" {
		return fmt.Errorf("unsupported GitHub comment kind %q", input.CommentKind)
	}
	if input.CommentKind == "review_comment" && input.CommentID <= 0 {
		return errors.New("GitHub review comment ID must be positive")
	}
	if input.CommentID < 0 {
		return errors.New("GitHub comment ID is invalid")
	}
	if input.CommentKind == "" && input.CommentID != 0 {
		return errors.New("GitHub comment kind is required for a comment ID")
	}
	return nil
}

func (c *Client) replyListEndpoint(owner, repository string, input forge.ReplyRequest, page int) (string, error) {
	base, err := c.endpoint(owner, repository)
	if err != nil {
		return "", err
	}
	target, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("parse GitHub reply endpoint: %w", err)
	}
	basePath := strings.TrimSuffix(target.Path, "/pulls")
	baseEscapedPath := strings.TrimSuffix(target.EscapedPath(), "/pulls")
	path := "/issues/" + strconv.Itoa(input.PullRequestNumber) + "/comments"
	if input.CommentKind == "review_comment" {
		path = "/pulls/" + strconv.Itoa(input.PullRequestNumber) + "/comments"
	}
	target.Path = basePath + path
	target.RawPath = baseEscapedPath + path
	query := target.Query()
	query.Set("per_page", strconv.Itoa(commentPageSize))
	query.Set("page", strconv.Itoa(page))
	target.RawQuery = query.Encode()
	target.Fragment = ""
	return target.String(), nil
}
