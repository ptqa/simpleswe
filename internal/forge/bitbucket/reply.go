package bitbucket

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

// ReplyToPullRequest posts a general or parented Bitbucket pull-request
// comment.
func (c *Client) ReplyToPullRequest(ctx context.Context, workspace, repository string, input forge.ReplyRequest) (resultErr error) {
	if err := forge.ValidateReplyRequest(input); err != nil {
		return forge.MarkPermanent(err)
	}
	endpoint, err := c.replyEndpoint(workspace, repository, input)
	if err != nil {
		return forge.MarkPermanent(err)
	}
	payload := bitbucketReplyPayload{Content: bitbucketReplyContent{Raw: input.Body}}
	if input.CommentKind == "comment" {
		payload.Parent = &bitbucketReplyParent{ID: input.CommentID}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode Bitbucket reply: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create Bitbucket reply request: %w", err)
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
			return contextErr
		}
		return fmt.Errorf("Bitbucket reply failed: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, response.Body.Close()) }()
	responseBody, err := readBounded(response.Body)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return fmt.Errorf("read Bitbucket reply response: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return c.httpError(response, responseBody)
	}
	return nil
}

// PullRequestReplyExists searches parent and child comments using a bounded
// window of provider page numbers built from the configured endpoint; response
// next URLs are never followed. A duplicate is possible only when more than
// this window of comments arrives during crash recovery; deterministic liveness
// wins over an unbounded retry scan.
func (c *Client) PullRequestReplyExists(ctx context.Context, workspace, repository string, input forge.ReplyRequest, marker string) (bool, error) {
	if input.PullRequestNumber <= 0 {
		return false, forge.MarkPermanent(errors.New("reply pull request number must be positive"))
	}
	if err := forge.ValidateNormalizedText("reply marker", marker, true); err != nil {
		return false, forge.MarkPermanent(err)
	}
	first, err := c.replyPage(ctx, workspace, repository, input.PullRequestNumber, 1)
	if err != nil {
		return false, err
	}
	lastPage, pageLength, err := first.validatedPagination(1)
	if err != nil {
		return false, fmt.Errorf("validate Bitbucket reply pagination: %w", err)
	}
	if first.contains(marker) {
		return true, nil
	}
	firstRecentPage := max(2, lastPage-recentCommentPageLimit+1)
	for page := lastPage; page >= firstRecentPage; page-- {
		decoded, err := c.replyPage(ctx, workspace, repository, input.PullRequestNumber, page)
		if err != nil {
			return false, err
		}
		candidateLastPage, candidatePageLength, err := decoded.validatedPagination(page)
		if err != nil {
			return false, fmt.Errorf("validate Bitbucket reply pagination page %d: %w", page, err)
		}
		if candidateLastPage != lastPage || candidatePageLength != pageLength {
			return false, errors.New("Bitbucket reply pagination changed during lookup")
		}
		if decoded.contains(marker) {
			return true, nil
		}
	}
	return false, nil
}

func (c *Client) replyPage(ctx context.Context, workspace, repository string, pullRequestNumber, page int) (bitbucketReplyList, error) {
	if err := ctx.Err(); err != nil {
		return bitbucketReplyList{}, err
	}
	endpoint, err := c.replyEndpoint(workspace, repository, forge.ReplyRequest{PullRequestNumber: pullRequestNumber})
	if err != nil {
		return bitbucketReplyList{}, forge.MarkPermanent(err)
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		return bitbucketReplyList{}, fmt.Errorf("parse Bitbucket reply lookup endpoint: %w", err)
	}
	query := target.Query()
	query.Set("pagelen", strconv.Itoa(commentPageSize))
	query.Set("page", strconv.Itoa(page))
	target.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return bitbucketReplyList{}, fmt.Errorf("create Bitbucket reply lookup request: %w", err)
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
			return bitbucketReplyList{}, contextErr
		}
		return bitbucketReplyList{}, fmt.Errorf("Bitbucket reply lookup failed: %w", err)
	}
	body, readErr := readCommentListBounded(response.Body)
	closeErr := response.Body.Close()
	if contextErr := ctx.Err(); contextErr != nil {
		return bitbucketReplyList{}, contextErr
	}
	if readErr != nil || closeErr != nil {
		return bitbucketReplyList{}, fmt.Errorf("read Bitbucket reply lookup response: %w", errors.Join(readErr, closeErr))
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return bitbucketReplyList{}, c.httpError(response, body)
	}
	var decoded bitbucketReplyList
	if err := json.Unmarshal(body, &decoded); err != nil {
		return bitbucketReplyList{}, fmt.Errorf("decode Bitbucket reply lookup response: %w", err)
	}
	return decoded, nil
}

func (c *Client) replyEndpoint(workspace, repository string, input forge.ReplyRequest) (string, error) {
	if input.CommentKind != "" && input.CommentKind != "comment" {
		return "", fmt.Errorf("unsupported Bitbucket comment kind %q", input.CommentKind)
	}
	if input.CommentKind == "comment" && input.CommentID <= 0 {
		return "", errors.New("Bitbucket comment ID must be positive")
	}
	if input.CommentID < 0 {
		return "", errors.New("Bitbucket comment ID is invalid")
	}
	if input.CommentKind == "" && input.CommentID != 0 {
		return "", errors.New("Bitbucket comment kind is required for a comment ID")
	}
	endpoint, err := c.endpoint(workspace, repository)
	if err != nil {
		return "", err
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("parse Bitbucket reply endpoint: %w", err)
	}
	baseEscapedPath := target.EscapedPath()
	suffix := "/" + strconv.Itoa(input.PullRequestNumber) + "/comments"
	target.Path += suffix
	target.RawPath = baseEscapedPath + suffix
	target.RawQuery = ""
	target.Fragment = ""
	return target.String(), nil
}

type bitbucketReplyPayload struct {
	Content bitbucketReplyContent `json:"content"`
	Parent  *bitbucketReplyParent `json:"parent,omitempty"`
}

type bitbucketReplyContent struct {
	Raw string `json:"raw"`
}

type bitbucketReplyParent struct {
	ID int `json:"id"`
}

type bitbucketReplyList struct {
	Values  []bitbucketReplyComment `json:"values"`
	Next    string                  `json:"next"`
	Page    *int                    `json:"page,omitempty"`
	PageLen *int                    `json:"pagelen,omitempty"`
	Size    *int                    `json:"size,omitempty"`
}

func (r bitbucketReplyList) validatedPagination(requestedPage int) (int, int, error) {
	metadataFields := 0
	for _, field := range []*int{r.Page, r.PageLen, r.Size} {
		if field != nil {
			metadataFields++
		}
	}
	if metadataFields == 0 {
		if r.Next != "" {
			return 0, 0, errors.New("next is present without page, pagelen, and size")
		}
		return 1, commentPageSize, nil
	}
	if metadataFields != 3 {
		return 0, 0, errors.New("page, pagelen, and size must be provided together")
	}
	if *r.Page <= 0 || *r.Page != requestedPage {
		return 0, 0, fmt.Errorf("page %d does not match requested page %d", *r.Page, requestedPage)
	}
	if *r.PageLen <= 0 || *r.PageLen > commentPageSize {
		return 0, 0, fmt.Errorf("pagelen %d is invalid", *r.PageLen)
	}
	if *r.Size < 0 {
		return 0, 0, fmt.Errorf("size %d is invalid", *r.Size)
	}
	if len(r.Values) > *r.PageLen || len(r.Values) > *r.Size {
		return 0, 0, errors.New("values are inconsistent with pagelen or size")
	}
	lastPage := 1
	if *r.Size > 0 {
		lastPage = 1 + (*r.Size-1) / *r.PageLen
	}
	if *r.Page > lastPage {
		return 0, 0, fmt.Errorf("page %d exceeds last page %d", *r.Page, lastPage)
	}
	if (*r.Page < lastPage) != (r.Next != "") {
		return 0, 0, fmt.Errorf("next is inconsistent with page %d of %d", *r.Page, lastPage)
	}
	return lastPage, *r.PageLen, nil
}

func (r bitbucketReplyList) contains(marker string) bool {
	for _, comment := range r.Values {
		if comment.contains(marker) {
			return true
		}
	}
	return false
}

type bitbucketReplyComment struct {
	Content  bitbucketReplyContent   `json:"content"`
	Children []bitbucketReplyComment `json:"children"`
}

func (c bitbucketReplyComment) contains(marker string) bool {
	if strings.Contains(c.Content.Raw, marker) {
		return true
	}
	for _, child := range c.Children {
		if child.contains(marker) {
			return true
		}
	}
	return false
}
