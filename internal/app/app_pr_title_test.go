package app

import (
	"context"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/simpleswe/simpleswe/internal/client"
)

func TestRunParsesPrTitle(t *testing.T) {
	var gotRequest client.CreateTaskRequest
	deps := Dependencies{
		PortForward: func(context.Context, string, string) (string, func() error, error) {
			return "http://controller", func() error { return nil }, nil
		},
		CreateTask: func(_ context.Context, _ string, request client.CreateTaskRequest) (client.Task, error) {
			gotRequest = request
			return client.Task{}, nil
		},
	}
	args := []string{"task", "create", "--pr-title", "My PR", "repo", "prompt"}

	if err := Run(context.Background(), args, strings.NewReader(""), io.Discard, io.Discard, deps); err != nil {
		t.Fatalf("Run(%q) error = %v", args, err)
	}

	want := client.CreateTaskRequest{Repository: "repo", Prompt: "prompt", PRTitle: "My PR"}
	if !reflect.DeepEqual(gotRequest, want) {
		t.Fatalf("CreateTaskRequest = %#v, want %#v", gotRequest, want)
	}
}

func TestRunParsesPrTitleWithRuntimeFlagOrdering(t *testing.T) {
	var gotAddress string
	var gotRequest client.CreateTaskRequest
	deps := Dependencies{
		PortForward: func(context.Context, string, string) (string, func() error, error) {
			t.Fatal("create used port-forward with an explicit address")
			return "", nil, nil
		},
		CreateTask: func(_ context.Context, address string, request client.CreateTaskRequest) (client.Task, error) {
			gotAddress, gotRequest = address, request
			return client.Task{}, nil
		},
	}
	args := []string{
		"task", "create", "--idempotency-key", "KEY", "--address", "https://controller.example", "--pr-title", "My PR",
		"repo", "prompt",
	}

	if err := Run(context.Background(), args, strings.NewReader(""), io.Discard, io.Discard, deps); err != nil {
		t.Fatalf("Run(%q) error = %v", args, err)
	}

	if gotAddress != "https://controller.example" {
		t.Fatalf("create address = %q, want explicit address", gotAddress)
	}
	want := client.CreateTaskRequest{Repository: "repo", Prompt: "prompt", PRTitle: "My PR", IdempotencyKey: "KEY"}
	if !reflect.DeepEqual(gotRequest, want) {
		t.Fatalf("CreateTaskRequest = %#v, want %#v", gotRequest, want)
	}
}

func TestRunRejectsInvalidPrTitle(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "duplicate",
			args: []string{"task", "create", "--pr-title", "one", "--pr-title", "two", "repo", "prompt"},
		},
		{
			name: "missing value",
			args: []string{"task", "create", "--pr-title", "--address", "https://controller.example", "repo", "prompt"},
		},
		{
			name: "empty value",
			args: []string{"task", "create", "--pr-title", "", "repo", "prompt"},
		},
		{
			name: "whitespace value",
			args: []string{"task", "create", "--pr-title", " \t\n ", "repo", "prompt"},
		},
		{
			name: "too long",
			args: []string{"task", "create", "--pr-title", strings.Repeat("界", 257), "repo", "prompt"},
		},
		{
			name: "late placement",
			args: []string{"task", "create", "repo", "prompt", "--pr-title", "My PR"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Run(context.Background(), tt.args, strings.NewReader(""), io.Discard, io.Discard, Dependencies{}); err == nil || err.Error() != usageTaskCreate {
				t.Fatalf("Run(%q) error = %v, want %q", tt.args, err, usageTaskCreate)
			}
		})
	}
}

func TestTaskCreateUsageIncludesPrTitle(t *testing.T) {
	if taskCreateUsage != usageTaskCreate {
		t.Fatalf("task create usage = %q, want %q", taskCreateUsage, usageTaskCreate)
	}
}

func TestRunParsesPrTitleWithHyphenPrefixedPrompt(t *testing.T) {
	var gotRequest client.CreateTaskRequest
	deps := Dependencies{
		PortForward: func(context.Context, string, string) (string, func() error, error) {
			return "http://controller", func() error { return nil }, nil
		},
		CreateTask: func(_ context.Context, _ string, request client.CreateTaskRequest) (client.Task, error) {
			gotRequest = request
			return client.Task{}, nil
		},
	}
	args := []string{"task", "create", "--pr-title", "My PR", "repo", "- update the failing test"}

	if err := Run(context.Background(), args, strings.NewReader(""), io.Discard, io.Discard, deps); err != nil {
		t.Fatalf("Run(%q) error = %v", args, err)
	}

	want := client.CreateTaskRequest{Repository: "repo", Prompt: "- update the failing test", PRTitle: "My PR"}
	if !reflect.DeepEqual(gotRequest, want) {
		t.Fatalf("CreateTaskRequest = %#v, want %#v", gotRequest, want)
	}
}
