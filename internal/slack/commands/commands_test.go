package commands

import (
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name string
		text string
		want Command
	}{
		{
			name: "app mention run",
			text: "<@U123ABC> run workspace/repository fix   the flaky test",
			want: Command{
				Name:   "run",
				Repo:   "workspace/repository",
				Prompt: "fix   the flaky test",
			},
		},
		{
			name: "slash run",
			text: "run workspace/repository fix the flaky test",
			want: Command{
				Name:   "run",
				Repo:   "workspace/repository",
				Prompt: "fix the flaky test",
			},
		},
		{
			name: "status",
			text: "status task-123",
			want: Command{Name: "status", TaskID: "task-123"},
		},
		{
			name: "cancel",
			text: "cancel task-123",
			want: Command{Name: "cancel", TaskID: "task-123"},
		},
		{
			name: "retry",
			text: "retry task-123",
			want: Command{Name: "retry", TaskID: "task-123"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.text)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.text, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse(%q) = %#v, want %#v", tt.text, got, tt.want)
			}
		})
	}
}

func TestParseRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{name: "empty", text: ""},
		{name: "mention only", text: "<@U123ABC>"},
		{name: "run without repository", text: "run"},
		{name: "run without prompt", text: "run workspace/repository"},
		{name: "unknown command", text: "deploy workspace/repository now"},
		{name: "arbitrary text", text: "please run workspace/repository fix it"},
		{name: "status without task", text: "status"},
		{name: "status with extra argument", text: "status task-123 now"},
		{name: "cancel without task", text: "cancel"},
		{name: "cancel with extra argument", text: "cancel task-123 now"},
		{name: "retry without task", text: "retry"},
		{name: "retry with extra argument", text: "retry task-123 now"},
		{name: "command after arbitrary text", text: "hello <@U123ABC> run workspace/repository fix it"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.text)
			if err == nil {
				t.Fatalf("Parse(%q) = %#v, want an error", tt.text, got)
			}
			if got.Name == "run" {
				t.Fatalf("Parse(%q) interpreted invalid input as run", tt.text)
			}
		})
	}
}

func TestParseRepeatedDeliveryIsDeterministic(t *testing.T) {
	text := "<@U123ABC> run workspace/repository preserve this response input"

	first, err := Parse(text)
	if err != nil {
		t.Fatalf("first Parse(%q): %v", text, err)
	}
	second, err := Parse(text)
	if err != nil {
		t.Fatalf("second Parse(%q): %v", text, err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("repeated Parse(%q) = %#v then %#v", text, first, second)
	}
}
