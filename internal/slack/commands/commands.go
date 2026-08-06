package commands

import (
	"errors"
	"strings"
	"unicode"
)

// Command is one explicitly supported Slack command.
type Command struct {
	Name   string
	Repo   string
	Prompt string
	TaskID string
}

var errInvalidCommand = errors.New("invalid Slack command")

// Parse parses Slack app-mention text or the text supplied to a slash command.
func Parse(text string) (Command, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return Command{}, errInvalidCommand
	}

	first, rest, ok := nextToken(text)
	if !ok {
		return Command{}, errInvalidCommand
	}
	if isMention(first) {
		first, rest, ok = nextToken(rest)
		if !ok {
			return Command{}, errInvalidCommand
		}
	}

	switch first {
	case "run":
		repo, prompt, ok := nextToken(rest)
		if !ok {
			return Command{}, errInvalidCommand
		}
		prompt = strings.TrimSpace(prompt)
		if prompt == "" {
			return Command{}, errInvalidCommand
		}
		return Command{Name: "run", Repo: repo, Prompt: prompt}, nil
	case "status", "cancel", "retry":
		taskID, extra, ok := nextToken(rest)
		if !ok || strings.TrimSpace(extra) != "" {
			return Command{}, errInvalidCommand
		}
		return Command{Name: first, TaskID: taskID}, nil
	default:
		return Command{}, errInvalidCommand
	}
}

func nextToken(text string) (token, rest string, ok bool) {
	text = strings.TrimLeftFunc(text, unicode.IsSpace)
	if text == "" {
		return "", "", false
	}
	for index, character := range text {
		if unicode.IsSpace(character) {
			return text[:index], text[index:], true
		}
	}
	return text, "", true
}

func isMention(token string) bool {
	if len(token) < 4 || !strings.HasPrefix(token, "<@") || !strings.HasSuffix(token, ">") {
		return false
	}
	inner := token[2 : len(token)-1]
	return inner != "" && !strings.ContainsAny(inner, "<> \t\r\n")
}
