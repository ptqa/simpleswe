package protocol

import (
	"fmt"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"
)

const maxFixAttempts = 10

// TaskManifest is the worker's trusted, serialized task boundary. Commands
// are argv vectors, never shell snippets.
type TaskManifest struct {
	TaskID                     string     `json:"task_id"`
	Repository                 string     `json:"repository,omitempty"`
	CloneURL                   string     `json:"clone_url,omitempty"`
	BaseBranch                 string     `json:"base_branch,omitempty"`
	TaskBranch                 string     `json:"task_branch,omitempty"`
	Prompt                     string     `json:"prompt"`
	OpenCodeCommand            []string   `json:"opencode_command,omitempty"`
	ValidationCommand          []string   `json:"validation_command,omitempty"`
	ValidationCommands         [][]string `json:"validation_commands,omitempty"`
	MaxFixAttempts             int        `json:"max_fix_attempts"`
	ForgeProvider              string     `json:"forge_provider"`
	ForgeOwner                 string     `json:"forge_owner"`
	ForgeRepository            string     `json:"forge_repository"`
	RequestedPullRequestTitle  string     `json:"requested_pull_request_title,omitempty"`
	ExistingPullRequestNumber  int        `json:"existing_pull_request_number,omitempty"`
	ExistingPullRequestHeadSHA string     `json:"existing_pull_request_head_sha,omitempty"`
}

// ValidateManifest checks values crossing into the worker. It deliberately
// permits the legacy Repository and ValidationCommand fields used by the
// first worker boundary while accepting the fuller manifest shape as well.
func ValidateManifest(manifest TaskManifest) error {
	if err := validateText("task_id", manifest.TaskID, 128, true); err != nil {
		return err
	}
	if strings.TrimSpace(manifest.Repository) == "" && strings.TrimSpace(manifest.CloneURL) == "" {
		return fmt.Errorf("repository or clone_url is required")
	}
	if err := validateText("repository", manifest.Repository, 2048, false); err != nil {
		return err
	}
	if err := validateCloneURL("clone_url", manifest.CloneURL); err != nil {
		return err
	}
	if err := validateText("prompt", manifest.Prompt, 128*1024, true); err != nil {
		return err
	}
	if err := validateBranch("base_branch", manifest.BaseBranch); err != nil {
		return err
	}
	if err := validateBranch("task_branch", manifest.TaskBranch); err != nil {
		return err
	}
	if manifest.ForgeProvider != "bitbucket" && manifest.ForgeProvider != "github" {
		return fmt.Errorf("forge_provider %q is not supported", manifest.ForgeProvider)
	}
	if err := validateText("forge_owner", manifest.ForgeOwner, 256, true); err != nil {
		return err
	}
	if manifest.ForgeOwner != strings.TrimSpace(manifest.ForgeOwner) {
		return fmt.Errorf("forge_owner must not have surrounding whitespace")
	}
	if err := validateText("forge_repository", manifest.ForgeRepository, 256, true); err != nil {
		return err
	}
	if manifest.ForgeRepository != strings.TrimSpace(manifest.ForgeRepository) {
		return fmt.Errorf("forge_repository must not have surrounding whitespace")
	}
	if manifest.RequestedPullRequestTitle != "" {
		if err := validateText("requested_pull_request_title", manifest.RequestedPullRequestTitle, 1024, true); err != nil {
			return err
		}
		if utf8.RuneCountInString(manifest.RequestedPullRequestTitle) > 256 {
			return fmt.Errorf("requested_pull_request_title is too long")
		}
	}
	if manifest.ExistingPullRequestNumber < 0 {
		return fmt.Errorf("existing_pull_request_number must not be negative")
	}
	if manifest.ExistingPullRequestNumber > 0 && !FullLowerGitObjectID(manifest.ExistingPullRequestHeadSHA) {
		return fmt.Errorf("existing_pull_request_head_sha must be a full lowercase Git object ID when existing_pull_request_number is positive")
	}
	if manifest.ExistingPullRequestNumber == 0 && manifest.ExistingPullRequestHeadSHA != "" {
		return fmt.Errorf("existing_pull_request_head_sha must be empty when existing_pull_request_number is zero")
	}
	if err := validateCommand("opencode_command", manifest.OpenCodeCommand); err != nil {
		return err
	}
	if err := validateCommand("validation_command", manifest.ValidationCommand); err != nil {
		return err
	}
	if len(manifest.ValidationCommands) > 0 {
		for i, command := range manifest.ValidationCommands {
			if len(command) == 0 {
				return fmt.Errorf("validation_commands[%d] is empty", i)
			}
			if err := validateCommand(fmt.Sprintf("validation_commands[%d]", i), command); err != nil {
				return err
			}
		}
	}
	if len(manifest.ValidationCommand) == 0 && len(manifest.ValidationCommands) == 0 {
		return fmt.Errorf("validation command is required")
	}
	if manifest.MaxFixAttempts < 0 || manifest.MaxFixAttempts > maxFixAttempts {
		return fmt.Errorf("max_fix_attempts must be between 0 and %d", maxFixAttempts)
	}
	return nil
}

func validateCloneURL(name, value string) error {
	if value == "" {
		return nil
	}
	if err := validateText(name, value, 2048, true); err != nil {
		return err
	}
	u, err := url.Parse(value)
	if err != nil {
		return fmt.Errorf("%s is not a valid URL: %w", name, err)
	}
	passwordSet := false
	if u.User != nil {
		_, passwordSet = u.User.Password()
	}
	if u.User != nil && !(u.Scheme == "ssh" && u.User.Username() == "git" && !passwordSet) {
		return fmt.Errorf("%s must not contain credentials", name)
	}
	if u.Scheme != "" && u.Host == "" {
		return fmt.Errorf("%s has no host", name)
	}
	return nil
}

func validateBranch(name, value string) error {
	if value == "" {
		return nil
	}
	if err := validateText(name, value, 255, true); err != nil {
		return err
	}
	if strings.HasPrefix(value, "-") || strings.Contains(value, "..") || strings.Contains(value, "//") || strings.Contains(value, "@{") || strings.HasSuffix(value, ".") || strings.HasSuffix(value, "/") {
		return fmt.Errorf("%s is not a safe branch name", name)
	}
	return nil
}

// ValidateBranch checks a branch name crossing a protocol boundary.
func ValidateBranch(value string) error {
	return validateBranch("branch", value)
}

func validateCommand(name string, command []string) error {
	if len(command) == 0 {
		return nil
	}
	if len(command) > 128 {
		return fmt.Errorf("%s has too many arguments", name)
	}
	if strings.TrimSpace(command[0]) == "" {
		return fmt.Errorf("%s executable is empty", name)
	}
	for i, arg := range command {
		if strings.IndexByte(arg, 0) >= 0 {
			return fmt.Errorf("%s[%d] contains NUL", name, i)
		}
		if len(arg) > 4096 {
			return fmt.Errorf("%s[%d] is too long", name, i)
		}
	}
	return nil
}

func validateText(name, value string, maxLength int, required bool) error {
	if required && strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if len(value) > maxLength {
		return fmt.Errorf("%s is too long", name)
	}
	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("%s contains control characters", name)
	}
	return nil
}
