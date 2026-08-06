package config

import (
	"fmt"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestLoadGitHubRepositoryAppliesDefaults(t *testing.T) {
	for _, test := range []struct {
		name      string
		sshSecret string
		cloneURL  string
	}{
		{name: "https", cloneURL: "https://github.com/Acme%20Org/Widget%2FAPI.git"},
		{name: "ssh", sshSecret: "git-ssh", cloneURL: "ssh://git@github.com/Acme%20Org/Widget%2FAPI.git"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ssh := ""
			if test.sshSecret != "" {
				ssh = "\n    git:\n      ssh_secret: " + test.sshSecret
			}
			cfg, err := Load(strings.NewReader(`
github: {}
repositories:
  - worker: {image: worker:v1}
    github:
      owner: Acme Org
      repository: Widget/API
      credentials_secret_name: github-widget` + ssh + "\n"))
			if err != nil {
				t.Fatalf("Load GitHub repository: %v", err)
			}

			encoded, err := yaml.Marshal(cfg)
			if err != nil {
				t.Fatalf("marshal GitHub config: %v", err)
			}
			for _, want := range []string{
				"base_url: https://api.github.com",
				"default_branch: main",
				"clone_url: " + test.cloneURL,
				"credentials_secret_name: github-widget",
			} {
				if !strings.Contains(string(encoded), want) {
					t.Errorf("loaded config does not contain %q:\n%s", want, encoded)
				}
			}
		})
	}
}

func TestLoadRequiresExactlyOneForge(t *testing.T) {
	for _, test := range []struct {
		name string
		yaml string
	}{
		{
			name: "neither",
			yaml: `
repositories:
  - clone_url: https://github.com/acme/widget.git
    default_branch: main
    worker: {image: worker:v1}
`,
		},
		{
			name: "both",
			yaml: `
repositories:
  - clone_url: https://github.com/acme/widget.git
    default_branch: main
    worker: {image: worker:v1}
    bitbucket: {workspace: acme, repository: widget, credentials_secret_name: bitbucket-widget}
    github: {owner: acme, repository: widget, credentials_secret_name: github-widget}
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Load(strings.NewReader(test.yaml))
			if err == nil {
				t.Fatalf("Load forge selection error = %v", err)
			}
		})
	}
}

func TestLoadValidatesGitHubCoordinatesAndCredentials(t *testing.T) {
	base := `
github: {}
repositories:
  widget:
    worker: {image: worker:v1}
    github:
      owner: acme
      repository: widget
      credentials_secret_name: github-widget
`
	for _, test := range []struct {
		name string
		yaml string
	}{
		{
			name: "blank owner",
			yaml: strings.Replace(base, "owner: acme", "owner: ' '", 1),
		},
		{
			name: "owner surrounding whitespace",
			yaml: strings.Replace(base, "owner: acme", "owner: ' acme'", 1),
		},
		{
			name: "blank repository",
			yaml: strings.Replace(base, "repository: widget", "repository: ' '", 1),
		},
		{
			name: "repository surrounding whitespace",
			yaml: strings.Replace(base, "repository: widget", "repository: 'widget '", 1),
		},
		{
			name: "invalid Secret name",
			yaml: strings.Replace(base, "github-widget", "Bad_Name", 1),
		},
		{
			name: "missing Secret name",
			yaml: strings.Replace(base, "      credentials_secret_name: github-widget\n", "", 1),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Load(strings.NewReader(test.yaml))
			if err == nil {
				t.Fatal("Load accepted invalid GitHub coordinates or credentials")
			}
		})
	}
}

func TestLoadAllowsDuplicateGitHubCoordinatesOnlyWithSameCredentials(t *testing.T) {
	valid := `
github: {}
repositories:
  widget-a:
    worker: {image: worker:v1}
    github: {owner: Acme, repository: Widget, credentials_secret_name: github-widget}
  widget-b:
    worker: {image: worker:v1}
    github: {owner: acme, repository: widget, credentials_secret_name: github-widget}
`
	if _, err := Load(strings.NewReader(valid)); err != nil {
		t.Fatalf("Load duplicate GitHub coordinates with shared Secret: %v", err)
	}

	conflicting := strings.Replace(valid, "credentials_secret_name: github-widget}", "credentials_secret_name: other-github}", 1)
	if _, err := Load(strings.NewReader(conflicting)); err == nil || !strings.Contains(err.Error(), "github conflicts with credentials") {
		t.Fatalf("Load duplicate GitHub coordinates error = %v", err)
	}
}

func TestLoadRequiresHTTPSGitHubBaseURLExceptLoopback(t *testing.T) {
	for _, baseURL := range []string{
		"http://github.example",
		"https://user:password@github.example",
		"https://github.example?token=secret",
		"https://github.example/#fragment",
	} {
		yaml := fmt.Sprintf(`
github: {base_url: %q}
repositories:
  widget:
    worker: {image: worker:v1}
    github: {owner: acme, repository: widget, credentials_secret_name: github-widget}
`, baseURL)
		if _, err := Load(strings.NewReader(yaml)); err == nil {
			t.Errorf("Load GitHub base URL %q error = %v", baseURL, err)
		}
	}

	if _, err := Load(strings.NewReader(`
github: {base_url: http://127.0.0.1:8080}
repositories:
  widget:
    worker: {image: worker:v1}
    github: {owner: acme, repository: widget, credentials_secret_name: github-widget}
`)); err != nil {
		t.Fatalf("Load loopback GitHub test server: %v", err)
	}
}
