package config

import (
	"strings"
	"testing"
)

func TestLoadProviderWebhookSecretSources(t *testing.T) {
	cfg, err := Load(strings.NewReader(`
github:
  webhook_secret:
    file: /run/secrets/webhooks/github
bitbucket:
  webhook_secret:
    env: BITBUCKET_WEBHOOK_SECRET
repositories:
  - clone_url: https://github.com/acme/widget.git
    default_branch: main
    worker: {image: worker:v1}
    github:
      owner: acme
      repository: widget
      credentials_secret_name: github-widget
  - clone_url: https://bitbucket.org/acme/gadget.git
    default_branch: main
    worker: {image: worker:v1}
    bitbucket:
      workspace: acme
      repository: gadget
      credentials_secret_name: bitbucket-gadget
`))
	if err != nil {
		t.Fatalf("Load provider webhook secrets: %v", err)
	}
	if got, want := cfg.GitHub.WebhookSecret, (SecretSource{File: "/run/secrets/webhooks/github"}); got != want {
		t.Fatalf("GitHub webhook secret = %#v, want %#v", got, want)
	}
	if got, want := cfg.Bitbucket.WebhookSecret, (SecretSource{Env: "BITBUCKET_WEBHOOK_SECRET"}); got != want {
		t.Fatalf("Bitbucket webhook secret = %#v, want %#v", got, want)
	}
}

func TestLoadRequiresWebhookSecretForConfiguredProviderRepository(t *testing.T) {
	tests := map[string]string{
		"GitHub": `
github: {}
repositories:
  - clone_url: https://github.com/acme/widget.git
    default_branch: main
    worker: {image: worker:v1}
    github: {owner: acme, repository: widget, credentials_secret_name: github-widget}
`,
		"Bitbucket": `
bitbucket: {}
repositories:
  - clone_url: https://bitbucket.org/acme/widget.git
    default_branch: main
    worker: {image: worker:v1}
    bitbucket: {workspace: acme, repository: widget, credentials_secret_name: bitbucket-widget}
`,
		"empty source": `
github:
  webhook_secret: {}
repositories:
  - clone_url: https://github.com/acme/widget.git
    default_branch: main
    worker: {image: worker:v1}
    github: {owner: acme, repository: widget, credentials_secret_name: github-widget}
`,
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(strings.NewReader(input)); err == nil || !strings.Contains(err.Error(), "webhook_secret") {
				t.Fatalf("Load webhook configuration error = %v, want required webhook_secret error", err)
			}
		})
	}
}

func TestLoadValidatesWebhookSecretSourcesStrictly(t *testing.T) {
	for name, input := range map[string]string{
		"unknown field": `
github:
  webhook_secret: {file: /run/secrets/webhooks/github, value: inline-secret}
repositories: []
`,
		"file and env": `
github:
  webhook_secret: {file: /run/secrets/webhooks/github, env: GITHUB_WEBHOOK_SECRET}
repositories: []
`,
		"relative file": `
github:
  webhook_secret: {file: webhooks/github}
repositories: []
`,
		"invalid environment": `
bitbucket:
  webhook_secret: {env: bitbucket-webhook-secret}
repositories: []
`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(strings.NewReader(input)); err == nil {
				t.Fatal("Load accepted invalid webhook secret source")
			}
		})
	}
}

func TestLoadAllowsUnconfiguredProviderWithoutWebhookSecret(t *testing.T) {
	if _, err := Load(strings.NewReader("repositories: []\n")); err != nil {
		t.Fatalf("Load config without provider repositories: %v", err)
	}
}
