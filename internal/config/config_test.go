package config

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLoadAppliesSafeDefaults(t *testing.T) {
	cfg, err := Load(strings.NewReader(`
repositories:
  - clone_url: https://bitbucket.example/acme/widget.git
    default_branch: main
    worker:
      image: ghcr.io/acme/widget-worker:latest
    bitbucket:
      workspace: acme
      repository: widget
      credentials_secret_name: widget-bitbucket
`))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if got, want := cfg.Controller.ListenAddress, ":8080"; got != want {
		t.Errorf("controller listen address = %q, want %q", got, want)
	}
	if got, want := cfg.Controller.Namespace, "simpleswe"; got != want {
		t.Errorf("controller namespace = %q, want %q", got, want)
	}
	if got, want := cfg.Worker.Command, "opencode"; got != want {
		t.Errorf("worker command = %q, want %q", got, want)
	}
	if got, want := cfg.Repositories[0].OpenCode.Command, []string{"opencode", "run"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("default OpenCode argv = %#v, want %#v", got, want)
	}
	if got, want := cfg.Worker.BranchPrefix, "simpleswe/"; got != want {
		t.Errorf("worker branch prefix = %q, want %q", got, want)
	}
	if got, want := cfg.Controller.Deadline, 30*time.Minute; got != want {
		t.Errorf("controller deadline = %s, want %s", got, want)
	}
	if got, want := cfg.Controller.MaxFixAttempts, 3; got != want {
		t.Errorf("controller max fix attempts = %d, want %d", got, want)
	}
}

func TestLoadAllowsDisabledSlack(t *testing.T) {
	cfg, err := Load(strings.NewReader("slack:\n  disabled: true\nrepositories: []\n"))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.Slack.Disabled {
		t.Fatal("Slack disabled setting was not preserved")
	}
}

func TestLoadPreservesRepositoryWorkerConfiguration(t *testing.T) {
	cfg, err := Load(strings.NewReader(`
controller:
  namespace: engineering
repositories:
  - clone_url: https://bitbucket.example/acme/widget.git
    default_branch: trunk
    credentials:
      secret_name: bitbucket-widget
    worker:
      image: ghcr.io/acme/widget-worker:v7
      resources:
        requests:
          cpu: 250m
          memory: 512Mi
        limits:
          cpu: "2"
          memory: 2Gi
      scheduling:
        node_selector:
          kubernetes.io/arch: arm64
        tolerations:
          - key: workload
            operator: Equal
            value: worker
            effect: NoSchedule
      mounts:
        - name: workspace-cache
          mount_path: /workspace/.cache
          empty_dir:
            medium: Memory
    bitbucket:
      workspace: acme
      repository: widget
      credentials_secret_name: widget-bitbucket
`))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if len(cfg.Repositories) != 1 {
		t.Fatalf("repositories = %d, want 1", len(cfg.Repositories))
	}
	repository := cfg.Repositories[0]
	if got, want := repository.CloneURL, "https://bitbucket.example/acme/widget.git"; got != want {
		t.Errorf("clone URL = %q, want %q", got, want)
	}
	if got, want := repository.DefaultBranch, "trunk"; got != want {
		t.Errorf("default branch = %q, want %q", got, want)
	}
	if got, want := repository.Credentials.SecretName, "bitbucket-widget"; got != want {
		t.Errorf("credential Secret name = %q, want %q", got, want)
	}
	if got, want := repository.Worker.Image, "ghcr.io/acme/widget-worker:v7"; got != want {
		t.Errorf("worker image = %q, want %q", got, want)
	}

	// Quantities must remain Kubernetes quantity strings rather than being
	// converted through floating-point numbers or otherwise normalized.
	if got, want := fmt.Sprint(repository.Worker.Resources.Requests["cpu"]), "250m"; got != want {
		t.Errorf("CPU request = %q, want %q", got, want)
	}
	if got, want := fmt.Sprint(repository.Worker.Resources.Limits["memory"]), "2Gi"; got != want {
		t.Errorf("memory limit = %q, want %q", got, want)
	}
	if got, want := fmt.Sprint(repository.Worker.Scheduling.NodeSelector["kubernetes.io/arch"]), "arm64"; got != want {
		t.Errorf("node selector = %q, want %q", got, want)
	}
	if len(repository.Worker.Scheduling.Tolerations) != 1 {
		t.Fatalf("tolerations = %d, want 1", len(repository.Worker.Scheduling.Tolerations))
	}
	if got, want := fmt.Sprint(repository.Worker.Scheduling.Tolerations[0].Effect), "NoSchedule"; got != want {
		t.Errorf("toleration effect = %q, want %q", got, want)
	}
	if len(repository.Worker.Mounts) != 1 {
		t.Fatalf("mounts = %d, want 1", len(repository.Worker.Mounts))
	}
	if got, want := repository.Worker.Mounts[0].MountPath, "/workspace/.cache"; got != want {
		t.Errorf("mount path = %q, want %q", got, want)
	}
	if got, want := fmt.Sprint(repository.Worker.Mounts[0].EmptyDir.Medium), "Memory"; got != want {
		t.Errorf("emptyDir medium = %q, want %q", got, want)
	}
}

func TestLoadRejectsMissingRequiredRepositoryFields(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "clone URL",
			yaml: `repositories:
  - default_branch: main
    worker:
      image: ghcr.io/acme/worker:v1
`,
		},
		{
			name: "default branch",
			yaml: `repositories:
  - clone_url: https://bitbucket.example/acme/widget.git
    worker:
      image: ghcr.io/acme/worker:v1
`,
		},
		{
			name: "worker image",
			yaml: `repositories:
  - clone_url: https://bitbucket.example/acme/widget.git
    default_branch: main
    worker: {}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Load(strings.NewReader(tt.yaml)); err == nil {
				t.Fatalf("Load accepted config without %s", tt.name)
			}
		})
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	_, err := Load(strings.NewReader(`
repositories:
  - clone_url: https://bitbucket.example/acme/widget.git
    default_branch: main
    worker:
      image: ghcr.io/acme/worker:v1
      unrecognized_field: true
`))
	if err == nil {
		t.Fatal("Load accepted an unknown field")
	}
}

func TestLoadRejectsInvalidSecretNames(t *testing.T) {
	for _, secretName := range []string{"Bitbucket-Credentials", "credentials/name", ""} {
		t.Run(secretName, func(t *testing.T) {
			_, err := Load(strings.NewReader(`
repositories:
  - clone_url: https://bitbucket.example/acme/widget.git
    default_branch: main
    credentials:
      secret_name: ` + secretName + `
    worker:
      image: ghcr.io/acme/worker:v1
`))
			if err == nil {
				t.Fatalf("Load accepted invalid Secret name %q", secretName)
			}
		})
	}
}

func TestLoadRejectsInlineCredentialValues(t *testing.T) {
	_, err := Load(strings.NewReader(`
repositories:
  - clone_url: https://build-user:super-secret@bitbucket.example/acme/widget.git
    default_branch: main
    worker:
      image: ghcr.io/acme/worker:v1
`))
	if err == nil {
		t.Fatal("Load accepted credentials embedded in clone_url")
	}
}

func TestLoadValidatesRepositoryBitbucketCredentials(t *testing.T) {
	base := `
repositories:
  widget:
    worker: {image: worker:v1}
    bitbucket:
      workspace: acme
      repository: widget
      credentials_secret_name: widget-bitbucket
`
	if _, err := Load(strings.NewReader(base)); err != nil {
		t.Fatalf("Load valid repository credentials: %v", err)
	}

	for name, yaml := range map[string]string{
		"missing secret ref": strings.Replace(base, "      credentials_secret_name: widget-bitbucket\n", "", 1),
		"unsafe secret ref":  strings.Replace(base, "widget-bitbucket", "../widget", 1),
		"legacy secret tag":  strings.Replace(base, "credentials_secret_name", "credentials_secret", 1),
		"global username":    base + "bitbucket:\n  username: {env: BITBUCKET_USERNAME}\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(strings.NewReader(yaml)); err == nil {
				t.Fatal("Load accepted ambiguous or unsafe Bitbucket credentials")
			}
		})
	}
}

func TestLoadRejectsDuplicateForgeCoordinatesWithConflictingCredentials(t *testing.T) {
	_, err := Load(strings.NewReader(`
repositories:
  widget-a:
    worker: {image: worker:v1}
    bitbucket: {workspace: Acme, repository: Widget, credentials_secret_name: first-secret}
  widget-b:
    worker: {image: worker:v1}
    bitbucket: {workspace: acme, repository: widget, credentials_secret_name: second-secret}
`))
	if err == nil || !strings.Contains(err.Error(), "conflicts with credentials") {
		t.Fatalf("Load duplicate forge coordinates error = %v", err)
	}
}

func TestLoadRequiresHTTPSBitbucketBaseURLExceptLoopback(t *testing.T) {
	for _, baseURL := range []string{"http://bitbucket.example", "https://user:password@bitbucket.example", "https://bitbucket.example?password=secret"} {
		yaml := fmt.Sprintf(`
bitbucket: {base_url: %q}
repositories:
  widget:
    worker: {image: worker:v1}
    bitbucket: {workspace: acme, repository: widget, credentials_secret_name: widget-bitbucket}
`, baseURL)
		if _, err := Load(strings.NewReader(yaml)); err == nil {
			t.Errorf("Load accepted unsafe Bitbucket base URL %q", baseURL)
		}
	}
	if _, err := Load(strings.NewReader(`
bitbucket: {base_url: http://127.0.0.1:8080}
repositories:
  widget:
    worker: {image: worker:v1}
    bitbucket: {workspace: acme, repository: widget, credentials_secret_name: widget-bitbucket}
`)); err != nil {
		t.Fatalf("Load loopback test server: %v", err)
	}
}

func TestExampleConfigUsesRepositoryCredentialTag(t *testing.T) {
	file, err := os.Open("../../examples/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	cfg, err := Load(file)
	if err != nil {
		t.Fatalf("Load example config: %v", err)
	}
	if got := cfg.Repositories[0].Bitbucket.CredentialsSecret; got != "bitbucket-widget" {
		t.Fatalf("example Bitbucket credentials Secret = %q", got)
	}
}
