package config

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestLoadAppliesSafeDefaults(t *testing.T) {
	cfg, err := Load(strings.NewReader(`
bitbucket:
  webhook_secret:
    file: /run/secrets/webhooks/bitbucket
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
	if got, want := cfg.Controller.WebhookListenAddress, ":8081"; got != want {
		t.Errorf("controller webhook listen address = %q, want %q", got, want)
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
	if got, want := cfg.Controller.ReviewDebounce, 30*time.Minute; got != want {
		t.Errorf("controller review debounce = %s, want %s", got, want)
	}
	if got, want := cfg.Controller.MaxFixAttempts, 3; got != want {
		t.Errorf("controller max fix attempts = %d, want %d", got, want)
	}
}

func TestLoadRejectsSlackAndLoadsSlackFreeConfig(t *testing.T) {
	if _, err := Load(strings.NewReader("repositories: []\n")); err != nil {
		t.Fatalf("load Slack-free config: %v", err)
	}
	if _, err := Load(strings.NewReader("slack:\n  disabled: true\nrepositories: []\n")); err == nil || !strings.Contains(strings.ToLower(err.Error()), "slack") {
		t.Fatalf("Load slack key error = %v, want unknown top-level slack key", err)
	}
}

func TestLoadPreservesRepositoryWorkerConfiguration(t *testing.T) {
	cfg, err := Load(strings.NewReader(`
controller:
  namespace: engineering
  review_debounce: 5m
bitbucket:
  webhook_secret:
    file: /run/secrets/webhooks/bitbucket
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
	if got, want := cfg.Controller.ReviewDebounce, 5*time.Minute; got != want {
		t.Errorf("controller review debounce = %s, want %s", got, want)
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
bitbucket:
  webhook_secret:
    file: /run/secrets/webhooks/bitbucket
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
bitbucket:
  base_url: http://127.0.0.1:8080
  webhook_secret:
    file: /run/secrets/webhooks/bitbucket
repositories:
  widget:
    worker: {image: worker:v1}
    bitbucket: {workspace: acme, repository: widget, credentials_secret_name: widget-bitbucket}
`)); err != nil {
		t.Fatalf("Load loopback test server: %v", err)
	}
}

func TestLoadSidecarsAndRejectsMalformedSidecars(t *testing.T) {
	valid := `
bitbucket:
  webhook_secret: {file: /run/secrets/webhooks/bitbucket}
repositories:
  - clone_url: https://bitbucket.example/acme/widget.git
    default_branch: main
    worker:
      image: worker:v1
      sidecars:
        - name: database
          image: registry.example/database:v1
          command: [database]
          args: [--listen, "3306"]
          env:
            - name: DATABASE_PASSWORD
              secret: {name: database-secret, key: password}
            - name: DATABASE_MODE
              value: test
          resources:
            requests: {cpu: 100m, memory: 128Mi}
          startup_probe:
            port: 3306
            period_seconds: 5
            timeout_seconds: 2
            failure_threshold: 30
          security_context: {runAsNonRoot: true}
    bitbucket: {workspace: acme, repository: widget, credentials_secret_name: widget-bitbucket}
`
	cfg, err := Load(strings.NewReader(valid))
	if err != nil {
		t.Fatalf("Load valid sidecar config: %v", err)
	}
	sidecar := cfg.Repositories[0].Worker.Sidecars[0]
	if sidecar.Name != "database" || sidecar.Image != "registry.example/database:v1" || fmt.Sprint(sidecar.Command) != "[database]" || fmt.Sprint(sidecar.Args) != "[--listen 3306]" {
		t.Fatalf("sidecar identity/command = %#v", sidecar)
	}
	if sidecar.StartupProbe == nil || sidecar.StartupProbe.Port != 3306 {
		t.Fatalf("sidecar startup probe = %#v", sidecar.StartupProbe)
	}
	if sidecar.StartupProbe.PeriodSeconds == nil || *sidecar.StartupProbe.PeriodSeconds != 5 || sidecar.StartupProbe.TimeoutSeconds == nil || *sidecar.StartupProbe.TimeoutSeconds != 2 || sidecar.StartupProbe.FailureThreshold == nil || *sidecar.StartupProbe.FailureThreshold != 30 {
		t.Fatalf("sidecar startup probe timing = %#v", sidecar.StartupProbe)
	}
	if sidecar.SecurityContext["runAsNonRoot"] != true {
		t.Fatalf("sidecar security context = %#v", sidecar.SecurityContext)
	}
	if sidecar.Env[0].Value != "" || sidecar.Env[0].Secret == nil || sidecar.Env[0].Secret.Name != "database-secret" {
		t.Fatalf("sidecar secret env = %#v", sidecar.Env[0])
	}
	withoutTiming := strings.NewReplacer("            period_seconds: 5\n", "", "            timeout_seconds: 2\n", "", "            failure_threshold: 30\n", "").Replace(valid)
	defaults, err := Load(strings.NewReader(withoutTiming))
	if err != nil {
		t.Fatalf("Load sidecar config with native probe defaults: %v", err)
	}
	defaultProbe := defaults.Repositories[0].Worker.Sidecars[0].StartupProbe
	if defaultProbe == nil || defaultProbe.PeriodSeconds != nil || defaultProbe.TimeoutSeconds != nil || defaultProbe.FailureThreshold != nil {
		t.Fatalf("omitted startup probe timing = %#v; want native defaults", defaultProbe)
	}

	for name, input := range map[string]string{
		"missing name":         strings.Replace(valid, "name: database\n", "name:\n", 1),
		"missing image":        strings.Replace(valid, "          image: registry.example/database:v1\n", "", 1),
		"invalid image":        strings.Replace(valid, "          image: registry.example/database:v1\n", "          image: 'database image'\n", 1),
		"missing startup":      strings.Replace(valid, "          startup_probe:\n", "", 1),
		"invalid startup port": strings.Replace(valid, "            port: 3306\n", "            port: 0\n", 1),
		"invalid probe period": strings.Replace(valid, "            period_seconds: 5\n", "            period_seconds: 0\n", 1),
		"unknown field":        strings.Replace(valid, "          security_context: {runAsNonRoot: true}\n", "          security_context: {runAsNonRoot: true}\n          unknown: true\n", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(strings.NewReader(input)); err == nil {
				t.Fatalf("Load accepted malformed sidecar config")
			}
		})
	}
}
