package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadRepositoryRegistryProductionShape(t *testing.T) {
	cfg, err := Load(strings.NewReader(`
repositories:
  widget:
    worker:
      image: registry.example/widget:v1
      deadline: 20m
      resources:
        requests: {cpu: 250m, memory: 512Mi}
      node_selector: {workload: swe}
      tolerations: [{key: dedicated, operator: Equal, value: swe, effect: NoSchedule}]
      affinity:
        nodeAffinity: {}
      priority_class_name: swe
      service_account_name: worker
      image_pull_secrets: [registry]
      env:
        - name: MODE
          value: production
        - name: API_TOKEN
          secret: {name: worker-secret, key: token}
      mounted_secrets: [{name: tools-secret, mount_path: /run/secrets/tools}]
      mounted_config_maps: [{name: tools-config, mount_path: /etc/tools}]
    git:
      branch_prefix: swe/
      ssh_secret: git-ssh
    opencode:
      command: [opencode, run]
      config_secret: opencode-config
    validation:
      commands: [[go, test, ./...], [go, vet, ./...]]
      max_fix_attempts: 2
    bitbucket:
      workspace: acme
      repository: widget
      credentials_secret_name: bitbucket-app-password
`))
	if err != nil {
		t.Fatalf("Load production config: %v", err)
	}
	repository := cfg.Repositories[0]
	if repository.Name != "widget" || repository.CloneURL != "ssh://git@bitbucket.org/acme/widget.git" || repository.DefaultBranch != "main" {
		t.Fatalf("repository defaults = %#v", repository)
	}
	if repository.Worker.Deadline.Value() != 20*time.Minute || len(repository.Worker.MountedConfigMaps) != 1 {
		t.Fatalf("worker config = %#v", repository.Worker)
	}
	if repository.Validation.MaxFixAttempts == nil || *repository.Validation.MaxFixAttempts != 2 {
		t.Fatalf("validation config = %#v", repository.Validation)
	}
}
