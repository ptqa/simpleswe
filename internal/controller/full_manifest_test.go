package controller

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestCreateTaskMapsRepositoryConfigurationIntoFullManifestAndJob(t *testing.T) {
	fixture := newFixture(t)
	maxFixes := 2
	repository := &fixture.config.Repositories[0]
	repository.Worker.Deadline = config.Duration(7 * time.Minute)
	repository.Worker.NodeSelector = map[string]string{"workload": "swe"}
	repository.Worker.PriorityClassName = "swe-worker"
	repository.Worker.ServiceAccountName = "swe-worker"
	repository.Worker.ImagePullSecrets = []string{"registry"}
	repository.Worker.MountedConfigMaps = []config.NamedMount{{Name: "worker-config", MountPath: "/etc/worker"}}
	repository.Git = config.GitConfig{BranchPrefix: "work/", SSHSecret: "git-ssh"}
	repository.OpenCode = config.OpenCodeConfig{Command: []string{"opencode", "run"}, ConfigSecret: "opencode-config"}
	repository.Validation = config.ValidationConfig{Commands: [][]string{{"go", "test", "./..."}}, MaxFixAttempts: &maxFixes}
	control, err := New(fixture.store, fixture.kube, fixture.config, fixture.pullRequests)
	if err != nil {
		t.Fatalf("recreate controller: %v", err)
	}
	created, err := control.CreateTask(fixture.ctx, store.CreateTaskParams{Repository: repositoryURL, Prompt: "fix", IdempotencyKey: "request-1"})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	secrets, err := fixture.kube.CoreV1().Secrets(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
	if err != nil || len(secrets.Items) != 1 {
		t.Fatalf("task Secrets = %d, err %v", len(secrets.Items), err)
	}
	var manifest protocol.TaskManifest
	if err := json.Unmarshal(secrets.Items[0].Data["task.json"], &manifest); err != nil {
		t.Fatalf("decode task manifest: %v", err)
	}
	if manifest.TaskID != created.ID || manifest.CloneURL != repositoryURL || manifest.BaseBranch != "main" || manifest.TaskBranch != "work/"+created.ID+"-a1" {
		t.Fatalf("manifest identity = %#v", manifest)
	}
	if !reflect.DeepEqual(manifest.OpenCodeCommand, []string{"opencode", "run"}) || !reflect.DeepEqual(manifest.ValidationCommands, [][]string{{"go", "test", "./..."}}) || manifest.MaxFixAttempts != 2 {
		t.Fatalf("manifest execution = %#v", manifest)
	}

	jobs, err := fixture.kube.BatchV1().Jobs(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 1 {
		t.Fatalf("Jobs = %d, err %v", len(jobs.Items), err)
	}
	pod := jobs.Items[0].Spec.Template.Spec
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Fatal("worker service-account token is mounted")
	}
	if jobs.Items[0].Spec.ActiveDeadlineSeconds == nil || *jobs.Items[0].Spec.ActiveDeadlineSeconds != 420 || pod.PriorityClassName != "swe-worker" || pod.ServiceAccountName != "swe-worker" {
		t.Fatalf("Job scheduling = %#v", pod)
	}
}
