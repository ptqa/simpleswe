package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/kubernetes/jobs"
	"github.com/simpleswe/simpleswe/internal/store"
)

func TestGitHubHTTPSWorkerMountsTokenSecretAndNeverEmbedsToken(t *testing.T) {
	const token = "ghp_test_token_must_stay_in_kubernetes_secret"
	fixture := newFixture(t)
	cfg := fixture.config
	repository := &cfg.Repositories[0]
	repository.CloneURL = "https://github.com/Acme/Widget.git"
	repository.Bitbucket = config.RepositoryBitbucketConfig{}
	repository.GitHub = config.RepositoryGitHubConfig{Owner: "Acme", Repository: "Widget", CredentialsSecret: "github-widget"}
	if _, err := fixture.kube.CoreV1().Secrets(workerNamespace).Create(fixture.ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "github-widget", Namespace: workerNamespace},
		Data:       map[string][]byte{"token": []byte(token)},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed GitHub Secret: %v", err)
	}
	control, err := New(fixture.store, fixture.kube, cfg, fixture.pullRequests)
	if err != nil {
		t.Fatalf("recreate controller: %v", err)
	}

	created, err := control.CreateTask(context.Background(), store.CreateTaskParams{Repository: repository.CloneURL, Prompt: "push a GitHub branch"})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobs.Name(created.ID, 1), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get worker Job: %v", err)
	}
	pod := job.Spec.Template.Spec
	githubMounts := 0
	for _, volume := range pod.Volumes {
		if volume.Secret == nil || volume.Secret.SecretName != "github-widget" {
			continue
		}
		githubMounts++
		mounted := false
		for _, mount := range pod.Containers[0].VolumeMounts {
			if mount.Name != volume.Name {
				continue
			}
			mounted = true
			if mount.MountPath != "/run/secrets/github" || !mount.ReadOnly {
				t.Fatalf("GitHub Secret mount = %#v; want read-only /run/secrets/github", mount)
			}
		}
		if !mounted {
			t.Fatalf("GitHub Secret volume %q has no mount", volume.Name)
		}
	}
	if githubMounts != 1 {
		t.Fatalf("GitHub Secret mounts = %d; want 1", githubMounts)
	}

	assertGitHubEnv(t, pod.Containers[0].Env, "GIT_TERMINAL_PROMPT", "0")
	assertGitHubEnv(t, pod.Containers[0].Env, "GIT_CONFIG_COUNT", "3")
	assertGitHubEnv(t, pod.Containers[0].Env, "GIT_CONFIG_KEY_0", "credential.useHttpPath")
	assertGitHubEnv(t, pod.Containers[0].Env, "GIT_CONFIG_KEY_1", "credential."+repository.CloneURL+".username")
	assertGitHubEnv(t, pod.Containers[0].Env, "GIT_CONFIG_VALUE_1", "x-access-token")
	assertGitHubEnv(t, pod.Containers[0].Env, "GIT_CONFIG_KEY_2", "credential."+repository.CloneURL+".helper")
	assertGitHubEnv(t, pod.Containers[0].Env, "GIT_CONFIG_VALUE_2", `!f() { if test "$1" = get; then printf 'password=%s\n' "$(cat /run/secrets/github/token)"; fi; }; f`)

	jobData, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal Job: %v", err)
	}
	if bytes.Contains(jobData, []byte(token)) {
		t.Fatal("GitHub token was embedded in the Job")
	}
	for _, secret := range listTaskSecrets(t, fixture, created.ID) {
		if bytes.Contains(secret.Data["task.json"], []byte(token)) {
			t.Fatal("GitHub token was embedded in the task manifest")
		}
	}
}

func TestGitHubHTTPSWorkerUsesOnlySeparateWorkerCredentials(t *testing.T) {
	fixture := newFixture(t)
	cfg := fixture.config
	repository := &cfg.Repositories[0]
	repository.CloneURL = "https://github.com/Acme/Widget.git"
	repository.Bitbucket = config.RepositoryBitbucketConfig{}
	repository.GitHub = config.RepositoryGitHubConfig{Owner: "Acme", Repository: "Widget", CredentialsSecret: "github-controller"}
	repository.Credentials.SecretName = "github-worker"
	control, err := New(fixture.store, fixture.kube, cfg, fixture.pullRequests)
	if err != nil {
		t.Fatalf("recreate controller: %v", err)
	}
	created, err := control.CreateTask(fixture.ctx, store.CreateTaskParams{Repository: repository.CloneURL, Prompt: "push with worker token"})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobs.Name(created.ID, 1), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get worker Job: %v", err)
	}
	workerMounts, workerPathMounts := 0, 0
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Secret == nil {
			continue
		}
		if volume.Secret.SecretName == "github-controller" {
			t.Fatal("controller GitHub PR token entered the worker Job")
		}
		if volume.Secret.SecretName == "github-worker" {
			workerMounts++
			for _, mount := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
				if mount.Name == volume.Name {
					workerPathMounts++
					if mount.MountPath != "/run/secrets/repository" || !mount.ReadOnly {
						t.Fatalf("worker GitHub Secret mount = %#v; want read-only /run/secrets/repository", mount)
					}
				}
			}
		}
	}
	if workerMounts != 1 || workerPathMounts != 1 {
		t.Fatalf("worker GitHub credential volume/path mounts = %d/%d; want 1/1", workerMounts, workerPathMounts)
	}
	assertGitHubEnv(t, job.Spec.Template.Spec.Containers[0].Env, "GIT_CONFIG_VALUE_2", `!f() { if test "$1" = get; then printf 'password=%s\n' "$(cat /run/secrets/repository/token)"; fi; }; f`)
}

func TestGitHubCredentialHelperIsScopedToExactCloneURL(t *testing.T) {
	fixture := newFixture(t)
	repository := fixture.config.Repositories[0]
	repository.CloneURL = "https://github.com/Acme/Widget.git"
	repository.Bitbucket = config.RepositoryBitbucketConfig{}
	repository.GitHub = config.RepositoryGitHubConfig{Owner: "Acme", Repository: "Widget", CredentialsSecret: "github-widget"}
	jobConfig, err := fixture.controller.(*Controller).jobConfig(repository)
	if err != nil {
		t.Fatalf("jobConfig: %v", err)
	}
	env := make(map[string]string, len(jobConfig.Env))
	for _, variable := range jobConfig.Env {
		env[variable.Name] = variable.Value
	}
	if env["GIT_CONFIG_KEY_2"] == "credential.helper" || env["GIT_CONFIG_KEY_1"] != "credential."+repository.CloneURL+".username" || env["GIT_CONFIG_KEY_2"] != "credential."+repository.CloneURL+".helper" {
		t.Fatalf("Git credential config is not URL-scoped: %#v", env)
	}

	root := t.TempDir()
	tokenPath := filepath.Join(root, "token")
	if err := os.WriteFile(tokenPath, []byte("worker-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	env["GIT_CONFIG_VALUE_2"] = strings.Replace(env["GIT_CONFIG_VALUE_2"], "/run/secrets/github/token", tokenPath, 1)
	runCredentialFill := func(path string) ([]byte, error) {
		command := exec.Command("git", "credential", "fill")
		command.Stdin = strings.NewReader("protocol=https\nhost=github.com\npath=" + path + "\n\n")
		command.Env = append(os.Environ(), "HOME="+root, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
		for name, value := range env {
			command.Env = append(command.Env, name+"="+value)
		}
		return command.Output()
	}
	output, err := runCredentialFill("Acme/Widget.git")
	if err != nil {
		t.Fatalf("git credential fill for configured URL: %v", err)
	}
	if text := string(output); !strings.Contains(text, "username=x-access-token") || !strings.Contains(text, "password=worker-token") {
		t.Fatalf("configured credential output = %q", text)
	}
	unrelated, err := runCredentialFill("Acme/Other.git")
	if err == nil {
		t.Fatal("git credential fill unexpectedly found credentials for another repository")
	}
	if strings.Contains(string(unrelated), "worker-token") || strings.Contains(string(unrelated), "x-access-token") {
		t.Fatalf("unrelated credential output leaked configured credentials: %q", unrelated)
	}
}

func TestGitHubWorkerDoesNotDuplicateGenericCredentialMountAndRetainsSSH(t *testing.T) {
	fixture := newFixture(t)
	control := fixture.controller.(*Controller)
	repository := fixture.config.Repositories[0]
	repository.CloneURL = "ssh://git@github.com/Acme/Widget.git"
	repository.Bitbucket = config.RepositoryBitbucketConfig{}
	repository.Credentials.SecretName = "github-widget"
	repository.GitHub = config.RepositoryGitHubConfig{Owner: "Acme", Repository: "Widget", CredentialsSecret: "github-widget"}
	repository.Git.SSHSecret = "git-ssh"

	jobConfig, err := control.jobConfig(repository)
	if err != nil {
		t.Fatalf("jobConfig: %v", err)
	}
	githubPath, genericPath, sshPath := 0, 0, 0
	for _, mount := range jobConfig.CredentialSecrets {
		switch {
		case mount.Name == "github-widget" && mount.MountPath == "/run/secrets/github":
			githubPath++
		case mount.Name == "github-widget" && mount.MountPath == "/run/secrets/repository":
			genericPath++
		case mount.Name == "git-ssh" && mount.MountPath == "/run/secrets/git":
			sshPath++
		}
	}
	if githubPath != 0 || genericPath != 1 {
		t.Fatalf("GitHub generic credential mounts = github:%d repository:%d; want 0/1", githubPath, genericPath)
	}
	if sshPath != 1 {
		t.Fatalf("SSH credential mounts = %d; want 1", sshPath)
	}
	assertGitHubEnv(t, jobConfig.Env, "GIT_SSH_COMMAND", "ssh -i /run/secrets/git/ssh-privatekey -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new")
}

func assertGitHubEnv(t *testing.T, env []corev1.EnvVar, name, want string) {
	t.Helper()
	for _, variable := range env {
		if variable.Name == name {
			if variable.Value != want {
				t.Fatalf("environment %s = %q; want %q", name, variable.Value, want)
			}
			return
		}
	}
	t.Fatalf("environment %s is missing", name)
}

func listTaskSecrets(t *testing.T, fixture *fixture, taskID string) []*corev1.Secret {
	t.Helper()
	secrets, err := fixture.kube.CoreV1().Secrets(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list task Secrets: %v", err)
	}
	var result []*corev1.Secret
	for i := range secrets.Items {
		if strings.Contains(string(secrets.Items[i].Data["task.json"]), taskID) {
			result = append(result, &secrets.Items[i])
		}
	}
	return result
}
