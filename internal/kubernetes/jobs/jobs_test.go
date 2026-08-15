package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8sresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestNameSanitizesDNSAndKeepsAttemptNamesBoundedAndDistinct(t *testing.T) {
	taskID := "My Task/With_Illegal.Chars-" + strings.Repeat("very-long-task-id-", 10)

	first := Name(taskID, 1)
	second := Name(taskID, 2)
	if first == second {
		t.Fatalf("Name(%q, 1) and Name(%q, 2) must be distinct", taskID, taskID)
	}
	if first != Name(taskID, 1) {
		t.Fatalf("Name(%q, 1) is not deterministic", taskID)
	}
	for _, name := range []string{first, second} {
		if len(name) > 63 {
			t.Errorf("name %q has length %d; want at most 63", name, len(name))
		}
		if !regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`).MatchString(name) {
			t.Errorf("name %q is not a DNS-compatible name", name)
		}
	}
}

func TestBuildJobAndTaskSecret(t *testing.T) {
	deadline := 11 * time.Minute
	config := Config{
		Namespace:          "simpleswe-workers",
		Image:              "registry.example/simpleswe-worker:2026-08-06",
		Resources:          resourceRequirements(),
		NodeSelector:       map[string]string{"workload": "swe"},
		Tolerations:        []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "swe", Effect: corev1.TaintEffectNoSchedule}},
		Affinity:           &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "arch", Operator: corev1.NodeSelectorOpIn, Values: []string{"amd64"}}}}}}}},
		PriorityClassName:  "simpleswe-worker",
		ServiceAccountName: "simpleswe-worker",
		ImagePullSecrets:   []string{"registry-pull"},
		Env: []corev1.EnvVar{
			{Name: "WORKER_MODE", Value: "task"},
			{Name: "API_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "api-secret"}, Key: "token"}}},
		},
		Deadline: deadline,
		// CredentialSecrets are references only; their long-lived values stay in Kubernetes.
		CredentialSecrets: []SecretMount{
			{Name: "git-credentials", MountPath: "/run/secrets/git"},
			{Name: "opencode-credentials", MountPath: "/run/secrets/opencode"},
			{Name: "bitbucket-credentials", MountPath: "/run/secrets/bitbucket"},
		},
	}
	manifest := TaskManifest{
		TaskID:                     "task-123",
		Repository:                 "https://bitbucket.example/team/repo",
		Prompt:                     "Fix the failing test",
		ForgeProvider:              "bitbucket",
		ForgeOwner:                 "team",
		ForgeRepository:            "repo",
		RequestedPullRequestTitle:  "Fix the failing test",
		ExistingPullRequestNumber:  42,
		ExistingPullRequestHeadSHA: "0123456789abcdef0123456789abcdef01234567",
	}
	attempt := Attempt{ID: "attempt-123", Number: 3}

	job, taskSecret, err := Build(config, manifest, attempt)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if job == nil || taskSecret == nil {
		t.Fatal("Build() returned a nil Job or task Secret")
	}
	if job.Namespace != config.Namespace || taskSecret.Namespace != config.Namespace {
		t.Fatalf("Build() namespace = %q/%q; want %q", job.Namespace, taskSecret.Namespace, config.Namespace)
	}
	if job.Name != Name(manifest.TaskID, attempt.Number) {
		t.Errorf("Job name = %q; want %q", job.Name, Name(manifest.TaskID, attempt.Number))
	}
	if taskSecret.Name == "" || taskSecret.Name == job.Name {
		t.Errorf("task Secret name = %q; want a task-specific name distinct from Job name", taskSecret.Name)
	}

	wantLabels := map[string]string{
		"app.kubernetes.io/name":      "simpleswe",
		"app.kubernetes.io/component": "worker",
		"simpleswe.dev/task-id":       manifest.TaskID,
		"simpleswe.dev/attempt":       strconv.Itoa(attempt.Number),
		"simpleswe.dev/attempt-id":    attempt.ID,
	}
	assertLabels(t, "Job", job.Labels, wantLabels)
	assertLabels(t, "Pod template", job.Spec.Template.Labels, wantLabels)
	assertLabels(t, "task Secret", taskSecret.Labels, wantLabels)

	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Errorf("backoffLimit = %v; want 0", job.Spec.BackoffLimit)
	}
	if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != int64(deadline/time.Second) {
		t.Errorf("activeDeadlineSeconds = %v; want %d", job.Spec.ActiveDeadlineSeconds, int64(deadline/time.Second))
	}
	if job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished != int32((24*time.Hour)/time.Second) {
		t.Errorf("ttlSecondsAfterFinished = %v; want %d", job.Spec.TTLSecondsAfterFinished, int32((24*time.Hour)/time.Second))
	}
	pod := job.Spec.Template.Spec
	if pod.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy = %q; want %q", pod.RestartPolicy, corev1.RestartPolicyNever)
	}
	if pod.TerminationGracePeriodSeconds == nil || *pod.TerminationGracePeriodSeconds != 30 {
		t.Errorf("terminationGracePeriodSeconds = %v; want 30", pod.TerminationGracePeriodSeconds)
	}
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Errorf("automountServiceAccountToken = %v; want false", pod.AutomountServiceAccountToken)
	}
	if pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot != nil {
		t.Errorf("pod runAsNonRoot = %#v; want unset", pod.SecurityContext)
	}
	if pod.SecurityContext == nil || pod.SecurityContext.SeccompProfile == nil || pod.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("pod seccompProfile = %#v; want RuntimeDefault", pod.SecurityContext)
	}
	if pod.NodeSelector == nil || !reflect.DeepEqual(pod.NodeSelector, config.NodeSelector) {
		t.Errorf("nodeSelector = %#v; want %#v", pod.NodeSelector, config.NodeSelector)
	}
	if !reflect.DeepEqual(pod.Tolerations, config.Tolerations) {
		t.Errorf("tolerations = %#v; want %#v", pod.Tolerations, config.Tolerations)
	}
	if !reflect.DeepEqual(pod.Affinity, config.Affinity) {
		t.Errorf("affinity = %#v; want %#v", pod.Affinity, config.Affinity)
	}
	if pod.PriorityClassName != config.PriorityClassName {
		t.Errorf("priorityClassName = %q; want %q", pod.PriorityClassName, config.PriorityClassName)
	}
	if pod.ServiceAccountName != config.ServiceAccountName {
		t.Errorf("serviceAccountName = %q; want %q", pod.ServiceAccountName, config.ServiceAccountName)
	}
	wantPullSecrets := []corev1.LocalObjectReference{{Name: "registry-pull"}}
	if !reflect.DeepEqual(pod.ImagePullSecrets, wantPullSecrets) {
		t.Errorf("imagePullSecrets = %#v; want %#v", pod.ImagePullSecrets, wantPullSecrets)
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("containers = %d; want exactly one worker container", len(pod.Containers))
	}
	worker := pod.Containers[0]
	if worker.Image != config.Image {
		t.Errorf("worker image = %q; want %q", worker.Image, config.Image)
	}
	if worker.ImagePullPolicy != corev1.PullAlways {
		t.Errorf("worker imagePullPolicy = %q; want %q", worker.ImagePullPolicy, corev1.PullAlways)
	}
	if !reflect.DeepEqual(worker.Resources, config.Resources) {
		t.Errorf("resources = %#v; want %#v", worker.Resources, config.Resources)
	}
	assertEnvValue(t, worker.Env, "WORKER_MODE", "task")
	assertEnvValue(t, worker.Env, "TMPDIR", workspaceMountPath)
	assertEnvValue(t, worker.Env, "HOME", workspaceMountPath)
	assertEnvValue(t, worker.Env, secretEnvNamesVariable, "API_TOKEN")
	if worker.SecurityContext == nil || worker.SecurityContext.AllowPrivilegeEscalation == nil || *worker.SecurityContext.AllowPrivilegeEscalation {
		t.Errorf("allowPrivilegeEscalation = %#v; want false", worker.SecurityContext)
	}
	if worker.SecurityContext == nil || worker.SecurityContext.ReadOnlyRootFilesystem == nil || !*worker.SecurityContext.ReadOnlyRootFilesystem {
		t.Errorf("readOnlyRootFilesystem = %#v; want true", worker.SecurityContext)
	}
	if worker.SecurityContext == nil || worker.SecurityContext.RunAsUser == nil || *worker.SecurityContext.RunAsUser != 0 || worker.SecurityContext.RunAsGroup == nil || *worker.SecurityContext.RunAsGroup != 0 {
		t.Errorf("runAsUser/runAsGroup = %#v; want 0/0", worker.SecurityContext)
	}
	if worker.SecurityContext == nil || worker.SecurityContext.Capabilities == nil || !reflect.DeepEqual(worker.SecurityContext.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Errorf("dropped capabilities = %#v; want ALL", worker.SecurityContext)
	}

	taskMount, taskVolume := findVolumeMount(t, worker, pod.Volumes, taskSecret.Name)
	if taskMount.MountPath != "/run/simpleswe/task.json" || taskMount.SubPath != "task.json" || !taskMount.ReadOnly {
		t.Errorf("task mount = %#v; want read-only task.json at /run/simpleswe/task.json", taskMount)
	}
	if taskVolume.Secret == nil || taskVolume.Secret.SecretName != taskSecret.Name {
		t.Errorf("task volume = %#v; want Secret %q", taskVolume, taskSecret.Name)
	}

	wantManifest, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("json.Marshal(manifest) error = %v", err)
	}
	if !bytes.Equal(taskSecret.Data["task.json"], wantManifest) {
		t.Errorf("task Secret task.json = %s; want %s", taskSecret.Data["task.json"], wantManifest)
	}
	if len(taskSecret.Data) != 1 {
		t.Errorf("task Secret data keys = %v; want only task.json", keys(taskSecret.Data))
	}

	for _, credential := range config.CredentialSecrets {
		mount, volume := findVolumeMount(t, worker, pod.Volumes, credential.Name)
		if mount.MountPath != credential.MountPath || !mount.ReadOnly {
			t.Errorf("credential %q mount = %#v; want read-only mount at %q", credential.Name, mount, credential.MountPath)
		}
		if volume.Secret == nil || volume.Secret.SecretName != credential.Name {
			t.Errorf("credential %q volume = %#v; want referenced Secret", credential.Name, volume)
		}
		if volume.Secret == nil || volume.Secret.DefaultMode == nil || *volume.Secret.DefaultMode != 0o400 {
			t.Errorf("credential %q defaultMode = %#v; want 0400", credential.Name, volume.Secret)
		}
	}
	workspaceMount, workspaceVolume := findNamedVolumeMount(t, worker, pod.Volumes, workspaceVolumeName)
	if workspaceMount.MountPath != workspaceMountPath || workspaceMount.ReadOnly || workspaceVolume.EmptyDir == nil {
		t.Errorf("workspace volume = %#v/%#v; want writable emptyDir at %s", workspaceMount, workspaceVolume, workspaceMountPath)
	}
	if len(pod.Volumes) != 2+len(config.CredentialSecrets) {
		t.Errorf("volumes = %d; want task Secret, workspace, plus %d credential Secrets", len(pod.Volumes), len(config.CredentialSecrets))
	}

}

func TestBuildRetryUsesDistinctJobsAndTaskSecrets(t *testing.T) {
	config := Config{Namespace: "simpleswe-workers", Image: "worker:latest", Deadline: time.Minute}
	manifest := TaskManifest{TaskID: "task-retry", Repository: "repo", Prompt: "retry"}

	firstJob, firstSecret, err := Build(config, manifest, Attempt{ID: "attempt-1", Number: 1})
	if err != nil {
		t.Fatalf("Build(first attempt) error = %v", err)
	}
	secondJob, secondSecret, err := Build(config, manifest, Attempt{ID: "attempt-2", Number: 2})
	if err != nil {
		t.Fatalf("Build(second attempt) error = %v", err)
	}
	if firstJob.Name == secondJob.Name {
		t.Errorf("retry Job names are equal: %q", firstJob.Name)
	}
	if firstSecret.Name == secondSecret.Name {
		t.Errorf("retry task Secret names are equal: %q", firstSecret.Name)
	}
	if firstJob.Labels["simpleswe.dev/attempt"] != "1" || secondJob.Labels["simpleswe.dev/attempt"] != "2" {
		t.Errorf("retry attempt labels = %q/%q; want 1/2", firstJob.Labels["simpleswe.dev/attempt"], secondJob.Labels["simpleswe.dev/attempt"])
	}
}

func TestCancelForegroundDeletesOnlyTheActiveJobInItsNamespace(t *testing.T) {
	const namespace = "simpleswe-workers"
	taskID := "task-cancel"
	active := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      Name(taskID, 2),
		Namespace: namespace,
		Labels:    map[string]string{"simpleswe.dev/task-id": taskID, "simpleswe.dev/attempt": "2"},
	}}
	previous := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      Name(taskID, 1),
		Namespace: namespace,
		Labels:    map[string]string{"simpleswe.dev/task-id": taskID, "simpleswe.dev/attempt": "1"},
	}}
	unrelated := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "other-task-a1", Namespace: namespace}}
	crossNamespace := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: active.Name, Namespace: "other-namespace"}}
	client := fake.NewSimpleClientset(active, previous, unrelated, crossNamespace)

	if err := Cancel(context.Background(), client, namespace, active.Name); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if _, err := client.BatchV1().Jobs(namespace).Get(context.Background(), active.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("active Job lookup error = %v; want NotFound", err)
	}
	for _, job := range []*batchv1.Job{previous, unrelated} {
		if _, err := client.BatchV1().Jobs(namespace).Get(context.Background(), job.Name, metav1.GetOptions{}); err != nil {
			t.Errorf("Job %s was deleted; Get() error = %v", job.Name, err)
		}
	}
	if _, err := client.BatchV1().Jobs(crossNamespace.Namespace).Get(context.Background(), crossNamespace.Name, metav1.GetOptions{}); err != nil {
		t.Errorf("same-named Job in another namespace was deleted; Get() error = %v", err)
	}

	var deleteAction k8stesting.DeleteAction
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			deleteAction, _ = action.(k8stesting.DeleteAction)
			break
		}
	}
	if deleteAction == nil {
		t.Fatal("Cancel() did not issue a Job delete action")
	}
	if deleteAction.GetNamespace() != namespace || deleteAction.GetName() != active.Name {
		t.Errorf("delete action = %s/%s; want %s/%s", deleteAction.GetNamespace(), deleteAction.GetName(), namespace, active.Name)
	}
	opts := deleteAction.GetDeleteOptions()
	if opts.PropagationPolicy == nil || *opts.PropagationPolicy != metav1.DeletePropagationForeground {
		t.Errorf("delete propagationPolicy = %v; want Foreground", opts.PropagationPolicy)
	}

}

func TestBuildRejectsInvalidConfigManifestAndAttempt(t *testing.T) {
	validConfig := func() Config {
		return Config{Namespace: "simpleswe-workers", Image: "worker:latest", Deadline: time.Minute}
	}
	validManifest := func() TaskManifest {
		return TaskManifest{TaskID: "task", Prompt: "prompt"}
	}
	validAttempt := Attempt{ID: "attempt", Number: 1}

	for _, test := range []struct {
		name   string
		config Config
		want   string
	}{
		{name: "invalid namespace", config: Config{Namespace: "INVALID_NAMESPACE", Image: "worker", Deadline: time.Minute}, want: "namespace is invalid"},
		{name: "missing image", config: Config{Namespace: "workers", Deadline: time.Minute}, want: "image is required"},
		{name: "image whitespace", config: Config{Namespace: "workers", Image: "worker image", Deadline: time.Minute}, want: "image contains whitespace"},
		{name: "image control", config: Config{Namespace: "workers", Image: "worker\nimage", Deadline: time.Minute}, want: "image contains whitespace"},
		{name: "short deadline", config: Config{Namespace: "workers", Image: "worker", Deadline: 500 * time.Millisecond}, want: "deadline must be at least one second"},
		{name: "reserved environment", config: Config{Namespace: "workers", Image: "worker", Deadline: time.Minute, Env: []corev1.EnvVar{{Name: "HOME"}}}, want: "environment variable HOME is reserved"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := Build(test.config, validManifest(), validAttempt)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Build() error = %v; want %q", err, test.want)
			}
		})
	}

	for _, test := range []struct {
		name     string
		manifest TaskManifest
		want     string
	}{
		{name: "missing task ID", manifest: TaskManifest{Prompt: "prompt"}, want: "task ID is required"},
		{name: "long task ID", manifest: TaskManifest{TaskID: strings.Repeat("a", 254), Prompt: "prompt"}, want: "task ID is invalid"},
		{name: "control task ID", manifest: TaskManifest{TaskID: "task\n", Prompt: "prompt"}, want: "task ID is invalid"},
		{name: "missing prompt", manifest: TaskManifest{TaskID: "task"}, want: "task prompt is required"},
		{name: "control prompt", manifest: TaskManifest{TaskID: "task", Prompt: "prompt\n"}, want: "task prompt contains control"},
		{name: "protocol validation", manifest: TaskManifest{TaskID: "task", Prompt: "prompt", ValidationCommand: []string{"go", "test"}}, want: "repository or clone_url is required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := Build(validConfig(), test.manifest, validAttempt)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Build() error = %v; want %q", err, test.want)
			}
		})
	}

	for _, test := range []struct {
		name    string
		attempt Attempt
		want    string
	}{
		{name: "missing attempt ID", attempt: Attempt{Number: 1}, want: "attempt ID is required"},
		{name: "long attempt ID", attempt: Attempt{ID: strings.Repeat("a", 254), Number: 1}, want: "attempt ID is invalid"},
		{name: "control attempt ID", attempt: Attempt{ID: "attempt\n", Number: 1}, want: "attempt ID is invalid"},
		{name: "non-positive attempt number", attempt: Attempt{ID: "attempt", Number: 0}, want: "attempt number must be positive"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := Build(validConfig(), validManifest(), test.attempt)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Build() error = %v; want %q", err, test.want)
			}
		})
	}
}

func TestBuildIncludesConfigMapAndEmptyDirMounts(t *testing.T) {
	sizeLimit := k8sresource.MustParse("1Gi")
	config := Config{
		Namespace: "workers",
		Image:     "worker:latest",
		Deadline:  time.Minute,
		CredentialSecrets: []SecretMount{
			{Name: "git-secret", MountPath: "/run/secrets/git"},
		},
		ConfigMaps: []ConfigMapMount{
			{Name: "worker-config", MountPath: "/etc/worker"},
		},
		EmptyDirs: []EmptyDirMount{
			{Name: "cache", MountPath: "/var/cache/worker", Medium: corev1.StorageMediumMemory, SizeLimit: &sizeLimit},
		},
	}
	job, _, err := Build(config, TaskManifest{TaskID: "task", Prompt: "prompt"}, Attempt{ID: "attempt", Number: 1})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	worker := job.Spec.Template.Spec.Containers[0]
	if len(job.Spec.Template.Spec.Volumes) != 5 || len(worker.VolumeMounts) != 5 {
		t.Fatalf("volumes/mounts = %d/%d; want 5/5", len(job.Spec.Template.Spec.Volumes), len(worker.VolumeMounts))
	}
	configMapMount, configMapVolume := findNamedVolumeMount(t, worker, job.Spec.Template.Spec.Volumes, "configmap-0")
	if configMapMount.MountPath != "/etc/worker" || !configMapMount.ReadOnly || configMapVolume.ConfigMap == nil || configMapVolume.ConfigMap.Name != "worker-config" {
		t.Errorf("ConfigMap mount/volume = %#v/%#v", configMapMount, configMapVolume)
	}
	emptyDirMount, emptyDirVolume := findNamedVolumeMount(t, worker, job.Spec.Template.Spec.Volumes, "emptydir-0")
	if emptyDirMount.MountPath != "/var/cache/worker" || emptyDirMount.ReadOnly || emptyDirVolume.EmptyDir == nil || emptyDirVolume.EmptyDir.Medium != corev1.StorageMediumMemory || emptyDirVolume.EmptyDir.SizeLimit == nil || !emptyDirVolume.EmptyDir.SizeLimit.Equal(sizeLimit) {
		t.Errorf("emptyDir mount/volume = %#v/%#v", emptyDirMount, emptyDirVolume)
	}
}

func TestValidateMountsRejectsInvalidAndOverlappingPaths(t *testing.T) {
	for _, test := range []struct {
		name string
		call func() error
		want string
	}{
		{name: "invalid Secret name", call: func() error {
			return validateMounts([]SecretMount{{Name: "BAD_NAME", MountPath: "/run/secrets/git"}}, nil, nil)
		}, want: "credential Secret 0 name is invalid"},
		{name: "invalid Secret path", call: func() error {
			return validateMounts([]SecretMount{{Name: "git-secret", MountPath: "relative"}}, nil, nil)
		}, want: "credential Secret \"git-secret\" mount path is invalid"},
		{name: "overlapping Secret path", call: func() error {
			return validateMounts([]SecretMount{{Name: "git-secret", MountPath: "/run/simpleswe/task.json/cache"}}, nil, nil)
		}, want: "overlaps"},
		{name: "invalid ConfigMap", call: func() error {
			return validateMounts(nil, []ConfigMapMount{{Name: "BAD_NAME", MountPath: "/etc/config"}}, nil)
		}, want: "ConfigMap mount 0 is invalid"},
		{name: "overlapping ConfigMap path", call: func() error {
			return validateMounts(nil, []ConfigMapMount{{Name: "worker-config", MountPath: "/workspace/cache"}}, nil)
		}, want: "ConfigMap \"worker-config\" mount path overlaps"},
		{name: "invalid emptyDir", call: func() error {
			return validateMounts(nil, nil, []EmptyDirMount{{Name: "BAD_NAME", MountPath: "/var/cache"}})
		}, want: "emptyDir mount 0 is invalid"},
		{name: "overlapping emptyDir path", call: func() error {
			return validateMounts(nil, nil, []EmptyDirMount{{Name: "cache", MountPath: "/workspace/cache"}})
		}, want: "emptyDir \"cache\" mount path overlaps"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateMounts() error = %v; want %q", err, test.want)
			}
		})
	}
	if err := validateMounts([]SecretMount{{Name: "git-secret", MountPath: "/run/secrets/git"}}, []ConfigMapMount{{Name: "worker-config", MountPath: "/etc/config"}}, []EmptyDirMount{{Name: "cache", MountPath: "/var/cache"}}); err != nil {
		t.Fatalf("validateMounts(valid mounts) error = %v", err)
	}
}

func TestCancelRejectsInvalidArgumentsAndReturnsDeleteError(t *testing.T) {
	client := fake.NewSimpleClientset()
	var nilContext context.Context
	if err := Cancel(nilContext, client, "workers", "job"); err == nil || !strings.Contains(err.Error(), "context is nil") {
		t.Fatalf("Cancel(nil context) error = %v", err)
	}
	if err := Cancel(context.Background(), nil, "workers", "job"); err == nil || !strings.Contains(err.Error(), "Kubernetes client is nil") {
		t.Fatalf("Cancel(nil client) error = %v", err)
	}
	if err := Cancel(context.Background(), client, "", "job"); err == nil || !strings.Contains(err.Error(), "namespace and Job name are required") {
		t.Fatalf("Cancel(empty namespace) error = %v", err)
	}
	if err := Cancel(context.Background(), client, "workers", ""); err == nil || !strings.Contains(err.Error(), "namespace and Job name are required") {
		t.Fatalf("Cancel(empty job) error = %v", err)
	}

	client.PrependReactor("delete", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("delete failed")
	})
	if err := Cancel(context.Background(), client, "workers", "job"); err == nil || !strings.Contains(err.Error(), "delete failed") {
		t.Fatalf("Cancel(delete failure) error = %v", err)
	}
}

func TestNameAndPathHelpersCoverFallbacks(t *testing.T) {
	if got := Name("", 1); got != "task-a1" {
		t.Fatalf("Name(empty task ID) = %q; want task-a1", got)
	}
	if got := labelValue(""); got != "unknown" {
		t.Fatalf("labelValue(empty) = %q; want unknown", got)
	}
	for _, test := range []struct {
		first  string
		second string
		want   bool
	}{
		{first: "/", second: "/var", want: true},
		{first: "/var/cache", second: "/var/cache", want: true},
		{first: "/var", second: "/var/cache", want: true},
		{first: "/var", second: "/etc", want: false},
	} {
		if got := pathsOverlap(test.first, test.second); got != test.want {
			t.Errorf("pathsOverlap(%q, %q) = %t; want %t", test.first, test.second, got, test.want)
		}
	}
}

func resourceRequirements() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: k8sresource.MustParse("500m"), corev1.ResourceMemory: k8sresource.MustParse("1Gi")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: k8sresource.MustParse("2"), corev1.ResourceMemory: k8sresource.MustParse("4Gi")},
	}
}

func assertLabels(t *testing.T, object string, got, want map[string]string) {
	t.Helper()
	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s label %q = %q; want %q", object, key, got[key], value)
		}
	}
}

func findVolumeMount(t *testing.T, container corev1.Container, volumes []corev1.Volume, secretName string) (corev1.VolumeMount, corev1.Volume) {
	t.Helper()
	for _, mount := range container.VolumeMounts {
		for _, volume := range volumes {
			if mount.Name == volume.Name && volume.Secret != nil && volume.Secret.SecretName == secretName {
				return mount, volume
			}
		}
	}
	t.Fatalf("no volume mount references Secret %q", secretName)
	return corev1.VolumeMount{}, corev1.Volume{}
}

func findNamedVolumeMount(t *testing.T, container corev1.Container, volumes []corev1.Volume, name string) (corev1.VolumeMount, corev1.Volume) {
	t.Helper()
	for _, mount := range container.VolumeMounts {
		if mount.Name != name {
			continue
		}
		for _, volume := range volumes {
			if volume.Name == name {
				return mount, volume
			}
		}
	}
	t.Fatalf("no volume mount named %q", name)
	return corev1.VolumeMount{}, corev1.Volume{}
}

func assertEnvValue(t *testing.T, env []corev1.EnvVar, name, want string) {
	t.Helper()
	for _, variable := range env {
		if variable.Name == name {
			if variable.Value != want {
				t.Errorf("environment %s = %q; want %q", name, variable.Value, want)
			}
			return
		}
	}
	t.Fatalf("environment %s is missing", name)
}

func keys(values map[string][]byte) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	return result
}
