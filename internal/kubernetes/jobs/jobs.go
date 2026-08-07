package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"

	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

const (
	workerCommand          = "simpleswe"
	workerSubcommand       = "worker"
	taskMountPath          = "/run/simpleswe/task.json"
	managedBy              = "simpleswe"
	workerContainerName    = "worker"
	workspaceVolumeName    = "workspace"
	workspaceMountPath     = "/tmp/workspace"
	secretEnvNamesVariable = protocol.SecretEnvNamesVariable
)

// Config contains the Kubernetes settings needed to create a worker Job.
type Config struct {
	Namespace          string
	Image              string
	Resources          corev1.ResourceRequirements
	NodeSelector       map[string]string
	Tolerations        []corev1.Toleration
	Affinity           *corev1.Affinity
	PriorityClassName  string
	ServiceAccountName string
	ImagePullSecrets   []string
	Env                []corev1.EnvVar
	Deadline           time.Duration
	CredentialSecrets  []SecretMount
	ConfigMaps         []ConfigMapMount
	EmptyDirs          []EmptyDirMount
}

type ConfigMapMount struct {
	Name      string
	MountPath string
}

type EmptyDirMount struct {
	Name      string
	MountPath string
	Medium    corev1.StorageMedium
	SizeLimit *resource.Quantity
}

// SecretMount names an existing Secret and the read-only path where it is
// made available to the worker.
type SecretMount struct {
	Name      string
	MountPath string
}

// TaskManifest is the task data passed to a worker through task.json.
type TaskManifest = protocol.TaskManifest

// Attempt identifies one immutable execution attempt for a task.
type Attempt struct {
	ID     string
	Number int
}

// Name returns a deterministic DNS-compatible Job name for one task attempt.
func Name(taskID string, attempt int) string {
	attemptText := strconv.Itoa(attempt)
	normalizedTaskID := strings.ToLower(taskID)
	base := sanitizeName(normalizedTaskID) + "-a" + attemptText
	if base == "-a"+attemptText {
		base = "task-a" + attemptText
	}
	if sanitizeName(normalizedTaskID) != taskID {
		base += "-" + shortHash(taskID+"\x00"+attemptText)
	}
	return boundedName(base, taskID+"\x00"+attemptText)
}

// Build returns the Job and its task-specific Secret. Credential Secrets are
// referenced by name only; their data never enters either object.
func Build(config Config, manifest TaskManifest, attempt Attempt) (*batchv1.Job, *corev1.Secret, error) {
	if err := validateConfig(config); err != nil {
		return nil, nil, err
	}
	if err := validateManifest(manifest); err != nil {
		return nil, nil, err
	}
	if err := validateAttempt(attempt); err != nil {
		return nil, nil, err
	}

	if err := validateMounts(config.CredentialSecrets, config.ConfigMaps, config.EmptyDirs); err != nil {
		return nil, nil, err
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal task manifest: %w", err)
	}

	jobName := Name(manifest.TaskID, attempt.Number)
	taskSecretName := boundedName(jobName+"-task", jobName+"-task")
	labels := buildLabels(manifest, attempt)

	volumes := []corev1.Volume{
		{
			Name: "task-secret",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: taskSecretName,
				Items:      []corev1.KeyToPath{{Key: "task.json", Path: "task.json"}},
			}},
		},
		{Name: workspaceVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	volumeMounts := []corev1.VolumeMount{
		{Name: "task-secret", MountPath: taskMountPath, SubPath: "task.json", ReadOnly: true},
		{Name: workspaceVolumeName, MountPath: workspaceMountPath},
	}
	for i, credential := range config.CredentialSecrets {
		volumeName := "credential-" + strconv.Itoa(i)
		volumes = append(volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: credential.Name,
			}},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: credential.MountPath,
			ReadOnly:  true,
		})
	}
	for i, mounted := range config.ConfigMaps {
		volumeName := "configmap-" + strconv.Itoa(i)
		volumes = append(volumes, corev1.Volume{Name: volumeName, VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: mounted.Name}}}})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: volumeName, MountPath: mounted.MountPath, ReadOnly: true})
	}
	for i, mounted := range config.EmptyDirs {
		volumeName := "emptydir-" + strconv.Itoa(i)
		volumes = append(volumes, corev1.Volume{Name: volumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: mounted.Medium, SizeLimit: mounted.SizeLimit}}})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: volumeName, MountPath: mounted.MountPath})
	}

	backoffLimit := int32(0)
	activeDeadline := int64(config.Deadline / time.Second)
	terminationGracePeriod := int64(30)
	automountServiceAccountToken := false
	allowPrivilegeEscalation := false
	readOnlyRootFilesystem := true
	env := append([]corev1.EnvVar(nil), config.Env...)
	env = append(env,
		corev1.EnvVar{Name: "TMPDIR", Value: workspaceMountPath},
		corev1.EnvVar{Name: "HOME", Value: workspaceMountPath},
	)
	var secretEnvironmentNames []string
	for _, variable := range config.Env {
		if variable.ValueFrom != nil && variable.ValueFrom.SecretKeyRef != nil {
			secretEnvironmentNames = append(secretEnvironmentNames, variable.Name)
		}
	}
	if len(secretEnvironmentNames) > 0 {
		env = append(env, corev1.EnvVar{Name: secretEnvNamesVariable, Value: strings.Join(secretEnvironmentNames, ",")})
	}
	worker := corev1.Container{
		Name:         workerContainerName,
		Image:        config.Image,
		Command:      []string{workerCommand, workerSubcommand},
		Env:          env,
		Resources:    config.Resources,
		VolumeMounts: volumeMounts,
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: &allowPrivilegeEscalation,
			ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}
	podSpec := corev1.PodSpec{
		Containers:                    []corev1.Container{worker},
		RestartPolicy:                 corev1.RestartPolicyNever,
		TerminationGracePeriodSeconds: &terminationGracePeriod,
		AutomountServiceAccountToken:  &automountServiceAccountToken,
		NodeSelector:                  copyStringMap(config.NodeSelector),
		Tolerations:                   append([]corev1.Toleration(nil), config.Tolerations...),
		Affinity:                      config.Affinity.DeepCopy(),
		PriorityClassName:             config.PriorityClassName,
		ServiceAccountName:            config.ServiceAccountName,
		ImagePullSecrets:              imagePullSecrets(config.ImagePullSecrets),
		Volumes:                       volumes,
		SecurityContext: &corev1.PodSecurityContext{
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: config.Namespace,
			Labels:    copyStringMap(labels),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          &backoffLimit,
			ActiveDeadlineSeconds: &activeDeadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: copyStringMap(labels)},
				Spec:       podSpec,
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskSecretName,
			Namespace: config.Namespace,
			Labels:    copyStringMap(labels),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"task.json": manifestData},
	}
	return job, secret, nil
}

// Cancel deletes only the supplied namespaced Job and waits for foreground
// propagation to remove its owned resources.
func Cancel(ctx context.Context, client kubernetes.Interface, namespace, jobName string) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	if client == nil {
		return fmt.Errorf("Kubernetes client is nil")
	}
	if namespace == "" || jobName == "" {
		return fmt.Errorf("namespace and Job name are required")
	}
	policy := metav1.DeletePropagationForeground
	return client.BatchV1().Jobs(namespace).Delete(ctx, jobName, metav1.DeleteOptions{PropagationPolicy: &policy})
}

func validateConfig(config Config) error {
	if errors := validation.IsDNS1123Label(config.Namespace); len(errors) > 0 {
		return fmt.Errorf("namespace is invalid: %s", errors[0])
	}
	if strings.TrimSpace(config.Image) == "" {
		return fmt.Errorf("image is required")
	}
	if strings.IndexFunc(config.Image, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return fmt.Errorf("image contains whitespace or control characters")
	}
	if config.Deadline < time.Second {
		return fmt.Errorf("deadline must be at least one second")
	}
	for _, variable := range config.Env {
		if variable.Name == "HOME" || variable.Name == "TMPDIR" || variable.Name == secretEnvNamesVariable {
			return fmt.Errorf("environment variable %s is reserved", variable.Name)
		}
	}
	return nil
}

func validateManifest(manifest TaskManifest) error {
	if strings.TrimSpace(manifest.TaskID) == "" {
		return fmt.Errorf("task ID is required")
	}
	if len(manifest.TaskID) > 253 || strings.IndexFunc(manifest.TaskID, unicode.IsControl) >= 0 {
		return fmt.Errorf("task ID is invalid")
	}
	if strings.TrimSpace(manifest.Prompt) == "" {
		return fmt.Errorf("task prompt is required")
	}
	if strings.IndexFunc(manifest.Prompt, unicode.IsControl) >= 0 {
		return fmt.Errorf("task prompt contains control characters")
	}
	if len(manifest.OpenCodeCommand) > 0 || len(manifest.ValidationCommand) > 0 || len(manifest.ValidationCommands) > 0 {
		return protocol.ValidateManifest(manifest)
	}
	return nil
}

func validateAttempt(attempt Attempt) error {
	if strings.TrimSpace(attempt.ID) == "" {
		return fmt.Errorf("attempt ID is required")
	}
	if len(attempt.ID) > 253 || strings.IndexFunc(attempt.ID, unicode.IsControl) >= 0 {
		return fmt.Errorf("attempt ID is invalid")
	}
	if attempt.Number <= 0 {
		return fmt.Errorf("attempt number must be positive")
	}
	return nil
}

func validateMounts(mounts []SecretMount, configMaps []ConfigMapMount, emptyDirs []EmptyDirMount) error {
	paths := []string{taskMountPath, workspaceMountPath}
	for i, mount := range mounts {
		if len(validation.IsDNS1123Subdomain(mount.Name)) > 0 {
			return fmt.Errorf("credential Secret %d name is invalid", i)
		}
		if !validMountPath(mount.MountPath) {
			return fmt.Errorf("credential Secret %q mount path is invalid", mount.Name)
		}
		mountPath := path.Clean(mount.MountPath)
		for _, existing := range paths {
			if pathsOverlap(existing, mountPath) {
				return fmt.Errorf("credential Secret %q mount path overlaps %q", mount.Name, existing)
			}
		}
		paths = append(paths, mountPath)
	}
	for i, mount := range configMaps {
		if len(validation.IsDNS1123Subdomain(mount.Name)) > 0 || !validMountPath(mount.MountPath) {
			return fmt.Errorf("ConfigMap mount %d is invalid", i)
		}
		for _, existing := range paths {
			if pathsOverlap(existing, mount.MountPath) {
				return fmt.Errorf("ConfigMap %q mount path overlaps %q", mount.Name, existing)
			}
		}
		paths = append(paths, mount.MountPath)
	}
	for i, mount := range emptyDirs {
		if len(validation.IsDNS1123Label(mount.Name)) > 0 || !validMountPath(mount.MountPath) {
			return fmt.Errorf("emptyDir mount %d is invalid", i)
		}
		for _, existing := range paths {
			if pathsOverlap(existing, mount.MountPath) {
				return fmt.Errorf("emptyDir %q mount path overlaps %q", mount.Name, existing)
			}
		}
		paths = append(paths, mount.MountPath)
	}
	return nil
}

func validMountPath(mountPath string) bool {
	return mountPath != "" && path.IsAbs(mountPath) && path.Clean(mountPath) == mountPath
}

func pathsOverlap(first, second string) bool {
	if first == "/" || second == "/" || first == second {
		return true
	}
	return strings.HasPrefix(first, second+"/") || strings.HasPrefix(second, first+"/")
}

func buildLabels(manifest TaskManifest, attempt Attempt) map[string]string {
	labels := map[string]string{
		"app.kubernetes.io/name":       "simpleswe",
		"app.kubernetes.io/component":  "worker",
		"app.kubernetes.io/managed-by": managedBy,
		"simpleswe.dev/task-id":        labelValue(manifest.TaskID),
		"simpleswe.dev/attempt":        strconv.Itoa(attempt.Number),
		"simpleswe.dev/attempt-id":     labelValue(attempt.ID),
	}
	if strings.TrimSpace(manifest.Repository) != "" {
		labels["simpleswe.dev/repository"] = labelValue(manifest.Repository)
	}
	return labels
}

func labelValue(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	safe := sanitizeName(value)
	if safe == "" {
		return "unknown"
	}
	if safe == value && len(safe) <= 63 {
		return safe
	}
	return boundedName(safe, value)
}

func boundedName(value, hashInput string) string {
	value = sanitizeName(value)
	if value == "" {
		value = "task"
	}
	if len(value) <= 63 {
		return value
	}
	suffix := "-" + shortHash(hashInput)
	prefix := strings.TrimRight(value[:63-len(suffix)], "-")
	if prefix == "" {
		prefix = "task"
	}
	return prefix + suffix
}

func sanitizeName(value string) string {
	var result strings.Builder
	result.Grow(len(value))
	dash := false
	for _, r := range strings.ToLower(value) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			result.WriteRune(r)
			dash = false
			continue
		}
		if !dash {
			result.WriteByte('-')
			dash = true
		}
	}
	return strings.Trim(result.String(), "-")
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:16]
}

func copyStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func imagePullSecrets(names []string) []corev1.LocalObjectReference {
	if names == nil {
		return nil
	}
	secrets := make([]corev1.LocalObjectReference, len(names))
	for i, name := range names {
		secrets[i] = corev1.LocalObjectReference{Name: name}
	}
	return secrets
}
