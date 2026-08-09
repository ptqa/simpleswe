package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultListenAddress        = ":8080"
	defaultWebhookListenAddress = ":8081"
	defaultNamespace            = "simpleswe"
	defaultWorkerCommand        = "opencode"
	defaultBranchPrefix         = "simpleswe/"
	defaultDeadline             = 30 * time.Minute
	defaultReviewDebounce       = 30 * time.Minute
	defaultMaxFixAttempts       = 3
)

// Config is the controller configuration file.
type Config struct {
	Controller   ControllerConfig  `yaml:"controller"`
	Worker       WorkerConfig      `yaml:"worker"`
	Bitbucket    BitbucketConfig   `yaml:"bitbucket,omitempty"`
	GitHub       GitHubConfig      `yaml:"github,omitempty"`
	Repositories RepositoryConfigs `yaml:"repositories"`
}

// SecretSource identifies a mounted file or environment variable. It never
// contains the secret value itself.
type SecretSource struct {
	File string `yaml:"file,omitempty"`
	Env  string `yaml:"env,omitempty"`
}

type BitbucketConfig struct {
	BaseURL       string       `yaml:"base_url,omitempty"`
	WebhookSecret SecretSource `yaml:"webhook_secret,omitempty"`
}

type GitHubConfig struct {
	BaseURL       string       `yaml:"base_url,omitempty"`
	WebhookSecret SecretSource `yaml:"webhook_secret,omitempty"`
}

type ControllerConfig struct {
	ListenAddress        string        `yaml:"listen_address"`
	WebhookListenAddress string        `yaml:"webhook_listen_address"`
	Namespace            string        `yaml:"namespace"`
	Deadline             time.Duration `yaml:"deadline"`
	ReviewDebounce       time.Duration `yaml:"review_debounce"`
	MaxFixAttempts       int           `yaml:"max_fix_attempts"`
	maxFixAttemptsSet    bool
}

type WorkerConfig struct {
	Image              string               `yaml:"image,omitempty"`
	Command            string               `yaml:"command,omitempty"`
	BranchPrefix       string               `yaml:"branch_prefix,omitempty"`
	Deadline           Duration             `yaml:"deadline,omitempty"`
	Resources          ResourceRequirements `yaml:"resources,omitempty"`
	Scheduling         Scheduling           `yaml:"scheduling,omitempty"`
	Mounts             []Mount              `yaml:"mounts,omitempty"`
	NodeSelector       map[string]string    `yaml:"node_selector,omitempty"`
	Tolerations        []Toleration         `yaml:"tolerations,omitempty"`
	Affinity           map[string]any       `yaml:"affinity,omitempty"`
	PriorityClassName  string               `yaml:"priority_class_name,omitempty"`
	ServiceAccountName string               `yaml:"service_account_name,omitempty"`
	ImagePullSecrets   []string             `yaml:"image_pull_secrets,omitempty"`
	Env                []Env                `yaml:"env,omitempty"`
	MountedSecrets     []NamedMount         `yaml:"mounted_secrets,omitempty"`
	MountedConfigMaps  []NamedMount         `yaml:"mounted_config_maps,omitempty"`
}

type RepositoryConfigs []RepositoryConfig

type RepositoryConfig struct {
	Name          string                    `yaml:"name,omitempty"`
	CloneURL      string                    `yaml:"clone_url"`
	DefaultBranch string                    `yaml:"default_branch"`
	Credentials   Credentials               `yaml:"credentials,omitempty"`
	Worker        WorkerConfig              `yaml:"worker"`
	Git           GitConfig                 `yaml:"git,omitempty"`
	OpenCode      OpenCodeConfig            `yaml:"opencode,omitempty"`
	Validation    ValidationConfig          `yaml:"validation,omitempty"`
	Bitbucket     RepositoryBitbucketConfig `yaml:"bitbucket,omitempty"`
	GitHub        RepositoryGitHubConfig    `yaml:"github,omitempty"`

	credentialsSet bool
	bitbucketSet   bool
	githubSet      bool
}

type GitConfig struct {
	BranchPrefix string `yaml:"branch_prefix,omitempty"`
	SSHSecret    string `yaml:"ssh_secret,omitempty"`
}

type OpenCodeConfig struct {
	Command      []string `yaml:"command,omitempty"`
	ConfigSecret string   `yaml:"config_secret,omitempty"`
}

type ValidationConfig struct {
	Commands       [][]string `yaml:"commands,omitempty"`
	MaxFixAttempts *int       `yaml:"max_fix_attempts,omitempty"`
	MaxFixes       *int       `yaml:"max_fixes,omitempty"`
}

type RepositoryBitbucketConfig struct {
	Workspace         string `yaml:"workspace,omitempty"`
	Repository        string `yaml:"repository,omitempty"`
	CredentialsSecret string `yaml:"credentials_secret_name,omitempty"`
}

type RepositoryGitHubConfig struct {
	Owner             string `yaml:"owner,omitempty"`
	Repository        string `yaml:"repository,omitempty"`
	CredentialsSecret string `yaml:"credentials_secret_name,omitempty"`
}

type Credentials struct {
	SecretName string `yaml:"secret_name"`
}

type ResourceRequirements struct {
	Requests ResourceList `yaml:"requests,omitempty"`
	Limits   ResourceList `yaml:"limits,omitempty"`
}

type Scheduling struct {
	NodeSelector       map[string]string `yaml:"node_selector,omitempty"`
	Tolerations        []Toleration      `yaml:"tolerations,omitempty"`
	Affinity           map[string]any    `yaml:"affinity,omitempty"`
	PriorityClassName  string            `yaml:"priority_class_name,omitempty"`
	ServiceAccountName string            `yaml:"service_account_name,omitempty"`
	ImagePullSecrets   []string          `yaml:"image_pull_secrets,omitempty"`
}

type Duration time.Duration

func (d Duration) Value() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return fmt.Errorf("duration must be a string")
	}
	value, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("duration must be valid: %w", err)
	}
	*d = Duration(value)
	return nil
}

type Toleration struct {
	Key               string `yaml:"key,omitempty"`
	Operator          string `yaml:"operator,omitempty"`
	Value             string `yaml:"value,omitempty"`
	Effect            string `yaml:"effect,omitempty"`
	TolerationSeconds *int64 `yaml:"toleration_seconds,omitempty"`
}

type Mount struct {
	Name      string          `yaml:"name"`
	MountPath string          `yaml:"mount_path"`
	EmptyDir  *EmptyDir       `yaml:"empty_dir,omitempty"`
	Secret    *SecretMount    `yaml:"secret,omitempty"`
	ConfigMap *ConfigMapMount `yaml:"config_map,omitempty"`
}

type NamedMount struct {
	Name      string `yaml:"name"`
	MountPath string `yaml:"mount_path"`
}

type Env struct {
	Name      string  `yaml:"name"`
	Value     string  `yaml:"value,omitempty"`
	Secret    *KeyRef `yaml:"secret,omitempty"`
	ConfigMap *KeyRef `yaml:"config_map,omitempty"`
}

type KeyRef struct {
	Name string `yaml:"name"`
	Key  string `yaml:"key"`
}

type EmptyDir struct {
	Medium    string    `yaml:"medium,omitempty"`
	SizeLimit *Quantity `yaml:"size_limit,omitempty"`
}

type ResourceList map[string]Quantity

// Quantity retains the source Kubernetes quantity spelling instead of
// converting it through a floating-point representation.
type Quantity string

func (q Quantity) String() string { return string(q) }

func (q Quantity) Sign() int {
	value := strings.TrimSpace(string(q))
	if strings.HasPrefix(value, "-") {
		return -1
	}
	if strings.TrimPrefix(value, "+") == "0" || strings.TrimPrefix(value, "+") == "0.0" {
		return 0
	}
	return 1
}

func (q *Quantity) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || (node.Tag != "!!str" && node.Tag != "!!int" && node.Tag != "!!float") {
		return fmt.Errorf("resource quantity must be a scalar")
	}
	if !validQuantity(node.Value) {
		return fmt.Errorf("invalid resource quantity")
	}
	*q = Quantity(node.Value)
	return nil
}

var quantityPattern = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:m|[kMGTPE]|[KMGTPE]i|[eE][+-]?[0-9]+)?$`)

func validQuantity(value string) bool {
	return quantityPattern.MatchString(value)
}

type SecretMount struct {
	SecretName string `yaml:"secret_name"`
	ReadOnly   bool   `yaml:"read_only,omitempty"`
}

type ConfigMapMount struct {
	Name     string `yaml:"name"`
	ReadOnly bool   `yaml:"read_only,omitempty"`
}

// Load decodes one strict YAML document and validates it after applying defaults.
func Load(r io.Reader) (Config, error) {
	var cfg Config
	if r == nil {
		return cfg, fmt.Errorf("config reader is nil")
	}

	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return cfg, fmt.Errorf("config is empty")
		}
		return cfg, fmt.Errorf("decode config: %w", err)
	}

	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return cfg, fmt.Errorf("config contains more than one YAML document")
		}
		return cfg, fmt.Errorf("decode config document: %w", err)
	}

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Controller.ListenAddress == "" {
		c.Controller.ListenAddress = defaultListenAddress
	}
	if c.Controller.WebhookListenAddress == "" {
		c.Controller.WebhookListenAddress = defaultWebhookListenAddress
	}
	if c.Controller.Namespace == "" {
		c.Controller.Namespace = defaultNamespace
	}
	if c.Controller.Deadline == 0 {
		c.Controller.Deadline = defaultDeadline
	}
	if c.Controller.ReviewDebounce == 0 {
		c.Controller.ReviewDebounce = defaultReviewDebounce
	}
	if !c.Controller.maxFixAttemptsSet && c.Controller.MaxFixAttempts == 0 {
		c.Controller.MaxFixAttempts = defaultMaxFixAttempts
	}
	if c.Worker.Command == "" {
		c.Worker.Command = defaultWorkerCommand
	}
	if c.Worker.BranchPrefix == "" {
		c.Worker.BranchPrefix = defaultBranchPrefix
	}
	if c.Bitbucket.BaseURL == "" {
		c.Bitbucket.BaseURL = "https://api.bitbucket.org"
	}
	if c.GitHub.BaseURL == "" {
		c.GitHub.BaseURL = "https://api.github.com"
	}
	for i := range c.Repositories {
		repository := &c.Repositories[i]
		if len(repository.OpenCode.Command) == 0 {
			repository.OpenCode.Command = []string{c.Worker.Command, "run"}
		}
		if repository.DefaultBranch == "" && repository.CloneURL == "" && repository.Bitbucket.Workspace != "" && repository.Bitbucket.Repository != "" {
			repository.DefaultBranch = "main"
		}
		if repository.CloneURL == "" && repository.Bitbucket.Workspace != "" && repository.Bitbucket.Repository != "" {
			if repository.Git.SSHSecret != "" {
				repository.CloneURL = "ssh://git@bitbucket.org/" + url.PathEscape(repository.Bitbucket.Workspace) + "/" + url.PathEscape(repository.Bitbucket.Repository) + ".git"
			} else {
				repository.CloneURL = "https://bitbucket.org/" + url.PathEscape(repository.Bitbucket.Workspace) + "/" + url.PathEscape(repository.Bitbucket.Repository) + ".git"
			}
		}
		if repository.DefaultBranch == "" && repository.CloneURL == "" && repository.GitHub.Owner != "" && repository.GitHub.Repository != "" {
			repository.DefaultBranch = "main"
		}
		if repository.CloneURL == "" && repository.GitHub.Owner != "" && repository.GitHub.Repository != "" {
			if repository.Git.SSHSecret != "" {
				repository.CloneURL = "ssh://git@github.com/" + url.PathEscape(repository.GitHub.Owner) + "/" + url.PathEscape(repository.GitHub.Repository) + ".git"
			} else {
				repository.CloneURL = "https://github.com/" + url.PathEscape(repository.GitHub.Owner) + "/" + url.PathEscape(repository.GitHub.Repository) + ".git"
			}
		}
	}
}

func (c Config) validate() error {
	if c.Controller.ListenAddress == "" {
		return fmt.Errorf("controller.listen_address must not be empty")
	}
	if strings.TrimSpace(c.Controller.WebhookListenAddress) == "" {
		return fmt.Errorf("controller.webhook_listen_address must not be blank")
	}
	if c.Controller.WebhookListenAddress == c.Controller.ListenAddress {
		return fmt.Errorf("controller.webhook_listen_address must differ from controller.listen_address")
	}
	if !validDNSName(c.Controller.Namespace) {
		return fmt.Errorf("controller.namespace must be a valid Kubernetes namespace")
	}
	if c.Controller.Deadline <= 0 {
		return fmt.Errorf("controller.deadline must be positive")
	}
	if c.Controller.ReviewDebounce <= 0 {
		return fmt.Errorf("controller.review_debounce must be positive")
	}
	if c.Controller.MaxFixAttempts < 0 {
		return fmt.Errorf("controller.max_fix_attempts must not be negative")
	}
	if strings.TrimSpace(c.Worker.Command) == "" {
		return fmt.Errorf("worker.command must not be empty")
	}
	if strings.TrimSpace(c.Worker.BranchPrefix) == "" {
		return fmt.Errorf("worker.branch_prefix must not be empty")
	}
	if err := validateSecretSource(c.Bitbucket.WebhookSecret, "bitbucket.webhook_secret"); err != nil {
		return err
	}
	if err := validateSecretSource(c.GitHub.WebhookSecret, "github.webhook_secret"); err != nil {
		return err
	}
	if err := validateBitbucketBaseURL(c.Bitbucket.BaseURL); err != nil {
		return err
	}
	if c.GitHub.BaseURL != "" {
		if err := validateGitHubBaseURL(c.GitHub.BaseURL); err != nil {
			return err
		}
	}

	if err := validateResources(c.Worker.Resources, "worker.resources"); err != nil {
		return err
	}
	if err := validateWorker(c.Worker, "worker"); err != nil {
		return err
	}
	forgeCredentials := make(map[[3]string]string, len(c.Repositories))
	for i, repository := range c.Repositories {
		if repository.bitbucketSet == repository.githubSet {
			return fmt.Errorf("repositories[%d] must configure exactly one of bitbucket or github", i)
		}
		if strings.TrimSpace(repository.CloneURL) == "" {
			return fmt.Errorf("repositories[%d].clone_url is required", i)
		}
		cloneURL, err := url.Parse(repository.CloneURL)
		if err != nil || cloneURL.Scheme == "" || cloneURL.Host == "" {
			return fmt.Errorf("repositories[%d].clone_url is not a valid URL", i)
		}
		if cloneURL.User != nil && !(cloneURL.Scheme == "ssh" && cloneURL.User.Username() == "git" && !hasPassword(cloneURL.User)) {
			return fmt.Errorf("repositories[%d].clone_url must not contain inline credentials", i)
		}
		if strings.TrimSpace(repository.DefaultBranch) == "" {
			return fmt.Errorf("repositories[%d].default_branch is required", i)
		}
		if strings.TrimSpace(repository.Worker.Image) == "" {
			return fmt.Errorf("repositories[%d].worker.image is required", i)
		}
		var provider, owner, repositoryName, credentialsSecret string
		if repository.bitbucketSet {
			provider = "bitbucket"
			owner = repository.Bitbucket.Workspace
			repositoryName = repository.Bitbucket.Repository
			credentialsSecret = repository.Bitbucket.CredentialsSecret
		} else {
			provider = "github"
			owner = repository.GitHub.Owner
			repositoryName = repository.GitHub.Repository
			credentialsSecret = repository.GitHub.CredentialsSecret
		}
		trimmedOwner, trimmedRepository := strings.TrimSpace(owner), strings.TrimSpace(repositoryName)
		if trimmedOwner == "" || trimmedRepository == "" {
			return fmt.Errorf("repositories[%d].%s owner/workspace and repository are required", i, provider)
		}
		if trimmedOwner != owner || trimmedRepository != repositoryName {
			return fmt.Errorf("repositories[%d].%s coordinates must not have surrounding whitespace", i, provider)
		}
		if credentialsSecret == "" {
			return fmt.Errorf("repositories[%d].%s.credentials_secret_name is required", i, provider)
		}
		coordinate := [3]string{provider, strings.ToLower(owner), strings.ToLower(repositoryName)}
		if previous, exists := forgeCredentials[coordinate]; exists && previous != credentialsSecret {
			return fmt.Errorf("repositories[%d].%s conflicts with credentials for %s/%s", i, provider, owner, repositoryName)
		}
		forgeCredentials[coordinate] = credentialsSecret
		if repository.Git.BranchPrefix != "" && strings.TrimSpace(repository.Git.BranchPrefix) == "" {
			return fmt.Errorf("repositories[%d].git.branch_prefix must not be blank", i)
		}
		for j, command := range repository.Validation.Commands {
			if len(command) == 0 || strings.TrimSpace(command[0]) == "" {
				return fmt.Errorf("repositories[%d].validation.commands[%d] must not be empty", i, j)
			}
		}
		if repository.Validation.MaxFixAttempts != nil && repository.Validation.MaxFixes != nil {
			return fmt.Errorf("repositories[%d].validation must not set both max_fix_attempts and max_fixes", i)
		}
		maxFixes := repository.Validation.MaxFixAttempts
		if maxFixes == nil {
			maxFixes = repository.Validation.MaxFixes
		}
		if maxFixes != nil && (*maxFixes < 0 || *maxFixes > 10) {
			return fmt.Errorf("repositories[%d].validation.max_fixes must be between 0 and 10", i)
		}
		for path, name := range map[string]string{
			"git.ssh_secret":                    repository.Git.SSHSecret,
			"opencode.config_secret":            repository.OpenCode.ConfigSecret,
			"bitbucket.credentials_secret_name": repository.Bitbucket.CredentialsSecret,
			"github.credentials_secret_name":    repository.GitHub.CredentialsSecret,
		} {
			if name != "" {
				if err := validateSecretName(name, fmt.Sprintf("repositories[%d].%s", i, path)); err != nil {
					return err
				}
			}
		}
		if repository.credentialsSet {
			if err := validateSecretName(repository.Credentials.SecretName, fmt.Sprintf("repositories[%d].credentials.secret_name", i)); err != nil {
				return err
			}
		}
		if err := validateWorker(repository.Worker, fmt.Sprintf("repositories[%d].worker", i)); err != nil {
			return err
		}
	}
	if hasProviderRepository(c.Repositories, "bitbucket") && c.Bitbucket.WebhookSecret.File == "" && c.Bitbucket.WebhookSecret.Env == "" {
		return fmt.Errorf("bitbucket.webhook_secret must configure either file or env")
	}
	if hasProviderRepository(c.Repositories, "github") && c.GitHub.WebhookSecret.File == "" && c.GitHub.WebhookSecret.Env == "" {
		return fmt.Errorf("github.webhook_secret must configure either file or env")
	}
	return nil
}

func hasProviderRepository(repositories RepositoryConfigs, provider string) bool {
	for _, repository := range repositories {
		if provider == "bitbucket" && repository.bitbucketSet || provider == "github" && repository.githubSet {
			return true
		}
	}
	return false
}

func hasPassword(user *url.Userinfo) bool { _, set := user.Password(); return set }

func validateWorker(worker WorkerConfig, path string) error {
	if worker.Deadline.Value() < 0 {
		return fmt.Errorf("%s.deadline must not be negative", path)
	}
	if err := validateResources(worker.Resources, path+".resources"); err != nil {
		return err
	}
	for i, mount := range worker.Mounts {
		if strings.TrimSpace(mount.Name) == "" || strings.TrimSpace(mount.MountPath) == "" {
			return fmt.Errorf("%s.mounts[%d] requires name and mount_path", path, i)
		}
		if mount.Secret != nil {
			if err := validateSecretName(mount.Secret.SecretName, fmt.Sprintf("%s.mounts[%d].secret.secret_name", path, i)); err != nil {
				return err
			}
		}
		if mount.ConfigMap != nil && !validDNSName(mount.ConfigMap.Name) {
			return fmt.Errorf("%s.mounts[%d].config_map.name must be a valid Kubernetes ConfigMap name", path, i)
		}
		sources := 0
		if mount.Secret != nil {
			sources++
		}
		if mount.ConfigMap != nil {
			sources++
		}
		if mount.EmptyDir != nil {
			sources++
		}
		if sources != 1 {
			return fmt.Errorf("%s.mounts[%d] requires exactly one source", path, i)
		}
		if mount.EmptyDir != nil && mount.EmptyDir.SizeLimit != nil && mount.EmptyDir.SizeLimit.Sign() < 0 {
			return fmt.Errorf("%s.mounts[%d].empty_dir.size_limit must not be negative", path, i)
		}
	}
	for i, mount := range worker.MountedSecrets {
		if !validDNSName(mount.Name) || !strings.HasPrefix(mount.MountPath, "/") {
			return fmt.Errorf("%s.mounted_secrets[%d] is invalid", path, i)
		}
	}
	for i, mount := range worker.MountedConfigMaps {
		if !validDNSName(mount.Name) || !strings.HasPrefix(mount.MountPath, "/") {
			return fmt.Errorf("%s.mounted_configmaps[%d] is invalid", path, i)
		}
	}
	for i, env := range worker.Env {
		if strings.TrimSpace(env.Name) == "" {
			return fmt.Errorf("%s.env[%d].name is required", path, i)
		}
		if env.Name == "SIMPLESWE_SECRET_PATHS" {
			return fmt.Errorf("%s.env[%d].name is reserved", path, i)
		}
		sources := 0
		if env.Value != "" {
			sources++
		}
		if env.Secret != nil {
			sources++
		}
		if env.ConfigMap != nil {
			sources++
		}
		if sources > 1 {
			return fmt.Errorf("%s.env[%d] has multiple value sources", path, i)
		}
		for kind, ref := range map[string]*KeyRef{"secret": env.Secret, "config_map": env.ConfigMap} {
			if ref != nil && (!validDNSName(ref.Name) || strings.TrimSpace(ref.Key) == "") {
				return fmt.Errorf("%s.env[%d].%s is invalid", path, i, kind)
			}
		}
	}
	return nil
}

func validateSecretSource(source SecretSource, path string) error {
	if source.File != "" && source.Env != "" {
		return fmt.Errorf("%s must select either file or env", path)
	}
	if source.File != "" && !strings.HasPrefix(source.File, "/") {
		return fmt.Errorf("%s.file must be absolute", path)
	}
	if source.Env != "" {
		for i, r := range source.Env {
			if !(r == '_' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9') {
				return fmt.Errorf("%s.env must be an environment variable name", path)
			}
		}
	}
	return nil
}

func validateBitbucketBaseURL(value string) error {
	if !validForgeBaseURL(value) {
		return fmt.Errorf("bitbucket.base_url must be an HTTPS URL without credentials (HTTP is allowed only for loopback test servers)")
	}
	return nil
}

func validateGitHubBaseURL(value string) error {
	if !validForgeBaseURL(value) {
		return fmt.Errorf("github.base_url must be an HTTPS URL without credentials (HTTP is allowed only for loopback test servers)")
	}
	return nil
}

func validForgeBaseURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Host != "" && parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == "" && (parsed.Scheme == "https" || parsed.Scheme == "http" && loopbackHost(parsed.Hostname()))
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validateResources(resources ResourceRequirements, path string) error {
	for name, quantity := range resources.Requests {
		if quantity.Sign() < 0 {
			return fmt.Errorf("%s.requests[%s] must not be negative", path, name)
		}
	}
	for name, quantity := range resources.Limits {
		if quantity.Sign() < 0 {
			return fmt.Errorf("%s.limits[%s] must not be negative", path, name)
		}
	}
	return nil
}

func validateSecretName(name, path string) error {
	if !validDNSName(name) {
		return fmt.Errorf("%s must be a valid Kubernetes Secret name", path)
	}
	return nil
}

func validDNSName(name string) bool {
	if len(name) == 0 || len(name) > 253 {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
				return false
			}
		}
	}
	return true
}

func decodeNodeStrict(node *yaml.Node, target any) error {
	encoded, err := yaml.Marshal(node)
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	return decoder.Decode(target)
}

func (c *ControllerConfig) UnmarshalYAML(node *yaml.Node) error {
	var raw struct {
		ListenAddress        string `yaml:"listen_address"`
		WebhookListenAddress string `yaml:"webhook_listen_address"`
		Namespace            string `yaml:"namespace"`
		Deadline             string `yaml:"deadline"`
		ReviewDebounce       string `yaml:"review_debounce"`
		MaxFixAttempts       *int   `yaml:"max_fix_attempts"`
	}
	if err := decodeNodeStrict(node, &raw); err != nil {
		return err
	}
	deadline := time.Duration(0)
	if raw.Deadline != "" {
		parsed, err := time.ParseDuration(raw.Deadline)
		if err != nil {
			return fmt.Errorf("controller.deadline must be a duration")
		}
		deadline = parsed
	}
	reviewDebounce := time.Duration(0)
	if raw.ReviewDebounce != "" {
		parsed, err := time.ParseDuration(raw.ReviewDebounce)
		if err != nil {
			return fmt.Errorf("controller.review_debounce must be a duration")
		}
		reviewDebounce = parsed
	}
	*c = ControllerConfig{
		ListenAddress:        raw.ListenAddress,
		WebhookListenAddress: raw.WebhookListenAddress,
		Namespace:            raw.Namespace,
		Deadline:             deadline,
		ReviewDebounce:       reviewDebounce,
		maxFixAttemptsSet:    raw.MaxFixAttempts != nil,
	}
	if raw.MaxFixAttempts != nil {
		c.MaxFixAttempts = *raw.MaxFixAttempts
	}
	return nil
}

func (r *RepositoryConfig) UnmarshalYAML(node *yaml.Node) error {
	var raw struct {
		Name          string                     `yaml:"name,omitempty"`
		CloneURL      string                     `yaml:"clone_url"`
		DefaultBranch string                     `yaml:"default_branch"`
		Credentials   *Credentials               `yaml:"credentials,omitempty"`
		Worker        WorkerConfig               `yaml:"worker"`
		Git           GitConfig                  `yaml:"git,omitempty"`
		OpenCode      OpenCodeConfig             `yaml:"opencode,omitempty"`
		Validation    ValidationConfig           `yaml:"validation,omitempty"`
		Bitbucket     *RepositoryBitbucketConfig `yaml:"bitbucket,omitempty"`
		GitHub        *RepositoryGitHubConfig    `yaml:"github,omitempty"`
	}
	if err := decodeNodeStrict(node, &raw); err != nil {
		return err
	}
	*r = RepositoryConfig{
		Name:           raw.Name,
		CloneURL:       raw.CloneURL,
		DefaultBranch:  raw.DefaultBranch,
		Worker:         raw.Worker,
		Git:            raw.Git,
		OpenCode:       raw.OpenCode,
		Validation:     raw.Validation,
		credentialsSet: raw.Credentials != nil,
		bitbucketSet:   raw.Bitbucket != nil,
		githubSet:      raw.GitHub != nil,
	}
	if raw.Credentials != nil {
		r.Credentials = *raw.Credentials
	}
	if raw.Bitbucket != nil {
		r.Bitbucket = *raw.Bitbucket
	}
	if raw.GitHub != nil {
		r.GitHub = *raw.GitHub
	}
	return nil
}

func (r *RepositoryConfigs) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.SequenceNode:
		var repositories []RepositoryConfig
		if err := decodeNodeStrict(node, &repositories); err != nil {
			return err
		}
		*r = repositories
		return nil
	case yaml.MappingNode:
		var registry map[string]RepositoryConfig
		if err := decodeNodeStrict(node, &registry); err != nil {
			return err
		}
		repositories := make([]RepositoryConfig, 0, len(registry))
		for name, repository := range registry {
			if repository.Name == "" {
				repository.Name = name
			}
			repositories = append(repositories, repository)
		}
		*r = repositories
		return nil
	default:
		return fmt.Errorf("repositories must be a list or registry map")
	}
}
