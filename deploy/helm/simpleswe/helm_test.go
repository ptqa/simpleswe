package helm

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const githubValues = `
config:
  repositories:
    widget:
      worker: {image: worker:v1}
      github:
        owner: octo
        repository: widget
        credentials_secret_name: github-widget
`

const hermesValues = `
hermes:
  enabled: true
  image:
    repository: ghcr.io/simpleswe/simpleswe-hermes
    digest: sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  config:
    model: openai/gpt-5.4
  secrets:
    slack:
      name: simpleswe-hermes-slack
      keys:
        botToken: bot-token
        appToken: app-token
        allowedUser: allowed-user
    modelProvider:
      name: simpleswe-hermes-model
      key: api-key
      env: OPENROUTER_API_KEY
config:
  repositories:
    widget:
      worker: {image: worker:v1}
      github:
        owner: octo
        repository: widget
        credentials_secret_name: github-widget
`

func TestHelmHermesSidecarRendersExactlyWithController(t *testing.T) {
	deployment := parseDeployment(t, renderHelm(t, hermesValues))
	counts := make(map[string]int, len(deployment.Spec.Template.Spec.Containers))
	for _, container := range deployment.Spec.Template.Spec.Containers {
		counts[container.Name]++
	}
	if len(deployment.Spec.Template.Spec.Containers) != 2 || counts["controller"] != 1 || counts["hermes"] != 1 {
		t.Fatalf("containers = %#v, want exactly controller and hermes", deployment.Spec.Template.Spec.Containers)
	}
}

func TestHelmHermesSidecarMountsAreIsolatedFromController(t *testing.T) {
	deployment := parseDeployment(t, renderHelm(t, hermesValues))
	controller := deploymentContainer(t, deployment, "controller")
	hermes := deploymentContainer(t, deployment, "hermes")

	for _, want := range []struct {
		path     string
		readOnly bool
	}{
		{path: "/var/lib/simpleswe", readOnly: false},
		{path: "/etc/simpleswe", readOnly: true},
		{path: "/run/secrets/github/github-widget", readOnly: true},
		{path: "/run/secrets/webhooks", readOnly: true},
	} {
		assertVolumeMount(t, controller, want.path, want.readOnly)
	}
	for _, forbidden := range []string{"/run/secrets/slack", "/run/secrets/model-provider"} {
		assertNoVolumeMount(t, controller, forbidden)
	}

	for _, env := range []struct {
		name      string
		secret    string
		secretKey string
	}{
		{name: "SLACK_BOT_TOKEN", secret: "simpleswe-hermes-slack", secretKey: "bot-token"},
		{name: "SLACK_APP_TOKEN", secret: "simpleswe-hermes-slack", secretKey: "app-token"},
		{name: "SLACK_ALLOWED_USERS", secret: "simpleswe-hermes-slack", secretKey: "allowed-user"},
		{name: "OPENROUTER_API_KEY", secret: "simpleswe-hermes-model", secretKey: "api-key"},
	} {
		assertSecretEnv(t, hermes, env.name, env.secret, env.secretKey)
		assertNoEnv(t, controller, env.name)
	}
	assertLiteralEnv(t, hermes, "HERMES_MODEL_PROVIDER_ENV", "OPENROUTER_API_KEY")
	assertNoEnv(t, controller, "HERMES_MODEL_PROVIDER_ENV")
	for _, forbidden := range []string{
		"/var/lib/simpleswe",
		"/run/secrets",
	} {
		assertNoVolumeMount(t, hermes, forbidden)
	}
	for _, want := range []struct {
		path       string
		readOnly   bool
		hasSubPath bool
	}{
		{path: "/opt/data", readOnly: false, hasSubPath: false},
		{path: "/opt/data/config.yaml", readOnly: true, hasSubPath: true},
		{path: "/opt/data/skills/simpleswe/SKILL.md", readOnly: true, hasSubPath: true},
	} {
		assertVolumeMountWithSubPath(t, hermes, want.path, want.readOnly, want.hasSubPath)
	}
	assertNoVolumeMount(t, controller, "/opt/data")
}

func TestHelmHermesSidecarProcessImageAndSecurity(t *testing.T) {
	hermes := deploymentContainer(t, parseDeployment(t, renderHelm(t, hermesValues)), "hermes")
	if !reflect.DeepEqual(hermes.Command, []string{"/bin/sh", "-ec"}) || len(hermes.Args) != 1 {
		t.Fatalf("Hermes command/args = %#v/%#v, want /bin/sh -ec with one startup script", hermes.Command, hermes.Args)
	}
	script := strings.TrimSpace(hermes.Args[0])
	if lines := strings.Split(script, "\n"); len(lines) > 5 {
		t.Fatalf("Hermes startup guard has %d lines, want at most 5:\n%s", len(lines), script)
	}
	for _, want := range []string{"SLACK_BOT_TOKEN:?", "SLACK_APP_TOKEN:?", "SLACK_ALLOWED_USERS:?", "HERMES_MODEL_PROVIDER_ENV", "exec hermes gateway"} {
		if !strings.Contains(script, want) {
			t.Errorf("Hermes startup script does not contain %q:\n%s", want, script)
		}
	}
	if got, want := hermes.Image, "ghcr.io/simpleswe/simpleswe-hermes@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"; got != want {
		t.Fatalf("Hermes image = %q, want pinned %q", got, want)
	}
	if !hermes.SecurityContext.RunAsNonRoot || !hermes.SecurityContext.ReadOnlyRootFilesystem || hermes.SecurityContext.AllowPrivilegeEscalation == nil || *hermes.SecurityContext.AllowPrivilegeEscalation {
		t.Fatalf("Hermes securityContext = %#v, want non-root/read-only-root/no-privilege-escalation", hermes.SecurityContext)
	}
}

func TestHelmHermesStartupGuardRejectsEmptyCredentialsBeforeExec(t *testing.T) {
	hermes := deploymentContainer(t, parseDeployment(t, renderHelm(t, hermesValues)), "hermes")
	bin := t.TempDir()
	marker := filepath.Join(bin, "invoked")
	if err := os.WriteFile(filepath.Join(bin, "hermes"), []byte("#!/bin/sh\n: > \"$HERMES_INVOKED\"\n"), 0o700); err != nil {
		t.Fatalf("write fake Hermes: %v", err)
	}
	baseEnv := map[string]string{
		"PATH":                      bin + ":/usr/bin:/bin",
		"HERMES_INVOKED":            marker,
		"HERMES_MODEL_PROVIDER_ENV": "OPENROUTER_API_KEY",
		"SLACK_BOT_TOKEN":           "bot",
		"SLACK_APP_TOKEN":           "app",
		"SLACK_ALLOWED_USERS":       "U1",
		"OPENROUTER_API_KEY":        "model",
	}
	for _, empty := range []string{"SLACK_BOT_TOKEN", "SLACK_APP_TOKEN", "SLACK_ALLOWED_USERS", "OPENROUTER_API_KEY"} {
		t.Run(empty, func(t *testing.T) {
			env := make([]string, 0, len(baseEnv))
			for name, value := range baseEnv {
				if name == empty {
					value = ""
				}
				env = append(env, name+"="+value)
			}
			command := exec.Command(hermes.Command[0], append(hermes.Command[1:], hermes.Args...)...)
			command.Env = env
			if output, err := command.CombinedOutput(); err == nil {
				t.Fatalf("startup accepted empty %s:\n%s", empty, output)
			}
			if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("gateway executed with empty %s", empty)
			}
		})
	}
	var env []string
	for name, value := range baseEnv {
		env = append(env, name+"="+value)
	}
	command := exec.Command(hermes.Command[0], append(hermes.Command[1:], hermes.Args...)...)
	command.Env = env
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("startup rejected complete credentials: %v\n%s", err, output)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("gateway was not executed with complete credentials: %v", err)
	}
}

func TestHelmHermesConfigMapContainsNarrowSkill(t *testing.T) {
	configMap := parseConfigMap(t, renderHelm(t, hermesValues))
	skill := findHermesSkill(t, configMap.Data)
	lowerSkill := strings.ToLower(skill)
	for _, command := range []string{
		"task create",
		"task list",
		"task show",
		"task wait",
		"task logs",
		"task cancel",
		"task retry",
	} {
		if !strings.Contains(lowerSkill, command) {
			t.Errorf("Hermes skill does not allow %q", command)
		}
	}
	for _, match := range regexp.MustCompile(`(?i)\bsimpleswe\s+task\s+([a-z]+)\b`).FindAllStringSubmatch(skill, -1) {
		if !map[string]bool{"create": true, "list": true, "show": true, "wait": true, "logs": true, "cancel": true, "retry": true}[strings.ToLower(match[1])] {
			t.Errorf("Hermes skill allows unexpected task command %q", match[0])
		}
	}
	if !strings.Contains(lowerSkill, "--address http://127.0.0.1:8080") {
		t.Error("Hermes skill does not require the localhost controller address")
	}
	for _, command := range []string{"simpleswe controller", "simpleswe worker", "simpleswe tui"} {
		if strings.Contains(lowerSkill, command) {
			t.Errorf("Hermes skill invokes forbidden command %q", command)
		}
	}
	for _, phrase := range []string{
		"one idempotency key",
		"per slack request",
		"reuse the same key",
		"preserve the user's engineering request",
		"without inventing requirements",
		"accepted task id immediately",
		"terminal(background=true, notify_on_complete=true)",
		"slack conversation remains responsive",
		"completion generates a follow-up event",
		"terminal failure",
		"originating thread",
		"wait command completes",
		"explicit status questions",
		"logs or diagnosis",
		"user asks for logs or diagnosis",
		"minimum toolset",
		"cli workflow",
		"unrelated file",
		"browser",
		"infrastructure",
		"by default",
	} {
		if !strings.Contains(lowerSkill, phrase) {
			t.Errorf("Hermes skill does not state %q", phrase)
		}
	}
	if !regexp.MustCompile(`(?i)configured repository(?: name)?[^.!?\n]{0,80}before (?:creating|create)`).MatchString(skill) {
		t.Error("Hermes skill does not require a configured repository before task creation")
	}
	if !regexp.MustCompile(`(?i)task show[^.!?\n]{0,100}explicit status questions`).MatchString(skill) {
		t.Error("Hermes skill does not reserve task show for explicit status questions")
	}
	if !regexp.MustCompile(`(?i)task wait[^.!?\n]{0,160}terminal\(background=true, notify_on_complete=true\)`).MatchString(skill) {
		t.Error("Hermes skill does not run task wait with background completion notification")
	}
	if !regexp.MustCompile(`(?i)pull[- ]request\s+url`).MatchString(skill) {
		t.Error("Hermes skill does not report the pull-request URL")
	}
	if !regexp.MustCompile(`(?i)(?:pull[- ]request\s+url|terminal failure)[^.!?\n]{0,160}(?:originating thread|wait command completes)`).MatchString(skill) {
		t.Error("Hermes skill does not report the wait result to the originating thread")
	}
	for _, operation := range []string{"cancel", "retry"} {
		pattern := regexp.MustCompile(`(?i)(?:confirm\w*|confirmation)[^.!?\n]{0,100}\b` + operation + `\w*\b|\b` + operation + `\w*\b[^.!?\n]{0,100}(?:confirm\w*|confirmation)`)
		if !pattern.MatchString(skill) {
			t.Errorf("Hermes skill does not require confirmation for %s", operation)
		}
	}
	if !regexp.MustCompile(`(?i)(?:cancel\w*|retry\w*)[^.!?\n]{0,160}(?:unless|except)[^.!?\n]{0,100}(?:explicit|request|ask)|(?:unless|except)[^.!?\n]{0,100}(?:explicit|request|ask)[^.!?\n]{0,160}(?:cancel\w*|retry\w*)`).MatchString(skill) {
		t.Error("Hermes skill does not allow explicitly requested cancellation or retry")
	}
}

func TestHelmHermesConfigDisablesAutoIncludedToolsets(t *testing.T) {
	configMap := parseConfigMap(t, renderHelm(t, hermesValues))
	var config struct {
		Agent struct {
			DisabledToolsets []string `yaml:"disabled_toolsets"`
		} `yaml:"agent"`
		PlatformToolsets map[string][]string `yaml:"platform_toolsets"`
	}
	if err := yaml.Unmarshal([]byte(configMap.Data["hermes-config.yaml"]), &config); err != nil {
		t.Fatalf("decode rendered Hermes config: %v", err)
	}
	if !reflect.DeepEqual(config.Agent.DisabledToolsets, []string{"bfl", "kanban"}) {
		t.Fatalf("disabled toolsets = %#v, want bfl and kanban", config.Agent.DisabledToolsets)
	}
	if !reflect.DeepEqual(config.PlatformToolsets["slack"], []string{"terminal", "skills"}) {
		t.Fatalf("Slack platform toolsets = %#v, want terminal and skills only", config.PlatformToolsets["slack"])
	}
}

func TestHelmHermesSchemaRejectsIncompleteConfiguration(t *testing.T) {
	tests := map[string]string{
		"image pin":                     strings.Replace(hermesValues, "    digest: sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n", "", 1),
		"model config":                  strings.Replace(hermesValues, "  config:\n    model: openai/gpt-5.4\n", "", 1),
		"Slack bot Secret ref":          strings.Replace(hermesValues, "        botToken: bot-token\n", "", 1),
		"Slack app Secret ref":          strings.Replace(hermesValues, "        appToken: app-token\n", "", 1),
		"Slack allowed-user Secret ref": strings.Replace(hermesValues, "        allowedUser: allowed-user\n", "", 1),
		"model-provider Secret ref":     strings.Replace(hermesValues, "    modelProvider:\n      name: simpleswe-hermes-model\n      key: api-key\n      env: OPENROUTER_API_KEY\n", "", 1),
		"model-provider env":            strings.Replace(hermesValues, "      env: OPENROUTER_API_KEY\n", "", 1),
	}
	for name, values := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := renderHelmError(t, values); err == nil {
				t.Fatal("Helm accepted incomplete Hermes configuration")
			}
		})
	}
}

func TestHelmHermesSchemaRejectsInvalidConfiguration(t *testing.T) {
	tests := map[string]string{
		"unknown field":              strings.Replace(hermesValues, "  enabled: true\n", "  enabled: true\n  unsupported: true\n", 1),
		"image digest":               strings.Replace(hermesValues, "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "sha256:not-a-digest", 1),
		"pull policy":                strings.Replace(hermesValues, "    digest: sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n", "    digest: sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n    pullPolicy: Sometimes\n", 1),
		"Slack Secret name":          strings.Replace(hermesValues, "name: simpleswe-hermes-slack", "name: Invalid_Name", 1),
		"model environment name":     strings.Replace(hermesValues, "env: OPENROUTER_API_KEY", "env: 1INVALID", 1),
		"reserved model environment": strings.Replace(hermesValues, "env: OPENROUTER_API_KEY", "env: SLACK_BOT_TOKEN", 1),
		"resources":                  strings.Replace(hermesValues, "  config:\n    model: openai/gpt-5.4\n", "  resources:\n    requests: {cpu: nope, memory: 512Mi}\n    limits: {cpu: '1', memory: 2Gi}\n  config:\n    model: openai/gpt-5.4\n", 1),
		"storage":                    strings.Replace(hermesValues, "  config:\n    model: openai/gpt-5.4\n", "  config:\n    model: openai/gpt-5.4\n  storage:\n    storageClass: ''\n    size: 5Gi\n    accessModes: [InvalidMode]\n", 1),
		"read-only storage":          strings.Replace(hermesValues, "  config:\n    model: openai/gpt-5.4\n", "  config:\n    model: openai/gpt-5.4\n  storage:\n    storageClass: ''\n    size: 5Gi\n    accessModes: [ReadOnlyMany]\n", 1),
	}
	for name, values := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := renderHelmError(t, values); err == nil {
				t.Fatal("Helm accepted invalid Hermes configuration")
			}
		})
	}
}

func TestHelmHermesDoesNotExpandRBAC(t *testing.T) {
	rendered := renderHelm(t, hermesValues)
	decoder := yaml.NewDecoder(strings.NewReader(rendered))
	var roles []roleManifest
	for {
		var document roleManifest
		err := decoder.Decode(&document)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode Helm output: %v", err)
		}
		if document.Kind == "ClusterRole" || document.Kind == "ClusterRoleBinding" {
			t.Fatalf("Helm rendered cluster-wide RBAC object %q", document.Kind)
		}
		if document.Kind == "Role" {
			roles = append(roles, document)
		}
	}
	want := []roleRule{
		{APIGroups: []string{"batch"}, Resources: []string{"jobs"}, Verbs: []string{"get", "list", "watch", "create", "delete"}},
		{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"}},
		{APIGroups: []string{""}, Resources: []string{"pods/log"}, Verbs: []string{"get"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create", "get", "delete"}},
	}
	if len(roles) != 1 || !reflect.DeepEqual(roles[0].Rules, want) {
		t.Fatalf("namespaced Role rules = %#v, want %#v", roles, want)
	}
}

func TestHelmHermesControllerConfigOmitsSlack(t *testing.T) {
	configMap := parseConfigMap(t, renderHelm(t, hermesValues))
	var config map[string]any
	if err := yaml.Unmarshal([]byte(configMap.Data["config.yaml"]), &config); err != nil {
		t.Fatalf("decode rendered controller config: %v", err)
	}
	if _, ok := config["slack"]; ok {
		t.Fatalf("rendered controller config contains slack: %#v", config["slack"])
	}
}

func TestHelmSchemaAcceptsGitHubRepository(t *testing.T) {
	renderHelm(t, githubValues)
}

func TestHelmSchemaRejectsRepositoryWithoutForge(t *testing.T) {
	if _, err := renderHelmError(t, `
config:
  repositories:
    widget:
      worker: {image: worker:v1}
`); err == nil {
		t.Fatal("Helm accepted a repository without a forge")
	}
}

func TestHelmSchemaRejectsRepositoryWithBothForges(t *testing.T) {
	if _, err := renderHelmError(t, `
config:
  repositories:
    widget:
      worker: {image: worker:v1}
      bitbucket: {workspace: acme, repository: widget, credentials_secret_name: bitbucket-widget}
      github: {owner: octo, repository: widget, credentials_secret_name: github-widget}
`); err == nil {
		t.Fatal("Helm accepted a repository with both forges")
	}
}

func TestHelmSchemaRejectsEmptyWebhookIngressPeer(t *testing.T) {
	if _, err := renderHelmError(t, "networkPolicy:\n  webhookIngress: [{}]\n"); err == nil {
		t.Fatal("Helm accepted an empty webhook ingress peer")
	}
}

func TestHelmSchemaRejectsUnsupportedWebhookListenPort(t *testing.T) {
	if output, err := renderHelmError(t, "config:\n  controller:\n    webhook_listen_address: ':9090'\n"); err == nil {
		t.Fatalf("Helm accepted unsupported webhook listen port:\n%s", output)
	}
}

func TestHelmSchemaRejectsWebhookSecretSourcesDeploymentCannotSupply(t *testing.T) {
	for name, values := range map[string]string{
		"GitHub env":               "config:\n  github:\n    webhook_secret: {env: GITHUB_WEBHOOK_SECRET}\n",
		"GitHub arbitrary file":    "config:\n  github:\n    webhook_secret: {file: /other/github}\n",
		"Bitbucket env":            "config:\n  bitbucket:\n    webhook_secret: {env: BITBUCKET_WEBHOOK_SECRET}\n",
		"Bitbucket arbitrary file": "config:\n  bitbucket:\n    webhook_secret: {file: /other/bitbucket}\n",
	} {
		t.Run(name, func(t *testing.T) {
			if output, err := renderHelmError(t, values); err == nil {
				t.Fatalf("Helm accepted unsupported webhook secret source:\n%s", output)
			}
		})
	}
}

func TestHelmDeploymentMountsEachGitHubTokenSecret(t *testing.T) {
	rendered := renderHelm(t, `
config:
  repositories:
    widget:
      worker: {image: worker:v1}
      github: {owner: octo, repository: widget, credentials_secret_name: github-widget}
    service:
      worker: {image: worker:v1}
      github: {owner: octo, repository: service, credentials_secret_name: github-service}
`)

	deployment := parseDeployment(t, rendered)
	secrets := map[string]bool{"github-widget": false, "github-service": false}
	for _, mount := range deployment.Spec.Template.Spec.Containers[0].VolumeMounts {
		if !strings.HasPrefix(mount.MountPath, "/run/secrets/github/") {
			continue
		}
		secretName := strings.TrimPrefix(mount.MountPath, "/run/secrets/github/")
		if _, ok := secrets[secretName]; !ok {
			t.Errorf("unexpected GitHub Secret mount %q", mount.MountPath)
			continue
		}
		for _, volume := range deployment.Spec.Template.Spec.Volumes {
			if volume.Name != mount.Name || volume.Secret == nil {
				continue
			}
			if volume.Secret.SecretName != secretName {
				t.Errorf("GitHub volume %q Secret = %q, want %q", mount.Name, volume.Secret.SecretName, secretName)
			}
			if len(volume.Secret.Items) != 1 || volume.Secret.Items[0].Key != "token" || volume.Secret.Items[0].Path != "token" {
				t.Errorf("GitHub volume %q items = %#v, want token/token", mount.Name, volume.Secret.Items)
			}
			secrets[secretName] = true
		}
	}
	for secretName, found := range secrets {
		if !found {
			t.Errorf("GitHub Secret %q was not mounted", secretName)
		}
	}
}

func TestHelmDeploymentMountsOnlyConfiguredWebhookSecretsAndRendersDefaultPaths(t *testing.T) {
	const values = `
secrets:
  webhooks:
    name: simpleswe-webhooks
    keys:
      github: github-signing
      bitbucket: bitbucket-signing
config:
  repositories:
    widget:
      worker: {image: worker:v1}
      github: {owner: octo, repository: widget, credentials_secret_name: github-widget}
    gadget:
      worker: {image: worker:v1}
      bitbucket: {workspace: acme, repository: gadget, credentials_secret_name: bitbucket-gadget}
`
	rendered := renderHelm(t, values)
	deployment := parseDeployment(t, rendered)
	assertWebhookSecretMounts(t, deployment, "simpleswe-webhooks", map[string]string{
		"github-signing":    "github",
		"bitbucket-signing": "bitbucket",
	})
	configMap := parseConfigMap(t, rendered)
	var config struct {
		GitHub struct {
			WebhookSecret struct {
				File string `yaml:"file"`
			} `yaml:"webhook_secret"`
		} `yaml:"github"`
		Bitbucket struct {
			WebhookSecret struct {
				File string `yaml:"file"`
			} `yaml:"webhook_secret"`
		} `yaml:"bitbucket"`
	}
	if err := yaml.Unmarshal([]byte(configMap.Data["config.yaml"]), &config); err != nil {
		t.Fatalf("decode rendered controller config: %v", err)
	}
	if config.GitHub.WebhookSecret.File != "/run/secrets/webhooks/github" || config.Bitbucket.WebhookSecret.File != "/run/secrets/webhooks/bitbucket" {
		t.Fatalf("rendered webhook secret files = %q/%q, want mounted provider paths", config.GitHub.WebhookSecret.File, config.Bitbucket.WebhookSecret.File)
	}
	assertNoIngress(t, rendered)

	githubOnly := renderHelm(t, strings.Replace(values, "    gadget:\n      worker: {image: worker:v1}\n      bitbucket: {workspace: acme, repository: gadget, credentials_secret_name: bitbucket-gadget}\n", "", 1))
	assertWebhookSecretMounts(t, parseDeployment(t, githubOnly), "simpleswe-webhooks", map[string]string{
		"github-signing": "github",
	})
	assertNoIngress(t, githubOnly)
}

func TestHelmWebhookStartupParity(t *testing.T) {
	tests := []struct {
		name   string
		values string
		items  map[string]string
	}{
		{name: "GitHub only", values: githubValues, items: map[string]string{"github": "github"}},
		{name: "Bitbucket only", values: `
config:
  repositories:
    widget:
      worker: {image: worker:v1}
      bitbucket: {workspace: acme, repository: widget, credentials_secret_name: bitbucket-widget}
`, items: map[string]string{"bitbucket": "bitbucket"}},
		{name: "empty repositories", values: "", items: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rendered := renderHelm(t, tt.values)
			configMap := parseConfigMap(t, rendered)
			var cfg struct {
				Controller struct {
					WebhookListenAddress string `yaml:"webhook_listen_address"`
				} `yaml:"controller"`
			}
			if err := yaml.Unmarshal([]byte(configMap.Data["config.yaml"]), &cfg); err != nil {
				t.Fatalf("decode rendered controller config: %v", err)
			}
			if cfg.Controller.WebhookListenAddress != ":8081" {
				t.Fatalf("rendered webhook listen address = %q, want :8081", cfg.Controller.WebhookListenAddress)
			}
			items, mounted := webhookSecretItems(parseDeployment(t, rendered))
			if mounted != (len(tt.items) > 0) {
				t.Fatalf("webhook Secret mounted = %t, want %t", mounted, len(tt.items) > 0)
			}
			if len(items) != len(tt.items) {
				t.Fatalf("webhook Secret items = %#v, want %#v", items, tt.items)
			}
			for key, path := range tt.items {
				if items[key] != path {
					t.Errorf("webhook Secret item %q = %q, want %q", key, items[key], path)
				}
			}
		})
	}
}

func TestHelmServicesAndNetworkPolicySeparateWebhookBoundary(t *testing.T) {
	rendered := renderHelm(t, githubValues+`
networkPolicy:
  trustedIngress:
    - namespaceSelector: {matchLabels: {access: api}}
  webhookIngress:
    - namespaceSelector: {matchLabels: {access: webhooks}}
`)
	services := parseServices(t, rendered)
	if got := services["simpleswe"]; len(got) != 1 || got[0] != 8080 {
		t.Fatalf("controller Service ports = %#v, want [8080]", got)
	}
	if got := services["simpleswe-webhooks"]; len(got) != 1 || got[0] != 8081 {
		t.Fatalf("webhook Service ports = %#v, want [8081]", got)
	}

	deployment := parseDeployment(t, rendered)
	ports := deployment.Spec.Template.Spec.Containers[0].Ports
	if len(ports) != 2 || ports[0].ContainerPort != 8080 || ports[1].ContainerPort != 8081 {
		t.Fatalf("controller container ports = %#v, want 8080 and 8081", ports)
	}

	policy := parseControllerNetworkPolicy(t, rendered)
	if len(policy.Spec.Ingress) != 2 {
		t.Fatalf("controller ingress rules = %#v, want two isolated rules", policy.Spec.Ingress)
	}
	for _, rule := range policy.Spec.Ingress {
		if len(rule.Ports) != 1 || len(rule.From) != 1 {
			t.Fatalf("controller ingress rule = %#v", rule)
		}
		port := rule.Ports[0].Port
		access := rule.From[0].NamespaceSelector.MatchLabels["access"]
		if port == 8080 && access != "api" || port == 8081 && access != "webhooks" || port != 8080 && port != 8081 {
			t.Errorf("ingress port %d granted to %q", port, access)
		}
	}

	empty := renderHelm(t, "")
	if services := parseServices(t, empty); len(services) != 1 || len(services["simpleswe"]) != 1 {
		t.Fatalf("empty repositories rendered Services = %#v, want controller only", services)
	}
	if ingress := parseControllerNetworkPolicy(t, empty).Spec.Ingress; len(ingress) != 0 {
		t.Fatalf("default controller ingress = %#v, want deny all", ingress)
	}
}

func deploymentContainer(t *testing.T, deployment deploymentManifest, name string) containerManifest {
	t.Helper()
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name == name {
			return container
		}
	}
	t.Fatalf("Deployment has no %q container", name)
	return containerManifest{}
}

func assertVolumeMount(t *testing.T, container containerManifest, path string, readOnly bool) {
	assertVolumeMountWithSubPath(t, container, path, readOnly, false)
}

func assertVolumeMountWithSubPath(t *testing.T, container containerManifest, path string, readOnly, hasSubPath bool) {
	t.Helper()
	for _, mount := range container.VolumeMounts {
		if mount.MountPath != path {
			continue
		}
		if mount.ReadOnly != readOnly {
			t.Errorf("%s mount %q readOnly = %t, want %t", container.Name, path, mount.ReadOnly, readOnly)
		}
		if (mount.SubPath != "") != hasSubPath {
			t.Errorf("%s mount %q subPath = %q, want present = %t", container.Name, path, mount.SubPath, hasSubPath)
		}
		return
	}
	t.Errorf("%s has no mount at %q", container.Name, path)
}

func assertNoVolumeMount(t *testing.T, container containerManifest, path string) {
	t.Helper()
	for _, mount := range container.VolumeMounts {
		if mount.MountPath == path || strings.HasPrefix(mount.MountPath, path) {
			t.Errorf("%s unexpectedly mounts %q", container.Name, mount.MountPath)
		}
	}
}

func assertSecretEnv(t *testing.T, container containerManifest, name, secretName, key string) {
	t.Helper()
	for _, env := range container.Env {
		if env.Name != name {
			continue
		}
		if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
			t.Errorf("%s env %q has no SecretKeyRef", container.Name, name)
			return
		}
		ref := env.ValueFrom.SecretKeyRef
		if ref.Name != secretName || ref.Key != key {
			t.Errorf("%s env %q SecretKeyRef = %#v, want name=%q key=%q", container.Name, name, ref, secretName, key)
		}
		return
	}
	t.Errorf("%s has no env %q", container.Name, name)
}

func assertNoEnv(t *testing.T, container containerManifest, name string) {
	t.Helper()
	for _, env := range container.Env {
		if env.Name == name {
			t.Errorf("%s unexpectedly has env %q", container.Name, name)
		}
	}
}

func assertLiteralEnv(t *testing.T, container containerManifest, name, value string) {
	t.Helper()
	for _, env := range container.Env {
		if env.Name == name {
			if env.Value != value || env.ValueFrom != nil {
				t.Errorf("%s env %q = %#v, want literal %q", container.Name, name, env, value)
			}
			return
		}
	}
	t.Errorf("%s has no env %q", container.Name, name)
}

func findHermesSkill(t *testing.T, data map[string]string) string {
	t.Helper()
	for key, value := range data {
		if key != "config.yaml" && strings.Contains(strings.ToLower(value), "task create") {
			return value
		}
	}
	t.Fatalf("ConfigMap has no Hermes skill: %#v", data)
	return ""
}

func webhookSecretItems(deployment deploymentManifest) (map[string]string, bool) {
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Name != "webhook-credentials" || volume.Secret == nil {
			continue
		}
		items := make(map[string]string, len(volume.Secret.Items))
		for _, item := range volume.Secret.Items {
			items[item.Key] = item.Path
		}
		return items, true
	}
	return nil, false
}

func assertWebhookSecretMounts(t *testing.T, deployment deploymentManifest, secretName string, want map[string]string) {
	t.Helper()
	mounted := false
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Secret == nil || volume.Secret.SecretName != secretName {
			continue
		}
		mounted = true
		mountFound := false
		for _, mount := range deployment.Spec.Template.Spec.Containers[0].VolumeMounts {
			if mount.Name != volume.Name {
				continue
			}
			mountFound = true
			if mount.MountPath != "/run/secrets/webhooks" || !mount.ReadOnly {
				t.Errorf("webhook volume mount = %#v, want read-only /run/secrets/webhooks", mount)
			}
		}
		if !mountFound {
			t.Errorf("webhook volume %q has no matching mount", volume.Name)
		}
		if len(volume.Secret.Items) != len(want) {
			t.Fatalf("webhook Secret items = %#v, want %d items", volume.Secret.Items, len(want))
		}
		for _, item := range volume.Secret.Items {
			if got, ok := want[item.Key]; !ok || item.Path != got {
				t.Errorf("webhook Secret item = %#v, want key/path from %#v", item, want)
			}
			delete(want, item.Key)
		}
	}
	if !mounted {
		t.Fatalf("webhook Secret %q was not mounted", secretName)
	}
	for key := range want {
		t.Errorf("webhook Secret key %q was not mounted", key)
	}
}

func parseConfigMap(t *testing.T, rendered string) configMapManifest {
	t.Helper()
	decoder := yaml.NewDecoder(strings.NewReader(rendered))
	for {
		var document configMapManifest
		err := decoder.Decode(&document)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode Helm output: %v", err)
		}
		if document.Kind == "ConfigMap" {
			return document
		}
	}
	t.Fatal("Helm output did not contain a ConfigMap")
	return configMapManifest{}
}

func assertNoIngress(t *testing.T, rendered string) {
	t.Helper()
	decoder := yaml.NewDecoder(strings.NewReader(rendered))
	for {
		var document struct {
			Kind string `yaml:"kind"`
		}
		err := decoder.Decode(&document)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			t.Fatalf("decode Helm output: %v", err)
		}
		if document.Kind == "Ingress" {
			t.Fatal("Helm rendered an Ingress")
		}
	}
}

func parseServices(t *testing.T, rendered string) map[string][]int {
	t.Helper()
	services := make(map[string][]int)
	decoder := yaml.NewDecoder(strings.NewReader(rendered))
	for {
		var document serviceManifest
		err := decoder.Decode(&document)
		if errors.Is(err, io.EOF) {
			return services
		}
		if err != nil {
			t.Fatalf("decode Helm output: %v", err)
		}
		if document.Kind != "Service" {
			continue
		}
		for _, port := range document.Spec.Ports {
			services[document.Metadata.Name] = append(services[document.Metadata.Name], port.Port)
		}
	}
}

func parseControllerNetworkPolicy(t *testing.T, rendered string) networkPolicyManifest {
	t.Helper()
	decoder := yaml.NewDecoder(strings.NewReader(rendered))
	for {
		var document networkPolicyManifest
		err := decoder.Decode(&document)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode Helm output: %v", err)
		}
		if document.Kind == "NetworkPolicy" && document.Metadata.Name == "simpleswe" {
			return document
		}
	}
	t.Fatal("Helm output did not contain the controller NetworkPolicy")
	return networkPolicyManifest{}
}

func renderHelm(t *testing.T, values string) string {
	t.Helper()
	output, err := renderHelmError(t, values)
	if err != nil {
		t.Fatalf("helm template rejected values: %v\n%s", err, output)
	}
	return output
}

func renderHelmError(t *testing.T, values string) (string, error) {
	t.Helper()
	valuesPath := filepath.Join(t.TempDir(), "values.yaml")
	if err := os.WriteFile(valuesPath, []byte(values), 0o600); err != nil {
		t.Fatalf("write Helm values: %v", err)
	}
	chartPath, err := os.Getwd()
	if err != nil {
		t.Fatalf("get Helm chart path: %v", err)
	}
	command := exec.Command("helm", "template", "simpleswe", chartPath, "--values", valuesPath, "--set", "image.tag=test")
	output, err := command.CombinedOutput()
	return string(output), err
}

type deploymentManifest struct {
	Kind string `yaml:"kind"`
	Spec struct {
		Template struct {
			Spec struct {
				Containers []containerManifest `yaml:"containers"`
				Volumes    []volumeManifest    `yaml:"volumes"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

type containerManifest struct {
	Name    string        `yaml:"name"`
	Image   string        `yaml:"image"`
	Command []string      `yaml:"command"`
	Args    []string      `yaml:"args"`
	Env     []envManifest `yaml:"env"`
	Ports   []struct {
		Name          string `yaml:"name"`
		ContainerPort int    `yaml:"containerPort"`
	} `yaml:"ports"`
	VolumeMounts    []volumeMountManifest `yaml:"volumeMounts"`
	SecurityContext struct {
		RunAsNonRoot             bool  `yaml:"runAsNonRoot"`
		AllowPrivilegeEscalation *bool `yaml:"allowPrivilegeEscalation"`
		ReadOnlyRootFilesystem   bool  `yaml:"readOnlyRootFilesystem"`
	} `yaml:"securityContext"`
}

type volumeMountManifest struct {
	Name      string `yaml:"name"`
	MountPath string `yaml:"mountPath"`
	SubPath   string `yaml:"subPath"`
	ReadOnly  bool   `yaml:"readOnly"`
}

type envManifest struct {
	Name      string `yaml:"name"`
	Value     string `yaml:"value"`
	ValueFrom *struct {
		SecretKeyRef *struct {
			Name string `yaml:"name"`
			Key  string `yaml:"key"`
		} `yaml:"secretKeyRef"`
	} `yaml:"valueFrom"`
}

type volumeManifest struct {
	Name   string `yaml:"name"`
	Secret *struct {
		SecretName string `yaml:"secretName"`
		Items      []struct {
			Key  string `yaml:"key"`
			Path string `yaml:"path"`
		} `yaml:"items"`
	} `yaml:"secret"`
}

type roleManifest struct {
	Kind  string     `yaml:"kind"`
	Rules []roleRule `yaml:"rules"`
}

type roleRule struct {
	APIGroups []string `yaml:"apiGroups"`
	Resources []string `yaml:"resources"`
	Verbs     []string `yaml:"verbs"`
}

type configMapManifest struct {
	Kind string            `yaml:"kind"`
	Data map[string]string `yaml:"data"`
}

type serviceManifest struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Ports []struct {
			Port int `yaml:"port"`
		} `yaml:"ports"`
	} `yaml:"spec"`
}

type networkPolicyManifest struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Ingress []struct {
			From []struct {
				NamespaceSelector struct {
					MatchLabels map[string]string `yaml:"matchLabels"`
				} `yaml:"namespaceSelector"`
			} `yaml:"from"`
			Ports []struct {
				Port int `yaml:"port"`
			} `yaml:"ports"`
		} `yaml:"ingress"`
	} `yaml:"spec"`
}

func parseDeployment(t *testing.T, rendered string) deploymentManifest {
	t.Helper()
	decoder := yaml.NewDecoder(strings.NewReader(rendered))
	for {
		var document deploymentManifest
		err := decoder.Decode(&document)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode Helm output: %v", err)
		}
		if document.Kind == "Deployment" {
			return document
		}
	}
	t.Fatal("Helm output did not contain a Deployment")
	return deploymentManifest{}
}
