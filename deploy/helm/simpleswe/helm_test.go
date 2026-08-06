package helm

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/simpleswe/simpleswe/internal/config"
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
			cfg, err := config.Load(strings.NewReader(configMap.Data["config.yaml"]))
			if err != nil {
				t.Fatalf("load rendered controller config: %v", err)
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
	command := exec.Command("helm", "template", "simpleswe", chartPath, "--values", valuesPath, "--set", "image.tag=test", "--set", "config.slack.disabled=true")
	output, err := command.CombinedOutput()
	return string(output), err
}

type deploymentManifest struct {
	Kind string `yaml:"kind"`
	Spec struct {
		Template struct {
			Spec struct {
				Containers []struct {
					Ports []struct {
						Name          string `yaml:"name"`
						ContainerPort int    `yaml:"containerPort"`
					} `yaml:"ports"`
					VolumeMounts []struct {
						Name      string `yaml:"name"`
						MountPath string `yaml:"mountPath"`
						ReadOnly  bool   `yaml:"readOnly"`
					} `yaml:"volumeMounts"`
				} `yaml:"containers"`
				Volumes []struct {
					Name   string `yaml:"name"`
					Secret *struct {
						SecretName string `yaml:"secretName"`
						Items      []struct {
							Key  string `yaml:"key"`
							Path string `yaml:"path"`
						} `yaml:"items"`
					} `yaml:"secret"`
				} `yaml:"volumes"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
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
