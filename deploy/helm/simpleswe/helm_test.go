package helm

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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
					VolumeMounts []struct {
						Name      string `yaml:"name"`
						MountPath string `yaml:"mountPath"`
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
