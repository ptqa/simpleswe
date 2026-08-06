package config

import (
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestLoadRejectsReaderAndDocumentErrors(t *testing.T) {
	for name, input := range map[string]string{
		"empty":            "",
		"malformed":        ":",
		"multiple":         "---\nrepositories: []\n---\nrepositories: []\n",
		"second malformed": "---\nrepositories: []\n---\n:",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(strings.NewReader(input)); err == nil {
				t.Fatal("Load accepted invalid YAML input")
			}
		})
	}
	if _, err := Load(nil); err == nil {
		t.Fatal("Load accepted a nil reader")
	}
}

func TestDurationAndQuantityYAMLParsing(t *testing.T) {
	for name, input := range map[string]string{
		"duration non-string": "1",
		"duration invalid":    "not-a-duration",
	} {
		t.Run(name, func(t *testing.T) {
			var duration Duration
			if err := yaml.Unmarshal([]byte(input), &duration); err == nil {
				t.Fatal("invalid duration accepted")
			}
		})
	}
	for name, input := range map[string]string{
		"mapping":       "{}",
		"invalid value": "not-a-quantity",
	} {
		t.Run(name, func(t *testing.T) {
			var quantity Quantity
			if err := yaml.Unmarshal([]byte(input), &quantity); err == nil {
				t.Fatal("invalid quantity accepted")
			}
		})
	}
	for name, input := range map[string]string{
		"integer": "2",
		"float":   "1.5",
	} {
		t.Run(name, func(t *testing.T) {
			var quantity Quantity
			if err := yaml.Unmarshal([]byte(input), &quantity); err != nil {
				t.Fatalf("valid quantity rejected: %v", err)
			}
		})
	}
}

func TestQuantitySign(t *testing.T) {
	for value, want := range map[Quantity]int{
		"-1":  -1,
		"0":   0,
		"+0":  0,
		"0.0": 0,
		"1":   1,
	} {
		if got := value.Sign(); got != want {
			t.Errorf("Quantity(%q).Sign() = %d, want %d", value, got, want)
		}
	}
}

func validConfig() Config {
	return Config{
		Controller: ControllerConfig{
			ListenAddress:  ":8080",
			Namespace:      "simpleswe",
			Deadline:       time.Minute,
			MaxFixAttempts: 1,
		},
		Worker: WorkerConfig{
			Command:      "opencode",
			BranchPrefix: "simpleswe/",
		},
		Bitbucket: BitbucketConfig{BaseURL: "https://bitbucket.org"},
	}
}

func TestConfigValidateTopLevelFields(t *testing.T) {
	for name, edit := range map[string]func(*Config){
		"listen address": func(c *Config) { c.Controller.ListenAddress = "" },
		"namespace":      func(c *Config) { c.Controller.Namespace = "INVALID" },
		"deadline":       func(c *Config) { c.Controller.Deadline = 0 },
		"max fix attempts": func(c *Config) {
			c.Controller.MaxFixAttempts = -1
		},
		"worker command":       func(c *Config) { c.Worker.Command = " " },
		"worker branch prefix": func(c *Config) { c.Worker.BranchPrefix = " " },
		"slack bot token": func(c *Config) {
			c.Slack.BotToken = SecretSource{File: "relative"}
		},
		"slack app token": func(c *Config) {
			c.Slack.AppToken = SecretSource{Env: "bad-name"}
		},
		"bitbucket URL": func(c *Config) { c.Bitbucket.BaseURL = "http://example.com" },
		"worker resources": func(c *Config) {
			c.Worker.Resources.Requests = ResourceList{"cpu": Quantity("-1")}
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			edit(&cfg)
			if err := cfg.validate(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	if err := validConfig().validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestValidateWorkerBranches(t *testing.T) {
	for name, edit := range map[string]func(*WorkerConfig){
		"negative deadline": func(w *WorkerConfig) { w.Deadline = Duration(-time.Second) },
		"negative limit": func(w *WorkerConfig) {
			w.Resources.Limits = ResourceList{"memory": Quantity("-1")}
		},
		"mount identity": func(w *WorkerConfig) {
			w.Mounts = []Mount{{Name: " ", MountPath: "/cache"}}
		},
		"mount secret": func(w *WorkerConfig) {
			w.Mounts = []Mount{{Name: "cache", MountPath: "/cache", Secret: &SecretMount{SecretName: "Bad"}}}
		},
		"mount config map": func(w *WorkerConfig) {
			w.Mounts = []Mount{{Name: "cache", MountPath: "/cache", ConfigMap: &ConfigMapMount{Name: "Bad"}}}
		},
		"mount source": func(w *WorkerConfig) {
			w.Mounts = []Mount{{Name: "cache", MountPath: "/cache"}}
		},
		"mount multiple sources": func(w *WorkerConfig) {
			w.Mounts = []Mount{{Name: "cache", MountPath: "/cache", Secret: &SecretMount{SecretName: "secret"}, EmptyDir: &EmptyDir{}}}
		},
		"negative emptyDir size": func(w *WorkerConfig) {
			size := Quantity("-1")
			w.Mounts = []Mount{{Name: "cache", MountPath: "/cache", EmptyDir: &EmptyDir{SizeLimit: &size}}}
		},
		"mounted secret": func(w *WorkerConfig) {
			w.MountedSecrets = []NamedMount{{Name: "Bad", MountPath: "relative"}}
		},
		"mounted config map": func(w *WorkerConfig) {
			w.MountedConfigMaps = []NamedMount{{Name: "Bad", MountPath: "relative"}}
		},
		"env name":     func(w *WorkerConfig) { w.Env = []Env{{}} },
		"reserved env": func(w *WorkerConfig) { w.Env = []Env{{Name: "SIMPLESWE_SECRET_PATHS"}} },
		"env sources": func(w *WorkerConfig) {
			w.Env = []Env{{Name: "TOKEN", Value: "value", Secret: &KeyRef{Name: "secret", Key: "token"}}}
		},
		"env secret ref": func(w *WorkerConfig) {
			w.Env = []Env{{Name: "TOKEN", Secret: &KeyRef{Name: "Bad"}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			worker := WorkerConfig{}
			edit(&worker)
			if err := validateWorker(worker, "worker"); err == nil {
				t.Fatal("invalid worker accepted")
			}
		})
	}

	valid := WorkerConfig{
		Mounts:            []Mount{{Name: "cache", MountPath: "/cache", EmptyDir: &EmptyDir{}}},
		MountedSecrets:    []NamedMount{{Name: "secret", MountPath: "/run/secret"}},
		MountedConfigMaps: []NamedMount{{Name: "config", MountPath: "/etc/config"}},
		Env: []Env{
			{Name: "VALUE", Value: "value"},
			{Name: "SECRET", Secret: &KeyRef{Name: "secret", Key: "token"}},
			{Name: "CONFIG", ConfigMap: &KeyRef{Name: "config", Key: "value"}},
		},
	}
	if err := validateWorker(valid, "worker"); err != nil {
		t.Fatalf("valid worker rejected: %v", err)
	}
}

func TestValidateSecretSourceAndResources(t *testing.T) {
	for name, source := range map[string]SecretSource{
		"both":               {File: "/run/token", Env: "TOKEN"},
		"relative file":      {File: "token"},
		"bad env first rune": {Env: "1TOKEN"},
		"bad env character":  {Env: "TOKEN-NAME"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateSecretSource(source, "secret"); err == nil {
				t.Fatal("invalid secret source accepted")
			}
		})
	}
	for name, source := range map[string]SecretSource{
		"empty":         {},
		"absolute file": {File: "/run/token"},
		"valid env":     {Env: "TOKEN_1"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateSecretSource(source, "secret"); err != nil {
				t.Fatalf("valid secret source rejected: %v", err)
			}
		})
	}

	if err := validateResources(ResourceRequirements{Requests: ResourceList{"cpu": "1"}, Limits: ResourceList{"memory": "2"}}, "resources"); err != nil {
		t.Fatalf("valid resources rejected: %v", err)
	}
	if err := validateResources(ResourceRequirements{Requests: ResourceList{"cpu": "-1"}}, "resources"); err == nil {
		t.Fatal("negative resource request accepted")
	}
}

func TestValidationHelpers(t *testing.T) {
	for host, want := range map[string]bool{
		"localhost": true,
		"127.0.0.1": true,
		"::1":       true,
		"192.0.2.1": false,
	} {
		if got := loopbackHost(host); got != want {
			t.Errorf("loopbackHost(%q) = %t, want %t", host, got, want)
		}
	}
	for name, want := range map[string]bool{
		"":                      false,
		"a..b":                  false,
		"-name":                 false,
		"name-":                 false,
		"UPPER":                 false,
		strings.Repeat("a", 64): false,
		"valid.name-1":          true,
	} {
		if got := validDNSName(name); got != want {
			t.Errorf("validDNSName(%q) = %t, want %t", name, got, want)
		}
	}
}

func TestYAMLUnmarshalStrictErrors(t *testing.T) {
	for name, input := range map[string]string{
		"controller unknown field": "unexpected: true\n",
		"controller bad deadline":  "deadline: not-a-duration\n",
	} {
		t.Run(name, func(t *testing.T) {
			var controller ControllerConfig
			if err := yaml.Unmarshal([]byte(input), &controller); err == nil {
				t.Fatal("invalid controller accepted")
			}
		})
	}
	var repository RepositoryConfig
	if err := yaml.Unmarshal([]byte("unexpected: true\n"), &repository); err == nil {
		t.Fatal("unknown repository field accepted")
	}
	var repositories RepositoryConfigs
	if err := yaml.Unmarshal([]byte("scalar\n"), &repositories); err == nil {
		t.Fatal("scalar repository registry accepted")
	}
}
