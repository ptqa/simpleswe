package forge

import (
	"errors"
	"testing"
)

func TestValidateTargetRequiresCompleteSafeRouteIdentity(t *testing.T) {
	valid := Target{
		Provider: ProviderGitHub, BaseURL: "https://api.github.com",
		Owner: "Acme", Repository: "Widget", CredentialsSecret: "widget-github",
	}
	if err := ValidateTarget(valid); err != nil {
		t.Fatalf("ValidateTarget(valid) error = %v", err)
	}

	tests := map[string]func(*Target){
		"provider":          func(target *Target) { target.Provider = "gitlab" },
		"base URL":          func(target *Target) { target.BaseURL = "http://forge.example" },
		"owner":             func(target *Target) { target.Owner = "" },
		"repository":        func(target *Target) { target.Repository = " widget" },
		"credential Secret": func(target *Target) { target.CredentialsSecret = "../secret" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			target := valid
			mutate(&target)
			if err := ValidateTarget(target); err == nil {
				t.Fatal("ValidateTarget accepted incomplete or unsafe target")
			}
		})
	}
}

func TestMarkedPermanentErrorClassification(t *testing.T) {
	err := MarkPermanent(errors.New("route removed"))
	if !IsPermanent(err) {
		t.Fatalf("IsPermanent(%v) = false", err)
	}
}
