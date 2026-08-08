package redaction

import (
	"encoding/json"
	"sort"
	"strings"
)

const Placeholder = "[REDACTED]"

const (
	jsonNestingLevels      = 2
	minPartialSecretLength = 4
)

// ExpandSecrets returns whole secret values, their JSON-escaped forms, and
// nontrivial lines from multiline values. Longer values are replaced first so
// overlapping values do not expose a suffix.
func ExpandSecrets(values []string) []string {
	seen := make(map[string]struct{})
	add := func(value string) {
		if value == "" {
			return
		}
		seen[value] = struct{}{}
		frontier := []string{value}
		for range jsonNestingLevels {
			next := make([]string, 0, len(frontier)*2)
			for _, current := range frontier {
				for _, escaped := range jsonEscapedVariants(current) {
					if escaped != "" {
						seen[escaped] = struct{}{}
						next = append(next, escaped)
					}
				}
			}
			frontier = next
		}
	}
	for _, value := range values {
		add(value)
		for _, line := range strings.Split(value, "\n") {
			line = strings.TrimSpace(line)
			if len(line) >= minPartialSecretLength {
				add(line)
			}
		}
	}
	result := make([]string, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return len(result[i]) > len(result[j]) })
	return result
}

func jsonEscapedVariants(value string) []string {
	variants := make([]string, 0, 2)
	add := func(encoded string) {
		if len(encoded) >= 2 {
			variants = append(variants, encoded[1:len(encoded)-1])
		}
	}
	if encoded, err := json.Marshal(value); err == nil {
		add(string(encoded))
	}
	var encoded strings.Builder
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err == nil {
		add(strings.TrimSuffix(encoded.String(), "\n"))
	}
	return variants
}

func Redact(output string, secrets []string) string {
	for _, secret := range secrets {
		if secret != "" {
			output = strings.ReplaceAll(output, secret, Placeholder)
		}
	}
	return output
}
