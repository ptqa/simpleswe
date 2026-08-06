package redaction

import (
	"sort"
	"strings"
)

const Placeholder = "[REDACTED]"

const minPartialSecretLength = 4

// ExpandSecrets returns whole secret values plus nontrivial lines from
// multiline values. Longer values are replaced first so overlapping values do
// not expose a suffix.
func ExpandSecrets(values []string) []string {
	seen := make(map[string]struct{})
	for _, value := range values {
		if value == "" {
			continue
		}
		seen[value] = struct{}{}
		for _, line := range strings.Split(value, "\n") {
			line = strings.TrimSpace(line)
			if len(line) >= minPartialSecretLength {
				seen[line] = struct{}{}
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

func Redact(output string, secrets []string) string {
	for _, secret := range secrets {
		if secret != "" {
			output = strings.ReplaceAll(output, secret, Placeholder)
		}
	}
	return output
}
