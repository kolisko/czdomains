package domainutil

import (
	"errors"
	"net"
	"net/url"
	"strings"
)

var ErrNotCZDomain = errors.New("not a .cz domain")

// FromURL extracts a registrable .cz domain from a URL or hostname.
func FromURL(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", ErrNotCZDomain
	}

	host := value
	if strings.Contains(value, "://") {
		parsed, err := url.Parse(value)
		if err != nil {
			return "", err
		}
		host = parsed.Host
	} else if strings.HasPrefix(value, "//") {
		parsed, err := url.Parse(value)
		if err != nil {
			return "", err
		}
		host = parsed.Host
	}

	return FromHost(host)
}

// FromHost reduces a hostname to its registrable .cz domain.
func FromHost(host string) (string, error) {
	host = strings.TrimSpace(strings.ToLower(host))
	host = strings.TrimSuffix(host, ".")
	if host == "" {
		return "", ErrNotCZDomain
	}

	if strings.Contains(host, "@") {
		if parsed, err := url.Parse("//" + host); err == nil {
			host = parsed.Host
		}
	}

	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	host = strings.TrimPrefix(host, "www.")

	labels := strings.Split(host, ".")
	clean := labels[:0]
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label != "" {
			clean = append(clean, label)
		}
	}
	if len(clean) < 2 || clean[len(clean)-1] != "cz" {
		return "", ErrNotCZDomain
	}

	secondLevel := clean[len(clean)-2]
	if secondLevel == "" || strings.Contains(secondLevel, "_") {
		return "", ErrNotCZDomain
	}

	return secondLevel + ".cz", nil
}

func Dedupe(domains []string) []string {
	seen := make(map[string]struct{}, len(domains))
	out := make([]string, 0, len(domains))
	for _, domain := range domains {
		normalized, err := FromURL(domain)
		if err != nil {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}
