package domainutil

import "testing"

func TestFromURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{"plain", "seznam.cz", "seznam.cz", true},
		{"www", "https://www.seznam.cz/path", "seznam.cz", true},
		{"subdomain", "https://mail.firma.example.cz/login", "example.cz", true},
		{"port", "http://sub.example.cz:8080/x", "example.cz", true},
		{"trailing dot", "example.cz.", "example.cz", true},
		{"not cz", "example.com", "", false},
		{"empty", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FromURL(tt.in)
			if tt.ok && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tt.ok && err == nil {
				t.Fatalf("expected error, got %q", got)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDedupe(t *testing.T) {
	got := Dedupe([]string{
		"https://www.seznam.cz/",
		"seznam.cz",
		"mail.example.cz",
		"example.com",
		"www.example.cz",
	})
	want := []string{"seznam.cz", "example.cz"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
