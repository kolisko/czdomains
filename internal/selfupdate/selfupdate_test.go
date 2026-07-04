package selfupdate

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		a    string
		b    string
		want int
	}{
		{a: "v0.2.3", b: "v0.2.4", want: -1},
		{a: "v0.2.10", b: "v0.2.9", want: 1},
		{a: "v1.0.0", b: "v1.0.0", want: 0},
	}
	for _, test := range tests {
		t.Run(test.a+"_"+test.b, func(t *testing.T) {
			got := CompareVersions(test.a, test.b)
			if got != test.want {
				t.Fatalf("CompareVersions(%q, %q)=%d, want %d", test.a, test.b, got, test.want)
			}
		})
	}
}

func TestCheckDetectsOutdatedReleaseVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tag_name":"v0.2.4","html_url":"https://example.test/release","assets":[]}`))
	}))
	defer server.Close()

	got, err := Check(context.Background(), Config{
		CurrentVersion: "v0.2.3",
		APIURL:         server.URL,
		Client:         server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Outdated || got.LatestVersion != "v0.2.4" {
		t.Fatalf("unexpected check result: %+v", got)
	}
}

func TestCheckSkipsDevBuildAsOutdated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tag_name":"v9.9.9","assets":[]}`))
	}))
	defer server.Close()

	got, err := Check(context.Background(), Config{
		CurrentVersion: "dev",
		APIURL:         server.URL,
		Client:         server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Outdated {
		t.Fatalf("dev build should not be marked outdated: %+v", got)
	}
}

func TestUpdateReplacesExecutableAndLeavesNoTempFile(t *testing.T) {
	dir := t.TempDir()
	exePath := filepath.Join(dir, "czdomains")
	if err := os.WriteFile(exePath, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := []byte("new")
	digest := sha256.Sum256(payload)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest":
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"tag_name":"v0.2.4","assets":[{"name":"czdomains-linux-amd64","browser_download_url":"%s/asset","digest":"sha256:%x","size":%d}]}`, serverURL(r), digest, len(payload))
		case "/asset":
			_, _ = w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	err := Update(context.Background(), Config{
		CurrentVersion: "v0.2.3",
		APIURL:         server.URL + "/latest",
		ExecutablePath: exePath,
		GOOS:           "linux",
		GOARCH:         "amd64",
		Client:         server.Client(),
	}, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(exePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Fatalf("updated executable=%q, want new", string(got))
	}
	leftovers, err := filepath.Glob(filepath.Join(dir, ".czdomains.update.*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("unexpected temp leftovers: %v", leftovers)
	}
}

func TestUpdateRejectsDigestMismatchAndCleansTemp(t *testing.T) {
	dir := t.TempDir()
	exePath := filepath.Join(dir, "czdomains")
	if err := os.WriteFile(exePath, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := []byte("new")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest":
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"tag_name":"v0.2.4","assets":[{"name":"czdomains-linux-amd64","browser_download_url":"%s/asset","digest":"sha256:0000","size":%d}]}`, serverURL(r), len(payload))
		case "/asset":
			_, _ = w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	err := Update(context.Background(), Config{
		CurrentVersion: "v0.2.3",
		APIURL:         server.URL + "/latest",
		ExecutablePath: exePath,
		GOOS:           "linux",
		GOARCH:         "amd64",
		Client:         server.Client(),
	}, os.Stderr)
	if err == nil {
		t.Fatal("expected digest mismatch")
	}
	got, err := os.ReadFile(exePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old" {
		t.Fatalf("executable changed after failed update: %q", string(got))
	}
	leftovers, err := filepath.Glob(filepath.Join(dir, ".czdomains.update.*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("unexpected temp leftovers: %v", leftovers)
	}
}

func serverURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}
