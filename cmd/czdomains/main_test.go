package main

import (
	"os"
	"testing"

	"czdomains/internal/discovery"
)

func TestWriteDiscoveredWritesOnlyDomains(t *testing.T) {
	path := t.TempDir() + "/discovered.txt"
	err := writeDiscovered(path, []discovery.Result{
		{Domain: "example.cz", Source: "commoncrawl"},
		{Domain: "seznam.cz", Source: "crtsh"},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "example.cz\nseznam.cz\n"
	if string(got) != want {
		t.Fatalf("got %q, want %q", string(got), want)
	}
}
