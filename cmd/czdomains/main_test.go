package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"czdomains/internal/discovery"
	"czdomains/internal/storage"
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

func TestExportDomainsWritesOnlyDomains(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.Open(filepath.Join(dir, "domains.sqlite"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	for _, domain := range []string{"seznam.cz", "example.cz"} {
		_, err := store.AddDomain(ctx, discovery.FoundDomain{Domain: domain, Source: "test", Block: -1})
		if err != nil {
			t.Fatal(err)
		}
	}

	path := filepath.Join(dir, "discovered.txt")
	count, err := exportDomainsToFile(ctx, store, path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 || string(got) != "example.cz\nseznam.cz\n" {
		t.Fatalf("count=%d output=%q", count, string(got))
	}
}

func TestExportDomainsWritesToWriter(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.Open(filepath.Join(dir, "domains.sqlite"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	for _, domain := range []string{"seznam.cz", "example.cz"} {
		_, err := store.AddDomain(ctx, discovery.FoundDomain{Domain: domain, Source: "test", Block: -1})
		if err != nil {
			t.Fatal(err)
		}
	}

	var out bytes.Buffer
	count, err := exportDomains(ctx, store, &out)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 || out.String() != "example.cz\nseznam.cz\n" {
		t.Fatalf("count=%d output=%q", count, out.String())
	}
}
