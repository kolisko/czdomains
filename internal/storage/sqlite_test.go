package storage

import (
	"context"
	"path/filepath"
	"testing"

	"czdomains/internal/discovery"
)

func TestStoreDeduplicatesDomains(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "domains.sqlite"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	first, err := store.AddDomain(ctx, discovery.FoundDomain{
		Domain:   "example.cz",
		Source:   "commoncrawl",
		IndexURL: "https://index.commoncrawl.org/test",
		Page:     0,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.AddDomain(ctx, discovery.FoundDomain{
		Domain: "example.cz",
		Source: "crtsh",
		Page:   -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	count, err := store.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !first || second || count != 1 {
		t.Fatalf("first=%v second=%v count=%d", first, second, count)
	}
}

func TestStoreTracksCompletedPages(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "domains.sqlite"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	page := discovery.CrawlPage{Source: "commoncrawl", IndexURL: "https://index.commoncrawl.org/test", Page: 7}
	complete, err := store.PageComplete(ctx, page)
	if err != nil {
		t.Fatal(err)
	}
	if complete {
		t.Fatal("new page should not be complete")
	}
	if err := store.MarkPageStarted(ctx, page); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPageCompleted(ctx, page); err != nil {
		t.Fatal(err)
	}
	complete, err = store.PageComplete(ctx, page)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatal("page should be complete")
	}
}

func TestStoreForEachDomainIsSorted(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "domains.sqlite"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	for _, domain := range []string{"z.cz", "a.cz"} {
		if _, err := store.AddDomain(ctx, discovery.FoundDomain{Domain: domain, Source: "test", Page: -1}); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	if err := store.ForEachDomain(ctx, func(domain string) error {
		got = append(got, domain)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "a.cz" || got[1] != "z.cz" {
		t.Fatalf("unexpected domains: %v", got)
	}
}
