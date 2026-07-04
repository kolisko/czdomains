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
		Domain:    "example.cz",
		Source:    "commoncrawl",
		IndexFile: "cc-index/collections/CC-MAIN-2026-25/indexes/cdx-00042.gz",
		Block:     123,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.AddDomain(ctx, discovery.FoundDomain{
		Domain: "example.cz",
		Source: "crtsh",
		Block:  -1,
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

func TestStoreTracksCompletedBlocks(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "domains.sqlite"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	block := discovery.CrawlBlock{Source: "commoncrawl", Crawl: "CC-MAIN-2026-25", IndexFile: "cc-index/collections/CC-MAIN-2026-25/indexes/cdx-00042.gz", Block: 7}
	complete, err := store.BlockComplete(ctx, block)
	if err != nil {
		t.Fatal(err)
	}
	if complete {
		t.Fatal("new block should not be complete")
	}
	if err := store.MarkBlockStarted(ctx, block); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkBlockCompleted(ctx, block); err != nil {
		t.Fatal(err)
	}
	complete, err = store.BlockComplete(ctx, block)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatal("block should be complete")
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
		if _, err := store.AddDomain(ctx, discovery.FoundDomain{Domain: domain, Source: "test", Block: -1}); err != nil {
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
