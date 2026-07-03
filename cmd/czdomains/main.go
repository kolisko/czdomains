package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"czdomains/internal/discovery"
	"czdomains/internal/domainutil"
	"czdomains/internal/enrich"
	"czdomains/internal/output"
	"czdomains/internal/rdap"
	"czdomains/internal/storage"
)

const userAgent = "czdomains/1.0 (+https://github.com/)"

type domainInput struct {
	Domain string
	Source string
}

func main() {
	if err := run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 2 {
		usage()
		return errors.New("missing command")
	}

	switch args[1] {
	case "discover":
		return runDiscover(args[2:])
	case "enrich":
		return runEnrich(args[2:])
	case "export":
		return runExport(args[2:])
	case "run":
		return runAll(args[2:])
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[1])
	}
}

func runDiscover(args []string) error {
	flags := flag.NewFlagSet("discover", flag.ContinueOnError)
	limit := flags.Int("limit", 10000, "maximum number of unique domains to discover")
	dbPath := flags.String("db", "czdomains.sqlite", "SQLite discovery database path")
	outPath := flags.String("out", "", "optional TXT output path for newly discovered domains")
	sources := flags.String("source", "commoncrawl", "comma-separated sources: commoncrawl,crtsh")
	ccIndex := flags.String("cc-index", "latest", "Common Crawl index id, URL, or latest")
	ccIndexCount := flags.Int("cc-index-count", 0, "number of recent Common Crawl indexes to scan when --cc-index=latest; 0 scans all")
	fresh := flags.Bool("fresh", false, "start from a fresh database and truncate --out if set")
	timeout := flags.Duration("timeout", 60*time.Second, "HTTP timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}

	count, err := discoverToSQLite(context.Background(), *dbPath, *outPath, *fresh, *limit, *sources, *ccIndex, *ccIndexCount, *timeout)
	if err != nil && count == 0 {
		return err
	}
	fmt.Fprintf(os.Stderr, "database contains %d domains in %s\n", count, *dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "discovery completed with warning: %v\n", err)
	}
	return nil
}

func runEnrich(args []string) error {
	flags := flag.NewFlagSet("enrich", flag.ContinueOnError)
	inputPath := flags.String("input", "discovered.txt", "input domain list path")
	dbPath := flags.String("db", "", "SQLite discovery database path; when set, --input is ignored")
	csvPath := flags.String("csv", "domains.csv", "CSV output path")
	jsonlPath := flags.String("jsonl", "domains.jsonl", "JSONL output path")
	delay := flags.Duration("delay", 200*time.Millisecond, "delay between RDAP requests")
	timeout := flags.Duration("timeout", 30*time.Second, "HTTP timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}

	if *dbPath != "" {
		store, err := storage.Open(*dbPath, storage.Options{})
		if err != nil {
			return err
		}
		defer store.Close()
		count, err := store.Count(context.Background())
		if err != nil {
			return err
		}
		return enrichEach(context.Background(), func(yield func(domainInput) error) error {
			return store.ForEachDomain(context.Background(), func(domain string) error {
				return yield(domainInput{Domain: domain, Source: "db"})
			})
		}, count, *csvPath, *jsonlPath, *delay, *timeout)
	}

	inputs, err := readDomainInputs(*inputPath)
	if err != nil {
		return err
	}
	return enrichDomains(context.Background(), inputs, *csvPath, *jsonlPath, *delay, *timeout)
}

func runExport(args []string) error {
	flags := flag.NewFlagSet("export", flag.ContinueOnError)
	dbPath := flags.String("db", "czdomains.sqlite", "SQLite discovery database path")
	outPath := flags.String("out", "", "TXT output path; defaults to stdout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	store, err := storage.Open(*dbPath, storage.Options{})
	if err != nil {
		return err
	}
	defer store.Close()

	if *outPath == "" {
		_, err := exportDomains(context.Background(), store, os.Stdout)
		return err
	}

	count, err := exportDomainsToFile(context.Background(), store, *outPath)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "exported %d domains to %s\n", count, *outPath)
	return nil
}

func runAll(args []string) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	limit := flags.Int("limit", 10000, "maximum number of unique domains to discover")
	dbPath := flags.String("db", "czdomains.sqlite", "SQLite discovery database path")
	csvPath := flags.String("csv", "domains.csv", "CSV output path")
	jsonlPath := flags.String("jsonl", "domains.jsonl", "JSONL output path")
	sources := flags.String("source", "commoncrawl", "comma-separated sources: commoncrawl,crtsh")
	ccIndex := flags.String("cc-index", "latest", "Common Crawl index id, URL, or latest")
	ccIndexCount := flags.Int("cc-index-count", 0, "number of recent Common Crawl indexes to scan when --cc-index=latest; 0 scans all")
	fresh := flags.Bool("fresh", false, "start from a fresh database")
	delay := flags.Duration("delay", 200*time.Millisecond, "delay between RDAP requests")
	timeout := flags.Duration("timeout", 60*time.Second, "HTTP timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}

	count, err := discoverToSQLite(context.Background(), *dbPath, "", *fresh, *limit, *sources, *ccIndex, *ccIndexCount, *timeout)
	if err != nil && count == 0 {
		return err
	}
	store, openErr := storage.Open(*dbPath, storage.Options{})
	if openErr != nil {
		return openErr
	}
	defer store.Close()
	if enrichErr := enrichEach(context.Background(), func(yield func(domainInput) error) error {
		return store.ForEachDomain(context.Background(), func(domain string) error {
			return yield(domainInput{Domain: domain, Source: "db"})
		})
	}, count, *csvPath, *jsonlPath, *delay, *timeout); enrichErr != nil {
		return enrichErr
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "discovery completed with warning: %v\n", err)
	}
	return nil
}

func discoverToSQLite(ctx context.Context, dbPath string, outPath string, fresh bool, limit int, sources string, ccIndex string, ccIndexCount int, timeout time.Duration) (int, error) {
	store, err := storage.Open(dbPath, storage.Options{Fresh: fresh})
	if err != nil {
		return 0, err
	}
	defer store.Close()

	sink, err := newOptionalTXTSink(store, outPath, fresh)
	if err != nil {
		return 0, err
	}
	defer sink.Close()

	client := &http.Client{Timeout: timeout}
	discoverer := discovery.New(client, discovery.Config{
		Limit:        limit,
		Sources:      splitCSV(sources),
		CCIndex:      ccIndex,
		CCIndexCount: ccIndexCount,
		UserAgent:    userAgent,
		PageTracker:  store,
		Progress: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, format, args...)
		},
	})
	err = discoverer.DiscoverTo(ctx, sink)
	if flushErr := sink.Flush(); flushErr != nil && err == nil {
		err = flushErr
	}
	count, countErr := store.Count(ctx)
	if countErr != nil && err == nil {
		err = countErr
	}
	return count, err
}

func discoverDomains(ctx context.Context, limit int, sources string, ccIndex string, ccIndexCount int, timeout time.Duration) ([]discovery.Result, error) {
	client := &http.Client{Timeout: timeout}
	discoverer := discovery.New(client, discovery.Config{
		Limit:        limit,
		Sources:      splitCSV(sources),
		CCIndex:      ccIndex,
		CCIndexCount: ccIndexCount,
		UserAgent:    userAgent,
		Progress: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, format, args...)
		},
	})
	return discoverer.Discover(ctx)
}

func enrichDomains(ctx context.Context, inputs []domainInput, csvPath string, jsonlPath string, delay time.Duration, timeout time.Duration) error {
	return enrichEach(ctx, func(yield func(domainInput) error) error {
		for _, input := range inputs {
			if err := yield(input); err != nil {
				return err
			}
		}
		return nil
	}, len(inputs), csvPath, jsonlPath, delay, timeout)
}

func enrichEach(ctx context.Context, each func(func(domainInput) error) error, total int, csvPath string, jsonlPath string, delay time.Duration, timeout time.Duration) error {
	csvFile, err := os.Create(csvPath)
	if err != nil {
		return err
	}
	defer csvFile.Close()

	jsonlFile, err := os.Create(jsonlPath)
	if err != nil {
		return err
	}
	defer jsonlFile.Close()

	csvWriter := output.NewCSVWriter(csvFile)
	if err := csvWriter.WriteHeader(); err != nil {
		return err
	}
	jsonlWriter := output.NewJSONLWriter(jsonlFile)

	client := rdap.New(&http.Client{Timeout: timeout}, rdap.Config{Delay: delay, UserAgent: userAgent})
	processor := enrich.NewProcessor(client)
	i := 0
	err = each(func(input domainInput) error {
		record := processor.Enrich(ctx, input.Domain, input.Source)
		if err := csvWriter.WriteRecord(record); err != nil {
			return err
		}
		if err := jsonlWriter.WriteRecord(record); err != nil {
			return err
		}
		i++
		if i%100 == 0 {
			fmt.Fprintf(os.Stderr, "enriched %d/%d domains\n", i, total)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := csvWriter.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %d records to %s and %s\n", i, csvPath, jsonlPath)
	return nil
}

func exportDomainsToFile(ctx context.Context, store *storage.Store, outPath string) (int, error) {
	file, err := os.Create(outPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	return exportDomains(ctx, store, file)
}

func exportDomains(ctx context.Context, store *storage.Store, out io.Writer) (int, error) {
	writer := bufio.NewWriter(out)
	count := 0
	err := store.ForEachDomain(ctx, func(domain string) error {
		if _, err := fmt.Fprintln(writer, domain); err != nil {
			return err
		}
		count++
		return nil
	})
	if err != nil {
		return count, err
	}
	if err := writer.Flush(); err != nil {
		return count, err
	}
	return count, nil
}

func writeDiscovered(path string, results []discovery.Result) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, result := range results {
		if _, err := fmt.Fprintf(writer, "%s\n", result.Domain); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func readDomainInputs(path string) ([]domainInput, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var inputs []domainInput
	seen := map[string]struct{}{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.FieldsFunc(line, func(r rune) bool {
			return r == '\t' || r == ','
		})
		if len(fields) == 0 {
			continue
		}
		domain, err := domainutil.FromURL(fields[0])
		if err != nil {
			continue
		}
		if _, ok := seen[domain]; ok {
			continue
		}
		source := "input"
		if len(fields) > 1 && strings.TrimSpace(fields[1]) != "" {
			source = strings.TrimSpace(fields[1])
		}
		inputs = append(inputs, domainInput{Domain: domain, Source: source})
		seen[domain] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return inputs, nil
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func usage() {
	fmt.Fprint(os.Stderr, `czdomains discovers .cz domains from public best-effort sources and enriches them with public CZ.NIC RDAP data.

Usage:
  czdomains discover --limit 10000 --db czdomains.sqlite
  czdomains discover --limit 1000000 --db czdomains.sqlite --out discovered.txt
  czdomains export --db czdomains.sqlite
  czdomains export --db czdomains.sqlite --out discovered.txt
  czdomains enrich --db czdomains.sqlite --csv domains.csv --jsonl domains.jsonl
  czdomains enrich --input discovered.txt --csv domains.csv --jsonl domains.jsonl
  czdomains run --limit 10000 --db czdomains.sqlite --csv domains.csv --jsonl domains.jsonl

Discovery:
  --db       SQLite database for dedupe, checkpoints, and resume
  --out      Optional TXT stream of newly inserted domains only
  --fresh    Delete the old database first and truncate --out if set

Export:
  Without --out, export writes domains to stdout.
  With --out, export writes domains to the given TXT file.

Sources:
  commoncrawl  Common Crawl CDX index, default
  crtsh        crt.sh certificate search, optional and often rate-limited
`)
}
