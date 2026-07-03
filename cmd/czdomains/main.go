package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"czdomains/internal/discovery"
	"czdomains/internal/domainutil"
	"czdomains/internal/enrich"
	"czdomains/internal/output"
	"czdomains/internal/rdap"
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
	outPath := flags.String("out", "discovered.txt", "output domain list path")
	sources := flags.String("source", "commoncrawl", "comma-separated sources: commoncrawl,crtsh")
	ccIndex := flags.String("cc-index", "latest", "Common Crawl index id, URL, or latest")
	ccIndexCount := flags.Int("cc-index-count", 0, "number of recent Common Crawl indexes to scan when --cc-index=latest; 0 scans all")
	timeout := flags.Duration("timeout", 60*time.Second, "HTTP timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}

	results, err := discoverDomains(context.Background(), *limit, *sources, *ccIndex, *ccIndexCount, *timeout)
	if err != nil && len(results) == 0 {
		return err
	}
	if err := writeDiscovered(*outPath, results); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %d domains to %s\n", len(results), *outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "discovery completed with warning: %v\n", err)
	}
	return nil
}

func runEnrich(args []string) error {
	flags := flag.NewFlagSet("enrich", flag.ContinueOnError)
	inputPath := flags.String("input", "discovered.txt", "input domain list path")
	csvPath := flags.String("csv", "domains.csv", "CSV output path")
	jsonlPath := flags.String("jsonl", "domains.jsonl", "JSONL output path")
	delay := flags.Duration("delay", 200*time.Millisecond, "delay between RDAP requests")
	timeout := flags.Duration("timeout", 30*time.Second, "HTTP timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}

	inputs, err := readDomainInputs(*inputPath)
	if err != nil {
		return err
	}
	return enrichDomains(context.Background(), inputs, *csvPath, *jsonlPath, *delay, *timeout)
}

func runAll(args []string) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	limit := flags.Int("limit", 10000, "maximum number of unique domains to discover")
	csvPath := flags.String("csv", "domains.csv", "CSV output path")
	jsonlPath := flags.String("jsonl", "domains.jsonl", "JSONL output path")
	sources := flags.String("source", "commoncrawl", "comma-separated sources: commoncrawl,crtsh")
	ccIndex := flags.String("cc-index", "latest", "Common Crawl index id, URL, or latest")
	ccIndexCount := flags.Int("cc-index-count", 0, "number of recent Common Crawl indexes to scan when --cc-index=latest; 0 scans all")
	delay := flags.Duration("delay", 200*time.Millisecond, "delay between RDAP requests")
	timeout := flags.Duration("timeout", 60*time.Second, "HTTP timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}

	results, err := discoverDomains(context.Background(), *limit, *sources, *ccIndex, *ccIndexCount, *timeout)
	if err != nil && len(results) == 0 {
		return err
	}
	inputs := make([]domainInput, 0, len(results))
	for _, result := range results {
		inputs = append(inputs, domainInput{Domain: result.Domain, Source: result.Source})
	}
	if enrichErr := enrichDomains(context.Background(), inputs, *csvPath, *jsonlPath, *delay, *timeout); enrichErr != nil {
		return enrichErr
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "discovery completed with warning: %v\n", err)
	}
	return nil
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
	for i, input := range inputs {
		record := processor.Enrich(ctx, input.Domain, input.Source)
		if err := csvWriter.WriteRecord(record); err != nil {
			return err
		}
		if err := jsonlWriter.WriteRecord(record); err != nil {
			return err
		}
		if (i+1)%100 == 0 {
			fmt.Fprintf(os.Stderr, "enriched %d/%d domains\n", i+1, len(inputs))
		}
	}
	if err := csvWriter.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %d records to %s and %s\n", len(inputs), csvPath, jsonlPath)
	return nil
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
  czdomains discover --limit 10000 --out discovered.txt
  czdomains discover --limit 1000000 --cc-index-count 0 --out discovered.txt
  czdomains enrich --input discovered.txt --csv domains.csv --jsonl domains.jsonl
  czdomains run --limit 10000 --csv domains.csv --jsonl domains.jsonl

Sources:
  commoncrawl  Common Crawl CDX index, default
  crtsh        crt.sh certificate search, optional and often rate-limited
`)
}
