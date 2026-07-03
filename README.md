# czdomains

`czdomains` is a Go CLI that discovers `.cz` domains from public best-effort sources and enriches them with public CZ.NIC RDAP data.

It does not bypass hidden or private registry data. If CZ.NIC RDAP does not publish a contact field, the output records that absence explicitly.

## Usage

```sh
czdomains discover --limit 10000 --db czdomains.sqlite
czdomains export --db czdomains.sqlite
czdomains enrich --db czdomains.sqlite --csv domains.csv --jsonl domains.jsonl
czdomains run --limit 10000 --db czdomains.sqlite --csv domains.csv --jsonl domains.jsonl
```

`discover` stores domains in SQLite by default. This keeps memory usage bounded, deduplicates domains across all sources and Common Crawl indexes, and allows the next run to resume from already completed pages.

Use `--fresh` when you explicitly want to delete the previous database and start again:

```sh
czdomains discover --fresh --limit 10000 --db czdomains.sqlite
```

The default discovery source is Common Crawl.

```sh
czdomains discover --source commoncrawl --limit 1000
```

When using `--cc-index latest`, `czdomains` scans Common Crawl indexes until it reaches `--limit` or runs out of available indexes. By default it scans every available Common Crawl index. Use `--cc-index-count N` to limit discovery to the newest `N` indexes for quicker test runs:

```sh
czdomains discover --limit 1000000 --db czdomains.sqlite
czdomains discover --limit 10000 --cc-index-count 3 --db quick-sample.sqlite
```

You can add `crt.sh`, but it is often rate-limited or temporarily unavailable:

```sh
czdomains run --source commoncrawl,crtsh --limit 1000
```

If you want a plain text domain list during discovery, add `--out`. It writes only newly inserted domains, one per line:

```sh
czdomains discover --limit 1000000 --db czdomains.sqlite --out discovered.txt
```

For a complete text export from the SQLite database, use `export`. Without `--out`, it writes domains to stdout:

```sh
czdomains export --db czdomains.sqlite
```

Write the export to a file with `--out`:

```sh
czdomains export --db czdomains.sqlite --out discovered.txt
```

## Output

`domains.csv` contains a table-friendly summary. `domains.jsonl` contains one JSON record per domain, including the parsed RDAP domain record and contact status details.

`discovered.txt` stores one domain per line as:

```txt
example.cz
```

Hand-written input files may contain just domains. Legacy `domain<TAB>source` files are also accepted by `enrich`.

## Large Runs

Discovery is designed for large best-effort runs:

- Domains are written into SQLite as they are found.
- Uniqueness is enforced by the database primary key, not an in-memory map.
- Completed Common Crawl pages are checkpointed and skipped on later runs.
- HTTP failures are recorded per page and do not discard already discovered domains.

RDAP enrichment is intentionally rate-limited because it queries CZ.NIC RDAP per domain. For very large databases, expect enrichment to take much longer than discovery.

## Releases

GitHub Actions builds binaries for:

- Linux amd64/arm64
- macOS amd64/arm64
- Windows amd64/arm64

Push a tag like `v0.1.0` to create a GitHub Release with all binaries attached.
