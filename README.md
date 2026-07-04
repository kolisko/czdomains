# czdomains

`czdomains` is a Go CLI that discovers `.cz` domains from public best-effort sources and enriches them with public CZ.NIC RDAP data.

It does not bypass hidden or private registry data. If CZ.NIC RDAP does not publish a contact field, the output records that absence explicitly.

## Usage

```sh
czdomains discover --limit 10000 --db czdomains.sqlite
czdomains export --db czdomains.sqlite
czdomains enrich --db czdomains.sqlite --csv domains.csv --jsonl domains.jsonl
czdomains run --limit 10000 --db czdomains.sqlite --csv domains.csv --jsonl domains.jsonl
czdomains version
czdomains update
```

Release binaries check the latest GitHub Release before running discovery, export, enrich, or run commands. If the binary is outdated, it prints a prominent warning and stops before doing work. Update it explicitly with:

```sh
czdomains update
```

The update command downloads the matching release asset for your OS/architecture, verifies the GitHub asset digest when available, replaces the current binary, and cleans up its temporary download file.

`discover` stores domains in SQLite by default. This keeps memory usage bounded, deduplicates domains across all sources and Common Crawl data indexes, and allows the next run to resume from already completed index blocks.

Use `--fresh` when you explicitly want to delete the previous database and start again:

```sh
czdomains discover --fresh --limit 10000 --db czdomains.sqlite
```

The default discovery source is Common Crawl. `czdomains` reads Common Crawl data files from `data.commoncrawl.org`: it resolves a crawl, downloads `cc-index.paths.gz`, uses `cluster.idx` when available to fetch only relevant `cdx-*.gz` byte ranges, and falls back to sequential CDX streaming if needed.

```sh
czdomains discover --source commoncrawl --limit 1000
```

When using `--cc-index latest`, `czdomains` first tries the official structured Common Crawl crawl list and falls back to the data index HTML page if that lookup fails. By default it scans the newest crawl. Use `--cc-index-count N` to scan the newest `N` crawls:

```sh
czdomains discover --limit 1000000 --db czdomains.sqlite
czdomains discover --limit 10000 --cc-index-count 3 --db quick-sample.sqlite
```

Common Crawl retries are conservative by default. After 3 transient failures such as EOF, connection refused, timeout, HTTP 429, or HTTP 5xx, `czdomains` waits 15 minutes and retries the same request instead of skipping it. During the wait it shows a single updating countdown line on stderr, so stdout exports and pipelines stay clean.

The retry defaults can be tuned when needed:

```sh
czdomains discover --cc-fail-threshold 3 --cc-cooldown 15m --cc-wait-progress 1s --cc-max-cooldowns 0
```

`--cc-max-cooldowns 0` means wait indefinitely until the request succeeds or the process is interrupted.

Discovery progress is written to stderr so stdout remains safe for exports and pipelines. Common Crawl discovery reports the crawl-list source, selected crawl IDs, manifest URL, number of CDX files, whether `cluster.idx` is available, scan mode, current block or file, inserted domain counts, retries, cooldowns, and final database totals.

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
- Completed Common Crawl blocks/files are checkpointed and skipped on later runs.
- Transient Common Crawl failures trigger a default cooldown and retry the same request.
- HTTP failures that remain after configured cooldown limits are recorded per block/file and do not discard already discovered domains.

RDAP enrichment is intentionally rate-limited because it queries CZ.NIC RDAP per domain. For very large databases, expect enrichment to take much longer than discovery.

## Releases

GitHub Actions builds binaries for:

- Linux amd64/arm64
- macOS amd64/arm64
- Windows amd64/arm64

Push a tag like `v0.1.0` to create a GitHub Release with all binaries attached.

Release builds embed the version from the git tag at build time. The source tree keeps `version=dev`, so the binary version cannot drift from the GitHub release tag.
