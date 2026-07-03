# czdomains

`czdomains` is a Go CLI that discovers `.cz` domains from public best-effort sources and enriches them with public CZ.NIC RDAP data.

It does not bypass hidden or private registry data. If CZ.NIC RDAP does not publish a contact field, the output records that absence explicitly.

## Usage

```sh
czdomains discover --limit 10000 --out discovered.txt
czdomains enrich --input discovered.txt --csv domains.csv --jsonl domains.jsonl
czdomains run --limit 10000 --csv domains.csv --jsonl domains.jsonl
```

The default discovery source is Common Crawl:

```sh
czdomains run --source commoncrawl --limit 1000
```

When using `--cc-index latest`, `czdomains` scans Common Crawl indexes until it reaches `--limit` or runs out of available indexes. By default it scans every available Common Crawl index. Use `--cc-index-count N` to limit discovery to the newest `N` indexes for quicker test runs:

```sh
czdomains discover --limit 1000000 --out discovered.txt
czdomains discover --limit 10000 --cc-index-count 3 --out quick-sample.txt
```

You can add `crt.sh`, but it is often rate-limited or temporarily unavailable:

```sh
czdomains run --source commoncrawl,crtsh --limit 1000
```

## Output

`domains.csv` contains a table-friendly summary. `domains.jsonl` contains one JSON record per domain, including the parsed RDAP domain record and contact status details.

`discovered.txt` stores one domain per line as:

```txt
example.cz
```

Hand-written input files may contain just domains. Legacy `domain<TAB>source` files are also accepted by `enrich`.

## Releases

GitHub Actions builds binaries for:

- Linux amd64/arm64
- macOS amd64/arm64
- Windows amd64/arm64

Push a tag like `v0.1.0` to create a GitHub Release with all binaries attached.
