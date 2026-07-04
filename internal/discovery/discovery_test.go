package discovery

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCommonCrawlDiscoveryUsesClusterRangeScan(t *testing.T) {
	cdxPath := "cc-index/collections/CC-MAIN-2026-25/indexes/cdx-00042.gz"
	cdxBlock := gzipData(t, strings.Join([]string{
		`cz,example,mail)/path 20260601000000 {"url":"https://mail.example.cz/path"}`,
		`cz,seznam,www)/ 20260601000000 {"url":"https://www.seznam.cz/"}`,
		`com,example)/ 20260601000000 {"url":"https://example.com/"}`,
	}, "\n")+"\n")
	manifest := gzipData(t, cdxPath+"\ncc-index/collections/CC-MAIN-2026-25/indexes/cluster.idx\ncc-index/collections/CC-MAIN-2026-25/metadata.yaml\n")
	var gotRange string
	var progress bytes.Buffer

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/collinfo.json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"CC-MAIN-2026-25"},{"id":"CC-MAIN-2026-21"}]`))
		case "/crawl-data/CC-MAIN-2026-25/cc-index.paths.gz":
			_, _ = w.Write(manifest)
		case "/cc-index/collections/CC-MAIN-2026-25/indexes/cluster.idx":
			_, _ = fmt.Fprintf(w, "cz,example)/ 20260601000000 %s 0 %d 1\nda,example)/ 20260601000000 %s %d 10 2\n", cdxPath, len(cdxBlock), cdxPath, len(cdxBlock))
		case "/" + cdxPath:
			gotRange = r.Header.Get("Range")
			if gotRange == "" {
				t.Errorf("expected Range header")
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(cdxBlock)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	d := New(server.Client(), Config{
		Limit:           10,
		CollInfoURL:     server.URL + "/collinfo.json",
		CCDataBaseURL:   server.URL,
		CCFailThreshold: 1,
		CCMaxCooldowns:  1,
		CooldownWait:    immediateCooldownWait,
		Progress: func(format string, args ...any) {
			_, _ = progress.WriteString(formatString(format, args...))
		},
	})
	got, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotRange != fmt.Sprintf("bytes=0-%d", len(cdxBlock)-1) {
		t.Fatalf("Range=%q", gotRange)
	}
	if len(got) != 2 || got[0].Domain != "example.cz" || got[1].Domain != "seznam.cz" {
		t.Fatalf("unexpected domains: %v", got)
	}
	out := progress.String()
	for _, want := range []string{"collinfo.json returned", "manifest has 1 CDX files", "scan mode cluster range scan", "block 1/1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("progress missing %q in:\n%s", want, out)
		}
	}
}

func TestCommonCrawlFallsBackToSequentialScan(t *testing.T) {
	firstPath := "cc-index/collections/CC-MAIN-2026-25/indexes/cdx-00000.gz"
	secondPath := "cc-index/collections/CC-MAIN-2026-25/indexes/cdx-00001.gz"
	firstFile := gzipData(t, `am,example)/ 20260601000000 {"url":"https://example.am/"}`+"\n")
	secondFile := gzipData(t, strings.Join([]string{
		`cz,example)/ 20260601000000 {"url":"https://example.cz/"}`,
		`cz,seznam,www)/ 20260601000000 {"url":"https://www.seznam.cz/"}`,
		`da,example)/ 20260601000000 {"url":"https://example.dk/"}`,
	}, "\n")+"\n")
	manifest := gzipData(t, firstPath+"\n"+secondPath+"\n")
	var progress bytes.Buffer

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/collinfo.json":
			_, _ = w.Write([]byte(`[{"id":"CC-MAIN-2026-25"}]`))
		case "/crawl-data/CC-MAIN-2026-25/cc-index.paths.gz":
			_, _ = w.Write(manifest)
		case "/" + firstPath:
			_, _ = w.Write(firstFile)
		case "/" + secondPath:
			_, _ = w.Write(secondFile)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	d := New(server.Client(), Config{
		Limit:           10,
		CollInfoURL:     server.URL + "/collinfo.json",
		CCDataBaseURL:   server.URL,
		CCFailThreshold: 1,
		CCMaxCooldowns:  1,
		CooldownWait:    immediateCooldownWait,
		Progress: func(format string, args ...any) {
			_, _ = progress.WriteString(formatString(format, args...))
		},
	})
	got, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 || got[0].Domain != "example.cz" || got[1].Domain != "seznam.cz" {
		t.Fatalf("unexpected domains: %v", got)
	}
	if !strings.Contains(progress.String(), "scan mode sequential fallback") {
		t.Fatalf("progress missing fallback mode:\n%s", progress.String())
	}
}

func TestCommonCrawlCrawlLookupFallsBackToHTMLAndSorts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/collinfo.json":
			http.Error(w, "nope", http.StatusInternalServerError)
		case "/collections/index.html":
			_, _ = w.Write([]byte(`<a>CC-MAIN-2025-51</a><a>CC-MAIN-2026-21</a><a>CC-MAIN-2026-25</a><a>CC-MAIN-2026-25</a>`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	d := New(server.Client(), Config{
		CCIndex:          "latest",
		CCIndexCount:     2,
		CollInfoURL:      server.URL + "/collinfo.json",
		CCCollectionsURL: server.URL + "/collections/index.html",
		CCFailThreshold:  1,
		CCMaxCooldowns:   1,
		CooldownWait:     immediateCooldownWait,
	})
	got, err := d.commonCrawlCrawls(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "CC-MAIN-2026-25" || got[1].ID != "CC-MAIN-2026-21" {
		t.Fatalf("unexpected crawls: %v", got)
	}
}

func TestCommonCrawlRejectsOldIndexEndpoint(t *testing.T) {
	d := New(nil, Config{CCIndex: "https://index.commoncrawl.org/CC-MAIN-2026-25-index"})
	_, err := d.commonCrawlCrawls(context.Background())
	if err == nil || !strings.Contains(err.Error(), "must be latest or a crawl id") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseCommonCrawlManifest(t *testing.T) {
	manifest, err := parseCommonCrawlManifest(strings.NewReader(strings.Join([]string{
		"cc-index/collections/CC-MAIN-2026-25/indexes/cdx-00000.gz",
		"cc-index/collections/CC-MAIN-2026-25/indexes/cdx-00001.gz",
		"cc-index/collections/CC-MAIN-2026-25/indexes/cluster.idx",
		"cc-index/collections/CC-MAIN-2026-25/metadata.yaml",
	}, "\n")))
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.CDXPaths) != 2 || manifest.ClusterPath == "" || manifest.MetadataPath == "" {
		t.Fatalf("unexpected manifest: %+v", manifest)
	}
}

func TestParseCommonCrawlManifestRequiresCDX(t *testing.T) {
	_, err := parseCommonCrawlManifest(strings.NewReader("cc-index/collections/CC-MAIN-2026-25/indexes/cluster.idx\n"))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDomainFromCDXKey(t *testing.T) {
	cases := []struct {
		key  string
		want string
		ok   bool
	}{
		{key: "cz,seznam)/", want: "seznam.cz", ok: true},
		{key: "cz,seznam,www)/path", want: "seznam.cz", ok: true},
		{key: "com,example)/", ok: false},
		{key: "cz,)/", ok: false},
		{key: "cz,exa_mple)/", ok: false},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			got, err := domainFromCDXKey(tc.key)
			if tc.ok && (err != nil || got != tc.want) {
				t.Fatalf("domainFromCDXKey=%q,%v want %q", got, err, tc.want)
			}
			if !tc.ok && err == nil {
				t.Fatalf("expected error, got %q", got)
			}
		})
	}
}

func TestCommonCrawlCooldownRetriesAfterThreeTransientFailures(t *testing.T) {
	client := &sequenceClient{
		responses: []sequenceResponse{
			{err: io.EOF},
			{err: io.ErrUnexpectedEOF},
			{err: errors.New("dial tcp: connect: connection refused")},
			{status: http.StatusOK, body: "ok"},
		},
	}
	cooldowns := 0
	d := New(client, Config{
		CCFailThreshold: 3,
		CooldownWait: func(ctx context.Context, duration time.Duration, progressEvery time.Duration, onProgress func(time.Duration)) error {
			cooldowns++
			onProgress(duration)
			return nil
		},
	})
	resp, err := d.getOKWithCooldown(context.Background(), "https://example.test/index", "commoncrawl", "test request")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = resp.Body.Close()
	if cooldowns != 1 {
		t.Fatalf("cooldowns=%d, want 1", cooldowns)
	}
	if client.calls != 4 {
		t.Fatalf("client calls=%d, want 4", client.calls)
	}
}

func TestCommonCrawlTransientErrorClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "eof", err: io.EOF, want: true},
		{name: "unexpected eof", err: io.ErrUnexpectedEOF, want: true},
		{name: "connection refused", err: errors.New("dial tcp: connect: connection refused"), want: true},
		{name: "timeout", err: timeoutError{}, want: true},
		{name: "429", err: httpStatusError{label: "test", status: http.StatusTooManyRequests}, want: true},
		{name: "503", err: httpStatusError{label: "test", status: http.StatusServiceUnavailable}, want: true},
		{name: "404", err: httpStatusError{label: "test", status: http.StatusNotFound}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isTransientError(tc.err)
			if got != tc.want {
				t.Fatalf("isTransientError(%v)=%v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestCommonCrawlRetryAfterOverridesDefaultCooldown(t *testing.T) {
	var gotDuration time.Duration
	d := New(nil, Config{
		CCFailThreshold: 1,
		CCCooldown:      15 * time.Minute,
		CooldownWait: func(ctx context.Context, duration time.Duration, progressEvery time.Duration, onProgress func(time.Duration)) error {
			gotDuration = duration
			return nil
		},
	})
	failures := 0
	cooldowns := 0
	cooledDown, err := d.cooldownAfterTransient(context.Background(), "request", httpStatusError{
		label:      "commoncrawl",
		status:     http.StatusServiceUnavailable,
		retryAfter: 2 * time.Minute,
	}, &failures, &cooldowns)
	if err != nil {
		t.Fatal(err)
	}
	if !cooledDown {
		t.Fatal("expected cooldown")
	}
	if gotDuration != 2*time.Minute {
		t.Fatalf("duration=%s, want 2m", gotDuration)
	}
}

func TestCommonCrawlWaitingProgressUsesCarriageReturnAndFinalNewline(t *testing.T) {
	var progress bytes.Buffer
	d := New(nil, Config{
		CCFailThreshold: 1,
		CooldownWait: func(ctx context.Context, duration time.Duration, progressEvery time.Duration, onProgress func(time.Duration)) error {
			onProgress(42 * time.Second)
			return nil
		},
		Progress: func(format string, args ...any) {
			_, _ = progress.WriteString(formatString(format, args...))
		},
	})
	failures := 0
	cooldowns := 0
	cooledDown, err := d.cooldownAfterTransient(context.Background(), "request", io.EOF, &failures, &cooldowns)
	if err != nil {
		t.Fatal(err)
	}
	if !cooledDown {
		t.Fatal("expected cooldown")
	}
	got := progress.String()
	if !strings.Contains(got, "\rcommoncrawl: waiting 42s before retrying request") {
		t.Fatalf("progress does not contain carriage-return countdown: %q", got)
	}
	if !strings.Contains(got, "\rcommoncrawl: retrying request after cooldown\n") {
		t.Fatalf("progress does not finish line before retry: %q", got)
	}
}

func TestCRTShDiscovery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"name_value":"*.example.cz\nwww.seznam.cz"}]`))
	}))
	defer server.Close()

	d := New(server.Client(), Config{Sources: []string{"crtsh"}, CRTShURL: server.URL, Limit: 10})
	got, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %v", got)
	}
}

func gzipData(t *testing.T, value string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write([]byte(value)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func immediateCooldownWait(context.Context, time.Duration, time.Duration, func(time.Duration)) error {
	return nil
}

func formatString(format string, args ...any) string {
	if len(args) == 0 {
		return format
	}
	return fmt.Sprintf(format, args...)
}

type sequenceResponse struct {
	status int
	body   string
	err    error
}

type sequenceClient struct {
	responses []sequenceResponse
	calls     int
}

func (c *sequenceClient) Do(*http.Request) (*http.Response, error) {
	if c.calls >= len(c.responses) {
		return nil, errors.New("unexpected request")
	}
	response := c.responses[c.calls]
	c.calls++
	if response.err != nil {
		return nil, response.err
	}
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(response.body)),
	}, nil
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

var _ net.Error = timeoutError{}
