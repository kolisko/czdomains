package discovery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCommonCrawlDiscovery(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/collinfo.json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"CC-MAIN-TEST","cdx-api":"` + server.URL + `/index"}]`))
		case "/index":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"url":"https://www.seznam.cz/"}` + "\n"))
			_, _ = w.Write([]byte(`{"url":"https://mail.example.cz/path"}` + "\n"))
			_, _ = w.Write([]byte(`{"url":"https://example.com/"}` + "\n"))
			_, _ = w.Write([]byte(`{"url":"https://example.cz/again"}` + "\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	d := New(server.Client(), Config{
		Limit:           10,
		CollInfoURL:     server.URL + "/collinfo.json",
		CCFailThreshold: 1,
		CCMaxCooldowns:  1,
		CooldownWait:    immediateCooldownWait,
	})
	got, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %v", got)
	}
	if got[0].Domain != "seznam.cz" || got[1].Domain != "example.cz" {
		t.Fatalf("unexpected domains: %v", got)
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

func TestCommonCrawlKeepsPartialResultsWhenPageFails(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/collinfo.json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"CC-MAIN-TEST","cdx-api":"` + server.URL + `/index"}]`))
		case "/index":
			if r.URL.Query().Get("showNumPages") == "true" {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"pages":2}`))
				return
			}
			if r.URL.Query().Get("page") == "0" {
				http.Error(w, "bad gateway", http.StatusBadGateway)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"url":"https://www.seznam.cz/"}` + "\n"))
			_, _ = w.Write([]byte(`{"url":"https://mail.example.cz/path"}` + "\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	d := New(server.Client(), Config{
		Limit:           10,
		CollInfoURL:     server.URL + "/collinfo.json",
		CCFailThreshold: 1,
		CCMaxCooldowns:  1,
		CooldownWait:    immediateCooldownWait,
	})
	got, err := d.Discover(context.Background())
	if err == nil {
		t.Fatal("expected warning error from failed page")
	}
	if len(got) != 2 {
		t.Fatalf("got %v, err %v", got, err)
	}
}

func TestCommonCrawlContinuesAcrossIndexesUntilLimit(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/collinfo.json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"ONE","cdx-api":"` + server.URL + `/one"},{"id":"TWO","cdx-api":"` + server.URL + `/two"}]`))
		case "/one", "/two":
			if r.URL.Query().Get("showNumPages") == "true" {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"pages":1}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Path == "/one" {
				_, _ = w.Write([]byte(`{"url":"https://one.cz/"}` + "\n"))
				return
			}
			_, _ = w.Write([]byte(`{"url":"https://two.cz/"}` + "\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	d := New(server.Client(), Config{Limit: 2, CollInfoURL: server.URL + "/collinfo.json", CCIndexCount: 2})
	got, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %v", got)
	}
	if got[0].Domain != "one.cz" || got[1].Domain != "two.cz" {
		t.Fatalf("unexpected domains: %v", got)
	}
}

func TestCommonCrawlUsesSamePageSizeForCountAndPageFetch(t *testing.T) {
	var countPageSize string
	var fetchPageSize string
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/collinfo.json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"CC-MAIN-TEST","cdx-api":"` + server.URL + `/index"}]`))
		case "/index":
			if r.URL.Query().Get("showNumPages") == "true" {
				countPageSize = r.URL.Query().Get("pageSize")
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"pages":1}`))
				return
			}
			fetchPageSize = r.URL.Query().Get("pageSize")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"url":"https://example.cz/"}` + "\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	d := New(server.Client(), Config{Limit: 10, CollInfoURL: server.URL + "/collinfo.json"})
	got, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %v", got)
	}
	want := strconv.Itoa(commonCrawlPageSize)
	if countPageSize != want || fetchPageSize != want {
		t.Fatalf("count pageSize=%q fetch pageSize=%q want %q", countPageSize, fetchPageSize, want)
	}
	if commonCrawlPageSize != 5 {
		t.Fatalf("commonCrawlPageSize=%d, want 5 for checkpoint compatibility", commonCrawlPageSize)
	}
}

func TestCommonCrawlCooldownRetriesSamePageAfterThreeTransientFailures(t *testing.T) {
	client := &sequenceClient{
		responses: []sequenceResponse{
			{err: io.EOF},
			{err: io.ErrUnexpectedEOF},
			{err: errors.New("dial tcp: connect: connection refused")},
			{status: http.StatusOK, body: `{"url":"https://example.cz/"}` + "\n"},
		},
	}
	cooldowns := 0
	d := New(client, Config{
		Limit:           10,
		CCFailThreshold: 3,
		CooldownWait: func(ctx context.Context, duration time.Duration, progressEvery time.Duration, onProgress func(time.Duration)) error {
			cooldowns++
			onProgress(duration)
			return nil
		},
	})
	sink := newMemorySink()
	total := 0
	err := d.scanCommonCrawlPage(context.Background(), CrawlPage{Source: "commoncrawl", IndexURL: "test", Page: 0}, "page 1/1", "https://example.test/index", sink, &total)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cooldowns != 1 {
		t.Fatalf("cooldowns=%d, want 1", cooldowns)
	}
	if total != 1 || len(sink.Results()) != 1 || sink.Results()[0].Domain != "example.cz" {
		t.Fatalf("unexpected results total=%d results=%v", total, sink.Results())
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
	cooledDown, err := d.cooldownAfterTransient(context.Background(), "page 1/1", httpStatusError{
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
	cooledDown, err := d.cooldownAfterTransient(context.Background(), "page 1/1", io.EOF, &failures, &cooldowns)
	if err != nil {
		t.Fatal(err)
	}
	if !cooledDown {
		t.Fatal("expected cooldown")
	}
	got := progress.String()
	if !strings.Contains(got, "\rcommoncrawl: waiting 42s before retrying page 1/1") {
		t.Fatalf("progress does not contain carriage-return countdown: %q", got)
	}
	if !strings.Contains(got, "\rcommoncrawl: retrying page 1/1 after cooldown\n") {
		t.Fatalf("progress does not finish line before retry: %q", got)
	}
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
