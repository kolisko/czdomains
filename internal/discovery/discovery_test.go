package discovery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
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

	d := New(server.Client(), Config{Limit: 10, CollInfoURL: server.URL + "/collinfo.json"})
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

	d := New(server.Client(), Config{Limit: 10, CollInfoURL: server.URL + "/collinfo.json"})
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
