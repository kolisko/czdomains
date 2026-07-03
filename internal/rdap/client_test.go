package rdap

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientDomainAndEntity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/rdap+json")
		switch r.URL.Path {
		case "/domain/example.cz":
			_, _ = w.Write([]byte(`{"objectClassName":"domain","ldhName":"example.cz","entities":[{"handle":"OWNER","roles":["registrant"],"links":[{"rel":"self","href":"` + "http://" + r.Host + `/entity/OWNER"}]}]}`))
		case "/entity/OWNER":
			_, _ = w.Write([]byte(`{"objectClassName":"entity","handle":"OWNER","vcardArray":["vcard",[["fn",{},"text","Owner"]]]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := New(server.Client(), Config{BaseURL: server.URL})
	domain, status, err := client.Domain(context.Background(), "example.cz")
	if err != nil || status != 200 {
		t.Fatalf("status=%d err=%v", status, err)
	}
	if domain.LDHName != "example.cz" {
		t.Fatalf("domain = %+v", domain)
	}
	entity, status, err := client.EntityByURL(context.Background(), domain.Entities[0].SelfLink())
	if err != nil || status != 200 {
		t.Fatalf("status=%d err=%v", status, err)
	}
	if ParseContact(*entity).Name != "Owner" {
		t.Fatalf("entity = %+v", entity)
	}
}

func TestClientHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "temporary", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := New(server.Client(), Config{BaseURL: server.URL})
	_, status, err := client.Domain(context.Background(), "example.cz")
	if err == nil || status != http.StatusServiceUnavailable {
		t.Fatalf("status=%d err=%v", status, err)
	}
}
