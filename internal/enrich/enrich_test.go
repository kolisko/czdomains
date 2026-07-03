package enrich

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"czdomains/internal/rdap"
)

type fakeRDAP struct {
	domainStatus int
	domainErr    error
	domain       *rdap.Domain
	entities     map[string]*rdap.Entity
	statuses     map[string]int
	errors       map[string]error
}

func (f fakeRDAP) Domain(context.Context, string) (*rdap.Domain, int, error) {
	if f.domainErr != nil {
		return nil, f.domainStatus, f.domainErr
	}
	return f.domain, 200, nil
}

func (f fakeRDAP) EntityByURL(_ context.Context, rawURL string) (*rdap.Entity, int, error) {
	if err := f.errors[rawURL]; err != nil {
		return nil, f.statuses[rawURL], err
	}
	return f.entities[rawURL], 200, nil
}

func TestEnrichDomainWithContacts(t *testing.T) {
	registrantVCard := json.RawMessage(`["vcard",[["fn",{},"text","Owner"],["org",{},"text","Owner Org"],["email",{},"text","owner@example.cz"]]]`)
	techVCard := json.RawMessage(`["vcard",[["fn",{},"text","Tech"],["org",{},"text","Tech Org"],["email",{},"text","tech@example.cz"]]]`)
	client := fakeRDAP{
		domain: &rdap.Domain{
			Events: []rdap.Event{{EventAction: "registration", EventDate: "2020-01-01"}, {EventAction: "expiration", EventDate: "2027-01-01"}},
			Entities: []rdap.Entity{
				{Handle: "OWNER", Roles: []string{"registrant"}, Links: []rdap.Link{{Rel: "self", Href: "https://rdap.test/entity/OWNER"}}},
				{Handle: "REG-TEST", Roles: []string{"registrar"}},
				{Handle: "TECH", Roles: []string{"technical"}, Links: []rdap.Link{{Rel: "self", Href: "https://rdap.test/entity/TECH"}}},
			},
			Nameservers: []rdap.Nameserver{{LDHName: "ns1.example.cz"}},
		},
		entities: map[string]*rdap.Entity{
			"https://rdap.test/entity/OWNER": {Handle: "OWNER", VCardArray: registrantVCard},
			"https://rdap.test/entity/TECH":  {Handle: "TECH", VCardArray: techVCard},
		},
	}

	record := NewProcessor(client).Enrich(context.Background(), "example.cz", "test")
	if record.RDAPStatus != "found" || record.RegistrarHandle != "REG-TEST" {
		t.Fatalf("unexpected record: %+v", record)
	}
	if record.Registrant.Email != "owner@example.cz" {
		t.Fatalf("registrant = %+v", record.Registrant)
	}
	if len(record.Techs) != 1 || record.Techs[0].Email != "tech@example.cz" {
		t.Fatalf("techs = %+v", record.Techs)
	}
}

func TestEnrichDomainError(t *testing.T) {
	client := fakeRDAP{domainStatus: 503, domainErr: errors.New("HTTP 503")}
	record := NewProcessor(client).Enrich(context.Background(), "missing.cz", "test")
	if record.RDAPStatus != "server_error_503" || record.LastError == "" {
		t.Fatalf("unexpected record: %+v", record)
	}
}
