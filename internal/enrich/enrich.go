package enrich

import (
	"context"
	"fmt"
	"strings"

	"czdomains/internal/rdap"
)

type RDAPClient interface {
	Domain(ctx context.Context, domain string) (*rdap.Domain, int, error)
	EntityByURL(ctx context.Context, rawURL string) (*rdap.Entity, int, error)
}

type Processor struct {
	client      RDAPClient
	entityCache map[string]rdap.Contact
}

type Record struct {
	Domain           string         `json:"domain"`
	Source           string         `json:"source"`
	RDAPStatus       string         `json:"rdap_status"`
	RegisteredAt     string         `json:"registered_at,omitempty"`
	ExpiresAt        string         `json:"expires_at,omitempty"`
	RegistrarHandle  string         `json:"registrar_handle,omitempty"`
	Registrant       rdap.Contact   `json:"registrant,omitempty"`
	Admins           []rdap.Contact `json:"admins,omitempty"`
	Techs            []rdap.Contact `json:"techs,omitempty"`
	Nameservers      []string       `json:"nameservers,omitempty"`
	LastError        string         `json:"last_error,omitempty"`
	RDAPDomainRecord *rdap.Domain   `json:"rdap_domain_record,omitempty"`
}

func NewProcessor(client RDAPClient) *Processor {
	return &Processor{
		client:      client,
		entityCache: map[string]rdap.Contact{},
	}
}

func (p *Processor) Enrich(ctx context.Context, domain string, source string) Record {
	record := Record{Domain: domain, Source: source}
	domainRecord, status, err := p.client.Domain(ctx, domain)
	if err != nil {
		record.RDAPStatus = statusLabel(status)
		record.LastError = err.Error()
		return record
	}

	record.RDAPStatus = "found"
	record.RDAPDomainRecord = domainRecord
	record.RegisteredAt = rdap.EventDate(domainRecord.Events, "registration")
	record.ExpiresAt = rdap.EventDate(domainRecord.Events, "expiration")
	record.Nameservers = rdap.NameserverNames(domainRecord.Nameservers)

	for _, entity := range domainRecord.Entities {
		switch {
		case entity.HasRole("registrar"):
			if record.RegistrarHandle == "" {
				record.RegistrarHandle = entity.Handle
			}
		case entity.HasRole("registrant"):
			record.Registrant = p.resolveContact(ctx, entity)
		case entity.HasRole("administrative"):
			record.Admins = append(record.Admins, p.resolveContact(ctx, entity))
		case entity.HasRole("technical"):
			record.Techs = append(record.Techs, p.resolveContact(ctx, entity))
		}
	}

	return record
}

func (p *Processor) resolveContact(ctx context.Context, entity rdap.Entity) rdap.Contact {
	if cached, ok := p.entityCache[entity.Handle]; ok {
		return cached
	}

	contact := rdap.Contact{Handle: entity.Handle, Status: "missing"}
	self := entity.SelfLink()
	if self == "" {
		p.entityCache[entity.Handle] = contact
		return contact
	}

	detail, status, err := p.client.EntityByURL(ctx, self)
	if err != nil {
		contact.Status = statusLabel(status)
		contact.Error = err.Error()
		p.entityCache[entity.Handle] = contact
		return contact
	}
	contact = rdap.ParseContact(*detail)
	if contact.Handle == "" {
		contact.Handle = entity.Handle
	}
	p.entityCache[entity.Handle] = contact
	return contact
}

func statusLabel(status int) string {
	if status == 0 {
		return "error"
	}
	if status == 404 {
		return "not_found"
	}
	if status == 429 {
		return "rate_limited"
	}
	if status >= 500 {
		return fmt.Sprintf("server_error_%d", status)
	}
	return fmt.Sprintf("http_%d", status)
}

func ContactHandles(contacts []rdap.Contact) string {
	values := make([]string, 0, len(contacts))
	for _, contact := range contacts {
		if contact.Handle != "" {
			values = append(values, contact.Handle)
		}
	}
	return strings.Join(values, "; ")
}

func ContactNames(contacts []rdap.Contact) string {
	values := make([]string, 0, len(contacts))
	for _, contact := range contacts {
		if contact.Name != "" {
			values = append(values, contact.Name)
		}
	}
	return strings.Join(values, "; ")
}

func ContactOrgs(contacts []rdap.Contact) string {
	values := make([]string, 0, len(contacts))
	for _, contact := range contacts {
		if contact.Org != "" {
			values = append(values, contact.Org)
		}
	}
	return strings.Join(values, "; ")
}

func ContactEmails(contacts []rdap.Contact) string {
	values := make([]string, 0, len(contacts))
	for _, contact := range contacts {
		if contact.Email != "" {
			values = append(values, contact.Email)
		}
	}
	return strings.Join(values, "; ")
}

func ContactStatuses(contacts []rdap.Contact) string {
	values := make([]string, 0, len(contacts))
	for _, contact := range contacts {
		if contact.Status != "" {
			values = append(values, contact.Status)
		}
	}
	return strings.Join(values, "; ")
}
