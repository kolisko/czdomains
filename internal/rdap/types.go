package rdap

import (
	"encoding/json"
	"strings"
)

type Link struct {
	Value string `json:"value,omitempty"`
	Rel   string `json:"rel,omitempty"`
	Href  string `json:"href,omitempty"`
	Type  string `json:"type,omitempty"`
}

type Event struct {
	EventAction string `json:"eventAction,omitempty"`
	EventDate   string `json:"eventDate,omitempty"`
	EventActor  string `json:"eventActor,omitempty"`
}

type Entity struct {
	ObjectClassName string          `json:"objectClassName,omitempty"`
	Handle          string          `json:"handle,omitempty"`
	Roles           []string        `json:"roles,omitempty"`
	Links           []Link          `json:"links,omitempty"`
	VCardArray      json.RawMessage `json:"vcardArray,omitempty"`
	Events          []Event         `json:"events,omitempty"`
	Entities        []Entity        `json:"entities,omitempty"`
	Status          []string        `json:"status,omitempty"`
	Port43          string          `json:"port43,omitempty"`
}

type Nameserver struct {
	ObjectClassName string              `json:"objectClassName,omitempty"`
	Handle          string              `json:"handle,omitempty"`
	LDHName         string              `json:"ldhName,omitempty"`
	Links           []Link              `json:"links,omitempty"`
	IPAddresses     map[string][]string `json:"ipAddresses,omitempty"`
}

type Notice struct {
	Title       string   `json:"title,omitempty"`
	Description []string `json:"description,omitempty"`
}

type Domain struct {
	ObjectClassName string          `json:"objectClassName,omitempty"`
	Handle          string          `json:"handle,omitempty"`
	LDHName         string          `json:"ldhName,omitempty"`
	Links           []Link          `json:"links,omitempty"`
	Port43          string          `json:"port43,omitempty"`
	Events          []Event         `json:"events,omitempty"`
	Entities        []Entity        `json:"entities,omitempty"`
	Status          []string        `json:"status,omitempty"`
	Nameservers     []Nameserver    `json:"nameservers,omitempty"`
	Notices         []Notice        `json:"notices,omitempty"`
	Raw             json.RawMessage `json:"-"`
}

type Contact struct {
	Handle  string `json:"handle,omitempty"`
	Name    string `json:"name,omitempty"`
	Org     string `json:"org,omitempty"`
	Email   string `json:"email,omitempty"`
	Address string `json:"address,omitempty"`
	Status  string `json:"status,omitempty"`
	Error   string `json:"error,omitempty"`
}

func (e Entity) SelfLink() string {
	for _, link := range e.Links {
		if link.Rel == "self" && link.Href != "" {
			return link.Href
		}
	}
	return ""
}

func (e Entity) HasRole(role string) bool {
	for _, candidate := range e.Roles {
		if candidate == role {
			return true
		}
	}
	return false
}

func EventDate(events []Event, action string) string {
	for _, event := range events {
		if event.EventAction == action {
			return event.EventDate
		}
	}
	return ""
}

func NameserverNames(nameservers []Nameserver) []string {
	out := make([]string, 0, len(nameservers))
	for _, nameserver := range nameservers {
		name := nameserver.LDHName
		if name == "" {
			name = nameserver.Handle
		}
		if name != "" {
			out = append(out, name)
		}
	}
	return out
}

func ParseContact(entity Entity) Contact {
	contact := Contact{Handle: entity.Handle}
	if len(entity.VCardArray) == 0 {
		contact.Status = "missing"
		return contact
	}

	values := parseVCard(entity.VCardArray)
	contact.Name = first(values["fn"])
	contact.Org = first(values["org"])
	contact.Email = first(values["email"])
	contact.Address = strings.Join(values["adr"], "; ")
	if contact.Name == "" && contact.Org == "" && contact.Email == "" && contact.Address == "" {
		contact.Status = "not_public"
	} else {
		contact.Status = "found"
	}
	return contact
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func parseVCard(raw json.RawMessage) map[string][]string {
	out := map[string][]string{}
	var top []json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil || len(top) < 2 {
		return out
	}
	var fields []json.RawMessage
	if err := json.Unmarshal(top[1], &fields); err != nil {
		return out
	}

	for _, fieldRaw := range fields {
		var field []json.RawMessage
		if err := json.Unmarshal(fieldRaw, &field); err != nil || len(field) < 4 {
			continue
		}
		var name string
		if err := json.Unmarshal(field[0], &name); err != nil {
			continue
		}
		name = strings.ToLower(name)
		value := parseVCardValue(field[3])
		if value == "" {
			continue
		}
		out[name] = append(out[name], value)
	}
	return out
}

func parseVCardValue(raw json.RawMessage) string {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return strings.TrimSpace(text)
	}
	var parts []string
	if err := json.Unmarshal(raw, &parts); err == nil {
		clean := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part != "" {
				clean = append(clean, part)
			}
		}
		return strings.Join(clean, ", ")
	}
	var nested []any
	if err := json.Unmarshal(raw, &nested); err == nil {
		clean := make([]string, 0, len(nested))
		for _, part := range nested {
			if text, ok := part.(string); ok && strings.TrimSpace(text) != "" {
				clean = append(clean, strings.TrimSpace(text))
			}
		}
		return strings.Join(clean, ", ")
	}
	return ""
}
