package rdap

import (
	"encoding/json"
	"testing"
)

func TestParseContact(t *testing.T) {
	raw := json.RawMessage(`["vcard",[
		["version",{},"text","4.0"],
		["fn",{},"text","Pavel Zima"],
		["org",{},"text","Seznam.cz, a.s."],
		["adr",{"type":""},"text",["","Radlicka 3294/10","","","Praha 5","","150 00","CZ"]],
		["email",{},"text","domeny@example.cz"]
	]]`)
	contact := ParseContact(Entity{Handle: "TEST", VCardArray: raw})
	if contact.Status != "found" {
		t.Fatalf("status = %q", contact.Status)
	}
	if contact.Name != "Pavel Zima" || contact.Org != "Seznam.cz, a.s." || contact.Email != "domeny@example.cz" {
		t.Fatalf("unexpected contact: %+v", contact)
	}
	if contact.Address != "Radlicka 3294/10, Praha 5, 150 00, CZ" {
		t.Fatalf("address = %q", contact.Address)
	}
}

func TestEventDate(t *testing.T) {
	events := []Event{
		{EventAction: "registration", EventDate: "1996-10-07T00:00:00+00:00"},
		{EventAction: "expiration", EventDate: "2026-10-28T23:00:00+00:00"},
	}
	if got := EventDate(events, "expiration"); got != "2026-10-28T23:00:00+00:00" {
		t.Fatalf("got %q", got)
	}
}
