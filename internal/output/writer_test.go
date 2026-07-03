package output

import (
	"bytes"
	"strings"
	"testing"

	"czdomains/internal/enrich"
	"czdomains/internal/rdap"
)

func TestCSVWriterEscapesValues(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCSVWriter(&buf)
	if err := writer.WriteHeader(); err != nil {
		t.Fatal(err)
	}
	err := writer.WriteRecord(enrich.Record{
		Domain:     "example.cz",
		Source:     "test",
		RDAPStatus: "found",
		Registrant: rdap.Contact{
			Handle: "OWNER",
			Name:   "Name, With Comma",
			Email:  "owner@example.cz",
		},
		Nameservers: []string{"ns1.example.cz", "ns2.example.cz"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"Name, With Comma"`) {
		t.Fatalf("CSV did not escape comma: %s", buf.String())
	}
}

func TestJSONLWriter(t *testing.T) {
	var buf bytes.Buffer
	writer := NewJSONLWriter(&buf)
	if err := writer.WriteRecord(enrich.Record{Domain: "example.cz", RDAPStatus: "found"}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(buf.String(), "\n") {
		t.Fatalf("JSONL record should end with newline: %q", buf.String())
	}
}
