package output

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"strings"

	"czdomains/internal/enrich"
)

var CSVHeader = []string{
	"domain",
	"source",
	"rdap_status",
	"registered_at",
	"expires_at",
	"registrar_handle",
	"registrant_handle",
	"registrant_name",
	"registrant_org",
	"registrant_email",
	"registrant_status",
	"admin_handles",
	"admin_statuses",
	"tech_handles",
	"tech_names",
	"tech_orgs",
	"tech_emails",
	"tech_statuses",
	"nameservers",
	"last_error",
}

type CSVWriter struct {
	writer *csv.Writer
}

func NewCSVWriter(w io.Writer) *CSVWriter {
	return &CSVWriter{writer: csv.NewWriter(w)}
}

func (w *CSVWriter) WriteHeader() error {
	return w.writer.Write(CSVHeader)
}

func (w *CSVWriter) WriteRecord(record enrich.Record) error {
	return w.writer.Write([]string{
		record.Domain,
		record.Source,
		record.RDAPStatus,
		record.RegisteredAt,
		record.ExpiresAt,
		record.RegistrarHandle,
		record.Registrant.Handle,
		record.Registrant.Name,
		record.Registrant.Org,
		record.Registrant.Email,
		record.Registrant.Status,
		enrich.ContactHandles(record.Admins),
		enrich.ContactStatuses(record.Admins),
		enrich.ContactHandles(record.Techs),
		enrich.ContactNames(record.Techs),
		enrich.ContactOrgs(record.Techs),
		enrich.ContactEmails(record.Techs),
		enrich.ContactStatuses(record.Techs),
		strings.Join(record.Nameservers, "; "),
		record.LastError,
	})
}

func (w *CSVWriter) Flush() error {
	w.writer.Flush()
	return w.writer.Error()
}

type JSONLWriter struct {
	encoder *json.Encoder
}

func NewJSONLWriter(w io.Writer) *JSONLWriter {
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	return &JSONLWriter{encoder: encoder}
}

func (w *JSONLWriter) WriteRecord(record enrich.Record) error {
	return w.encoder.Encode(record)
}
