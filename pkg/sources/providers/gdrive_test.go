package providers

import (
	"net/url"
	"strings"
	"testing"
)

func TestGDriveBuildFilesListPath_EncodesAndAllDrives(t *testing.T) {
	g := NewGDriveProvider()

	query := "mimeType = 'application/pdf' and modifiedTime > '2026-01-01T00:00:00Z' and trashed = false"
	uri := g.buildFilesListPath(query, 50, "modifiedTime desc", "nextPageToken,files(id,name)", "", true)

	if strings.Contains(uri, " ") {
		t.Fatalf("expected request URI to contain no spaces, got: %q", uri)
	}

	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatalf("failed to parse request URI %q: %v", uri, err)
	}

	q := parsed.Query()
	if got := q.Get("q"); got != query {
		t.Fatalf("q mismatch: got %q, want %q", got, query)
	}
	if got := q.Get("orderBy"); got != "modifiedTime desc" {
		t.Fatalf("orderBy mismatch: got %q, want %q", got, "modifiedTime desc")
	}
	if got := q.Get("spaces"); got != "drive" {
		t.Fatalf("spaces mismatch: got %q, want %q", got, "drive")
	}
	if got := q.Get("corpora"); got != "allDrives" {
		t.Fatalf("corpora mismatch: got %q, want %q", got, "allDrives")
	}
	if got := q.Get("includeItemsFromAllDrives"); got != "true" {
		t.Fatalf("includeItemsFromAllDrives mismatch: got %q, want %q", got, "true")
	}
	if got := q.Get("supportsAllDrives"); got != "true" {
		t.Fatalf("supportsAllDrives mismatch: got %q, want %q", got, "true")
	}
}

func TestGDriveFormatFilename_DoesNotDuplicateExtension(t *testing.T) {
	g := NewGDriveProvider()

	metadata := map[string]string{
		"id":      "abc123",
		"name":    "invoice",
		"ext":     ".pdf",
		"date":    "2026-01-01",
		"created": "2026-01-01",
	}

	got := g.FormatFilename("{date}_{name}_{id}", metadata)
	want := "2026-01-01_invoice_abc123.pdf"
	if got != want {
		t.Fatalf("filename mismatch: got %q, want %q", got, want)
	}

	got2 := g.FormatFilename("{name}_{id}{ext}", metadata)
	want2 := "invoice_abc123.pdf"
	if got2 != want2 {
		t.Fatalf("filename mismatch: got %q, want %q", got2, want2)
	}
}

