package views

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

// Artifact derives stable identity and display properties from a TaskOutput.
// BAML ExtractOutputs provides artifact_key, artifact_label, and artifact_kind
// at generation time. The methods below prefer those explicit values and fall
// back to lightweight heuristics for outputs created before the BAML enrichment.
type Artifact struct {
	o *types.TaskOutput
}

func ArtifactOf(o *types.TaskOutput) Artifact { return Artifact{o: o} }

func (a Artifact) Key() string {
	if a.o == nil {
		return ""
	}
	if explicit := normalizeToken(meta(a.o, types.TaskOutputMetadataArtifactKey)); explicit != "" {
		return explicit
	}
	kind := a.Kind()
	outputType := normalizeToken(a.o.OutputType)
	switch {
	case kind != "" && kind != outputType:
		return kind
	case outputType != "":
		return outputType
	default:
		return ""
	}
}

func (a Artifact) Label() string {
	if a.o == nil {
		return ""
	}
	if explicit := strings.TrimSpace(meta(a.o, types.TaskOutputMetadataArtifactLabel)); explicit != "" {
		return explicit
	}
	kind := humanizeToken(a.Kind())
	if kind != "" {
		return pluralize(kind)
	}
	return pluralize(humanizeToken(a.o.OutputType))
}

func (a Artifact) Kind() string {
	if a.o == nil {
		return ""
	}
	if kind := normalizeToken(meta(a.o, types.TaskOutputMetadataArtifactKind)); kind != "" {
		return kind
	}
	if kind := normalizeToken(meta(a.o, "classifier_kind")); kind != "" {
		return kind
	}
	if ext := a.fileExt(); ext != "" {
		return ext
	}
	if lk := a.linkKind(); lk != "" {
		return lk
	}
	return normalizeToken(a.o.OutputType)
}

func (a Artifact) MatchesKey(artifactKey string) bool {
	return normalizeToken(a.Key()) == normalizeToken(artifactKey)
}

// ---------------------------------------------------------------------------
// Private derivation helpers
// ---------------------------------------------------------------------------

func (a Artifact) schemaKey() string {
	keys := sortedMapKeys(a.o.Data)
	if len(keys) == 0 {
		return ""
	}
	raw, err := json.Marshal(keys)
	if err != nil {
		return ""
	}
	sum := sha1.Sum(raw)
	return hex.EncodeToString(sum[:])[:8]
}

func (a Artifact) filePath() string {
	for _, path := range []string{
		toString(dotGet(a.o.Data, "path")),
		toString(dotGet(a.o.Metadata, "path")),
	} {
		if strings.TrimSpace(path) != "" {
			return strings.TrimSpace(path)
		}
	}
	return ""
}

func (a Artifact) fileExt() string {
	path := a.filePath()
	if path == "" {
		return ""
	}
	return normalizeToken(strings.TrimPrefix(filepath.Ext(path), "."))
}

func (a Artifact) linkKind() string {
	rawURI := a.uri()
	if rawURI == "" {
		return ""
	}
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return ""
	}
	host := strings.ToLower(parsed.Hostname())
	switch {
	case strings.Contains(host, "drive.google.com"):
		return "drive-link"
	case host != "":
		parts := strings.Split(host, ".")
		return normalizeToken(parts[0] + "-link")
	default:
		return "link"
	}
}

func (a Artifact) uri() string {
	if a.o.URI != nil && strings.TrimSpace(*a.o.URI) != "" {
		return strings.TrimSpace(*a.o.URI)
	}
	for _, v := range []string{
		toString(dotGet(a.o.Data, "uri")),
		toString(dotGet(a.o.Metadata, "deeplink")),
	} {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Standalone helpers
// ---------------------------------------------------------------------------

func OutputMatchesDataSource(output *types.TaskOutput, ds *types.DataSource) bool {
	if output == nil || ds == nil {
		return false
	}
	if ds.OutputType != "" && !strings.EqualFold(strings.TrimSpace(ds.OutputType), strings.TrimSpace(output.OutputType)) {
		return false
	}
	if ds.ArtifactKey != "" && !ArtifactOf(output).MatchesKey(ds.ArtifactKey) {
		return false
	}
	refs := uniqueTrimmedStrings(append([]string{ds.AgentID}, ds.AgentIDs...))
	if len(refs) == 0 {
		return true
	}
	if output.AgentID == nil || strings.TrimSpace(*output.AgentID) == "" {
		return false
	}
	aid := strings.TrimSpace(*output.AgentID)
	for _, ref := range refs {
		if ref == aid {
			return true
		}
	}
	return false
}

func sortedMapKeys(m map[string]any) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ---------------------------------------------------------------------------
// Metadata accessors
// ---------------------------------------------------------------------------

func meta(o *types.TaskOutput, key string) string {
	if o == nil || o.Metadata == nil {
		return ""
	}
	return toString(o.Metadata[key])
}

func toString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return ""
	}
}

// ---------------------------------------------------------------------------
// Token normalization
// ---------------------------------------------------------------------------

func normalizeToken(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

func humanizeToken(value string) string {
	parts := strings.Split(normalizeToken(value), "-")
	for i, part := range parts {
		switch strings.ToLower(part) {
		case "pdf", "html", "json", "csv", "url":
			parts[i] = strings.ToUpper(part)
		default:
			if len(part) > 0 {
				parts[i] = strings.ToUpper(part[:1]) + part[1:]
			}
		}
	}
	return strings.Join(parts, " ")
}

func pluralize(label string) string {
	if label == "" || strings.HasSuffix(label, "s") || strings.HasSuffix(label, "S") {
		return label
	}
	return label + "s"
}

func isGenericKey(key string) bool {
	switch normalizeToken(key) {
	case "", "file", "text", "json", "link", "email", "report":
		return true
	default:
		return false
	}
}

func isGenericToken(value string) bool {
	switch normalizeToken(value) {
	case "", "file", "text", "json", "link", "email", "report", "output", "outputs", "result", "results", "artifact":
		return true
	default:
		return false
	}
}
