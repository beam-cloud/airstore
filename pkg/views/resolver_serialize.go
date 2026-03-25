package views

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"html"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	bamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"
)

var ansiEscapeRe = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]`)
var markupTagRe = regexp.MustCompile(`(?s)<[^>]+>`)
var whitespaceRe = regexp.MustCompile(`\s+`)

type mappingField struct {
	Path  string
	Value string
}

const (
	maxMappingFieldValueLen = 1200
	maxMappingNestedItems   = 6
)

func serializeOutputsForMapping(outputs []*types.TaskOutput, taskPrompts map[string]string) (string, error) {
	grouped := make(map[string][]*types.TaskOutput)
	for _, o := range outputs {
		if o == nil {
			continue
		}
		grouped[o.TaskID] = append(grouped[o.TaskID], o)
	}

	taskIDs := make([]string, 0, len(grouped))
	for taskID := range grouped {
		taskIDs = append(taskIDs, taskID)
	}
	sort.Strings(taskIDs)

	var b strings.Builder
	for i, taskID := range taskIDs {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString("<<<BEGIN_TASK id=")
		b.WriteString(taskID)
		b.WriteString(">>>\n")
		writeMappingLine(&b, "PROMPT", taskPrompts[taskID])

		group := grouped[taskID]
		sort.SliceStable(group, func(i, j int) bool {
			if !group[i].CreatedAt.Equal(group[j].CreatedAt) {
				return group[i].CreatedAt.Before(group[j].CreatedAt)
			}
			return group[i].ID < group[j].ID
		})
		for _, output := range group {
			writeTaskOutputForMapping(&b, output)
		}
		b.WriteString("<<<END_TASK>>>\n")
	}
	return b.String(), nil
}

func writeTaskOutputForMapping(b *strings.Builder, output *types.TaskOutput) {
	if output == nil {
		return
	}
	b.WriteString("<<<BEGIN_OUTPUT id=")
	b.WriteString(output.ID)
	b.WriteString(">>>\n")
	if output.Status != "" && output.Status != types.TaskOutputStatusActive {
		writeMappingLine(b, "STATUS", output.Status)
	}
	if ak, _ := output.Metadata[types.TaskOutputMetadataArtifactKey].(string); ak != "" {
		writeMappingLine(b, "ARTIFACT_KEY", ak)
	}
	writeMappingLine(b, "TITLE", output.Title)
	writeMappingLine(b, "OUTPUT_TYPE", output.OutputType)
	writeMappingLine(b, "AGENT_NAME", output.AgentName)
	writeMappingLine(b, "CREATED_AT", output.CreatedAt.Format(time.RFC3339))
	if output.Summary != nil {
		writeMappingLine(b, "SUMMARY", *output.Summary)
	}
	if output.URI != nil {
		writeMappingLine(b, "URI", *output.URI)
	}
	writeMappingSection(b, "DATA_FIELDS", collectMappingFields(filterInternalKeys(output.Data)))
	writeMappingSection(b, "METADATA_FIELDS", collectMappingFields(filterInternalKeys(output.Metadata)))
	b.WriteString("<<<END_OUTPUT>>>\n")
}

func outputGroupSignature(outputs []*types.TaskOutput) string {
	if len(outputs) == 0 {
		return ""
	}
	group := dedupeOutputs(outputs)
	sort.SliceStable(group, func(i, j int) bool {
		if !group[i].CreatedAt.Equal(group[j].CreatedAt) {
			return group[i].CreatedAt.Before(group[j].CreatedAt)
		}
		return group[i].ID < group[j].ID
	})
	var b strings.Builder
	for _, output := range group {
		writeTaskOutputForMapping(&b, output)
	}
	if b.Len() == 0 {
		return ""
	}
	h := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(h[:])[:16]
}

func filterInternalKeys(m map[string]any) map[string]any {
	if len(m) == 0 {
		return m
	}
	filtered := make(map[string]any, len(m))
	for k, v := range m {
		if !strings.HasPrefix(k, "_") {
			filtered[k] = v
		}
	}
	return filtered
}

func writeMappingLine(b *strings.Builder, key, value string) {
	if b == nil {
		return
	}
	value = sanitizeMappingScalar(value)
	if value == "" {
		return
	}
	b.WriteString(key)
	b.WriteString(": ")
	b.WriteString(value)
	b.WriteByte('\n')
}

func writeMappingSection(b *strings.Builder, title string, fields []mappingField) {
	if b == nil || len(fields) == 0 {
		return
	}
	b.WriteString(title)
	b.WriteString(":\n")
	for _, field := range fields {
		if strings.TrimSpace(field.Path) == "" || strings.TrimSpace(field.Value) == "" {
			continue
		}
		b.WriteString("- ")
		b.WriteString(field.Path)
		b.WriteString(": ")
		b.WriteString(field.Value)
		b.WriteByte('\n')
	}
}

func collectMappingFields(values map[string]any) []mappingField {
	if len(values) == 0 {
		return nil
	}
	var out []mappingField
	collectMappingFieldsFromValue(&out, "", values)
	return out
}

func collectMappingFieldsFromValue(out *[]mappingField, path string, value any) {
	switch val := value.(type) {
	case nil:
		return
	case map[string]any:
		if path != "" && shouldCondenseMappingPath(path) {
			if excerpt := summarizeMappingExcerpt(val); excerpt != "" {
				*out = append(*out, mappingField{Path: path + "_excerpt", Value: excerpt})
			}
			return
		}
		keys := make([]string, 0, len(val))
		for key := range val {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			childPath := key
			if path != "" {
				childPath = path + "." + key
			}
			collectMappingFieldsFromValue(out, childPath, val[key])
		}
	case []string:
		if summary := summarizeScalarSlice(val); summary != "" {
			*out = append(*out, mappingField{Path: path, Value: summary})
		}
	case []map[string]any:
		if path != "" && shouldCondenseMappingPath(path) {
			if excerpt := summarizeMappingExcerpt(val); excerpt != "" {
				*out = append(*out, mappingField{Path: path + "_excerpt", Value: excerpt})
			}
			return
		}
		if strings.HasSuffix(path, "data_fields") {
			if summary := summarizeDataFields(val); summary != "" {
				*out = append(*out, mappingField{Path: path, Value: summary})
			}
			return
		}
		for i, item := range val {
			if i >= maxMappingNestedItems {
				break
			}
			childPath := fmt.Sprintf("%s[%d]", path, i)
			collectMappingFieldsFromValue(out, childPath, item)
		}
	case []any:
		if path != "" && shouldCondenseMappingPath(path) {
			if excerpt := summarizeMappingExcerpt(val); excerpt != "" {
				*out = append(*out, mappingField{Path: path + "_excerpt", Value: excerpt})
			}
			return
		}
		if strings.HasSuffix(path, "data_fields") {
			if summary := summarizeDataFields(val); summary != "" {
				*out = append(*out, mappingField{Path: path, Value: summary})
			}
			return
		}
		if summary := summarizeScalarSlice(val); summary != "" {
			*out = append(*out, mappingField{Path: path, Value: summary})
			return
		}
		for i, item := range val {
			if i >= maxMappingNestedItems {
				break
			}
			childPath := fmt.Sprintf("%s[%d]", path, i)
			collectMappingFieldsFromValue(out, childPath, item)
		}
	default:
		if path != "" && shouldCondenseMappingPath(path) {
			if excerpt := summarizeMappingExcerpt(val); excerpt != "" {
				*out = append(*out, mappingField{Path: path + "_excerpt", Value: excerpt})
			}
			return
		}
		if text := sanitizeMappingScalar(fmt.Sprint(val)); text != "" && text != "<nil>" {
			*out = append(*out, mappingField{Path: path, Value: text})
		}
	}
}

func shouldCondenseMappingPath(path string) bool {
	leaf := path
	if i := strings.LastIndexByte(path, '.'); i >= 0 {
		leaf = path[i+1:]
	}
	return leaf == "source_input" || leaf == "source_input_text" || leaf == "source_excerpt"
}

func summarizeDataFields(value any) string {
	var items []any
	switch fields := value.(type) {
	case []any:
		items = fields
	case []map[string]any:
		items = make([]any, 0, len(fields))
		for _, field := range fields {
			items = append(items, field)
		}
	default:
		return ""
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		field, ok := item.(map[string]any)
		if !ok {
			continue
		}
		key := sanitizeMappingScalar(fmt.Sprint(field["key"]))
		if key == "" {
			continue
		}
		label := sanitizeMappingScalar(fmt.Sprint(field["label"]))
		typ := sanitizeMappingScalar(fmt.Sprint(field["type"]))
		part := key
		if label != "" || typ != "" {
			part += " ["
			if label != "" {
				part += label
			}
			if typ != "" {
				if label != "" {
					part += ": "
				}
				part += typ
			}
			part += "]"
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, "; ")
}

func summarizeScalarSlice(values any) string {
	var items []string
	switch vals := values.(type) {
	case []string:
		items = vals
	case []any:
		items = make([]string, 0, len(vals))
		for _, raw := range vals {
			switch v := raw.(type) {
			case nil:
				continue
			case map[string]any, []any:
				return ""
			default:
				if text := sanitizeMappingScalar(fmt.Sprint(v)); text != "" {
					items = append(items, text)
				}
			}
		}
	default:
		return ""
	}
	if len(items) == 0 {
		return ""
	}
	if len(items) > maxMappingNestedItems {
		items = items[:maxMappingNestedItems]
	}
	return strings.Join(items, ", ")
}

func summarizeMappingExcerpt(value any) string {
	switch val := value.(type) {
	case string:
		return sanitizeMappingScalar(val)
	case map[string]any:
		for _, key := range []string{"content", "command", "description", "path", "file_path"} {
			if child, ok := val[key]; ok {
				if excerpt := summarizeMappingExcerpt(child); excerpt != "" {
					return excerpt
				}
			}
		}
		keys := make([]string, 0, len(val))
		for key := range val {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			if len(parts) == 4 {
				break
			}
			if excerpt := summarizeMappingExcerpt(val[key]); excerpt != "" {
				parts = append(parts, key+": "+excerpt)
			}
		}
		return strings.Join(parts, " | ")
	case []string:
		return summarizeScalarSlice(val)
	case []map[string]any:
		if summary := summarizeDataFields(val); summary != "" {
			return summary
		}
		return ""
	case []any:
		return summarizeScalarSlice(val)
	default:
		if value == nil {
			return ""
		}
		return sanitizeMappingScalar(fmt.Sprint(value))
	}
}

func sanitizeMappingScalar(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = html.UnescapeString(value)
	value = ansiEscapeRe.ReplaceAllString(value, "")
	value = strings.ReplaceAll(value, "\u00a0", " ")
	if markupTagRe.MatchString(value) {
		value = markupTagRe.ReplaceAllString(value, " ")
	}
	value = whitespaceRe.ReplaceAllString(value, " ")
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	runes := []rune(value)
	if len(runes) > maxMappingFieldValueLen {
		return string(runes[:maxMappingFieldValueLen]) + "…"
	}
	return value
}

func serializeExistingRows(rows []ViewRow, cols []bamltypes.ColumnSchema) string {
	if len(rows) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, row := range rows {
		if row.Marker {
			continue
		}
		merged := row.MergedCells()
		isImport := row.IsImport()

		if isImport {
			fmt.Fprintf(&sb, "Row (row_key=%s, source=IMPORTED):\n", row.RowKey)
		} else if stableRef := strings.TrimSpace(row.StableRef); stableRef != "" {
			fmt.Fprintf(&sb, "Row (task_id=%s, row_key=%s, stable_ref=%s):\n", row.TaskID, row.RowKey, stableRef)
		} else {
			fmt.Fprintf(&sb, "Row (task_id=%s, row_key=%s):\n", row.TaskID, row.RowKey)
		}
		for _, col := range cols {
			val := merged[col.Key]
			if row.Manual[col.Key] != "" {
				fmt.Fprintf(&sb, "  - %s [key=%s]: %q [USER EDIT]\n", col.Name, col.Key, val)
			} else if isImport && row.Pinned[col.Key] != "" {
				fmt.Fprintf(&sb, "  - %s [key=%s]: %q [IMPORTED]\n", col.Name, col.Key, val)
			} else {
				fmt.Fprintf(&sb, "  - %s [key=%s]: %q\n", col.Name, col.Key, val)
			}
		}
	}
	return sb.String()
}

func serializeExcludedRows(snapshots []ExcludedRowSnapshot) string {
	if len(snapshots) == 0 {
		return ""
	}
	var sb strings.Builder
	for i, s := range snapshots {
		if strings.TrimSpace(s.ComponentID) != "" {
			fmt.Fprintf(&sb, "Excluded row %d (component_id=%s, task_id=%s, row_key=%s):\n", i+1, s.ComponentID, s.TaskID, s.RowKey)
		} else {
			fmt.Fprintf(&sb, "Excluded row %d (task_id=%s, row_key=%s):\n", i+1, s.TaskID, s.RowKey)
		}
		if len(s.SourceOutputIDs) > 0 {
			fmt.Fprintf(&sb, "  - source_output_ids: %q\n", strings.Join(s.SourceOutputIDs, ","))
		}
		for k, v := range s.Cells {
			if v != "" {
				fmt.Fprintf(&sb, "  - %s: %q\n", k, v)
			}
		}
	}
	return sb.String()
}
