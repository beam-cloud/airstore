package views

import (
	"fmt"
	"sort"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

type schemaFieldSummary struct {
	Key      string
	Source   string
	Type     string
	Label    string
	Coverage int
}

type outputSchemaSummary struct {
	ArtifactKey   string
	ArtifactLabel string
	OutputType    string
	Fields        []schemaFieldSummary
}

// Top-level fields available on every TaskOutput regardless of schema.
var topLevelFields = []schemaFieldSummary{
	{Key: "title", Source: "title", Type: "text", Label: "Title"},
	{Key: "summary", Source: "summary", Type: "text", Label: "Summary"},
	{Key: "uri", Source: "uri", Type: "link", Label: "Link"},
	{Key: "created_at", Source: "created_at", Type: "date", Label: "Created At"},
	{Key: "output_type", Source: "output_type", Type: "text", Label: "Output Type"},
	{Key: "artifact_key", Source: "artifact_key", Type: "text", Label: "Artifact Key"},
	{Key: "agent_name", Source: "agent_name", Type: "text", Label: "Agent Name"},
	{Key: "agent_id", Source: "agent_id", Type: "text", Label: "Agent ID"},
}

func summarizeOutputSchema(outputs []*types.TaskOutput) outputSchemaSummary {
	merged := make(map[string]*schemaFieldSummary)
	var artifactKey, artifactLabel, outputType string

	for _, output := range outputs {
		if output == nil {
			continue
		}
		a := ArtifactOf(output)
		if artifactKey == "" {
			artifactKey = a.Key()
		}
		if artifactLabel == "" {
			artifactLabel = a.Label()
		}
		if outputType == "" {
			outputType = strings.TrimSpace(output.OutputType)
		}

		for _, f := range fieldsForOutput(output) {
			if existing, ok := merged[f.Source]; ok {
				existing.Coverage++
				if existing.Key == "" {
					existing.Key = f.Key
				}
				if existing.Type == "" {
					existing.Type = f.Type
				}
				if existing.Label == "" {
					existing.Label = f.Label
				}
			} else {
				entry := f
				entry.Coverage = 1
				merged[f.Source] = &entry
			}
		}
	}

	fields := make([]schemaFieldSummary, 0, len(merged))
	for _, f := range merged {
		fields = append(fields, *f)
	}
	sort.Slice(fields, func(i, j int) bool {
		if fields[i].Coverage != fields[j].Coverage {
			return fields[i].Coverage > fields[j].Coverage
		}
		return fields[i].Source < fields[j].Source
	})

	return outputSchemaSummary{
		ArtifactKey:   artifactKey,
		ArtifactLabel: artifactLabel,
		OutputType:    outputType,
		Fields:        fields,
	}
}

func summarizeWorkspaceSchemas(outputs []*types.TaskOutput) []outputSchemaSummary {
	grouped := make(map[string][]*types.TaskOutput)
	for _, output := range outputs {
		if output == nil {
			continue
		}
		key := strings.TrimSpace(ArtifactOf(output).Key())
		if key == "" {
			key = "output-type:" + strings.TrimSpace(output.OutputType)
		}
		grouped[key] = append(grouped[key], output)
	}

	summaries := make([]outputSchemaSummary, 0, len(grouped))
	for _, group := range grouped {
		if s := summarizeOutputSchema(group); len(s.Fields) > 0 {
			summaries = append(summaries, s)
		}
	}
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].ArtifactKey < summaries[j].ArtifactKey
	})
	return summaries
}

// fieldsForOutput extracts the bindable field list from a single output
// by inspecting its data keys, flattening nested maps up to 3 levels deep.
func fieldsForOutput(output *types.TaskOutput) []schemaFieldSummary {
	base := make([]schemaFieldSummary, len(topLevelFields))
	copy(base, topLevelFields)
	flattenDataFields(output.Data, "data", &base, 0)
	return dedupeBySource(base)
}

func flattenDataFields(m map[string]any, prefix string, out *[]schemaFieldSummary, depth int) {
	if depth > 2 {
		return
	}
	for _, key := range sortedMapKeys(m) {
		if isExcludedDataKey(key) {
			continue
		}
		source := prefix + "." + key
		*out = append(*out, schemaFieldSummary{
			Key:    fallbackFieldKey(source),
			Source: source,
			Type:   inferTypeFromKey(key),
			Label:  humanizeColumn(key),
		})
		if nested, ok := m[key].(map[string]any); ok {
			flattenDataFields(nested, source, out, depth+1)
		}
		if arr, ok := m[key].([]any); ok && len(arr) > 0 {
			if nested, ok := arr[0].(map[string]any); ok {
				flattenDataFields(nested, source+".[]", out, depth+1)
			}
		}
	}
}

var excludedDataKeys = map[string]bool{
	"source_result": true, "source_excerpt": true,
	"source_input": true, "source_input_text": true,
}

func isExcludedDataKey(key string) bool {
	return excludedDataKeys[strings.ToLower(key)]
}

func inferTypeFromKey(key string) string {
	lower := strings.ToLower(key)
	switch {
	case strings.Contains(lower, "url"), strings.Contains(lower, "uri"),
		strings.Contains(lower, "link"), strings.Contains(lower, "deeplink"):
		return "link"
	case strings.Contains(lower, "email"):
		return "email"
	case strings.Contains(lower, "date"), strings.Contains(lower, "time"),
		strings.Contains(lower, "created"), strings.Contains(lower, "updated"):
		return "date"
	default:
		return "text"
	}
}

func dedupeBySource(fields []schemaFieldSummary) []schemaFieldSummary {
	seen := make(map[string]int, len(fields))
	out := make([]schemaFieldSummary, 0, len(fields))
	for _, f := range fields {
		f.Source = strings.TrimSpace(f.Source)
		if f.Source == "" {
			continue
		}
		if f.Key == "" {
			f.Key = fallbackFieldKey(f.Source)
		}
		if f.Label == "" {
			f.Label = humanizeColumn(f.Key)
		}
		if idx, ok := seen[f.Source]; ok {
			if out[idx].Label == humanizeColumn(out[idx].Key) && f.Label != "" {
				out[idx].Label = f.Label
			}
			continue
		}
		seen[f.Source] = len(out)
		out = append(out, f)
	}
	return out
}

func fallbackFieldKey(source string) string {
	key := strings.TrimPrefix(source, "data.")
	key = strings.TrimPrefix(key, "metadata.")
	key = strings.ReplaceAll(key, "[]", "")
	key = strings.ReplaceAll(key, ".", "_")
	return key
}

// writeWorkspaceSchemaSummaries formats schema summaries for the copilot prompt.
func writeWorkspaceSchemaSummaries(sb *strings.Builder, summaries []outputSchemaSummary) {
	if len(summaries) == 0 {
		return
	}
	sb.WriteString("\n" + strings.Repeat("─", 60) + "\n")
	sb.WriteString("ARTIFACT SCHEMAS (from persisted outputs)\n")
	sb.WriteString("Use dataSource.artifact_key to bind a component to one artifact family.\n")
	sb.WriteString("At render time, a BAML mapper dynamically maps output data into widget columns.\n")
	sb.WriteString("Transform rules serve as semantic hints — column names and types guide the mapper.\n")
	sb.WriteString(strings.Repeat("─", 60) + "\n")

	for _, s := range summaries {
		label := s.ArtifactLabel
		if label == "" {
			label = humanizeToken(s.ArtifactKey)
		}
		fmt.Fprintf(sb, "\n  artifact_key=%q  label=%q  output_type=%q\n", s.ArtifactKey, label, s.OutputType)
		sb.WriteString("    available fields:\n")
		for _, f := range s.Fields {
			fmt.Fprintf(sb, "      %s (%s, coverage=%d) — %s\n", f.Source, f.Type, f.Coverage, f.Label)
		}
	}
}
