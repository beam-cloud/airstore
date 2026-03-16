package views

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestFieldsForOutputScansAllArrayObjects(t *testing.T) {
	output := &types.TaskOutput{
		Data: map[string]any{
			"items": []any{
				map[string]any{"name": "alpha"},
				map[string]any{"price": "10"},
			},
		},
	}

	fields := fieldsForOutput(output)
	sources := make(map[string]bool, len(fields))
	for _, field := range fields {
		sources[field.Source] = true
	}

	for _, want := range []string{"data.items", "data.items.[].name", "data.items.[].price"} {
		if !sources[want] {
			t.Fatalf("missing schema source %q in %#v", want, fields)
		}
	}
}

func TestFieldsForOutputCapsArrayObjectSampling(t *testing.T) {
	items := make([]any, 0, maxArrayObjectSchemaSamples+1)
	for i := 0; i < maxArrayObjectSchemaSamples; i++ {
		items = append(items, map[string]any{"name": "alpha"})
	}
	items = append(items, map[string]any{"late_field": "value"})

	output := &types.TaskOutput{
		Data: map[string]any{
			"items": items,
		},
	}

	fields := fieldsForOutput(output)
	sources := make(map[string]bool, len(fields))
	for _, field := range fields {
		sources[field.Source] = true
	}

	if sources["data.items.[].late_field"] {
		t.Fatalf("unexpected late sampled field in %#v", fields)
	}
}

func TestSummarizeOutputSchemaFromDataKeys(t *testing.T) {
	output := &types.TaskOutput{
		ID:         "out-1",
		OutputType: "text",
		Title:      "Recipe report",
		Data: map[string]any{
			"recipe_name": "Spaghetti in Tomato Water",
			"video_url":   "https://example.com/video",
		},
		Metadata: map[string]any{
			types.TaskOutputMetadataArtifactKey:   "recipes",
			types.TaskOutputMetadataArtifactLabel: "Recipes",
		},
	}

	summary := summarizeOutputSchema([]*types.TaskOutput{output})
	if got, want := summary.ArtifactKey, "recipes"; got != want {
		t.Fatalf("artifact key = %q, want %q", got, want)
	}
	fieldSources := map[string]bool{}
	for _, f := range summary.Fields {
		fieldSources[f.Source] = true
	}
	if !fieldSources["data.recipe_name"] {
		t.Fatal("expected data.recipe_name in fields")
	}
	if !fieldSources["data.video_url"] {
		t.Fatal("expected data.video_url in fields")
	}
}

func TestSummarizeOutputSchemaFallsBackToDataKeys(t *testing.T) {
	output := &types.TaskOutput{
		ID:         "out-1",
		OutputType: "text",
		Title:      "Some output",
		Data: map[string]any{
			"name":    "Test",
			"url":     "https://example.com",
			"created": "2024-01-01",
		},
		Metadata: map[string]any{},
	}

	summary := summarizeOutputSchema([]*types.TaskOutput{output})
	fieldSources := map[string]bool{}
	for _, f := range summary.Fields {
		fieldSources[f.Source] = true
	}

	if !fieldSources["data.name"] {
		t.Fatal("expected data.name in fallback fields")
	}
	if !fieldSources["data.url"] {
		t.Fatal("expected data.url in fallback fields")
	}

	for _, f := range summary.Fields {
		if f.Source == "data.url" && f.Type != "link" {
			t.Fatalf("expected data.url type=link, got %q", f.Type)
		}
	}
}
