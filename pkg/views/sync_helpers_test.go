package views

import (
	"testing"

	viewbamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"
)

func TestBuildSearchCriteriaUsesPlanCriteria(t *testing.T) {
	plan := &viewbamltypes.RowSearchPlan{
		Criteria: []viewbamltypes.SearchCriterion{
			{Column: "property_address", Value: "2539 Telegraph Ave"},
			{Column: "property_address", Value: "2539 Telegraph Ave"},
			{Column: "leasing_broker", Value: "Cody Maxwell"},
		},
	}

	criteria := buildSearchCriteria(plan)
	if len(criteria) != 2 {
		t.Fatalf("expected 2 criteria, got %d: %#v", len(criteria), criteria)
	}
	if criteria[0].Value != "2539 Telegraph Ave" || criteria[1].Value != "Cody Maxwell" {
		t.Fatalf("unexpected criteria values: %#v", criteria)
	}
}

func TestEntityHintsPrefersClassifierUnmatched(t *testing.T) {
	plan := &viewbamltypes.RowSearchPlan{
		Entity_labels: []string{"Cody Maxwell", "Inessa Romano"},
	}
	hints := entityHints(plan, []string{"2539 Telegraph Ave", "320 Hillcrest Rd"})
	if len(hints) != 2 {
		t.Fatalf("expected 2 preferred hints, got %d: %#v", len(hints), hints)
	}
	if hints[0] != "2539 Telegraph Ave" || hints[1] != "320 Hillcrest Rd" {
		t.Fatalf("unexpected hints: %#v", hints)
	}
}

func TestVectorQueryTextsIncludesCriteriaAndHints(t *testing.T) {
	queries := vectorQueryTexts(
		[]SearchCriterion{
			{Column: "property_address", Value: "2539 Telegraph Ave"},
			{Column: "leasing_broker", Value: "Cody Maxwell"},
		},
		[]string{"320 Hillcrest Rd"},
		"text",
		"Outreach Summary",
		"",
		`{"status":"sent"}`,
	)

	if len(queries) < 4 {
		t.Fatalf("expected multiple vector queries, got %#v", queries)
	}
}

func TestDeriveRowKeyIsDeterministicAndOrderIndependent(t *testing.T) {
	a := deriveRowKey(map[string]string{
		"city":             "Berkeley",
		"property_address": "2539 Telegraph Ave",
		"status":           "sent",
	})
	b := deriveRowKey(map[string]string{
		"status":           "sent",
		"property_address": "2539 Telegraph Ave",
		"city":             "Berkeley",
	})
	c := deriveRowKey(map[string]string{
		"property_address": "320 Hillcrest Rd",
		"city":             "Hollister",
		"status":           "sent",
	})

	if a == "" || b == "" || c == "" {
		t.Fatalf("expected non-empty row keys")
	}
	if a != b {
		t.Fatalf("expected order-independent row keys: %q vs %q", a, b)
	}
	if a == c {
		t.Fatalf("expected different content to produce different row keys")
	}
}
