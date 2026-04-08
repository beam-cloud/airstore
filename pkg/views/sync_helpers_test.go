package views

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
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

func TestSkipOutputSkipsApprovalBackedEmailDrafts(t *testing.T) {
	output := &types.TaskOutput{
		OutputType: "email",
		Title:      "Draft Email - 925 3rd St",
		Metadata: map[string]any{
			types.TaskOutputMetadataBlockingKind: "approval",
			types.TaskOutputMetadataArtifactKind: "email-draft",
		},
	}
	if !skipOutput(output) {
		t.Fatalf("expected draft approval email output to be skipped")
	}
}

func TestOutputAllowsInsertDisablesEmailArtifacts(t *testing.T) {
	cases := []struct {
		name   string
		output *types.TaskOutput
		want   bool
	}{
		{
			name: "sent email output is update-only",
			output: &types.TaskOutput{
				OutputType: "email",
				Metadata: map[string]any{
					types.TaskOutputMetadataArtifactKind: "email",
				},
			},
			want: false,
		},
		{
			name: "email-like artifact kind is update-only",
			output: &types.TaskOutput{
				OutputType: "json",
				Metadata: map[string]any{
					types.TaskOutputMetadataArtifactKind: "email-thread",
				},
			},
			want: false,
		},
		{
			name: "row-centric json output may insert",
			output: &types.TaskOutput{
				OutputType: "json",
				Metadata: map[string]any{
					types.TaskOutputMetadataArtifactKind: "crm-update",
				},
			},
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := outputAllowsInsert(tc.output); got != tc.want {
				t.Fatalf("outputAllowsInsert() = %v, want %v", got, tc.want)
			}
		})
	}
}
