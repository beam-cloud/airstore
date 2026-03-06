package apiv1

import "testing"

func TestIsSimpleListRunsQuery(t *testing.T) {
	if !isSimpleListRunsQuery("", "", "", "", "", "", "", "", "") {
		t.Fatalf("expected empty query to be simple")
	}
	if isSimpleListRunsQuery("", "", "", "", "", "", "", "2026-03-05T00:00:00Z", "") {
		t.Fatalf("expected updated_after to force filtered mode")
	}
	if isSimpleListRunsQuery("", "", "", "", "", "", "", "", "2026-03-05T00:00:00Z") {
		t.Fatalf("expected updated_before to force filtered mode")
	}
}
