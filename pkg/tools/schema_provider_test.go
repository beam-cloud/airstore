package tools

import "testing"

func intPtr(v int) *int {
	return &v
}

func TestSchemaProviderParseArgsEnforcesOneOfRequired(t *testing.T) {
	provider := NewSchemaProvider(&ToolSchema{Name: "gdrive", Description: "test"}, nil)
	cmd := &CommandSchema{
		Description: "write-file",
		OneOfRequired: [][]string{
			{"content", "content_base64"},
		},
		Params: []*ParamSchema{
			{Name: "content", Type: "string", Position: intPtr(0)},
			{Name: "content_base64", Type: "string", Flag: "--content-base64"},
		},
	}

	if _, err := provider.parseArgs(cmd, nil); err == nil {
		t.Fatal("expected missing one-of argument to fail")
	}

	if _, err := provider.parseArgs(cmd, []string{"hello", "--content-base64", "aGVsbG8="}); err == nil {
		t.Fatal("expected both one-of arguments to fail")
	}

	args, err := provider.parseArgs(cmd, []string{"hello"})
	if err != nil {
		t.Fatalf("expected single one-of argument to succeed: %v", err)
	}
	if got := args["content"]; got != "hello" {
		t.Fatalf("content = %#v, want hello", got)
	}
}
