package views

import "testing"

func TestMongoColumnFieldPathPreservesNonEmptyWhitespace(t *testing.T) {
	got, err := mongoColumnFieldPath("cells", "  spaced key  ")
	if err != nil {
		t.Fatalf("mongoColumnFieldPath returned error: %v", err)
	}
	if want := "cells.  spaced key  "; got != want {
		t.Fatalf("mongoColumnFieldPath = %q, want %q", got, want)
	}
}

func TestMongoColumnFieldPathRejectsWhitespaceOnlyKey(t *testing.T) {
	if _, err := mongoColumnFieldPath("cells", "   "); err == nil {
		t.Fatal("expected whitespace-only key to be rejected")
	}
}
