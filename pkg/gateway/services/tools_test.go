package services

import (
	"strings"
	"testing"
)

func TestBuildToolCallSummaryAndDetailsForAgentMailSend(t *testing.T) {
	summary, details := buildToolCallSummaryAndDetails(
		"agentmail",
		[]string{"send", "sender@agentmail.to", "lead@example.com", "Hello", "Body copy"},
		nil,
	)

	if !strings.Contains(summary, "sender@agentmail.to") || !strings.Contains(summary, "lead@example.com") {
		t.Fatalf("unexpected summary: %q", summary)
	}
	for _, fragment := range []string{"**From:** sender@agentmail.to", "**To:** lead@example.com", "**Subject:** Hello", "Body copy"} {
		if !strings.Contains(details, fragment) {
			t.Fatalf("details %q missing fragment %q", details, fragment)
		}
	}
}

func TestBuildToolCallSummaryAndDetailsForAgentMailReply(t *testing.T) {
	summary, details := buildToolCallSummaryAndDetails(
		"agentmail",
		[]string{"reply", "sender@agentmail.to", "msg-123", "Thanks"},
		nil,
	)

	if !strings.Contains(summary, "sender@agentmail.to") || !strings.Contains(summary, "msg-123") {
		t.Fatalf("unexpected summary: %q", summary)
	}
	for _, fragment := range []string{"**From:** sender@agentmail.to", "**Message ID:** msg-123", "Thanks"} {
		if !strings.Contains(details, fragment) {
			t.Fatalf("details %q missing fragment %q", details, fragment)
		}
	}
}
