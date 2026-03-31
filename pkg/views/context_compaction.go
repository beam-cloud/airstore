package views

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	compactionThreshold    = 20
	compactionModel        = "gpt-4o-mini"
	compactionAPIURL       = "https://api.openai.com/v1/chat/completions"
	compactionSystemPrompt = `You are a context manager for an AI agent workspace. Given a list of context entries (feedback, notes, links, and any prior compaction summaries), produce a concise, actionable summary that an AI agent should treat as standing instructions when working in this project.

Rules:
- Preserve every concrete fact (names, emails, dates, preferences, constraints).
- Merge duplicates and contradictions (latest wins).
- Use bullet-point format. Each bullet should be self-contained.
- Omit meta-commentary about the summarization process.
- If a prior compaction summary is included, integrate its content rather than nesting summaries.`
)

// ContextCompactor reads a view's context stream, optionally compacts it via
// LLM summarization, and provides the compacted context for injection.
type ContextCompactor struct {
	s2     *common.S2Client
	apiKey string
	http   *http.Client
}

func NewContextCompactor(s2 *common.S2Client, openAIKey string) *ContextCompactor {
	return &ContextCompactor{
		s2:     s2,
		apiKey: openAIKey,
		http:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (cc *ContextCompactor) Available() bool {
	return cc != nil && cc.s2 != nil && cc.s2.Enabled()
}

// ReadContext reads the view context stream and returns the effective entries:
// the last compaction summary (if any) plus all entries appended after it.
func (cc *ContextCompactor) ReadContext(ctx context.Context, viewID string) ([]types.ViewContextEntry, error) {
	if !cc.Available() {
		return nil, nil
	}

	const pageSize = 1000
	const maxPages = 50
	stream := common.Streams.ViewContext(viewID)

	var entries []types.ViewContextEntry
	seqNum := int64(0)
	for page := 0; page < maxPages; page++ {
		records, err := cc.s2.Read(ctx, stream, seqNum, pageSize)
		if err != nil {
			return nil, fmt.Errorf("read view context stream: %w", err)
		}
		if len(records) == 0 {
			break
		}
		for _, r := range records {
			var e types.ViewContextEntry
			if json.Unmarshal([]byte(r.Body), &e) == nil {
				e.SeqNum = r.SeqNum
				entries = append(entries, e)
			}
			if r.SeqNum >= seqNum {
				seqNum = r.SeqNum + 1
			}
		}
		if len(records) < pageSize {
			break
		}
	}

	// Find the last compaction entry and return it + everything after.
	lastCompaction := -1
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].EntryType == types.ViewContextEntryCompaction {
			lastCompaction = i
			break
		}
	}
	if lastCompaction >= 0 {
		return entries[lastCompaction:], nil
	}
	return entries, nil
}

// AppendEntry appends a context entry to the view's S2 stream.
func (cc *ContextCompactor) AppendEntry(ctx context.Context, entry types.ViewContextEntry) error {
	if !cc.Available() {
		return nil
	}
	return cc.s2.Append(ctx, common.Streams.ViewContext(entry.ViewID), entry)
}

// ShouldCompact returns true if there are enough raw entries since the last
// compaction to justify a new compaction pass.
func (cc *ContextCompactor) ShouldCompact(entries []types.ViewContextEntry) bool {
	raw := 0
	for _, e := range entries {
		if e.EntryType != types.ViewContextEntryCompaction {
			raw++
		}
	}
	return raw >= compactionThreshold
}

// Compact summarizes the given entries via LLM and appends a compaction frame.
func (cc *ContextCompactor) Compact(ctx context.Context, viewID string, entries []types.ViewContextEntry) error {
	if cc.apiKey == "" {
		return fmt.Errorf("compaction requires OpenAI API key")
	}
	if len(entries) == 0 {
		return nil
	}

	var maxSeq int64
	var lines []string
	for _, e := range entries {
		if e.SeqNum > maxSeq {
			maxSeq = e.SeqNum
		}
		prefix := e.EntryType
		if prefix == "" {
			prefix = "note"
		}
		anchor := ""
		if ap := formatAnchorPrefix(e); ap != "" {
			anchor = " " + strings.TrimSpace(ap)
		}
		lines = append(lines, fmt.Sprintf("[%s%s] %s", prefix, anchor, e.Content))
	}
	userMsg := strings.Join(lines, "\n")

	summary, err := cc.llmSummarize(ctx, userMsg)
	if err != nil {
		return fmt.Errorf("compaction LLM call: %w", err)
	}

	compactionEntry := types.ViewContextEntry{
		ID:          fmt.Sprintf("compact-%d", time.Now().UnixMilli()),
		ViewID:      viewID,
		Timestamp:   time.Now().UnixMilli(),
		EntryType:   types.ViewContextEntryCompaction,
		Content:     summary,
		CompactUpTo: maxSeq,
	}
	if err := cc.AppendEntry(ctx, compactionEntry); err != nil {
		return fmt.Errorf("append compaction entry: %w", err)
	}

	log.Info().
		Str("view_id", viewID).
		Int64("compact_up_to", maxSeq).
		Int("source_entries", len(entries)).
		Msg("view context compacted")
	return nil
}

// FormatForPrompt renders context entries as a markdown section suitable for
// injection into an agent's system prompt.
func FormatForPrompt(entries []types.ViewContextEntry) string {
	if len(entries) == 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString("Project context (standing instructions from prior work and feedback):\n")
	for _, e := range entries {
		content := strings.TrimSpace(e.Content)
		if content == "" {
			continue
		}
		if e.EntryType == types.ViewContextEntryCompaction {
			b.WriteString(content)
			b.WriteByte('\n')
		} else {
			b.WriteString("- ")
			if prefix := formatAnchorPrefix(e); prefix != "" {
				b.WriteString(prefix)
			}
			b.WriteString(content)
			b.WriteByte('\n')
		}
	}
	return strings.TrimSpace(b.String())
}

// formatAnchorPrefix builds a bracketed label when a context entry is anchored
// to a specific email thread (or other anchor type in the future).
func formatAnchorPrefix(e types.ViewContextEntry) string {
	if len(e.Metadata) == 0 {
		return ""
	}
	anchorType, _ := e.Metadata["anchor_type"].(string)
	if anchorType != "email" {
		return ""
	}

	recipient, _ := e.Metadata["recipient"].(string)
	subject, _ := e.Metadata["subject"].(string)

	var label string
	switch e.EntryType {
	case types.ViewContextEntryFeedback:
		label = "Feedback on email"
	case types.ViewContextEntryLink:
		label = "Link shared on email"
	default:
		label = "Note on email"
	}

	if recipient != "" {
		label += " to " + recipient
	}
	if subject != "" {
		label += fmt.Sprintf(", re: %q", subject)
	}

	return "[" + label + "] "
}

// FilterByThreadID returns entries whose metadata.thread_id matches the given
// value. Compaction entries are excluded since they are aggregate summaries.
func FilterByThreadID(entries []types.ViewContextEntry, threadID string) []types.ViewContextEntry {
	var out []types.ViewContextEntry
	for _, e := range entries {
		if e.EntryType == types.ViewContextEntryCompaction {
			continue
		}
		tid, _ := e.Metadata["thread_id"].(string)
		if tid == threadID {
			out = append(out, e)
		}
	}
	return out
}

// FeedbackCountsByThread scans entries and returns a map of thread_id to the
// number of feedback/link/note entries anchored to that thread.
func FeedbackCountsByThread(entries []types.ViewContextEntry) map[string]int {
	counts := make(map[string]int)
	for _, e := range entries {
		if e.EntryType == types.ViewContextEntryCompaction {
			continue
		}
		tid, _ := e.Metadata["thread_id"].(string)
		if tid != "" {
			counts[tid]++
		}
	}
	return counts
}

// ---------------------------------------------------------------------------
// OpenAI chat completion (minimal, self-contained)
// ---------------------------------------------------------------------------

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
}

func (cc *ContextCompactor) llmSummarize(ctx context.Context, userContent string) (string, error) {
	body, err := json.Marshal(chatRequest{
		Model: compactionModel,
		Messages: []chatMessage{
			{Role: "system", Content: compactionSystemPrompt},
			{Role: "user", Content: userContent},
		},
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", compactionAPIURL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cc.apiKey)

	resp, err := cc.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("OpenAI API error %d: %s", resp.StatusCode, string(b))
	}

	var result chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if len(result.Choices) == 0 {
		return "", fmt.Errorf("no choices in response")
	}
	return strings.TrimSpace(result.Choices[0].Message.Content), nil
}
