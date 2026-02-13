package compression

import (
	"bytes"
	"context"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

var (
	// Shared
	reMailtoLine    = regexp.MustCompile(`(?m)^\s*mailto:\S+\s*$`)
	reHTMLComment   = regexp.MustCompile(`<!--[\s\S]*?-->`)
	reHTMLTag       = regexp.MustCompile(`</?[a-zA-Z][a-zA-Z0-9]*[^>]*>`)
	reHTMLEntity    = regexp.MustCompile(`&(?:[a-zA-Z]+|#\d+|#x[0-9a-fA-F]+);`)
	reMultiSpace    = regexp.MustCompile(`[^\S\n]{2,}`)
	reMultiNewline  = regexp.MustCompile(`\n{3,}`)
	reURLLinePrefix = regexp.MustCompile(`^https?://`)

	// Gmail
	reMIMEHeader   = regexp.MustCompile(`(?mi)^(Content-Type|Content-Transfer-Encoding|Content-Disposition|MIME-Version):.*$`)
	reMIMEBoundary = regexp.MustCompile(`(?m)^--[A-Za-z0-9_=+/-]+\s*$`)
	reBase64Block  = regexp.MustCompile(`(?m)^[A-Za-z0-9+/=]{76,}\s*$`)
	reSignature    = regexp.MustCompile(`(?m)^--\s*$`)
	reQuotedReply  = regexp.MustCompile(`(?m)^On .+wrote:\s*$`)
	reQuotedLine   = regexp.MustCompile(`(?m)^>.*$`)

	// GitHub
	reGHMetaLine   = regexp.MustCompile(`(?m)^\*\*(URL|Status|Comments):\*\*\s+.*$`)
	reDiffIndex    = regexp.MustCompile(`(?m)^index [0-9a-f]+\.\.[0-9a-f]+.*$`)
	reDiffMeta     = regexp.MustCompile(`(?m)^(old|new) mode \d+\s*$`)
	reDiffStat     = regexp.MustCompile(`(?m)^\s*\d+ files? changed,.*$`)
	reBinaryDiff   = regexp.MustCompile(`(?m)^Binary files .+ differ\s*$`)
	reDiffBlankAdd = regexp.MustCompile(`(?m)^[+-]\s*$`)

	// Slack
	reSlackReaction   = regexp.MustCompile(`(?m)^Reactions:.*$`)
	reSlackJoinLeave  = regexp.MustCompile(`(?mi)^.*(has joined|has left|was added to|was removed from) (the channel|#\S+).*$`)
	reSlackTopicSet   = regexp.MustCompile(`(?mi)^.*set the channel (topic|purpose|description).*$`)
	reSlackFileUpload = regexp.MustCompile(`(?mi)^.*uploaded a file:.*$`)
	reSlackEdited     = regexp.MustCompile(`\s*\(edited\)\s*`)

	// Notion
	reNotionMeta = regexp.MustCompile(`(?m)^\*\*(URL|Created|Last edited):\*\*\s+.*$`)

	// Linear
	reLinearMeta = regexp.MustCompile(`(?m)^\|\s*(Created|Updated|URL|Team|Project)\s*\|.*$`)
)

// Phrases that mark the start of legal/boilerplate footer sections in emails.
var gmailFooterMarkers = []string{
	"free shipping is valid", "shipping to stores:",
	"this is an advertising message", "confidentiality notice",
	"if you no longer wish", "to stop receiving",
	"you are receiving this email because",
	"this email was sent to you because",
	"reserves the right to suspend",
	"total savings and corresponding",
}

// Lines removed unconditionally from email bodies (case-insensitive exact match).
var gmailJunkLines = []string{
	"view in browser", "view online", "view email",
	"view this email", "unsubscribe",
	"unsubscribe here", "email preferences",
	"manage your preferences", "manage preferences",
}

// ---------------------------------------------------------------------------
// StripCompressor — the only exported type in this file.
// ---------------------------------------------------------------------------

type StripCompressor struct {
	counter *TokenCounter
}

func NewStripCompressor(cfg Config) *StripCompressor {
	return &StripCompressor{counter: newTokenCounter(cfg)}
}

func (s *StripCompressor) Name() Strategy { return StrategyStrip }

func (s *StripCompressor) Compress(ctx context.Context, content []byte, meta ContentMeta) (*CompressionResult, error) {
	start := time.Now()
	origTokens := s.counter.Count(content)

	data := stripNullBytes(content)
	src := types.SourceType(strings.ToLower(meta.Integration))

	switch src {
	case types.SourcePostHog:
		// Structured JSON — pass through unchanged.
	case types.SourceGmail:
		data = stripGmail(data)
	case types.SourceGitHub:
		data = stripGitHub(data)
	case types.SourceSlack:
		data = stripSlack(data)
	case types.SourceNotion:
		data = stripNotion(data)
	case types.SourceLinear:
		data = stripLinear(data)
	default:
		data = stripURLOnlyLines(data, 10)
	}

	if src != types.SourcePostHog {
		data = cleanup(data)
	}

	return &CompressionResult{
		Data:             data,
		OriginalTokens:   origTokens,
		CompressedTokens: s.counter.Count(data),
		Strategy:         StrategyStrip,
		Outcome:          OutcomeCompressed,
		DurationMs:       time.Since(start).Milliseconds(),
	}, nil
}

// ---------------------------------------------------------------------------
// Per-source strippers. Each one knows the format and removes noise.
// ---------------------------------------------------------------------------

func stripGmail(data []byte) []byte {
	header, body := splitAtMarker(data, "=== BODY ===")

	body = reMIMEHeader.ReplaceAll(body, nil)
	body = reMIMEBoundary.ReplaceAll(body, nil)
	body = reBase64Block.ReplaceAll(body, nil)

	if idx := reSignature.FindIndex(body); idx != nil {
		body = body[:idx[0]]
	}

	body = reQuotedReply.ReplaceAll(body, nil)
	body = reQuotedLine.ReplaceAll(body, nil)
	body = replaceHTMLEntities(body)
	body = dropLines(body, gmailJunkLines)
	body = stripURLOnlyLines(body, 3)
	body = collapseWhitespace(body)
	body = truncateAtFooter(body, gmailFooterMarkers, 0.2)

	return append(header, body...)
}

func stripGitHub(data []byte) []byte {
	data = reGHMetaLine.ReplaceAll(data, nil)
	data = reDiffIndex.ReplaceAll(data, nil)
	data = reDiffMeta.ReplaceAll(data, nil)
	data = reDiffStat.ReplaceAll(data, nil)
	data = reBinaryDiff.ReplaceAll(data, []byte("[binary file]"))
	data = reDiffBlankAdd.ReplaceAll(data, nil)
	return data
}

func stripSlack(data []byte) []byte {
	data = reSlackReaction.ReplaceAll(data, nil)
	data = reSlackJoinLeave.ReplaceAll(data, nil)
	data = reSlackTopicSet.ReplaceAll(data, nil)
	data = reSlackFileUpload.ReplaceAll(data, nil)
	data = reSlackEdited.ReplaceAll(data, nil)
	data = stripURLOnlyLines(data, 5)
	return data
}

func stripNotion(data []byte) []byte {
	return reNotionMeta.ReplaceAll(data, nil)
}

func stripLinear(data []byte) []byte {
	return reLinearMeta.ReplaceAll(data, nil)
}

// ---------------------------------------------------------------------------
// Helpers — pure functions, no state, easy to test in isolation.
// ---------------------------------------------------------------------------

// stripNullBytes removes 0x00 padding from FUSE read buffers.
func stripNullBytes(data []byte) []byte {
	if !bytes.ContainsRune(data, 0) {
		return data
	}
	return bytes.ReplaceAll(data, []byte{0}, nil)
}

// splitAtMarker splits data at the first occurrence of marker.
// Returns (everything up to and including the marker line, everything after).
// If marker is not found, returns (nil, data).
func splitAtMarker(data []byte, marker string) (before, after []byte) {
	m := []byte(marker)
	idx := bytes.Index(data, m)
	if idx < 0 {
		return nil, data
	}
	end := idx + len(m)
	// Skip trailing newline if present
	if end < len(data) && data[end] == '\n' {
		end++
	}
	return data[:end], data[end:]
}

// replaceHTMLEntities decodes common entities and strips the rest.
func replaceHTMLEntities(data []byte) []byte {
	r := strings.NewReplacer(
		"&nbsp;", " ", "&amp;", "&", "&lt;", "<", "&gt;", ">",
		"&quot;", `"`, "&apos;", "'",
	)
	return reHTMLEntity.ReplaceAll([]byte(r.Replace(string(data))), nil)
}

// stripURLOnlyLines keeps the first `budget` unique URL-only lines, drops the rest.
// Also removes all mailto: lines.
func stripURLOnlyLines(data []byte, budget int) []byte {
	data = reMailtoLine.ReplaceAll(data, nil)

	lines := bytes.Split(data, []byte("\n"))
	out := make([][]byte, 0, len(lines))
	seen := make(map[string]bool)
	kept := 0

	for _, line := range lines {
		trimmed := bytes.TrimSpace(line)

		if len(trimmed) > 0 && reURLLinePrefix.Match(trimmed) && !bytes.ContainsRune(trimmed, ' ') {
			key := normalizeURL(string(trimmed))
			if seen[key] {
				continue // dedup
			}
			seen[key] = true
			kept++
			if kept > budget {
				continue // over budget
			}
		}

		out = append(out, line)
	}
	return bytes.Join(out, []byte("\n"))
}

func normalizeURL(raw string) string {
	if u, err := url.Parse(raw); err == nil {
		return u.Host + u.Path
	}
	return raw
}

// dropLines removes lines whose trimmed content exactly matches any entry (case-insensitive).
func dropLines(data []byte, junk []string) []byte {
	lines := bytes.Split(data, []byte("\n"))
	out := make([][]byte, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(string(line))
		drop := false
		for _, j := range junk {
			if strings.EqualFold(trimmed, j) {
				drop = true
				break
			}
		}
		if !drop {
			out = append(out, line)
		}
	}
	return bytes.Join(out, []byte("\n"))
}

// truncateAtFooter finds the earliest footer marker past minFrac of content length.
// Everything from that line onward is replaced with "[footer removed]".
func truncateAtFooter(data []byte, markers []string, minFrac float64) []byte {
	lower := bytes.ToLower(data)
	best := len(data)
	for _, m := range markers {
		idx := bytes.Index(lower, []byte(m))
		if idx < 0 || idx >= best {
			continue
		}
		// Back up to start of line
		lineStart := bytes.LastIndexByte(lower[:idx], '\n')
		if lineStart < 0 {
			lineStart = 0
		} else {
			lineStart++
		}
		if lineStart < best {
			best = lineStart
		}
	}
	if best < len(data) && best >= int(float64(len(data))*minFrac) {
		return append(bytes.TrimRight(data[:best], "\n "), []byte("\n\n[footer removed]")...)
	}
	return data
}

// dedup removes exact duplicate non-empty lines, keeping first occurrence.
func dedup(data []byte) []byte {
	lines := bytes.Split(data, []byte("\n"))
	seen := make(map[string]bool, len(lines))
	out := make([][]byte, 0, len(lines))
	for _, line := range lines {
		t := bytes.TrimSpace(line)
		if len(t) == 0 {
			out = append(out, line)
			continue
		}
		key := string(t)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, line)
	}
	return bytes.Join(out, []byte("\n"))
}

// collapseWhitespace trims and compresses runs of blank lines.
func collapseWhitespace(data []byte) []byte {
	data = reMultiSpace.ReplaceAll(data, []byte(" "))
	data = reMultiNewline.ReplaceAll(data, []byte("\n\n"))
	return bytes.TrimSpace(data)
}

// cleanup is the final pass applied to all sources.
func cleanup(data []byte) []byte {
	data = reHTMLComment.ReplaceAll(data, nil)
	data = reHTMLTag.ReplaceAll(data, nil)
	data = replaceHTMLEntities(data)
	data = dedup(data)
	data = collapseWhitespace(data)
	return data
}
