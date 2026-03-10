package worker

import (
	"bytes"
	"encoding/json"
)

// extractAssistantText scans raw stream-json output (from claude --print
// --output-format stream-json), pulls out assistant text blocks, and returns
// the last `limit` characters of concatenated text.  This is used to build
// context for approval summary extraction without sending tool call / tool
// result noise to the summariser.
func extractAssistantText(raw []byte, limit int) string {
	var texts []string
	totalLen := 0

	for _, line := range bytes.Split(raw, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] != '{' {
			continue
		}

		var envelope struct {
			Type    string `json:"type"`
			Message *struct {
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"message"`
			// "result" type messages carry a top-level result string.
			Result  string `json:"result"`
			IsError bool   `json:"is_error"`
		}
		if err := json.Unmarshal(line, &envelope); err != nil {
			continue
		}

		switch envelope.Type {
		case "assistant":
			if envelope.Message == nil {
				continue
			}
			for _, block := range envelope.Message.Content {
				if block.Type == "text" && block.Text != "" {
					texts = append(texts, block.Text)
					totalLen += len(block.Text)
				}
			}
		case "result":
			if !envelope.IsError && envelope.Result != "" {
				texts = append(texts, envelope.Result)
				totalLen += len(envelope.Result)
			}
		}
	}

	if totalLen == 0 {
		return ""
	}

	// Concatenate all texts with double-newline separators, then take the
	// last `limit` characters so the most recent content is preserved.
	var buf bytes.Buffer
	for i, t := range texts {
		if i > 0 {
			buf.WriteString("\n\n")
		}
		buf.WriteString(t)
	}
	s := buf.String()
	if limit > 0 && len(s) > limit {
		s = s[len(s)-limit:]
	}
	return s
}
