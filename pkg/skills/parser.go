package skills

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ParseFile reads a SKILL.md file and returns its manifest and full content.
func ParseFile(path string) (*SkillManifest, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("reading skill file: %w", err)
	}
	manifest, err := Parse(data)
	if err != nil {
		return nil, "", fmt.Errorf("parsing %s: %w", filepath.Base(path), err)
	}
	return manifest, string(data), nil
}

// Parse extracts a SkillManifest from the YAML frontmatter of a SKILL.md file.
// The file format is:
//
//	---
//	name: My Skill
//	description: What it does
//	needs: [gmail]
//	triggers:
//	  - on: source.change
//	    path: /sources/gmail
//	writes:
//	  - /memory/my-skill/
//	---
//
//	# Instructions
//	...
func Parse(data []byte) (*SkillManifest, error) {
	frontmatter, err := extractFrontmatter(data)
	if err != nil {
		return nil, err
	}

	var manifest SkillManifest
	if err := yaml.Unmarshal(frontmatter, &manifest); err != nil {
		return nil, fmt.Errorf("invalid frontmatter YAML: %w", err)
	}

	if manifest.Name == "" {
		return nil, fmt.Errorf("skill name is required")
	}

	// Validate trigger event types
	for i, t := range manifest.Triggers {
		if t.On == "" {
			return nil, fmt.Errorf("trigger %d: 'on' is required", i)
		}
		if t.Path == "" {
			return nil, fmt.Errorf("trigger %d: 'path' is required", i)
		}
		switch t.On {
		case "source.change", "fs.create", "fs.write", "fs.delete":
			// valid
		default:
			return nil, fmt.Errorf("trigger %d: unknown event type %q (valid: source.change, fs.create, fs.write, fs.delete)", i, t.On)
		}
	}

	return &manifest, nil
}

// extractFrontmatter pulls the YAML block between the first pair of "---" lines.
func extractFrontmatter(data []byte) ([]byte, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))

	// Find opening ---
	foundOpen := false
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "---" {
			foundOpen = true
			break
		}
	}
	if !foundOpen {
		return nil, fmt.Errorf("no frontmatter found (missing opening ---)")
	}

	// Collect lines until closing ---
	var buf bytes.Buffer
	foundClose := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "---" {
			foundClose = true
			break
		}
		buf.WriteString(line)
		buf.WriteByte('\n')
	}
	if !foundClose {
		return nil, fmt.Errorf("unterminated frontmatter (missing closing ---)")
	}

	return buf.Bytes(), nil
}

// ExtractInstructions returns everything after the frontmatter block.
func ExtractInstructions(data []byte) string {
	reader := bufio.NewReader(bytes.NewReader(data))

	// Skip to opening ---
	for {
		line, err := reader.ReadString('\n')
		if strings.TrimSpace(line) == "---" {
			break
		}
		if err == io.EOF {
			return ""
		}
	}

	// Skip to closing ---
	for {
		line, err := reader.ReadString('\n')
		if strings.TrimSpace(line) == "---" {
			break
		}
		if err == io.EOF {
			return ""
		}
	}

	// Rest is instructions
	rest, _ := io.ReadAll(reader)
	return strings.TrimSpace(string(rest))
}
