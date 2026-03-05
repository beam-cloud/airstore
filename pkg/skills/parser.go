package skills

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// nameRegex validates skill names per the Agent Skills spec:
// lowercase alphanumeric and hyphens, no leading/trailing/consecutive hyphens, max 64 chars.
var nameRegex = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

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
//
// The format follows the Agent Skills specification (https://agentskills.io/specification):
//
//	---
//	name: email-triage
//	description: Categorizes emails by urgency. Use when processing Gmail inbox.
//	license: MIT
//	metadata:
//	  author: beam-cloud
//	  version: "1.0"
//	  airstore:
//	    needs:
//	      - gmail
//	    writes:
//	      - /reports/email-triage/
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

	if manifest.Description == "" {
		return nil, fmt.Errorf("skill description is required")
	}

	// Validate name format per Agent Skills spec
	if len(manifest.Name) > 64 {
		return nil, fmt.Errorf("skill name must be at most 64 characters")
	}
	if !nameRegex.MatchString(manifest.Name) {
		return nil, fmt.Errorf("skill name %q must be lowercase alphanumeric with hyphens, no leading/trailing hyphens", manifest.Name)
	}
	if strings.Contains(manifest.Name, "--") {
		return nil, fmt.Errorf("skill name %q must not contain consecutive hyphens", manifest.Name)
	}

	// Validate description length per spec
	if len(manifest.Description) > 1024 {
		return nil, fmt.Errorf("skill description must be at most 1024 characters")
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
