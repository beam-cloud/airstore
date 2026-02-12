package common

import (
	"regexp"
	"strings"
	"unicode"
)

// stopWords are common words filtered out of task name slugs.
var stopWords = map[string]bool{
	"the": true, "a": true, "an": true, "is": true, "are": true,
	"was": true, "were": true, "be": true, "been": true, "being": true,
	"to": true, "in": true, "for": true, "of": true, "on": true,
	"at": true, "by": true, "with": true, "from": true, "and": true,
	"or": true, "but": true, "not": true, "this": true, "that": true,
	"it": true, "its": true, "as": true, "if": true, "so": true,
	"please": true, "can": true, "you": true, "my": true, "me": true,
	"your": true, "we": true, "our": true, "do": true, "does": true,
	"did": true, "will": true, "would": true, "should": true, "could": true,
	"has": true, "have": true, "had": true, "i": true, "im": true,
}

var nonAlphaNum = regexp.MustCompile(`[^a-z0-9]+`)
var multiHyphen = regexp.MustCompile(`-{2,}`)

// GenerateTaskName generates a human-readable slug for a task.
//
// Algorithm:
//  1. Source text: prompt > image name (extracted from container ref) > "task" fallback
//  2. Lowercase, replace non-alphanumeric with spaces, split on whitespace
//  3. Filter stop words
//  4. Take first 5 meaningful words, join with hyphens
//  5. Cap slug portion at 50 chars
//  6. Append first 8 chars of externalId for uniqueness
//  7. Final result: only [a-z0-9-], no consecutive/leading/trailing hyphens
func GenerateTaskName(prompt, image, externalId string) string {
	source := pickSource(prompt, image)
	slug := slugify(source)

	// Ensure we have a suffix from the external ID
	suffix := idSuffix(externalId)

	if slug == "" {
		return "task-" + suffix
	}

	// Cap slug at 50 chars
	if len(slug) > 50 {
		slug = slug[:50]
		// Trim trailing hyphen from truncation
		slug = strings.TrimRight(slug, "-")
	}

	return slug + "-" + suffix
}

// pickSource selects the best source text for the slug.
func pickSource(prompt, image string) string {
	if prompt != "" {
		return prompt
	}
	if image != "" {
		return extractImageName(image)
	}
	return "task"
}

// extractImageName extracts a human-readable name from a container image reference.
// Examples:
//
//	"ghcr.io/org/my-sandbox:latest" → "my-sandbox"
//	"ubuntu:22.04"                  → "ubuntu"
//	"registry.com/team/app"         → "app"
func extractImageName(image string) string {
	// Remove tag/digest
	if idx := strings.LastIndex(image, ":"); idx > 0 {
		// Make sure we're not stripping a port from the registry
		afterColon := image[idx+1:]
		if !strings.Contains(afterColon, "/") {
			image = image[:idx]
		}
	}
	if idx := strings.LastIndex(image, "@"); idx > 0 {
		image = image[:idx]
	}

	// Take the last path component
	if idx := strings.LastIndex(image, "/"); idx >= 0 {
		image = image[idx+1:]
	}

	return image
}

// slugify converts source text into a slug of up to 5 meaningful words.
func slugify(source string) string {
	// Lowercase and replace non-alphanumeric chars with spaces
	s := strings.ToLower(source)
	s = nonAlphaNum.ReplaceAllString(s, " ")

	// Split and filter
	words := strings.Fields(s)
	var meaningful []string
	for _, w := range words {
		if stopWords[w] {
			continue
		}
		// Skip single-char words that aren't meaningful
		if len(w) == 1 && !unicode.IsDigit(rune(w[0])) {
			continue
		}
		meaningful = append(meaningful, w)
		if len(meaningful) == 5 {
			break
		}
	}

	slug := strings.Join(meaningful, "-")
	// Clean up any remaining issues
	slug = multiHyphen.ReplaceAllString(slug, "-")
	slug = strings.Trim(slug, "-")
	return slug
}

// idSuffix returns the first 8 characters of the external ID for uniqueness.
func idSuffix(externalId string) string {
	// Strip hyphens from UUID for a denser suffix
	clean := strings.ReplaceAll(externalId, "-", "")
	if len(clean) >= 8 {
		return clean[:8]
	}
	if len(clean) > 0 {
		return clean
	}
	return "00000000"
}
