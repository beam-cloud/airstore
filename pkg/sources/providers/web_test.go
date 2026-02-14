package providers

import (
	"net/url"
	"testing"
)

func TestFilterLinksToSite_RestrictsToSameHost(t *testing.T) {
	site, err := url.Parse("https://docs.airstore.ai/")
	if err != nil {
		t.Fatal(err)
	}

	links := []mapLink{
		{URL: "https://docs.airstore.ai/getting-started"},
		{URL: "https://docs.airstore.ai/reference/api"},
		{URL: "https://airstore.ai/"},
		{URL: "https://example.com/"},
	}

	got := filterLinksToSite(site, links)
	if len(got) != 2 {
		t.Fatalf("expected 2 same-host links, got %d", len(got))
	}
	for _, l := range got {
		u, parseErr := url.Parse(l.URL)
		if parseErr != nil {
			t.Fatalf("invalid URL in output: %q", l.URL)
		}
		if u.Hostname() != "docs.airstore.ai" {
			t.Fatalf("unexpected host %q in output", u.Hostname())
		}
	}
}

func TestFilterLinksToSite_AllowsWwwVariant(t *testing.T) {
	site, err := url.Parse("https://airstore.ai/")
	if err != nil {
		t.Fatal(err)
	}

	links := []mapLink{
		{URL: "https://airstore.ai/docs"},
		{URL: "https://www.airstore.ai/blog"},
		{URL: "https://docs.airstore.ai/"},
	}

	got := filterLinksToSite(site, links)
	if len(got) != 2 {
		t.Fatalf("expected 2 links on host/www variant, got %d", len(got))
	}
}
