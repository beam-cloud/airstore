package providers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
)

type graphURLRewriteTransport struct {
	baseURL *url.URL
}

func (tr *graphURLRewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	cloned.URL.Scheme = tr.baseURL.Scheme
	cloned.URL.Host = tr.baseURL.Host
	return http.DefaultTransport.RoundTrip(cloned)
}

func newTeamsProviderWithMockAPI(t *testing.T, handler func(method, path string) any) (*TeamsProvider, func()) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Strip the /v1.0 prefix that graphRequest prepends
		path := strings.TrimPrefix(r.URL.Path, "/v1.0")
		resp := handler(r.Method, path)
		if resp == nil {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "not found"}})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))

	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}

	p := &TeamsProvider{
		httpClient: &http.Client{
			Transport: &graphURLRewriteTransport{baseURL: baseURL},
			Timeout:   5 * time.Second,
		},
	}

	return p, server.Close
}

func TestTeamsResultIDRoundTrip(t *testing.T) {
	// 3-part: top-level message (channel ID contains colons, like real MS Teams)
	id := buildTeamsResultID("1616990032035", "a8274cb0-e3a1-4d5f-b8c2-abc123def456", "19:abc123@thread.tacv2", "")
	if id != "1616990032035||a8274cb0-e3a1-4d5f-b8c2-abc123def456||19:abc123@thread.tacv2" {
		t.Fatalf("unexpected 3-part id: %q", id)
	}

	messageID, teamID, channelID, replyToID, err := parseTeamsResultID(id)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if messageID != "1616990032035" || teamID != "a8274cb0-e3a1-4d5f-b8c2-abc123def456" || channelID != "19:abc123@thread.tacv2" || replyToID != "" {
		t.Fatalf("unexpected parse result: msg=%q team=%q channel=%q reply=%q", messageID, teamID, channelID, replyToID)
	}

	// 4-part: reply
	id4 := buildTeamsResultID("1616990099000", "a8274cb0-e3a1-4d5f-b8c2-abc123def456", "19:abc123@thread.tacv2", "1616990032035")
	if id4 != "1616990099000||a8274cb0-e3a1-4d5f-b8c2-abc123def456||19:abc123@thread.tacv2||1616990032035" {
		t.Fatalf("unexpected 4-part id: %q", id4)
	}

	messageID, teamID, channelID, replyToID, err = parseTeamsResultID(id4)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if messageID != "1616990099000" || teamID != "a8274cb0-e3a1-4d5f-b8c2-abc123def456" || channelID != "19:abc123@thread.tacv2" || replyToID != "1616990032035" {
		t.Fatalf("unexpected parse: msg=%q team=%q channel=%q reply=%q", messageID, teamID, channelID, replyToID)
	}
}

func TestTeamsProviderReadResult_ChannelMessage(t *testing.T) {
	teamID := "team-abc"
	channelID := "channel-def"
	messageID := "msg-123"

	p, cleanup := newTeamsProviderWithMockAPI(t, func(method, path string) any {
		expected := "/teams/" + teamID + "/channels/" + channelID + "/messages/" + messageID
		if method == "GET" && path == expected {
			return map[string]any{
				"id":              messageID,
				"createdDateTime": "2026-04-08T10:00:00Z",
				"body": map[string]any{
					"contentType": "text",
					"content":     "Hello from Teams!",
				},
				"from": map[string]any{
					"user": map[string]any{
						"id":          "user-1",
						"displayName": "Alice",
					},
				},
			}
		}
		// Replies endpoint returns empty
		if method == "GET" && path == expected+"/replies" {
			return map[string]any{"value": []any{}}
		}
		return nil
	})
	defer cleanup()

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{AccessToken: "test-token"},
	}

	content, err := p.ReadResult(context.Background(), pctx, buildTeamsResultID(messageID, teamID, channelID, ""))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	body := string(content)
	if !strings.Contains(body, "Hello from Teams!") {
		t.Fatalf("expected message body in content, got: %s", body)
	}
	if !strings.Contains(body, "@Alice") {
		t.Fatalf("expected sender name in content, got: %s", body)
	}
}

func TestTeamsProviderReadResult_ThreadReply(t *testing.T) {
	teamID := "team-abc"
	channelID := "channel-def"
	messageID := "msg-123"

	p, cleanup := newTeamsProviderWithMockAPI(t, func(method, path string) any {
		msgPath := "/teams/" + teamID + "/channels/" + channelID + "/messages/" + messageID
		if method == "GET" && path == msgPath {
			return map[string]any{
				"id":              messageID,
				"createdDateTime": "2026-04-08T10:00:00Z",
				"body": map[string]any{
					"contentType": "text",
					"content":     "Original message",
				},
				"from": map[string]any{
					"user": map[string]any{"displayName": "Alice"},
				},
			}
		}
		if method == "GET" && path == msgPath+"/replies" {
			return map[string]any{
				"value": []map[string]any{
					{
						"id":              "reply-1",
						"createdDateTime": "2026-04-08T10:05:00Z",
						"body": map[string]any{
							"contentType": "text",
							"content":     "This is a reply",
						},
						"from": map[string]any{
							"user": map[string]any{"displayName": "Bob"},
						},
					},
				},
			}
		}
		return nil
	})
	defer cleanup()

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{AccessToken: "test-token"},
	}

	content, err := p.ReadResult(context.Background(), pctx, buildTeamsResultID(messageID, teamID, channelID, ""))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	body := string(content)
	if !strings.Contains(body, "Thread (1 replies):") {
		t.Fatalf("expected thread section, got: %s", body)
	}
	if !strings.Contains(body, "This is a reply") {
		t.Fatalf("expected reply body in thread section, got: %s", body)
	}
	if !strings.Contains(body, "@Bob") {
		t.Fatalf("expected reply sender in thread section, got: %s", body)
	}
}

func TestTeamsProviderListResources(t *testing.T) {
	p, cleanup := newTeamsProviderWithMockAPI(t, func(method, path string) any {
		if method == "GET" && path == "/me/joinedTeams" {
			return map[string]any{
				"value": []map[string]any{
					{"id": "team-1", "displayName": "Engineering"},
					{"id": "team-2", "displayName": "Marketing"},
				},
			}
		}
		if method == "GET" && path == "/teams/team-1/channels" {
			return map[string]any{
				"value": []map[string]any{
					{"id": "ch-1", "displayName": "General"},
					{"id": "ch-2", "displayName": "Random"},
				},
			}
		}
		if method == "GET" && path == "/teams/team-2/channels" {
			return map[string]any{
				"value": []map[string]any{
					{"id": "ch-3", "displayName": "General"},
				},
			}
		}
		return nil
	})
	defer cleanup()

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{AccessToken: "test-token"},
	}

	resources, err := p.ListResources(context.Background(), pctx, "channels")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(resources) != 3 {
		t.Fatalf("expected 3 resources, got %d", len(resources))
	}

	// Check first resource
	if resources[0].ID != "Engineering/General" {
		t.Fatalf("expected ID 'Engineering/General', got %q", resources[0].ID)
	}
	if resources[0].Name != "Engineering > #General" {
		t.Fatalf("expected Name 'Engineering > #General', got %q", resources[0].Name)
	}

	// Check last resource
	if resources[2].ID != "Marketing/General" {
		t.Fatalf("expected ID 'Marketing/General', got %q", resources[2].ID)
	}
}

func TestTeamsProviderParseChannelQuery(t *testing.T) {
	// Valid channel query
	team, channel, ok := parseTeamsChannelQuery("in:Engineering/General")
	if !ok {
		t.Fatal("expected parseTeamsChannelQuery to return ok=true")
	}
	if team != "Engineering" || channel != "General" {
		t.Fatalf("expected Engineering/General, got %s/%s", team, channel)
	}

	// With # prefix
	team, channel, ok = parseTeamsChannelQuery("in:#Engineering/General")
	if !ok {
		t.Fatal("expected ok=true for # prefix")
	}
	if team != "Engineering" || channel != "General" {
		t.Fatalf("expected Engineering/General, got %s/%s", team, channel)
	}

	// Not a channel query (has text terms)
	_, _, ok = parseTeamsChannelQuery("in:Engineering/General hello world")
	if ok {
		t.Fatal("expected ok=false for query with text terms")
	}

	// No team/channel
	_, _, ok = parseTeamsChannelQuery("hello world")
	if ok {
		t.Fatal("expected ok=false for plain text query")
	}

	// Only team name, no channel
	_, _, ok = parseTeamsChannelQuery("in:Engineering")
	if ok {
		t.Fatal("expected ok=false for team-only query")
	}

	// Quoted value with spaces
	team, channel, ok = parseTeamsChannelQuery(`in:"Engineering Team/General Discussion"`)
	if !ok {
		t.Fatal("expected ok=true for quoted value with spaces")
	}
	if team != "Engineering Team" || channel != "General Discussion" {
		t.Fatalf("expected Engineering Team/General Discussion, got %s/%s", team, channel)
	}

	// Quoted value with trailing text should fail
	_, _, ok = parseTeamsChannelQuery(`in:"Engineering Team/General Discussion" hello`)
	if ok {
		t.Fatal("expected ok=false for quoted value with trailing text")
	}
}

func TestTeamsBodyToText(t *testing.T) {
	// Plain text
	body := graphMessageBody{ContentType: "text", Content: "Hello world"}
	if got := teamsBodyToText(body); got != "Hello world" {
		t.Fatalf("expected 'Hello world', got %q", got)
	}

	// HTML body
	body = graphMessageBody{ContentType: "html", Content: "<p>Hello <b>world</b></p><br>Next line"}
	got := teamsBodyToText(body)
	if !strings.Contains(got, "Hello") || !strings.Contains(got, "world") {
		t.Fatalf("expected stripped HTML, got %q", got)
	}
}
