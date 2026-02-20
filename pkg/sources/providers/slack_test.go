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

type slackURLRewriteTransport struct {
	baseURL *url.URL
}

func (t *slackURLRewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	cloned.URL.Scheme = t.baseURL.Scheme
	cloned.URL.Host = t.baseURL.Host
	return http.DefaultTransport.RoundTrip(cloned)
}

func newSlackProviderWithMockAPI(t *testing.T, handler func(apiMethod string, query url.Values) map[string]any) (*SlackProvider, func()) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiMethod := strings.TrimPrefix(r.URL.Path, "/api/")
		resp := handler(apiMethod, r.URL.Query())
		if resp == nil {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":    false,
				"error": "unexpected API method",
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))

	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}

	p := &SlackProvider{
		httpClient: &http.Client{
			Transport: &slackURLRewriteTransport{baseURL: baseURL},
			Timeout:   5 * time.Second,
		},
	}

	return p, server.Close
}

func TestSlackResultIDRoundTrip(t *testing.T) {
	id := buildSlackResultID("1771428086.540649", "C123", "1771427000.000100")
	if id != "1771428086.540649:C123:1771427000.000100" {
		t.Fatalf("unexpected thread-aware id: %q", id)
	}

	ts, channelID, threadTS, err := parseSlackResultID(id)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if ts != "1771428086.540649" || channelID != "C123" || threadTS != "1771427000.000100" {
		t.Fatalf("unexpected parse result: ts=%q channel=%q thread=%q", ts, channelID, threadTS)
	}

	legacy := buildSlackResultID("1771448784.952069", "C456", "1771448784.952069")
	if legacy != "1771448784.952069:C456" {
		t.Fatalf("unexpected legacy id: %q", legacy)
	}
}

func TestSlackProviderReadResult_ThreadReplyWithThreadID(t *testing.T) {
	targetTS := "1771428086.540649"
	threadTS := "1771427000.000100"
	channelID := "C123"

	p, cleanup := newSlackProviderWithMockAPI(t, func(apiMethod string, query url.Values) map[string]any {
		switch apiMethod {
		case "conversations.history":
			// Direct timestamp lookup misses for thread replies.
			if query.Get("oldest") == targetTS && query.Get("latest") == targetTS {
				return map[string]any{
					"ok":       true,
					"messages": []map[string]any{},
				}
			}
			return map[string]any{"ok": true, "messages": []map[string]any{}}
		case "conversations.replies":
			if query.Get("ts") == threadTS {
				return map[string]any{
					"ok": true,
					"messages": []map[string]any{
						{"ts": threadTS, "user": "U_PARENT", "text": "thread root"},
						{"ts": targetTS, "user": "U_REPLY", "text": "thread reply body"},
					},
					"has_more": false,
					"response_metadata": map[string]any{
						"next_cursor": "",
					},
				}
			}
			return map[string]any{"ok": true, "messages": []map[string]any{}}
		default:
			return nil
		}
	})
	defer cleanup()

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{AccessToken: "xoxp-test"},
	}

	content, err := p.ReadResult(context.Background(), pctx, targetTS+":"+channelID+":"+threadTS)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !strings.Contains(string(content), "thread reply body") {
		t.Fatalf("expected reply body in content, got: %s", string(content))
	}
}

func TestSlackProviderReadResult_LegacyIDTopLevelMessage(t *testing.T) {
	targetTS := "1771448784.952069"
	channelID := "C999"

	p, cleanup := newSlackProviderWithMockAPI(t, func(apiMethod string, query url.Values) map[string]any {
		switch apiMethod {
		case "conversations.history":
			if query.Get("oldest") == targetTS && query.Get("latest") == targetTS {
				return map[string]any{
					"ok": true,
					"messages": []map[string]any{
						{"ts": targetTS, "user": "U_A", "text": "top-level message", "reply_count": float64(0)},
					},
				}
			}
			return map[string]any{"ok": true, "messages": []map[string]any{}}
		default:
			return nil
		}
	})
	defer cleanup()

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{AccessToken: "xoxp-test"},
	}

	content, err := p.ReadResult(context.Background(), pctx, targetTS+":"+channelID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !strings.Contains(string(content), "top-level message") {
		t.Fatalf("expected top-level message in content, got: %s", string(content))
	}
}

func TestSlackProviderReadResult_LegacyReplyIDFallback(t *testing.T) {
	targetTS := "1773000010.000200"
	threadTS := "1773000000.000100"
	channelID := "C555"

	p, cleanup := newSlackProviderWithMockAPI(t, func(apiMethod string, query url.Values) map[string]any {
		switch apiMethod {
		case "conversations.history":
			// Direct lookup misses.
			if query.Get("oldest") == targetTS && query.Get("latest") == targetTS {
				return map[string]any{"ok": true, "messages": []map[string]any{}}
			}
			// Recent history fallback returns a thread root candidate.
			return map[string]any{
				"ok": true,
				"messages": []map[string]any{
					{"ts": threadTS, "user": "U_PARENT", "text": "root", "reply_count": float64(1)},
				},
			}
		case "conversations.replies":
			if query.Get("ts") == threadTS {
				return map[string]any{
					"ok": true,
					"messages": []map[string]any{
						{"ts": threadTS, "user": "U_PARENT", "text": "root"},
						{"ts": targetTS, "user": "U_REPLY", "text": "legacy reply body"},
					},
					"has_more": false,
					"response_metadata": map[string]any{
						"next_cursor": "",
					},
				}
			}
			return map[string]any{"ok": true, "messages": []map[string]any{}}
		default:
			return nil
		}
	})
	defer cleanup()

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{AccessToken: "xoxp-test"},
	}

	content, err := p.ReadResult(context.Background(), pctx, targetTS+":"+channelID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !strings.Contains(string(content), "legacy reply body") {
		t.Fatalf("expected legacy reply body in content, got: %s", string(content))
	}
}

func TestSlackProviderReadResult_TopLevelThreadIncludesReplies(t *testing.T) {
	targetTS := "1774000000.000100"
	channelID := "C777"

	p, cleanup := newSlackProviderWithMockAPI(t, func(apiMethod string, query url.Values) map[string]any {
		switch apiMethod {
		case "conversations.history":
			return map[string]any{
				"ok": true,
				"messages": []map[string]any{
					{"ts": targetTS, "user": "U_MAIN", "text": "main body", "reply_count": float64(1)},
				},
			}
		case "conversations.replies":
			if query.Get("ts") == targetTS {
				return map[string]any{
					"ok": true,
					"messages": []map[string]any{
						{"ts": targetTS, "user": "U_MAIN", "text": "main body"},
						{"ts": "1774000010.000200", "user": "U_R1", "text": "reply body"},
					},
					"has_more": false,
					"response_metadata": map[string]any{
						"next_cursor": "",
					},
				}
			}
			return map[string]any{"ok": true, "messages": []map[string]any{}}
		default:
			return nil
		}
	})
	defer cleanup()

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{AccessToken: "xoxp-test"},
	}

	content, err := p.ReadResult(context.Background(), pctx, targetTS+":"+channelID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	body := string(content)
	if !strings.Contains(body, "Thread (1 replies):") {
		t.Fatalf("expected thread section, got: %s", body)
	}
	if !strings.Contains(body, "reply body") {
		t.Fatalf("expected reply body in thread section, got: %s", body)
	}
}
