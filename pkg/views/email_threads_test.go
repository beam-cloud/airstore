package views

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

type emailThreadTestBackend struct {
	repository.BackendRepository
	connections map[string]*types.IntegrationConnection
	lookups     []string
}

func (b *emailThreadTestBackend) GetConnection(_ context.Context, _ uint, _ uint, integration string) (*types.IntegrationConnection, error) {
	b.lookups = append(b.lookups, integration)
	if b.connections == nil {
		return nil, nil
	}
	return b.connections[integration], nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func jsonResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func testIntegrationConnection(t *testing.T, integration, token string) *types.IntegrationConnection {
	t.Helper()

	creds, err := json.Marshal(&types.IntegrationCredentials{AccessToken: token})
	if err != nil {
		t.Fatalf("marshal credentials: %v", err)
	}
	return &types.IntegrationConnection{
		IntegrationType: integration,
		Credentials:     creds,
	}
}

func TestGetConnectionCredentialsReturnsNilWhenConnectionMissing(t *testing.T) {
	backend := &emailThreadTestBackend{}
	fetcher := NewEmailThreadFetcher(backend)

	creds := fetcher.getConnectionCredentials(context.Background(), 7, string(types.SourceOutlook))
	if creds != nil {
		t.Fatalf("credentials = %#v, want nil", creds)
	}
	if got, want := backend.lookups, []string{string(types.SourceOutlook)}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("connection lookups = %#v, want %#v", got, want)
	}
}

func TestFetchThreadsSkipsMissingSecondaryProviderForUnqualifiedRefs(t *testing.T) {
	backend := &emailThreadTestBackend{
		connections: map[string]*types.IntegrationConnection{
			string(types.SourceGmail): testIntegrationConnection(t, string(types.SourceGmail), "gmail-token"),
		},
	}
	fetcher := NewEmailThreadFetcher(backend)
	fetcher.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/gmail/v1/users/me/profile":
				return jsonResponse(http.StatusOK, `{"emailAddress":"agent@example.com"}`), nil
			case "/gmail/v1/users/me/threads/thread-123":
				return jsonResponse(http.StatusOK, `{
					"messages":[
						{
							"id":"msg-123",
							"threadId":"thread-123",
							"internalDate":"1710000000000",
							"labelIds":["SENT"],
							"snippet":"hi there",
							"payload":{
								"headers":[
									{"name":"From","value":"Agent <agent@example.com>"},
									{"name":"To","value":"luke@example.com"},
									{"name":"Subject","value":"Beam sandboxes"},
									{"name":"Date","value":"Tue, 09 Apr 2024 12:00:00 +0000"}
								],
								"body":{"data":"aGkgdGhlcmU="}
							}
						}
					]
				}`), nil
			default:
				t.Fatalf("unexpected request path: %s", req.URL.String())
				return nil, nil
			}
		}),
	}

	result := fetcher.FetchThreads(context.Background(), 7, []EmailThreadRef{{ID: "thread-123"}})

	if got, want := backend.lookups, []string{string(types.SourceGmail), string(types.SourceOutlook)}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("connection lookups = %#v, want %#v", got, want)
	}
	messages, ok := result[":thread-123"]
	if !ok {
		t.Fatalf("expected unqualified thread key, got %#v", result)
	}
	if got, want := len(messages), 1; got != want {
		t.Fatalf("message count = %d, want %d", got, want)
	}
	if got := messages[0].Snippet; got != "hi there" {
		t.Fatalf("snippet = %q, want hi there", got)
	}
	if !messages[0].IsOutbound {
		t.Fatal("expected gmail message to be marked outbound")
	}
}
