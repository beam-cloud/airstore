package repository

import (
	"context"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/airstore/pkg/instrumentation"
	"github.com/beam-cloud/airstore/pkg/types"
)

type collectingEventRecorder struct {
	mu     sync.Mutex
	events []instrumentation.Event
}

func (r *collectingEventRecorder) Record(_ context.Context, event instrumentation.Event) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *collectingEventRecorder) snapshot() []instrumentation.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]instrumentation.Event, len(r.events))
	copy(out, r.events)
	return out
}

func TestSaveConnectionEmitsCreatedEventOnInsert(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	recorder := &collectingEventRecorder{}
	backend := &PostgresBackend{db: db, recorder: recorder}

	createdAt := time.Now().UTC()
	updatedAt := createdAt
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO integration_connection")).
		WithArgs(uint(42), (*uint)(nil), "github", sqlmock.AnyArg(), "shared", nil).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "external_id", "workspace_id", "member_id", "integration_type", "credentials", "scope", "expires_at", "created_at", "updated_at", "inserted",
		}).AddRow(
			1,
			"conn-ext-1",
			42,
			nil,
			"github",
			[]byte(`{"access_token":"x"}`),
			"shared",
			nil,
			createdAt,
			updatedAt,
			true,
		))

	conn, err := backend.SaveConnection(
		context.Background(),
		42,
		nil,
		"github",
		&types.IntegrationCredentials{AccessToken: "x"},
		"shared",
	)
	if err != nil {
		t.Fatalf("SaveConnection returned error: %v", err)
	}
	if conn == nil {
		t.Fatal("SaveConnection returned nil connection")
	}
	if conn.ExternalId != "conn-ext-1" {
		t.Fatalf("expected external id conn-ext-1, got %q", conn.ExternalId)
	}

	events := recorder.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Type != "connection.created" {
		t.Fatalf("expected event type connection.created, got %q", events[0].Type)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestSaveConnectionSkipsCreatedEventOnUpdate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	recorder := &collectingEventRecorder{}
	backend := &PostgresBackend{db: db, recorder: recorder}

	createdAt := time.Now().UTC()
	updatedAt := createdAt.Add(time.Minute)
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO integration_connection")).
		WithArgs(uint(42), (*uint)(nil), "github", sqlmock.AnyArg(), "shared", nil).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "external_id", "workspace_id", "member_id", "integration_type", "credentials", "scope", "expires_at", "created_at", "updated_at", "inserted",
		}).AddRow(
			1,
			"conn-ext-1",
			42,
			nil,
			"github",
			[]byte(`{"access_token":"x"}`),
			"shared",
			nil,
			createdAt,
			updatedAt,
			false,
		))

	conn, err := backend.SaveConnection(
		context.Background(),
		42,
		nil,
		"github",
		&types.IntegrationCredentials{AccessToken: "x"},
		"shared",
	)
	if err != nil {
		t.Fatalf("SaveConnection returned error: %v", err)
	}
	if conn == nil {
		t.Fatal("SaveConnection returned nil connection")
	}
	if conn.ExternalId != "conn-ext-1" {
		t.Fatalf("expected external id conn-ext-1, got %q", conn.ExternalId)
	}

	events := recorder.snapshot()
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGetAgentStatsTotalCostUsesUsageJSONWhenRunCostMissing(t *testing.T) {
	backend, mock, cleanup := newStatsBackend(t)
	defer cleanup()

	workspaceID := uint(42)
	agentID := "agent-1"
	expectGetAgentStatsBaseQueries(mock, workspaceID, agentID)
	mock.ExpectQuery(agentStatsTotalCostQueryPattern).
		WithArgs(workspaceID, agentID).
		WillReturnRows(sqlmock.NewRows([]string{"total_cost"}).AddRow(2.1530685))

	stats, err := backend.GetAgentStats(context.Background(), workspaceID, agentID)
	if err != nil {
		t.Fatalf("GetAgentStats returned error: %v", err)
	}
	if stats == nil {
		t.Fatal("GetAgentStats returned nil stats")
	}
	if stats.TotalCostUSD < 2.153068499 || stats.TotalCostUSD > 2.153068501 {
		t.Fatalf("expected usage-backed total cost 2.1530685, got %v", stats.TotalCostUSD)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGetAgentStatsTotalCostCombinesLegacyAndUsageBackedRuns(t *testing.T) {
	backend, mock, cleanup := newStatsBackend(t)
	defer cleanup()

	workspaceID := uint(42)
	agentID := "agent-1"
	expectGetAgentStatsBaseQueries(mock, workspaceID, agentID)
	mock.ExpectQuery(agentStatsTotalCostQueryPattern).
		WithArgs(workspaceID, agentID).
		WillReturnRows(sqlmock.NewRows([]string{"total_cost"}).AddRow(5.4030685))

	stats, err := backend.GetAgentStats(context.Background(), workspaceID, agentID)
	if err != nil {
		t.Fatalf("GetAgentStats returned error: %v", err)
	}
	if stats == nil {
		t.Fatal("GetAgentStats returned nil stats")
	}
	if stats.TotalCostUSD < 5.403068499 || stats.TotalCostUSD > 5.403068501 {
		t.Fatalf("expected mixed total cost 5.4030685, got %v", stats.TotalCostUSD)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGetAgentStatsTotalCostTreatsMalformedUsageValuesAsZero(t *testing.T) {
	backend, mock, cleanup := newStatsBackend(t)
	defer cleanup()

	workspaceID := uint(42)
	agentID := "agent-1"
	expectGetAgentStatsBaseQueries(mock, workspaceID, agentID)
	mock.ExpectQuery(agentStatsTotalCostQueryPattern).
		WithArgs(workspaceID, agentID).
		WillReturnRows(sqlmock.NewRows([]string{"total_cost"}).AddRow(0))

	stats, err := backend.GetAgentStats(context.Background(), workspaceID, agentID)
	if err != nil {
		t.Fatalf("GetAgentStats returned error: %v", err)
	}
	if stats == nil {
		t.Fatal("GetAgentStats returned nil stats")
	}
	if stats.TotalCostUSD != 0 {
		t.Fatalf("expected malformed usage-backed total cost to be treated as zero, got %v", stats.TotalCostUSD)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func newStatsBackend(t *testing.T) (*PostgresBackend, sqlmock.Sqlmock, func()) {
	t.Helper()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}

	return &PostgresBackend{db: db}, mock, func() { _ = db.Close() }
}

func expectGetAgentStatsBaseQueries(mock sqlmock.Sqlmock, workspaceID uint, agentID string) {
	mock.ExpectQuery(regexp.QuoteMeta("SELECT state::text, COUNT(*) FROM agent_task")).
		WithArgs(workspaceID, agentID).
		WillReturnRows(sqlmock.NewRows([]string{"state", "count"}))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT AVG(EXTRACT(EPOCH FROM (COALESCE(ended_at, updated_at) - COALESCE(started_at, created_at))))")).
		WithArgs(workspaceID, agentID).
		WillReturnRows(sqlmock.NewRows([]string{"avg_run_sec"}).AddRow(nil))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT quality_score")).
		WithArgs(workspaceID, agentID).
		WillReturnRows(sqlmock.NewRows([]string{"quality_score"}).AddRow(nil))
}

const agentStatsTotalCostQueryPattern = `(?s)SELECT COALESCE\(SUM\(.*WHEN ar\.cost_usd > 0 THEN ar\.cost_usd.*->>'billing_total_cost_microusd'.*->>'total_cost_usd'.*->>'cost_usd'.*->>'usd_cost'.*->>'cost'.*FROM agent_run ar.*WHERE ar\.workspace_id = \$1 AND ar\.agent_id = \$2`
