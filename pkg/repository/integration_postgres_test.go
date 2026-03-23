package repository

import (
	"context"
	"errors"
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

func TestCreateTaskOutputTreatsSameTaskConflictAsIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backend := &PostgresBackend{db: db}
	output := &types.TaskOutput{
		ID:          "output-1",
		WorkspaceID: 7,
		TaskID:      "task-1",
		OutputType:  "report",
		Title:       "Report",
		Data:        map[string]any{},
		Status:      types.TaskOutputStatusPending,
	}
	createdAt := time.Now().UTC()

	mock.ExpectQuery(regexp.QuoteMeta(`
			INSERT INTO task_output (id, workspace_id, task_id, run_id, agent_id, output_type, title, summary, uri, data_json, metadata_json, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT (id) DO NOTHING
			RETURNING created_at`)).
		WithArgs(
			"output-1",
			uint(7),
			"task-1",
			nil,
			nil,
			"report",
			"Report",
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			types.TaskOutputStatusPending,
		).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}))

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT created_at, workspace_id, task_id FROM task_output WHERE id = $1`)).
		WithArgs("output-1").
		WillReturnRows(sqlmock.NewRows([]string{"created_at", "workspace_id", "task_id"}).AddRow(createdAt, int64(7), "task-1"))

	if err := backend.CreateTaskOutput(context.Background(), output); err != nil {
		t.Fatalf("CreateTaskOutput returned error: %v", err)
	}
	if !output.CreatedAt.Equal(createdAt) {
		t.Fatalf("expected created_at %v, got %v", createdAt, output.CreatedAt)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestCreateTaskOutputRejectsCrossTaskConflict(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backend := &PostgresBackend{db: db}
	output := &types.TaskOutput{
		ID:          "output-1",
		WorkspaceID: 7,
		TaskID:      "task-1",
		OutputType:  "report",
		Title:       "Report",
		Data:        map[string]any{},
		Status:      types.TaskOutputStatusPending,
	}
	createdAt := time.Now().UTC()

	mock.ExpectQuery(regexp.QuoteMeta(`
			INSERT INTO task_output (id, workspace_id, task_id, run_id, agent_id, output_type, title, summary, uri, data_json, metadata_json, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT (id) DO NOTHING
			RETURNING created_at`)).
		WithArgs(
			"output-1",
			uint(7),
			"task-1",
			nil,
			nil,
			"report",
			"Report",
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
			types.TaskOutputStatusPending,
		).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}))

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT created_at, workspace_id, task_id FROM task_output WHERE id = $1`)).
		WithArgs("output-1").
		WillReturnRows(sqlmock.NewRows([]string{"created_at", "workspace_id", "task_id"}).AddRow(createdAt, int64(9), "task-2"))

	err = backend.CreateTaskOutput(context.Background(), output)
	if err == nil {
		t.Fatal("expected CreateTaskOutput to return conflict error")
	}

	var conflictErr *types.ErrTaskOutputConflict
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected ErrTaskOutputConflict, got %T: %v", err, err)
	}
	if conflictErr.ExistingWorkspaceID != 9 || conflictErr.ExistingTaskID != "task-2" {
		t.Fatalf("unexpected conflict scope: %+v", conflictErr)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestClaimQueuedTaskForDispatchClearsTargetRunID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	backend := &PostgresBackend{db: db}

	taskID := "7ab0d8a4-7f2b-4fc5-b08a-08dcf6cc4fb1"
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)UPDATE agent_task.*SET state = 'running'::agent_task_state,.*target_run_id = NULL.*RETURNING id`).
		WithArgs(taskID, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(taskID))
	mock.ExpectExec(regexp.QuoteMeta(`
		DELETE FROM task_wake_agenda_item WHERE task_id = $1
	`)).
		WithArgs(taskID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectQuery(`(?s)SELECT id, workspace_id, agent_id, agent_name, queue_mode, state,.*WHERE id = \$1`).
		WithArgs(taskID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "workspace_id", "agent_id", "agent_name", "queue_mode", "state",
			"idempotency_key", "payload_json", "routing_json", "parent_envelope_id", "target_run_id", "current_blocker_id",
			"accepted_at", "queued_at", "dispatched_at", "deadline", "dropped_reason", "priority", "budget_usd", "cost_usd", "archived_at",
			"created_at", "updated_at", "wake_at", "wake_reason", "wake_count", "input_kind", "waiting_summary",
		}).AddRow(
			taskID,
			uint(347),
			nil,
			"",
			string(types.AgentQueueModeQueue),
			string(types.AgentTaskStateRunning),
			"claim-test",
			[]byte(`{}`),
			[]byte(`{}`),
			nil,
			nil,
			nil,
			now,
			now,
			now,
			nil,
			nil,
			string(types.AgentTaskPriorityNormal),
			nil,
			nil,
			nil,
			now,
			now,
			nil,
			nil,
			0,
			nil,
			nil,
		))

	task, claimed, err := backend.ClaimQueuedTaskForDispatch(context.Background(), taskID, 45*time.Second)
	if err != nil {
		t.Fatalf("ClaimQueuedTaskForDispatch returned error: %v", err)
	}
	if !claimed {
		t.Fatal("claimed = false, want true")
	}
	if task == nil {
		t.Fatal("task = nil, want task")
	}
	if task.TargetRunID != nil {
		t.Fatalf("target_run_id = %v, want nil", *task.TargetRunID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestGetWatchedSourceQueriesPrioritizesTaskFollowups(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	store := &filesystemStore{db: db}

	mock.ExpectQuery(`(?s)WITH watched_queries AS .*SELECT DISTINCT ON \(q.id\).*CASE WHEN q.lifecycle = 'task_followup' THEN 0 ELSE 1 END AS lifecycle_rank.*CASE WHEN q.system_managed THEN 0 ELSE 1 END AS managed_rank.*FROM watched_queries w.*ORDER BY.*w.lifecycle_rank.*w.managed_rank.*LIMIT \$2`).
		WithArgs("120 seconds", 5).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "external_id", "workspace_id", "credential_member_id", "system_managed", "lifecycle", "owner_task_id", "owner_run_id",
			"integration", "path", "name", "query_spec", "guidance", "output_format", "file_ext", "filename_format", "cache_ttl", "mode", "filter",
			"created_at", "updated_at", "last_executed",
		}))

	queries, err := store.GetWatchedSourceQueries(context.Background(), 2*time.Minute, 5)
	if err != nil {
		t.Fatalf("GetWatchedSourceQueries returned error: %v", err)
	}
	if len(queries) != 0 {
		t.Fatalf("query count = %d, want 0", len(queries))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestBindOutputsToBlockerTxUpdatesMetadataJSON(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("db.Begin: %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(`
		UPDATE task_output
		SET metadata_json = COALESCE(metadata_json, '{}'::jsonb) || jsonb_build_object($3::text, $4::text)
		WHERE workspace_id = $1
		  AND id = ANY($2::uuid[])
	`)).
		WithArgs(
			uint(7),
			sqlmock.AnyArg(),
			types.TaskOutputMetadataBlockerID,
			"blocker-1",
		).
		WillReturnResult(sqlmock.NewResult(0, 2))

	if err := bindOutputsToBlockerTx(
		context.Background(),
		tx,
		7,
		"blocker-1",
		[]string{"out-1", "out-2"},
	); err != nil {
		t.Fatalf("bindOutputsToBlockerTx returned error: %v", err)
	}

	mock.ExpectCommit()
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}
