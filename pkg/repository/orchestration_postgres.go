package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/lib/pq"
)

const agentTaskSelect = `
	SELECT id, workspace_id, agent_id, agent_name, queue_mode, state, idempotency_key, payload_json, routing_json,
	       parent_envelope_id, target_run_id, accepted_at, queued_at, dispatched_at, dropped_reason, archived_at, created_at, updated_at
	FROM (
		SELECT
			t.id,
			t.workspace_id,
			t.agent_id,
			COALESCE(ap.name, '') AS agent_name,
			t.queue_mode,
			t.state,
			t.kind::text AS kind,
			t.idempotency_key,
			t.payload_json,
			t.routing_json,
			t.parent_envelope_id,
			t.target_run_id,
			t.accepted_at,
			t.queued_at,
			t.dispatched_at,
			t.dropped_reason,
			t.archived_at,
			t.created_at,
			t.updated_at
		FROM agent_task t
		LEFT JOIN agent_profile ap ON ap.id = t.agent_id
	) task_view
`

const agentRunSelect = `
	SELECT id, workspace_id, agent_id, origin_task_id, hook_id, status, session_id, session_key, provider, model,
	       exec_host, exec_security, exec_ask, runtime_type, workspace_access, network_enabled, interactive,
	       timeout_ms, started_at, ended_at, claimed_by_worker_id, claim_heartbeat_at, claim_expires_at,
	       error, snapshot_ts, usage_json, delivery_json, created_at, updated_at
	FROM agent_run
`

const runAttemptFromRunSelect = `
	SELECT id, run_attempt_id, attempt, status, provider, model,
	       exec_host, exec_security, exec_ask, runtime_type, workspace_access,
	       network_enabled, interactive, image,
	       started_at, ended_at, exit_code, error, created_at, updated_at
	FROM agent_run
`

func marshalJSONMap(v map[string]any) ([]byte, error) {
	if v == nil {
		v = map[string]any{}
	}
	return json.Marshal(v)
}

func unmarshalJSONMap(data []byte) map[string]any {
	if len(data) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return map[string]any{}
	}
	if out == nil {
		out = map[string]any{}
	}
	return out
}

func normalizeLimitOffset(limit, offset, defaultLimit, maxLimit int) (int, int) {
	if limit <= 0 {
		limit = defaultLimit
	}
	if maxLimit > 0 && limit > maxLimit {
		limit = maxLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func optionalStringArg(value *string) any {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func optionalUintArg(value *uint) any {
	if value == nil || *value == 0 {
		return nil
	}
	return int64(*value)
}

func (b *PostgresBackend) CreateAgentProfile(ctx context.Context, profile *types.AgentProfile) error {
	configJSON, err := marshalJSONMap(profile.ConfigJSON)
	if err != nil {
		return fmt.Errorf("marshal agent config: %w", err)
	}
	if profile.AgentKey == "" {
		return fmt.Errorf("agent_key is required")
	}
	if profile.Name == "" {
		return fmt.Errorf("name is required")
	}
	query := `
		INSERT INTO agent_profile (workspace_id, agent_key, name, config_json, active)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, created_at, updated_at
	`
	if err := b.db.QueryRowContext(
		ctx,
		query,
		profile.WorkspaceID,
		profile.AgentKey,
		profile.Name,
		configJSON,
		profile.Active,
	).Scan(&profile.ID, &profile.CreatedAt, &profile.UpdatedAt); err != nil {
		return fmt.Errorf("create agent profile: %w", err)
	}
	return nil
}

func (b *PostgresBackend) scanAgentProfile(row scanner) (*types.AgentProfile, error) {
	profile := &types.AgentProfile{}
	var configJSON []byte
	err := row.Scan(
		&profile.ID,
		&profile.WorkspaceID,
		&profile.AgentKey,
		&profile.Name,
		&configJSON,
		&profile.Active,
		&profile.CreatedAt,
		&profile.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, &types.ErrAgentProfileNotFound{}
	}
	if err != nil {
		return nil, err
	}
	profile.ConfigJSON = unmarshalJSONMap(configJSON)
	return profile, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func (b *PostgresBackend) GetAgentProfile(ctx context.Context, workspaceId uint, agentId string) (*types.AgentProfile, error) {
	query := `
		SELECT id, workspace_id, agent_key, name, config_json, active, created_at, updated_at
		FROM agent_profile
		WHERE workspace_id = $1 AND id = $2
	`
	profile, err := b.scanAgentProfile(b.db.QueryRowContext(ctx, query, workspaceId, agentId))
	if err != nil {
		if _, ok := err.(*types.ErrAgentProfileNotFound); ok {
			return nil, &types.ErrAgentProfileNotFound{ID: agentId}
		}
		return nil, fmt.Errorf("get agent profile: %w", err)
	}
	return profile, nil
}

func (b *PostgresBackend) GetAgentProfileByKey(ctx context.Context, workspaceId uint, agentKey string) (*types.AgentProfile, error) {
	query := `
		SELECT id, workspace_id, agent_key, name, config_json, active, created_at, updated_at
		FROM agent_profile
		WHERE workspace_id = $1 AND agent_key = $2
	`
	profile, err := b.scanAgentProfile(b.db.QueryRowContext(ctx, query, workspaceId, agentKey))
	if err != nil {
		if _, ok := err.(*types.ErrAgentProfileNotFound); ok {
			return nil, &types.ErrAgentProfileNotFound{ID: agentKey}
		}
		return nil, fmt.Errorf("get agent profile by key: %w", err)
	}
	return profile, nil
}

func (b *PostgresBackend) ListAgentProfiles(ctx context.Context, workspaceId uint) ([]*types.AgentProfile, error) {
	query := `
		SELECT id, workspace_id, agent_key, name, config_json, active, created_at, updated_at
		FROM agent_profile
		WHERE workspace_id = $1
		ORDER BY created_at DESC
	`
	rows, err := b.db.QueryContext(ctx, query, workspaceId)
	if err != nil {
		return nil, fmt.Errorf("list agent profiles: %w", err)
	}
	defer rows.Close()

	out := make([]*types.AgentProfile, 0)
	for rows.Next() {
		profile, err := b.scanAgentProfile(rows)
		if err != nil {
			return nil, fmt.Errorf("scan agent profile: %w", err)
		}
		out = append(out, profile)
	}
	return out, rows.Err()
}

func (b *PostgresBackend) UpdateAgentProfile(ctx context.Context, profile *types.AgentProfile) error {
	configJSON, err := marshalJSONMap(profile.ConfigJSON)
	if err != nil {
		return fmt.Errorf("marshal agent config: %w", err)
	}
	query := `
		UPDATE agent_profile
		SET name = $3, config_json = $4, active = $5, updated_at = CURRENT_TIMESTAMP
		WHERE workspace_id = $1 AND id = $2
		RETURNING updated_at
	`
	if err := b.db.QueryRowContext(
		ctx,
		query,
		profile.WorkspaceID,
		profile.ID,
		profile.Name,
		configJSON,
		profile.Active,
	).Scan(&profile.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return &types.ErrAgentProfileNotFound{ID: profile.ID}
		}
		return fmt.Errorf("update agent profile: %w", err)
	}
	return nil
}

func (b *PostgresBackend) DeleteAgentProfile(ctx context.Context, workspaceId uint, agentId string) error {
	query := `
		DELETE FROM agent_profile
		WHERE workspace_id = $1 AND id = $2
	`
	result, err := b.db.ExecContext(ctx, query, workspaceId, agentId)
	if err != nil {
		return fmt.Errorf("delete agent profile: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete agent profile rows affected: %w", err)
	}
	if rows == 0 {
		return &types.ErrAgentProfileNotFound{ID: agentId}
	}
	return nil
}

func (b *PostgresBackend) CreateTask(ctx context.Context, task *types.AgentTask) error {
	payloadJSON, err := marshalJSONMap(task.PayloadJSON)
	if err != nil {
		return fmt.Errorf("marshal task payload: %w", err)
	}
	routingJSON, err := marshalJSONMap(task.RoutingJSON)
	if err != nil {
		return fmt.Errorf("marshal task routing: %w", err)
	}
	query := `
		INSERT INTO agent_task (
			workspace_id, agent_id, kind, queue_mode, state, idempotency_key,
			payload_json, routing_json, parent_envelope_id, target_run_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id, accepted_at, created_at, updated_at
	`
	if err := b.db.QueryRowContext(
		ctx,
		query,
		task.WorkspaceID,
		task.AgentID,
		types.AgentTaskKindAgentCommand,
		task.QueueMode,
		task.State,
		task.IdempotencyKey,
		payloadJSON,
		routingJSON,
		task.ParentTaskID,
		task.TargetRunID,
	).Scan(&task.ID, &task.AcceptedAt, &task.CreatedAt, &task.UpdatedAt); err != nil {
		return fmt.Errorf("create agent task: %w", err)
	}
	return nil
}

func (b *PostgresBackend) scanAgentTask(row scanner) (*types.AgentTask, error) {
	task := &types.AgentTask{}
	var payloadJSON []byte
	var routingJSON []byte
	var agentID sql.NullString
	var agentName sql.NullString
	var parentID sql.NullString
	var targetRunID sql.NullString
	var queuedAt sql.NullTime
	var dispatchedAt sql.NullTime
	var droppedReason sql.NullString
	var archivedAt sql.NullTime
	err := row.Scan(
		&task.ID,
		&task.WorkspaceID,
		&agentID,
		&agentName,
		&task.QueueMode,
		&task.State,
		&task.IdempotencyKey,
		&payloadJSON,
		&routingJSON,
		&parentID,
		&targetRunID,
		&task.AcceptedAt,
		&queuedAt,
		&dispatchedAt,
		&droppedReason,
		&archivedAt,
		&task.CreatedAt,
		&task.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, &types.ErrAgentTaskNotFound{}
	}
	if err != nil {
		return nil, err
	}
	if agentID.Valid {
		task.AgentID = &agentID.String
	}
	if agentName.Valid {
		task.AgentName = agentName.String
	}
	if parentID.Valid {
		task.ParentTaskID = &parentID.String
	}
	if targetRunID.Valid {
		task.TargetRunID = &targetRunID.String
	}
	if queuedAt.Valid {
		task.QueuedAt = &queuedAt.Time
	}
	if dispatchedAt.Valid {
		task.DispatchedAt = &dispatchedAt.Time
	}
	if droppedReason.Valid {
		task.DroppedReason = &droppedReason.String
	}
	if archivedAt.Valid {
		task.ArchivedAt = &archivedAt.Time
	}
	task.PayloadJSON = unmarshalJSONMap(payloadJSON)
	task.RoutingJSON = unmarshalJSONMap(routingJSON)
	return task, nil
}

func (b *PostgresBackend) GetTask(ctx context.Context, workspaceId uint, taskID string) (*types.AgentTask, error) {
	query := agentTaskSelect + `
		WHERE workspace_id = $1 AND id = $2
	`
	task, err := b.scanAgentTask(b.db.QueryRowContext(ctx, query, workspaceId, taskID))
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return nil, &types.ErrAgentTaskNotFound{ID: taskID}
		}
		return nil, fmt.Errorf("get agent task: %w", err)
	}
	return task, nil
}

func (b *PostgresBackend) ListTasks(ctx context.Context, workspaceId uint, limit int) ([]*types.AgentTask, error) {
	if limit <= 0 {
		limit = 100
	}

	query := agentTaskSelect + `
		WHERE workspace_id = $1
		  AND archived_at IS NULL
		  AND kind::text = $2
		ORDER BY created_at DESC
		LIMIT $3
	`

	rows, err := b.db.QueryContext(ctx, query, workspaceId, types.AgentTaskKindAgentCommand, limit)
	if err != nil {
		return nil, fmt.Errorf("list agent tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]*types.AgentTask, 0, limit)
	for rows.Next() {
		task, err := b.scanAgentTask(rows)
		if err != nil {
			return nil, fmt.Errorf("scan agent task: %w", err)
		}
		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate agent task rows: %w", err)
	}

	return tasks, nil
}

func (b *PostgresBackend) ListTasksFiltered(ctx context.Context, workspaceId uint, filter types.AgentTaskListFilter) ([]*types.AgentTask, error) {
	limit, offset := normalizeLimitOffset(filter.Limit, filter.Offset, 100, 500)

	stateValues := make([]string, 0, len(filter.States))
	for _, state := range filter.States {
		if state == "" {
			continue
		}
		stateValues = append(stateValues, string(state))
	}

	var statesArg any
	if len(stateValues) > 0 {
		statesArg = pq.Array(stateValues)
	}

	query := agentTaskSelect + `
		WHERE workspace_id = $1
		  AND archived_at IS NULL
		  AND ($2::uuid IS NULL OR agent_id = $2::uuid)
		  AND kind::text = $3
		  AND ($4::text[] IS NULL OR state::text = ANY($4::text[]))
		  AND ($5::timestamptz IS NULL OR created_at >= $5::timestamptz)
		  AND ($6::timestamptz IS NULL OR created_at <= $6::timestamptz)
		ORDER BY created_at DESC, id DESC
		LIMIT $7 OFFSET $8
	`

	rows, err := b.db.QueryContext(
		ctx,
		query,
		workspaceId,
		optionalStringArg(filter.AgentID),
		types.AgentTaskKindAgentCommand,
		statesArg,
		filter.CreatedAfter,
		filter.CreatedBefore,
		limit,
		offset,
	)
	if err != nil {
		return nil, fmt.Errorf("list filtered agent tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]*types.AgentTask, 0, limit)
	for rows.Next() {
		task, err := b.scanAgentTask(rows)
		if err != nil {
			return nil, fmt.Errorf("scan filtered agent task: %w", err)
		}
		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate filtered agent task rows: %w", err)
	}

	return tasks, nil
}

func (b *PostgresBackend) GetTaskByID(ctx context.Context, taskID string) (*types.AgentTask, error) {
	query := agentTaskSelect + `
		WHERE id = $1
	`
	task, err := b.scanAgentTask(b.db.QueryRowContext(ctx, query, taskID))
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return nil, &types.ErrAgentTaskNotFound{ID: taskID}
		}
		return nil, fmt.Errorf("get task by id: %w", err)
	}
	return task, nil
}

func (b *PostgresBackend) GetTaskByIdempotency(ctx context.Context, workspaceId uint, agentId *string, idempotencyKey string) (*types.AgentTask, error) {
	query := agentTaskSelect + `
		WHERE workspace_id = $1
		  AND idempotency_key = $2
		  AND (($3::uuid IS NULL AND agent_id IS NULL) OR agent_id = $3::uuid)
		  AND kind::text = $4
		ORDER BY created_at DESC
		LIMIT 1
	`
	var agentArg any
	if agentId != nil {
		agentArg = *agentId
	}
	task, err := b.scanAgentTask(
		b.db.QueryRowContext(
			ctx,
			query,
			workspaceId,
			idempotencyKey,
			agentArg,
			types.AgentTaskKindAgentCommand,
		),
	)
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return nil, &types.ErrAgentTaskNotFound{ID: idempotencyKey}
		}
		return nil, fmt.Errorf("get task by idempotency: %w", err)
	}
	return task, nil
}

func (b *PostgresBackend) UpdateTaskState(ctx context.Context, taskID string, state types.AgentTaskState, droppedReason *string, targetRunID *string) error {
	now := time.Now()
	query := `
		UPDATE agent_task
		SET state = $2::agent_task_state,
		    queued_at = CASE WHEN $2::agent_task_state = 'queued'::agent_task_state THEN $3 ELSE queued_at END,
		    dispatched_at = CASE WHEN $2::agent_task_state = 'running'::agent_task_state THEN $3 ELSE dispatched_at END,
		    dropped_reason = CASE WHEN $2::agent_task_state = 'dropped'::agent_task_state THEN $4 ELSE dropped_reason END,
		    target_run_id = COALESCE($5::uuid, target_run_id),
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	res, err := b.db.ExecContext(ctx, query, taskID, state, now, droppedReason, targetRunID)
	if err != nil {
		return fmt.Errorf("update task state: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return &types.ErrAgentTaskNotFound{ID: taskID}
	}
	return nil
}

func (b *PostgresBackend) UpdateTaskStateIfCurrentRun(
	ctx context.Context,
	taskID string,
	expectedRunID string,
	state types.AgentTaskState,
	droppedReason *string,
	targetRunID *string,
) (bool, error) {
	now := time.Now()
	baseArgs := []any{taskID, state, now, droppedReason, targetRunID}
	expectedRunID = strings.TrimSpace(expectedRunID)

	query := `
		UPDATE agent_task
		SET state = $2::agent_task_state,
		    queued_at = CASE WHEN $2::agent_task_state = 'queued'::agent_task_state THEN $3 ELSE queued_at END,
		    dispatched_at = CASE WHEN $2::agent_task_state = 'running'::agent_task_state THEN $3 ELSE dispatched_at END,
		    dropped_reason = CASE WHEN $2::agent_task_state = 'dropped'::agent_task_state THEN $4 ELSE dropped_reason END,
		    target_run_id = COALESCE($5::uuid, target_run_id),
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		  AND state NOT IN ('done'::agent_task_state, 'dropped'::agent_task_state, 'cancelled'::agent_task_state)
	`
	var args []any
	if expectedRunID == "" {
		query += " AND target_run_id IS NULL"
		args = baseArgs
	} else {
		query += " AND target_run_id = $6::uuid"
		args = append(baseArgs, expectedRunID)
	}

	res, err := b.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("update task state if current run: %w", err)
	}
	affected, _ := res.RowsAffected()
	return affected > 0, nil
}

func (b *PostgresBackend) ArchiveTask(ctx context.Context, taskID string) error {
	query := `
		UPDATE agent_task
		SET archived_at = COALESCE(archived_at, CURRENT_TIMESTAMP),
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	res, err := b.db.ExecContext(ctx, query, taskID)
	if err != nil {
		return fmt.Errorf("archive task: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return &types.ErrAgentTaskNotFound{ID: taskID}
	}
	return nil
}

func (b *PostgresBackend) CreateAgentRun(ctx context.Context, run *types.AgentRun) error {
	usageJSON, err := marshalJSONMap(run.UsageJSON)
	if err != nil {
		return fmt.Errorf("marshal run usage: %w", err)
	}
	deliveryJSON, err := marshalJSONMap(run.DeliveryJSON)
	if err != nil {
		return fmt.Errorf("marshal run delivery: %w", err)
	}
	query := `
		INSERT INTO agent_run (
			workspace_id, agent_id, origin_task_id, hook_id, status, session_id, session_key,
			provider, model, exec_host, exec_security, exec_ask, runtime_type, workspace_access,
			network_enabled, interactive, timeout_ms, usage_json, delivery_json
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10, $11, $12, $13, $14,
			$15, $16, $17, $18, $19
		)
		RETURNING id, snapshot_ts, created_at, updated_at
	`
	if err := b.db.QueryRowContext(
		ctx,
		query,
		run.WorkspaceID,
		run.AgentID,
		run.OriginTaskID,
		optionalUintArg(run.HookID),
		run.Status,
		run.SessionID,
		run.SessionKey,
		run.Provider,
		run.Model,
		run.ExecHost,
		run.ExecSecurity,
		run.ExecAsk,
		run.RuntimeType,
		run.WorkspaceAccess,
		run.NetworkEnabled,
		run.Interactive,
		run.TimeoutMs,
		usageJSON,
		deliveryJSON,
	).Scan(&run.ID, &run.SnapshotTS, &run.CreatedAt, &run.UpdatedAt); err != nil {
		return fmt.Errorf("create run: %w", err)
	}
	return nil
}

func (b *PostgresBackend) scanAgentRun(row scanner) (*types.AgentRun, error) {
	run := &types.AgentRun{}
	var usageJSON []byte
	var deliveryJSON []byte
	var agentID sql.NullString
	var hookID sql.NullInt64
	var sessionKey sql.NullString
	var provider sql.NullString
	var model sql.NullString
	var startedAt sql.NullTime
	var endedAt sql.NullTime
	var claimedByWorker sql.NullString
	var claimHeartbeatAt sql.NullTime
	var claimExpiresAt sql.NullTime
	var errMsg sql.NullString
	err := row.Scan(
		&run.ID,
		&run.WorkspaceID,
		&agentID,
		&run.OriginTaskID,
		&hookID,
		&run.Status,
		&run.SessionID,
		&sessionKey,
		&provider,
		&model,
		&run.ExecHost,
		&run.ExecSecurity,
		&run.ExecAsk,
		&run.RuntimeType,
		&run.WorkspaceAccess,
		&run.NetworkEnabled,
		&run.Interactive,
		&run.TimeoutMs,
		&startedAt,
		&endedAt,
		&claimedByWorker,
		&claimHeartbeatAt,
		&claimExpiresAt,
		&errMsg,
		&run.SnapshotTS,
		&usageJSON,
		&deliveryJSON,
		&run.CreatedAt,
		&run.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, &types.ErrAgentRunNotFound{}
	}
	if err != nil {
		return nil, err
	}
	if agentID.Valid {
		run.AgentID = &agentID.String
	}
	if hookID.Valid {
		v := uint(hookID.Int64)
		run.HookID = &v
	}
	if sessionKey.Valid {
		run.SessionKey = &sessionKey.String
	}
	if provider.Valid {
		run.Provider = &provider.String
	}
	if model.Valid {
		run.Model = &model.String
	}
	if startedAt.Valid {
		run.StartedAt = &startedAt.Time
	}
	if endedAt.Valid {
		run.EndedAt = &endedAt.Time
	}
	if claimedByWorker.Valid {
		run.ClaimedByWorker = &claimedByWorker.String
	}
	if claimHeartbeatAt.Valid {
		run.ClaimHeartbeatAt = &claimHeartbeatAt.Time
	}
	if claimExpiresAt.Valid {
		run.ClaimExpiresAt = &claimExpiresAt.Time
	}
	if errMsg.Valid {
		run.Error = &errMsg.String
	}
	run.UsageJSON = unmarshalJSONMap(usageJSON)
	run.DeliveryJSON = unmarshalJSONMap(deliveryJSON)
	return run, nil
}

func (b *PostgresBackend) GetAgentRun(ctx context.Context, workspaceId uint, runId string) (*types.AgentRun, error) {
	query := agentRunSelect + `
		WHERE workspace_id = $1 AND id = $2
	`
	run, err := b.scanAgentRun(b.db.QueryRowContext(ctx, query, workspaceId, runId))
	if err != nil {
		if _, ok := err.(*types.ErrAgentRunNotFound); ok {
			return nil, &types.ErrAgentRunNotFound{ID: runId}
		}
		return nil, fmt.Errorf("get run: %w", err)
	}
	return run, nil
}

func (b *PostgresBackend) GetAgentRunByID(ctx context.Context, runId string) (*types.AgentRun, error) {
	query := agentRunSelect + `
		WHERE id = $1
	`
	run, err := b.scanAgentRun(b.db.QueryRowContext(ctx, query, runId))
	if err != nil {
		if _, ok := err.(*types.ErrAgentRunNotFound); ok {
			return nil, &types.ErrAgentRunNotFound{ID: runId}
		}
		return nil, fmt.Errorf("get run by id: %w", err)
	}
	return run, nil
}

func (b *PostgresBackend) ListAgentRuns(ctx context.Context, workspaceId uint, limit int) ([]*types.AgentRun, error) {
	if limit <= 0 {
		limit = 100
	}
	query := agentRunSelect + `
		WHERE workspace_id = $1
		ORDER BY created_at DESC
		LIMIT $2
	`
	rows, err := b.db.QueryContext(ctx, query, workspaceId, limit)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()
	out := make([]*types.AgentRun, 0)
	for rows.Next() {
		run, err := b.scanAgentRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func (b *PostgresBackend) ListAgentRunsFiltered(ctx context.Context, workspaceId uint, filter types.AgentRunListFilter) ([]*types.AgentRun, error) {
	limit, offset := normalizeLimitOffset(filter.Limit, filter.Offset, 100, 500)

	statusValues := make([]string, 0, len(filter.Statuses))
	for _, status := range filter.Statuses {
		if status == "" {
			continue
		}
		statusValues = append(statusValues, string(status))
	}

	var statusesArg any
	if len(statusValues) > 0 {
		statusesArg = pq.Array(statusValues)
	}

	query := agentRunSelect + `
		WHERE workspace_id = $1
		  AND ($2::uuid IS NULL OR agent_id = $2::uuid)
		  AND ($3::text[] IS NULL OR status::text = ANY($3::text[]))
		  AND ($4::text IS NULL OR session_id = $4::text)
		  AND ($5::timestamptz IS NULL OR created_at >= $5::timestamptz)
		  AND ($6::timestamptz IS NULL OR created_at <= $6::timestamptz)
		ORDER BY created_at DESC, id DESC
		LIMIT $7 OFFSET $8
	`

	rows, err := b.db.QueryContext(
		ctx,
		query,
		workspaceId,
		optionalStringArg(filter.AgentID),
		statusesArg,
		optionalStringArg(filter.SessionID),
		filter.CreatedAfter,
		filter.CreatedBefore,
		limit,
		offset,
	)
	if err != nil {
		return nil, fmt.Errorf("list filtered runs: %w", err)
	}
	defer rows.Close()

	out := make([]*types.AgentRun, 0, limit)
	for rows.Next() {
		run, err := b.scanAgentRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan filtered run: %w", err)
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func (b *PostgresBackend) ListActiveRunsBySession(
	ctx context.Context,
	workspaceId uint,
	sessionID string,
	excludeRunIDs []string,
	limit int,
) ([]*types.AgentRun, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return []*types.AgentRun{}, nil
	}
	if limit <= 0 {
		limit = 10
	}

	exclude := make([]string, 0, len(excludeRunIDs))
	for _, runID := range excludeRunIDs {
		runID = strings.TrimSpace(runID)
		if runID == "" {
			continue
		}
		exclude = append(exclude, runID)
	}
	var excludeArg any
	if len(exclude) > 0 {
		excludeArg = pq.Array(exclude)
	}

	query := agentRunSelect + `
		WHERE workspace_id = $1
		  AND session_id = $2
		  AND ` + runExecutionActiveWhere + `
		  AND ($3::uuid[] IS NULL OR id <> ALL($3::uuid[]))
		ORDER BY created_at DESC, id DESC
		LIMIT $4
	`
	rows, err := b.db.QueryContext(ctx, query, workspaceId, sessionID, excludeArg, limit)
	if err != nil {
		return nil, fmt.Errorf("list active runs by session: %w", err)
	}
	defer rows.Close()

	out := make([]*types.AgentRun, 0, limit)
	for rows.Next() {
		run, err := b.scanAgentRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan active run by session: %w", err)
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func (b *PostgresBackend) UpdateAgentRunLifecycle(ctx context.Context, runId string, status types.AgentRunStatus, startedAt, endedAt *time.Time, errorMsg *string) error {
	query := `
		UPDATE agent_run
		SET status = $2,
		    started_at = COALESCE($3, started_at),
		    ended_at = COALESCE($4, ended_at),
		    error = $5,
		    claimed_by_worker_id = CASE
		        WHEN $2 IN ('ok'::agent_run_status, 'error'::agent_run_status, 'timeout'::agent_run_status, 'cancelled'::agent_run_status)
		          THEN NULL
		        ELSE claimed_by_worker_id
		    END,
		    claim_heartbeat_at = CASE
		        WHEN $2 IN ('ok'::agent_run_status, 'error'::agent_run_status, 'timeout'::agent_run_status, 'cancelled'::agent_run_status)
		          THEN NULL
		        ELSE claim_heartbeat_at
		    END,
		    claim_expires_at = CASE
		        WHEN $2 IN ('ok'::agent_run_status, 'error'::agent_run_status, 'timeout'::agent_run_status, 'cancelled'::agent_run_status)
		          THEN NULL
		        ELSE claim_expires_at
		    END,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	res, err := b.db.ExecContext(ctx, query, runId, status, startedAt, endedAt, errorMsg)
	if err != nil {
		return fmt.Errorf("update run lifecycle: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return &types.ErrAgentRunNotFound{ID: runId}
	}
	return nil
}

func (b *PostgresBackend) SetAgentRunClaim(ctx context.Context, runId string, workerId string, heartbeatAt time.Time, expiresAt time.Time) error {
	query := `
		UPDATE agent_run
		SET claimed_by_worker_id = $2,
		    claim_heartbeat_at = $3,
		    claim_expires_at = $4,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		  AND run_attempt_id IS NOT NULL
		  AND ` + runExecutionActiveWhere + `
		  AND (
		      claimed_by_worker_id IS NULL
		      OR claimed_by_worker_id = $2
		      OR claim_expires_at IS NULL
		      OR claim_expires_at <= $3
		  )
	`
	res, err := b.db.ExecContext(ctx, query, runId, workerId, heartbeatAt, expiresAt)
	if err != nil {
		return fmt.Errorf("set run claim: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		run, lookupErr := b.GetAgentRunByID(ctx, runId)
		if lookupErr != nil {
			return lookupErr
		}
		if run.Status.IsTerminal() {
			return fmt.Errorf("set run claim: run is already terminal")
		}
		return fmt.Errorf("set run claim: run is already claimed by another worker")
	}
	return nil
}

func (b *PostgresBackend) ClearAgentRunClaim(ctx context.Context, runId string) error {
	query := `
		UPDATE agent_run
		SET claimed_by_worker_id = NULL,
		    claim_heartbeat_at = NULL,
		    claim_expires_at = NULL,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	res, err := b.db.ExecContext(ctx, query, runId)
	if err != nil {
		return fmt.Errorf("clear run claim: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return &types.ErrAgentRunNotFound{ID: runId}
	}
	return nil
}

func (b *PostgresBackend) ClearExpiredAgentRunClaim(ctx context.Context, runId string, workerId string, expiresAt time.Time) (bool, error) {
	query := `
		UPDATE agent_run
		SET claimed_by_worker_id = NULL,
		    claim_heartbeat_at = NULL,
		    claim_expires_at = NULL,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		  AND ` + runExecutionActiveWhere + `
		  AND claimed_by_worker_id = $2
		  AND claim_expires_at IS NOT NULL
		  AND claim_expires_at <= $3
	`
	res, err := b.db.ExecContext(ctx, query, runId, workerId, expiresAt)
	if err != nil {
		return false, fmt.Errorf("clear expired run claim: %w", err)
	}
	affected, _ := res.RowsAffected()
	return affected > 0, nil
}

func (b *PostgresBackend) RefreshAgentRunClaims(ctx context.Context, workerId string, heartbeatAt time.Time, expiresAt time.Time) (int64, error) {
	query := `
		UPDATE agent_run
		SET claim_heartbeat_at = $2,
		    claim_expires_at = $3,
		    updated_at = CURRENT_TIMESTAMP
		WHERE claimed_by_worker_id = $1
		  AND run_attempt_id IS NOT NULL
		  AND ` + runExecutionActiveWhere + `
	`
	res, err := b.db.ExecContext(ctx, query, workerId, heartbeatAt, expiresAt)
	if err != nil {
		return 0, fmt.Errorf("refresh run claims: %w", err)
	}
	affected, _ := res.RowsAffected()
	return affected, nil
}

func (b *PostgresBackend) ListClaimedAgentRuns(ctx context.Context, limit int) ([]*types.AgentRun, error) {
	limit = normalizeClaimBatchLimit(limit)
	query := agentRunSelect + `
		WHERE run_attempt_id IS NOT NULL
		  AND ` + runExecutionActiveWhere + `
		  AND claimed_by_worker_id IS NOT NULL
		ORDER BY claim_heartbeat_at ASC NULLS FIRST, updated_at ASC
		LIMIT $1
	`
	rows, err := b.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("list claimed runs: %w", err)
	}
	defer rows.Close()

	out := make([]*types.AgentRun, 0)
	for rows.Next() {
		run, err := b.scanAgentRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan claimed run: %w", err)
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func normalizeClaimBatchLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	if limit > 500 {
		return 500
	}
	return limit
}

func (b *PostgresBackend) ListExpiredClaimedAgentRuns(ctx context.Context, now time.Time, limit int) ([]*types.AgentRun, error) {
	limit = normalizeClaimBatchLimit(limit)
	query := agentRunSelect + `
		WHERE run_attempt_id IS NOT NULL
		  AND ` + runExecutionActiveWhere + `
		  AND claimed_by_worker_id IS NOT NULL
		  AND claim_expires_at IS NOT NULL
		  AND claim_expires_at < $1
		ORDER BY claim_expires_at ASC
		LIMIT $2
	`
	rows, err := b.db.QueryContext(ctx, query, now, limit)
	if err != nil {
		return nil, fmt.Errorf("list expired claimed runs: %w", err)
	}
	defer rows.Close()

	out := make([]*types.AgentRun, 0)
	for rows.Next() {
		run, err := b.scanAgentRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan expired claimed run: %w", err)
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func (b *PostgresBackend) ListStaleUnclaimedAgentRuns(ctx context.Context, cutoff time.Time, limit int) ([]*types.AgentRun, error) {
	limit = normalizeClaimBatchLimit(limit)
	query := agentRunSelect + `
		WHERE run_attempt_id IS NOT NULL
		  AND ` + runExecutionActiveWhere + `
		  AND claimed_by_worker_id IS NULL
		  AND updated_at < $1
		ORDER BY updated_at ASC
		LIMIT $2
	`
	rows, err := b.db.QueryContext(ctx, query, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("list stale unclaimed runs: %w", err)
	}
	defer rows.Close()

	out := make([]*types.AgentRun, 0)
	for rows.Next() {
		run, err := b.scanAgentRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan stale unclaimed run: %w", err)
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func (b *PostgresBackend) IncrementAgentRunSnapshotSeq(ctx context.Context, runId string) (int64, error) {
	var seq int64
	query := `
		UPDATE agent_run
		SET snapshot_ts = snapshot_ts + 1, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		RETURNING snapshot_ts
	`
	if err := b.db.QueryRowContext(ctx, query, runId).Scan(&seq); err != nil {
		if err == sql.ErrNoRows {
			return 0, &types.ErrAgentRunNotFound{ID: runId}
		}
		return 0, fmt.Errorf("increment snapshot seq: %w", err)
	}
	return seq, nil
}

func (b *PostgresBackend) CreateAgentRunAttempt(ctx context.Context, attempt *types.AgentRunAttempt) error {
	if attempt == nil {
		return fmt.Errorf("attempt is required")
	}
	attempt.RunID = strings.TrimSpace(attempt.RunID)
	if attempt.RunID == "" {
		return fmt.Errorf("run_id is required")
	}
	if attempt.AttemptNo <= 0 {
		attempt.AttemptNo = 1
	}
	if strings.TrimSpace(attempt.ID) == "" {
		if err := b.db.QueryRowContext(ctx, `SELECT uuid_generate_v4()`).Scan(&attempt.ID); err != nil {
			return fmt.Errorf("generate run attempt id: %w", err)
		}
	}
	attempt.ID = strings.TrimSpace(attempt.ID)
	if attempt.Strategy == "" {
		if attempt.AttemptNo > 1 {
			attempt.Strategy = types.AgentAttemptStrategyRetry
		} else {
			attempt.Strategy = types.AgentAttemptStrategyPrimary
		}
	}
	if attempt.Status == "" {
		attempt.Status = types.AgentAttemptStatusPending
	}

	query := `
		UPDATE agent_run
		SET run_attempt_id = $2::uuid,
		    attempt = $3,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		RETURNING provider, model, exec_host, exec_security, exec_ask, runtime_type, workspace_access,
		          network_enabled, interactive, created_at, updated_at
	`
	var provider sql.NullString
	var model sql.NullString
	if err := b.db.QueryRowContext(ctx, query, attempt.RunID, attempt.ID, attempt.AttemptNo).Scan(
		&provider,
		&model,
		&attempt.ExecHost,
		&attempt.ExecSecurity,
		&attempt.ExecAsk,
		&attempt.RuntimeType,
		&attempt.WorkspaceAccess,
		&attempt.NetworkEnabled,
		&attempt.Interactive,
		&attempt.CreatedAt,
		&attempt.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return &types.ErrAgentRunNotFound{ID: attempt.RunID}
		}
		return fmt.Errorf("create run attempt: %w", err)
	}
	if provider.Valid && attempt.Provider == nil {
		attempt.Provider = &provider.String
	}
	if model.Valid && attempt.Model == nil {
		attempt.Model = &model.String
	}
	return nil
}

func (b *PostgresBackend) scanAgentRunAttempt(row scanner) (*types.AgentRunAttempt, bool, error) {
	attempt := &types.AgentRunAttempt{}
	var runID string
	var runAttemptID sql.NullString
	var runStatus types.AgentRunStatus
	var image sql.NullString
	var provider sql.NullString
	var model sql.NullString
	var startedAt sql.NullTime
	var endedAt sql.NullTime
	var exitCode sql.NullInt32
	var errMsg sql.NullString
	err := row.Scan(
		&runID,
		&runAttemptID,
		&attempt.AttemptNo,
		&runStatus,
		&provider,
		&model,
		&attempt.ExecHost,
		&attempt.ExecSecurity,
		&attempt.ExecAsk,
		&attempt.RuntimeType,
		&attempt.WorkspaceAccess,
		&attempt.NetworkEnabled,
		&attempt.Interactive,
		&image,
		&startedAt,
		&endedAt,
		&exitCode,
		&errMsg,
		&attempt.CreatedAt,
		&attempt.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, false, &types.ErrAgentRunAttemptNotFound{}
	}
	if err != nil {
		return nil, false, err
	}
	attempt.RunID = runID
	if runAttemptID.Valid && strings.TrimSpace(runAttemptID.String) != "" {
		attempt.ID = strings.TrimSpace(runAttemptID.String)
	} else {
		attempt.ID = runID
	}
	attempt.Status = types.AttemptStatusFromRunStatus(runStatus, attempt.ExecAsk, image.Valid && strings.TrimSpace(image.String) != "")
	if attempt.AttemptNo <= 0 {
		attempt.AttemptNo = 1
	}
	if attempt.AttemptNo > 1 {
		attempt.Strategy = types.AgentAttemptStrategyRetry
	} else {
		attempt.Strategy = types.AgentAttemptStrategyPrimary
	}
	if provider.Valid {
		attempt.Provider = &provider.String
	}
	if model.Valid {
		attempt.Model = &model.String
	}
	if image.Valid && strings.TrimSpace(image.String) != "" {
		executionID := runID
		attempt.ExecutionID = &executionID
	}
	if startedAt.Valid {
		attempt.StartedAt = &startedAt.Time
	}
	if endedAt.Valid {
		attempt.EndedAt = &endedAt.Time
	}
	if exitCode.Valid {
		v := int(exitCode.Int32)
		attempt.ExitCode = &v
	}
	if errMsg.Valid {
		attempt.Error = &errMsg.String
	}
	return attempt, runAttemptID.Valid && strings.TrimSpace(runAttemptID.String) != "", nil
}

func (b *PostgresBackend) GetAgentRunAttempt(ctx context.Context, attemptId string) (*types.AgentRunAttempt, error) {
	query := runAttemptFromRunSelect + `
		WHERE run_attempt_id = $1::uuid
		   OR id = $1::uuid
		ORDER BY CASE WHEN run_attempt_id = $1::uuid THEN 0 ELSE 1 END
		LIMIT 1
	`
	attempt, _, err := b.scanAgentRunAttempt(b.db.QueryRowContext(ctx, query, attemptId))
	if err != nil {
		if _, ok := err.(*types.ErrAgentRunAttemptNotFound); ok {
			return nil, &types.ErrAgentRunAttemptNotFound{ID: attemptId}
		}
		return nil, fmt.Errorf("get run attempt: %w", err)
	}
	return attempt, nil
}

func (b *PostgresBackend) ListAgentRunAttempts(ctx context.Context, runId string) ([]*types.AgentRunAttempt, error) {
	query := runAttemptFromRunSelect + `
		WHERE id = $1
	`
	attempt, hasAttemptID, err := b.scanAgentRunAttempt(b.db.QueryRowContext(ctx, query, runId))
	if err != nil {
		if _, ok := err.(*types.ErrAgentRunAttemptNotFound); ok {
			return []*types.AgentRunAttempt{}, nil
		}
		return nil, fmt.Errorf("list run attempts: %w", err)
	}
	if !hasAttemptID {
		return []*types.AgentRunAttempt{}, nil
	}
	return []*types.AgentRunAttempt{attempt}, nil
}

func (b *PostgresBackend) GetRunAttemptByExecutionID(ctx context.Context, executionID string) (*types.AgentRunAttempt, error) {
	query := runAttemptFromRunSelect + `
		WHERE id = $1::uuid
		  AND image IS NOT NULL
		  AND image <> ''
	`
	attempt, hasAttemptID, err := b.scanAgentRunAttempt(b.db.QueryRowContext(ctx, query, executionID))
	if err != nil {
		if _, ok := err.(*types.ErrAgentRunAttemptNotFound); ok {
			return nil, &types.ErrAgentRunAttemptNotFound{ID: executionID}
		}
		return nil, fmt.Errorf("get attempt by execution id: %w", err)
	}
	if !hasAttemptID {
		return nil, &types.ErrAgentRunAttemptNotFound{ID: executionID}
	}
	return attempt, nil
}

func (b *PostgresBackend) UpdateAgentRunAttemptStart(ctx context.Context, attemptId string, startedAt time.Time) error {
	runID, err := b.resolveRunIDForAttempt(ctx, attemptId)
	if err != nil {
		return err
	}
	query := `
		UPDATE agent_run
		SET status = $2::agent_run_status, started_at = $3, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	res, err := b.db.ExecContext(ctx, query, runID, types.AgentRunStatusRunning, startedAt)
	if err != nil {
		return fmt.Errorf("update attempt start: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return &types.ErrAgentRunAttemptNotFound{ID: attemptId}
	}
	return nil
}

func (b *PostgresBackend) UpdateAgentRunAttemptResult(ctx context.Context, attemptId string, status types.AgentAttemptStatus, exitCode *int, endedAt time.Time, errorMsg *string) error {
	runID, err := b.resolveRunIDForAttempt(ctx, attemptId)
	if err != nil {
		return err
	}
	runStatus := types.RunStatusFromAttemptStatus(status)
	query := `
		UPDATE agent_run
		SET status = $2::agent_run_status,
		    exit_code = $3,
		    ended_at = $4,
		    error = $5,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	res, err := b.db.ExecContext(ctx, query, runID, runStatus, exitCode, endedAt, errorMsg)
	if err != nil {
		return fmt.Errorf("update attempt result: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return &types.ErrAgentRunAttemptNotFound{ID: attemptId}
	}
	return nil
}

func (b *PostgresBackend) BindAttemptExecutionTask(ctx context.Context, attemptId, taskExternalID string) error {
	runID, err := b.resolveRunIDForAttempt(ctx, attemptId)
	if err != nil {
		return err
	}
	taskExternalID = strings.TrimSpace(taskExternalID)
	if taskExternalID != "" && taskExternalID != runID {
		return fmt.Errorf("bind attempt run execution: run %s does not match execution %s", runID, taskExternalID)
	}
	query := `
		UPDATE agent_run
		SET run_attempt_id = $2::uuid, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	res, err := b.db.ExecContext(ctx, query, runID, attemptId)
	if err != nil {
		return fmt.Errorf("bind attempt run execution: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return &types.ErrAgentRunAttemptNotFound{ID: attemptId}
	}
	return nil
}

func (b *PostgresBackend) resolveRunIDForAttempt(ctx context.Context, attemptId string) (string, error) {
	query := `
		SELECT id
		FROM agent_run
		WHERE run_attempt_id = $1::uuid
		   OR id = $1::uuid
		ORDER BY CASE WHEN run_attempt_id = $1::uuid THEN 0 ELSE 1 END
		LIMIT 1
	`
	var runID string
	if err := b.db.QueryRowContext(ctx, query, attemptId).Scan(&runID); err != nil {
		if err == sql.ErrNoRows {
			return "", &types.ErrAgentRunAttemptNotFound{ID: attemptId}
		}
		return "", fmt.Errorf("resolve run by attempt id: %w", err)
	}
	return runID, nil
}

func (b *PostgresBackend) AppendAgentRunSnapshot(ctx context.Context, snap *types.AgentRunSnapshot) error {
	payloadJSON, err := marshalJSONMap(snap.PayloadJSON)
	if err != nil {
		return fmt.Errorf("marshal snapshot payload: %w", err)
	}
	query := `
		INSERT INTO agent_run_snapshot (run_id, seq, status, started_at_ms, ended_at_ms, error, ts, payload_json)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id, created_at
	`
	if err := b.db.QueryRowContext(
		ctx,
		query,
		snap.RunID,
		snap.Seq,
		snap.Status,
		snap.StartedAtMs,
		snap.EndedAtMs,
		snap.Error,
		snap.TS,
		payloadJSON,
	).Scan(&snap.ID, &snap.CreatedAt); err != nil {
		return fmt.Errorf("append run snapshot: %w", err)
	}
	return nil
}

func (b *PostgresBackend) ListAgentRunSnapshots(ctx context.Context, runId string, limit int) ([]*types.AgentRunSnapshot, error) {
	if limit <= 0 {
		limit = 200
	}
	query := `
		SELECT id, run_id, seq, status, started_at_ms, ended_at_ms, error, ts, payload_json, created_at
		FROM agent_run_snapshot
		WHERE run_id = $1
		ORDER BY seq ASC
		LIMIT $2
	`
	rows, err := b.db.QueryContext(ctx, query, runId, limit)
	if err != nil {
		return nil, fmt.Errorf("list run snapshots: %w", err)
	}
	defer rows.Close()

	out := make([]*types.AgentRunSnapshot, 0)
	for rows.Next() {
		snap := &types.AgentRunSnapshot{}
		var payloadJSON []byte
		var startedAtMs sql.NullInt64
		var endedAtMs sql.NullInt64
		var errMsg sql.NullString
		if err := rows.Scan(
			&snap.ID,
			&snap.RunID,
			&snap.Seq,
			&snap.Status,
			&startedAtMs,
			&endedAtMs,
			&errMsg,
			&snap.TS,
			&payloadJSON,
			&snap.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan run snapshot: %w", err)
		}
		if startedAtMs.Valid {
			v := startedAtMs.Int64
			snap.StartedAtMs = &v
		}
		if endedAtMs.Valid {
			v := endedAtMs.Int64
			snap.EndedAtMs = &v
		}
		if errMsg.Valid {
			snap.Error = &errMsg.String
		}
		snap.PayloadJSON = unmarshalJSONMap(payloadJSON)
		out = append(out, snap)
	}
	return out, rows.Err()
}

func (b *PostgresBackend) GetOrCreateExecutionInstance(ctx context.Context, inst *types.AgentExecutionInstance) (*types.AgentExecutionInstance, error) {
	query := `
		INSERT INTO agent_execution_instance (
			instance_key, workspace_id, agent_id, lane, execution_class_key,
			pool_name, active, status, failed_attempt_threshold, desired_dispatch_concurrency,
			running_attempts, pending_attempts, stopping_attempts
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			$11, $12, $13
		)
		ON CONFLICT (instance_key) DO NOTHING
	`
	_, err := b.db.ExecContext(
		ctx,
		query,
		inst.InstanceKey,
		inst.WorkspaceID,
		inst.AgentID,
		inst.Lane,
		inst.ExecutionClassKey,
		inst.PoolName,
		inst.Active,
		inst.Status,
		inst.FailedAttemptThreshold,
		inst.DesiredDispatchConcurrency,
		inst.RunningAttempts,
		inst.PendingAttempts,
		inst.StoppingAttempts,
	)
	if err != nil {
		return nil, fmt.Errorf("upsert execution instance: %w", err)
	}
	return b.GetExecutionInstanceByKey(ctx, inst.InstanceKey)
}

func (b *PostgresBackend) GetExecutionInstanceByKey(ctx context.Context, instanceKey string) (*types.AgentExecutionInstance, error) {
	query := `
		SELECT id, instance_key, workspace_id, agent_id, lane, execution_class_key, pool_name, active,
		       status, failed_attempt_threshold, desired_dispatch_concurrency,
		       running_attempts, pending_attempts, stopping_attempts, last_event_at, created_at, updated_at
		FROM agent_execution_instance
		WHERE instance_key = $1
	`
	inst := &types.AgentExecutionInstance{}
	var agentID sql.NullString
	var lane sql.NullString
	var lastEventAt sql.NullTime
	if err := b.db.QueryRowContext(ctx, query, instanceKey).Scan(
		&inst.ID,
		&inst.InstanceKey,
		&inst.WorkspaceID,
		&agentID,
		&lane,
		&inst.ExecutionClassKey,
		&inst.PoolName,
		&inst.Active,
		&inst.Status,
		&inst.FailedAttemptThreshold,
		&inst.DesiredDispatchConcurrency,
		&inst.RunningAttempts,
		&inst.PendingAttempts,
		&inst.StoppingAttempts,
		&lastEventAt,
		&inst.CreatedAt,
		&inst.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("execution instance not found: %s", instanceKey)
		}
		return nil, fmt.Errorf("get execution instance: %w", err)
	}
	if agentID.Valid {
		inst.AgentID = &agentID.String
	}
	if lane.Valid {
		inst.Lane = &lane.String
	}
	if lastEventAt.Valid {
		inst.LastEventAt = &lastEventAt.Time
	}
	return inst, nil
}

func (b *PostgresBackend) UpdateExecutionInstanceState(
	ctx context.Context,
	instanceKey string,
	running, pending, stopping, desired int,
	status types.AgentExecutionInstanceStatus,
	lastEventAt *time.Time,
) error {
	query := `
		UPDATE agent_execution_instance
		SET running_attempts = $2,
		    pending_attempts = $3,
		    stopping_attempts = $4,
		    desired_dispatch_concurrency = $5,
		    status = $6,
		    last_event_at = COALESCE($7, last_event_at),
		    updated_at = CURRENT_TIMESTAMP
		WHERE instance_key = $1
	`
	res, err := b.db.ExecContext(ctx, query, instanceKey, running, pending, stopping, desired, status, lastEventAt)
	if err != nil {
		return fmt.Errorf("update execution instance state: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("execution instance not found: %s", instanceKey)
	}
	return nil
}

func (b *PostgresBackend) AdjustExecutionInstanceRunningAttempts(
	ctx context.Context,
	instanceKey string,
	runningDelta int,
	lastEventAt *time.Time,
) error {
	query := `
		UPDATE agent_execution_instance
		SET running_attempts = GREATEST(running_attempts + $2, 0),
		    last_event_at = COALESCE($3, last_event_at),
		    updated_at = CURRENT_TIMESTAMP
		WHERE instance_key = $1
	`
	res, err := b.db.ExecContext(ctx, query, instanceKey, runningDelta, lastEventAt)
	if err != nil {
		return fmt.Errorf("adjust execution instance running attempts: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("execution instance not found: %s", instanceKey)
	}
	return nil
}

const runExecutionSelectSQL = `
	SELECT
		id,
		workspace_id,
		created_by_member_id,
		status,
		type,
		prompt,
		image,
		entrypoint,
		env,
		exit_code,
		error,
		created_at,
		started_at,
		ended_at,
		hook_id,
		attempt,
		max_attempts,
		run_attempt_id,
		timeout_ms,
		exec_host,
		exec_security,
		exec_ask,
		runtime_type,
		workspace_access,
		network_enabled,
		execution_policy_json
	FROM agent_run
`

const runExecutionScopeWhere = "(run_attempt_id IS NOT NULL OR hook_id IS NOT NULL)"
const runExecutionActiveWhere = "status IN ('accepted'::agent_run_status, 'running'::agent_run_status)"

func (b *PostgresBackend) CreateRunExecution(ctx context.Context, task *types.RunExecution) error {
	envJSON, err := json.Marshal(task.Env)
	if err != nil {
		return fmt.Errorf("marshal run execution env: %w", err)
	}
	if task.ExecutionPolicy == nil {
		task.ExecutionPolicy = map[string]any{}
	}
	executionPolicyJSON, err := json.Marshal(task.ExecutionPolicy)
	if err != nil {
		return fmt.Errorf("marshal run execution policy: %w", err)
	}
	if task.Attempt == 0 {
		task.Attempt = 1
	}
	if task.MaxAttempts == 0 {
		task.MaxAttempts = 1
	}
	task.NormalizeType()
	if strings.TrimSpace(task.Image) == "" {
		return fmt.Errorf("run execution image is required")
	}

	var memberID any
	if task.CreatedByMemberId != nil {
		memberID = *task.CreatedByMemberId
	}

	status := agentRunStatusFromRunExecutionStatus(task.Status)
	timeoutMs := normalizeRunExecutionTimeout(task.TimeoutMs)
	execHost := runExecutionStringOrDefault(task.ExecHost, "sandbox")
	execSecurity := runExecutionStringOrDefault(task.ExecSecurity, "allowlist")
	execAsk := runExecutionStringOrDefault(task.ExecAsk, "off")
	runtimeType := runExecutionStringOrDefault(task.RuntimeType, "gvisor")
	workspaceAccess := runExecutionStringOrDefault(task.WorkspaceAccess, "rw")
	networkEnabled := runExecutionBoolOrDefault(task.NetworkEnabled, true)

	if task.RunAttemptID != nil && strings.TrimSpace(*task.RunAttemptID) != "" {
		attemptID := strings.TrimSpace(*task.RunAttemptID)
		var runID string
		err := b.db.QueryRowContext(
			ctx,
			`SELECT id FROM agent_run WHERE run_attempt_id = $1::uuid`,
			attemptID,
		).Scan(&runID)
		switch {
		case err == nil:
			query := `
				UPDATE agent_run
				SET created_by_member_id = $2,
				    status = $3::agent_run_status,
				    type = $4,
				    prompt = NULLIF($5, ''),
				    image = $6,
				    entrypoint = $7,
				    env = $8,
				    hook_id = $9,
				    attempt = $10,
				    max_attempts = $11,
				    run_attempt_id = $12::uuid,
				    timeout_ms = $13,
				    exec_host = $14,
				    exec_security = $15,
				    exec_ask = $16,
				    runtime_type = $17,
				    workspace_access = $18,
				    network_enabled = $19,
				    execution_policy_json = $20,
				    updated_at = CURRENT_TIMESTAMP
				WHERE id = $1
				RETURNING created_at, workspace_id
			`
			var createdAt time.Time
			var workspaceID uint
			if err := b.db.QueryRowContext(
				ctx,
				query,
				runID,
				memberID,
				status,
				task.Type,
				task.Prompt,
				task.Image,
				pq.Array(task.Entrypoint),
				envJSON,
				task.HookId,
				task.Attempt,
				task.MaxAttempts,
				attemptID,
				timeoutMs,
				execHost,
				execSecurity,
				execAsk,
				runtimeType,
				workspaceAccess,
				networkEnabled,
				executionPolicyJSON,
			).Scan(&createdAt, &workspaceID); err != nil {
				return fmt.Errorf("update run execution on run %s: %w", runID, err)
			}
			task.ExternalId = runID
			task.CreatedAt = createdAt
			if task.WorkspaceId == 0 {
				task.WorkspaceId = workspaceID
			}
			task.RunAttemptID = &attemptID
			return nil
		case err != sql.ErrNoRows:
			return fmt.Errorf("resolve run by attempt %s: %w", attemptID, err)
		}
	}

	if task.RunAttemptID != nil && strings.TrimSpace(*task.RunAttemptID) != "" {
		runID := strings.TrimSpace(*task.RunAttemptID)
		query := `
			INSERT INTO agent_run (
				id,
				workspace_id,
				origin_task_id,
				status,
				session_id,
				exec_host,
				exec_security,
				exec_ask,
				runtime_type,
				workspace_access,
				network_enabled,
				interactive,
				timeout_ms,
				created_by_member_id,
				type,
				prompt,
				image,
				entrypoint,
				env,
				hook_id,
				attempt,
				max_attempts,
				run_attempt_id,
				exit_code,
				error,
				execution_policy_json
			) VALUES (
				$1::uuid,
				$2,
				$1::uuid,
				$3::agent_run_status,
				$1::text,
				$4,
				$5,
				$6,
				$7,
				$8,
				$9,
				$10,
				$11,
				$12,
				$13,
				NULLIF($14, ''),
				$15,
				$16,
				$17,
				$18,
				$19,
				$20,
				$21::uuid,
				$22,
				NULLIF($23, ''),
				$24
			)
			RETURNING created_at
		`
		if err := b.db.QueryRowContext(
			ctx,
			query,
			runID,
			task.WorkspaceId,
			status,
			execHost,
			execSecurity,
			execAsk,
			runtimeType,
			workspaceAccess,
			networkEnabled,
			task.IsInteractive(),
			timeoutMs,
			memberID,
			task.Type,
			task.Prompt,
			task.Image,
			pq.Array(task.Entrypoint),
			envJSON,
			task.HookId,
			task.Attempt,
			task.MaxAttempts,
			runID,
			task.ExitCode,
			task.Error,
			executionPolicyJSON,
		).Scan(&task.CreatedAt); err != nil {
			return fmt.Errorf("insert run execution with fixed id: %w", err)
		}
		task.ExternalId = runID
		return nil
	}

	query := `
		WITH generated AS (SELECT uuid_generate_v4() AS id)
		INSERT INTO agent_run (
			id,
			workspace_id,
			origin_task_id,
			status,
			session_id,
			exec_host,
			exec_security,
			exec_ask,
			runtime_type,
			workspace_access,
			network_enabled,
			interactive,
			timeout_ms,
			created_by_member_id,
			type,
			prompt,
			image,
			entrypoint,
			env,
			hook_id,
			attempt,
			max_attempts,
			run_attempt_id,
			exit_code,
			error,
			execution_policy_json
		)
		SELECT
			generated.id,
			$1,
			generated.id,
			$2::agent_run_status,
			generated.id::text,
			$3,
			$4,
			$5,
			$6,
			$7,
			$8,
			$9,
			$10,
			$11,
			$12,
			NULLIF($13, ''),
			$14,
			$15,
			$16,
			$17,
			$18,
			$19,
			generated.id,
			$20,
			NULLIF($21, ''),
			$22
		FROM generated
		RETURNING id, created_at
	`
	if err := b.db.QueryRowContext(
		ctx,
		query,
		task.WorkspaceId,
		status,
		execHost,
		execSecurity,
		execAsk,
		runtimeType,
		workspaceAccess,
		networkEnabled,
		task.IsInteractive(),
		timeoutMs,
		memberID,
		task.Type,
		task.Prompt,
		task.Image,
		pq.Array(task.Entrypoint),
		envJSON,
		task.HookId,
		task.Attempt,
		task.MaxAttempts,
		task.ExitCode,
		task.Error,
		executionPolicyJSON,
	).Scan(&task.ExternalId, &task.CreatedAt); err != nil {
		return fmt.Errorf("insert run execution: %w", err)
	}
	return nil
}

func (b *PostgresBackend) GetRunExecution(ctx context.Context, externalId string) (*types.RunExecution, error) {
	query := runExecutionSelectSQL + `
		WHERE id = $1::uuid
		  AND ` + runExecutionScopeWhere
	return b.scanRunExecution(b.db.QueryRowContext(ctx, query, externalId))
}

func (b *PostgresBackend) GetRunExecutionByID(_ context.Context, id uint) (*types.RunExecution, error) {
	return nil, &types.ErrRunExecutionNotFound{ExternalId: fmt.Sprintf("%d", id)}
}

func (b *PostgresBackend) ListRunExecutions(ctx context.Context, workspaceId uint) ([]*types.RunExecution, error) {
	if workspaceId == 0 {
		return nil, fmt.Errorf("workspace id is required")
	}

	query := runExecutionSelectSQL + `
		WHERE workspace_id = $1
		  AND ` + runExecutionScopeWhere + `
		ORDER BY created_at DESC
		LIMIT 100
	`
	return b.scanRunExecutionRows(ctx, query, workspaceId)
}

func (b *PostgresBackend) UpdateRunExecutionStatus(ctx context.Context, externalId string, status types.RunExecutionStatus) error {
	query := `
		UPDATE agent_run
		SET status = $2::agent_run_status,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1::uuid
		  AND ` + runExecutionScopeWhere
	result, err := b.db.ExecContext(ctx, query, externalId, agentRunStatusFromRunExecutionStatus(status))
	if err != nil {
		return fmt.Errorf("update run execution status: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		return &types.ErrRunExecutionNotFound{ExternalId: externalId}
	}
	return nil
}

func (b *PostgresBackend) SetRunExecutionStarted(ctx context.Context, externalId string) error {
	now := time.Now()
	query := `
		UPDATE agent_run
		SET status = 'running'::agent_run_status,
		    started_at = COALESCE(started_at, $2),
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1::uuid
		  AND ` + runExecutionScopeWhere + `
		  AND status = 'accepted'::agent_run_status`
	result, err := b.db.ExecContext(ctx, query, externalId, now)
	if err != nil {
		return fmt.Errorf("set run execution started: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		run, lookupErr := b.GetRunExecution(ctx, externalId)
		if lookupErr != nil {
			return lookupErr
		}
		if run.IsTerminal() {
			return fmt.Errorf("run execution cannot be started (already finished)")
		}
		if run.Status == types.RunExecutionStatusRunning {
			return fmt.Errorf("run execution cannot be started (already running)")
		}
		return fmt.Errorf("run execution cannot be started (state=%s)", run.Status)
	}
	return nil
}

func (b *PostgresBackend) SetRunExecutionResult(ctx context.Context, externalId string, exitCode int, errorMsg string) error {
	_, status, _ := types.ClassifyExecutionOutcome(exitCode, errorMsg)
	endedAt := time.Now()
	query := `
		UPDATE agent_run
		SET status = $2::agent_run_status,
		    exit_code = $3,
		    error = NULLIF($4, ''),
		    ended_at = $5,
		    claimed_by_worker_id = NULL,
		    claim_heartbeat_at = NULL,
		    claim_expires_at = NULL,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1::uuid
		  AND ` + runExecutionActiveWhere
	result, err := b.db.ExecContext(ctx, query, externalId, status, exitCode, errorMsg, endedAt)
	if err != nil {
		return fmt.Errorf("set run execution result: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		run, lookupErr := b.GetAgentRunByID(ctx, externalId)
		if lookupErr != nil {
			if _, ok := lookupErr.(*types.ErrAgentRunNotFound); ok {
				// Late retries after retention/cleanup should be harmless.
				return nil
			}
			return lookupErr
		}
		// Late duplicate callbacks are expected during crashes/retries.
		if run.Status.IsTerminal() {
			return nil
		}
		return fmt.Errorf("run execution result was not applied (status=%s)", run.Status)
	}
	return nil
}

func (b *PostgresBackend) MarkRunExecutionRetried(ctx context.Context, externalId string) error {
	result, err := b.db.ExecContext(
		ctx,
		`UPDATE agent_run
		 SET attempt = max_attempts,
		     updated_at = CURRENT_TIMESTAMP
		 WHERE id = $1::uuid
		   AND `+runExecutionScopeWhere,
		externalId,
	)
	if err != nil {
		return fmt.Errorf("mark run execution retried: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		return &types.ErrRunExecutionNotFound{ExternalId: externalId}
	}
	return nil
}

func (b *PostgresBackend) DeleteRunExecution(ctx context.Context, externalId string) error {
	result, err := b.db.ExecContext(
		ctx,
		`DELETE FROM agent_run
		 WHERE id = $1::uuid
		   AND `+runExecutionScopeWhere,
		externalId,
	)
	if err != nil {
		return fmt.Errorf("delete run execution: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		return &types.ErrRunExecutionNotFound{ExternalId: externalId}
	}
	return nil
}

func (b *PostgresBackend) CancelRunExecution(ctx context.Context, externalId string) error {
	query := `
		UPDATE agent_run
		SET status = 'cancelled'::agent_run_status,
		    ended_at = $2,
		    claimed_by_worker_id = NULL,
		    claim_heartbeat_at = NULL,
		    claim_expires_at = NULL,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1::uuid
		  AND ` + runExecutionScopeWhere + `
		  AND status IN ('accepted'::agent_run_status, 'running'::agent_run_status)
	`
	result, err := b.db.ExecContext(ctx, query, externalId, time.Now())
	if err != nil {
		return fmt.Errorf("cancel run execution: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		if _, lookupErr := b.GetRunExecution(ctx, externalId); lookupErr != nil {
			return lookupErr
		}
		return fmt.Errorf("run execution cannot be cancelled (already finished)")
	}
	return nil
}

func (b *PostgresBackend) GetRetryableRunExecutions(ctx context.Context) ([]*types.RunExecution, error) {
	query := runExecutionSelectSQL + `
		WHERE hook_id IS NOT NULL
		  AND status = 'error'::agent_run_status
		  AND attempt < max_attempts
		  AND ` + runExecutionScopeWhere + `
		ORDER BY ended_at ASC NULLS LAST
		LIMIT 50
	`
	return b.scanRunExecutionRows(ctx, query)
}

func (b *PostgresBackend) GetStuckHookRunExecutions(ctx context.Context, timeout time.Duration) ([]*types.RunExecution, error) {
	cutoff := time.Now().Add(-timeout)
	query := runExecutionSelectSQL + `
		WHERE hook_id IS NOT NULL
		  AND status IN ('accepted'::agent_run_status, 'running'::agent_run_status)
		  AND created_at < $1
		  AND ` + runExecutionScopeWhere + `
		LIMIT 50
	`
	return b.scanRunExecutionRows(ctx, query, cutoff)
}

func (b *PostgresBackend) ListRunExecutionsByHook(ctx context.Context, hookId uint) ([]*types.RunExecution, error) {
	query := runExecutionSelectSQL + `
		WHERE hook_id = $1
		  AND ` + runExecutionScopeWhere + `
		ORDER BY created_at DESC
		LIMIT 50
	`
	return b.scanRunExecutionRows(ctx, query, hookId)
}

func (b *PostgresBackend) scanRunExecution(row scanner) (*types.RunExecution, error) {
	run := &types.RunExecution{}
	var runID string
	var createdByMemberID sql.NullInt64
	var runStatus types.AgentRunStatus
	var runType sql.NullString
	var prompt sql.NullString
	var entrypoint pq.StringArray
	var envJSON []byte
	var exitCode sql.NullInt32
	var errorMsg sql.NullString
	var startedAt sql.NullTime
	var endedAt sql.NullTime
	var hookID sql.NullInt64
	var runAttemptID sql.NullString
	var timeoutMs sql.NullInt32
	var execHost sql.NullString
	var execSecurity sql.NullString
	var execAsk sql.NullString
	var runtimeType sql.NullString
	var workspaceAccess sql.NullString
	var networkEnabled sql.NullBool
	var executionPolicyJSON []byte

	err := row.Scan(
		&runID,
		&run.WorkspaceId,
		&createdByMemberID,
		&runStatus,
		&runType,
		&prompt,
		&run.Image,
		&entrypoint,
		&envJSON,
		&exitCode,
		&errorMsg,
		&run.CreatedAt,
		&startedAt,
		&endedAt,
		&hookID,
		&run.Attempt,
		&run.MaxAttempts,
		&runAttemptID,
		&timeoutMs,
		&execHost,
		&execSecurity,
		&execAsk,
		&runtimeType,
		&workspaceAccess,
		&networkEnabled,
		&executionPolicyJSON,
	)
	if err == sql.ErrNoRows {
		return nil, &types.ErrRunExecutionNotFound{ExternalId: runID}
	}
	if err != nil {
		return nil, fmt.Errorf("scan run execution: %w", err)
	}

	run.ExternalId = runID
	run.Status = runExecutionStatusFromAgentRunStatus(runStatus)

	if createdByMemberID.Valid {
		memberID := uint(createdByMemberID.Int64)
		run.CreatedByMemberId = &memberID
	}
	if runType.Valid {
		run.Type = types.RunExecutionType(runType.String)
	}
	run.NormalizeType()
	if prompt.Valid {
		run.Prompt = prompt.String
	}
	if hookID.Valid {
		v := uint(hookID.Int64)
		run.HookId = &v
	}
	run.Entrypoint = []string(entrypoint)
	if len(envJSON) > 0 {
		_ = json.Unmarshal(envJSON, &run.Env)
	}
	if run.Env == nil {
		run.Env = map[string]string{}
	}
	if exitCode.Valid {
		v := int(exitCode.Int32)
		run.ExitCode = &v
	}
	if errorMsg.Valid {
		run.Error = errorMsg.String
	}
	if startedAt.Valid {
		run.StartedAt = &startedAt.Time
	}
	if endedAt.Valid {
		run.FinishedAt = &endedAt.Time
	}
	if runAttemptID.Valid {
		run.RunAttemptID = &runAttemptID.String
	}
	if timeoutMs.Valid {
		v := int(timeoutMs.Int32)
		run.TimeoutMs = &v
	}
	if execHost.Valid {
		run.ExecHost = &execHost.String
	}
	if execSecurity.Valid {
		run.ExecSecurity = &execSecurity.String
	}
	if execAsk.Valid {
		run.ExecAsk = &execAsk.String
	}
	if runtimeType.Valid {
		run.RuntimeType = &runtimeType.String
	}
	if workspaceAccess.Valid {
		run.WorkspaceAccess = &workspaceAccess.String
	}
	if networkEnabled.Valid {
		v := networkEnabled.Bool
		run.NetworkEnabled = &v
	}
	if len(executionPolicyJSON) > 0 {
		_ = json.Unmarshal(executionPolicyJSON, &run.ExecutionPolicy)
	}
	if run.ExecutionPolicy == nil {
		run.ExecutionPolicy = map[string]any{}
	}
	return run, nil
}

func (b *PostgresBackend) scanRunExecutionRows(ctx context.Context, query string, args ...any) ([]*types.RunExecution, error) {
	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query run executions: %w", err)
	}
	defer rows.Close()

	out := make([]*types.RunExecution, 0)
	for rows.Next() {
		run, err := b.scanRunExecution(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func agentRunStatusFromRunExecutionStatus(status types.RunExecutionStatus) types.AgentRunStatus {
	switch status {
	case types.RunExecutionStatusRunning:
		return types.AgentRunStatusRunning
	case types.RunExecutionStatusComplete:
		return types.AgentRunStatusOK
	case types.RunExecutionStatusCancelled:
		return types.AgentRunStatusCancelled
	case types.RunExecutionStatusFailed:
		return types.AgentRunStatusError
	case types.RunExecutionStatusPending, types.RunExecutionStatusScheduled:
		fallthrough
	default:
		return types.AgentRunStatusAccepted
	}
}

func runExecutionStatusFromAgentRunStatus(status types.AgentRunStatus) types.RunExecutionStatus {
	switch status {
	case types.AgentRunStatusAccepted:
		return types.RunExecutionStatusPending
	case types.AgentRunStatusRunning:
		return types.RunExecutionStatusRunning
	case types.AgentRunStatusOK:
		return types.RunExecutionStatusComplete
	case types.AgentRunStatusCancelled:
		return types.RunExecutionStatusCancelled
	case types.AgentRunStatusError, types.AgentRunStatusTimeout:
		return types.RunExecutionStatusFailed
	default:
		return types.RunExecutionStatusFailed
	}
}

func normalizeRunExecutionTimeout(timeoutMs *int) int {
	if timeoutMs == nil {
		return 0
	}
	return *timeoutMs
}

func runExecutionStringOrDefault(value *string, fallback string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return fallback
	}
	return strings.TrimSpace(*value)
}

func runExecutionBoolOrDefault(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}
