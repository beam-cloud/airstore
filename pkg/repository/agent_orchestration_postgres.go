package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

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

func (b *PostgresBackend) CreateTask(ctx context.Context, task *types.AgentTask) error {
	payloadJSON, err := marshalJSONMap(task.PayloadJSON)
	if err != nil {
		return fmt.Errorf("marshal envelope payload: %w", err)
	}
	routingJSON, err := marshalJSONMap(task.RoutingJSON)
	if err != nil {
		return fmt.Errorf("marshal envelope routing: %w", err)
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
		task.Kind,
		task.QueueMode,
		task.State,
		task.IdempotencyKey,
		payloadJSON,
		routingJSON,
		task.ParentTaskID,
		task.TargetRunID,
	).Scan(&task.ID, &task.AcceptedAt, &task.CreatedAt, &task.UpdatedAt); err != nil {
		return fmt.Errorf("create agent task envelope: %w", err)
	}
	return nil
}

func (b *PostgresBackend) scanAgentTask(row scanner) (*types.AgentTask, error) {
	task := &types.AgentTask{}
	var payloadJSON []byte
	var routingJSON []byte
	var agentID sql.NullString
	var parentID sql.NullString
	var targetRunID sql.NullString
	var queuedAt sql.NullTime
	var dispatchedAt sql.NullTime
	var droppedReason sql.NullString
	err := row.Scan(
		&task.ID,
		&task.WorkspaceID,
		&agentID,
		&task.Kind,
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
	task.PayloadJSON = unmarshalJSONMap(payloadJSON)
	task.RoutingJSON = unmarshalJSONMap(routingJSON)
	return task, nil
}

func (b *PostgresBackend) GetTask(ctx context.Context, workspaceId uint, taskID string) (*types.AgentTask, error) {
	query := `
		SELECT id, workspace_id, agent_id, kind, queue_mode, state, idempotency_key, payload_json, routing_json,
		       parent_envelope_id, target_run_id, accepted_at, queued_at, dispatched_at, dropped_reason, created_at, updated_at
		FROM agent_task
		WHERE workspace_id = $1 AND id = $2
	`
	task, err := b.scanAgentTask(b.db.QueryRowContext(ctx, query, workspaceId, taskID))
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return nil, &types.ErrAgentTaskNotFound{ID: taskID}
		}
		return nil, fmt.Errorf("get agent task envelope: %w", err)
	}
	return task, nil
}

func (b *PostgresBackend) ListTasks(ctx context.Context, workspaceId uint, limit int) ([]*types.AgentTask, error) {
	if limit <= 0 {
		limit = 100
	}

	query := `
		SELECT id, workspace_id, agent_id, kind, queue_mode, state, idempotency_key, payload_json, routing_json,
		       parent_envelope_id, target_run_id, accepted_at, queued_at, dispatched_at, dropped_reason, created_at, updated_at
		FROM agent_task
		WHERE workspace_id = $1
		ORDER BY created_at DESC
		LIMIT $2
	`

	rows, err := b.db.QueryContext(ctx, query, workspaceId, limit)
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

func (b *PostgresBackend) GetTaskByID(ctx context.Context, taskID string) (*types.AgentTask, error) {
	query := `
		SELECT id, workspace_id, agent_id, kind, queue_mode, state, idempotency_key, payload_json, routing_json,
		       parent_envelope_id, target_run_id, accepted_at, queued_at, dispatched_at, dropped_reason, created_at, updated_at
		FROM agent_task
		WHERE id = $1
	`
	task, err := b.scanAgentTask(b.db.QueryRowContext(ctx, query, taskID))
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return nil, &types.ErrAgentTaskNotFound{ID: taskID}
		}
		return nil, fmt.Errorf("get envelope by id: %w", err)
	}
	return task, nil
}

func (b *PostgresBackend) GetTaskByIdempotency(ctx context.Context, workspaceId uint, agentId *string, idempotencyKey string) (*types.AgentTask, error) {
	query := `
		SELECT id, workspace_id, agent_id, kind, queue_mode, state, idempotency_key, payload_json, routing_json,
		       parent_envelope_id, target_run_id, accepted_at, queued_at, dispatched_at, dropped_reason, created_at, updated_at
		FROM agent_task
		WHERE workspace_id = $1
		  AND idempotency_key = $2
		  AND (($3::uuid IS NULL AND agent_id IS NULL) OR agent_id = $3::uuid)
		ORDER BY created_at DESC
		LIMIT 1
	`
	var agentArg any
	if agentId != nil {
		agentArg = *agentId
	}
	task, err := b.scanAgentTask(b.db.QueryRowContext(ctx, query, workspaceId, idempotencyKey, agentArg))
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return nil, &types.ErrAgentTaskNotFound{ID: idempotencyKey}
		}
		return nil, fmt.Errorf("get envelope by idempotency: %w", err)
	}
	return task, nil
}

func (b *PostgresBackend) UpdateTaskState(ctx context.Context, taskID string, state types.AgentTaskState, droppedReason *string, targetRunID *string) error {
	now := time.Now()
	query := `
		UPDATE agent_task
		SET state = $2::agent_task_state,
		    queued_at = CASE WHEN $2::agent_task_state = 'queued'::agent_task_state THEN $3 ELSE queued_at END,
		    dispatched_at = CASE WHEN $2::agent_task_state = 'dispatched'::agent_task_state THEN $3 ELSE dispatched_at END,
		    dropped_reason = CASE WHEN $2::agent_task_state = 'dropped'::agent_task_state THEN $4 ELSE dropped_reason END,
		    target_run_id = COALESCE($5::uuid, target_run_id),
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	res, err := b.db.ExecContext(ctx, query, taskID, state, now, droppedReason, targetRunID)
	if err != nil {
		return fmt.Errorf("update envelope state: %w", err)
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
			workspace_id, agent_id, origin_envelope_id, status, session_id, session_key,
			provider, model, exec_host, exec_security, exec_ask, runtime_type, workspace_access,
			network_enabled, interactive, timeout_ms, usage_json, delivery_json
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11, $12, $13,
			$14, $15, $16, $17, $18
		)
		RETURNING id, snapshot_ts, created_at, updated_at
	`
	if err := b.db.QueryRowContext(
		ctx,
		query,
		run.WorkspaceID,
		run.AgentID,
		run.OriginTaskID,
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
	var sessionKey sql.NullString
	var provider sql.NullString
	var model sql.NullString
	var startedAt sql.NullTime
	var endedAt sql.NullTime
	var errMsg sql.NullString
	err := row.Scan(
		&run.ID,
		&run.WorkspaceID,
		&agentID,
		&run.OriginTaskID,
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
	if errMsg.Valid {
		run.Error = &errMsg.String
	}
	run.UsageJSON = unmarshalJSONMap(usageJSON)
	run.DeliveryJSON = unmarshalJSONMap(deliveryJSON)
	return run, nil
}

func (b *PostgresBackend) GetAgentRun(ctx context.Context, workspaceId uint, runId string) (*types.AgentRun, error) {
	query := `
		SELECT id, workspace_id, agent_id, origin_envelope_id, status, session_id, session_key, provider, model,
		       exec_host, exec_security, exec_ask, runtime_type, workspace_access, network_enabled, interactive,
		       timeout_ms, started_at, ended_at, error, snapshot_ts, usage_json, delivery_json, created_at, updated_at
		FROM agent_run
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
	query := `
		SELECT id, workspace_id, agent_id, origin_envelope_id, status, session_id, session_key, provider, model,
		       exec_host, exec_security, exec_ask, runtime_type, workspace_access, network_enabled, interactive,
		       timeout_ms, started_at, ended_at, error, snapshot_ts, usage_json, delivery_json, created_at, updated_at
		FROM agent_run
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
	query := `
		SELECT id, workspace_id, agent_id, origin_envelope_id, status, session_id, session_key, provider, model,
		       exec_host, exec_security, exec_ask, runtime_type, workspace_access, network_enabled, interactive,
		       timeout_ms, started_at, ended_at, error, snapshot_ts, usage_json, delivery_json, created_at, updated_at
		FROM agent_run
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

func (b *PostgresBackend) UpdateAgentRunLifecycle(ctx context.Context, runId string, status types.AgentRunStatus, startedAt, endedAt *time.Time, errorMsg *string) error {
	query := `
		UPDATE agent_run
		SET status = $2,
		    started_at = COALESCE($3, started_at),
		    ended_at = COALESCE($4, ended_at),
		    error = $5,
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
	query := `
		INSERT INTO agent_run_attempt (
			run_id, attempt_no, status, strategy, provider, model,
			exec_host, exec_security, exec_ask, runtime_type, workspace_access,
			network_enabled, interactive, execution_task_external_id
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11,
			$12, $13, $14
		)
		RETURNING id, created_at, updated_at
	`
	if err := b.db.QueryRowContext(
		ctx,
		query,
		attempt.RunID,
		attempt.AttemptNo,
		attempt.Status,
		attempt.Strategy,
		attempt.Provider,
		attempt.Model,
		attempt.ExecHost,
		attempt.ExecSecurity,
		attempt.ExecAsk,
		attempt.RuntimeType,
		attempt.WorkspaceAccess,
		attempt.NetworkEnabled,
		attempt.Interactive,
		attempt.ExecutionID,
	).Scan(&attempt.ID, &attempt.CreatedAt, &attempt.UpdatedAt); err != nil {
		return fmt.Errorf("create run attempt: %w", err)
	}
	return nil
}

func (b *PostgresBackend) scanAgentRunAttempt(row scanner) (*types.AgentRunAttempt, error) {
	attempt := &types.AgentRunAttempt{}
	var provider sql.NullString
	var model sql.NullString
	var executionTaskExternalID sql.NullString
	var startedAt sql.NullTime
	var endedAt sql.NullTime
	var exitCode sql.NullInt32
	var errMsg sql.NullString
	err := row.Scan(
		&attempt.ID,
		&attempt.RunID,
		&attempt.AttemptNo,
		&attempt.Status,
		&attempt.Strategy,
		&provider,
		&model,
		&attempt.ExecHost,
		&attempt.ExecSecurity,
		&attempt.ExecAsk,
		&attempt.RuntimeType,
		&attempt.WorkspaceAccess,
		&attempt.NetworkEnabled,
		&attempt.Interactive,
		&executionTaskExternalID,
		&startedAt,
		&endedAt,
		&exitCode,
		&errMsg,
		&attempt.CreatedAt,
		&attempt.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, &types.ErrAgentRunAttemptNotFound{}
	}
	if err != nil {
		return nil, err
	}
	if provider.Valid {
		attempt.Provider = &provider.String
	}
	if model.Valid {
		attempt.Model = &model.String
	}
	if executionTaskExternalID.Valid {
		attempt.ExecutionID = &executionTaskExternalID.String
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
	return attempt, nil
}

func (b *PostgresBackend) GetAgentRunAttempt(ctx context.Context, attemptId string) (*types.AgentRunAttempt, error) {
	query := `
		SELECT id, run_id, attempt_no, status, strategy, provider, model,
		       exec_host, exec_security, exec_ask, runtime_type, workspace_access,
		       network_enabled, interactive, execution_task_external_id,
		       started_at, ended_at, exit_code, error, created_at, updated_at
		FROM agent_run_attempt
		WHERE id = $1
	`
	attempt, err := b.scanAgentRunAttempt(b.db.QueryRowContext(ctx, query, attemptId))
	if err != nil {
		if _, ok := err.(*types.ErrAgentRunAttemptNotFound); ok {
			return nil, &types.ErrAgentRunAttemptNotFound{ID: attemptId}
		}
		return nil, fmt.Errorf("get run attempt: %w", err)
	}
	return attempt, nil
}

func (b *PostgresBackend) ListAgentRunAttempts(ctx context.Context, runId string) ([]*types.AgentRunAttempt, error) {
	query := `
		SELECT id, run_id, attempt_no, status, strategy, provider, model,
		       exec_host, exec_security, exec_ask, runtime_type, workspace_access,
		       network_enabled, interactive, execution_task_external_id,
		       started_at, ended_at, exit_code, error, created_at, updated_at
		FROM agent_run_attempt
		WHERE run_id = $1
		ORDER BY attempt_no ASC
	`
	rows, err := b.db.QueryContext(ctx, query, runId)
	if err != nil {
		return nil, fmt.Errorf("list run attempts: %w", err)
	}
	defer rows.Close()
	out := make([]*types.AgentRunAttempt, 0)
	for rows.Next() {
		attempt, err := b.scanAgentRunAttempt(rows)
		if err != nil {
			return nil, fmt.Errorf("scan run attempt: %w", err)
		}
		out = append(out, attempt)
	}
	return out, rows.Err()
}

func (b *PostgresBackend) GetRunAttemptByExecutionID(ctx context.Context, executionID string) (*types.AgentRunAttempt, error) {
	query := `
		SELECT id, run_id, attempt_no, status, strategy, provider, model,
		       exec_host, exec_security, exec_ask, runtime_type, workspace_access,
		       network_enabled, interactive, execution_task_external_id,
		       started_at, ended_at, exit_code, error, created_at, updated_at
		FROM agent_run_attempt
		WHERE execution_task_external_id = $1
	`
	attempt, err := b.scanAgentRunAttempt(b.db.QueryRowContext(ctx, query, executionID))
	if err != nil {
		if _, ok := err.(*types.ErrAgentRunAttemptNotFound); ok {
			return nil, &types.ErrAgentRunAttemptNotFound{ID: executionID}
		}
		return nil, fmt.Errorf("get attempt by execution id: %w", err)
	}
	return attempt, nil
}

func (b *PostgresBackend) UpdateAgentRunAttemptStart(ctx context.Context, attemptId string, startedAt time.Time) error {
	query := `
		UPDATE agent_run_attempt
		SET status = $2, started_at = $3, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	res, err := b.db.ExecContext(ctx, query, attemptId, types.AgentAttemptStatusRunning, startedAt)
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
	query := `
		UPDATE agent_run_attempt
		SET status = $2, exit_code = $3, ended_at = $4, error = $5, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	res, err := b.db.ExecContext(ctx, query, attemptId, status, exitCode, endedAt, errorMsg)
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
	query := `
		UPDATE agent_run_attempt
		SET execution_task_external_id = $2, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	res, err := b.db.ExecContext(ctx, query, attemptId, taskExternalID)
	if err != nil {
		return fmt.Errorf("bind attempt run execution: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return &types.ErrAgentRunAttemptNotFound{ID: attemptId}
	}
	return nil
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
