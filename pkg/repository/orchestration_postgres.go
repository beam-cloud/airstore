package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

const agentTaskSelect = `
	SELECT id, workspace_id, agent_id, agent_name, queue_mode, state,
	       idempotency_key, payload_json, routing_json, parent_envelope_id, target_run_id, current_blocker_id,
	       accepted_at, queued_at, dispatched_at, deadline, dropped_reason, priority, budget_usd, cost_usd, archived_at,
	       created_at, updated_at, wake_at, wake_reason, wake_count, input_kind, waiting_summary
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
			t.current_blocker_id,
			t.accepted_at,
			t.queued_at,
			t.dispatched_at,
			t.deadline,
			t.dropped_reason,
			t.priority,
			t.budget_usd,
			t.cost_usd,
			t.archived_at,
			t.created_at,
			t.updated_at,
			t.wake_at,
			t.wake_reason,
			t.wake_count,
			t.input_kind,
			t.waiting_summary
		FROM agent_task t
		LEFT JOIN agent_profile ap ON ap.id = t.agent_id
	) task_view
`

const agentRunSelect = `
	SELECT id, workspace_id, agent_id, created_by_member_id, origin_task_id, hook_id, status, session_id, session_key, provider, model,
	       exec_host, exec_security, exec_ask, runtime_type, workspace_access, network_enabled, interactive,
	       timeout_ms, started_at, ended_at, claimed_by_worker_id, claim_heartbeat_at, claim_expires_at,
	       error, snapshot_ts, cost_usd, usage_json, delivery_json, created_at, updated_at
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

func usageCostUSD(usage map[string]any) float64 {
	if len(usage) == 0 {
		return 0
	}
	for _, key := range []string{"cost_usd", "total_cost_usd", "usd_cost", "cost"} {
		if value, ok := usage[key]; ok {
			switch typed := value.(type) {
			case float64:
				return typed
			case float32:
				return float64(typed)
			case int:
				return float64(typed)
			case int32:
				return float64(typed)
			case int64:
				return float64(typed)
			case json.Number:
				if parsed, err := typed.Float64(); err == nil {
					return parsed
				}
			case string:
				if parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64); err == nil {
					return parsed
				}
			case map[string]any:
				if nested := usageCostUSD(typed); nested > 0 {
					return nested
				}
			}
		}
	}
	for _, key := range []string{"usage", "totals", "billing"} {
		if nested, ok := usage[key].(map[string]any); ok {
			if value := usageCostUSD(nested); value > 0 {
				return value
			}
		}
	}
	return 0
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
		INSERT INTO agent_profile (
			workspace_id, agent_key, name, role, memory_scope, quality_score, cost_budget_usd, config_json, active
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id, created_at, updated_at
	`
	if strings.TrimSpace(profile.Role) == "" {
		profile.Role = "generalist"
	}
	if strings.TrimSpace(profile.MemoryScope) == "" {
		profile.MemoryScope = "workspace"
	}
	if err := b.db.QueryRowContext(
		ctx,
		query,
		profile.WorkspaceID,
		profile.AgentKey,
		profile.Name,
		profile.Role,
		profile.MemoryScope,
		profile.QualityScore,
		profile.CostBudgetUSD,
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
	var qualityScore sql.NullFloat64
	var costBudgetUSD sql.NullFloat64
	err := row.Scan(
		&profile.ID,
		&profile.WorkspaceID,
		&profile.AgentKey,
		&profile.Name,
		&profile.Role,
		&profile.MemoryScope,
		&qualityScore,
		&costBudgetUSD,
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
	if qualityScore.Valid {
		profile.QualityScore = &qualityScore.Float64
	}
	if costBudgetUSD.Valid {
		profile.CostBudgetUSD = &costBudgetUSD.Float64
	}
	profile.ConfigJSON = unmarshalJSONMap(configJSON)
	return profile, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func (b *PostgresBackend) GetAgentProfile(ctx context.Context, workspaceId uint, agentId string) (*types.AgentProfile, error) {
	if _, err := uuid.Parse(agentId); err != nil {
		profile, keyErr := b.GetAgentProfileByKey(ctx, workspaceId, agentId)
		if keyErr == nil {
			return profile, nil
		}
		profile, fuzzyErr := b.getAgentProfileFuzzy(ctx, workspaceId, agentId)
		if fuzzyErr == nil {
			return profile, nil
		}
		return nil, &types.ErrAgentProfileNotFound{ID: agentId}
	}
	query := `
		SELECT id, workspace_id, agent_key, name, role, memory_scope, quality_score, cost_budget_usd, config_json, active, created_at, updated_at
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

func (b *PostgresBackend) getAgentProfileFuzzy(ctx context.Context, workspaceId uint, ref string) (*types.AgentProfile, error) {
	normalized := strings.TrimSuffix(strings.TrimSuffix(strings.ToLower(ref), "-agent"), "_agent")
	asName := strings.ReplaceAll(normalized, "-", " ")
	asName = strings.ReplaceAll(asName, "_", " ")
	query := `
		SELECT id, workspace_id, agent_key, name, role, memory_scope, quality_score, cost_budget_usd, config_json, active, created_at, updated_at
		FROM agent_profile
		WHERE workspace_id = $1 AND active = true
		  AND (LOWER(name) = LOWER($2) OR LOWER(agent_key) = LOWER($3)
		       OR LOWER(name) = LOWER($3) OR LOWER(agent_key) = LOWER($2))
		LIMIT 1
	`
	return b.scanAgentProfile(b.db.QueryRowContext(ctx, query, workspaceId, asName, normalized))
}

func (b *PostgresBackend) GetAgentProfileByKey(ctx context.Context, workspaceId uint, agentKey string) (*types.AgentProfile, error) {
	query := `
		SELECT id, workspace_id, agent_key, name, role, memory_scope, quality_score, cost_budget_usd, config_json, active, created_at, updated_at
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
		SELECT id, workspace_id, agent_key, name, role, memory_scope, quality_score, cost_budget_usd, config_json, active, created_at, updated_at
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
	if strings.TrimSpace(profile.Role) == "" {
		profile.Role = "generalist"
	}
	if strings.TrimSpace(profile.MemoryScope) == "" {
		profile.MemoryScope = "workspace"
	}
	query := `
		UPDATE agent_profile
		SET name = $3,
		    role = $4,
		    memory_scope = $5,
		    quality_score = $6,
		    cost_budget_usd = $7,
		    config_json = $8,
		    active = $9,
		    updated_at = CURRENT_TIMESTAMP
		WHERE workspace_id = $1 AND id = $2
		RETURNING updated_at
	`
	if err := b.db.QueryRowContext(
		ctx,
		query,
		profile.WorkspaceID,
		profile.ID,
		profile.Name,
		profile.Role,
		profile.MemoryScope,
		profile.QualityScore,
		profile.CostBudgetUSD,
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

// --- Channel Bindings ---

func (b *PostgresBackend) ListChannelBindings(ctx context.Context, workspaceId uint, agentID *string) ([]*types.ChannelBinding, error) {
	var query string
	var args []any
	if agentID != nil {
		query = `SELECT id, workspace_id, agent_id, channel_type, address, config_json, active, created_at, updated_at
			FROM agent_channel_binding WHERE workspace_id = $1 AND agent_id = $2 ORDER BY created_at ASC`
		args = []any{workspaceId, *agentID}
	} else {
		query = `SELECT id, workspace_id, agent_id, channel_type, address, config_json, active, created_at, updated_at
			FROM agent_channel_binding WHERE workspace_id = $1 AND agent_id IS NULL ORDER BY created_at ASC`
		args = []any{workspaceId}
	}
	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list channel bindings: %w", err)
	}
	defer rows.Close()

	out := make([]*types.ChannelBinding, 0)
	for rows.Next() {
		binding, err := b.scanChannelBinding(rows)
		if err != nil {
			return nil, fmt.Errorf("scan channel binding: %w", err)
		}
		out = append(out, binding)
	}
	return out, rows.Err()
}

func (b *PostgresBackend) UpsertChannelBinding(ctx context.Context, binding *types.ChannelBinding) error {
	configJSON, err := marshalJSONMap(binding.ConfigJSON)
	if err != nil {
		return fmt.Errorf("marshal channel binding config: %w", err)
	}
	query := `
		INSERT INTO agent_channel_binding (workspace_id, agent_id, channel_type, address, config_json, active)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (channel_type, address) DO UPDATE SET
			workspace_id = EXCLUDED.workspace_id,
			agent_id = EXCLUDED.agent_id,
			config_json = EXCLUDED.config_json,
			active = EXCLUDED.active,
			updated_at = CURRENT_TIMESTAMP
		RETURNING id, created_at, updated_at
	`
	return b.db.QueryRowContext(
		ctx, query,
		binding.WorkspaceID, binding.AgentID, binding.ChannelType,
		binding.Address, configJSON, binding.Active,
	).Scan(&binding.ID, &binding.CreatedAt, &binding.UpdatedAt)
}

func (b *PostgresBackend) DeleteChannelBinding(ctx context.Context, workspaceId uint, agentID *string, channelType string) error {
	var query string
	var args []any
	if agentID != nil {
		query = `DELETE FROM agent_channel_binding WHERE workspace_id = $1 AND agent_id = $2 AND channel_type = $3`
		args = []any{workspaceId, *agentID, channelType}
	} else {
		query = `DELETE FROM agent_channel_binding WHERE workspace_id = $1 AND agent_id IS NULL AND channel_type = $2`
		args = []any{workspaceId, channelType}
	}
	res, err := b.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("delete channel binding: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("channel binding not found")
	}
	return nil
}

func (b *PostgresBackend) GetChannelBindingByAddress(ctx context.Context, channelType string, address string) (*types.ChannelBinding, error) {
	query := `
		SELECT id, workspace_id, agent_id, channel_type, address, config_json, active, created_at, updated_at
		FROM agent_channel_binding
		WHERE channel_type = $1 AND address = $2 AND active = true
	`
	binding, err := b.scanChannelBinding(b.db.QueryRowContext(ctx, query, channelType, address))
	if err != nil {
		return nil, fmt.Errorf("get channel binding by address: %w", err)
	}
	return binding, nil
}

func (b *PostgresBackend) scanChannelBinding(row scanner) (*types.ChannelBinding, error) {
	binding := &types.ChannelBinding{}
	var configJSON []byte
	var agentID sql.NullString
	err := row.Scan(
		&binding.ID, &binding.WorkspaceID, &agentID,
		&binding.ChannelType, &binding.Address, &configJSON,
		&binding.Active, &binding.CreatedAt, &binding.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("channel binding not found")
	}
	if err != nil {
		return nil, err
	}
	if agentID.Valid {
		binding.AgentID = &agentID.String
	}
	binding.ConfigJSON = unmarshalJSONMap(configJSON)
	return binding, nil
}

func (b *PostgresBackend) CreateTask(ctx context.Context, task *types.AgentTask) error {
	return b.CreateTaskWithOutbox(ctx, task, nil)
}

func (b *PostgresBackend) CreateTaskWithOutbox(
	ctx context.Context,
	task *types.AgentTask,
	event *types.OrchestrationOutboxEvent,
) error {
	payloadJSON, err := marshalJSONMap(task.PayloadJSON)
	if err != nil {
		return fmt.Errorf("marshal task payload: %w", err)
	}
	routingJSON, err := marshalJSONMap(task.RoutingJSON)
	if err != nil {
		return fmt.Errorf("marshal task routing: %w", err)
	}
	if strings.TrimSpace(task.Priority) == "" {
		task.Priority = string(types.AgentTaskPriorityNormal)
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin create task tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	insertTaskQuery := `
		INSERT INTO agent_task (
			workspace_id, agent_id, kind, queue_mode, state,
			idempotency_key, payload_json, routing_json, parent_envelope_id,
			target_run_id, deadline, priority, budget_usd, cost_usd,
			wake_at, wake_reason, wake_count
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
		RETURNING id, accepted_at, created_at, updated_at
	`
	if err := tx.QueryRowContext(
		ctx,
		insertTaskQuery,
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
		task.Deadline,
		task.Priority,
		task.BudgetUSD,
		task.CostUSD,
		task.WakeAt,
		task.WakeReason,
		task.WakeCount,
	).Scan(&task.ID, &task.AcceptedAt, &task.CreatedAt, &task.UpdatedAt); err != nil {
		return fmt.Errorf("create agent task: %w", err)
	}

	if event == nil {
		event = &types.OrchestrationOutboxEvent{
			EventType: types.OrchestrationOutboxEventTypeTaskDispatch,
			DedupeKey: fmt.Sprintf("task_dispatch:%s:initial", task.ID),
			PayloadJSON: map[string]any{
				types.OrchestrationOutboxPayloadTaskID: task.ID,
			},
		}
	}
	if event.EventType == "" {
		event.EventType = types.OrchestrationOutboxEventTypeTaskDispatch
	}
	if strings.TrimSpace(event.DedupeKey) == "" {
		event.DedupeKey = fmt.Sprintf("%s:%s", event.EventType, task.ID)
	}
	if event.AvailableAt.IsZero() {
		event.AvailableAt = time.Now()
	}
	if event.PayloadJSON == nil {
		event.PayloadJSON = map[string]any{}
	}
	if _, ok := event.PayloadJSON[types.OrchestrationOutboxPayloadTaskID]; !ok {
		event.PayloadJSON[types.OrchestrationOutboxPayloadTaskID] = task.ID
	}
	payloadBytes, err := marshalJSONMap(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("marshal orchestration outbox payload: %w", err)
	}

	insertOutboxQuery := `
		INSERT INTO orchestration_outbox (
			event_type, dedupe_key, payload_json, available_at
		) VALUES ($1, $2, $3, $4)
	`
	if _, err := tx.ExecContext(
		ctx,
		insertOutboxQuery,
		event.EventType,
		event.DedupeKey,
		payloadBytes,
		event.AvailableAt,
	); err != nil {
		return fmt.Errorf("insert orchestration outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit create task tx: %w", err)
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
	var currentBlockerID sql.NullString
	var queuedAt sql.NullTime
	var dispatchedAt sql.NullTime
	var deadline sql.NullTime
	var droppedReason sql.NullString
	var budgetUSD sql.NullFloat64
	var costUSD sql.NullFloat64
	var archivedAt sql.NullTime
	var wakeAt sql.NullTime
	var wakeReason sql.NullString
	var inputKind sql.NullString
	var waitingSummary sql.NullString
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
		&currentBlockerID,
		&task.AcceptedAt,
		&queuedAt,
		&dispatchedAt,
		&deadline,
		&droppedReason,
		&task.Priority,
		&budgetUSD,
		&costUSD,
		&archivedAt,
		&task.CreatedAt,
		&task.UpdatedAt,
		&wakeAt,
		&wakeReason,
		&task.WakeCount,
		&inputKind,
		&waitingSummary,
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
	if currentBlockerID.Valid {
		task.CurrentBlockerID = &currentBlockerID.String
	}
	if queuedAt.Valid {
		task.QueuedAt = &queuedAt.Time
	}
	if dispatchedAt.Valid {
		task.DispatchedAt = &dispatchedAt.Time
	}
	if deadline.Valid {
		task.Deadline = &deadline.Time
	}
	if droppedReason.Valid {
		task.DroppedReason = &droppedReason.String
	}
	if budgetUSD.Valid {
		task.BudgetUSD = &budgetUSD.Float64
	}
	if costUSD.Valid {
		task.CostUSD = costUSD.Float64
	}
	if archivedAt.Valid {
		task.ArchivedAt = &archivedAt.Time
	}
	if wakeAt.Valid {
		task.WakeAt = &wakeAt.Time
	}
	if wakeReason.Valid {
		task.WakeReason = &wakeReason.String
	}
	if inputKind.Valid {
		task.InputKind = types.InputKind(inputKind.String)
	}
	if task.State == types.AgentTaskStateWaiting && waitingSummary.Valid {
		task.WaitingSummary = &waitingSummary.String
	}
	task.PayloadJSON = unmarshalJSONMap(payloadJSON)
	task.RoutingJSON = unmarshalJSONMap(routingJSON)
	return task, nil
}

func wakeAgendaSummary(items []*types.TaskWakeAgendaItem) string {
	for _, item := range items {
		if item == nil {
			continue
		}
		if title := strings.TrimSpace(item.Title); title != "" {
			return title
		}
		if reason := strings.TrimSpace(item.Reason); reason != "" {
			return reason
		}
	}
	return ""
}

func (b *PostgresBackend) attachWakeAgenda(ctx context.Context, tasks []*types.AgentTask) error {
	if len(tasks) == 0 {
		return nil
	}

	taskByID := make(map[string]*types.AgentTask, len(tasks))
	taskIDs := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		task.WakeAgenda = nil
		if task.State != types.AgentTaskStateSleeping || task.WakeAt == nil {
			continue
		}
		taskByID[task.ID] = task
		taskIDs = append(taskIDs, task.ID)
	}
	if len(taskIDs) == 0 {
		return nil
	}

	rows, err := b.db.QueryContext(ctx, `
		SELECT task_id, seq, item_type, title, reason
		FROM task_wake_agenda_item
		WHERE task_id = ANY($1::uuid[])
		ORDER BY task_id, seq
	`, pq.Array(taskIDs))
	if err != nil {
		return fmt.Errorf("list wake agenda items: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var taskID string
		item := &types.TaskWakeAgendaItem{}
		if err := rows.Scan(&taskID, &item.Seq, &item.Type, &item.Title, &item.Reason); err != nil {
			return fmt.Errorf("scan wake agenda item: %w", err)
		}
		task, ok := taskByID[taskID]
		if !ok {
			continue
		}
		task.WakeAgenda = append(task.WakeAgenda, item)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate wake agenda items: %w", err)
	}

	for _, task := range taskByID {
		if task == nil || task.WakeReason != nil {
			continue
		}
		if summary := wakeAgendaSummary(task.WakeAgenda); summary != "" {
			task.WakeReason = &summary
		}
	}
	return nil
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
	if err := b.attachWakeAgenda(ctx, []*types.AgentTask{task}); err != nil {
		return nil, err
	}
	if err := b.attachCurrentBlockers(ctx, []*types.AgentTask{task}); err != nil {
		return nil, err
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
	if err := b.attachWakeAgenda(ctx, tasks); err != nil {
		return nil, err
	}
	if err := b.attachCurrentBlockers(ctx, tasks); err != nil {
		return nil, err
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
		  AND ($9::boolean OR archived_at IS NULL)
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
		filter.IncludeArchived,
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
	if err := b.attachWakeAgenda(ctx, tasks); err != nil {
		return nil, err
	}
	if err := b.attachCurrentBlockers(ctx, tasks); err != nil {
		return nil, err
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
	if err := b.attachWakeAgenda(ctx, []*types.AgentTask{task}); err != nil {
		return nil, err
	}
	if err := b.attachCurrentBlockers(ctx, []*types.AgentTask{task}); err != nil {
		return nil, err
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
	if err := b.attachWakeAgenda(ctx, []*types.AgentTask{task}); err != nil {
		return nil, err
	}
	if err := b.attachCurrentBlockers(ctx, []*types.AgentTask{task}); err != nil {
		return nil, err
	}
	return task, nil
}

func (b *PostgresBackend) ClaimQueuedTaskForDispatch(
	ctx context.Context,
	taskID string,
	staleAfter time.Duration,
) (*types.AgentTask, bool, error) {
	if strings.TrimSpace(taskID) == "" {
		return nil, false, fmt.Errorf("task id is required")
	}
	if staleAfter <= 0 {
		staleAfter = 45 * time.Second
	}

	staleCutoff := time.Now().Add(-staleAfter)
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("begin claim queued task tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	query := `
		UPDATE agent_task
		SET state = 'running'::agent_task_state,
		    dispatched_at = CURRENT_TIMESTAMP,
		    wake_at = NULL,
		    wake_reason = NULL,
		    current_blocker_id = NULL,
		    input_kind = NULL,
		    waiting_summary = NULL,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		  AND (
		    state = 'queued'::agent_task_state
		    OR state = 'sleeping'::agent_task_state
		    OR (
		      state = 'running'::agent_task_state
		      AND target_run_id IS NULL
		      AND dispatched_at IS NOT NULL
		      AND dispatched_at <= $2
		    )
		  )
		RETURNING id
	`

	var claimedID string
	if err := tx.QueryRowContext(ctx, query, taskID, staleCutoff).Scan(&claimedID); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("claim queued task for dispatch: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM task_wake_agenda_item WHERE task_id = $1`, claimedID); err != nil {
		return nil, false, fmt.Errorf("clear wake agenda on dispatch claim: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit claim queued task tx: %w", err)
	}

	task, err := b.GetTaskByID(ctx, claimedID)
	if err != nil {
		return nil, false, err
	}
	return task, true, nil
}

func (b *PostgresBackend) UpdateTaskState(ctx context.Context, update types.TaskStateUpdate) error {
	now := time.Now()
	query := `
		UPDATE agent_task
		SET state = $2::agent_task_state,
		    queued_at = CASE WHEN $2::agent_task_state = 'queued'::agent_task_state THEN $3 ELSE queued_at END,
		    dispatched_at = CASE
		      WHEN $2::agent_task_state = 'running'::agent_task_state THEN $3
		      WHEN $2::agent_task_state = 'sleeping'::agent_task_state THEN NULL
		      ELSE dispatched_at
		    END,
		    dropped_reason = CASE WHEN $2::agent_task_state = 'dropped'::agent_task_state THEN $4 ELSE dropped_reason END,
		    target_run_id = COALESCE($5::uuid, target_run_id),
		    current_blocker_id = NULL,
		    input_kind = NULL,
		    waiting_summary = NULL,
		    wake_at = CASE WHEN $2::agent_task_state = 'sleeping'::agent_task_state THEN wake_at ELSE NULL END,
		    wake_reason = CASE WHEN $2::agent_task_state = 'sleeping'::agent_task_state THEN wake_reason ELSE NULL END,
		    wake_count = CASE WHEN $2::agent_task_state = 'sleeping'::agent_task_state THEN wake_count ELSE 0 END,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	res, err := b.db.ExecContext(ctx, query, update.TaskID, update.State, now, update.DroppedReason, update.TargetRunID)
	if err != nil {
		return fmt.Errorf("update task state: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return &types.ErrAgentTaskNotFound{ID: update.TaskID}
	}
	return nil
}

func (b *PostgresBackend) UpdateTaskStateIfCurrentRun(ctx context.Context, update types.CurrentRunTaskStateUpdate) (bool, error) {
	if update.State == types.AgentTaskStateWaiting {
		return false, fmt.Errorf("update task state if current run does not support waiting without blocker")
	}
	now := time.Now()
	baseArgs := []any{update.TaskID, update.State, now, update.DroppedReason, update.TargetRunID}
	expectedRunID := strings.TrimSpace(update.ExpectedRunID)

	query := `
		UPDATE agent_task
		SET state = $2::agent_task_state,
		    queued_at = CASE WHEN $2::agent_task_state = 'queued'::agent_task_state THEN $3 ELSE queued_at END,
		    dispatched_at = CASE
		      WHEN $2::agent_task_state = 'running'::agent_task_state THEN $3
		      WHEN $2::agent_task_state = 'sleeping'::agent_task_state THEN NULL
		      ELSE dispatched_at
		    END,
		    dropped_reason = CASE WHEN $2::agent_task_state = 'dropped'::agent_task_state THEN $4 ELSE dropped_reason END,
		    target_run_id = COALESCE($5::uuid, target_run_id),
		    current_blocker_id = NULL,
		    input_kind = NULL,
		    waiting_summary = NULL,
		    wake_at = CASE WHEN $2::agent_task_state = 'sleeping'::agent_task_state THEN wake_at ELSE NULL END,
		    wake_reason = CASE WHEN $2::agent_task_state = 'sleeping'::agent_task_state THEN wake_reason ELSE NULL END,
		    wake_count = CASE WHEN $2::agent_task_state = 'sleeping'::agent_task_state THEN wake_count ELSE 0 END,
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

func (b *PostgresBackend) SleepTaskWithOutbox(
	ctx context.Context,
	taskID string,
	expectedRunID string,
	wakeAt time.Time,
	wakeReason string,
	wakeAgenda []*types.TaskWakeAgendaItem,
	outboxEvent *types.OrchestrationOutboxEvent,
) (bool, error) {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin sleep task tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, `
		UPDATE agent_task
		SET state = 'sleeping'::agent_task_state,
		    wake_at = $2,
		    wake_reason = $3,
		    wake_count = wake_count + 1,
		    dispatched_at = NULL,
		    current_blocker_id = NULL,
		    input_kind = NULL,
		    waiting_summary = NULL,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		  AND target_run_id = $4::uuid
		  AND state NOT IN ('done'::agent_task_state, 'dropped'::agent_task_state, 'cancelled'::agent_task_state)
	`, taskID, wakeAt, wakeReason, expectedRunID)
	if err != nil {
		return false, fmt.Errorf("update task to sleeping: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return false, nil
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM task_wake_agenda_item WHERE task_id = $1`, taskID); err != nil {
		return false, fmt.Errorf("clear prior wake agenda: %w", err)
	}
	for idx, item := range wakeAgenda {
		if item == nil {
			continue
		}
		seq := item.Seq
		if seq <= 0 {
			seq = idx + 1
		}
		title := strings.TrimSpace(item.Title)
		reason := strings.TrimSpace(item.Reason)
		if title == "" {
			title = reason
		}
		if title == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO task_wake_agenda_item (task_id, seq, item_type, title, reason)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (task_id, seq) DO UPDATE
			SET item_type = EXCLUDED.item_type,
			    title = EXCLUDED.title,
			    reason = EXCLUDED.reason
		`, taskID, seq, strings.TrimSpace(item.Type), title, reason); err != nil {
			return false, fmt.Errorf("insert wake agenda item: %w", err)
		}
	}

	payloadJSON, err := marshalJSONMap(outboxEvent.PayloadJSON)
	if err != nil {
		return false, fmt.Errorf("marshal outbox payload: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO orchestration_outbox (event_type, dedupe_key, payload_json, available_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (dedupe_key) DO NOTHING
	`, outboxEvent.EventType, outboxEvent.DedupeKey, payloadJSON, outboxEvent.AvailableAt); err != nil {
		return false, fmt.Errorf("enqueue wake dispatch event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit sleep task tx: %w", err)
	}
	return true, nil
}

func (b *PostgresBackend) RequeueTaskWithOutboxIfCurrentRun(
	ctx context.Context,
	task *types.AgentTask,
	expectedRunID string,
	outboxEvent *types.OrchestrationOutboxEvent,
) (bool, error) {
	if task == nil {
		return false, fmt.Errorf("task is required")
	}
	expectedRunID = strings.TrimSpace(expectedRunID)
	if expectedRunID == "" {
		return false, fmt.Errorf("expected run id is required")
	}

	payloadJSON, err := marshalJSONMap(task.PayloadJSON)
	if err != nil {
		return false, fmt.Errorf("marshal task payload: %w", err)
	}
	routingJSON, err := marshalJSONMap(task.RoutingJSON)
	if err != nil {
		return false, fmt.Errorf("marshal task routing: %w", err)
	}
	if strings.TrimSpace(task.Priority) == "" {
		task.Priority = string(types.AgentTaskPriorityNormal)
	}

	now := time.Now()
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin requeue task tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var queuedAt time.Time
	if err := tx.QueryRowContext(
		ctx,
		`UPDATE agent_task
		    SET payload_json = $2,
		        routing_json = $3,
		        deadline = $4,
		        priority = $5,
		        budget_usd = $6,
		        cost_usd = $7,
		        state = 'queued'::agent_task_state,
		        queued_at = CURRENT_TIMESTAMP,
		        dispatched_at = NULL,
		        dropped_reason = NULL,
		        target_run_id = NULL,
		        current_blocker_id = NULL,
		        input_kind = NULL,
		        waiting_summary = NULL,
		        wake_at = NULL,
		        wake_reason = NULL,
		        wake_count = 0,
		        updated_at = CURRENT_TIMESTAMP
		  WHERE id = $1
		    AND target_run_id = $8::uuid
		  RETURNING queued_at, updated_at`,
		task.ID,
		payloadJSON,
		routingJSON,
		task.Deadline,
		task.Priority,
		task.BudgetUSD,
		task.CostUSD,
		expectedRunID,
	).Scan(&queuedAt, &task.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("requeue task for dispatch: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE orchestration_outbox
		SET published_at = CURRENT_TIMESTAMP,
		    last_error = 'superseded_by_followup',
		    updated_at = CURRENT_TIMESTAMP
		WHERE published_at IS NULL
		  AND payload_json->>'task_id' = $1
	`, task.ID); err != nil {
		return false, fmt.Errorf("cancel superseded outbox events for task: %w", err)
	}

	if outboxEvent == nil {
		outboxEvent = &types.OrchestrationOutboxEvent{
			EventType: types.OrchestrationOutboxEventTypeTaskDispatch,
			DedupeKey: fmt.Sprintf("task_dispatch:%s:resume:%d", task.ID, now.UnixNano()),
			PayloadJSON: map[string]any{
				types.OrchestrationOutboxPayloadTaskID: task.ID,
			},
			AvailableAt: now,
		}
	}
	if outboxEvent.EventType == "" {
		outboxEvent.EventType = types.OrchestrationOutboxEventTypeTaskDispatch
	}
	if strings.TrimSpace(outboxEvent.DedupeKey) == "" {
		outboxEvent.DedupeKey = fmt.Sprintf("task_dispatch:%s:resume:%d", task.ID, now.UnixNano())
	}
	if outboxEvent.AvailableAt.IsZero() {
		outboxEvent.AvailableAt = now
	}
	if outboxEvent.PayloadJSON == nil {
		outboxEvent.PayloadJSON = map[string]any{}
	}
	if _, ok := outboxEvent.PayloadJSON[types.OrchestrationOutboxPayloadTaskID]; !ok {
		outboxEvent.PayloadJSON[types.OrchestrationOutboxPayloadTaskID] = task.ID
	}
	outboxPayload, err := marshalJSONMap(outboxEvent.PayloadJSON)
	if err != nil {
		return false, fmt.Errorf("marshal outbox payload: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO orchestration_outbox (event_type, dedupe_key, payload_json, available_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (dedupe_key) DO NOTHING
	`, outboxEvent.EventType, outboxEvent.DedupeKey, outboxPayload, outboxEvent.AvailableAt); err != nil {
		return false, fmt.Errorf("enqueue requeue outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit requeue task tx: %w", err)
	}

	task.State = types.AgentTaskStateQueued
	task.TargetRunID = nil
	task.InputKind = ""
	task.QueuedAt = &queuedAt
	task.DispatchedAt = nil
	task.DroppedReason = nil
	task.WakeAt = nil
	task.WakeReason = nil
	task.WakeCount = 0
	return true, nil
}

func (b *PostgresBackend) ListActiveChildTaskIDs(ctx context.Context, parentTaskID string) ([]string, error) {
	rows, err := b.db.QueryContext(ctx, `
		SELECT id FROM agent_task
		WHERE parent_envelope_id = $1
		AND state NOT IN ('done', 'error', 'dropped', 'cancelled')
	`, parentTaskID)
	if err != nil {
		return nil, fmt.Errorf("list active child task ids: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (b *PostgresBackend) ListSubtasks(ctx context.Context, parentTaskID string) ([]*types.AgentTask, error) {
	query := agentTaskSelect + `
		WHERE parent_envelope_id = $1
		ORDER BY created_at DESC
	`
	rows, err := b.db.QueryContext(ctx, query, parentTaskID)
	if err != nil {
		return nil, fmt.Errorf("list subtasks: %w", err)
	}
	defer rows.Close()
	var tasks []*types.AgentTask
	for rows.Next() {
		task, err := b.scanAgentTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	if err := b.attachCurrentBlockers(ctx, tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

func (b *PostgresBackend) ListSubtasksByOutputIDs(ctx context.Context, outputIDs []string) ([]*types.AgentTask, error) {
	if len(outputIDs) == 0 {
		return nil, nil
	}
	query := agentTaskSelect + `
		WHERE id IN (
			SELECT task_id FROM task_spawn_binding WHERE source_output_id = ANY($1)
		)
		ORDER BY created_at DESC
	`
	rows, err := b.db.QueryContext(ctx, query, pq.Array(outputIDs))
	if err != nil {
		return nil, fmt.Errorf("list subtasks by output IDs: %w", err)
	}
	defer rows.Close()
	var tasks []*types.AgentTask
	for rows.Next() {
		task, err := b.scanAgentTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	if err := b.attachCurrentBlockers(ctx, tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

type SpawnBinding struct {
	TaskID         string    `db:"task_id"`
	SourceOutputID string    `db:"source_output_id"`
	EntityLabel    string    `db:"entity_label"`
	CreatedAt      time.Time `db:"created_at"`
}

func (b *PostgresBackend) CreateSpawnBinding(ctx context.Context, taskID, sourceOutputID, entityLabel string) error {
	_, err := b.db.ExecContext(ctx, `
		INSERT INTO task_spawn_binding (task_id, source_output_id, entity_label)
		VALUES ($1, $2, $3)
		ON CONFLICT (task_id) DO NOTHING
	`, taskID, sourceOutputID, entityLabel)
	if err != nil {
		return fmt.Errorf("create spawn binding: %w", err)
	}
	return nil
}

func (b *PostgresBackend) ListSpawnBindingsForOutputs(ctx context.Context, outputIDs []string) ([]SpawnBinding, error) {
	if len(outputIDs) == 0 {
		return nil, nil
	}
	rows, err := b.db.QueryContext(ctx, `
		SELECT task_id, source_output_id, entity_label, created_at
		FROM task_spawn_binding
		WHERE source_output_id = ANY($1)
	`, pq.Array(outputIDs))
	if err != nil {
		return nil, fmt.Errorf("list spawn bindings: %w", err)
	}
	defer rows.Close()
	var bindings []SpawnBinding
	for rows.Next() {
		var b SpawnBinding
		if err := rows.Scan(&b.TaskID, &b.SourceOutputID, &b.EntityLabel, &b.CreatedAt); err != nil {
			return nil, err
		}
		bindings = append(bindings, b)
	}
	return bindings, nil
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

func (b *PostgresBackend) UpdateTask(ctx context.Context, task *types.AgentTask) error {
	if task == nil {
		return fmt.Errorf("task is required")
	}
	payloadJSON, err := marshalJSONMap(task.PayloadJSON)
	if err != nil {
		return fmt.Errorf("marshal task payload: %w", err)
	}
	routingJSON, err := marshalJSONMap(task.RoutingJSON)
	if err != nil {
		return fmt.Errorf("marshal task routing: %w", err)
	}
	if strings.TrimSpace(task.Priority) == "" {
		task.Priority = string(types.AgentTaskPriorityNormal)
	}
	err = b.db.QueryRowContext(
		ctx,
		`UPDATE agent_task
		    SET payload_json = $2,
		        routing_json = $3,
		        deadline = $4,
		        priority = $5,
		        budget_usd = $6,
		        cost_usd = $7,
		        updated_at = CURRENT_TIMESTAMP
		  WHERE id = $1
		  RETURNING updated_at`,
		task.ID,
		payloadJSON,
		routingJSON,
		task.Deadline,
		task.Priority,
		task.BudgetUSD,
		task.CostUSD,
	).Scan(&task.UpdatedAt)
	if err == sql.ErrNoRows {
		return &types.ErrAgentTaskNotFound{ID: task.ID}
	}
	if err != nil {
		return fmt.Errorf("update task: %w", err)
	}
	return nil
}

func (b *PostgresBackend) UpdateTaskCost(ctx context.Context, taskID string, costUSD float64) error {
	res, err := b.db.ExecContext(
		ctx,
		`UPDATE agent_task SET cost_usd = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $1`,
		taskID,
		costUSD,
	)
	if err != nil {
		return fmt.Errorf("update task cost: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return &types.ErrAgentTaskNotFound{ID: taskID}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Task input inbox (durable follow-up inputs)
// ---------------------------------------------------------------------------

func (b *PostgresBackend) AppendTaskInput(ctx context.Context, input *types.TaskInput) error {
	if input == nil {
		return fmt.Errorf("input is required")
	}
	err := b.db.QueryRowContext(
		ctx,
		`WITH _lock AS MATERIALIZED (
		   SELECT pg_advisory_xact_lock(hashtext($2::text))
		 ),
		 _seq AS (
		   SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq
		     FROM task_input
		    WHERE task_id = $2::uuid
		 )
		 INSERT INTO task_input (workspace_id, task_id, session_id, kind, action, message, idempotency_key, status,
		   seq)
		 SELECT $1::bigint, $2::uuid, $3, $4, $5, $6, $7, 'pending', next_seq
		   FROM _lock, _seq
		 ON CONFLICT (task_id, idempotency_key) DO NOTHING
		 RETURNING id, seq, created_at`,
		input.WorkspaceID, input.TaskID, input.SessionID,
		input.Kind, input.Action, input.Message, input.IdempotencyKey,
	).Scan(&input.ID, &input.Seq, &input.CreatedAt)
	if err == sql.ErrNoRows {
		return nil // idempotency conflict — already exists
	}
	if err != nil {
		return fmt.Errorf("append task input: %w", err)
	}
	input.Status = types.TaskInputStatusPending
	return nil
}

func (b *PostgresBackend) ListPendingTaskInputs(ctx context.Context, taskID string, limit int) ([]*types.TaskInput, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := b.db.QueryContext(
		ctx,
		`SELECT id, workspace_id, task_id, session_id, seq, kind, action, message,
		        idempotency_key, status, claimed_by_run_id, claimed_by_execution_id,
		        created_at, claimed_at, consumed_at
		   FROM task_input
		  WHERE task_id = $1::uuid AND status = 'pending'
		  ORDER BY seq ASC
		  LIMIT $2`,
		taskID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending task inputs: %w", err)
	}
	defer rows.Close()
	var out []*types.TaskInput
	for rows.Next() {
		ti, err := scanTaskInput(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ti)
	}
	return out, rows.Err()
}

func (b *PostgresBackend) ListOrphanedPendingInputs(ctx context.Context, maxAge time.Duration, limit int) ([]*types.TaskInput, error) {
	if maxAge <= 0 {
		maxAge = 30 * time.Second
	}
	if limit <= 0 {
		limit = 100
	}
	maxAgeSeconds := int64(maxAge / time.Second)
	if maxAgeSeconds <= 0 {
		maxAgeSeconds = 1
	}
	rows, err := b.db.QueryContext(
		ctx,
		`SELECT id, workspace_id, task_id, session_id, seq, kind, action, message,
		        idempotency_key, status, claimed_by_run_id, claimed_by_execution_id,
		        created_at, claimed_at, consumed_at
		   FROM task_input
		  WHERE status = 'pending'
		    AND created_at < CURRENT_TIMESTAMP - ($1 * interval '1 second')
		  ORDER BY created_at ASC
		  LIMIT $2`,
		maxAgeSeconds,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list orphaned pending task inputs: %w", err)
	}
	defer rows.Close()
	var out []*types.TaskInput
	for rows.Next() {
		ti, err := scanTaskInput(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ti)
	}
	return out, rows.Err()
}

func (b *PostgresBackend) ClaimNextTaskInput(ctx context.Context, taskID string, runID string, executionID string) (*types.TaskInput, error) {
	row := b.db.QueryRowContext(
		ctx,
		`UPDATE task_input
		    SET status = 'claimed',
		        claimed_by_run_id = $2::uuid,
		        claimed_by_execution_id = $3,
		        claimed_at = CURRENT_TIMESTAMP
		  WHERE id = (
		    SELECT id FROM task_input
		     WHERE task_id = $1::uuid AND status = 'pending'
		     ORDER BY seq ASC
		     LIMIT 1
		     FOR UPDATE SKIP LOCKED
		  )
		  RETURNING id, workspace_id, task_id, session_id, seq, kind, action, message,
		            idempotency_key, status, claimed_by_run_id, claimed_by_execution_id,
		            created_at, claimed_at, consumed_at`,
		taskID, runID, executionID,
	)
	ti, err := scanTaskInput(row)
	if err == sql.ErrNoRows {
		return nil, nil // no pending input
	}
	if err != nil {
		return nil, fmt.Errorf("claim next task input: %w", err)
	}
	return ti, nil
}

func (b *PostgresBackend) ConsumeOldestPendingInput(ctx context.Context, taskID string) (string, error) {
	var message string
	err := b.db.QueryRowContext(ctx,
		`UPDATE task_input
		    SET status = 'consumed', consumed_at = CURRENT_TIMESTAMP
		  WHERE id = (
		    SELECT id FROM task_input
		     WHERE task_id = $1::uuid AND status = 'pending'
		     ORDER BY seq ASC LIMIT 1
		     FOR UPDATE SKIP LOCKED
		  )
		  RETURNING message`,
		taskID,
	).Scan(&message)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("consume oldest pending input: %w", err)
	}
	return strings.TrimSpace(message), nil
}

func (b *PostgresBackend) AckTaskInputConsumed(ctx context.Context, inputID string) error {
	_, err := b.db.ExecContext(
		ctx,
		`UPDATE task_input
		    SET status = 'consumed',
		        consumed_at = CURRENT_TIMESTAMP
		  WHERE id = $1::uuid AND status = 'claimed'`,
		inputID,
	)
	if err != nil {
		return fmt.Errorf("ack task input consumed: %w", err)
	}
	return nil
}

func (b *PostgresBackend) ReleaseStaleTaskInputClaims(ctx context.Context, runID string) error {
	_, err := b.db.ExecContext(
		ctx,
		`UPDATE task_input
		    SET status = 'pending',
		        claimed_by_run_id = NULL,
		        claimed_by_execution_id = NULL,
		        claimed_at = NULL
		  WHERE claimed_by_run_id = $1::uuid AND status = 'claimed'`,
		runID,
	)
	if err != nil {
		return fmt.Errorf("release stale task input claims: %w", err)
	}
	return nil
}

func (b *PostgresBackend) CountPendingTaskInputs(ctx context.Context, taskID string) (int, error) {
	var count int
	err := b.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM task_input WHERE task_id = $1::uuid AND status = 'pending'`,
		taskID,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count pending task inputs: %w", err)
	}
	return count, nil
}

func scanTaskInput(row scanner) (*types.TaskInput, error) {
	ti := &types.TaskInput{}
	var action sql.NullString
	var claimedByRunID sql.NullString
	var claimedByExecID sql.NullString
	var claimedAt sql.NullTime
	var consumedAt sql.NullTime
	err := row.Scan(
		&ti.ID, &ti.WorkspaceID, &ti.TaskID, &ti.SessionID, &ti.Seq,
		&ti.Kind, &action, &ti.Message,
		&ti.IdempotencyKey, &ti.Status, &claimedByRunID, &claimedByExecID,
		&ti.CreatedAt, &claimedAt, &consumedAt,
	)
	if err != nil {
		return nil, err
	}
	if action.Valid {
		a := types.TaskInputAction(action.String)
		ti.Action = &a
	}
	if claimedByRunID.Valid {
		ti.ClaimedByRunID = &claimedByRunID.String
	}
	if claimedByExecID.Valid {
		ti.ClaimedByExecID = &claimedByExecID.String
	}
	if claimedAt.Valid {
		ti.ClaimedAt = &claimedAt.Time
	}
	if consumedAt.Valid {
		ti.ConsumedAt = &consumedAt.Time
	}
	return ti, nil
}

func scanOrchestrationOutboxEvent(row scanner) (*types.OrchestrationOutboxEvent, error) {
	event := &types.OrchestrationOutboxEvent{}
	var payloadJSON []byte
	var publishedAt sql.NullTime
	var lastError sql.NullString
	err := row.Scan(
		&event.ID,
		&event.EventType,
		&event.DedupeKey,
		&payloadJSON,
		&event.AvailableAt,
		&publishedAt,
		&event.Attempts,
		&lastError,
		&event.CreatedAt,
		&event.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, sql.ErrNoRows
	}
	if err != nil {
		return nil, err
	}
	event.PayloadJSON = unmarshalJSONMap(payloadJSON)
	if publishedAt.Valid {
		event.PublishedAt = &publishedAt.Time
	}
	if lastError.Valid {
		event.LastError = &lastError.String
	}
	return event, nil
}

func (b *PostgresBackend) EnqueueOrchestrationOutboxEvent(
	ctx context.Context,
	event *types.OrchestrationOutboxEvent,
) error {
	if event == nil {
		return fmt.Errorf("outbox event is required")
	}
	if strings.TrimSpace(string(event.EventType)) == "" {
		return fmt.Errorf("outbox event_type is required")
	}
	event.DedupeKey = strings.TrimSpace(event.DedupeKey)
	if event.DedupeKey == "" {
		return fmt.Errorf("outbox dedupe_key is required")
	}
	if event.AvailableAt.IsZero() {
		event.AvailableAt = time.Now()
	}

	payloadJSON, err := marshalJSONMap(event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}

	query := `
		INSERT INTO orchestration_outbox (
			event_type, dedupe_key, payload_json, available_at
		) VALUES ($1, $2, $3, $4)
		ON CONFLICT (dedupe_key) DO NOTHING
	`
	_, err = b.db.ExecContext(
		ctx,
		query,
		event.EventType,
		event.DedupeKey,
		payloadJSON,
		event.AvailableAt,
	)
	if err != nil {
		return fmt.Errorf("enqueue orchestration outbox event: %w", err)
	}
	return nil
}

func (b *PostgresBackend) ClaimPendingOrchestrationOutboxEvents(
	ctx context.Context,
	limit int,
) ([]*types.OrchestrationOutboxEvent, error) {
	if limit <= 0 {
		limit = 100
	}

	query := `
		WITH claimed AS (
			SELECT id
			FROM orchestration_outbox
			WHERE published_at IS NULL
			  AND available_at <= CURRENT_TIMESTAMP
			ORDER BY available_at ASC, id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE orchestration_outbox o
		SET attempts = o.attempts + 1,
		    updated_at = CURRENT_TIMESTAMP
		FROM claimed
		WHERE o.id = claimed.id
		RETURNING
			o.id,
			o.event_type,
			o.dedupe_key,
			o.payload_json,
			o.available_at,
			o.published_at,
			o.attempts,
			o.last_error,
			o.created_at,
			o.updated_at
	`

	rows, err := b.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("claim orchestration outbox events: %w", err)
	}
	defer rows.Close()

	out := make([]*types.OrchestrationOutboxEvent, 0, limit)
	for rows.Next() {
		event, scanErr := scanOrchestrationOutboxEvent(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan orchestration outbox event: %w", scanErr)
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

func (b *PostgresBackend) MarkOrchestrationOutboxEventPublished(ctx context.Context, eventID int64) error {
	query := `
		UPDATE orchestration_outbox
		SET published_at = COALESCE(published_at, CURRENT_TIMESTAMP),
		    last_error = NULL,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	res, err := b.db.ExecContext(ctx, query, eventID)
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("outbox event not found: %d", eventID)
	}
	return nil
}

func (b *PostgresBackend) CancelPendingOutboxEventsForTask(ctx context.Context, taskID string) error {
	query := `
		UPDATE orchestration_outbox
		SET published_at = CURRENT_TIMESTAMP,
		    last_error = 'task_cancelled',
		    updated_at = CURRENT_TIMESTAMP
		WHERE published_at IS NULL
		  AND payload_json->>'task_id' = $1
	`
	_, err := b.db.ExecContext(ctx, query, taskID)
	if err != nil {
		return fmt.Errorf("cancel pending outbox events for task: %w", err)
	}
	return nil
}

func (b *PostgresBackend) MarkOrchestrationOutboxEventError(
	ctx context.Context,
	eventID int64,
	lastError string,
) error {
	query := `
		UPDATE orchestration_outbox
		SET last_error = NULLIF($2, ''),
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`
	res, err := b.db.ExecContext(ctx, query, eventID, strings.TrimSpace(lastError))
	if err != nil {
		return fmt.Errorf("mark outbox event error: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("outbox event not found: %d", eventID)
	}
	return nil
}

func (b *PostgresBackend) AcquireOrchestrationResultInbox(
	ctx context.Context,
	resultKey string,
	streamID string,
) (bool, error) {
	resultKey = strings.TrimSpace(resultKey)
	if resultKey == "" {
		return false, fmt.Errorf("result key is required")
	}
	streamID = strings.TrimSpace(streamID)
	if streamID == "" {
		return false, fmt.Errorf("stream id is required")
	}
	query := `
		INSERT INTO orchestration_inbox_results (result_key, stream_id)
		VALUES ($1, $2)
		ON CONFLICT (result_key) DO NOTHING
	`
	res, err := b.db.ExecContext(ctx, query, resultKey, streamID)
	if err != nil {
		return false, fmt.Errorf("acquire orchestration result inbox: %w", err)
	}
	affected, _ := res.RowsAffected()
	return affected > 0, nil
}

func (b *PostgresBackend) AcquireOrchestrationRetryGuard(
	ctx context.Context,
	guardKey string,
) (bool, error) {
	guardKey = strings.TrimSpace(guardKey)
	if guardKey == "" {
		return false, fmt.Errorf("guard key is required")
	}
	query := `
		INSERT INTO orchestration_retry_guard (guard_key)
		VALUES ($1)
		ON CONFLICT (guard_key) DO NOTHING
	`
	res, err := b.db.ExecContext(ctx, query, guardKey)
	if err != nil {
		return false, fmt.Errorf("acquire orchestration retry guard: %w", err)
	}
	affected, _ := res.RowsAffected()
	return affected > 0, nil
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
			network_enabled, interactive, timeout_ms, cost_usd, usage_json, delivery_json
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10, $11, $12, $13, $14,
			$15, $16, $17, $18, $19, $20
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
		run.CostUSD,
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
	var createdByMemberID sql.NullInt64
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
	var costUSD sql.NullFloat64
	err := row.Scan(
		&run.ID,
		&run.WorkspaceID,
		&agentID,
		&createdByMemberID,
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
		&costUSD,
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
	if createdByMemberID.Valid {
		value := uint(createdByMemberID.Int64)
		run.CreatedByMemberID = &value
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
	if costUSD.Valid {
		run.CostUSD = costUSD.Float64
	}
	run.UsageJSON = unmarshalJSONMap(usageJSON)
	run.DeliveryJSON = unmarshalJSONMap(deliveryJSON)
	if run.CostUSD <= 0 {
		run.CostUSD = usageCostUSD(run.UsageJSON)
	}
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
		  AND ($3::uuid IS NULL OR origin_task_id = $3::uuid)
		  AND ($4::text[] IS NULL OR status::text = ANY($4::text[]))
		  AND ($5::text IS NULL OR session_id = $5::text)
		  AND ($6::timestamptz IS NULL OR created_at >= $6::timestamptz)
		  AND ($7::timestamptz IS NULL OR created_at <= $7::timestamptz)
		ORDER BY created_at DESC, id DESC
		LIMIT $8 OFFSET $9
	`

	rows, err := b.db.QueryContext(
		ctx,
		query,
		workspaceId,
		optionalStringArg(filter.AgentID),
		optionalStringArg(filter.TaskID),
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

func (b *PostgresBackend) SetRunExecutionStartedForAttempt(
	ctx context.Context,
	externalId string,
	attemptID string,
) (bool, error) {
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" {
		return false, fmt.Errorf("set run execution started for attempt: attempt id is required")
	}
	now := time.Now()
	query := `
		UPDATE agent_run
		SET status = 'running'::agent_run_status,
		    started_at = COALESCE(started_at, $2),
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1::uuid
		  AND run_attempt_id = $3::uuid
		  AND ` + runExecutionScopeWhere + `
		  AND status = 'accepted'::agent_run_status`
	result, err := b.db.ExecContext(ctx, query, externalId, now, attemptID)
	if err != nil {
		return false, fmt.Errorf("set run execution started for attempt: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows > 0 {
		return true, nil
	}

	run, lookupErr := b.GetRunExecution(ctx, externalId)
	if lookupErr != nil {
		return false, lookupErr
	}
	currentAttemptID := ""
	if run.RunAttemptID != nil {
		currentAttemptID = strings.TrimSpace(*run.RunAttemptID)
	}
	if currentAttemptID != attemptID || run.Status == types.RunExecutionStatusRunning || run.IsTerminal() {
		return false, nil
	}
	return false, fmt.Errorf("run execution cannot be started (state=%s)", run.Status)
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

func (b *PostgresBackend) SetRunExecutionResultForAttempt(
	ctx context.Context,
	externalId string,
	attemptID string,
	exitCode int,
	errorMsg string,
) (bool, error) {
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" {
		return false, fmt.Errorf("set run execution result for attempt: attempt id is required")
	}
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
		  AND run_attempt_id = $6::uuid
		  AND ` + runExecutionActiveWhere
	result, err := b.db.ExecContext(ctx, query, externalId, status, exitCode, errorMsg, endedAt, attemptID)
	if err != nil {
		return false, fmt.Errorf("set run execution result for attempt: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows > 0 {
		return true, nil
	}

	run, lookupErr := b.GetRunExecution(ctx, externalId)
	if lookupErr != nil {
		if _, ok := lookupErr.(*types.ErrRunExecutionNotFound); ok {
			// Late retries after retention/cleanup should be harmless.
			return false, nil
		}
		return false, lookupErr
	}
	currentAttemptID := ""
	if run.RunAttemptID != nil {
		currentAttemptID = strings.TrimSpace(*run.RunAttemptID)
	}
	if currentAttemptID != attemptID || run.IsTerminal() {
		// Superseded or duplicate terminal callback.
		return false, nil
	}
	return false, fmt.Errorf("run execution result was not applied (status=%s)", run.Status)
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

// --- Scheduled Tasks ---

func (b *PostgresBackend) CreateScheduledTask(ctx context.Context, st *types.ScheduledTask) error {
	query := `
		INSERT INTO scheduled_task (
			workspace_id, agent_id, cron_expr, timezone, prompt, skill_paths,
			active, next_run_at, token_id, encrypted_token, created_by_member_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING id, external_id, created_at, updated_at
	`
	return b.db.QueryRowContext(ctx, query,
		st.WorkspaceID, st.AgentID, st.CronExpr, st.Timezone, st.Prompt, pq.Array(st.SkillPaths),
		st.Active, st.NextRunAt, st.TokenID, st.EncryptedToken, st.CreatedByMemberID,
	).Scan(&st.ID, &st.ExternalID, &st.CreatedAt, &st.UpdatedAt)
}

func (b *PostgresBackend) GetScheduledTask(ctx context.Context, workspaceID uint, externalID string) (*types.ScheduledTask, error) {
	st := &types.ScheduledTask{}
	var skillPaths pq.StringArray
	query := `
		SELECT id, external_id, workspace_id, agent_id, cron_expr, timezone, prompt, skill_paths,
		       active, next_run_at, last_run_at, token_id, encrypted_token,
		       created_by_member_id, created_at, updated_at
		FROM scheduled_task WHERE external_id = $1 AND workspace_id = $2
	`
	err := b.db.QueryRowContext(ctx, query, externalID, workspaceID).Scan(
		&st.ID, &st.ExternalID, &st.WorkspaceID, &st.AgentID,
		&st.CronExpr, &st.Timezone, &st.Prompt, &skillPaths,
		&st.Active, &st.NextRunAt, &st.LastRunAt, &st.TokenID, &st.EncryptedToken,
		&st.CreatedByMemberID, &st.CreatedAt, &st.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, &types.ErrScheduledTaskNotFound{ExternalID: externalID}
	}
	if err != nil {
		return nil, err
	}
	st.SkillPaths = []string(skillPaths)
	return st, nil
}

func (b *PostgresBackend) ListScheduledTasks(ctx context.Context, workspaceID uint) ([]*types.ScheduledTask, error) {
	query := `
		SELECT id, external_id, workspace_id, agent_id, cron_expr, timezone, prompt, skill_paths,
		       active, next_run_at, last_run_at, token_id, encrypted_token,
		       created_by_member_id, created_at, updated_at
		FROM scheduled_task WHERE workspace_id = $1
		ORDER BY created_at DESC
	`
	rows, err := b.db.QueryContext(ctx, query, workspaceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanScheduledTasks(rows)
}

func (b *PostgresBackend) UpdateScheduledTask(ctx context.Context, st *types.ScheduledTask) error {
	query := `
		UPDATE scheduled_task
		SET cron_expr = $1, timezone = $2, prompt = $3, skill_paths = $4, active = $5,
		    next_run_at = $6, updated_at = CURRENT_TIMESTAMP
		WHERE external_id = $7 AND workspace_id = $8
		RETURNING updated_at
	`
	err := b.db.QueryRowContext(ctx, query,
		st.CronExpr, st.Timezone, st.Prompt, pq.Array(st.SkillPaths), st.Active,
		st.NextRunAt, st.ExternalID, st.WorkspaceID,
	).Scan(&st.UpdatedAt)
	if err == sql.ErrNoRows {
		return &types.ErrScheduledTaskNotFound{ExternalID: st.ExternalID}
	}
	return err
}

func (b *PostgresBackend) DeleteScheduledTask(ctx context.Context, workspaceID uint, externalID string) error {
	result, err := b.db.ExecContext(ctx, `DELETE FROM scheduled_task WHERE external_id = $1 AND workspace_id = $2`, externalID, workspaceID)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return &types.ErrScheduledTaskNotFound{ExternalID: externalID}
	}
	return nil
}

func (b *PostgresBackend) DeleteScheduledTasksByAgent(ctx context.Context, workspaceID uint, agentID string) error {
	_, err := b.db.ExecContext(ctx, `DELETE FROM scheduled_task WHERE workspace_id = $1 AND agent_id = $2`, workspaceID, agentID)
	return err
}

func (b *PostgresBackend) ListDueScheduledTasks(ctx context.Context, now time.Time, limit int) ([]*types.ScheduledTask, error) {
	query := `
		SELECT id, external_id, workspace_id, agent_id, cron_expr, timezone, prompt, skill_paths,
		       active, next_run_at, last_run_at, token_id, encrypted_token,
		       created_by_member_id, created_at, updated_at
		FROM scheduled_task
		WHERE active = TRUE AND next_run_at <= $1
		ORDER BY next_run_at ASC
		LIMIT $2
	`
	rows, err := b.db.QueryContext(ctx, query, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanScheduledTasks(rows)
}

func (b *PostgresBackend) AdvanceScheduledTask(ctx context.Context, id string, oldNextRunAt, newNextRunAt time.Time) (bool, error) {
	query := `
		UPDATE scheduled_task
		SET next_run_at = $1, last_run_at = $2, updated_at = CURRENT_TIMESTAMP
		WHERE id = $3 AND next_run_at = $4
	`
	result, err := b.db.ExecContext(ctx, query, newNextRunAt, time.Now(), id, oldNextRunAt)
	if err != nil {
		return false, fmt.Errorf("advance scheduled task: %w", err)
	}
	n, _ := result.RowsAffected()
	return n > 0, nil
}

func (b *PostgresBackend) RevertScheduledTaskAdvance(ctx context.Context, id string, currentNextRunAt, revertTo time.Time) (bool, error) {
	query := `
		UPDATE scheduled_task
		SET next_run_at = $1, last_run_at = NULL, updated_at = CURRENT_TIMESTAMP
		WHERE id = $2 AND next_run_at = $3
	`
	result, err := b.db.ExecContext(ctx, query, revertTo, id, currentNextRunAt)
	if err != nil {
		return false, fmt.Errorf("revert scheduled task advance: %w", err)
	}
	n, _ := result.RowsAffected()
	return n > 0, nil
}

func scanScheduledTasks(rows *sql.Rows) ([]*types.ScheduledTask, error) {
	var result []*types.ScheduledTask
	for rows.Next() {
		st := &types.ScheduledTask{}
		var skillPaths pq.StringArray
		if err := rows.Scan(
			&st.ID, &st.ExternalID, &st.WorkspaceID, &st.AgentID,
			&st.CronExpr, &st.Timezone, &st.Prompt, &skillPaths,
			&st.Active, &st.NextRunAt, &st.LastRunAt, &st.TokenID, &st.EncryptedToken,
			&st.CreatedByMemberID, &st.CreatedAt, &st.UpdatedAt,
		); err != nil {
			return nil, err
		}
		st.SkillPaths = []string(skillPaths)
		result = append(result, st)
	}
	return result, rows.Err()
}

// ── Agent Stats ─────────────────────────────────────────────────────────────

func (b *PostgresBackend) GetAgentStats(ctx context.Context, workspaceId uint, agentID string) (*types.AgentStats, error) {
	rows, err := b.db.QueryContext(ctx, `
		SELECT state::text, COUNT(*) FROM agent_task
		WHERE workspace_id = $1 AND agent_id = $2
		GROUP BY state`, workspaceId, agentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := &types.AgentStats{ByState: make(map[string]int)}
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		stats.ByState[state] = count
		stats.Total += count
	}
	stats.RunningCount = stats.ByState[string(types.AgentTaskStateRunning)]
	stats.CompletedCount = stats.ByState[string(types.AgentTaskStateDone)]
	stats.FailedCount = stats.ByState[string(types.AgentTaskStateDropped)]

	var avgSec sql.NullFloat64
	_ = b.db.QueryRowContext(ctx, `
		SELECT AVG(EXTRACT(EPOCH FROM (COALESCE(ended_at, updated_at) - COALESCE(started_at, created_at))))
		FROM agent_run
		WHERE workspace_id = $1 AND agent_id = $2 AND status = 'ok'`, workspaceId, agentID).Scan(&avgSec)
	if avgSec.Valid {
		stats.AvgRunSec = &avgSec.Float64
	}

	var qualityScore sql.NullFloat64
	_ = b.db.QueryRowContext(ctx, `
		SELECT quality_score
		FROM agent_profile
		WHERE workspace_id = $1 AND id = $2`, workspaceId, agentID).Scan(&qualityScore)
	if qualityScore.Valid {
		stats.QualityScore = &qualityScore.Float64
	}

	var totalCost sql.NullFloat64
	_ = b.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(cost_usd), 0)
		FROM agent_run
		WHERE workspace_id = $1 AND agent_id = $2`, workspaceId, agentID).Scan(&totalCost)
	if totalCost.Valid {
		stats.TotalCostUSD = totalCost.Float64
	}

	return stats, rows.Err()
}

// ── Task Outputs ────────────────────────────────────────────────────────────

func (b *PostgresBackend) ListTaskOutputs(ctx context.Context, workspaceId uint, taskID string) ([]*types.TaskOutput, error) {
	rows, err := b.db.QueryContext(ctx, `
		SELECT o.id, o.workspace_id, o.task_id, o.run_id, o.agent_id,
		       COALESCE(ap.name, ''), o.output_type, o.title,
		       o.summary, o.uri, o.data_json, o.metadata_json, o.status, o.archived_at, o.created_at
		FROM task_output o
		LEFT JOIN agent_profile ap ON ap.id = o.agent_id
		WHERE o.workspace_id = $1 AND o.task_id = $2
		ORDER BY o.created_at ASC, o.id ASC`, workspaceId, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*types.TaskOutput
	for rows.Next() {
		o, err := scanTaskOutput(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, o)
	}
	return result, rows.Err()
}

func (b *PostgresBackend) ListWorkspaceTaskOutputs(
	ctx context.Context,
	workspaceId uint,
	filter types.TaskOutputListFilter,
) ([]*types.TaskOutput, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 60
	}

	rows, err := b.db.QueryContext(ctx, `
		SELECT o.id, o.workspace_id, o.task_id, o.run_id, o.agent_id,
		       COALESCE(ap.name, ''), o.output_type, o.title,
		       o.summary, o.uri, o.data_json, o.metadata_json, o.status, o.archived_at, o.created_at
		FROM task_output o
		LEFT JOIN agent_profile ap ON ap.id = o.agent_id
		WHERE o.workspace_id = $1
		  AND ($2::text IS NULL OR o.task_id = $2::uuid)
		  AND ($3::text IS NULL OR o.agent_id = $3::uuid)
		  AND ($4::text IS NULL OR o.output_type = $4)
		  AND ($5::boolean IS FALSE OR o.archived_at IS NULL)
		  AND ($7::boolean IS FALSE OR o.agent_id IS NULL)
		ORDER BY o.created_at DESC, o.id DESC
		LIMIT $6`,
		workspaceId,
		nilIfEmpty(filter.TaskID),
		nilIfEmpty(filter.AgentID),
		nilIfEmpty(filter.OutputType),
		filter.ExcludeArchived,
		limit,
		filter.AgentIDIsNull,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*types.TaskOutput
	for rows.Next() {
		output, scanErr := scanTaskOutput(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, output)
	}
	return result, rows.Err()
}

func (b *PostgresBackend) CreateTaskOutput(ctx context.Context, output *types.TaskOutput) error {
	var dataBytes, metaBytes []byte
	var err error

	if dataBytes, err = json.Marshal(output.Data); err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}
	if output.Metadata != nil {
		if metaBytes, err = json.Marshal(output.Metadata); err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}
	}
	if output.Status == "" {
		output.Status = types.TaskOutputStatusActive
	}
	if output.ID != "" {
		err = b.db.QueryRowContext(ctx, `
			INSERT INTO task_output (id, workspace_id, task_id, run_id, agent_id, output_type, title, summary, uri, data_json, metadata_json, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT (id) DO NOTHING
			RETURNING created_at`,
			output.ID, output.WorkspaceID, output.TaskID, nilIfEmpty(output.RunID), nilIfEmpty(output.AgentID),
			output.OutputType, output.Title, output.Summary, nilIfEmpty(output.URI),
			dataBytes, nullableJSONB(metaBytes), output.Status,
		).Scan(&output.CreatedAt)
		if err == sql.ErrNoRows {
			var existingWorkspaceID uint
			var existingTaskID string
			if lookupErr := b.db.QueryRowContext(ctx,
				`SELECT created_at, workspace_id, task_id FROM task_output WHERE id = $1`, output.ID,
			).Scan(&output.CreatedAt, &existingWorkspaceID, &existingTaskID); lookupErr != nil {
				return lookupErr
			}
			if existingWorkspaceID != output.WorkspaceID || existingTaskID != output.TaskID {
				return &types.ErrTaskOutputConflict{
					ID:                  output.ID,
					WorkspaceID:         output.WorkspaceID,
					TaskID:              output.TaskID,
					ExistingWorkspaceID: existingWorkspaceID,
					ExistingTaskID:      existingTaskID,
				}
			}
			return nil
		}
		return err
	}
	return b.db.QueryRowContext(ctx, `
		INSERT INTO task_output (workspace_id, task_id, run_id, agent_id, output_type, title, summary, uri, data_json, metadata_json, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING id, created_at`,
		output.WorkspaceID, output.TaskID, nilIfEmpty(output.RunID), nilIfEmpty(output.AgentID),
		output.OutputType, output.Title, output.Summary, nilIfEmpty(output.URI),
		dataBytes, nullableJSONB(metaBytes), output.Status,
	).Scan(&output.ID, &output.CreatedAt)
}

func (b *PostgresBackend) GetTaskOutput(ctx context.Context, workspaceId uint, outputID string) (*types.TaskOutput, error) {
	row := b.db.QueryRowContext(ctx, `
		SELECT o.id, o.workspace_id, o.task_id, o.run_id, o.agent_id,
		       COALESCE(ap.name, ''), o.output_type, o.title,
		       o.summary, o.uri, o.data_json, o.metadata_json, o.status, o.archived_at, o.created_at
		FROM task_output o
		LEFT JOIN agent_profile ap ON ap.id = o.agent_id
		WHERE o.workspace_id = $1 AND o.id = $2`, workspaceId, outputID)
	o, err := scanTaskOutput(row)
	if err == sql.ErrNoRows {
		return nil, &types.ErrTaskOutputNotFound{ID: outputID}
	}
	return o, err
}

func (b *PostgresBackend) AppendTaskOutputRows(ctx context.Context, workspaceId uint, outputID string, rowsJSON []byte) error {
	res, err := b.db.ExecContext(ctx, `
		UPDATE task_output
		SET data_json = jsonb_set(
			data_json,
			'{rows}',
			COALESCE(data_json->'rows', '[]'::jsonb) || $1::jsonb
		)
		WHERE id = $2 AND workspace_id = $3`, rowsJSON, outputID, workspaceId)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return &types.ErrTaskOutputNotFound{ID: outputID}
	}
	return nil
}

func (b *PostgresBackend) UpdateTaskOutputSummary(ctx context.Context, workspaceId uint, outputID string, summary string) error {
	res, err := b.db.ExecContext(ctx, `
		UPDATE task_output SET summary = $1
		WHERE id = $2 AND workspace_id = $3`, summary, outputID, workspaceId)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return &types.ErrTaskOutputNotFound{ID: outputID}
	}
	return nil
}

func (b *PostgresBackend) UpdateTaskOutputStatus(ctx context.Context, workspaceId uint, outputID string, status string) error {
	res, err := b.db.ExecContext(ctx, `
		UPDATE task_output SET status = $1
		WHERE id = $2 AND workspace_id = $3`, status, outputID, workspaceId)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return &types.ErrTaskOutputNotFound{ID: outputID}
	}
	return nil
}

func (b *PostgresBackend) ArchiveTaskOutput(ctx context.Context, workspaceId uint, outputID string) error {
	res, err := b.db.ExecContext(ctx, `
		UPDATE task_output SET archived_at = CURRENT_TIMESTAMP
		WHERE id = $1 AND workspace_id = $2 AND archived_at IS NULL`, outputID, workspaceId)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return &types.ErrTaskOutputNotFound{ID: outputID}
	}
	return nil
}

func (b *PostgresBackend) ArchiveAllTaskOutputs(ctx context.Context, workspaceId uint) (int64, error) {
	res, err := b.db.ExecContext(ctx, `
		UPDATE task_output SET archived_at = CURRENT_TIMESTAMP
		WHERE workspace_id = $1 AND archived_at IS NULL`, workspaceId)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (b *PostgresBackend) DeleteTaskOutput(ctx context.Context, workspaceId uint, outputID string) error {
	res, err := b.db.ExecContext(ctx, `
		DELETE FROM task_output WHERE id = $1 AND workspace_id = $2`, outputID, workspaceId)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return &types.ErrTaskOutputNotFound{ID: outputID}
	}
	return nil
}

func scanTaskOutput(s scanner) (*types.TaskOutput, error) {
	o := &types.TaskOutput{}
	var runID, agentID sql.NullString
	var summary, uri sql.NullString
	var dataBytes, metaBytes []byte
	if err := s.Scan(&o.ID, &o.WorkspaceID, &o.TaskID, &runID, &agentID,
		&o.AgentName, &o.OutputType, &o.Title, &summary, &uri,
		&dataBytes, &metaBytes, &o.Status, &o.ArchivedAt, &o.CreatedAt); err != nil {
		return nil, err
	}
	if runID.Valid {
		o.RunID = &runID.String
	}
	if agentID.Valid {
		o.AgentID = &agentID.String
	}
	if summary.Valid {
		o.Summary = &summary.String
	}
	if uri.Valid {
		o.URI = &uri.String
	}
	json.Unmarshal(dataBytes, &o.Data)
	json.Unmarshal(metaBytes, &o.Metadata)
	return o, nil
}

func nilIfEmpty(s *string) interface{} {
	if s == nil || *s == "" {
		return nil
	}
	return *s
}

func nullableJSONB(b []byte) interface{} {
	if b == nil || string(b) == "null" {
		return nil
	}
	return b
}

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

func (b *PostgresBackend) CreateView(ctx context.Context, v *types.View) error {
	v.SyncNameDescription()
	defJSON, err := json.Marshal(v.Definition)
	if err != nil {
		return fmt.Errorf("marshal view definition: %w", err)
	}
	return b.db.QueryRowContext(ctx, `
		INSERT INTO workspace_view (workspace_id, name, description, source_draft_id, definition_json)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, created_at, updated_at`,
		v.WorkspaceID, v.Name, v.Description, nullableString(v.SourceDraftID), defJSON,
	).Scan(&v.ID, &v.CreatedAt, &v.UpdatedAt)
}

func (b *PostgresBackend) GetView(ctx context.Context, workspaceID uint, viewID string) (*types.View, error) {
	v := &types.View{}
	var defBytes []byte
	err := b.db.QueryRowContext(ctx, `
		SELECT id, workspace_id, name, description, COALESCE(source_draft_id, ''), definition_json, created_at, updated_at
		FROM workspace_view WHERE id = $1 AND workspace_id = $2`,
		viewID, workspaceID,
	).Scan(&v.ID, &v.WorkspaceID, &v.Name, &v.Description, &v.SourceDraftID, &defBytes, &v.CreatedAt, &v.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("view not found")
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(defBytes, &v.Definition); err != nil {
		return nil, fmt.Errorf("unmarshal view definition: %w", err)
	}
	v.SyncNameDescription()
	return v, nil
}

func (b *PostgresBackend) ListViews(ctx context.Context, workspaceID uint) ([]*types.View, error) {
	rows, err := b.db.QueryContext(ctx, `
		SELECT id, workspace_id, name, description, COALESCE(source_draft_id, ''), definition_json, created_at, updated_at
		FROM workspace_view WHERE workspace_id = $1
		ORDER BY created_at DESC`,
		workspaceID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*types.View
	for rows.Next() {
		v := &types.View{}
		var defBytes []byte
		if err := rows.Scan(&v.ID, &v.WorkspaceID, &v.Name, &v.Description, &v.SourceDraftID, &defBytes, &v.CreatedAt, &v.UpdatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(defBytes, &v.Definition); err != nil {
			return nil, fmt.Errorf("unmarshal view definition: %w", err)
		}
		v.SyncNameDescription()
		result = append(result, v)
	}
	return result, rows.Err()
}

func (b *PostgresBackend) UpdateView(ctx context.Context, v *types.View) error {
	v.SyncNameDescription()
	defJSON, err := json.Marshal(v.Definition)
	if err != nil {
		return fmt.Errorf("marshal view definition: %w", err)
	}
	err = b.db.QueryRowContext(ctx, `
		UPDATE workspace_view
		SET name = $1, description = $2, source_draft_id = $3, definition_json = $4, updated_at = CURRENT_TIMESTAMP
		WHERE id = $5 AND workspace_id = $6
		RETURNING updated_at`,
		v.Name, v.Description, nullableString(v.SourceDraftID), defJSON, v.ID, v.WorkspaceID,
	).Scan(&v.UpdatedAt)
	if err == sql.ErrNoRows {
		return fmt.Errorf("view not found")
	}
	return err
}

func (b *PostgresBackend) DeleteView(ctx context.Context, workspaceID uint, viewID string) error {
	result, err := b.db.ExecContext(ctx,
		`DELETE FROM workspace_view WHERE id = $1 AND workspace_id = $2`,
		viewID, workspaceID,
	)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("view not found")
	}
	return nil
}
