package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/lib/pq"
)

const taskBlockerSelect = `
	SELECT id, workspace_id, task_id, run_id, kind, input_kind, status, wait_group_id,
	       payload_json, resolution_json, output_ids_json, revision, created_at, updated_at, resolved_at
	FROM task_blocker
`

func scanTaskBlocker(row scanner) (*types.TaskBlocker, error) {
	blocker := &types.TaskBlocker{}
	var runID sql.NullString
	var inputKind sql.NullString
	var waitGroupID sql.NullString
	var payloadJSON []byte
	var resolutionJSON []byte
	var outputIDsJSON []byte
	var resolvedAt sql.NullTime

	if err := row.Scan(
		&blocker.ID,
		&blocker.WorkspaceID,
		&blocker.TaskID,
		&runID,
		&blocker.Kind,
		&inputKind,
		&blocker.Status,
		&waitGroupID,
		&payloadJSON,
		&resolutionJSON,
		&outputIDsJSON,
		&blocker.Revision,
		&blocker.CreatedAt,
		&blocker.UpdatedAt,
		&resolvedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, &types.ErrTaskBlockerNotFound{}
		}
		return nil, err
	}

	if runID.Valid {
		blocker.RunID = &runID.String
	}
	if inputKind.Valid {
		blocker.InputKind = types.InputKind(strings.TrimSpace(inputKind.String))
	}
	if waitGroupID.Valid {
		blocker.WaitGroupID = &waitGroupID.String
	}
	blocker.PayloadJSON = unmarshalJSONMap(payloadJSON)
	blocker.ResolutionJSON = unmarshalJSONMap(resolutionJSON)
	blocker.OutputIDs = unmarshalStringSlice(outputIDsJSON)
	if resolvedAt.Valid {
		blocker.ResolvedAt = &resolvedAt.Time
	}
	return blocker, nil
}

func marshalStringSlice(values []string) ([]byte, error) {
	if len(values) == 0 {
		values = []string{}
	}
	return json.Marshal(values)
}

func unmarshalStringSlice(data []byte) []string {
	if len(data) == 0 {
		return nil
	}
	var out []string
	if err := json.Unmarshal(data, &out); err != nil {
		return nil
	}
	return out
}

func cloneAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func blockerWaitingSummary(payload map[string]any) *string {
	if len(payload) == 0 {
		return nil
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	summary := string(encoded)
	return &summary
}

func normalizeOptionalString(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func normalizeTaskBlockerSpec(spec *types.TaskBlockerSpec) *types.TaskBlockerSpec {
	if spec == nil {
		return nil
	}
	normalized := &types.TaskBlockerSpec{
		Kind:        spec.Kind,
		InputKind:   spec.InputKind,
		PayloadJSON: cloneAnyMap(spec.PayloadJSON),
	}
	if normalized.Kind == "" {
		normalized.Kind = types.TaskBlockerKindForInputKind(normalized.InputKind)
	}
	if normalized.InputKind == "" {
		switch normalized.Kind {
		case types.TaskBlockerKindApproval:
			normalized.InputKind = types.InputKindApproveReject
		default:
			normalized.InputKind = types.InputKindFreeText
		}
	}
	if spec.WaitGroupID != nil {
		if trimmed := strings.TrimSpace(*spec.WaitGroupID); trimmed != "" {
			normalized.WaitGroupID = &trimmed
		}
	}
	if len(spec.OutputIDs) > 0 {
		seen := make(map[string]struct{}, len(spec.OutputIDs))
		for _, id := range spec.OutputIDs {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			normalized.OutputIDs = append(normalized.OutputIDs, id)
		}
		sort.Strings(normalized.OutputIDs)
	}
	return normalized
}

func taskBlockerMatchesSpec(blocker *types.TaskBlocker, spec *types.TaskBlockerSpec, runID string) bool {
	if blocker == nil || spec == nil || blocker.Status != types.TaskBlockerStatusOpen {
		return false
	}
	if strings.TrimSpace(runID) == "" || blocker.RunID == nil || strings.TrimSpace(*blocker.RunID) != strings.TrimSpace(runID) {
		return false
	}
	if blocker.Kind != spec.Kind || blocker.InputKind != spec.InputKind {
		return false
	}
	currentWaitGroup := ""
	if blocker.WaitGroupID != nil {
		currentWaitGroup = strings.TrimSpace(*blocker.WaitGroupID)
	}
	specWaitGroup := ""
	if spec.WaitGroupID != nil {
		specWaitGroup = strings.TrimSpace(*spec.WaitGroupID)
	}
	return currentWaitGroup == specWaitGroup
}

func bindOutputsToBlockerTx(
	ctx context.Context,
	tx *sql.Tx,
	workspaceID uint,
	blockerID string,
	outputIDs []string,
) error {
	if tx == nil || len(outputIDs) == 0 || strings.TrimSpace(blockerID) == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
		UPDATE task_output
		SET metadata_json = COALESCE(metadata_json, '{}'::jsonb) || jsonb_build_object($3::text, $4::text)
		WHERE workspace_id = $1
		  AND id = ANY($2::uuid[])
	`, workspaceID, pq.Array(outputIDs), types.TaskOutputMetadataBlockerID, blockerID)
	return err
}

func (b *PostgresBackend) attachCurrentBlockers(ctx context.Context, tasks []*types.AgentTask) error {
	taskByBlockerID := make(map[string][]*types.AgentTask)
	blockerIDs := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if task == nil || task.CurrentBlockerID == nil {
			continue
		}
		blockerID := strings.TrimSpace(*task.CurrentBlockerID)
		if blockerID == "" {
			continue
		}
		if _, exists := taskByBlockerID[blockerID]; !exists {
			blockerIDs = append(blockerIDs, blockerID)
		}
		taskByBlockerID[blockerID] = append(taskByBlockerID[blockerID], task)
	}
	if len(blockerIDs) == 0 {
		return nil
	}

	rows, err := b.db.QueryContext(ctx, taskBlockerSelect+`
		WHERE id = ANY($1::uuid[])
	`, pq.Array(blockerIDs))
	if err != nil {
		return fmt.Errorf("list task blockers: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		blocker, scanErr := scanTaskBlocker(rows)
		if scanErr != nil {
			return fmt.Errorf("scan task blocker: %w", scanErr)
		}
		for _, task := range taskByBlockerID[blocker.ID] {
			task.CurrentBlocker = blocker
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate task blockers: %w", err)
	}
	return nil
}

func (b *PostgresBackend) GetTaskBlocker(ctx context.Context, workspaceID uint, blockerID string) (*types.TaskBlocker, error) {
	blocker, err := scanTaskBlocker(b.db.QueryRowContext(ctx, taskBlockerSelect+`
		WHERE workspace_id = $1 AND id = $2
	`, workspaceID, blockerID))
	if err != nil {
		if _, ok := err.(*types.ErrTaskBlockerNotFound); ok {
			return nil, &types.ErrTaskBlockerNotFound{ID: blockerID}
		}
		return nil, fmt.Errorf("get task blocker: %w", err)
	}
	return blocker, nil
}

func (b *PostgresBackend) GetCurrentTaskBlocker(ctx context.Context, workspaceID uint, taskID string) (*types.TaskBlocker, error) {
	blocker, err := scanTaskBlocker(b.db.QueryRowContext(ctx, taskBlockerSelect+`
		WHERE workspace_id = $1
		  AND id = (
		    SELECT current_blocker_id
		    FROM agent_task
		    WHERE workspace_id = $1 AND id = $2
		  )
	`, workspaceID, taskID))
	if err != nil {
		if _, ok := err.(*types.ErrTaskBlockerNotFound); ok {
			return nil, nil
		}
		return nil, fmt.Errorf("get current task blocker: %w", err)
	}
	return blocker, nil
}

func (b *PostgresBackend) OpenTaskBlockerIfCurrentRun(
	ctx context.Context,
	request types.TaskBlockerOpenRequest,
) (bool, *types.TaskBlocker, error) {
	expectedRunID := strings.TrimSpace(request.ExpectedRunID)
	spec := normalizeTaskBlockerSpec(request.Blocker)
	workspaceID := request.WorkspaceID
	taskID := request.TaskID
	if expectedRunID == "" || spec == nil {
		return false, nil, fmt.Errorf("open task blocker requires run_id and blocker")
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return false, nil, fmt.Errorf("begin task blocker tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var currentBlockerID sql.NullString
	var currentTaskState types.AgentTaskState
	if err := tx.QueryRowContext(ctx, `
		SELECT current_blocker_id, state
		FROM agent_task
		WHERE workspace_id = $1
		  AND id = $2
		  AND target_run_id = $3::uuid
		  AND state NOT IN ('done'::agent_task_state, 'dropped'::agent_task_state, 'cancelled'::agent_task_state)
		FOR UPDATE
	`, workspaceID, taskID, expectedRunID).Scan(&currentBlockerID, &currentTaskState); err != nil {
		if err == sql.ErrNoRows {
			return false, nil, nil
		}
		return false, nil, fmt.Errorf("lock task for blocker open: %w", err)
	}

	var current *types.TaskBlocker
	if currentBlockerID.Valid {
		current, err = scanTaskBlocker(tx.QueryRowContext(ctx, taskBlockerSelect+` WHERE id = $1`, currentBlockerID.String))
		if err != nil {
			if _, ok := err.(*types.ErrTaskBlockerNotFound); !ok {
				return false, nil, fmt.Errorf("load current blocker: %w", err)
			}
			current = nil
		}
	}

	var active *types.TaskBlocker
	if current != nil && taskBlockerMatchesSpec(current, spec, expectedRunID) {
		payloadJSON, err := marshalJSONMap(spec.PayloadJSON)
		if err != nil {
			return false, nil, fmt.Errorf("marshal blocker payload: %w", err)
		}
		outputIDsJSON, err := marshalStringSlice(spec.OutputIDs)
		if err != nil {
			return false, nil, fmt.Errorf("marshal blocker output ids: %w", err)
		}
		active, err = scanTaskBlocker(tx.QueryRowContext(ctx, `
			UPDATE task_blocker
			SET payload_json = $2,
			    output_ids_json = $3,
			    updated_at = CURRENT_TIMESTAMP
			WHERE id = $1
			RETURNING id, workspace_id, task_id, run_id, kind, input_kind, status, wait_group_id,
			          payload_json, resolution_json, output_ids_json, revision, created_at, updated_at, resolved_at
		`, current.ID, payloadJSON, outputIDsJSON))
		if err != nil {
			return false, nil, fmt.Errorf("update current blocker: %w", err)
		}
	} else {
		var revision int
		if err := tx.QueryRowContext(ctx, `
			SELECT COALESCE(MAX(revision), 0) + 1
			FROM task_blocker
			WHERE task_id = $1
		`, taskID).Scan(&revision); err != nil {
			return false, nil, fmt.Errorf("next blocker revision: %w", err)
		}

		payloadJSON, err := marshalJSONMap(spec.PayloadJSON)
		if err != nil {
			return false, nil, fmt.Errorf("marshal blocker payload: %w", err)
		}
		outputIDsJSON, err := marshalStringSlice(spec.OutputIDs)
		if err != nil {
			return false, nil, fmt.Errorf("marshal blocker output ids: %w", err)
		}
		active, err = scanTaskBlocker(tx.QueryRowContext(ctx, `
			INSERT INTO task_blocker (
				workspace_id, task_id, run_id, kind, input_kind, status, wait_group_id,
				payload_json, resolution_json, output_ids_json, revision
			) VALUES (
				$1, $2, $3::uuid, $4, $5, $6, $7, $8, '{}'::jsonb, $9, $10
			)
			RETURNING id, workspace_id, task_id, run_id, kind, input_kind, status, wait_group_id,
			          payload_json, resolution_json, output_ids_json, revision, created_at, updated_at, resolved_at
		`,
			workspaceID,
			taskID,
			expectedRunID,
			spec.Kind,
			nullableString(strings.TrimSpace(string(spec.InputKind))),
			types.TaskBlockerStatusOpen,
			nullableStringPtr(spec.WaitGroupID),
			payloadJSON,
			outputIDsJSON,
			revision,
		))
		if err != nil {
			return false, nil, fmt.Errorf("insert task blocker: %w", err)
		}

		if current != nil && current.Status == types.TaskBlockerStatusOpen {
			replacementJSON, jsonErr := marshalJSONMap(map[string]any{
				"reason":                 "superseded",
				"replacement_blocker_id": active.ID,
			})
			if jsonErr != nil {
				return false, nil, fmt.Errorf("marshal blocker supersede payload: %w", jsonErr)
			}
			if _, err := tx.ExecContext(ctx, `
				UPDATE task_blocker
				SET status = $2,
				    resolution_json = $3,
				    resolved_at = CURRENT_TIMESTAMP,
				    updated_at = CURRENT_TIMESTAMP
				WHERE id = $1
			`, current.ID, types.TaskBlockerStatusSuperseded, replacementJSON); err != nil {
				return false, nil, fmt.Errorf("supersede prior blocker: %w", err)
			}
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE agent_task
		SET state = 'waiting'::agent_task_state,
		    input_kind = $4,
		    waiting_summary = $5,
		    current_blocker_id = $6::uuid,
		    updated_at = CURRENT_TIMESTAMP
		WHERE workspace_id = $1
		  AND id = $2
		  AND target_run_id = $3::uuid
		  AND state NOT IN ('done'::agent_task_state, 'dropped'::agent_task_state, 'cancelled'::agent_task_state)
	`, workspaceID, taskID, expectedRunID, nullableString(strings.TrimSpace(string(spec.InputKind))), blockerWaitingSummary(spec.PayloadJSON), active.ID); err != nil {
		return false, nil, fmt.Errorf("project blocker onto task: %w", err)
	}

	if err := bindOutputsToBlockerTx(ctx, tx, workspaceID, active.ID, spec.OutputIDs); err != nil {
		return false, nil, fmt.Errorf("bind outputs to blocker: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, nil, fmt.Errorf("commit task blocker open: %w", err)
	}
	_ = currentTaskState
	return true, active, nil
}

func (b *PostgresBackend) ResolveCurrentTaskBlocker(
	ctx context.Context,
	workspaceID uint,
	taskID string,
	resolution *types.TaskBlockerResolution,
) (*types.TaskBlocker, error) {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin resolve blocker tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var blockerID sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT current_blocker_id
		FROM agent_task
		WHERE workspace_id = $1 AND id = $2
		FOR UPDATE
	`, workspaceID, taskID).Scan(&blockerID); err != nil {
		if err == sql.ErrNoRows {
			return nil, &types.ErrAgentTaskNotFound{ID: taskID}
		}
		return nil, fmt.Errorf("lock task for blocker resolution: %w", err)
	}
	if !blockerID.Valid || strings.TrimSpace(blockerID.String) == "" {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit empty blocker resolution: %w", err)
		}
		return nil, nil
	}

	status := types.TaskBlockerStatusResolved
	if resolution != nil && resolution.Status != "" {
		status = resolution.Status
	}
	resolutionJSON := map[string]any{}
	if resolution != nil {
		resolutionJSON = cloneAnyMap(resolution.ResolutionJSON)
	}
	resolutionBytes, err := marshalJSONMap(resolutionJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal blocker resolution: %w", err)
	}

	blocker, err := scanTaskBlocker(tx.QueryRowContext(ctx, `
		UPDATE task_blocker
		SET status = $2,
		    resolution_json = $3,
		    resolved_at = CURRENT_TIMESTAMP,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
		RETURNING id, workspace_id, task_id, run_id, kind, input_kind, status, wait_group_id,
		          payload_json, resolution_json, output_ids_json, revision, created_at, updated_at, resolved_at
	`, blockerID.String, status, resolutionBytes))
	if err != nil {
		return nil, fmt.Errorf("resolve task blocker: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE agent_task
		SET current_blocker_id = NULL,
		    input_kind = NULL,
		    waiting_summary = NULL,
		    updated_at = CURRENT_TIMESTAMP
		WHERE workspace_id = $1 AND id = $2
	`, workspaceID, taskID); err != nil {
		return nil, fmt.Errorf("clear blocker projection from task: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit blocker resolution: %w", err)
	}
	return blocker, nil
}
