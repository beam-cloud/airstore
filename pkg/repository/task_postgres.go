package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/lib/pq"
)

// Run execution methods on PostgresBackend.

// CreateRunExecution creates a new run execution.
func (b *PostgresBackend) CreateRunExecution(ctx context.Context, task *types.RunExecution) error {
	// Convert env map to JSON
	envJSON, err := json.Marshal(task.Env)
	if err != nil {
		return fmt.Errorf("failed to marshal env: %w", err)
	}

	if task.ExecutionPolicy == nil {
		task.ExecutionPolicy = map[string]any{}
	}
	executionPolicyJSON, err := json.Marshal(task.ExecutionPolicy)
	if err != nil {
		return fmt.Errorf("failed to marshal execution policy: %w", err)
	}

	query := `
		INSERT INTO task (
			workspace_id, created_by_member_id, status, type, prompt, image, entrypoint, env,
			hook_id, attempt, max_attempts, run_attempt_id, timeout_ms, exec_host, exec_security,
			exec_ask, runtime_type, workspace_access, network_enabled, execution_policy_json
		)
		VALUES (
			$1, $2, $3, $4, NULLIF($5, ''), $6, $7, $8,
			$9, $10, $11, $12, $13, $14, $15,
			$16, $17, $18, $19, $20
		)
		RETURNING id, external_id, created_at
	`

	// Handle nil member ID
	var memberIdArg interface{}
	if task.CreatedByMemberId != nil {
		memberIdArg = *task.CreatedByMemberId
	} else {
		memberIdArg = nil
	}

	// Defaults
	if task.Attempt == 0 {
		task.Attempt = 1
	}
	if task.MaxAttempts == 0 {
		task.MaxAttempts = 1
	}
	task.NormalizeType()

	err = b.db.QueryRowContext(ctx, query,
		task.WorkspaceId,
		memberIdArg,
		task.Status,
		task.Type,
		task.Prompt,
		task.Image,
		pq.Array(task.Entrypoint),
		envJSON,
		task.HookId,
		task.Attempt,
		task.MaxAttempts,
		task.RunAttemptID,
		task.TimeoutMs,
		task.ExecHost,
		task.ExecSecurity,
		task.ExecAsk,
		task.RuntimeType,
		task.WorkspaceAccess,
		task.NetworkEnabled,
		executionPolicyJSON,
	).Scan(&task.Id, &task.ExternalId, &task.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to create task: %w", err)
	}

	return nil
}

// GetRunExecution retrieves a run execution by external ID.
func (b *PostgresBackend) GetRunExecution(ctx context.Context, externalId string) (*types.RunExecution, error) {
	query := `
		SELECT id, external_id, workspace_id, created_by_member_id, status, type, prompt, image, entrypoint, env, 
		       exit_code, error, created_at, started_at, finished_at,
		       hook_id, attempt, max_attempts,
		       run_attempt_id, timeout_ms, exec_host, exec_security, exec_ask, runtime_type, workspace_access, network_enabled, execution_policy_json
		FROM task
		WHERE external_id = $1
	`

	return b.scanTask(b.db.QueryRowContext(ctx, query, externalId))
}

// GetRunExecutionByID retrieves a run execution by internal ID.
func (b *PostgresBackend) GetRunExecutionByID(ctx context.Context, id uint) (*types.RunExecution, error) {
	query := `
		SELECT id, external_id, workspace_id, created_by_member_id, status, type, prompt, image, entrypoint, env, 
		       exit_code, error, created_at, started_at, finished_at,
		       hook_id, attempt, max_attempts,
		       run_attempt_id, timeout_ms, exec_host, exec_security, exec_ask, runtime_type, workspace_access, network_enabled, execution_policy_json
		FROM task
		WHERE id = $1
	`

	return b.scanTask(b.db.QueryRowContext(ctx, query, id))
}

// scanTask scans a task row into a Task struct
func (b *PostgresBackend) scanTask(row *sql.Row) (*types.RunExecution, error) {
	task := &types.RunExecution{}
	var entrypoint pq.StringArray
	var envJSON []byte
	var createdByMemberId sql.NullInt64
	var taskType sql.NullString
	var prompt sql.NullString
	var exitCode sql.NullInt32
	var errorMsg sql.NullString
	var startedAt, finishedAt sql.NullTime
	var hookId sql.NullInt64
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
		&task.Id,
		&task.ExternalId,
		&task.WorkspaceId,
		&createdByMemberId,
		&task.Status,
		&taskType,
		&prompt,
		&task.Image,
		&entrypoint,
		&envJSON,
		&exitCode,
		&errorMsg,
		&task.CreatedAt,
		&startedAt,
		&finishedAt,
		&hookId,
		&task.Attempt,
		&task.MaxAttempts,
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
		return nil, &types.ErrRunExecutionNotFound{}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	if createdByMemberId.Valid {
		memberId := uint(createdByMemberId.Int64)
		task.CreatedByMemberId = &memberId
	}
	if taskType.Valid {
		task.Type = types.RunExecutionType(taskType.String)
	}
	task.NormalizeType()
	if hookId.Valid {
		hid := uint(hookId.Int64)
		task.HookId = &hid
	}
	if prompt.Valid {
		task.Prompt = prompt.String
	}
	task.Entrypoint = []string(entrypoint)
	if err := json.Unmarshal(envJSON, &task.Env); err != nil {
		task.Env = make(map[string]string)
	}
	if exitCode.Valid {
		ec := int(exitCode.Int32)
		task.ExitCode = &ec
	}
	if errorMsg.Valid {
		task.Error = errorMsg.String
	}
	if startedAt.Valid {
		task.StartedAt = &startedAt.Time
	}
	if finishedAt.Valid {
		task.FinishedAt = &finishedAt.Time
	}
	if runAttemptID.Valid {
		task.RunAttemptID = &runAttemptID.String
	}
	if timeoutMs.Valid {
		v := int(timeoutMs.Int32)
		task.TimeoutMs = &v
	}
	if execHost.Valid {
		task.ExecHost = &execHost.String
	}
	if execSecurity.Valid {
		task.ExecSecurity = &execSecurity.String
	}
	if execAsk.Valid {
		task.ExecAsk = &execAsk.String
	}
	if runtimeType.Valid {
		task.RuntimeType = &runtimeType.String
	}
	if workspaceAccess.Valid {
		task.WorkspaceAccess = &workspaceAccess.String
	}
	if networkEnabled.Valid {
		v := networkEnabled.Bool
		task.NetworkEnabled = &v
	}
	if len(executionPolicyJSON) > 0 {
		_ = json.Unmarshal(executionPolicyJSON, &task.ExecutionPolicy)
		if task.ExecutionPolicy == nil {
			task.ExecutionPolicy = map[string]any{}
		}
	}

	return task, nil
}

// ListRunExecutions returns all run executions for a workspace (0 = all workspaces).
// Limited to 100 most recent entries.
func (b *PostgresBackend) ListRunExecutions(ctx context.Context, workspaceId uint) ([]*types.RunExecution, error) {
	query := `
		SELECT id, external_id, workspace_id, created_by_member_id, status, type, prompt, image, entrypoint, env, 
		       exit_code, error, created_at, started_at, finished_at,
		       hook_id, attempt, max_attempts,
		       run_attempt_id, timeout_ms, exec_host, exec_security, exec_ask, runtime_type, workspace_access, network_enabled, execution_policy_json
		FROM task
		WHERE ($1 = 0 OR workspace_id = $1)
		ORDER BY created_at DESC
		LIMIT 100
	`

	rows, err := b.db.QueryContext(ctx, query, workspaceId)
	if err != nil {
		return nil, fmt.Errorf("failed to list tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*types.RunExecution
	for rows.Next() {
		task := &types.RunExecution{}
		var entrypoint pq.StringArray
		var envJSON []byte
		var createdByMemberId sql.NullInt64
		var taskType sql.NullString
		var prompt sql.NullString
		var exitCode sql.NullInt32
		var errorMsg sql.NullString
		var startedAt, finishedAt sql.NullTime
		var hookId sql.NullInt64
		var runAttemptID sql.NullString
		var timeoutMs sql.NullInt32
		var execHost sql.NullString
		var execSecurity sql.NullString
		var execAsk sql.NullString
		var runtimeType sql.NullString
		var workspaceAccess sql.NullString
		var networkEnabled sql.NullBool
		var executionPolicyJSON []byte

		if err := rows.Scan(
			&task.Id,
			&task.ExternalId,
			&task.WorkspaceId,
			&createdByMemberId,
			&task.Status,
			&taskType,
			&prompt,
			&task.Image,
			&entrypoint,
			&envJSON,
			&exitCode,
			&errorMsg,
			&task.CreatedAt,
			&startedAt,
			&finishedAt,
			&hookId,
			&task.Attempt,
			&task.MaxAttempts,
			&runAttemptID,
			&timeoutMs,
			&execHost,
			&execSecurity,
			&execAsk,
			&runtimeType,
			&workspaceAccess,
			&networkEnabled,
			&executionPolicyJSON,
		); err != nil {
			return nil, fmt.Errorf("failed to scan task: %w", err)
		}

		if createdByMemberId.Valid {
			memberId := uint(createdByMemberId.Int64)
			task.CreatedByMemberId = &memberId
		}
		if taskType.Valid {
			task.Type = types.RunExecutionType(taskType.String)
		}
		task.NormalizeType()
		if hookId.Valid {
			hid := uint(hookId.Int64)
			task.HookId = &hid
		}
		if prompt.Valid {
			task.Prompt = prompt.String
		}
		task.Entrypoint = []string(entrypoint)
		if err := json.Unmarshal(envJSON, &task.Env); err != nil {
			task.Env = make(map[string]string)
		}
		if exitCode.Valid {
			ec := int(exitCode.Int32)
			task.ExitCode = &ec
		}
		if errorMsg.Valid {
			task.Error = errorMsg.String
		}
		if startedAt.Valid {
			task.StartedAt = &startedAt.Time
		}
		if finishedAt.Valid {
			task.FinishedAt = &finishedAt.Time
		}
		if runAttemptID.Valid {
			task.RunAttemptID = &runAttemptID.String
		}
		if timeoutMs.Valid {
			v := int(timeoutMs.Int32)
			task.TimeoutMs = &v
		}
		if execHost.Valid {
			task.ExecHost = &execHost.String
		}
		if execSecurity.Valid {
			task.ExecSecurity = &execSecurity.String
		}
		if execAsk.Valid {
			task.ExecAsk = &execAsk.String
		}
		if runtimeType.Valid {
			task.RuntimeType = &runtimeType.String
		}
		if workspaceAccess.Valid {
			task.WorkspaceAccess = &workspaceAccess.String
		}
		if networkEnabled.Valid {
			v := networkEnabled.Bool
			task.NetworkEnabled = &v
		}
		if len(executionPolicyJSON) > 0 {
			_ = json.Unmarshal(executionPolicyJSON, &task.ExecutionPolicy)
			if task.ExecutionPolicy == nil {
				task.ExecutionPolicy = map[string]any{}
			}
		}

		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tasks: %w", err)
	}

	return tasks, nil
}

// UpdateRunExecutionStatus updates a run execution status.
func (b *PostgresBackend) UpdateRunExecutionStatus(ctx context.Context, externalId string, status types.RunExecutionStatus) error {
	query := `UPDATE task SET status = $2 WHERE external_id = $1`

	result, err := b.db.ExecContext(ctx, query, externalId, status)
	if err != nil {
		return fmt.Errorf("failed to update task status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return &types.ErrRunExecutionNotFound{ExternalId: externalId}
	}

	return nil
}

// SetRunExecutionStarted marks a run execution as started.
func (b *PostgresBackend) SetRunExecutionStarted(ctx context.Context, externalId string) error {
	query := `UPDATE task SET status = $2, started_at = $3 WHERE external_id = $1`

	result, err := b.db.ExecContext(ctx, query, externalId, types.RunExecutionStatusRunning, time.Now())
	if err != nil {
		return fmt.Errorf("failed to set task started: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return &types.ErrRunExecutionNotFound{ExternalId: externalId}
	}

	return nil
}

// SetRunExecutionResult sets the final result of a run execution.
func (b *PostgresBackend) SetRunExecutionResult(ctx context.Context, externalId string, exitCode int, errorMsg string) error {
	status := types.RunExecutionStatusComplete
	if exitCode != 0 || errorMsg != "" {
		status = types.RunExecutionStatusFailed
	}

	query := `
		UPDATE task 
		SET status = $2, exit_code = $3, error = NULLIF($4, ''), finished_at = $5
		WHERE external_id = $1
	`

	result, err := b.db.ExecContext(ctx, query, externalId, status, exitCode, errorMsg, time.Now())
	if err != nil {
		return fmt.Errorf("failed to set task result: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return &types.ErrRunExecutionNotFound{ExternalId: externalId}
	}

	return nil
}

// MarkRunExecutionRetried sets attempt = max_attempts on a failed run execution
// so the retry poller no longer picks it up. Called before creating a retry.
func (b *PostgresBackend) MarkRunExecutionRetried(ctx context.Context, externalId string) error {
	result, err := b.db.ExecContext(ctx,
		`UPDATE task SET attempt = max_attempts WHERE external_id = $1`, externalId)
	if err != nil {
		return fmt.Errorf("mark task retried: %w", err)
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return &types.ErrRunExecutionNotFound{ExternalId: externalId}
	}
	return nil
}

// DeleteRunExecution removes a run execution by external ID.
func (b *PostgresBackend) DeleteRunExecution(ctx context.Context, externalId string) error {
	query := `DELETE FROM task WHERE external_id = $1`

	result, err := b.db.ExecContext(ctx, query, externalId)
	if err != nil {
		return fmt.Errorf("failed to delete task: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return &types.ErrRunExecutionNotFound{ExternalId: externalId}
	}

	return nil
}

// CancelRunExecution cancels a running or pending run execution.
func (b *PostgresBackend) CancelRunExecution(ctx context.Context, externalId string) error {
	query := `
		UPDATE task 
		SET status = $2, finished_at = $3
		WHERE external_id = $1 
		  AND status IN ($4, $5, $6)
	`

	result, err := b.db.ExecContext(ctx, query,
		externalId,
		types.RunExecutionStatusCancelled,
		time.Now(),
		types.RunExecutionStatusPending,
		types.RunExecutionStatusScheduled,
		types.RunExecutionStatusRunning,
	)
	if err != nil {
		return fmt.Errorf("failed to cancel task: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		// Check if run execution exists.
		_, err := b.GetRunExecution(ctx, externalId)
		if err != nil {
			return err
		}
		// Entry exists but is not in a cancellable state.
		return fmt.Errorf("run execution cannot be cancelled (already finished)")
	}

	return nil
}

// GetRetryableRunExecutions returns failed hook-triggered entries eligible for retry.
// An entry is retryable if: hook_id set, status=failed, attempt < max_attempts,
// and enough time has passed since finished_at (exponential backoff).
func (b *PostgresBackend) GetRetryableRunExecutions(ctx context.Context) ([]*types.RunExecution, error) {
	// Fetch all failed hook tasks that haven't exhausted retries.
	// Backoff filtering is done in Go since the delay depends on attempt number.
	query := `
		SELECT id, external_id, workspace_id, created_by_member_id, status, type, prompt, image, entrypoint, env,
		       exit_code, error, created_at, started_at, finished_at,
		       hook_id, attempt, max_attempts,
		       run_attempt_id, timeout_ms, exec_host, exec_security, exec_ask, runtime_type, workspace_access, network_enabled, execution_policy_json
		FROM task
		WHERE hook_id IS NOT NULL
		  AND status = 'failed'
		  AND attempt < max_attempts
		ORDER BY finished_at ASC
		LIMIT 50
	`

	rows, err := b.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query retryable tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*types.RunExecution
	for rows.Next() {
		task := &types.RunExecution{}
		var entrypoint pq.StringArray
		var envJSON []byte
		var createdByMemberId sql.NullInt64
		var taskType sql.NullString
		var prompt sql.NullString
		var exitCode sql.NullInt32
		var errorMsg sql.NullString
		var startedAt, finishedAt sql.NullTime
		var hookId sql.NullInt64
		var runAttemptID sql.NullString
		var timeoutMs sql.NullInt32
		var execHost sql.NullString
		var execSecurity sql.NullString
		var execAsk sql.NullString
		var runtimeType sql.NullString
		var workspaceAccess sql.NullString
		var networkEnabled sql.NullBool
		var executionPolicyJSON []byte

		if err := rows.Scan(
			&task.Id, &task.ExternalId, &task.WorkspaceId, &createdByMemberId,
			&task.Status, &taskType, &prompt, &task.Image, &entrypoint, &envJSON,
			&exitCode, &errorMsg, &task.CreatedAt, &startedAt, &finishedAt,
			&hookId, &task.Attempt, &task.MaxAttempts,
			&runAttemptID, &timeoutMs, &execHost, &execSecurity, &execAsk, &runtimeType, &workspaceAccess, &networkEnabled, &executionPolicyJSON,
		); err != nil {
			return nil, fmt.Errorf("failed to scan retryable task: %w", err)
		}

		if createdByMemberId.Valid {
			mid := uint(createdByMemberId.Int64)
			task.CreatedByMemberId = &mid
		}
		if taskType.Valid {
			task.Type = types.RunExecutionType(taskType.String)
		}
		task.NormalizeType()
		if hookId.Valid {
			hid := uint(hookId.Int64)
			task.HookId = &hid
		}
		if prompt.Valid {
			task.Prompt = prompt.String
		}
		task.Entrypoint = []string(entrypoint)
		json.Unmarshal(envJSON, &task.Env)
		if task.Env == nil {
			task.Env = make(map[string]string)
		}
		if exitCode.Valid {
			ec := int(exitCode.Int32)
			task.ExitCode = &ec
		}
		if errorMsg.Valid {
			task.Error = errorMsg.String
		}
		if startedAt.Valid {
			task.StartedAt = &startedAt.Time
		}
		if finishedAt.Valid {
			task.FinishedAt = &finishedAt.Time
		}
		if runAttemptID.Valid {
			task.RunAttemptID = &runAttemptID.String
		}
		if timeoutMs.Valid {
			v := int(timeoutMs.Int32)
			task.TimeoutMs = &v
		}
		if execHost.Valid {
			task.ExecHost = &execHost.String
		}
		if execSecurity.Valid {
			task.ExecSecurity = &execSecurity.String
		}
		if execAsk.Valid {
			task.ExecAsk = &execAsk.String
		}
		if runtimeType.Valid {
			task.RuntimeType = &runtimeType.String
		}
		if workspaceAccess.Valid {
			task.WorkspaceAccess = &workspaceAccess.String
		}
		if networkEnabled.Valid {
			v := networkEnabled.Bool
			task.NetworkEnabled = &v
		}
		if len(executionPolicyJSON) > 0 {
			_ = json.Unmarshal(executionPolicyJSON, &task.ExecutionPolicy)
			if task.ExecutionPolicy == nil {
				task.ExecutionPolicy = map[string]any{}
			}
		}

		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

// GetStuckHookTasks returns hook-triggered tasks stuck in pending/running longer than timeout.
func (b *PostgresBackend) GetStuckHookRunExecutions(ctx context.Context, timeout time.Duration) ([]*types.RunExecution, error) {
	cutoff := time.Now().Add(-timeout)
	query := `
		SELECT id, external_id, workspace_id, created_by_member_id, status, type, prompt, image, entrypoint, env,
		       exit_code, error, created_at, started_at, finished_at,
		       hook_id, attempt, max_attempts,
		       run_attempt_id, timeout_ms, exec_host, exec_security, exec_ask, runtime_type, workspace_access, network_enabled, execution_policy_json
		FROM task
		WHERE hook_id IS NOT NULL
		  AND status IN ('pending', 'running', 'scheduled')
		  AND created_at < $1
		LIMIT 50
	`
	return b.scanTaskRows(ctx, query, cutoff)
}

// ListTasksByHook returns all tasks triggered by a specific hook, most recent first.
func (b *PostgresBackend) ListRunExecutionsByHook(ctx context.Context, hookId uint) ([]*types.RunExecution, error) {
	query := `
		SELECT id, external_id, workspace_id, created_by_member_id, status, type, prompt, image, entrypoint, env,
		       exit_code, error, created_at, started_at, finished_at,
		       hook_id, attempt, max_attempts,
		       run_attempt_id, timeout_ms, exec_host, exec_security, exec_ask, runtime_type, workspace_access, network_enabled, execution_policy_json
		FROM task
		WHERE hook_id = $1
		ORDER BY created_at DESC
		LIMIT 50
	`
	return b.scanTaskRows(ctx, query, hookId)
}

// scanTaskRows executes a query and scans multiple task rows.
func (b *PostgresBackend) scanTaskRows(ctx context.Context, query string, args ...any) ([]*types.RunExecution, error) {
	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*types.RunExecution
	for rows.Next() {
		task := &types.RunExecution{}
		var entrypoint pq.StringArray
		var envJSON []byte
		var createdByMemberId sql.NullInt64
		var taskType sql.NullString
		var prompt sql.NullString
		var exitCode sql.NullInt32
		var errorMsg sql.NullString
		var startedAt, finishedAt sql.NullTime
		var hookId sql.NullInt64
		var runAttemptID sql.NullString
		var timeoutMs sql.NullInt32
		var execHost sql.NullString
		var execSecurity sql.NullString
		var execAsk sql.NullString
		var runtimeType sql.NullString
		var workspaceAccess sql.NullString
		var networkEnabled sql.NullBool
		var executionPolicyJSON []byte

		if err := rows.Scan(
			&task.Id, &task.ExternalId, &task.WorkspaceId, &createdByMemberId,
			&task.Status, &taskType, &prompt, &task.Image, &entrypoint, &envJSON,
			&exitCode, &errorMsg, &task.CreatedAt, &startedAt, &finishedAt,
			&hookId, &task.Attempt, &task.MaxAttempts,
			&runAttemptID, &timeoutMs, &execHost, &execSecurity, &execAsk, &runtimeType, &workspaceAccess, &networkEnabled, &executionPolicyJSON,
		); err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}

		if createdByMemberId.Valid {
			mid := uint(createdByMemberId.Int64)
			task.CreatedByMemberId = &mid
		}
		if taskType.Valid {
			task.Type = types.RunExecutionType(taskType.String)
		}
		task.NormalizeType()
		if hookId.Valid {
			hid := uint(hookId.Int64)
			task.HookId = &hid
		}
		if prompt.Valid {
			task.Prompt = prompt.String
		}
		task.Entrypoint = []string(entrypoint)
		json.Unmarshal(envJSON, &task.Env)
		if task.Env == nil {
			task.Env = make(map[string]string)
		}
		if exitCode.Valid {
			ec := int(exitCode.Int32)
			task.ExitCode = &ec
		}
		if errorMsg.Valid {
			task.Error = errorMsg.String
		}
		if startedAt.Valid {
			task.StartedAt = &startedAt.Time
		}
		if finishedAt.Valid {
			task.FinishedAt = &finishedAt.Time
		}
		if runAttemptID.Valid {
			task.RunAttemptID = &runAttemptID.String
		}
		if timeoutMs.Valid {
			v := int(timeoutMs.Int32)
			task.TimeoutMs = &v
		}
		if execHost.Valid {
			task.ExecHost = &execHost.String
		}
		if execSecurity.Valid {
			task.ExecSecurity = &execSecurity.String
		}
		if execAsk.Valid {
			task.ExecAsk = &execAsk.String
		}
		if runtimeType.Valid {
			task.RuntimeType = &runtimeType.String
		}
		if workspaceAccess.Valid {
			task.WorkspaceAccess = &workspaceAccess.String
		}
		if networkEnabled.Valid {
			v := networkEnabled.Bool
			task.NetworkEnabled = &v
		}
		if len(executionPolicyJSON) > 0 {
			_ = json.Unmarshal(executionPolicyJSON, &task.ExecutionPolicy)
			if task.ExecutionPolicy == nil {
				task.ExecutionPolicy = map[string]any{}
			}
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}
