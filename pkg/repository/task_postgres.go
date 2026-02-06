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

// Task methods on PostgresBackend

// CreateTask creates a new task
func (b *PostgresBackend) CreateTask(ctx context.Context, task *types.Task) error {
	// Convert env map to JSON
	envJSON, err := json.Marshal(task.Env)
	if err != nil {
		return fmt.Errorf("failed to marshal env: %w", err)
	}

	query := `
		INSERT INTO task (workspace_id, created_by_member_id, status, prompt, image, entrypoint, env, hook_id, attempt, max_attempts)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, $7, $8, $9, $10)
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

	err = b.db.QueryRowContext(ctx, query,
		task.WorkspaceId,
		memberIdArg,
		task.Status,
		task.Prompt,
		task.Image,
		pq.Array(task.Entrypoint),
		envJSON,
		task.HookId,
		task.Attempt,
		task.MaxAttempts,
	).Scan(&task.Id, &task.ExternalId, &task.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to create task: %w", err)
	}

	return nil
}

// GetTask retrieves a task by external ID
func (b *PostgresBackend) GetTask(ctx context.Context, externalId string) (*types.Task, error) {
	query := `
		SELECT id, external_id, workspace_id, created_by_member_id, status, prompt, image, entrypoint, env, 
		       exit_code, error, created_at, started_at, finished_at,
		       hook_id, attempt, max_attempts
		FROM task
		WHERE external_id = $1
	`

	return b.scanTask(b.db.QueryRowContext(ctx, query, externalId))
}

// GetTaskById retrieves a task by internal ID
func (b *PostgresBackend) GetTaskById(ctx context.Context, id uint) (*types.Task, error) {
	query := `
		SELECT id, external_id, workspace_id, created_by_member_id, status, prompt, image, entrypoint, env, 
		       exit_code, error, created_at, started_at, finished_at,
		       hook_id, attempt, max_attempts
		FROM task
		WHERE id = $1
	`

	return b.scanTask(b.db.QueryRowContext(ctx, query, id))
}

// scanTask scans a task row into a Task struct
func (b *PostgresBackend) scanTask(row *sql.Row) (*types.Task, error) {
	task := &types.Task{}
	var entrypoint pq.StringArray
	var envJSON []byte
	var createdByMemberId sql.NullInt64
	var prompt sql.NullString
	var exitCode sql.NullInt32
	var errorMsg sql.NullString
	var startedAt, finishedAt sql.NullTime
	var hookId sql.NullInt64

	err := row.Scan(
		&task.Id,
		&task.ExternalId,
		&task.WorkspaceId,
		&createdByMemberId,
		&task.Status,
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
	)
	if err == sql.ErrNoRows {
		return nil, &types.ErrTaskNotFound{}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	if createdByMemberId.Valid {
		memberId := uint(createdByMemberId.Int64)
		task.CreatedByMemberId = &memberId
	}
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

	return task, nil
}

// ListTasks returns all tasks for a workspace (0 = all workspaces)
// Limited to 100 most recent tasks
func (b *PostgresBackend) ListTasks(ctx context.Context, workspaceId uint) ([]*types.Task, error) {
	query := `
		SELECT id, external_id, workspace_id, created_by_member_id, status, prompt, image, entrypoint, env, 
		       exit_code, error, created_at, started_at, finished_at,
		       hook_id, attempt, max_attempts
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

	var tasks []*types.Task
	for rows.Next() {
		task := &types.Task{}
		var entrypoint pq.StringArray
		var envJSON []byte
		var createdByMemberId sql.NullInt64
		var prompt sql.NullString
		var exitCode sql.NullInt32
		var errorMsg sql.NullString
		var startedAt, finishedAt sql.NullTime
		var hookId sql.NullInt64

		if err := rows.Scan(
			&task.Id,
			&task.ExternalId,
			&task.WorkspaceId,
			&createdByMemberId,
			&task.Status,
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
		); err != nil {
			return nil, fmt.Errorf("failed to scan task: %w", err)
		}

		if createdByMemberId.Valid {
			memberId := uint(createdByMemberId.Int64)
			task.CreatedByMemberId = &memberId
		}
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

		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tasks: %w", err)
	}

	return tasks, nil
}

// UpdateTaskStatus updates a task's status
func (b *PostgresBackend) UpdateTaskStatus(ctx context.Context, externalId string, status types.TaskStatus) error {
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
		return &types.ErrTaskNotFound{ExternalId: externalId}
	}

	return nil
}

// SetTaskStarted marks a task as started
func (b *PostgresBackend) SetTaskStarted(ctx context.Context, externalId string) error {
	query := `UPDATE task SET status = $2, started_at = $3 WHERE external_id = $1`

	result, err := b.db.ExecContext(ctx, query, externalId, types.TaskStatusRunning, time.Now())
	if err != nil {
		return fmt.Errorf("failed to set task started: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return &types.ErrTaskNotFound{ExternalId: externalId}
	}

	return nil
}

// SetTaskResult sets the final result of a task
func (b *PostgresBackend) SetTaskResult(ctx context.Context, externalId string, exitCode int, errorMsg string) error {
	status := types.TaskStatusComplete
	if exitCode != 0 || errorMsg != "" {
		status = types.TaskStatusFailed
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
		return &types.ErrTaskNotFound{ExternalId: externalId}
	}

	return nil
}

// MarkTaskRetried sets attempt = max_attempts on a failed task so the retry
// poller no longer picks it up. Called before creating the retry task.
func (b *PostgresBackend) MarkTaskRetried(ctx context.Context, externalId string) error {
	_, err := b.db.ExecContext(ctx,
		`UPDATE task SET attempt = max_attempts WHERE external_id = $1`, externalId)
	return err
}

// DeleteTask removes a task by external ID
func (b *PostgresBackend) DeleteTask(ctx context.Context, externalId string) error {
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
		return &types.ErrTaskNotFound{ExternalId: externalId}
	}

	return nil
}

// CancelTask cancels a running or pending task
func (b *PostgresBackend) CancelTask(ctx context.Context, externalId string) error {
	query := `
		UPDATE task 
		SET status = $2, finished_at = $3
		WHERE external_id = $1 
		  AND status IN ($4, $5, $6)
	`

	result, err := b.db.ExecContext(ctx, query,
		externalId,
		types.TaskStatusCancelled,
		time.Now(),
		types.TaskStatusPending,
		types.TaskStatusScheduled,
		types.TaskStatusRunning,
	)
	if err != nil {
		return fmt.Errorf("failed to cancel task: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		// Check if task exists
		_, err := b.GetTask(ctx, externalId)
		if err != nil {
			return err
		}
		// Task exists but is not in a cancellable state
		return fmt.Errorf("task cannot be cancelled (already finished)")
	}

	return nil
}

// GetRetryableTasks returns failed hook-triggered tasks eligible for retry.
// A task is retryable if: hook_id set, status=failed, attempt < max_attempts,
// and enough time has passed since finished_at (exponential backoff).
func (b *PostgresBackend) GetRetryableTasks(ctx context.Context) ([]*types.Task, error) {
	// Fetch all failed hook tasks that haven't exhausted retries.
	// Backoff filtering is done in Go since the delay depends on attempt number.
	query := `
		SELECT id, external_id, workspace_id, created_by_member_id, status, prompt, image, entrypoint, env,
		       exit_code, error, created_at, started_at, finished_at,
		       hook_id, attempt, max_attempts
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

	var tasks []*types.Task
	for rows.Next() {
		task := &types.Task{}
		var entrypoint pq.StringArray
		var envJSON []byte
		var createdByMemberId sql.NullInt64
		var prompt sql.NullString
		var exitCode sql.NullInt32
		var errorMsg sql.NullString
		var startedAt, finishedAt sql.NullTime
		var hookId sql.NullInt64

		if err := rows.Scan(
			&task.Id, &task.ExternalId, &task.WorkspaceId, &createdByMemberId,
			&task.Status, &prompt, &task.Image, &entrypoint, &envJSON,
			&exitCode, &errorMsg, &task.CreatedAt, &startedAt, &finishedAt,
			&hookId, &task.Attempt, &task.MaxAttempts,
		); err != nil {
			return nil, fmt.Errorf("failed to scan retryable task: %w", err)
		}

		if createdByMemberId.Valid {
			mid := uint(createdByMemberId.Int64)
			task.CreatedByMemberId = &mid
		}
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

		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

// GetStuckHookTasks returns hook-triggered tasks stuck in pending/running longer than timeout.
func (b *PostgresBackend) GetStuckHookTasks(ctx context.Context, timeout time.Duration) ([]*types.Task, error) {
	cutoff := time.Now().Add(-timeout)
	query := `
		SELECT id, external_id, workspace_id, created_by_member_id, status, prompt, image, entrypoint, env,
		       exit_code, error, created_at, started_at, finished_at,
		       hook_id, attempt, max_attempts
		FROM task
		WHERE hook_id IS NOT NULL
		  AND status IN ('pending', 'running', 'scheduled')
		  AND created_at < $1
		LIMIT 50
	`
	return b.scanTaskRows(ctx, query, cutoff)
}

// ListTasksByHook returns all tasks triggered by a specific hook, most recent first.
func (b *PostgresBackend) ListTasksByHook(ctx context.Context, hookId uint) ([]*types.Task, error) {
	query := `
		SELECT id, external_id, workspace_id, created_by_member_id, status, prompt, image, entrypoint, env,
		       exit_code, error, created_at, started_at, finished_at,
		       hook_id, attempt, max_attempts
		FROM task
		WHERE hook_id = $1
		ORDER BY created_at DESC
		LIMIT 50
	`
	return b.scanTaskRows(ctx, query, hookId)
}

// scanTaskRows executes a query and scans multiple task rows.
func (b *PostgresBackend) scanTaskRows(ctx context.Context, query string, args ...any) ([]*types.Task, error) {
	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*types.Task
	for rows.Next() {
		task := &types.Task{}
		var entrypoint pq.StringArray
		var envJSON []byte
		var createdByMemberId sql.NullInt64
		var prompt sql.NullString
		var exitCode sql.NullInt32
		var errorMsg sql.NullString
		var startedAt, finishedAt sql.NullTime
		var hookId sql.NullInt64

		if err := rows.Scan(
			&task.Id, &task.ExternalId, &task.WorkspaceId, &createdByMemberId,
			&task.Status, &prompt, &task.Image, &entrypoint, &envJSON,
			&exitCode, &errorMsg, &task.CreatedAt, &startedAt, &finishedAt,
			&hookId, &task.Attempt, &task.MaxAttempts,
		); err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}

		if createdByMemberId.Valid {
			mid := uint(createdByMemberId.Int64)
			task.CreatedByMemberId = &mid
		}
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
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}
