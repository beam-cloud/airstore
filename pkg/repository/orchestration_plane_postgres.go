package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

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
		task.Priority = "normal"
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
