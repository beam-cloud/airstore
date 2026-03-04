package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/types"
)

// Credit account column list for scans.
const creditAccountCols = `id, external_id, workspace_id, balance, created_at, updated_at`

func scanCreditAccount(row interface{ Scan(dest ...any) error }) (*types.CreditAccount, error) {
	a := &types.CreditAccount{}
	err := row.Scan(
		&a.ID,
		&a.ExternalID,
		&a.WorkspaceID,
		&a.Balance,
		&a.CreatedAt,
		&a.UpdatedAt,
	)
	return a, err
}

// Credit ledger column list for scans.
const creditLedgerCols = `id, external_id, account_id, workspace_id, type, amount, balance_after, description, reference_id, reference_type, created_at`

func scanCreditLedgerEntry(row interface{ Scan(dest ...any) error }) (*types.CreditLedgerEntry, error) {
	e := &types.CreditLedgerEntry{}
	err := row.Scan(
		&e.ID,
		&e.ExternalID,
		&e.AccountID,
		&e.WorkspaceID,
		&e.Type,
		&e.Amount,
		&e.BalanceAfter,
		&e.Description,
		&e.ReferenceID,
		&e.ReferenceType,
		&e.CreatedAt,
	)
	return e, err
}

// GetOrCreateCreditAccount returns the credit account for a workspace,
// creating one with a zero balance if none exists.
func (b *PostgresBackend) GetOrCreateCreditAccount(ctx context.Context, workspaceId uint) (*types.CreditAccount, error) {
	query := `
		INSERT INTO credit_account (workspace_id)
		VALUES ($1)
		ON CONFLICT (workspace_id) DO UPDATE SET updated_at = credit_account.updated_at
		RETURNING ` + creditAccountCols

	acct, err := scanCreditAccount(b.db.QueryRowContext(ctx, query, workspaceId))
	if err != nil {
		return nil, fmt.Errorf("failed to get or create credit account: %w", err)
	}
	return acct, nil
}

// GetCreditAccount retrieves the credit account for a workspace.
func (b *PostgresBackend) GetCreditAccount(ctx context.Context, workspaceId uint) (*types.CreditAccount, error) {
	query := `SELECT ` + creditAccountCols + ` FROM credit_account WHERE workspace_id = $1`

	acct, err := scanCreditAccount(b.db.QueryRowContext(ctx, query, workspaceId))
	if err == sql.ErrNoRows {
		return nil, &types.ErrCreditAccountNotFound{WorkspaceID: workspaceId}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get credit account: %w", err)
	}
	return acct, nil
}

// GrantCredits adds credits to a workspace account and records the transaction.
func (b *PostgresBackend) GrantCredits(ctx context.Context, workspaceId uint, amount int64, description string) (*types.CreditLedgerEntry, error) {
	if amount <= 0 {
		return nil, fmt.Errorf("grant amount must be positive, got %d", amount)
	}
	return b.recordTransaction(ctx, workspaceId, types.CreditTransactionTypeGrant, amount, description, nil, nil)
}

// DebitCredits removes credits from a workspace account. Returns ErrInsufficientCredits
// if the balance would go negative.
func (b *PostgresBackend) DebitCredits(ctx context.Context, workspaceId uint, amount int64, description string, refType *string, refID *string) (*types.CreditLedgerEntry, error) {
	if amount <= 0 {
		return nil, fmt.Errorf("debit amount must be positive, got %d", amount)
	}
	// Store as negative in the ledger.
	return b.recordTransaction(ctx, workspaceId, types.CreditTransactionTypeUsage, -amount, description, refType, refID)
}

// AdjustCredits records an admin adjustment (positive or negative).
func (b *PostgresBackend) AdjustCredits(ctx context.Context, workspaceId uint, amount int64, description string) (*types.CreditLedgerEntry, error) {
	if amount == 0 {
		return nil, fmt.Errorf("adjustment amount must be non-zero")
	}
	return b.recordTransaction(ctx, workspaceId, types.CreditTransactionTypeAdjustment, amount, description, nil, nil)
}

// recordTransaction is the single code path for all credit mutations.
// It uses a serializable transaction to atomically update the balance and
// append the ledger entry.
func (b *PostgresBackend) recordTransaction(
	ctx context.Context,
	workspaceId uint,
	txnType types.CreditTransactionType,
	amount int64,
	description string,
	refType *string,
	refID *string,
) (*types.CreditLedgerEntry, error) {
	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// Ensure account exists (upsert).
	var accountID uint
	var currentBalance int64
	err = tx.QueryRowContext(ctx, `
		INSERT INTO credit_account (workspace_id)
		VALUES ($1)
		ON CONFLICT (workspace_id) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
		RETURNING id, balance
	`, workspaceId).Scan(&accountID, &currentBalance)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert credit account: %w", err)
	}

	newBalance := currentBalance + amount
	if newBalance < 0 {
		return nil, &types.ErrInsufficientCredits{
			Required:  -amount,
			Available: currentBalance,
		}
	}

	// Update balance.
	_, err = tx.ExecContext(ctx, `
		UPDATE credit_account SET balance = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2
	`, newBalance, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to update credit balance: %w", err)
	}

	// Append ledger entry.
	entry, err := scanCreditLedgerEntry(tx.QueryRowContext(ctx, `
		INSERT INTO credit_ledger (account_id, workspace_id, type, amount, balance_after, description, reference_id, reference_type)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING `+creditLedgerCols,
		accountID, workspaceId, txnType, amount, newBalance, description, refID, refType,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to insert credit ledger entry: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit credit transaction: %w", err)
	}

	return entry, nil
}

// ListCreditLedger returns ledger entries for a workspace, newest first.
func (b *PostgresBackend) ListCreditLedger(ctx context.Context, workspaceId uint, limit, offset int) ([]*types.CreditLedgerEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	query := `SELECT ` + creditLedgerCols + `
		FROM credit_ledger
		WHERE workspace_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3`

	rows, err := b.db.QueryContext(ctx, query, workspaceId, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list credit ledger: %w", err)
	}
	defer rows.Close()

	var entries []*types.CreditLedgerEntry
	for rows.Next() {
		entry, err := scanCreditLedgerEntry(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan credit ledger entry: %w", err)
		}
		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating credit ledger: %w", err)
	}

	return entries, nil
}
