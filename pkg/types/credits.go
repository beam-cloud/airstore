package types

import "time"

// CreditTransactionType represents the type of credit ledger entry.
type CreditTransactionType string

const (
	CreditTransactionTypeGrant      CreditTransactionType = "grant"       // Credits added (purchase, promo, etc.)
	CreditTransactionTypeUsage      CreditTransactionType = "usage"       // Credits consumed by agent runs
	CreditTransactionTypeAdjustment CreditTransactionType = "adjustment"  // Manual admin adjustment
	CreditTransactionTypeExpiry     CreditTransactionType = "expiry"      // Credits expired
)

// CreditAccount holds the current credit balance for a workspace.
// Each workspace has at most one credit account.
type CreditAccount struct {
	ID          uint      `json:"id" db:"id"`
	ExternalID  string    `json:"external_id" db:"external_id"`
	WorkspaceID uint      `json:"workspace_id" db:"workspace_id"`
	Balance     int64     `json:"balance" db:"balance"`         // Current balance in credit units
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
}

// CreditLedgerEntry is an immutable record of a credit transaction.
type CreditLedgerEntry struct {
	ID            uint                  `json:"id" db:"id"`
	ExternalID    string                `json:"external_id" db:"external_id"`
	AccountID     uint                  `json:"account_id" db:"account_id"`
	WorkspaceID   uint                  `json:"workspace_id" db:"workspace_id"`
	Type          CreditTransactionType `json:"type" db:"type"`
	Amount        int64                 `json:"amount" db:"amount"`                 // Positive = credit, negative = debit
	BalanceAfter  int64                 `json:"balance_after" db:"balance_after"`
	Description   string                `json:"description" db:"description"`
	ReferenceID   *string               `json:"reference_id,omitempty" db:"reference_id"`     // Optional link (e.g. run ID)
	ReferenceType *string               `json:"reference_type,omitempty" db:"reference_type"` // e.g. "agent_run"
	CreatedAt     time.Time             `json:"created_at" db:"created_at"`
}

// ErrCreditAccountNotFound is returned when no credit account exists.
type ErrCreditAccountNotFound struct {
	WorkspaceID uint
}

func (e *ErrCreditAccountNotFound) Error() string {
	return "credit account not found"
}

// ErrInsufficientCredits is returned when a debit would exceed the balance.
type ErrInsufficientCredits struct {
	Required  int64
	Available int64
}

func (e *ErrInsufficientCredits) Error() string {
	return "insufficient credits"
}
