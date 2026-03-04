package apiv1

import (
	"net/http"
	"strconv"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type CreditsGroup struct {
	g       *echo.Group
	backend repository.BackendRepository
}

func NewCreditsGroup(g *echo.Group, backend repository.BackendRepository) *CreditsGroup {
	cg := &CreditsGroup{g: g, backend: backend}
	cg.g.GET("/balance", cg.GetBalance)
	cg.g.GET("/ledger", cg.ListLedger)
	cg.g.POST("/grant", cg.Grant)
	cg.g.POST("/debit", cg.Debit)
	return cg
}

// GetBalance returns the current credit balance for the workspace.
func (cg *CreditsGroup) GetBalance(c echo.Context) error {
	ctx := c.Request().Context()

	workspaceId := c.Param("workspace_id")
	ws, err := cg.backend.GetWorkspaceByExternalId(ctx, workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	acct, err := cg.backend.GetCreditAccount(ctx, ws.Id)
	if err != nil {
		if _, ok := err.(*types.ErrCreditAccountNotFound); ok {
			// No account yet — return zero balance.
			return c.JSON(http.StatusOK, Response{
				Success: true,
				Data: map[string]interface{}{
					"workspace_id": ws.ExternalId,
					"balance":      0,
				},
			})
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, Response{
		Success: true,
		Data:    acct,
	})
}

// ListLedger returns credit transactions for the workspace.
func (cg *CreditsGroup) ListLedger(c echo.Context) error {
	ctx := c.Request().Context()

	workspaceId := c.Param("workspace_id")
	ws, err := cg.backend.GetWorkspaceByExternalId(ctx, workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	limit := 50
	offset := 0
	if v := c.QueryParam("limit"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if v := c.QueryParam("offset"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	entries, err := cg.backend.ListCreditLedger(ctx, ws.Id, limit, offset)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, Response{Success: true, Data: entries})
}

type GrantRequest struct {
	Amount      int64  `json:"amount"`
	Description string `json:"description"`
}

// Grant adds credits to a workspace. Requires admin access.
func (cg *CreditsGroup) Grant(c echo.Context) error {
	ctx := c.Request().Context()

	if !auth.IsAdmin(ctx) {
		return ErrorResponse(c, http.StatusForbidden, "admin access required")
	}

	workspaceId := c.Param("workspace_id")
	ws, err := cg.backend.GetWorkspaceByExternalId(ctx, workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	var req GrantRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}
	if req.Amount <= 0 {
		return ErrorResponse(c, http.StatusBadRequest, "amount must be positive")
	}
	if req.Description == "" {
		req.Description = "manual grant"
	}

	entry, err := cg.backend.GrantCredits(ctx, ws.Id, req.Amount, req.Description)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, Response{Success: true, Data: entry})
}

type DebitRequest struct {
	Amount        int64   `json:"amount"`
	Description   string  `json:"description"`
	ReferenceType *string `json:"reference_type,omitempty"`
	ReferenceID   *string `json:"reference_id,omitempty"`
}

// Debit removes credits from a workspace. Requires admin access.
func (cg *CreditsGroup) Debit(c echo.Context) error {
	ctx := c.Request().Context()

	if !auth.IsAdmin(ctx) {
		return ErrorResponse(c, http.StatusForbidden, "admin access required")
	}

	workspaceId := c.Param("workspace_id")
	ws, err := cg.backend.GetWorkspaceByExternalId(ctx, workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	var req DebitRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}
	if req.Amount <= 0 {
		return ErrorResponse(c, http.StatusBadRequest, "amount must be positive")
	}
	if req.Description == "" {
		req.Description = "manual debit"
	}

	entry, err := cg.backend.DebitCredits(ctx, ws.Id, req.Amount, req.Description, req.ReferenceType, req.ReferenceID)
	if err != nil {
		if _, ok := err.(*types.ErrInsufficientCredits); ok {
			return ErrorResponse(c, http.StatusPaymentRequired, err.Error())
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, Response{Success: true, Data: entry})
}
