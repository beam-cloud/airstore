package apiv1

import (
	"net/http"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type MembersGroup struct {
	g       *echo.Group
	backend repository.BackendRepository
}

func NewMembersGroup(g *echo.Group, backend repository.BackendRepository) *MembersGroup {
	mg := &MembersGroup{g: g, backend: backend}
	mg.g.POST("", mg.Create)
	mg.g.GET("", mg.List)
	mg.g.GET("/:member_id", mg.Get)
	mg.g.PUT("/:member_id", mg.Update)
	mg.g.DELETE("/:member_id", mg.Delete)
	return mg
}

type CreateMemberRequest struct {
	Email string           `json:"email"`
	Name  string           `json:"name"`
	Role  types.MemberRole `json:"role"`
}

type UpdateMemberRequest struct {
	Name string           `json:"name"`
	Role types.MemberRole `json:"role"`
}

func (mg *MembersGroup) Create(c echo.Context) error {
	workspaceId := c.Param("workspace_id")

	var req CreateMemberRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}
	if req.Email == "" {
		return ErrorResponse(c, http.StatusBadRequest, "email required")
	}
	if req.Role == "" {
		req.Role = types.RoleMember
	}

	ws, err := mg.backend.GetWorkspaceByExternalId(c.Request().Context(), workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	member, err := mg.backend.CreateMember(c.Request().Context(), ws.Id, req.Email, req.Name, req.Role)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, Response{Success: true, Data: member})
}

func (mg *MembersGroup) List(c echo.Context) error {
	workspaceId := c.Param("workspace_id")

	ws, err := mg.backend.GetWorkspaceByExternalId(c.Request().Context(), workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	members, err := mg.backend.ListMembers(c.Request().Context(), ws.Id)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, Response{Success: true, Data: members})
}

func (mg *MembersGroup) Get(c echo.Context) error {
	memberId := c.Param("member_id")

	member, err := mg.backend.GetMember(c.Request().Context(), memberId)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if member == nil {
		return ErrorResponse(c, http.StatusNotFound, "member not found")
	}

	return c.JSON(http.StatusOK, Response{Success: true, Data: member})
}

func (mg *MembersGroup) Update(c echo.Context) error {
	memberId := c.Param("member_id")

	var req UpdateMemberRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}

	existing, err := mg.backend.GetMember(c.Request().Context(), memberId)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if existing == nil {
		return ErrorResponse(c, http.StatusNotFound, "member not found")
	}

	name := req.Name
	if name == "" {
		name = existing.Name
	}
	role := req.Role
	if role == "" {
		role = existing.Role
	}

	member, err := mg.backend.UpdateMember(c.Request().Context(), memberId, name, role)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, Response{Success: true, Data: member})
}

func (mg *MembersGroup) Delete(c echo.Context) error {
	memberId := c.Param("member_id")

	if err := mg.backend.DeleteMember(c.Request().Context(), memberId); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, Response{Success: true})
}
