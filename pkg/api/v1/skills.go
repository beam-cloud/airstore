package apiv1

import (
	"net/http"

	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/skills"
	"github.com/labstack/echo/v4"
)

type SkillsGroup struct {
	g       *echo.Group
	backend repository.BackendRepository
	storage *clients.StorageClient
}

func NewSkillsGroup(g *echo.Group, backend repository.BackendRepository, storage *clients.StorageClient) *SkillsGroup {
	sg := &SkillsGroup{g: g, backend: backend, storage: storage}
	sg.g.GET("", sg.List)
	return sg
}

type SkillInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Path        string `json:"path"`
}

func (sg *SkillsGroup) List(c echo.Context) error {
	ctx := c.Request().Context()

	ws, err := sg.backend.GetWorkspaceByExternalId(ctx, c.Param("workspace_id"))
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}
	if sg.storage == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "storage not configured")
	}

	bucket := sg.storage.WorkspaceBucketName(ws.ExternalId)
	output, err := sg.storage.ListObjects(ctx, bucket, skills.Dir+"/", 1000)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	// Collect unique skill names from SKILL.md keys
	seen := make(map[string]bool)
	var result []SkillInfo

	for _, obj := range output.Contents {
		if obj.Key == nil {
			continue
		}
		name := skills.KeyToName(*obj.Key)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true

		// Load manifest
		content, err := sg.storage.Download(ctx, bucket, skills.ManifestKey(name))
		if err != nil {
			continue
		}
		manifest, err := skills.Parse(content)
		if err != nil {
			continue
		}

		result = append(result, SkillInfo{
			Name:        manifest.Name,
			Description: manifest.Description,
			Path:        skills.NameToPath(name),
		})
	}

	return SuccessResponse(c, result)
}
