package hooks

import (
	"context"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/skills"
)

// StorageSkillReader reads skill content from S3.
type StorageSkillReader struct {
	storage *clients.StorageClient
	backend repository.BackendRepository
}

func NewStorageSkillReader(storage *clients.StorageClient, backend repository.BackendRepository) *StorageSkillReader {
	return &StorageSkillReader{storage: storage, backend: backend}
}

// ReadSkillContent reads SKILL.md for a skill path like "/skills/email-triage".
func (r *StorageSkillReader) ReadSkillContent(ctx context.Context, workspaceId uint, skillPath string) (string, error) {
	ws, err := r.backend.GetWorkspace(ctx, workspaceId)
	if err != nil {
		return "", fmt.Errorf("get workspace: %w", err)
	}
	if ws == nil {
		return "", fmt.Errorf("workspace not found")
	}

	name := skills.PathToName(skillPath)
	if name == "" {
		return "", fmt.Errorf("invalid skill path: %s", skillPath)
	}

	bucket := r.storage.WorkspaceBucketName(ws.ExternalId)
	content, err := r.storage.Download(ctx, bucket, skills.ManifestKey(name))
	if err != nil {
		return "", fmt.Errorf("download: %w", err)
	}

	return string(content), nil
}
