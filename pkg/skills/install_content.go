package skills

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

type workspaceSkillStore interface {
	WorkspaceBucketName(workspaceExternalID string) string
	Upload(ctx context.Context, bucket, key string, data []byte) error
	Exists(ctx context.Context, bucket, key string) (bool, error)
}

func ResolveInstallName(requestedName string, content []byte) (*SkillManifest, string, error) {
	manifest, err := Parse(content)
	if err != nil {
		return nil, "", fmt.Errorf("invalid SKILL.md: %w", err)
	}

	requestedName = strings.TrimSpace(requestedName)
	if requestedName == "" {
		return manifest, manifest.Name, nil
	}
	if requestedName != manifest.Name {
		return nil, "", fmt.Errorf(
			"skill name %q does not match SKILL.md frontmatter name %q",
			requestedName,
			manifest.Name,
		)
	}
	return manifest, requestedName, nil
}

func InstallContent(
	ctx context.Context,
	storage workspaceSkillStore,
	workspaceExternalID string,
	requestedName string,
	content []byte,
) (*SkillManifest, string, error) {
	if isNilWorkspaceSkillStore(storage) {
		return nil, "", fmt.Errorf("storage not configured")
	}
	workspaceExternalID = strings.TrimSpace(workspaceExternalID)
	if workspaceExternalID == "" {
		return nil, "", fmt.Errorf("workspace not found")
	}
	if len(content) == 0 {
		return nil, "", fmt.Errorf("skill content is required")
	}

	manifest, skillName, err := ResolveInstallName(requestedName, content)
	if err != nil {
		return nil, "", err
	}

	if err := storage.Upload(
		ctx,
		storage.WorkspaceBucketName(workspaceExternalID),
		ManifestKey(skillName),
		content,
	); err != nil {
		return nil, "", fmt.Errorf("upload skill: %w", err)
	}

	return manifest, skillName, nil
}

func ExistsInWorkspace(ctx context.Context, storage workspaceSkillStore, workspaceExternalID, skillName string) (bool, error) {
	if isNilWorkspaceSkillStore(storage) {
		return false, fmt.Errorf("storage not configured")
	}
	workspaceExternalID = strings.TrimSpace(workspaceExternalID)
	if workspaceExternalID == "" {
		return false, fmt.Errorf("workspace not found")
	}
	skillName = strings.TrimSpace(skillName)
	if skillName == "" {
		return false, fmt.Errorf("skill name is required")
	}
	return storage.Exists(ctx, storage.WorkspaceBucketName(workspaceExternalID), ManifestKey(skillName))
}

func isNilWorkspaceSkillStore(storage workspaceSkillStore) bool {
	if storage == nil {
		return true
	}
	val := reflect.ValueOf(storage)
	switch val.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Interface, reflect.Slice, reflect.Func:
		return val.IsNil()
	default:
		return false
	}
}
