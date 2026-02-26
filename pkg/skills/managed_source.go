package skills

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/beam-cloud/airstore/pkg/types"
)

const managedSourceSkillSuffix = "-writeback"

type ManagedSkillStorage interface {
	WorkspaceBucketName(workspaceExternalId string) string
	Upload(ctx context.Context, bucket, key string, data []byte) error
	ListObjects(ctx context.Context, bucket, prefix string, maxKeys int32) (*s3.ListObjectsV2Output, error)
	Delete(ctx context.Context, bucket, key string) error
}

func ManagedSourceSkillName(integration string) string {
	integration = strings.ToLower(strings.TrimSpace(integration))
	return integration + managedSourceSkillSuffix
}

func ManagedSourceSkillSource(integration string) string {
	return "airstore://managed/" + ManagedSourceSkillName(integration)
}

func managedSourceSkillDescription(integration string) string {
	return fmt.Sprintf("Managed OAuth write-back skill for %s source operations.", integration)
}

func managedSourceSkillManifest(integration string) []byte {
	skillName := ManagedSourceSkillName(integration)
	description := managedSourceSkillDescription(integration)
	return []byte(fmt.Sprintf(`---
name: %s
description: %s
metadata:
  airstore:
    needs:
      - %s
allowed-tools: %s
---
# %s

This skill is managed by Airstore and auto-created when %s is connected.
Use this skill when you need to read source context and then write back safely.

Source views are always read-only:
- Read context from /sources/%s/*
- Perform mutations only through filesystem tools at /tools/%s

## Recommended workflow

1. Inspect relevant data under /sources/%s/...
2. Discover available commands with /tools/%s --help
3. Inspect command params with /tools/%s <command> --help
4. Execute writes with /tools/%s <command> ...
5. Report the action result and any identifiers returned by the tool

## Hard rules

- Never write directly under /sources
- All external changes must go through /tools/%s
- If a command is denied, explain the missing OAuth permissions and ask the user to reconnect %s
`, skillName, description, integration, integration, skillName, integration, integration, integration, integration, integration, integration, integration, integration, integration))
}

func managedSourceInstallMetadata(integration string) ([]byte, error) {
	skillName := ManagedSourceSkillName(integration)
	meta := map[string]any{
		"name":         skillName,
		"description":  managedSourceSkillDescription(integration),
		"needs":        []string{integration},
		"source":       ManagedSourceSkillSource(integration),
		"managed":      true,
		"integration":  integration,
		"capabilities": []string{string(types.CapabilitySourceRead), string(types.CapabilitySourceWrite)},
	}
	return json.MarshalIndent(meta, "", "  ")
}

func UpsertManagedSourceSkill(ctx context.Context, storage ManagedSkillStorage, workspaceExternalID string, integration string) error {
	if storage == nil {
		return fmt.Errorf("storage not configured")
	}
	if !types.SupportsSourceWrite(types.IntegrationName(integration)) {
		return nil
	}

	bucket := storage.WorkspaceBucketName(workspaceExternalID)
	skillName := ManagedSourceSkillName(integration)
	manifestKey := ManifestKey(skillName)
	metaKey := Dir + "/" + skillName + "/" + InstalledMetaFile

	if err := storage.Upload(ctx, bucket, manifestKey, managedSourceSkillManifest(integration)); err != nil {
		return fmt.Errorf("upload managed skill manifest: %w", err)
	}
	meta, err := managedSourceInstallMetadata(integration)
	if err != nil {
		return fmt.Errorf("build managed install metadata: %w", err)
	}
	if err := storage.Upload(ctx, bucket, metaKey, meta); err != nil {
		return fmt.Errorf("upload managed install metadata: %w", err)
	}
	return nil
}

func DeleteManagedSourceSkill(ctx context.Context, storage ManagedSkillStorage, workspaceExternalID string, integration string) error {
	if storage == nil {
		return fmt.Errorf("storage not configured")
	}
	if !types.SupportsSourceWrite(types.IntegrationName(integration)) {
		return nil
	}

	bucket := storage.WorkspaceBucketName(workspaceExternalID)
	prefix := Dir + "/" + ManagedSourceSkillName(integration) + "/"
	out, err := storage.ListObjects(ctx, bucket, prefix, 1000)
	if err != nil {
		return fmt.Errorf("list managed skill objects: %w", err)
	}
	for _, obj := range out.Contents {
		if obj.Key == nil || *obj.Key == "" {
			continue
		}
		if err := storage.Delete(ctx, bucket, *obj.Key); err != nil {
			return fmt.Errorf("delete managed skill object %q: %w", *obj.Key, err)
		}
	}
	return nil
}
