package skills

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/beam-cloud/airstore/pkg/types"
)

const managedSourceLegacySkillSuffix = "-writeback"

type ManagedSkillStorage interface {
	WorkspaceBucketName(workspaceExternalId string) string
	Upload(ctx context.Context, bucket, key string, data []byte) error
	ListObjects(ctx context.Context, bucket, prefix string, maxKeys int32) (*s3.ListObjectsV2Output, error)
	Delete(ctx context.Context, bucket, key string) error
}

func ManagedSourceSkillName(integration string) string {
	integration = strings.ToLower(strings.TrimSpace(integration))
	return integration
}

func managedSourceLegacySkillName(integration string) string {
	integration = strings.ToLower(strings.TrimSpace(integration))
	return integration + managedSourceLegacySkillSuffix
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
- Read context from sources/%s/*
- Perform mutations only through filesystem tools at tools/%s

## Recommended workflow

1. Read and analyze the relevant files under sources/%s/ — these contain the actual data (diffs, messages, documents, etc.) you need to work with
2. Discover available commands with tools/%s --help
3. Inspect command params with tools/%s <command> --help
4. Execute writes with tools/%s <command> ...
5. Report the action result and any identifiers returned by the tool

## Hard rules

- Never write directly under sources/
- Always read source files before using tools — sources contain the data you need to analyze
- All external changes must go through tools/%s
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

	// Cleanup legacy managed skill path from earlier naming.
	legacyName := managedSourceLegacySkillName(integration)
	if legacyName != skillName {
		if err := deleteManagedSourceSkillByName(ctx, storage, bucket, legacyName); err != nil {
			return fmt.Errorf("cleanup legacy managed skill %q: %w", legacyName, err)
		}
	}

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
	skillName := ManagedSourceSkillName(integration)
	if err := deleteManagedSourceSkillByName(ctx, storage, bucket, skillName); err != nil {
		return err
	}

	// Also delete legacy path so rename does not leave stale managed skills behind.
	legacyName := managedSourceLegacySkillName(integration)
	if legacyName != skillName {
		if err := deleteManagedSourceSkillByName(ctx, storage, bucket, legacyName); err != nil {
			return err
		}
	}
	return nil
}

func deleteManagedSourceSkillByName(ctx context.Context, storage ManagedSkillStorage, bucket, skillName string) error {
	prefix := Dir + "/" + skillName + "/"
	for {
		out, err := storage.ListObjects(ctx, bucket, prefix, 1000)
		if err != nil {
			return fmt.Errorf("list managed skill objects: %w", err)
		}
		if len(out.Contents) == 0 {
			return nil
		}
		for _, obj := range out.Contents {
			if obj.Key == nil || *obj.Key == "" {
				continue
			}
			if err := storage.Delete(ctx, bucket, *obj.Key); err != nil {
				return fmt.Errorf("delete managed skill object %q: %w", *obj.Key, err)
			}
		}
		if len(out.Contents) < 1000 {
			return nil
		}
	}
}
