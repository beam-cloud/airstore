package skills

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type mockManagedSkillStorage struct {
	bucketPrefix string
	objects      map[string][]byte
}

func newMockManagedSkillStorage() *mockManagedSkillStorage {
	return &mockManagedSkillStorage{
		bucketPrefix: "ws",
		objects:      map[string][]byte{},
	}
}

func (m *mockManagedSkillStorage) WorkspaceBucketName(workspaceExternalId string) string {
	return m.bucketPrefix + "-" + workspaceExternalId
}

func (m *mockManagedSkillStorage) Upload(ctx context.Context, bucket, key string, data []byte) error {
	m.objects[bucket+"/"+key] = append([]byte(nil), data...)
	return nil
}

func (m *mockManagedSkillStorage) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int32) (*s3.ListObjectsV2Output, error) {
	out := &s3.ListObjectsV2Output{Contents: []s3types.Object{}}
	for fullKey := range m.objects {
		if !strings.HasPrefix(fullKey, bucket+"/"+prefix) {
			continue
		}
		key := strings.TrimPrefix(fullKey, bucket+"/")
		out.Contents = append(out.Contents, s3types.Object{Key: aws.String(key)})
		if maxKeys > 0 && int32(len(out.Contents)) >= maxKeys {
			break
		}
	}
	return out, nil
}

func (m *mockManagedSkillStorage) Delete(ctx context.Context, bucket, key string) error {
	delete(m.objects, bucket+"/"+key)
	return nil
}

func TestUpsertManagedSourceSkill(t *testing.T) {
	store := newMockManagedSkillStorage()
	err := UpsertManagedSourceSkill(context.Background(), store, "workspace-1", "github")
	if err != nil {
		t.Fatalf("upsert managed skill: %v", err)
	}

	bucket := store.WorkspaceBucketName("workspace-1")
	manifestKey := bucket + "/" + ManifestKey("github")
	manifest, ok := store.objects[manifestKey]
	if !ok {
		t.Fatalf("expected managed skill manifest upload")
	}
	content := string(manifest)
	if !strings.Contains(content, "allowed-tools: github") {
		t.Fatalf("expected manifest to include allowed-tools for github: %s", content)
	}
	if !strings.Contains(content, "needs:") || !strings.Contains(content, "- github") {
		t.Fatalf("expected manifest to include needs metadata: %s", content)
	}

	metaKey := bucket + "/" + Dir + "/github/" + InstalledMetaFile
	metaBytes, ok := store.objects[metaKey]
	if !ok {
		t.Fatalf("expected managed skill installed metadata upload")
	}
	var meta map[string]any
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if managed, _ := meta["managed"].(bool); !managed {
		t.Fatalf("expected managed=true in install metadata: %v", meta)
	}
}

func TestDeleteManagedSourceSkill(t *testing.T) {
	store := newMockManagedSkillStorage()
	bucket := store.WorkspaceBucketName("workspace-1")
	store.objects[bucket+"/"+Dir+"/github/SKILL.md"] = []byte("manifest")
	store.objects[bucket+"/"+Dir+"/github/.installed.json"] = []byte("{}")
	store.objects[bucket+"/"+Dir+"/github-writeback/SKILL.md"] = []byte("legacy manifest")
	store.objects[bucket+"/"+Dir+"/github-writeback/.installed.json"] = []byte("{}")
	store.objects[bucket+"/"+Dir+"/other-skill/SKILL.md"] = []byte("keep")

	if err := DeleteManagedSourceSkill(context.Background(), store, "workspace-1", "github"); err != nil {
		t.Fatalf("delete managed skill: %v", err)
	}
	if _, ok := store.objects[bucket+"/"+Dir+"/github/SKILL.md"]; ok {
		t.Fatalf("expected managed skill manifest to be deleted")
	}
	if _, ok := store.objects[bucket+"/"+Dir+"/github/.installed.json"]; ok {
		t.Fatalf("expected managed skill metadata to be deleted")
	}
	if _, ok := store.objects[bucket+"/"+Dir+"/github-writeback/SKILL.md"]; ok {
		t.Fatalf("expected legacy managed skill manifest to be deleted")
	}
	if _, ok := store.objects[bucket+"/"+Dir+"/github-writeback/.installed.json"]; ok {
		t.Fatalf("expected managed skill metadata to be deleted")
	}
	if _, ok := store.objects[bucket+"/"+Dir+"/other-skill/SKILL.md"]; !ok {
		t.Fatalf("expected non-target skill objects to remain")
	}
}

func TestUpsertManagedSourceSkill_CleansUpLegacyPath(t *testing.T) {
	store := newMockManagedSkillStorage()
	bucket := store.WorkspaceBucketName("workspace-1")
	store.objects[bucket+"/"+Dir+"/github-writeback/SKILL.md"] = []byte("legacy manifest")
	store.objects[bucket+"/"+Dir+"/github-writeback/.installed.json"] = []byte("{}")

	if err := UpsertManagedSourceSkill(context.Background(), store, "workspace-1", "github"); err != nil {
		t.Fatalf("upsert managed skill: %v", err)
	}
	if _, ok := store.objects[bucket+"/"+Dir+"/github-writeback/SKILL.md"]; ok {
		t.Fatalf("expected legacy managed skill manifest to be deleted")
	}
	if _, ok := store.objects[bucket+"/"+Dir+"/github-writeback/.installed.json"]; ok {
		t.Fatalf("expected legacy managed skill metadata to be deleted")
	}
	if _, ok := store.objects[bucket+"/"+Dir+"/github/SKILL.md"]; !ok {
		t.Fatalf("expected new managed skill manifest to exist")
	}
}

func TestDeleteManagedSourceSkill_DeletesAllPages(t *testing.T) {
	store := newMockManagedSkillStorage()
	bucket := store.WorkspaceBucketName("workspace-1")
	for i := 0; i < 1205; i++ {
		key := fmt.Sprintf("%s/github/item-%d.txt", Dir, i)
		store.objects[bucket+"/"+key] = []byte("x")
	}

	if err := DeleteManagedSourceSkill(context.Background(), store, "workspace-1", "github"); err != nil {
		t.Fatalf("delete managed skill: %v", err)
	}

	for fullKey := range store.objects {
		if strings.HasPrefix(fullKey, bucket+"/"+Dir+"/github/") {
			t.Fatalf("expected all managed skill objects deleted, found %s", fullKey)
		}
	}
}
