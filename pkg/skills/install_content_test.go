package skills

import (
	"context"
	"testing"
)

type stubWorkspaceSkillStore struct {
	objects map[string][]byte
}

func (s *stubWorkspaceSkillStore) WorkspaceBucketName(workspaceExternalID string) string {
	return "ws-" + workspaceExternalID
}

func (s *stubWorkspaceSkillStore) Upload(_ context.Context, bucket, key string, data []byte) error {
	if s.objects == nil {
		s.objects = map[string][]byte{}
	}
	s.objects[bucket+"/"+key] = append([]byte(nil), data...)
	return nil
}

func (s *stubWorkspaceSkillStore) Exists(_ context.Context, bucket, key string) (bool, error) {
	if s.objects == nil {
		return false, nil
	}
	_, ok := s.objects[bucket+"/"+key]
	return ok, nil
}

func TestInstallContentUsesManifestNameWhenRequestOmitsIt(t *testing.T) {
	store := &stubWorkspaceSkillStore{}
	content := []byte(`---
name: meeting-notes
description: Summarize meeting transcripts.
---

# Meeting Notes
`)

	manifest, skillName, err := InstallContent(context.Background(), store, "workspace-1", "", content)
	if err != nil {
		t.Fatalf("InstallContent returned error: %v", err)
	}
	if manifest.Name != "meeting-notes" {
		t.Fatalf("expected manifest name meeting-notes, got %q", manifest.Name)
	}
	if skillName != "meeting-notes" {
		t.Fatalf("expected installed skill name meeting-notes, got %q", skillName)
	}
	if _, ok := store.objects["ws-workspace-1/"+ManifestKey("meeting-notes")]; !ok {
		t.Fatalf("expected uploaded skill content")
	}
}

func TestInstallContentRejectsMismatchedRequestedName(t *testing.T) {
	store := &stubWorkspaceSkillStore{}
	content := []byte(`---
name: meeting-notes
description: Summarize meeting transcripts.
---

# Meeting Notes
`)

	_, _, err := InstallContent(context.Background(), store, "workspace-1", "other-skill", content)
	if err == nil {
		t.Fatal("expected mismatch error, got nil")
	}
}

func TestExistsInWorkspaceChecksManifestKey(t *testing.T) {
	store := &stubWorkspaceSkillStore{
		objects: map[string][]byte{
			"ws-workspace-1/" + ManifestKey("meeting-notes"): []byte("ok"),
		},
	}

	exists, err := ExistsInWorkspace(context.Background(), store, "workspace-1", "meeting-notes")
	if err != nil {
		t.Fatalf("ExistsInWorkspace returned error: %v", err)
	}
	if !exists {
		t.Fatal("expected skill to exist")
	}
}
