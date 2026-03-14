package worker

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
)

// outputCandidate is the canonical worker-side shape for a TaskOutput before
// it is persisted. All output producers normalize into this form so that
// metadata defaults, dedup, and persistence live in one place.
type outputCandidate struct {
	LocalID    string
	OutputType string
	Title      string
	Summary    string
	URI        string
	Path       string
	Data       map[string]any
	Metadata   map[string]any
	Role       string
}

func (c outputCandidate) identityKey() string {
	key := normalizeArtifactToken(anyToTrimmedString(c.Metadata[types.TaskOutputMetadataArtifactKey]))
	title := normalizeArtifactToken(c.Title)
	path := firstNonEmptyTrimmed(c.Path, anyToTrimmedString(c.Data[keyPath]))
	uri := strings.ToLower(firstNonEmptyTrimmed(c.URI, anyToTrimmedString(c.Data[keyURI]), anyToTrimmedString(c.Metadata[keyDeeplink])))

	switch {
	case key != "" && uri != "":
		return "key:" + key + "|uri:" + uri
	case key != "" && path != "":
		return "key:" + key + "|path:" + path
	case key != "" && title != "":
		return "key:" + key + "|title:" + title
	case uri != "":
		return "uri:" + uri
	case path != "":
		return "path:" + path
	case key != "":
		return "key:" + key
	case title != "":
		return "type:" + normalizeArtifactToken(c.OutputType) + "|title:" + title
	default:
		return ""
	}
}

func (c outputCandidate) artifactKey() string {
	return normalizeArtifactToken(anyToTrimmedString(c.Metadata[types.TaskOutputMetadataArtifactKey]))
}

func (c outputCandidate) artifactRole() string {
	role := normalizeArtifactRole(anyToTrimmedString(c.Metadata[types.TaskOutputMetadataArtifactRole]))
	if role != "" {
		return role
	}
	return normalizeArtifactRole(c.Role)
}

func (c outputCandidate) isPrimaryDeliverable() bool {
	return c.artifactRole() == types.TaskOutputArtifactRolePrimary
}

func (c outputCandidate) shouldPersist() bool {
	return c.OutputType != "" && c.Title != ""
}

func (c outputCandidate) normalize() outputCandidate {
	n := c
	n.OutputType = strings.TrimSpace(n.OutputType)
	n.Title = strings.TrimSpace(n.Title)
	n.Summary = strings.TrimSpace(n.Summary)
	n.URI = strings.TrimSpace(n.URI)
	n.Path = strings.TrimSpace(n.Path)
	n.Role = normalizeArtifactRole(n.Role)
	n.Data = cloneAnyMap(c.Data)
	n.Metadata = cloneAnyMap(c.Metadata)

	if n.Path == "" {
		n.Path = anyToTrimmedString(n.Data[keyPath])
	}
	if n.Path != "" && anyToTrimmedString(n.Data[keyPath]) == "" {
		n.Data[keyPath] = n.Path
	}

	if n.URI == "" {
		n.URI = firstNonEmptyTrimmed(
			anyToTrimmedString(n.Data[keyURI]),
			anyToTrimmedString(n.Metadata[keyDeeplink]),
		)
	}
	if n.URI != "" {
		if anyToTrimmedString(n.Data[keyURI]) == "" {
			n.Data[keyURI] = n.URI
		}
		if anyToTrimmedString(n.Metadata[keyDeeplink]) == "" {
			n.Metadata[keyDeeplink] = n.URI
		}
	}

	if n.Summary != "" && anyToTrimmedString(n.Data[keySummary]) == "" {
		n.Data[keySummary] = n.Summary
	}

	n.Metadata = defaultArtifactMetadata(n.Metadata, n.Role)

	return n
}

func publishOutputCandidate(
	ctx context.Context,
	client taskOutputClient,
	ids taskOutputIDs,
	tracker *taskOutputTracker,
	candidate outputCandidate,
) (string, error) {
	if client == nil || ids.taskID == "" {
		return "", nil
	}

	normalized := candidate.normalize()
	if !normalized.shouldPersist() {
		return "", nil
	}
	if tracker != nil && tracker.HasEquivalent(normalized) {
		return "", nil
	}

	req, err := normalized.buildRequest(ids)
	if err != nil {
		return "", err
	}
	serverID, err := client.CreateTaskOutput(ctx, req)
	if err != nil {
		return "", err
	}
	if tracker != nil {
		tracker.Remember(normalized)
	}

	if normalized.Summary != "" {
		if err := client.FinalizeTaskOutput(ctx, &pb.FinalizeTaskOutputRequest{
			WorkspaceId: ids.workspaceID,
			OutputId:    serverID,
			Summary:     normalized.Summary,
		}); err != nil {
			log.Warn().Err(err).Str("task", ids.taskID).Str("output", serverID).Msg("output finalize failed after create")
		}
	}

	return serverID, nil
}

func (c outputCandidate) buildRequest(ids taskOutputIDs) (*pb.CreateTaskOutputRequest, error) {
	req := &pb.CreateTaskOutputRequest{
		WorkspaceId: ids.workspaceID,
		TaskId:      ids.taskID,
		RunId:       ids.runID,
		AgentId:     ids.agentID,
		OutputType:  c.OutputType,
		Title:       c.Title,
	}

	if len(c.Data) > 0 {
		b, err := json.Marshal(c.Data)
		if err != nil {
			return nil, err
		}
		req.DataJson = string(b)
	}
	if len(c.Metadata) > 0 {
		b, err := json.Marshal(c.Metadata)
		if err != nil {
			return nil, err
		}
		req.MetadataJson = string(b)
	}
	if c.URI != "" {
		req.Uri = c.URI
	}

	return req, nil
}
