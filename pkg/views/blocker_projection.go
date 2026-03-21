package views

import (
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	viewprojection "github.com/beam-cloud/airstore/pkg/views/projection"
)

type DetailSurface string

const (
	DetailSurfaceDetails      DetailSurface = "details"
	DetailSurfaceApproval     DetailSurface = "approval"
	DetailSurfaceInput        DetailSurface = "input"
	DetailSurfaceConversation DetailSurface = "conversation"
	DetailSurfaceOutputs      DetailSurface = "outputs"
)

type DetailProjection struct {
	Surface        DetailSurface
	Blocker        *types.ResolvedBlocker
	Outputs        []*types.TaskOutput
	GalleryOutputs []*types.TaskOutput
	ThreadOutputs  []*types.TaskOutput
	HasTask        bool
	HasSubtasks    bool
	IsTaskWaiting  bool
	IsTaskError    bool
	IsTaskActive   bool
}

func ProjectDetail(task *types.AgentTask, outputs []*types.TaskOutput, subtasks []*types.AgentTask) DetailProjection {
	blocker := viewprojection.ProjectBlocker(task, outputs)
	blockerOutputIDs := blockerOutputIDSet(blocker)
	projection := DetailProjection{
		Blocker:     blocker,
		HasTask:     task != nil,
		HasSubtasks: len(subtasks) > 0,
	}
	if task != nil {
		projection.IsTaskWaiting = task.State == types.AgentTaskStateWaiting
		projection.IsTaskError = task.State == types.AgentTaskStateError
		projection.IsTaskActive = !task.State.IsTerminal()
	}
	for _, output := range outputs {
		if output == nil {
			continue
		}
		blockerOwned := isBlockerOwnedOutput(output, blockerOutputIDs)
		if includeDetailOutput(output, blockerOwned) {
			projection.Outputs = append(projection.Outputs, output)
		}
		if includeThreadOutput(output, blockerOwned) {
			projection.ThreadOutputs = append(projection.ThreadOutputs, output)
		}
		if includeGalleryOutput(output, blockerOwned) {
			projection.GalleryOutputs = append(projection.GalleryOutputs, output)
		}
	}
	projection.Surface = projection.resolveSurface()
	return projection
}

func blockerOutputIDSet(blocker *types.ResolvedBlocker) map[string]struct{} {
	if blocker == nil || len(blocker.OutputIDs) == 0 {
		return nil
	}
	ids := make(map[string]struct{}, len(blocker.OutputIDs))
	for _, outputID := range blocker.OutputIDs {
		outputID = strings.TrimSpace(outputID)
		if outputID == "" {
			continue
		}
		ids[outputID] = struct{}{}
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}

func isBlockerOwnedOutput(output *types.TaskOutput, blockerOutputIDs map[string]struct{}) bool {
	if output == nil || len(blockerOutputIDs) == 0 {
		return false
	}
	_, ok := blockerOutputIDs[strings.TrimSpace(output.ID)]
	return ok
}

func includeDetailOutput(output *types.TaskOutput, blockerOwned bool) bool {
	if output == nil || hidesDetailOutput(output.Status) {
		return false
	}
	if blockerOwned {
		return true
	}
	return output.OutputType != types.TaskOutputTypeEmail
}

func includeThreadOutput(output *types.TaskOutput, blockerOwned bool) bool {
	if output == nil || blockerOwned || hidesDetailOutput(output.Status) {
		return false
	}
	return output.OutputType == types.TaskOutputTypeEmail
}

func includeGalleryOutput(output *types.TaskOutput, blockerOwned bool) bool {
	if output == nil || blockerOwned || hidesDetailOutput(output.Status) {
		return false
	}
	return output.OutputType != types.TaskOutputTypeEmail
}

func hidesDetailOutput(status string) bool {
	switch strings.TrimSpace(status) {
	case types.TaskOutputStatusCancelled, types.TaskOutputStatusRejected:
		return true
	default:
		return false
	}
}

func (p DetailProjection) resolveSurface() DetailSurface {
	switch {
	case p.needsApproval():
		return DetailSurfaceApproval
	case p.needsInput():
		return DetailSurfaceInput
	case len(p.ThreadOutputs) > 0:
		return DetailSurfaceConversation
	case len(p.GalleryOutputs) > 0:
		return DetailSurfaceOutputs
	default:
		return DetailSurfaceDetails
	}
}
