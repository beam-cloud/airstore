package worker

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	agentsignal "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client"
	signaltypes "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client/types"
)

const (
	maxWakePlannerSkillChars   = 24_000
	maxWakePlannerHandoffChars = 12_000
	maxWakePlannerContextFiles = 4
)

var wakePlannerFilePathRE = regexp.MustCompile(`(?:/workspace/)?[A-Za-z0-9][A-Za-z0-9._/-]*\.(?:json|md|txt|csv|ya?ml)`)

func (w *Worker) classifyFollowUp(
	ctx context.Context,
	agentMsg, lastPrompt, mountSource string,
	env map[string]string,
	bamlEnv map[string]string,
) *types.RunExecutionWakeSignal {
	if agentMsg == "" {
		return nil
	}
	var userMsg *string
	if lastPrompt != "" {
		userMsg = &lastPrompt
	}
	skillContext, handoffContext := buildWakePlannerContext(mountSource, env)
	var skillContextPtr *string
	if skillContext != "" {
		skillContextPtr = &skillContext
	}
	var handoffContextPtr *string
	if handoffContext != "" {
		handoffContextPtr = &handoffContext
	}
	fu, err := agentsignal.ClassifyFollowUp(
		ctx,
		agentMsg,
		userMsg,
		time.Now().UTC().Format(time.RFC3339),
		skillContextPtr,
		handoffContextPtr,
		agentsignal.WithEnv(bamlEnv),
	)
	if err != nil || fu.Intent != signaltypes.FollowUpIntentFOLLOW_UP {
		return nil
	}
	ws := &types.RunExecutionWakeSignal{DelayMinutes: int(fu.Delay_minutes)}
	if fu.Reason != nil {
		ws.Reason = strings.TrimSpace(*fu.Reason)
	}
	if fu.Follow_up_prompt != nil {
		ws.FollowUpPrompt = strings.TrimSpace(*fu.Follow_up_prompt)
	}
	for idx, item := range fu.Wake_agenda {
		agendaItem := &types.TaskWakeAgendaItem{
			Seq:   idx + 1,
			Type:  strings.TrimSpace(item.Type),
			Title: strings.TrimSpace(item.Title),
		}
		if item.Reason != nil {
			agendaItem.Reason = strings.TrimSpace(*item.Reason)
		}
		if agendaItem.Title == "" {
			agendaItem.Title = agendaItem.Reason
		}
		if agendaItem.Title == "" {
			continue
		}
		ws.WakeAgenda = append(ws.WakeAgenda, agendaItem)
	}
	if len(ws.WakeAgenda) == 0 && ws.Reason != "" {
		ws.WakeAgenda = []*types.TaskWakeAgendaItem{{
			Seq:    1,
			Type:   "follow_up",
			Title:  ws.Reason,
			Reason: ws.Reason,
		}}
	}
	if ws.Reason == "" {
		ws.Reason = wakeAgendaSummary(ws.WakeAgenda)
	}
	if ws.FollowUpPrompt == "" {
		ws.FollowUpPrompt = synthesizeWakePrompt(ws.WakeAgenda, ws.Reason)
	}
	if ws.DelayMinutes <= 0 {
		ws.DelayMinutes = 5
	}
	return ws
}

func wakeAgendaSummary(items []*types.TaskWakeAgendaItem) string {
	for _, item := range items {
		if item == nil {
			continue
		}
		if title := strings.TrimSpace(item.Title); title != "" {
			return title
		}
		if reason := strings.TrimSpace(item.Reason); reason != "" {
			return reason
		}
	}
	return ""
}

func synthesizeWakePrompt(items []*types.TaskWakeAgendaItem, reason string) string {
	if len(items) == 0 {
		if strings.TrimSpace(reason) == "" {
			return ""
		}
		return "Resume this task and continue based on the latest context."
	}
	lines := []string{
		"Resume this task and work through the following next-wake agenda in order:",
	}
	for idx, item := range items {
		if item == nil {
			continue
		}
		line := fmt.Sprintf("%d. %s", idx+1, strings.TrimSpace(item.Title))
		if detail := strings.TrimSpace(item.Reason); detail != "" && detail != strings.TrimSpace(item.Title) {
			line += " -- " + detail
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func buildWakePlannerContext(mountSource string, env map[string]string) (string, string) {
	skillContext := readActiveSkillContext(mountSource, env)
	if skillContext == "" {
		return "", ""
	}
	return skillContext, readWakePlannerHandoffContext(mountSource, skillContext)
}

func readActiveSkillContext(mountSource string, env map[string]string) string {
	systemPrompt := strings.TrimSpace(env["AIRSTORE_AGENT_SYSTEM_PROMPT"])
	if systemPrompt == "" {
		return ""
	}
	paths := extractActiveSkillPaths(systemPrompt)
	if len(paths) == 0 || strings.TrimSpace(mountSource) == "" {
		return trimWakePlannerContext(systemPrompt, maxWakePlannerSkillChars)
	}

	blocks := make([]string, 0, len(paths))
	total := 0
	for _, skillPath := range paths {
		if len(blocks) >= maxWakePlannerContextFiles || total >= maxWakePlannerSkillChars {
			break
		}
		content, err := readWakePlannerFile(mountSource, path.Join(skillPath, "SKILL.md"), maxWakePlannerSkillChars-total)
		if err != nil || content == "" {
			continue
		}
		block := fmt.Sprintf("Skill file: %s/SKILL.md\n%s", skillPath, content)
		total += len(block)
		blocks = append(blocks, block)
	}
	if len(blocks) == 0 {
		return trimWakePlannerContext(systemPrompt, maxWakePlannerSkillChars)
	}
	return trimWakePlannerContext(strings.Join(blocks, "\n\n"), maxWakePlannerSkillChars)
}

func readWakePlannerHandoffContext(mountSource, skillContext string) string {
	if strings.TrimSpace(mountSource) == "" || strings.TrimSpace(skillContext) == "" {
		return ""
	}
	blocks := make([]string, 0, maxWakePlannerContextFiles)
	total := 0
	for _, filePath := range extractWakePlannerHandoffPaths(skillContext) {
		if len(blocks) >= maxWakePlannerContextFiles || total >= maxWakePlannerHandoffChars {
			break
		}
		content, err := readWakePlannerFile(mountSource, filePath, maxWakePlannerHandoffChars-total)
		if err != nil || content == "" {
			continue
		}
		block := fmt.Sprintf("Handoff file: %s\n%s", filePath, content)
		total += len(block)
		blocks = append(blocks, block)
	}
	return trimWakePlannerContext(strings.Join(blocks, "\n\n"), maxWakePlannerHandoffChars)
}

func extractActiveSkillPaths(systemPrompt string) []string {
	lines := strings.Split(systemPrompt, "\n")
	seen := make(map[string]struct{}, len(lines))
	paths := make([]string, 0, len(lines))
	for _, line := range lines {
		idx := strings.Index(line, "/workspace/skills/")
		if idx < 0 {
			continue
		}
		fields := strings.Fields(line[idx:])
		if len(fields) == 0 {
			continue
		}
		skillPath := strings.TrimSuffix(strings.TrimSpace(fields[0]), "/SKILL.md")
		if !strings.HasPrefix(skillPath, "/workspace/skills/") {
			continue
		}
		if _, ok := seen[skillPath]; ok {
			continue
		}
		seen[skillPath] = struct{}{}
		paths = append(paths, skillPath)
	}
	return paths
}

func extractWakePlannerHandoffPaths(skillContext string) []string {
	matches := wakePlannerFilePathRE.FindAllString(skillContext, -1)
	seen := make(map[string]struct{}, len(matches))
	paths := make([]string, 0, len(matches))
	for _, match := range matches {
		filePath := normalizeWakePlannerPath(match)
		if filePath == "" || strings.HasPrefix(filePath, "/workspace/skills/") {
			continue
		}
		if _, ok := seen[filePath]; ok {
			continue
		}
		seen[filePath] = struct{}{}
		paths = append(paths, filePath)
	}
	return paths
}

func normalizeWakePlannerPath(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.Trim(raw, "`'\"()[]{}.,:;")
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "/workspace/") {
		return path.Clean(raw)
	}
	if strings.HasPrefix(raw, "/") {
		return ""
	}
	return path.Clean(path.Join("/workspace", raw))
}

func readWakePlannerFile(mountSource, containerPath string, limit int) (string, error) {
	if strings.TrimSpace(mountSource) == "" || strings.TrimSpace(containerPath) == "" || limit <= 0 {
		return "", nil
	}
	hostPath, err := vfsHostPathWithinMount(mountSource, containerPath)
	if err != nil {
		return "", err
	}
	f, err := os.Open(hostPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	data, err := io.ReadAll(io.LimitReader(f, int64(limit+1)))
	if err != nil {
		return "", err
	}
	return trimWakePlannerContext(string(data), limit), nil
}

func trimWakePlannerContext(raw string, limit int) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || limit <= 0 || len(raw) <= limit {
		return raw
	}
	const suffix = "\n...[truncated]"
	if limit <= len(suffix) {
		return raw[:limit]
	}
	return raw[:limit-len(suffix)] + suffix
}
