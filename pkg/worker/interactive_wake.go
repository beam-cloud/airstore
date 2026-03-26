package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	agentsignal "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client"
	signaltypes "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client/types"
	"github.com/rs/zerolog/log"
)

const (
	maxWakePlannerSkillChars   = 24_000
	maxWakePlannerHandoffChars = 12_000
	maxWakePlannerContextFiles = 4
)

var wakePlannerFilePathRE = regexp.MustCompile(`(?:/workspace/)?[A-Za-z0-9][A-Za-z0-9._/-]*\.(?:json|md|txt|csv|ya?ml)`)
var sourceWatchEmailRE = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)

func (w *Worker) buildPostRunPlan(
	ctx context.Context,
	task types.RunExecution,
	tracker *taskOutputTracker,
	agentMsg, lastPrompt, mountSource string,
	env map[string]string,
	bamlEnv map[string]string,
	explicitWake *types.RunExecutionWakeSignal,
) *types.RunExecutionPostRun {
	planned := w.classifyFollowUp(ctx, tracker, agentMsg, lastPrompt, mountSource, env, bamlEnv)
	if planned == nil && explicitWake == nil {
		return nil
	}

	if planned == nil {
		planned = &types.RunExecutionPostRun{}
	}
	if explicitWake != nil {
		planned.WakeSignal = explicitWake
	}
	if planned.WakeSignal != nil {
		planned.SubtaskRequests = w.classifySubtasks(ctx, task, tracker, agentMsg, lastPrompt, planned.WakeSignal, bamlEnv)
	}
	return types.NormalizeRunExecutionPostRun(planned)
}

func (w *Worker) classifyFollowUp(
	ctx context.Context,
	tracker *taskOutputTracker,
	agentMsg, lastPrompt, mountSource string,
	env map[string]string,
	bamlEnv map[string]string,
) *types.RunExecutionPostRun {
	agentMsg = followUpPlanningMessage(agentMsg, tracker)
	if agentMsg == "" {
		return nil
	}
	debugFollowUp := shouldLogFollowUpDecision(agentMsg)
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
	if err != nil {
		if debugFollowUp {
			log.Warn().
				Err(err).
				Str("message_excerpt", trimWakePlannerContext(agentMsg, 600)).
				Msg("follow-up classification failed")
		}
		return nil
	}
	if debugFollowUp {
		log.Info().
			Str("intent", string(fu.Intent)).
			Int("delay_minutes", int(fu.Delay_minutes)).
			Int("source_watch_requests", len(fu.Source_watch_requests)).
			Str("reason", derefString(fu.Reason)).
			Str("follow_up_prompt", trimWakePlannerContext(derefString(fu.Follow_up_prompt), 300)).
			Str("message_excerpt", trimWakePlannerContext(agentMsg, 600)).
			Msg("follow-up classification result")
	}
	if fu.Intent != signaltypes.FollowUpIntentFOLLOW_UP {
		return nil
	}

	result := &types.RunExecutionPostRun{
		SourceWatchRequests: normalizeSourceWatchRequests(fu.Source_watch_requests, tracker, fu.Reason),
	}
	droppedCount := len(fu.Source_watch_requests) - len(result.SourceWatchRequests)
	if debugFollowUp && droppedCount > 0 {
		log.Warn().
			Int("raw", len(fu.Source_watch_requests)).
			Int("survived", len(result.SourceWatchRequests)).
			Int("dropped", droppedCount).
			Msg("source watch normalization dropped hallucinated watches")
	}

	shouldCreateWakeSignal := int(fu.Delay_minutes) > 0 || fu.Reason != nil || fu.Follow_up_prompt != nil || len(fu.Wake_agenda) > 0 || len(result.SourceWatchRequests) == 0
	if !shouldCreateWakeSignal {
		return types.NormalizeRunExecutionPostRun(result)
	}

	ws := &types.RunExecutionWakeSignal{DelayMinutes: int(fu.Delay_minutes)}
	if fu.Reason != nil {
		ws.Reason = strings.TrimSpace(*fu.Reason)
	}
	if fu.Follow_up_prompt != nil {
		ws.FollowUpPrompt = strings.TrimSpace(*fu.Follow_up_prompt)
	}
	if droppedCount > 0 {
		ws.FollowUpPrompt = sanitizeFollowUpPrompt(ws.FollowUpPrompt, result.SourceWatchRequests)
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
	result.WakeSignal = ws
	return types.NormalizeRunExecutionPostRun(result)
}

func normalizeSourceWatchRequests(
	raw []signaltypes.SourceWatchRequest,
	tracker *taskOutputTracker,
	fallbackReason *string,
) []*types.SourceWatchRequest {
	var normalized []*types.SourceWatchRequest
	seen := make(map[string]int)
	trackedFallbacks := deriveTrackedOutputSourceWatchRequests(tracker, fallbackReason)

	add := func(req *types.SourceWatchRequest) {
		req = normalizePlannedSourceWatchRequest(req, fallbackReason)
		if req == nil {
			return
		}
		key := types.SourceWatchRequestMergeKey(req)
		if idx, exists := seen[key]; exists {
			normalized[idx] = types.MergeSourceWatchRequests(normalized[idx], req)
			return
		}
		seen[key] = len(normalized)
		normalized = append(normalized, req)
	}

	for _, item := range raw {
		req := &types.SourceWatchRequest{
			Integration:        item.Integration,
			Reason:             derefString(item.Reason),
			Query:              derefString(item.Query),
			EntityKey:          derefString(item.Entity_key),
			EntityLabel:        derefString(item.Entity_label),
			SourceOutputID:     derefString(item.Source_output_id),
			ThreadID:           derefString(item.Thread_id),
			MessageID:          derefString(item.Message_id),
			IncludeAttachments: item.Include_attachments,
			IncludeInline:      item.Include_inline,
			IncludeMessageBody: item.Include_message_body,
		}
		tracked := bestMatchingTrackedSourceWatchRequest(req, trackedFallbacks)
		if tracked != nil {
			req = types.MergeSourceWatchRequests(req, tracked)
		} else if req.ThreadID != "" || req.MessageID != "" {
			// Classifier returned a specific thread/message ID that doesn't
			// match any tracked output -- likely a hallucination. Drop it.
			continue
		}
		add(req)
	}
	// Tracked outputs are a fallback when the classifier did not materialize a
	// concrete watch. Appending them unconditionally can arm unrelated watches
	// from older draft/sent artifacts in the same task.
	if len(normalized) == 0 {
		for _, req := range trackedFallbacks {
			add(req)
		}
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}


func bestMatchingTrackedSourceWatchRequest(
	req *types.SourceWatchRequest,
	tracked []*types.SourceWatchRequest,
) *types.SourceWatchRequest {
	req = types.CanonicalizeSourceWatchRequest(req)
	if req == nil || len(tracked) == 0 {
		return nil
	}

	bestScore := 0
	var best []*types.SourceWatchRequest
	for _, candidate := range tracked {
		score := trackedSourceWatchMatchScore(req, candidate)
		if score <= 0 {
			continue
		}
		if score > bestScore {
			bestScore = score
			best = []*types.SourceWatchRequest{candidate}
			continue
		}
		if score == bestScore {
			best = append(best, candidate)
		}
	}
	if len(best) == 0 {
		return nil
	}
	return mergeTrackedSourceWatchCandidates(best)
}

func trackedSourceWatchMatchScore(req, candidate *types.SourceWatchRequest) int {
	candidate = types.CanonicalizeSourceWatchRequest(candidate)
	if req == nil || candidate == nil {
		return 0
	}
	if !strings.EqualFold(req.Integration, candidate.Integration) {
		return 0
	}

	score := 0
	if req.SourceOutputID != "" && req.SourceOutputID == candidate.SourceOutputID {
		score += 100
	}
	if req.ThreadID != "" && req.ThreadID == candidate.ThreadID {
		score += 90
	}
	if req.MessageID != "" && req.MessageID == candidate.MessageID {
		score += 80
	}
	if req.EntityKey != "" && req.EntityKey == candidate.EntityKey {
		score += 70
	}
	if trackedSourceWatchEmailMatches(req, candidate) {
		score += 60
	}
	if trackedSourceWatchSubjectMatches(req, candidate) {
		score += 25
	}
	if req.EntityLabel != "" && req.EntityLabel == candidate.EntityLabel {
		score += 20
	}
	if req.Query != "" && req.Query == candidate.Query {
		score += 15
	}
	return score
}

func mergeTrackedSourceWatchCandidates(candidates []*types.SourceWatchRequest) *types.SourceWatchRequest {
	if len(candidates) == 0 {
		return nil
	}
	merged := types.CanonicalizeSourceWatchRequest(candidates[0])
	if merged == nil {
		return nil
	}
	threadID := strings.TrimSpace(merged.ThreadID)
	sourceOutputID := strings.TrimSpace(merged.SourceOutputID)
	for _, candidate := range candidates[1:] {
		candidate = types.CanonicalizeSourceWatchRequest(candidate)
		if candidate == nil {
			return nil
		}
		if !strings.EqualFold(candidate.Integration, merged.Integration) {
			return nil
		}
		if threadID != "" {
			if strings.TrimSpace(candidate.ThreadID) != threadID {
				return nil
			}
		} else if sourceOutputID != "" {
			if strings.TrimSpace(candidate.SourceOutputID) != sourceOutputID {
				return nil
			}
		} else {
			return nil
		}
		merged = types.MergeSourceWatchRequests(merged, candidate)
	}
	if threadID == "" {
		return merged
	}
	if !trackedSourceWatchFieldMatches(candidates, func(req *types.SourceWatchRequest) string { return req.MessageID }) {
		merged.MessageID = ""
	}
	if !trackedSourceWatchFieldMatches(candidates, func(req *types.SourceWatchRequest) string { return req.SourceOutputID }) {
		merged.SourceOutputID = ""
	}
	return types.NormalizeSourceWatchRequest(merged)
}

func trackedSourceWatchFieldMatches(candidates []*types.SourceWatchRequest, value func(*types.SourceWatchRequest) string) bool {
	if len(candidates) <= 1 {
		return true
	}
	first := strings.TrimSpace(value(candidates[0]))
	for _, candidate := range candidates[1:] {
		if strings.TrimSpace(value(candidate)) != first {
			return false
		}
	}
	return first != ""
}

func trackedSourceWatchEmailMatches(req, candidate *types.SourceWatchRequest) bool {
	recipients := sourceWatchEmails(candidate.EntityKey, candidate.EntityLabel, candidate.Query)
	if len(recipients) == 0 {
		return false
	}
	for _, email := range sourceWatchEmails(req.EntityKey, req.EntityLabel, req.Query) {
		for _, recipient := range recipients {
			if email == recipient {
				return true
			}
		}
	}
	return false
}

func trackedSourceWatchSubjectMatches(req, candidate *types.SourceWatchRequest) bool {
	subject := normalizeArtifactToken(firstNonEmptyTrimmed(candidate.EntityLabel))
	if subject == "" {
		return false
	}
	for _, value := range []string{req.EntityLabel, req.Query} {
		if strings.Contains(normalizeArtifactToken(value), subject) {
			return true
		}
	}
	return false
}

func sourceWatchEmails(values ...string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if email := normalizeFanOutEmailAddress(value); email != "" {
			if _, exists := seen[email]; !exists {
				seen[email] = struct{}{}
				out = append(out, email)
			}
		}
		for _, match := range sourceWatchEmailRE.FindAllString(value, -1) {
			if email := normalizeFanOutEmailAddress(match); email != "" {
				if _, exists := seen[email]; !exists {
					seen[email] = struct{}{}
					out = append(out, email)
				}
			}
		}
	}
	return out
}

func deriveTrackedOutputSourceWatchRequests(
	tracker *taskOutputTracker,
	fallbackReason *string,
) []*types.SourceWatchRequest {
	if tracker == nil {
		return nil
	}
	summaries := tracker.TrackedOutputSummaries()
	if len(summaries) == 0 {
		return nil
	}
	out := make([]*types.SourceWatchRequest, 0, len(summaries))
	for _, summary := range summaries {
		if !strings.EqualFold(strings.TrimSpace(summary.OutputType), types.TaskOutputTypeEmail) &&
			!strings.Contains(strings.ToLower(strings.TrimSpace(summary.ArtifactKey)), "email") {
			continue
		}
		threadID := strings.TrimSpace(summary.ThreadID)
		query := buildGmailWatchFallbackQuery(summary)
		if threadID == "" && query == "" {
			continue
		}
		if threadID == "" {
			log.Warn().
				Str("output_id", strings.TrimSpace(summary.OutputID)).
				Str("entity_key", strings.TrimSpace(summary.EntityKey)).
				Str("subject", strings.TrimSpace(summary.Subject)).
				Msg("email tracked output has no thread_id — follow-up watch will rely on text query fallback")
		}
		out = append(out, &types.SourceWatchRequest{
			Integration:        "gmail",
			Reason:             derefString(fallbackReason),
			Query:              query,
			EventTypes:         []string{"fs.create"},
			EntityKey:          strings.TrimSpace(summary.EntityKey),
			EntityLabel:        firstNonEmptyTrimmed(summary.Subject, summary.Title, summary.EntityKey),
			SourceOutputID:     strings.TrimSpace(summary.OutputID),
			ThreadID:           threadID,
			MessageID:          strings.TrimSpace(summary.MessageID),
			IncludeAttachments: true,
			IncludeMessageBody: true,
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func buildGmailWatchFallbackQuery(summary trackedOutputSummary) string {
	parts := make([]string, 0, 3)
	if recipient := strings.TrimSpace(summary.Recipient); recipient != "" {
		parts = append(parts, fmt.Sprintf("from:%q", recipient))
	}
	if subject := strings.TrimSpace(summary.Subject); subject != "" {
		parts = append(parts, fmt.Sprintf("subject:%q", subject))
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

func normalizePlannedSourceWatchRequest(req *types.SourceWatchRequest, fallbackReason *string) *types.SourceWatchRequest {
	if req == nil {
		return nil
	}
	normalized := types.CanonicalizeSourceWatchRequest(req)
	if normalized == nil {
		return nil
	}
	normalized.Reason = firstNonEmptyTrimmed(normalized.Reason, derefString(fallbackReason))
	if strings.EqualFold(normalized.Integration, string(types.SourceGmail)) &&
		(normalized.ThreadID != "" || normalized.MessageID != "") {
		normalized.IncludeAttachments = true
		normalized.IncludeMessageBody = true
	}
	return types.NormalizeSourceWatchRequest(normalized)
}

var threadIDLikeRE = regexp.MustCompile(`[0-9a-fA-F]{12,}`)

// sanitizeFollowUpPrompt strips references to thread IDs that are NOT in the
// surviving source watch requests. This prevents hallucinated thread references
// from the BAML classifier from leaking into the agent's wake prompt.
func sanitizeFollowUpPrompt(prompt string, surviving []*types.SourceWatchRequest) string {
	if strings.TrimSpace(prompt) == "" || len(surviving) == 0 {
		return prompt
	}
	validThreadIDs := make(map[string]struct{}, len(surviving))
	for _, req := range surviving {
		if tid := strings.TrimSpace(req.ThreadID); tid != "" {
			validThreadIDs[tid] = struct{}{}
		}
	}
	if len(validThreadIDs) == 0 {
		return prompt
	}
	lines := strings.Split(prompt, "\n")
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		ids := threadIDLikeRE.FindAllString(line, -1)
		if len(ids) == 0 {
			kept = append(kept, line)
			continue
		}
		hasValid := false
		hasInvalid := false
		for _, id := range ids {
			if _, ok := validThreadIDs[id]; ok {
				hasValid = true
			} else {
				hasInvalid = true
			}
		}
		if hasInvalid && !hasValid {
			continue
		}
		kept = append(kept, line)
	}
	result := strings.TrimSpace(strings.Join(kept, "\n"))
	if result == "" {
		return rebuildFollowUpPromptFromWatches(surviving)
	}
	return result
}

func rebuildFollowUpPromptFromWatches(watches []*types.SourceWatchRequest) string {
	if len(watches) == 0 {
		return ""
	}
	lines := []string{"Resume this task and check the following watched sources for updates:"}
	for _, w := range watches {
		label := firstNonEmptyTrimmed(w.EntityLabel, w.EntityKey)
		if w.ThreadID != "" {
			lines = append(lines, fmt.Sprintf("- %s thread %s: check for new replies and continue the follow-up", w.Integration, w.ThreadID))
		} else if label != "" {
			lines = append(lines, fmt.Sprintf("- %s: %s", w.Integration, label))
		}
	}
	return strings.Join(lines, "\n")
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
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

func (w *Worker) classifySubtasks(
	ctx context.Context,
	task types.RunExecution,
	tracker *taskOutputTracker,
	agentMsg, userMsg string,
	wakeSignal *types.RunExecutionWakeSignal,
	bamlEnv map[string]string,
) []*types.SubtaskRequest {
	agentMsg = followUpPlanningMessage(agentMsg, tracker)
	if agentMsg == "" {
		return nil
	}
	summaries := tracker.TrackedOutputSummaries()
	if !shouldAttemptFanOut(task, summaries) {
		return nil
	}

	type outputEntry struct {
		ID       string `json:"id"`
		Identity string `json:"identity"`
		Entity   string `json:"entity,omitempty"`
	}
	entries := make([]outputEntry, len(summaries))
	for i, s := range summaries {
		entries[i] = outputEntry{ID: s.OutputID, Identity: s.Identity, Entity: s.EntityKey}
	}
	outputsJSON, err := json.Marshal(entries)
	if err != nil {
		return nil
	}

	fo, err := agentsignal.ClassifyFanOut(
		ctx,
		string(outputsJSON),
		strings.TrimSpace(agentMsg),
		fanOutPlannerPrompt(wakeSignal, userMsg),
		time.Now().UTC().Format(time.RFC3339),
		agentsignal.WithEnv(bamlEnv),
	)
	if err != nil {
		log.Warn().Err(err).Msg("subtask classification failed")
		return nil
	}
	if fo.Intent != signaltypes.FanOutIntentFAN_OUT || len(fo.Specs) == 0 {
		return nil
	}

	reqs := make([]*types.SubtaskRequest, 0, len(fo.Specs))
	for _, s := range fo.Specs {
		prompt := strings.TrimSpace(s.Prompt)
		if prompt == "" {
			continue
		}
		reqs = append(reqs, &types.SubtaskRequest{
			SourceOutputID:   s.Source_output_id,
			EntityLabel:      strings.TrimSpace(s.Entity_label),
			Prompt:           prompt,
			WakeDelayMinutes: normalizeSubtaskWakeDelayMinutes(wakeSignal, int(s.Wake_delay_minutes)),
		})
	}
	if len(reqs) == 0 {
		return nil
	}
	log.Info().Int("count", len(reqs)).Msg("subtask requests detected")
	return reqs
}

func fanOutPlannerPrompt(wakeSignal *types.RunExecutionWakeSignal, fallback string) string {
	if wakeSignal != nil {
		if prompt := strings.TrimSpace(wakeSignal.FollowUpPrompt); prompt != "" {
			return prompt
		}
		if reason := strings.TrimSpace(wakeSignal.Reason); reason != "" {
			return reason
		}
		if summary := wakeAgendaSummary(wakeSignal.WakeAgenda); summary != "" {
			return summary
		}
	}
	return strings.TrimSpace(fallback)
}

func followUpPlanningMessage(agentMsg string, tracker *taskOutputTracker) string {
	if trimmed := strings.TrimSpace(agentMsg); trimmed != "" {
		return trimmed
	}
	if tracker == nil {
		return ""
	}
	summaries := tracker.TrackedOutputSummaries()
	if len(summaries) == 0 {
		return ""
	}
	sort.SliceStable(summaries, func(i, j int) bool {
		left := strings.Join([]string{
			strings.TrimSpace(summaries[i].EntityKey),
			strings.TrimSpace(summaries[i].ArtifactKey),
			strings.TrimSpace(summaries[i].Title),
			strings.TrimSpace(summaries[i].OutputID),
		}, "\x00")
		right := strings.Join([]string{
			strings.TrimSpace(summaries[j].EntityKey),
			strings.TrimSpace(summaries[j].ArtifactKey),
			strings.TrimSpace(summaries[j].Title),
			strings.TrimSpace(summaries[j].OutputID),
		}, "\x00")
		return left < right
	})

	lines := []string{
		"The agent completed the turn but did not emit a final natural-language summary.",
		"Infer any needed follow-up from these persisted outputs and the active skill context:",
	}
	for i, summary := range summaries {
		if i >= 8 {
			lines = append(lines, fmt.Sprintf("- ...and %d more output(s)", len(summaries)-i))
			break
		}
		label := firstNonEmptyTrimmed(
			strings.TrimSpace(summary.ArtifactKey),
			strings.TrimSpace(summary.OutputType),
			"output",
		)
		line := "- " + label
		if entity := strings.TrimSpace(summary.EntityKey); entity != "" {
			line += " for " + entity
		}
		if title := strings.TrimSpace(summary.Title); title != "" {
			line += ": " + title
		}
		if threadID := strings.TrimSpace(summary.ThreadID); threadID != "" {
			line += fmt.Sprintf(" [thread_id=%s]", threadID)
		}
		if messageID := strings.TrimSpace(summary.MessageID); messageID != "" {
			line += fmt.Sprintf(" [message_id=%s]", messageID)
		}
		if recipient := strings.TrimSpace(summary.Recipient); recipient != "" {
			line += fmt.Sprintf(" [recipient=%s]", recipient)
		}
		if subject := strings.TrimSpace(summary.Subject); subject != "" && subject != strings.TrimSpace(summary.Title) {
			line += fmt.Sprintf(" [subject=%s]", subject)
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func shouldLogFollowUpDecision(message string) bool {
	message = strings.ToLower(strings.TrimSpace(message))
	if message == "" {
		return false
	}
	for _, token := range []string{
		"wake me",
		"monitoring this thread",
		"follow-up monitoring schedule",
		"check for replies",
		"thread id",
	} {
		if strings.Contains(message, token) {
			return true
		}
	}
	return false
}

func normalizeSubtaskWakeDelayMinutes(wakeSignal *types.RunExecutionWakeSignal, requested int) int {
	if requested > 0 {
		return requested
	}
	if wakeSignal != nil && wakeSignal.DelayMinutes > 0 {
		return wakeSignal.DelayMinutes
	}
	return 5
}

func shouldAttemptFanOut(task types.RunExecution, summaries []trackedOutputSummary) bool {
	if distinctFanOutEntityCount(summaries) < 2 {
		return false
	}
	return !isFanOutChildTask(task.ExecutionPolicy)
}

func isFanOutChildTask(executionPolicy map[string]any) bool {
	return strings.EqualFold(
		executionPolicyString(executionPolicy, "spawned_by"),
		types.AgentTaskSpawnedByFanOut,
	)
}

func executionPolicyString(executionPolicy map[string]any, key string) string {
	if len(executionPolicy) == 0 {
		return ""
	}
	raw, ok := executionPolicy[key]
	if !ok || raw == nil {
		return ""
	}
	switch typed := raw.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", typed))
	}
}

func distinctFanOutEntityCount(summaries []trackedOutputSummary) int {
	entities := make(map[string]struct{}, len(summaries))
	for _, summary := range summaries {
		entity := strings.TrimSpace(summary.EntityKey)
		if entity == "" {
			continue
		}
		entities[entity] = struct{}{}
	}
	return len(entities)
}
