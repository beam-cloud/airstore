package worker

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

type AirRunnerOptions struct {
	AnthropicAPIKey string
	CerebrasAPIKey  string
	S2Key           string
	S2Basin         string
}

type AirRunner struct {
	anthropicAPIKey string
	cerebrasAPIKey  string
	s2Key           string
	s2Basin         string
}

type airTraceOutput struct {
	Summary          string                    `json:"summary"`
	NextStep         string                    `json:"next_step"`
	DraftedResponses []airTraceDraftedResponse `json:"drafted_responses"`
}

type airTraceDraftedResponse struct {
	Channel string `json:"channel"`
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func NewAirRunner(opts AirRunnerOptions) *AirRunner {
	return &AirRunner{
		anthropicAPIKey: strings.TrimSpace(opts.AnthropicAPIKey),
		cerebrasAPIKey:  strings.TrimSpace(opts.CerebrasAPIKey),
		s2Key:           strings.TrimSpace(opts.S2Key),
		s2Basin:         strings.TrimSpace(opts.S2Basin),
	}
}

func (r *AirRunner) Name() string { return "air" }

func (r *AirRunner) OutputAnalyzer() OutputAnalyzer {
	return NewAirAnalyzer()
}

func (r *AirRunner) ClassifierEnv() map[string]string {
	env := map[string]string{}
	if key := strings.TrimSpace(r.anthropicAPIKey); key != "" {
		env["ANTHROPIC_API_KEY"] = key
	}
	return env
}

func (r *AirRunner) BuildEntrypoint(task types.RunExecution, env map[string]string) []string {
	r.injectEnv(env)

	spLen := len(strings.TrimSpace(env[agentSystemPromptEnvKey]))
	addTaskExecutionContext(
		log.Info().
			Str("prompt", task.Prompt[:min(50, len(task.Prompt))]).
			Int("system_prompt_len", spLen),
		task,
	).Msg("running air task")

	return r.buildArgs(env, task.Prompt)
}

func (r *AirRunner) BuildTurnArgs(prompt string, env map[string]string, mode TurnArgMode) []string {
	r.injectEnv(env)

	log.Info().
		Str("session_id", strings.TrimSpace(env[agentSessionIDEnvKey])).
		Str("turn_mode", string(mode)).
		Msg("building air turn args")

	return r.buildArgs(env, prompt)
}

func (r *AirRunner) buildArgs(env map[string]string, prompt string) []string {
	builder := newPromptEntrypointBuilder("air").
		withKeyValue("--format", "json").
		withKeyValue("--session", strings.TrimSpace(env[agentSessionIDEnvKey])).
		withKeyValue("--system", strings.TrimSpace(env[agentSystemPromptEnvKey])).
		withKeyValue("--model", strings.TrimSpace(env[agentModelEnvKey]))
	return builder.withPrompt(prompt).build()
}

func (r *AirRunner) injectEnv(env map[string]string) {
	inject := func(key, value string) {
		if value != "" && env[key] == "" {
			env[key] = value
		}
	}
	inject("ANTHROPIC_API_KEY", r.anthropicAPIKey)
	inject("CEREBRAS_API_KEY", r.cerebrasAPIKey)
	inject("S2_KEY", r.s2Key)
	inject("S2_BASIN", r.s2Basin)
}

// ParseTurnOutput extracts turn state, assistant response text, and any
// structured artifacts from air's JSON trace. air emits JSONL events (with an
// "event" field) to stderr and a final JSON trace summary (without an "event"
// field) to stdout; in a PTY they're interleaved. We skip any line that has an
// "event" key and only consider the trace summary.
func (r *AirRunner) ParseTurnOutput(output []byte) (TurnParseResult, error) {
	var trace airTrace
	for _, line := range bytes.Split(output, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] != '{' {
			continue
		}
		var raw map[string]json.RawMessage
		if json.Unmarshal(line, &raw) != nil {
			continue
		}
		if _, hasEvent := raw["event"]; hasEvent {
			continue
		}
		var candidate airTrace
		if json.Unmarshal(line, &candidate) == nil && candidate.Status != "" {
			trace = candidate
		}
	}
	if trace.Status == "" {
		return TurnParseResult{}, nil
	}

	response := airTraceResponseText(trace)
	blocker := airTraceBlockerDirective(trace, response)
	return TurnParseResult{
		Response:  response,
		Artifacts: airDraftedResponseArtifacts(trace.Output, airTurnArtifactBlockingMetadata(blocker)),
		Control:   airTurnControl(blocker),
	}, nil
}

// ExtractResponseText implements ResponseExtractor for air's JSONL output.
// It scans for the last "response" event and returns its message.
func (r *AirRunner) ExtractResponseText(raw []byte, limit int) string {
	var last string
	for _, line := range bytes.Split(raw, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] != '{' {
			continue
		}
		var ev struct {
			Event   string `json:"event"`
			Message string `json:"message"`
		}
		if json.Unmarshal(line, &ev) == nil && ev.Event == "response" && ev.Message != "" {
			last = ev.Message
		}
	}
	if len(last) > limit {
		return last[len(last)-limit:]
	}
	return last
}

type airTrace struct {
	Status     string          `json:"status"`
	NeedsInput bool            `json:"needs_input"`
	Response   string          `json:"response"`
	InputKind  string          `json:"input_kind"`
	SessionID  string          `json:"session_id"`
	Output     *airTraceOutput `json:"output,omitempty"`
}

func airTurnControl(blocker *TurnBlockerDirective) *TurnControl {
	if blocker == nil {
		return nil
	}
	return &TurnControl{Blocker: blocker}
}

func airTraceResponseText(trace airTrace) string {
	response := strings.TrimSpace(trace.Response)
	if response != "" {
		return response
	}
	if trace.Output != nil {
		if nextStep := strings.TrimSpace(trace.Output.NextStep); nextStep != "" {
			return nextStep
		}
		return strings.TrimSpace(trace.Output.Summary)
	}
	return response
}

func airTraceBlockerDirective(trace airTrace, response string) *TurnBlockerDirective {
	if !trace.NeedsInput {
		return nil
	}
	inputKind := types.InputKind(strings.TrimSpace(trace.InputKind))
	if inputKind == "" {
		inputKind = types.InputKindFreeText
	}
	return &TurnBlockerDirective{
		InputKind: inputKind,
		Summary:   strings.TrimSpace(response),
	}
}

func airTurnArtifactBlockingMetadata(blocker *TurnBlockerDirective) *types.TaskOutputBlockingMetadata {
	if blocker == nil {
		return nil
	}
	metadata := &types.TaskOutputBlockingMetadata{
		InputKind: blocker.InputKind,
	}
	if blocker.InputKind == types.InputKindApproveReject {
		metadata.Kind = types.TaskOutputBlockingKindApproval
		metadata.ApprovalSurface = true
		return metadata
	}
	metadata.Kind = types.TaskOutputBlockingKindInput
	return metadata
}

func airDraftedResponseArtifacts(output *airTraceOutput, blocking *types.TaskOutputBlockingMetadata) []TurnArtifact {
	if output == nil || len(output.DraftedResponses) == 0 || blocking == nil {
		return nil
	}

	summary := strings.TrimSpace(output.Summary)
	var artifacts []TurnArtifact
	for _, draft := range output.DraftedResponses {
		channel := strings.ToLower(strings.TrimSpace(draft.Channel))
		if channel == "" {
			channel = "draft"
		}
		title := firstNonEmptyTrimmed(draft.Subject, draft.To, draft.Channel)
		content := firstNonEmptyTrimmed(draft.Body, draft.Subject, draft.To)
		if title == "" || content == "" {
			continue
		}

		data := map[string]any{}
		if c := strings.TrimSpace(draft.Channel); c != "" {
			data["channel"] = c
		}
		if to := strings.TrimSpace(draft.To); to != "" {
			data["to"] = to
		}
		if subject := strings.TrimSpace(draft.Subject); subject != "" {
			data["subject"] = subject
		}

		artifacts = append(artifacts, TurnArtifact{
			OutputType: channel,
			Title:      title,
			Summary:    summary,
			Content:    content,
			Data:       data,
			Metadata: map[string]any{
				types.TaskOutputMetadataArtifactKey:   channel + "-draft",
				types.TaskOutputMetadataArtifactLabel: strings.Title(channel) + " Drafts",
				types.TaskOutputMetadataArtifactKind:  channel,
			},
			Status:   types.TaskOutputStatusPending,
			Blocking: blocking,
		})
	}
	return artifacts
}
