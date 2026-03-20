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
	Summary string `json:"summary"`
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

// ParseTurnOutput extracts needs_input and response from air's JSON trace.
// air emits JSONL events (with an "event" field) to stderr and a final JSON
// trace summary (without an "event" field) to stdout; in a PTY they're
// interleaved. We skip any line that has an "event" key and only consider
// the trace summary.
func (r *AirRunner) ParseTurnOutput(output []byte) (bool, types.InputKind, string, error) {
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
		return false, "", "", nil
	}

	kind := types.InputKind("")
	if trace.NeedsInput {
		if trace.InputKind != "" {
			kind = types.InputKind(trace.InputKind)
		} else {
			kind = types.InputKindFreeText
		}
	}
	response := strings.TrimSpace(trace.Response)
	if response == "" && trace.Output != nil {
		response = strings.TrimSpace(trace.Output.Summary)
	}
	return trace.NeedsInput, kind, response, nil
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
