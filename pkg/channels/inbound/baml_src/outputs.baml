// =============================================================================
// Task Output Classification
// Infers structured outputs from agent log tails when no explicit outputs exist
// =============================================================================

class TableColumn {
  key string @description("Column identifier for data access")
  label string @description("Human-readable column header")
  type "string" | "number" | "boolean" | "date"
  display ("link" | "badge" | "currency")? @description("Optional display hint for the UI")
}

class InferredOutput {
  output_type "table" | "text" | "file" | "link" | "json"
  title string @description("Concise, human-friendly title for this output")
  summary string @description("One-sentence summary of what this output contains")
  schema TableColumn[]? @description("For tables: column definitions. Null for other types.")
  data map<string, string> @description("Output data. For tables: {rows: [...]}, for text: {content: ...}, for links: {url: ..., description: ...}, for files: {path: ..., mime_type: ...}")
}

function ClassifyTaskOutput(
  task_label: string,
  agent_name: string,
  log_tail: string,
  task_state: string
) -> InferredOutput[] {
  client InboundRouter
  prompt #"
    An AI agent completed a task. Analyze its log output and extract
    every meaningful result it produced as structured outputs.

    Rules:
    - For tabular data: infer column schema with clear human labels
    - For text results: extract the final answer or analysis
    - For files created/modified: note the path and type
    - For URLs/links: capture with a description
    - Skip internal logging, debug output, and progress indicators
    - Return an empty array if there are no meaningful outputs

    Task: {{ task_label }}
    Agent: {{ agent_name }}
    State: {{ task_state }}

    Log output (last 100 lines):
    ---
    {{ log_tail }}
    ---

    {{ ctx.output_format }}
  "#
}
