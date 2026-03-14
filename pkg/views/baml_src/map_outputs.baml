// =============================================================================
// MapOutputsToSchema
// Maps task outputs (grouped by task_id) into the column schema of a view
// widget. Multiple outputs from the same task are synthesized into one row.
// Called at view render time, results are cached.
// =============================================================================

class ColumnSchema {
  key string @description("Machine-stable column key, e.g. 'recipe_name', 'video_url'")
  type string @description("Column type: text, number, currency, date, link, email, status, tags, boolean")
  description string @description("Semantic description of what this column should contain, e.g. 'The name of the recipe', 'URL to the video'")
}

class MappedCell {
  column string @description("Column key matching a ColumnSchema.key")
  value string @description("The extracted value as a string. Empty string if no value found.")
}

class MappedRow {
  task_id string @description("The task ID this row was synthesized from")
  cells MappedCell[] @description("One cell per column in the schema, matched by column key")
}

class MappedResult {
  rows MappedRow[] @description("One row per task that has relevant data for this widget")
}

function MapOutputsToSchema(
  widget_title: string,
  widget_type: string,
  columns: ColumnSchema[],
  outputs_json: string
) -> MappedResult {
  client OutputMapperClient
  prompt #"
    You are mapping task output data into a widget's column schema for a
    dashboard view.

    WIDGET: {{ widget_title }} (type: {{ widget_type }})

    COLUMN SCHEMA (the columns the widget expects):
    {% for col in columns %}
    - {{ col.key }} ({{ col.type }}): {{ col.description }}
    {% endfor %}

    TASK OUTPUTS (JSON object, keyed by task_id. Each entry has an optional
    "prompt" field with the user's original request and an "outputs" array of
    the outputs that task produced):
    <outputs>{{ outputs_json }}</outputs>

    INSTRUCTIONS:
    1. Each top-level key is a task_id. The "prompt" field (when present)
       contains the user's original request that triggered the task — use it
       as context to better understand what the outputs represent. The
       "outputs" array contains all outputs that task produced.
    2. Produce ONE row per task_id by synthesizing the best value for each
       column from ANY output in that task's group. Use the prompt for
       additional context when output data alone is ambiguous.
    3. IMPORTANT: produce a row for EVERY task. Each task_id in the input
       should have a corresponding row in the output. Do not skip tasks.
    4. When multiple outputs have a value for the same column, prefer outputs
       with artifact_role "primary" over "supporting" or "incidental".
    5. Use semantic understanding — don't just match field names literally.
       For example, if a column asks for "Recipe Name" and an output has
       data.recipe_name or data.title with a recipe name, use it.
    6. For link columns, look for URLs in data, metadata.deeplink, URI, AND
       the prompt. The prompt often contains the source URL (e.g. a YouTube
       link) that is distinct from output URLs (e.g. a Google Drive link).
       Match the semantically correct URL to each link column — don't use a
       Drive link for a "Video" column when the prompt has a YouTube URL.
    7. For date columns, use ISO 8601 format (e.g. "2024-01-15T10:30:00Z").
    8. Preserve the original values — don't summarize or reformat unless
       the column type requires it (e.g. extracting a date from a string).
    9. For tags columns, join multiple values with ", ".
    10. For boolean columns, use "true" or "false".
    11. Every row MUST include the task_id.

    {{ ctx.output_format }}
  "#
}

client<llm> OutputMapperClient {
  provider anthropic
  options {
    model claude-sonnet-4-20250514
    api_key env.ANTHROPIC_API_KEY
    max_tokens 8192
  }
}
