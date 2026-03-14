// =============================================================================
// MapOutputsToSchema
// Maps task outputs (with rich BAML-generated metadata) into the column schema
// of a view widget. Called at view render time, results are cached.
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
  output_id string @description("The TaskOutput ID this row was derived from")
  cells MappedCell[] @description("One cell per column in the schema, matched by column key")
}

class MappedResult {
  rows MappedRow[] @description("One row per task output that has relevant data for this widget")
}

function MapOutputsToSchema(
  widget_title: string,
  widget_type: string,
  columns: ColumnSchema[],
  outputs_json: string,
  artifact_key_filter: string
) -> MappedResult {
  client OutputMapperClient
  prompt #"
    You are mapping task output data into a widget's column schema for a
    dashboard view.

    WIDGET: {{ widget_title }} (type: {{ widget_type }})
    {% if artifact_key_filter != "" %}
    ARTIFACT KEY FILTER: {{ artifact_key_filter }}
    {% endif %}

    COLUMN SCHEMA (the columns the widget expects):
    {% for col in columns %}
    - {{ col.key }} ({{ col.type }}): {{ col.description }}
    {% endfor %}

    TASK OUTPUTS (JSON array of outputs with their data and metadata):
    {{ outputs_json }}

    INSTRUCTIONS:
    1. For each task output, inspect its data fields, metadata, title, summary,
       and URI to find values that match the column descriptions.
    2. Map each output to a row with cells matching the column schema.
    3. Use semantic understanding — don't just match field names literally.
       For example, if a column asks for "Recipe Name" and the output has
       data.recipe_name or data.title with a recipe name, use it.
    4. For link columns, look for URLs in data, metadata.deeplink, or URI.
    5. For date columns, use ISO 8601 format (e.g. "2024-01-15T10:30:00Z").
    6. Skip outputs that have no relevant data for ANY column (don't create
       empty rows). But if an output has data for at least one column,
       include it with empty strings for missing columns.
    7. Preserve the original values — don't summarize or reformat unless
       the column type requires it (e.g. extracting a date from a string).
    8. For tags columns, join multiple values with ", ".
    9. For boolean columns, use "true" or "false".
    10. Every row MUST include the output_id from the source task output.

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
