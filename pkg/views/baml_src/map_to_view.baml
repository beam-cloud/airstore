class ViewColumn {
  key string @description("Machine-stable column key, e.g. 'property_address'")
  label string @description("Human-readable column name, e.g. 'Property Address'")
  type string @description("Column type: text, number, currency, date, link, email, status, tags, boolean")
}

class ViewCell {
  column string @description("Column key matching a ViewColumn.key")
  value string @description("The value to set. Empty string means leave the existing value as-is.")
}

class ViewRowMappingResult {
  action string @description("'enrich' to update an existing row, 'insert' to create a new row")
  target_row_id string @description("The _id of the existing row to enrich. Empty string for insert.")
  row_key string @description("Stable key for the row. For inserts, derive from the entity's identity (e.g. 'john-doe-123-main-st'). For enrichments, use the existing row's row_key.")
  cells ViewCell[] @description("One entry per column. Empty string value means 'leave existing value as-is' (for enrich) or 'no value' (for insert).")
}

function MapOutputToViewRow(
  sheet_name: string,
  table_title: string,
  columns: ViewColumn[],
  output_type: string,
  output_title: string,
  output_summary: string,
  output_data: string,
  existing_rows: string
) -> ViewRowMappingResult {
  client FastMappingClient
  prompt #"
    You are mapping a single task output into a table's column schema. Your job:
    1. Extract values from the output for every column where data exists.
    2. Decide whether this output should ENRICH an existing row or INSERT a new one.

    SHEET: {{ sheet_name }}
    TABLE: {{ table_title }}

    COLUMN SCHEMA:
    {% for col in columns %}
    - {{ col.label }} [key={{ col.key }}] ({{ col.type }})
    {% endfor %}

    OUTPUT:
    - Type: {{ output_type }}
    - Title: {{ output_title }}
    - Summary: {{ output_summary }}
    - Data:
    {{ output_data }}

    {% if existing_rows %}
    EXISTING ROWS in this table (compact JSON — each has _id, row_key, cells):
    <existing_rows>
    {{ existing_rows }}
    </existing_rows>
    {% endif %}

    MATCHING RULES — enrich vs insert:
    - Check if the output describes an entity that already exists in the table.
      Match by semantic identity: same person (name + email), same property
      (address), same company, same email thread (thread_id), etc.
    - If a match is found: set action="enrich" and target_row_id to that row's _id.
    - If no match: set action="insert" and target_row_id="".
    - For inserts, generate a stable row_key from the entity's identity
      (e.g. "john-doe-123-main-st", "thread-abc123"). Use lowercase-dash-case.

    CELL VALUE RULES:
    - For EVERY column in the schema, include a ViewCell entry.
    - Extract aggressively from the output data — search title, summary, and
      all data fields for values matching each column.
    - When enriching: set a non-empty value for any column where the output
      has relevant data. This REPLACES the existing value — use this for
      status transitions (e.g. "Waiting for Approval" → "Sent"), date updates,
      and any field that should reflect the latest state.
    - When enriching: set empty string "" for columns where the output has NO
      relevant data and you want to preserve the existing value.
    - When enriching identity fields (name, address, email): preserve the
      existing value unless the output has a clear correction. Set "" to keep
      the existing value.
    - When inserting: set all extractable values; use "" for columns with no data.
    - For status columns: infer the current status from the output type and
      context (EMAIL_SENT → "Sent", EMAIL_DRAFT → "Drafted", etc.)
    - For date columns: use ISO 8601 format (YYYY-MM-DD or full datetime).
    - For link columns: extract URLs from data fields, uri, or summary.

    {{ ctx.output_format }}
  "#
}
