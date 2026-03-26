class ViewColumn {
  key string @description("Machine-stable column key, e.g. 'property_address'")
  label string @description("Human-readable column name, e.g. 'Property Address'")
  type string @description("Column type: text, number, currency, date, link, email, status, tags, boolean")
}

class ViewCell {
  column string @description("Column key matching a ViewColumn.key")
  value string @description("The value to set. Empty string means leave the existing value as-is.")
}

class AffectedRowsResult {
  affected_row_ids string[] @description("IDs of existing rows this output is about. Empty if output doesn't match any rows.")
  unmatched_entities string[] @description("Brief descriptions of entities in the output that don't match any existing row. Each becomes a new row insert. Empty if all entities match existing rows.")
}

class PopulateRowResult {
  cells ViewCell[] @description("One entry per column. Set value for columns with data from the output. Empty string means 'leave existing value'.")
  row_key string @description("Stable key for the row. For new inserts, derive from entity identity (e.g. 'john-doe-123-main-st'). For existing rows, keep the current row_key.")
}

function ClassifyAffectedRows(
  columns: ViewColumn[],
  output_type: string,
  output_title: string,
  output_summary: string,
  output_data: string,
  existing_rows: string
) -> AffectedRowsResult {
  client FastMappingClient
  prompt #"
    You are classifying which existing rows in a table are affected by a task output.
    An output often references MULTIPLE entities — identify ALL of them.

    COLUMN SCHEMA (for understanding the data):
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
    EXISTING ROWS (each has _id and identity columns):
    <existing_rows>
    {{ existing_rows }}
    </existing_rows>
    {% endif %}

    CLASSIFICATION RULES:
    - Identify ALL entities (people, properties, companies, threads) mentioned in the output.
    - For each entity, check if it matches an existing row by semantic identity:
      same person (name OR email), same property (address), same company (name),
      same email thread (thread_id), etc.
    - Add matched row IDs to affected_row_ids.
    - Be aggressive about matching: partial name matches, address variations
      (e.g. "123 Main" vs "123 Main Street") should match.
    - If the output is about a batch action (e.g. "emailed 5 brokers"), each
      property/contact is a separate entity — match each one to its row individually.

    UNMATCHED ENTITIES — be very conservative:
    - Only add to unmatched_entities if an entity is genuinely NEW and not
      represented by ANY existing row, even partially.
    - Do NOT add an entity if it is already covered by an affected_row_id.
      For example, if a broker's name appears in the output and their properties
      are already matched as affected rows, the broker is NOT an unmatched entity.
    - Do NOT add email thread IDs, sender names, or metadata as unmatched entities.
      These are attributes of existing rows, not new entities to insert.
    - When in doubt, prefer matching to an existing row over creating a new entity.
    - The ideal case: affected_row_ids covers every entity, unmatched_entities is empty.

    {{ ctx.output_format }}
  "#
}

function PopulateRowCells(
  columns: ViewColumn[],
  output_type: string,
  output_title: string,
  output_summary: string,
  output_data: string,
  row_id: string,
  row_cells: string,
  entity_hint: string
) -> PopulateRowResult {
  client FastMappingClient
  prompt #"
    You are extracting cell values from a task output for a SINGLE row in a table.

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

    {% if row_id %}
    TARGET ROW (existing — row_id={{ row_id }}):
    {{ row_cells }}
    {% else %}
    TARGET: NEW ROW (insert)
    Entity: {{ entity_hint }}
    {% endif %}

    CELL VALUE RULES:
    - Extract values from the output for columns where data is relevant to THIS
      specific row/entity. The output may reference multiple entities — only
      extract data for the one matching this row.
    - For EVERY column in the schema, include a ViewCell entry.
    - When updating existing rows:
      * Set non-empty value for columns where the output has NEW or UPDATED data
        for this entity. This REPLACES the existing value.
      * Use this for status transitions (e.g. "Waiting for Approval" → "Sent"),
        date updates, and fields reflecting the latest state.
      * Set empty string "" for columns where the output has NO new data for
        this entity — this preserves the existing value.
      * For identity fields (name, address, email): preserve existing values
        unless the output has a clear correction. Set "" to keep.
    - When inserting new rows:
      * Set all extractable values from the output for this entity.
      * Use "" for columns with no data.
    - For status columns: infer status from output type and context
      (EMAIL_SENT → "Sent", EMAIL_DRAFT → "Drafted", etc.)
    - For date columns: use ISO 8601 format (YYYY-MM-DD).
    - For link columns: extract URLs from data fields, uri, or summary.
    - For row_key: use existing row_key if updating, or derive from entity
      identity for new inserts (lowercase-dash-case, e.g. "john-doe-123-main-st").

    {{ ctx.output_format }}
  "#
}
