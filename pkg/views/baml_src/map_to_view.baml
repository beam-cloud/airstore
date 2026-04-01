class ViewColumn {
  key string @description("Machine-stable column key, e.g. 'property_address'")
  label string @description("Human-readable column name, e.g. 'Property Address'")
  type string @description("Column type: text, number, currency, date, link, email, status, tags, boolean")
}

class ViewCell {
  column string @description("Column key matching a ViewColumn.key")
  value string @description("The value to set. Empty string means leave the existing value as-is.")
}

class SearchCriterion {
  column string @description("Column key to search in")
  value string @description("Search fragment — a name, address, email, ID, or other identifier. Keep short and specific.")
}

class RowSearchPlan {
  criteria SearchCriterion[] @description("Identity values to search for in existing rows. Multiple criteria per entity is fine.")
  entity_labels string[] @description("Canonical row-level identity for each distinct top-level entity in the output. Prefer exact values already present in the output, such as a property address, email, person name, company name, or record ID.")
}

class AffectedRowsResult {
  affected_row_ids string[] @description("IDs of existing rows this output is about.")
  unmatched_entities string[] @description("Brief label for each genuinely new entity not represented by any existing row. Almost always empty.")
}

class PopulateRowResult {
  cells ViewCell[] @description("One entry per schema column. Non-empty value = set/replace. Empty string = preserve existing.")
  row_key string @description("Stable row key. Existing row: keep current key. New row: derive from identity, e.g. 'john-doe-123-main-st'.")
}

function ClassifyAffectedRows(
  columns: ViewColumn[],
  output_type: string,
  output_title: string,
  output_summary: string,
  output_data: string,
  existing_rows: string
) -> AffectedRowsResult {
  client SlowMappingClient
  prompt #"
    Classify which existing table rows are affected by a task output.

    SCHEMA:
    {% for col in columns %}
    - {{ col.label }} [{{ col.key }}] ({{ col.type }})
    {% endfor %}

    OUTPUT:
    Type: {{ output_type }} | Title: {{ output_title }}
    Summary: {{ output_summary }}
    Data: {{ output_data }}

    {% if existing_rows %}
    EXISTING ROWS:
    {{ existing_rows }}
    {% endif %}

    RULES:
    1. Identify every top-level entity in the output at the SAME granularity as one
       table row (a person, property, company, product, tool, restaurant, etc.).
    2. Match each entity to existing rows by semantic identity — name, email, address,
       or other unique identifier. Abbreviation/format variants of the SAME identity match
       ("123 Main" = "123 Main St", "J. Smith" = "John Smith").
    3. Different street numbers are DIFFERENT entities even on the same street.
       "100 Greene Ave" ≠ "101 Greene Ave". "708 1st St" ≠ "710 1st St".
       Only match when the full unique identifier matches.
    4. Only match when the entity and row represent the SAME thing. A running shoe is
       not a restaurant; a dev tool is not a person. Different-domain entities are unmatched.
    5. Add every matched row ID to affected_row_ids. If one output summarizes several
       entities, include all of their matching rows.

    UNMATCHED ENTITIES:
    - List each entity that has no matching existing row.
    - An entity is a top-level record, NOT an attribute or detail (a cocktail program,
      a rating, a status update are NOT entities).
    - Use row-level identity strings, not container labels. Prefer exact values from
      the output such as a property address, email, person/company name, or record ID.
    - If the table is empty or has no rows of the same type, all entities are unmatched.

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
  entity_hint: string,
  sheet_name: string
) -> PopulateRowResult {
  client SlowMappingClient
  prompt #"
    Extract cell values from a task output for a single table row.

    TARGET SHEET: {{ sheet_name }}
    COLUMNS:
    {% for col in columns %}
    - {{ col.label }} [{{ col.key }}] ({{ col.type }})
    {% endfor %}

    OUTPUT:
    Type: {{ output_type }} | Title: {{ output_title }}
    Summary: {{ output_summary }}
    Data: {{ output_data }}

    {% if row_id %}
    TARGET ROW (existing, id={{ row_id }}):
    {{ row_cells }}
    {% else %}
    TARGET: NEW ROW in "{{ sheet_name }}" — Entity: {{ entity_hint }}
    {% endif %}

    RULES:
    - You are populating a row in the "{{ sheet_name }}" sheet. Fill columns that are
      appropriate for this sheet's purpose based on its name and column definitions.
    - Include one ViewCell per schema column.
    - The output may mention multiple entities. Only extract data for THIS row's entity.
    - Non-empty value → set or replace the cell (use for status transitions, new data, corrections).
    - Empty string "" → preserve whatever is already stored (use when the output has nothing new).
    - Identity fields (name, address, email): keep existing unless the output corrects them.
    - Dates: ISO 8601 (YYYY-MM-DD). Links: full URL. Status: infer from context.
    - If TARGET is NEW ROW and entity_hint is empty while the output describes multiple
      top-level entities, DO NOT synthesize a summary/batch row. Return empty strings
      for every cell and an empty row_key instead.
    - If entity_hint is provided, extract data ONLY for that entity. If you cannot
      isolate exactly one entity matching the hint, return empty strings and an empty row_key.
    - row_key must identify a single row-level entity only. Never use batch, broker,
      project, or summary labels when the output is about multiple row-level entities.

    {{ ctx.output_format }}
  "#
}

function PlanRowSearch(
  columns: ViewColumn[],
  output_type: string,
  output_title: string,
  output_summary: string,
  output_data: string,
  sheet_name: string,
  view_context: string
) -> RowSearchPlan {
  client SlowMappingClient
  prompt #"
    Extract search terms to find existing table rows related to a task output.

    {% if view_context %}
    VIEW STRUCTURE:
    {{ view_context }}

    {% endif %}
    TARGET SHEET: {{ sheet_name }}
    COLUMNS:
    {% for col in columns %}
    - {{ col.label }} [{{ col.key }}] ({{ col.type }})
    {% endfor %}

    OUTPUT:
    Type: {{ output_type }} | Title: {{ output_title }}
    Summary: {{ output_summary }}
    Data: {{ output_data }}

    INSTRUCTIONS:
    1. You are extracting entities for the "{{ sheet_name }}" sheet specifically.
       Consider what kind of rows this sheet tracks based on its name and columns.
    2. Identify each distinct top-level entity in the output at the same granularity
       as one table row (person, property, company, product, restaurant, etc.).
    3. If the output is a batch summary that mentions several row-level entities,
       list each one separately. Do NOT collapse them into a container label such as
       a broker, project, task, email thread, or batch name.
    4. For each entity, extract identity values: name, address, email, phone,
       URL, ID — whatever uniquely identifies it in the table.
    5. Map each value to the schema column it belongs in. One entity may have
       multiple criteria (e.g. name + address).
    6. Values should be concise search fragments, not full sentences.
       "708 1st St" not "708 1st St, Napa, CA 94559".
       "Inessa Romano" not "Broker Inessa Romano".
    7. Return entity_labels with one canonical row-level identity per entity. Prefer
       literal values already present in the output such as a property address, email,
       person/company name, or record ID.

    {{ ctx.output_format }}
  "#
}

enum RowMatchAction {
  UPDATE_EXISTING @description("The incoming data belongs to this existing row — same entity")
  INSERT_NEW      @description("The incoming data is a NEW entity — none of the candidates match")
}

class RowMatchResult {
  action RowMatchAction
  target_row_id string @description("Row ID to update. Required when action=UPDATE_EXISTING, empty string for INSERT_NEW.")
  reason string @description("One-sentence explanation of the decision.")
}

function ClassifyRowMatch(
  columns: ViewColumn[],
  incoming_cells: string,
  candidate_rows: string
) -> RowMatchResult {
  client FastMappingClient
  prompt #"
    Decide whether incoming data should UPDATE an existing row or INSERT a new one.

    SCHEMA:
    {% for col in columns %}
    - {{ col.label }} [{{ col.key }}] ({{ col.type }})
    {% endfor %}

    INCOMING DATA (cells the agent wants to add):
    {{ incoming_cells }}

    CANDIDATE ROWS (existing rows found by similarity search):
    {{ candidate_rows }}

    RULES:
    1. Match by unique identity: name, address, email, phone, URL, or ID.
    2. Different street numbers = DIFFERENT entities.
       "100 Greene Ave" ≠ "101 Greene Ave". "708 1st St" ≠ "710 1st St".
    3. Abbreviation/format variants of the SAME identity DO match.
       "123 Main" = "123 Main St". "J. Smith" = "John Smith".
    4. If NONE of the candidates represent the same entity, return INSERT_NEW.
    5. If exactly one candidate matches, return UPDATE_EXISTING with its _id.
    6. If multiple candidates match (rare), pick the best one.

    {{ ctx.output_format }}
  "#
}
