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
  entity_labels string[] @description("Brief label for each distinct top-level entity in the output, e.g. 'Kings County Imperial' or 'Inessa Romano'.")
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
  client FastMappingClient
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
    1. Identify every top-level entity in the output (a person, property, company,
       product, tool, restaurant — whatever the primary record type is).
    2. Match each entity to existing rows by semantic identity — name, email, address,
       or other unique identifier. Partial/variant matches count ("123 Main" = "123 Main St").
    3. Only match when the entity and row represent the SAME thing. A running shoe is
       not a restaurant; a dev tool is not a person. Different-domain entities are unmatched.
    4. Add every matched row ID to affected_row_ids.

    UNMATCHED ENTITIES:
    - List each entity that has no matching existing row.
    - An entity is a top-level record, NOT an attribute or detail (a cocktail program,
      a rating, a status update are NOT entities).
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
  entity_hint: string
) -> PopulateRowResult {
  client FastMappingClient
  prompt #"
    Extract cell values from a task output for a single table row.

    SCHEMA:
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
    TARGET: NEW ROW — Entity: {{ entity_hint }}
    {% endif %}

    RULES:
    - Include one ViewCell per schema column.
    - The output may mention multiple entities. Only extract data for THIS row's entity.
    - Non-empty value → set or replace the cell (use for status transitions, new data, corrections).
    - Empty string "" → preserve whatever is already stored (use when the output has nothing new).
    - Identity fields (name, address, email): keep existing unless the output corrects them.
    - Dates: ISO 8601 (YYYY-MM-DD). Links: full URL. Status: infer from context.

    {{ ctx.output_format }}
  "#
}

function PlanRowSearch(
  columns: ViewColumn[],
  output_type: string,
  output_title: string,
  output_summary: string,
  output_data: string
) -> RowSearchPlan {
  client FastMappingClient
  prompt #"
    Extract search terms to find existing table rows related to a task output.

    SCHEMA:
    {% for col in columns %}
    - {{ col.label }} [{{ col.key }}] ({{ col.type }})
    {% endfor %}

    OUTPUT:
    Type: {{ output_type }} | Title: {{ output_title }}
    Summary: {{ output_summary }}
    Data: {{ output_data }}

    INSTRUCTIONS:
    1. Identify each distinct top-level entity in the output (person, property,
       company, product, restaurant — whatever the primary record type is).
    2. For each entity, extract identity values: name, address, email, phone,
       URL, ID — whatever uniquely identifies it in the table.
    3. Map each value to the schema column it belongs in. One entity may have
       multiple criteria (e.g. name + address).
    4. Values should be concise search fragments, not full sentences.
       "708 1st St" not "708 1st St, Napa, CA 94559".
       "Inessa Romano" not "Broker Inessa Romano".
    5. Return entity_labels with one brief name per entity.

    {{ ctx.output_format }}
  "#
}
