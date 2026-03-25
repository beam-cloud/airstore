// =============================================================================
// MapImportColumns
// Lightweight one-shot mapper: takes CSV/JSON headers and existing view columns,
// returns a mapping of each header to the best-matching column key. Runs once
// per import — not per row — so cost is negligible.
// =============================================================================

class ColumnMapping {
  header string @description("The original CSV/source header, exactly as provided")
  column_key string @description("The column key to store this header's data under — either an existing key or a new snake_case key")
  is_new bool @description("True if this is a new key (no suitable existing column match)")
}

class ColumnMappingResult {
  mappings ColumnMapping[] @description("One entry per input header, in the same order")
}

function MapImportColumns(
  headers: string[],
  existing_columns: ColumnSchema[]
) -> ColumnMappingResult {
  client OutputMapperClient
  prompt #"
    Map file headers to table column keys.

    {% if existing_columns|length > 0 %}
    EXISTING TABLE COLUMNS:
    {% for col in existing_columns %}
    - {{ col.name }} [key={{ col.key }}] ({{ col.type }})
    {% endfor %}
    {% else %}
    No existing columns — create new keys for all headers.
    {% endif %}

    FILE HEADERS TO MAP:
    {% for h in headers %}
    - {{ h }}
    {% endfor %}

    RULES:
    1. For each header, find the BEST semantically matching existing column.
       Examples: "Property Address" → address, "Email Address" → email,
       "Sq Ft" → sq_ft, "City/Town" → city, "ZIP Code" → zip.
    2. If no existing column is a reasonable semantic match, create a new
       snake_case key (lowercase, spaces/hyphens to underscores, alphanumeric
       and underscores only). Mark is_new = true.
    3. Never map two different headers to the same column key.
    4. Every header must have exactly one mapping.
    5. Preserve the exact header string in the "header" field.
    6. Prefer exact key matches over fuzzy ones. "city" header → "city" key.
    7. Only match semantically related concepts. "Property Address" and
       "address" are the same concept. "Name" and "email" are not.

    {{ ctx.output_format }}
  "#
}
