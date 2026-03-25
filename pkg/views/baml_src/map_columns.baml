// =============================================================================
// MapImportColumns
// One-shot column mapper for imports. Sees headers, data preview, existing
// schema, and sheet name. Handles matching, creation, skipping, deduplication,
// removal of redundant existing columns, and column reordering.
// Runs once per import (not per row).
// =============================================================================

enum ColumnAction {
  Match @description("Header semantically matches an existing column — reuse its key")
  Create @description("Header is relevant to this sheet and should become a new column")
  Skip @description("Header doesn't fit this sheet's purpose — data is stored but no column is added to the visible schema")
}

class ColumnMapping {
  header string @description("The original CSV/source header, exactly as provided")
  column_key string @description("The column key: an existing key for 'match', a new snake_case key for 'create', or a fallback snake_case key for 'skip'")
  action ColumnAction @description("What to do with this header")
  label string @description("Human-readable column label. Use the existing column name for 'match', a clean title for 'create', empty for 'skip'")
  column_type string @description("Column type for 'create': text, number, currency, date, link, email, status, tags, boolean. Empty for 'match' and 'skip'")
}

class ColumnMappingResult {
  mappings ColumnMapping[] @description("One entry per input header, in the same order")
  column_order string[] @description("ALL visible column keys in recommended display order — existing columns that are kept + new columns. Exclude removed and skipped keys. Reorder for logical grouping.")
  remove_columns string[] @description("Existing column keys to drop because they are redundant with a matched or newly created column. Empty array if nothing to remove.")
}

function MapImportColumns(
  sheet_name: string,
  headers: string[],
  existing_columns: ColumnSchema[],
  data_preview: string
) -> ColumnMappingResult {
  client OutputMapperClient
  prompt #"
    You are mapping imported file data into a table's column schema. The table
    lives in a sheet called "{{ sheet_name }}".

    For each file header, decide whether it matches an existing column, deserves
    a new column, or should be skipped. Use BOTH the header names AND the actual
    data values to make your decision. Then clean up the schema: remove
    redundant columns and reorder everything logically.

    {% if existing_columns|length > 0 %}
    EXISTING COLUMNS IN THIS SHEET:
    {% for col in existing_columns %}
    - {{ col.name }} [key={{ col.key }}] ({{ col.type }})
    {% endfor %}
    {% else %}
    This sheet has no columns yet.
    {% endif %}

    FILE HEADERS:
    {% for h in headers %}
    - {{ h }}
    {% endfor %}

    {% if data_preview %}
    SAMPLE DATA (first rows from the file):
    {{ data_preview }}
    {% endif %}

    MATCHING RULES — use the data to make smarter decisions:

    MATCH — header maps to an existing column:
    - Use FUZZY semantic matching. Look at the actual values, not just names.
    - "Property Address" with values like "123 Main St" → matches "address"
    - "Loc" with values like "Oakland, CA" → matches "city" or "location"
    - "Asking" with values like "$2,500" → matches "rent" or "price"
    - "Contact" with values like "john@example.com" → matches "email"
    - "SF" with values like "1200" → matches "sq_ft"
    - If a header name is ambiguous, the data values are the tiebreaker.
    - Prefer exact key matches first, then semantic matches.

    DEDUPLICATION — aggressively consolidate:
    - "Property Owner Email" IS "email" if the sheet has an email column.
    - "Property Owner Name" IS "name" / "contact_name" if one exists.
    - "Broker Phone" IS "phone" if the sheet has a phone column.
    - When a header is a qualified variant of an existing column (e.g.
      "property_owner_X" vs "X"), MATCH it to the existing column.
    - If the CSV has BOTH "Email" and "Property Owner Email", match the
      more relevant one to the existing column and SKIP the other.
    - Never create a new column when an existing column covers the same
      concept. "contact_email" and "email" are the same thing.

    CREATE — header is relevant but no existing column matches:
    - Consider the sheet name and existing columns to judge relevance.
    - Pick the right column_type based on the actual data values:
      email if values look like emails, number if numeric, currency if
      money amounts, date if dates, link if URLs, status if categorical
      states, tags if comma-separated lists, text otherwise.
    - Generate a clean, concise snake_case key.
    - Provide a clear human-readable label.

    SKIP — header doesn't belong in this sheet:
    - Internal IDs, system metadata, row numbers, and fields clearly
      outside this sheet's domain.
    - Also skip when two headers map to the same concept — keep the
      better one, skip the duplicate.
    - When in doubt, CREATE. Only skip when it clearly doesn't fit.
    - If the sheet has no columns yet, almost everything should be
      CREATE.

    REMOVE REDUNDANT EXISTING COLUMNS (remove_columns):
    - If the existing schema has columns that are now clearly redundant
      with what the import provides, list their keys in remove_columns.
    - Example: schema has both "name" and "property_owner_name" from a
      previous bad import — remove "property_owner_name" since "name"
      covers it.
    - Only remove truly redundant columns. Never remove a column that
      holds unique data not covered by another column.
    - If nothing is redundant, return an empty array.

    COLUMN ORDER (column_order):
    - List ALL visible column keys in the recommended display order.
    - Include existing columns (minus removed ones) and new columns.
    - Do NOT include skipped or removed keys.
    - Group logically: identity fields first (name, email, phone),
      then location/address, then domain-specific fields, then metadata.
    - Within groups, put the most important columns first.
    - This is a generic view — order by what a human would scan first.

    GENERAL:
    - Never map two headers to the same column_key.
    - Every header must appear exactly once in mappings.
    - Preserve the exact header string in the "header" field.
    - For "skip": still generate a valid snake_case key but leave label
      and column_type empty.

    {{ ctx.output_format }}
  "#
}
