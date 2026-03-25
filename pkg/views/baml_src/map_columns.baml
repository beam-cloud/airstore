// =============================================================================
// Import Column Mapping — single-pass, compact
// =============================================================================

class ImportColumnMatch {
  header string @description("Original CSV header exactly as provided")
  existing_key string @description("Key of the existing column this header maps to")
}

class ImportMappingResult {
  matches ImportColumnMatch[] @description("Headers that match existing columns. Only list actual matches.")
  skip string[] @description("Header names to skip — duplicates, internal IDs, or irrelevant metadata. Exact header strings.")
}

function MapImportColumns(
  sheet_name: string,
  existing_columns: ColumnSchema[],
  headers: string[],
  data_preview: string
) -> ImportMappingResult {
  client FastMappingClient
  prompt #"
    Map CSV headers to existing table columns for sheet "{{ sheet_name }}".

    {% if existing_columns|length > 0 %}
    EXISTING COLUMNS:
    {% for col in existing_columns %}
    - {{ col.name }} [key={{ col.key }}] ({{ col.type }})
    {% endfor %}
    {% else %}
    No existing columns.
    {% endif %}

    CSV HEADERS ({{ headers|length }} total):
    {% for h in headers %}{{ h }}{% if not loop.last %} | {% endif %}{% endfor %}

    {% if data_preview %}
    SAMPLE DATA:
    {{ data_preview }}
    {% endif %}

    Return:
    1. `matches`: headers that semantically match an existing column. Use fuzzy matching —
       "Property Address" → "address", "Owner Email" → "email", "SF" with values "1200" → "sq_ft".
       Only match when confident. Don't match two headers to the same existing_key.
    2. `skip`: headers to hide — internal IDs, row numbers, system metadata, or near-duplicates
       of another header. When in doubt, don't skip.

    Headers not in matches or skip will automatically become new columns.
    Be concise. Only list what's needed.

    {{ ctx.output_format }}
  "#
}
