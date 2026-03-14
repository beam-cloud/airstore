// =============================================================================
// Output Detail Formatter
// Formats raw TaskOutput data into presentation-ready sections for the
// ViewDrawer. Results are cached per (view_id, output_id) so this function
// is only called once per output per view.
// =============================================================================

class FormattedSection {
  heading string @description("Section heading (e.g. 'Email Thread', 'Analysis', 'Key Findings')")
  content string @description("Markdown-formatted content for this section. Use headings, lists, bold, links, code blocks as appropriate.")
}

class FormattedDetail {
  title string @description("A concise title for this detail view")
  sections FormattedSection[] @description("Ordered list of content sections to display")
}

function FormatOutputDetail(
  view_description: string,
  output_type: string,
  output_title: string,
  output_data: string,
  output_metadata: string,
  output_summary: string
) -> FormattedDetail {
  client DetailFormatterClient
  prompt #"
    You are formatting raw agent output data for display in a detail drawer
    within a dashboard view.

    The view this output belongs to: {{ view_description }}

    Your job is to take the raw structured data and present it as clear,
    readable sections appropriate for this view's purpose. Think like a
    product designer — format the data so a user can quickly understand
    what happened and take action.

    Guidelines:
    - Use markdown in section content (bold, lists, links, code blocks)
    - Prioritize the most important information first
    - For email/conversation data, format as a readable thread with
      clear sender/recipient/timestamp headers
    - For analysis data, highlight key findings and metrics
    - For links/URIs, make them clickable markdown links
    - Skip internal IDs, timestamps in raw format, and metadata that
      isn't useful to the end user
    - Keep section headings concise and descriptive
    - If data contains nested objects or arrays, format them as readable
      lists or tables rather than raw JSON
    - Typically 2-5 sections is ideal

    <output_type>{{ output_type }}</output_type>
    <output_title>{{ output_title }}</output_title>
    <output_summary>{{ output_summary }}</output_summary>
    <output_data>{{ output_data }}</output_data>
    <output_metadata>{{ output_metadata }}</output_metadata>

    {{ ctx.output_format }}
  "#
}

client<llm> DetailFormatterClient {
  provider anthropic
  options {
    model claude-sonnet-4-20250514
    api_key env.ANTHROPIC_API_KEY
    max_tokens 4096
  }
}
