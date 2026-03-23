enum SectionType {
  EMAIL_THREAD    @description("Full email conversation view, grouped by thread. For tables tracking email outreach, correspondence, etc.")
  APPROVAL        @description("Approve/reject decision card. For tables where tasks may pause for user approval.")
  INPUT_FORM      @description("Free-text input. For tables where tasks may pause for open-ended user input.")
  TASK_PROGRESS   @description("Task state, wake schedule, and agenda. For any table with agent-managed rows.")
  OUTPUT_GALLERY  @description("Files, links, reports, and other non-email deliverables.")
  DATA_SUMMARY    @description("Key-value field display for the row data.")
  SUBTASKS        @description("Child task status list. For tables where rows may spawn subtasks.")
}

enum SectionEmphasis {
  PRIMARY    @description("Featured section — takes most visual space, shown first.")
  SECONDARY  @description("Shown below primary, normal height.")
  COLLAPSED  @description("Shown collapsed by default, expandable.")
}

class DetailSection {
  type SectionType
  title string              @description("Contextual section title based on the table's purpose, e.g. 'Outreach Thread' or 'Research Outputs'.")
  description string?       @description("Brief helper text for context.")
  emphasis SectionEmphasis
}

class DetailLayout {
  sections DetailSection[]  @description("Ordered list of ALL sections that could be relevant for rows in this table. Sections are filtered at render time based on per-row data availability.")
}

function ClassifyDetailTemplate(
  table_title: string,
  column_schema: string
) -> DetailLayout {
  client DetailClassifier
  prompt #"
    You are classifying the detail view template for a data table in an AI agent
    workspace. The table has a specific schema (columns) that tells you what kind
    of data it tracks. Your job: determine which detail sections are relevant for
    rows in this table, their ordering, emphasis, and contextual titles.

    This classification runs ONCE per table schema — individual rows will have
    sections filtered based on whether they actually have data (e.g. EMAIL_THREAD
    only shows if the row's task produced email outputs). Include all sections
    that COULD be relevant for this table type.

    RULES:
    1. The FIRST section with PRIMARY emphasis is the hero — most screen space.
       Pick the most important section for this table's purpose as PRIMARY.
    2. Every layout MUST include DATA_SUMMARY (COLLAPSED or SECONDARY).
    3. Include sections that make sense for this table schema:
       - EMAIL_THREAD if columns suggest email activity (recipient, subject, sent, email_link, thread, etc.)
       - APPROVAL for any agent-managed table (agents may need approval)
       - INPUT_FORM for any agent-managed table (agents may need input)
       - TASK_PROGRESS for any agent-managed table
       - OUTPUT_GALLERY if outputs beyond email are expected
       - SUBTASKS if the table may have per-row subtasks
    4. Order sections by importance for this table's primary use case.
    5. Give titles that match the table's context, not generic labels.
       e.g. for an email outreach table: "Conversation" not "Email Thread",
            "Outreach Status" not "Task Progress".

    Table title: {{ table_title }}

    Column schema (JSON array of column names and types):
    ---
    {{ column_schema }}
    ---

    {{ ctx.output_format }}
  "#
}
