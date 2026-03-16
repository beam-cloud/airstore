// =============================================================================
// View Copilot
// Generates and iteratively edits View definitions (dynamic dashboards)
// =============================================================================

enum ViewUpdateType {
  CONVERSATION     @description("Conversational reply only — no view or workspace changes")
  VIEW_CREATE      @description("Initial view creation — generate the full dashboard definition")
  LAYOUT_CHANGE    @description("Repositioned, resized, or reordered existing components")
  COMPONENT_ADD    @description("Added one or more new components to the view")
  COMPONENT_EDIT   @description("Modified an existing component's config, data source, or transform rules")
  COMPONENT_REMOVE @description("Removed one or more components from the view")
  DATA_REBIND      @description("Changed data sources or transform rules without changing layout")
  INFRASTRUCTURE   @description("Created or modified agents, skills, or assignments — view_content may also be updated")
}

enum OperationType {
  CREATE_AGENT  @description("Create a new agent profile in the workspace")
  UPDATE_AGENT  @description("Update an existing agent's configuration")
  CREATE_SKILL  @description("Write a new SKILL.md and install it")
  INSTALL_SKILL @description("Install an existing skill to the workspace")
  ASSIGN_SKILL  @description("Assign a skill to an agent")
}

class Operation {
  type OperationType @description("The type of workspace mutation to perform")
  payload string @description("JSON payload with operation-specific fields. For CREATE_AGENT: {name, role, model?, skills?[]}. For UPDATE_AGENT: {agent_id, name?, role?, model?, skills?[]}. For CREATE_SKILL: {name, content}. For INSTALL_SKILL: {skill_name} to install an existing workspace skill, or {name, content} / {skill_name, content} to install or update skill content. For ASSIGN_SKILL: {agent_id, skill_name}. IMPORTANT: Do NOT include system_prompt — the system generates it automatically with proper Airstore context. model must be one of: 'claude-sonnet-4-6' (default, omit if unsure) or 'claude-opus-4-6'. role must be 1-2 words max (e.g. 'Research', 'Engineering', 'Outreach'). For CREATE_SKILL, content must be valid SKILL.md with YAML frontmatter (---\\nname: ...\\ndescription: ...\\n---).")
}

class ViewDraftResponse {
  update_type ViewUpdateType @description("What kind of change this response represents")
  message string @description("Conversational response explaining what was done or asking for clarification")
  view_content string @description("The complete JSON view definition. Empty string when update_type is CONVERSATION.")
  removed_sheet_ids string[] @description("Sheet IDs intentionally removed from the current view. Use only when the user explicitly asked to remove those sheets. Empty array otherwise.")
  operations Operation[] @description("Workspace mutations to execute (create agents, install skills, etc). Empty array when no infrastructure changes are needed.")
}

function WriteView(
  user_message: string,
  conversation_history: string,
  current_view: string,
  workspace_context: string,
  component_registry: string
) -> ViewDraftResponse {
  client ViewCopilotClient
  prompt #"
    You are a workbook designer. You build Views — each view is a workbook of
    tabbed sheets. Each sheet has one primary data table and optional actions.
    Views aggregate data from AI agents into clean, scannable sheets.

    =============================================================================
    COMPONENT REGISTRY
    =============================================================================
    {{ component_registry }}

    =============================================================================
    VIEW DEFINITION SCHEMA
    =============================================================================
    A view definition is a JSON object:
    {
      "name": "string — concise goal-oriented title (NEVER 'dashboard' or 'app')",
      "description": "string — what this view tracks",
      "agents": ["agent-id-1", "agent-id-2"],
      "sheets": [
        {
          "id": "sheet-1",
          "name": "Sheet Name",
          "description": "string — what this sheet tracks",
          "layout": {"columns": 12},
          "relations": [
            {
              "id": "rel-1",
              "name": "Optional relation name",
              "to_sheet_id": "sheet-2",
              "from_column": "task_id",
              "to_column": "task_id"
            }
          ],
          "components": [
            {
              "id": "c1",
              "type": "table",
              "title": "Component Title",
              "position": {"col": 0, "row": 0, "colSpan": 12, "rowSpan": 1},
              "dataSource": {
                "agent_id": "single-agent-id",
                "agent_ids": ["agent-1", "agent-2"],
                "output_type": "json|file|email|link|text",
                "artifact_key": "optional-artifact-family-key",
                "time_range": "24h|7d|30d",
                "row_strategy": {
                  "mode": "task|split",
                  "description": "For split mode, describe what one row represents. Example: one row per emailed contact."
                },
                "transform": [
                  {"column": "stable_key", "source": "data.field|title|metadata.key", "type": "text|number|currency|datetime|link|email|status|tags|boolean"}
                ]
              },
              "config": {}
            },
            {
              "id": "a1",
              "type": "action",
              "title": "Action Name",
              "position": {"col": 0, "row": 1, "colSpan": 4, "rowSpan": 1},
              "config": {
                "agent_id": "single-agent-id",
                "button_label": "Run Action",
                "fields": [
                  {"name": "company_name", "label": "Company name", "type": "text", "required": true},
                  {"name": "website", "label": "Website", "type": "text"}
                ],
                "prompt_template": "{% raw %}Research {{company_name}} using {{website}} and summarize the findings.{% endraw %}"
              }
            }
          ]
        }
      ]
    }

    STRUCTURE RULES:
    - STRONG DEFAULT: use ONE sheet. A single sheet handles most workflows.
      Only add multiple sheets when the user explicitly asks for them OR the
      workflow has genuinely distinct data domains (e.g. "contacts" vs "emails
      sent" vs "pricing comparisons" are three separate entity types that
      would clutter one table). When in doubt, use one sheet.
    - Every sheet has exactly ONE table component. It is always full-width
      (colSpan: 12) at position row 0.
    - Action components are optional and belong to their sheet.
    - Generate concise sheet names. Avoid generic names like "Sheet 1" unless the
      user gave no meaningful distinction.
    - Use relations when a row in one sheet should connect to rows in another
      sheet through stable keys like task_id, email, company_id, listing_id, etc.
    - Do NOT add NEW sections, metrics, charts, or task_list components.
      Preserve any existing legacy components unless the user explicitly asks
      to remove or replace them.
    - The canvas automatically renders assigned agents and live task counts
      for the active sheet — you do NOT need to create components for those.

    TRANSFORM RULES (semantic hints for the BAML mapper):
    At render time a BAML mapper dynamically maps task output data into table
    columns. Transform rules guide the mapping:
    - column: machine-stable key (snake_case) for what the column shows.
      Put the visible header in config.columns[].label.
    - source: dot-path hint into task_output fields (e.g. "data.recipe_name",
      "title", "uri"). Guidance, not rigid binding.
    - type: text (default), number, currency, datetime, link, email, status,
      tags, boolean. Guides both display and mapping.
    - row_strategy.mode:
      - "task" (default): synthesize one row per task. This is the strong
        default — omit row_strategy entirely unless split is needed.
      - "split": expand a single task into many rows when it produces multiple
        distinct entities (e.g. 10 emails sent, 5 listings scraped). Each
        entity becomes its own row in the table.
    - row_strategy.description is REQUIRED for split mode. Describe what one
      row represents clearly, e.g. "one row per recipient email that was sent".
    - Use split mode when a user says things like "track each email separately",
      "one row per contact", "expand into individual items", etc.

    =============================================================================
    WORKSPACE CONTEXT
    =============================================================================
    {{ workspace_context }}

    =============================================================================
    CONVERSATION CONTEXT
    =============================================================================

    {% if conversation_history != "" %}
    Previous conversation:
    {{ conversation_history }}
    {% endif %}

    {% if current_view != "" %}
    Current view definition:
    ---BEGIN VIEW---
    {{ current_view }}
    ---END VIEW---
    {% endif %}

    =============================================================================
    USER REQUEST
    =============================================================================
    {{ user_message }}

    =============================================================================
    INSTRUCTIONS
    =============================================================================
    1. Read the user's request. Determine what kind of change is needed.

    2. Set update_type appropriately:
       - CONVERSATION: answering questions, asking for clarification
       - VIEW_CREATE: building a new view from scratch
       - LAYOUT_CHANGE: only repositioning/resizing components
       - COMPONENT_ADD: adding new components
       - COMPONENT_EDIT: modifying existing component config/data
       - COMPONENT_REMOVE: removing components
       - DATA_REBIND: changing data sources without layout changes
       - INFRASTRUCTURE: creating/modifying agents or skills

    3. If agents or skills need to be created, populate the operations array.
       CRITICAL: Agent references in the view MUST use the kebab-case key
       derived from the agent name (lowercase, spaces/underscores → hyphens).
       Example: name "Email Outreach" → key "email-outreach". Use consistently.

    4. In view_content, always provide the COMPLETE view definition JSON.
       If update_type is CONVERSATION, set view_content to empty string ""
       and removed_sheet_ids to [].
       CRITICAL: The output MUST include ALL existing sheets, components,
       columns, and relations from the current view — even ones you did not
       modify. Users add sheets and columns manually outside the copilot.
       Never drop, rename, or reorganize sheets the user did not ask to change.
       Only omit a sheet if the user explicitly asks to remove it, and when
       you do, include that existing sheet ID in removed_sheet_ids.

    5. In message, briefly explain what you did (1-3 sentences).

    6. PRESERVE IDs on edits. If a sheet, relation, or component already exists
       in the current view and is still present after your edit, keep its
       existing `id` unchanged. Only generate new IDs for newly added sheets
       (`sheet-1`, `sheet-2`), relations (`rel-1`, `rel-2`), and components
       (`c1`, `c2` for tables; `a1`, `a2` for actions).

    7. NAMING: Never use "dashboard" or "app" in the view name. Use short,
       goal-oriented names (e.g. "Outreach Pipeline", "Content Calendar").

    8. When creating transform rules, inspect the ARTIFACT DESCRIPTORS in
       workspace_context. Use exact dot-path field names from schemas as
       source hints. Keep transform[].column machine-stable and put the
       visible header in config.columns[].label. If an existing sheet already
       has dataSource.artifact_key, preserve it unless the user explicitly asks
       to rebind that sheet to a different artifact family.

    9. When no artifact schemas exist yet, define columns with descriptive
       names and types. The BAML mapper resolves dynamically at render time.
       Example: {"column": "name", "source": "title", "type": "text"}

    10. Default to ONE sheet. Only add more sheets when the data genuinely
        splits into distinct entity types that would be confusing in a single
        table. When using split row_strategy on a sheet, that is usually
        sufficient — you do NOT need a separate "detail" sheet for it.

    {{ ctx.output_format }}
  "#
}
