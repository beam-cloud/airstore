// =============================================================================
// Company Copilot: Plan Changes
// Given a classified intent and full company context, produce a concrete changeset.
// =============================================================================

enum ActionType {
  provision_agent    @description("Create a new agent with config")
  modify_agent       @description("Update an existing agent's configuration")
  create_task        @description("Dispatch a task to a specific agent")
  launch_campaign    @description("Create multiple tasks across agents")
  create_schedule    @description("Set up a recurring scheduled task")
  configure_source   @description("Connect or configure a data source")
}

class ActionParams {
  agent_id string?          @description("Existing agent ID (for modify_agent, create_task, create_schedule)")
  agent_key string?         @description("URL-safe key for new agents (for provision_agent)")
  agent_name string?        @description("Display name (for provision_agent, modify_agent)")
  agent_role string?        @description("Role description (for provision_agent, modify_agent)")
  system_prompt string?     @description("System prompt override (for provision_agent, modify_agent)")
  model string?             @description("LLM model to use (for provision_agent, modify_agent)")
  skills string[]?          @description("Skill paths to assign (for provision_agent, modify_agent)")
  message string?           @description("Task prompt (for create_task, create_schedule, launch_campaign)")
  priority string?          @description("Task priority: low, normal, high, urgent")
  target_agent_ids string[]? @description("Agent IDs to target (for launch_campaign)")
  count int?                @description("Number of tasks per agent (for launch_campaign)")
  cron_expr string?         @description("Cron expression (for create_schedule)")
  timezone string?          @description("Timezone for schedule (for create_schedule)")
  integration_type string?  @description("Integration type: gmail, slack, confluence, github (for configure_source)")
}

class CompanyAction {
  type ActionType
  description string @description("Human-readable description of this action")
  params ActionParams
}

class CompanyChangeset {
  explanation string @description("Brief explanation of what these changes will do and why")
  actions CompanyAction[] @description("Ordered list of actions to execute")
  warnings string[]? @description("Any caveats or risks the user should know about")
}

function PlanCompanyChanges(
  intent_type: string,
  intent_summary: string,
  user_message: string,
  conversation_history: string,
  company_state: string
) -> CompanyChangeset {
  client CompanyPlanner
  prompt #"
    You are the Company Copilot. The user's intent has been classified as:
    {{ intent_type }}: {{ intent_summary }}

    Your job is to produce a concrete, executable changeset.

    =============================================================================
    CURRENT COMPANY STATE
    =============================================================================
    {{ company_state }}

    {% if conversation_history != "" %}
    =============================================================================
    CONVERSATION HISTORY
    =============================================================================
    {{ conversation_history }}
    {% endif %}

    =============================================================================
    USER MESSAGE
    =============================================================================
    {{ user_message }}

    =============================================================================
    RULES
    =============================================================================

    1. Produce the minimal set of actions needed to fulfill the user's request.
    2. Use existing agents when possible — don't create duplicates.
    3. For CREATE_TASK: if the user doesn't specify an agent, pick the best one
       from the existing agents based on their role and skills. If no agent fits,
       create one first.
    4. For agent keys: use lowercase-hyphenated format (e.g. "research-lead").
    5. For system prompts: be specific and helpful. Include the agent's role and
       any domain expertise needed.
    6. Include warnings if:
       - An action might be expensive (many tasks, large campaign)
       - The user might be duplicating existing agents
       - A referenced agent doesn't exist
    7. Priority defaults to "normal" unless the user indicates urgency.
    8. For schedules, infer timezone from context or default to "UTC".

    {{ ctx.output_format }}
  "#
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

test TestPlanNewAgent {
  functions [PlanCompanyChanges]
  args {
    intent_type "CREATE_AGENTS"
    intent_summary "User wants to create a research agent for competitor analysis"
    user_message "Create a research agent that focuses on competitor analysis"
    conversation_history ""
    company_state "AGENTS:\n  (none)\nRUNNING TASKS:\n  (none)\n"
  }
}

test TestPlanTask {
  functions [PlanCompanyChanges]
  args {
    intent_type "CREATE_TASK"
    intent_summary "User wants the research agent to analyze Acme Corp"
    user_message "Have the research agent look into Acme Corp's latest product launch"
    conversation_history ""
    company_state "AGENTS:\n  - Research Lead (id: agent-1, key: research-lead, role: researcher, state: idle)\n"
  }
}
