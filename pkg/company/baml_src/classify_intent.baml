// =============================================================================
// Company Copilot: Intent Classification
// First BAML call per user message — determines what action (if any) to take.
// =============================================================================

enum IntentType {
  CREATE_AGENTS       @description("User wants to create one or more new agents")
  MODIFY_AGENT        @description("User wants to change an existing agent's config, role, skills, or system prompt")
  CREATE_TASK         @description("User wants to dispatch a task — auto-route to best agent if not specified")
  LAUNCH_CAMPAIGN     @description("User wants to create multiple tasks across one or more agents")
  CONFIGURE_SOURCE    @description("User wants to connect or configure a data source integration")
  CREATE_SCHEDULE     @description("User wants to set up a recurring/scheduled task")
  QUERY_STATUS        @description("User is asking about current state — no action needed, just respond")
  GENERAL_FEEDBACK    @description("User is providing general direction, feedback, or strategy — respond conversationally")
}

class CompanyIntent {
  intent IntentType
  summary string @description("One-sentence summary of what the user wants")
}

function ClassifyCompanyIntent(
  user_message: string,
  conversation_history: string,
  company_state: string
) -> CompanyIntent {
  client CompanyPlanner
  prompt #"
    You are the Company Copilot — a strategic AI assistant that helps orchestrate
    a company's AI agent workforce. Your job right now is to understand what the
    user wants to do.

    =============================================================================
    CURRENT COMPANY STATE
    =============================================================================
    {{ company_state }}

    =============================================================================
    CONVERSATION HISTORY
    =============================================================================
    {% if conversation_history != "" %}
    {{ conversation_history }}
    {% endif %}

    =============================================================================
    USER MESSAGE
    =============================================================================
    {{ user_message }}

    =============================================================================
    CLASSIFICATION RULES
    =============================================================================

    Classify the user's intent:

    - CREATE_AGENTS: They want to spin up new agents. Look for "create", "add",
      "spin up", "hire", "set up" + agent references.
    - MODIFY_AGENT: They want to change an existing agent. Look for "update",
      "change", "modify", "reconfigure" + specific agent references.
    - CREATE_TASK: They want to send work to an agent. This includes "do X",
      "have [agent] do Y", "run", "execute", "analyze", etc.
    - LAUNCH_CAMPAIGN: Multiple tasks at once. "Send X to all agents", "have
      everyone do Y", "launch", "campaign", "batch".
    - CONFIGURE_SOURCE: Connect Gmail, Slack, Confluence, GitHub, etc.
    - CREATE_SCHEDULE: Set up recurring tasks. "Every day", "weekly", "cron",
      "schedule", "recurring".
    - QUERY_STATUS: "How are things going?", "What's running?", "Show me stats",
      "How much have we spent?"
    - GENERAL_FEEDBACK: Strategy, direction, vision — "I think we should focus on...",
      "The agents should be more...", "Overall I want..."

    {{ ctx.output_format }}
  "#
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

test TestCreateAgent {
  functions [ClassifyCompanyIntent]
  args {
    user_message "Create a research agent that focuses on competitor analysis"
    conversation_history ""
    company_state "AGENTS:\n  (none)\nRUNNING TASKS:\n  (none)\n"
  }
}

test TestCreateTask {
  functions [ClassifyCompanyIntent]
  args {
    user_message "Have the research agent look into Acme Corp's latest product launch"
    conversation_history ""
    company_state "AGENTS:\n  - Research Lead (id: agent-1, key: research-lead, role: researcher, state: idle)\n"
  }
}

test TestQueryStatus {
  functions [ClassifyCompanyIntent]
  args {
    user_message "How are things going? What's everyone working on?"
    conversation_history ""
    company_state "AGENTS:\n  - Research Lead (id: agent-1, state: working, tasks: 2)\n  - Writer (id: agent-2, state: idle, tasks: 0)\n"
  }
}
