// =============================================================================
// Inbound Channel Processing
// Routes incoming messages (email, SMS) to the right action for an agent
// =============================================================================

// -----------------------------------------------------------------------------
// Types
// -----------------------------------------------------------------------------

class ActiveTask {
  id string @description("Task ID")
  label string @description("Short description of the task")
  state string @description("Current state: pending, running, done, failed")
  from_sender string? @description("Original sender who created this task, if known")
}

class AvailableAgent {
  id string @description("Agent ID")
  name string @description("Agent display name")
  description string? @description("What this agent does, if known")
}

enum InboundTaskType {
  CREATE_TASK    @description("New work — the message contains a request the agent should act on")
  UPDATE_TASK    @description("Follow-up on an existing task — sender is adding context or responding to prior work")
  ESCALATE       @description("Urgent or sensitive — flag for immediate attention")
}

class InboundTask {
  task_type InboundTaskType
  agent_id string? @description("Which agent should handle this task. Required when multiple agents are available. Use the id from available_agents.")
  label string? @description("Concise label for new tasks (CREATE_TASK, ESCALATE). Not needed for UPDATE_TASK.")
  message string @description("Cleaned/summarized message content for the agent")
  priority string @description("low | medium | high | urgent")
  existing_task_id string? @description("For UPDATE_TASK: ID of the task this relates to")
}

class InboundResponse {
  reply string @description("Email/SMS reply to send back to the sender. Always provide a helpful, natural response.")
  should_reply bool @description("Whether to actually send the reply. True for most messages. False only for spam or automated messages.")
  tasks InboundTask[] @description("Actions to take. Can be empty (pure conversation), one task, or multiple. Executed in order.")
}

// -----------------------------------------------------------------------------
// ProcessInboundMessage
// Routes an incoming channel message to the right action(s) for an agent
// -----------------------------------------------------------------------------

function ProcessInboundMessage(
  channel_type: string,
  sender: string,
  subject: string,
  body: string,
  conversation_history: string,
  agent_name: string,
  active_tasks: ActiveTask[],
  available_agents: AvailableAgent[]
) -> InboundResponse {
  client InboundRouter
  prompt #"
    You are {{ agent_name }}, an AI agent receiving messages via {{ channel_type }}.
    You should respond naturally as {{ agent_name }} — be professional, helpful, and concise.

    =============================================================================
    INCOMING MESSAGE
    =============================================================================
    From: {{ sender }}
    Subject: {{ subject }}
    Body: {{ body }}

    =============================================================================
    CONTEXT
    =============================================================================

    {% if conversation_history != "(no history)" %}
    CONVERSATION HISTORY WITH THIS SENDER:
    {{ conversation_history }}
    {% endif %}

    {% if active_tasks %}
    ACTIVE TASKS:
    {% for task in active_tasks %}
    - [{{ task.state }}] {{ task.label }} (id: {{ task.id }}){% if task.from_sender %} — from {{ task.from_sender }}{% endif %}
    {% endfor %}
    {% endif %}

    {% if available_agents %}
    AVAILABLE AGENTS (pick the best one for each task):
    {% for agent in available_agents %}
    - {{ agent.name }} (id: {{ agent.id }}){% if agent.description %} — {{ agent.description }}{% endif %}
    {% endfor %}
    {% endif %}

    =============================================================================
    WHAT TO DO
    =============================================================================

    1. ALWAYS write a reply to the sender. Be natural and conversational.
       Set should_reply=false ONLY for spam or automated bounce messages.

    2. Decide what tasks (if any) to create:

    TASK TYPES:
    - CREATE_TASK: The message contains a new request or piece of work.
      → Set label (concise title) + message (cleaned content) + priority
    - UPDATE_TASK: The sender is following up on an existing active task.
      → Set existing_task_id + message (the update/context to add)
      → Look at ACTIVE TASKS to find the matching task
    - ESCALATE: Something urgent, time-sensitive, or involving money/security/legal.
      → Like CREATE_TASK but marked urgent

    {% if available_agents %}
    AGENT SELECTION: For each task, set agent_id to the most appropriate agent from AVAILABLE AGENTS.
    Pick based on the agent's name and description. If unsure, pick the first agent.
    {% endif %}

    MULTIPLE TASKS: A single message can produce multiple tasks.
    Example: "Please update my invoice AND also schedule a meeting" → 2 tasks

    NO TASKS: Simple conversation, thank-you messages, questions you can answer
    from context. Just reply — no tasks needed.

    =============================================================================
    REPLY GUIDELINES
    =============================================================================

    - Acknowledge what you're doing: "Got it, I'll take care of that."
    - If creating a task: briefly confirm what you understood
    - If it's a follow-up: reference the existing task naturally
    - If no action needed: just respond helpfully
    - Keep it concise — 1-3 sentences usually
    - Don't say "I'm an AI" or "I'll create a task" — just be natural

    =============================================================================
    EXAMPLES
    =============================================================================

    New request:
      Body: "Can you review the Q3 financial report and flag any issues?"
      → reply: "On it — I'll review the Q3 report and flag anything that stands out."
      → tasks: [{ type: CREATE_TASK, label: "Review Q3 financial report", message: "Review Q3 financial report and flag issues", priority: "medium" }]

    Follow-up on existing task:
      Body: "Actually, focus on the marketing spend section specifically"
      Active task: [running] Review Q3 financial report (id: task-123) — from same sender
      → reply: "Noted, I'll focus on the marketing spend section."
      → tasks: [{ type: UPDATE_TASK, existing_task_id: "task-123", message: "Focus specifically on the marketing spend section", priority: "medium" }]

    Simple conversation (no task):
      Body: "Thanks for handling that!"
      → reply: "Happy to help! Let me know if anything else comes up."
      → tasks: []

    Urgent escalation:
      Body: "URGENT: Production database is down, customers can't access their data"
      → reply: "On it immediately — treating this as top priority."
      → tasks: [{ type: ESCALATE, label: "Production database down", message: "Production database is down, customers cannot access data. Immediate attention required.", priority: "urgent" }]

    {{ ctx.output_format }}
  "#
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

test TestNewRequest {
  functions [ProcessInboundMessage]
  args {
    channel_type "email"
    sender "alice@example.com"
    subject "Q3 Report Review"
    body "Can you review the Q3 financial report and flag any issues?"
    conversation_history "(no history)"
    agent_name "Finance Bot"
    active_tasks []
    available_agents []
  }
}

test TestFollowUp {
  functions [ProcessInboundMessage]
  args {
    channel_type "email"
    sender "alice@example.com"
    subject "Re: Q3 Report Review"
    body "Actually, focus on the marketing spend section specifically"
    conversation_history "[Mar 3 10:00] User: Can you review the Q3 financial report?\n[Mar 3 10:01] Agent: On it — I'll review the Q3 report."
    agent_name "Finance Bot"
    active_tasks [
      { id "task-123", label "Review Q3 financial report", state "running", from_sender "alice@example.com" }
    ]
    available_agents []
  }
}

test TestThankYou {
  functions [ProcessInboundMessage]
  args {
    channel_type "email"
    sender "bob@example.com"
    subject "Re: Invoice"
    body "Thanks for handling that so quickly!"
    conversation_history "[Mar 3 09:00] User: Please send the invoice to accounting\n[Mar 3 09:01] Agent: Done — invoice forwarded to accounting."
    agent_name "Admin Assistant"
    active_tasks []
    available_agents []
  }
}

test TestUrgent {
  functions [ProcessInboundMessage]
  args {
    channel_type "email"
    sender "ops@example.com"
    subject "URGENT: Production down"
    body "Production database is down, customers can't access their data. Need immediate help."
    conversation_history "(no history)"
    agent_name "DevOps Bot"
    active_tasks []
    available_agents []
  }
}

test TestWorkspaceRouting {
  functions [ProcessInboundMessage]
  args {
    channel_type "email"
    sender "manager@example.com"
    subject "Two things"
    body "Please update the client proposal with the new pricing AND schedule a follow-up meeting for next week."
    conversation_history "(no history)"
    agent_name "Workspace"
    active_tasks []
    available_agents [
      { id "agent-1", name "Sales Bot", description "Handles proposals, pricing, and client communications" },
      { id "agent-2", name "Scheduler", description "Manages calendar and meetings" }
    ]
  }
}
