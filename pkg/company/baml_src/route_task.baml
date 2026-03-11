// =============================================================================
// Company Copilot: Task Routing
// Given a task description and available agents, pick the best agent.
// =============================================================================

class AgentOption {
  id string
  name string
  role string
  skills string[]?
  active_task_count int
  state string @description("Current state: idle, working, waiting, sleeping, error")
}

class RunningTaskInfo {
  agent_id string
  prompt_summary string
  state string
}

class TaskRouting {
  agent_id string @description("ID of the best agent to handle this task")
  reasoning string @description("Brief explanation of why this agent was chosen")
  priority string @description("Suggested priority: low, normal, high, urgent")
}

function RouteTask(
  task_description: string,
  agents: AgentOption[],
  running_tasks: RunningTaskInfo[]
) -> TaskRouting {
  client CompanyRouter
  prompt #"
    Pick the best agent to handle this task.

    TASK: {{ task_description }}

    AVAILABLE AGENTS:
    {% for agent in agents %}
    - {{ agent.name }} (id: {{ agent.id }}, role: {{ agent.role }}, state: {{ agent.state }}, active tasks: {{ agent.active_task_count }}){% if agent.skills %} skills: {{ agent.skills }}{% endif %}
    {% endfor %}

    {% if running_tasks %}
    CURRENTLY RUNNING TASKS:
    {% for task in running_tasks %}
    - [{{ task.state }}] {{ task.prompt_summary }} (agent: {{ task.agent_id }})
    {% endfor %}
    {% endif %}

    SELECTION CRITERIA:
    1. Role match: pick the agent whose role best fits the task
    2. Skill match: prefer agents with relevant skills
    3. Load balance: prefer idle agents over busy ones
    4. Availability: skip agents in error state

    PRIORITY RULES:
    - "urgent": time-sensitive, production issues, security
    - "high": important but not emergency
    - "normal": standard work (default)
    - "low": background, nice-to-have

    {{ ctx.output_format }}
  "#
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

test TestRouteToResearcher {
  functions [RouteTask]
  args {
    task_description "Analyze competitor pricing strategies"
    agents [
      { id "agent-1", name "Research Lead", role "researcher", skills ["customer-research"], active_task_count 0, state "idle" },
      { id "agent-2", name "Writer", role "content writer", skills ["sop-writer"], active_task_count 1, state "working" }
    ]
    running_tasks []
  }
}
