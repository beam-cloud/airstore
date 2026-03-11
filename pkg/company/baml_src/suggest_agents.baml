// =============================================================================
// Company Copilot: Agent Suggestions
// Given a company goal, suggest agent configurations.
// =============================================================================

class ExistingAgent {
  id string
  name string
  role string
  skills string[]?
}

class SkillInfo {
  name string
  description string
}

class AgentSuggestion {
  agent_key string @description("URL-safe key, lowercase-hyphenated")
  name string @description("Human-readable display name")
  role string @description("What this agent does")
  system_prompt string @description("Detailed system prompt for the agent")
  skills string[]? @description("Skill paths to assign, from available skills")
  reasoning string @description("Why this agent is needed")
}

function SuggestAgents(
  goal: string,
  existing_agents: ExistingAgent[],
  available_skills: SkillInfo[]
) -> AgentSuggestion[] {
  client CompanyPlanner
  prompt #"
    The user wants to build or evolve their AI agent workforce. Suggest which
    agents to create based on their goal.

    GOAL: {{ goal }}

    {% if existing_agents %}
    EXISTING AGENTS (don't duplicate these):
    {% for agent in existing_agents %}
    - {{ agent.name }} (key: {{ agent.id }}, role: {{ agent.role }}){% if agent.skills %} skills: {{ agent.skills }}{% endif %}
    {% endfor %}
    {% endif %}

    {% if available_skills %}
    AVAILABLE SKILLS (reference these by name when assigning):
    {% for skill in available_skills %}
    - {{ skill.name }}: {{ skill.description }}
    {% endfor %}
    {% endif %}

    GUIDELINES:
    1. Don't create agents that overlap with existing ones.
    2. Each agent should have a clear, distinct purpose.
    3. System prompts should be specific and actionable (2-4 sentences).
    4. Use lowercase-hyphenated keys (e.g. "market-researcher").
    5. Assign relevant skills from the available list when applicable.
    6. Suggest 1-5 agents — don't over-provision.

    {{ ctx.output_format }}
  "#
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

test TestSuggestForRealEstate {
  functions [SuggestAgents]
  args {
    goal "I need a real estate company with agents for lead research, mystery shopping, and contract review"
    existing_agents []
    available_skills [
      { name "customer-research", description "Research potential customers and leads" },
      { name "real-estate-mystery-shopping", description "Conduct mystery shopping calls for real estate" },
      { name "contract-review", description "Review and analyze legal contracts" }
    ]
  }
}
