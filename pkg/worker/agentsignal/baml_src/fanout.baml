enum FanOutIntent {
  NONE       @description("The outputs do not represent independent threads that need separate management.")
  FAN_OUT    @description("The outputs represent multiple independent threads (e.g. individual outreach emails, per-entity follow-ups) that should each be managed as a separate subtask with its own lifecycle and wake schedule.")
}

class SubtaskSpec {
  source_output_id string   @description("The ID of the parent task output this subtask is responsible for (from the extracted outputs list).")
  entity_label string       @description("Short human-readable label for the entity this subtask manages, e.g. 'john@example.com' or 'Acme Corp'.")
  prompt string             @description("Instruction for the subtask agent. Should include enough context to act independently — what to check, what entity to focus on, what the parent task already did.")
  wake_delay_minutes int    @description("Minutes before the subtask should first wake to check on this thread. Typical: 30-120 for email response checks.")
}

class FanOutSignal {
  intent FanOutIntent
  specs SubtaskSpec[]       @description("One spec per independent thread. Empty when intent is NONE.")
  reason string?            @description("One-line summary of why fan-out is appropriate.")
}

function ClassifyFanOut(extracted_outputs: string, agent_message: string, user_prompt: string, current_time: string) -> FanOutSignal {
  client FanOutClassifier
  prompt #"
    An AI agent just finished a turn that produced multiple outputs. Your job
    is to decide whether these outputs represent **independent threads** that
    should each be managed as a separate subtask with its own lifecycle.

    FAN_OUT when ALL of these are true:
    1. The outputs represent actions taken toward **distinct entities** (people,
       companies, tickets, threads, etc.)
    2. Each entity needs **independent follow-up** on its own timeline (e.g.
       checking if person A replied is independent of checking if person B replied)
    3. There are at least 2 distinct entities

    FAN_OUT examples:
    - Sent outreach emails to 15 different people → each needs response checking
    - Created support tickets for 8 different customers → each needs resolution tracking
    - Posted to 5 different Slack channels → each conversation thread is independent
    - Filed PRs in 3 different repos → each needs review/merge tracking

    NONE examples:
    - Sent one email to one person
    - Created a single report with multiple sections
    - Made several API calls that are all part of one workflow
    - Produced outputs that are all part of the same deliverable
    - Fewer than 2 distinct entities

    When intent is FAN_OUT:
    - Return one SubtaskSpec per entity
    - Each spec.prompt should give the subtask agent full context to act
      independently: what was done, what entity to focus on, what to check
    - Set wake_delay_minutes based on the expected response time (email
      replies: 60-240, ticket resolution: 120-480, build/deploy: 15-60)
    - Cap at 50 subtask specs — if more entities, group the least critical

    When intent is NONE:
    - Return empty specs array

    Current time (RFC3339 UTC):
    ---
    {{ current_time }}
    ---

    User's original task prompt:
    ---
    {{ user_prompt }}
    ---

    Agent's final message (last 1500 chars):
    ---
    {{ agent_message }}
    ---

    Extracted outputs (JSON):
    ---
    {{ extracted_outputs }}
    ---

    {{ ctx.output_format }}
  "#
}
