enum FollowUpIntent {
  NONE       @description("Agent is fully done with this task. No revisit needed.")
  FOLLOW_UP  @description("Agent completed its current work but is waiting on an external event — email reply, webhook callback, data refresh, human approval — and wants to check back later.")
}

class FollowUpSignal {
  intent FollowUpIntent
  delay_minutes int        @description("Minutes to wait before the next check-in. Only meaningful when intent is FOLLOW_UP. Choose a reasonable interval: 5-15 min for fast APIs, 30-120 min for emails, 1440 for next-day checks.")
  reason string?           @description("One line: what the agent is waiting for, e.g. 'email replies from 3 real-estate agents'")
  follow_up_prompt string? @description("Instruction the agent should receive when it wakes up, e.g. 'Check inbox for new replies and respond to any that arrived'")
}

function ClassifyFollowUp(message: string) -> FollowUpSignal {
  client FollowUpClassifier
  prompt #"
    An AI agent just finished a turn with outcome COMPLETE — it is not
    asking the user a question.  Decide whether the agent expects to
    revisit this task later because it is waiting on an **external**
    event that will arrive asynchronously.

    FOLLOW_UP examples:
    - "I've sent emails to five agents. I'll check back for replies."
    - "The deployment is in progress. We should verify in 30 minutes."
    - "Waiting for the webhook to fire once the payment clears."
    - "I submitted the form; the confirmation email should arrive soon."

    NONE examples:
    - "Here's the completed report."
    - "All done — the PR has been merged."
    - "I've updated the spreadsheet as requested."
    - Agent delivers a final artifact with no pending external event.

    If the intent is FOLLOW_UP, pick a **delay_minutes** that matches
    the expected wait (don't guess wildly — use common sense about how
    long emails, APIs, deployments, etc. typically take).

    Agent's message (last 1500 chars):
    ---
    {{ message }}
    ---

    {{ ctx.output_format }}
  "#
}
