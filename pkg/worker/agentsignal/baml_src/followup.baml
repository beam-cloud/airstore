enum FollowUpIntent {
  NONE       @description("Agent is fully done with this task. No revisit needed.")
  FOLLOW_UP  @description("The task should be revisited later — either the agent is waiting on an external event, or the user explicitly requested a delay/pause.")
}

class FollowUpSignal {
  intent FollowUpIntent
  delay_minutes int        @description("Minutes to wait before the next check-in. Only meaningful when intent is FOLLOW_UP. Choose a reasonable interval: 5-15 min for fast APIs, 30-120 min for emails, 1440 for next-day checks, 2880 for 'a couple days', 10080 for 'next week'.")
  reason string?           @description("One line: what the agent is waiting for, e.g. 'email replies from 3 real-estate agents' or 'user requested a pause for a couple days'")
  follow_up_prompt string? @description("Instruction the agent should receive when it wakes up, e.g. 'Check inbox for new replies and respond to any that arrived'")
}

function ClassifyFollowUp(message: string, user_message: string?) -> FollowUpSignal {
  client FollowUpClassifier
  prompt #"
    An AI agent just finished a turn with outcome COMPLETE — it is not
    asking the user a question.  Decide whether the task should be
    revisited later.  There are two reasons for a FOLLOW_UP:

    1. **External event**: the agent is waiting on something asynchronous
       (email reply, webhook, deployment, data refresh, etc.).
    2. **User-requested delay**: the user's preceding message asked the
       agent to pause, wait, sleep, hold off, or come back later.

    FOLLOW_UP examples (external event):
    - "I've sent emails to five agents. I'll check back for replies."
    - "The deployment is in progress. We should verify in 30 minutes."
    - "Waiting for the webhook to fire once the payment clears."
    - "I submitted the form; the confirmation email should arrive soon."

    FOLLOW_UP examples (user-requested delay):
    - User: "let's sleep on this for a day or two" / Agent: "Sure, I'll pause."
    - User: "hold off until next week" / Agent: "Got it, I'll revisit then."
    - User: "let's pick this back up tomorrow" / Agent: "Sounds good."
    - User: "pause this for now" / Agent: "Understood, pausing."

    NONE examples:
    - "Here's the completed report."
    - "All done — the PR has been merged."
    - "I've updated the spreadsheet as requested."
    - User: "looks good, thanks!" / Agent: "Happy to help!"
    - Agent delivers a final artifact with no pending event or delay.

    If the intent is FOLLOW_UP, pick a **delay_minutes** that matches
    the expected wait (don't guess wildly — use common sense about how
    long emails, APIs, deployments, etc. typically take).  For
    user-requested delays, honour the timeframe they specified.

    {% if user_message %}
    User's preceding message:
    ---
    {{ user_message }}
    ---
    {% endif %}

    Agent's message (last 1500 chars):
    ---
    {{ message }}
    ---

    {{ ctx.output_format }}
  "#
}
