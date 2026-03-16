enum FollowUpIntent {
  NONE       @description("Agent is fully done with this task. No revisit needed.")
  FOLLOW_UP  @description("The task should be revisited later — either the agent is waiting on an external event, or the user explicitly requested a delay/pause.")
}

class WakeAgendaItem {
  type string           @description("Short snake_case action type such as check_replies, follow_up, send_digest, verify_result, scrape, or other.")
  title string          @description("Short imperative label for what the agent should do when it wakes, e.g. 'Check inbox for new replies'.")
  reason string?        @description("Why this action belongs in the next wake agenda.")
}

class FollowUpSignal {
  intent FollowUpIntent
  delay_minutes int        @description("Exact minutes to wait before the next wake. Respect explicit user delays and active skill cadence/rules. Do not return a placeholder or backoff delay.")
  reason string?           @description("One-line summary of why the task will wake next.")
  follow_up_prompt string? @description("Instruction the agent should receive when it wakes up. It should explicitly reflect the wake agenda and any active skill rules.")
  wake_agenda WakeAgendaItem[] @description("Ordered agenda for what the agent should do on its next wake.")
}

function ClassifyFollowUp(message: string, user_message: string?, now_rfc3339: string, active_skill_context: string?, handoff_context: string?) -> FollowUpSignal {
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

    If the intent is FOLLOW_UP:

    1. Pick the exact **delay_minutes** for the next wake.
    2. Write a one-line **reason** summarizing why the task is waking.
    3. Return an ordered **wake_agenda** describing what should happen
       when it wakes. Multiple things can happen on the same wake.
    4. Return a **follow_up_prompt** that turns that agenda into a clear
       instruction for the next run.

    IMPORTANT RULES:
    - Respect explicit user-requested delays.
    - If active skill context is present, treat it as the source of truth
      for cadence and follow-up policy. Do not wake earlier than the
      skill allows.
    - If the skill implies recurring checks ("check replies every run",
      "follow up in 3 business days", "send digest every 2-3 days"),
      convert those into the next wake's exact delay and agenda.
    - If handoff context is present, preserve any still-relevant planned
      actions from it unless the just-completed turn clearly made them
      obsolete.
    - If the next wake would involve approval-gated work, include the
      drafting/review step in the agenda. Do not describe it as already sent.
    - Use concise agenda items. The title should be imperative and easy
      to show in a UI.
    - Return 0 delay_minutes, empty wake_agenda, and no follow_up_prompt
      when intent is NONE.

    Current time (RFC3339 UTC):
    ---
    {{ now_rfc3339 }}
    ---

    {% if active_skill_context %}
    Active skill context:
    ---
    {{ active_skill_context }}
    ---
    {% endif %}

    {% if handoff_context %}
    Skill-authored handoff context:
    ---
    {{ handoff_context }}
    ---
    {% endif %}

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
