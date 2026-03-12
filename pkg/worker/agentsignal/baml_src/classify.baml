enum TurnOutcome {
  COMPLETE       @description("The agent finished its work. The response is a final deliverable, summary, or acknowledgment.")
  NEEDS_INPUT    @description("The agent is blocked. It asked a question, requested clarification, presented options to choose from, or otherwise cannot proceed without user input.")
}

enum InputKind {
  APPROVE_REJECT @description("Binary approval or confirmation decision: yes/no, proceed/cancel, approve/reject, should I go ahead, do you want me to, etc.")
  FREE_TEXT      @description("Open-ended input: needs information, clarification, a choice among many options, or detailed instructions.")
}

class TurnClassification {
  outcome TurnOutcome
  input_kind InputKind? @description("Only set when outcome is NEEDS_INPUT. Indicates whether the agent needs a simple approve/reject or free-text input.")
}

function ClassifyTurn(message: string) -> TurnClassification {
  client TurnClassifier
  prompt #"
    An AI agent just completed a turn. Based on its final message, determine:
    1. Whether the agent is done (COMPLETE) or needs user input (NEEDS_INPUT).
    2. If NEEDS_INPUT, whether the input is a binary approval/confirmation
       (APPROVE_REJECT) or requires free-text information (FREE_TEXT).

    NEEDS_INPUT + APPROVE_REJECT examples:
    - "Should I go ahead and deploy this?"
    - "Do you want me to proceed with option A?"
    - "Ready to merge — shall I create the PR?"
    - "I can delete these files. Should I proceed?"
    - "Would you like me to continue?"
    - "Here's the draft email. Want me to send it?"
    - "I've prepared the report. Should I publish it?"
    - "Awaiting your approval before sending."
    - "Please review and approve."
    - Presenting a draft (email, report, document) and asking to send/publish/submit
    - Any message where the user just needs to say yes/no/approve/reject to proceed
    - The agent has drafted or composed an email/message and is about to send it
    - The agent mentions it will send/submit/publish something external (email, form, API call)
      and the context suggests it hasn't done so yet or is awaiting confirmation

    NEEDS_INPUT + FREE_TEXT examples:
    - "Which option do you prefer? A, B, or C?"
    - "I need the API key to proceed"
    - "What email address should I use?"
    - "Could you clarify what you mean by X?"
    - "Let me know when you're ready"
    - "Hey! How can I help you?" (direct question expecting an answer)
    - "What would you like me to work on?" (direct question)
    - "Sure, what do you need?" (direct question)
    - "Let me know if you'd like to [specific action] or [specific action]"
      (proposes concrete next steps — this is NEEDS_INPUT, not a polite closing)
    - A direct question that requires the user to respond with a task or
      direction. The message must be actively asking — not just offering
      availability. "How can I help?" = NEEDS_INPUT. "I'm here if you
      need anything" = COMPLETE (polite closing, not a direct question).
    - "Both agents are still working — I'll report back when they're done"
    - "I've launched background tasks to check availability..."
    - "The subagents are still running. I'll let you know once they finish."
    - "Ran Task ... Async agent launched successfully."
    - Any message indicating work has been delegated to background processes,
      subagents, or async tasks that have not yet returned results.
      These are NOT complete — the agent is waiting for background work to finish.

    KEY DISTINCTION for ambiguous closings:
    - "Let me know if you'd like to draft a follow-up" → NEEDS_INPUT
      (proposes a SPECIFIC next action the agent can take)
    - "Let me know if you need anything" → COMPLETE
      (vague, no specific proposed action)
    The test: does the agent propose one or more CONCRETE actions it could
    perform next? If yes → NEEDS_INPUT. If it's just a generic offer → COMPLETE.

    When in doubt between APPROVE_REJECT and FREE_TEXT, prefer APPROVE_REJECT
    if the agent is presenting finished work (a draft, a plan, a list of actions)
    and asking the user to greenlight it.

    IMPORTANT: If the message mentions drafting, composing, preparing, OR SENDING
    an email, form submission, or outreach, classify as NEEDS_INPUT + APPROVE_REJECT.
    This applies whether the email is about to be sent OR was already sent.
    Sending external communications on behalf of the user should always require
    explicit approval. Even if the agent says "I sent the email," the user still
    needs to approve it. The only exception is if the user explicitly pre-approved
    sending in their original prompt.

    COMPLETE examples:
    - Delivers a final result with NO follow-up question or proposed actions
    - Confirms work is done ("Done — the file has been updated.")
    - Provides information without expecting a reply
    - Ends with a statement, not a question
    - ONLY these vague, generic closings (no specific actions proposed):
      "I'm here whenever you need me", "Feel free to reach out",
      "No worries! Let me know if anything comes up", "Happy to help anytime"
    - NOTE: A message saying "I'll report back" or "still working on it" is
      NOT complete if it indicates background work or subagents are still running.
    - NOTE: If the agent presents results AND THEN proposes specific next
      steps ("Let me know if you'd like to X"), that is NEEDS_INPUT, not
      COMPLETE. Results + proposed actions = NEEDS_INPUT.

    Agent's message (last ~4000 chars):
    ---
    {{ message }}
    ---

    {{ ctx.output_format }}
  "#
}
