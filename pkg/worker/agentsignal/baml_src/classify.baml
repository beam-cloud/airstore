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

    NEEDS_INPUT + FREE_TEXT examples:
    - "Which option do you prefer? A, B, or C?"
    - "I need the API key to proceed"
    - "What email address should I use?"
    - "Could you clarify what you mean by X?"
    - "Let me know when you're ready"

    When in doubt between APPROVE_REJECT and FREE_TEXT, prefer APPROVE_REJECT
    if the agent is presenting finished work (a draft, a plan, a list of actions)
    and asking the user to greenlight it.

    COMPLETE examples:
    - Delivers a final result ("Here's the report...")
    - Confirms work is done ("Done — the file has been updated.")
    - Provides information without expecting a reply
    - Ends with a statement, not a question

    Agent's message (last 1000 chars):
    ---
    {{ message }}
    ---

    {{ ctx.output_format }}
  "#
}
