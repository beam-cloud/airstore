enum TurnOutcome {
  COMPLETE       @description("The agent finished its work. The response is a final deliverable, summary, or acknowledgment.")
  NEEDS_INPUT    @description("The agent is blocked. It asked a question, requested clarification, presented options to choose from, or otherwise cannot proceed without user input.")
}

function ClassifyTurn(message: string) -> TurnOutcome {
  client TurnClassifier
  prompt #"
    An AI agent just completed a turn. Based on its final message, determine
    whether the agent is done (COMPLETE) or needs a response from the user
    (NEEDS_INPUT) before it can continue.

    NEEDS_INPUT examples:
    - Asks a question ("Which option do you prefer?")
    - Requests missing information ("I need the API key to proceed")
    - Presents choices and waits for selection
    - Asks for confirmation ("Should I go ahead and deploy?")
    - Explicitly says it's waiting ("Let me know when you're ready")

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
