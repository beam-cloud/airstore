client<llm> TurnClassifier {
  provider anthropic
  options {
    model claude-opus-4-5-20251101
    api_key env.ANTHROPIC_API_KEY
  }
}

client<llm> OutputClassifier {
  provider anthropic
  options {
    model claude-opus-4-5-20251101
    api_key env.ANTHROPIC_API_KEY
  }
}

client<llm> FollowUpClassifier {
  provider anthropic
  options {
    model claude-opus-4-5-20251101
    api_key env.ANTHROPIC_API_KEY
  }
}

client<llm> ApprovalSummarizer {
  provider anthropic
  options {
    model claude-opus-4-5-20251101
    api_key env.ANTHROPIC_API_KEY
  }
}

client<llm> FanOutClassifier {
  provider anthropic
  options {
    model claude-opus-4-5-20251101
    api_key env.ANTHROPIC_API_KEY
  }
}
