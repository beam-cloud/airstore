retry_policy Retry {
  max_retries 3
  strategy {
    type exponential_backoff
    initial_delay_ms 1000
    multiplier 2.0
    max_delay_ms 15000
  }
}

client<llm> TurnClassifier {
  provider anthropic
  retry_policy Retry
  options {
    model claude-opus-4-6
    api_key env.ANTHROPIC_API_KEY
  }
}

client<llm> OutputClassifier {
  provider anthropic
  retry_policy Retry
  options {
    model claude-opus-4-6
    api_key env.ANTHROPIC_API_KEY
  }
}

client<llm> FollowUpClassifier {
  provider anthropic
  retry_policy Retry
  options {
    model claude-opus-4-6
    api_key env.ANTHROPIC_API_KEY
  }
}

client<llm> ApprovalSummarizer {
  provider anthropic
  retry_policy Retry
  options {
    model claude-opus-4-6
    api_key env.ANTHROPIC_API_KEY
  }
}

client<llm> FanOutClassifier {
  provider anthropic
  retry_policy Retry
  options {
    model claude-opus-4-6
    api_key env.ANTHROPIC_API_KEY
  }
}
