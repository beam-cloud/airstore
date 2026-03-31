retry_policy Retry {
  max_retries 3
  strategy {
    type exponential_backoff
    initial_delay_ms 1000
    multiplier 2.0
    max_delay_ms 15000
  }
}

client<llm> SkillCopilotClient {
  provider anthropic
  retry_policy Retry
  options {
    model claude-opus-4-6
    api_key env.ANTHROPIC_API_KEY
  }
}
