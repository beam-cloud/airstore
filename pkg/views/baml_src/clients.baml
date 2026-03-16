client<llm> ViewCopilotClient {
  provider anthropic
  options {
    model claude-opus-4-5-20251101
    api_key env.ANTHROPIC_API_KEY
  }
}

client<llm> OutputMapperFallbackClient {
  provider anthropic
  options {
    model claude-opus-4-5-20251101
    api_key env.ANTHROPIC_API_KEY
    temperature 0
  }
}
