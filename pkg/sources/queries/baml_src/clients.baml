client<llm> AnthropicClient {
  provider anthropic
  options {
    model claude-opus-4-5-20251101
    api_key env.ANTHROPIC_API_KEY
  }
}