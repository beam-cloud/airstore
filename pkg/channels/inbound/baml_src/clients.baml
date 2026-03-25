client<llm> InboundRouter {
  provider anthropic
  options {
    model claude-opus-4-6
    api_key env.ANTHROPIC_API_KEY
  }
}