client<llm> TurnClassifier {
  provider anthropic
  options {
    model claude-sonnet-4-5-20250929
    api_key env.ANTHROPIC_API_KEY
  }
}
