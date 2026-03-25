client<llm> ViewCopilotClient {
  provider anthropic
  options {
    model claude-opus-4-6
    api_key env.ANTHROPIC_API_KEY
  }
}

client<llm> DetailClassifier {
  provider anthropic
  options {
    model claude-opus-4-6
    api_key env.ANTHROPIC_API_KEY
  }
}
