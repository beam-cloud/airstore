client<llm> ViewCopilotClient {
  provider anthropic
  options {
    model claude-opus-4-6
    api_key env.ANTHROPIC_API_KEY
    max_tokens 16384
  }
}

client<llm> FastMappingClient {
  provider "openai-generic"
  options {
    base_url "https://api.cerebras.ai/v1"
    api_key env.CEREBRAS_API_KEY
    model "gpt-oss-120b"
    temperature 0
  }
}

client<llm> DetailClassifier {
  provider anthropic
  options {
    model claude-opus-4-6
    api_key env.ANTHROPIC_API_KEY
  }
}
