client<llm> AnthropicClient {
  provider anthropic
  options {
    model claude-opus-4-5-20251101
    api_key env.ANTHROPIC_API_KEY
  }
}

client<llm> SmallModelCerebras {
  provider "openai-generic"
  options {
    base_url "https://api.cerebras.ai/v1"
    api_key env.CEREBRAS_API_KEY
    model "zai-glm-4.7"
  }
}