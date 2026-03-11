client<llm> CompanyPlanner {
  provider anthropic
  options {
    model claude-sonnet-4-5-20250929
    api_key env.ANTHROPIC_API_KEY
  }
}

client<llm> CompanyRouter {
  provider anthropic
  options {
    model claude-sonnet-4-5-20250929
    api_key env.ANTHROPIC_API_KEY
  }
}
