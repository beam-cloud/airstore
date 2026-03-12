enum OutputKind {
  FILE_CREATED   @description("Agent created a new file that is a FINAL DELIVERABLE — a report, document, asset, or artifact explicitly requested or clearly intended as a work product")
  FILE_MODIFIED  @description("Agent made a SIGNIFICANT change to an important file — not routine edits, config tweaks, or dependency updates")
  EMAIL_SENT     @description("Agent sent an email via an external email API or service")
  EMAIL_DRAFT    @description("Agent drafted an email via an external email API or service")
  LINK_CREATED   @description("Agent created a NEW resource in an external system that now has its own URL — e.g. opened a pull request, created a Jira ticket, published a page, created a GitHub issue")
  API_CALL       @description("Agent created or permanently mutated a record in an external system of record via an API call — e.g. updated a database, created a deployment, posted to Slack")
  REPORT         @description("Agent produced a structured report or analysis document and saved it")
  NONE           @description("No deliverable artifact was produced")
}

class ExtractedOutput {
  kind OutputKind
  title string @description("Short human-readable title, e.g. 'Created README.md'")
  path string? @description("Local file path if a file was created or modified")
  uri string? @description("If the tool result contains a 'url', 'html_url', or 'permalink' field for a resource the agent CREATED, copy it here. Never set this for URLs that were merely visited, read, or referenced.")
  summary string? @description("One-sentence description of the artifact that was produced")
}

function ExtractOutputs(tool_name: string, tool_input: string, tool_result: string) -> ExtractedOutput[] {
  client OutputClassifier
  prompt #"
    An AI agent just executed a tool call. Your job is to determine if the
    agent PRODUCED a tangible, persistent artifact or caused a meaningful
    side effect in an external system of record.

    STRICT CRITERIA — only classify as an output if ALL of these are true:
    1. Something persistent was CREATED or MEANINGFULLY CHANGED
    2. The result is a DELIVERABLE or SIDE EFFECT, not intermediate work
    3. The action went beyond the agent's local workspace into the real world,
       OR produced a final file/document that is clearly a work product

    THESE ARE OUTPUTS:
    - Creating a pull request, issue, or ticket (the PR/issue URL is the artifact)
    - Sending an email or message through an API
    - Deploying code or creating cloud resources
    - Writing a final report, analysis, or document to disk
    - Creating or updating records in external databases or systems

    THESE ARE NOT OUTPUTS — return an empty array:
    - Opening, visiting, reading, or browsing a URL or webpage
    - Writing intermediate/scratch files, scripts, or configs as part of doing work
    - Editing source code files (these are routine development, not deliverables)
    - Installing packages or dependencies
    - Running shell commands (git add, npm install, docker build, etc.)
    - Running tests, linters, or build tools
    - Creating/modifying config files (package.json, .env, tsconfig, etc.)
    - Searching, listing, or querying anything
    - Logging, printing, or echoing to stdout
    - ANY read-only or navigational action

    When in doubt, return an empty array. Be very conservative.

    Tool name: {{ tool_name }}
    Tool input (truncated): {{ tool_input }}
    Tool result (truncated): {{ tool_result }}

    {{ ctx.output_format }}
  "#
}
