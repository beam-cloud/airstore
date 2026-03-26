enum OutputKind {
  FILE_CREATED   @description("Agent created a new file that is a FINAL DELIVERABLE — a report, document, asset, or artifact explicitly requested or clearly intended as a work product. Do not use this for local staging files that are only inputs to some later tool.")
  FILE_MODIFIED  @description("Agent made a SIGNIFICANT change to an important file — not routine edits, config tweaks, or dependency updates")
  EMAIL_SENT     @description("Agent sent an email via an external email API or service")
  EMAIL_DRAFT    @description("Agent drafted an email via an external email API or service")
  LINK_CREATED   @description("Agent created a NEW resource in an external system that now has its own URL — e.g. opened a pull request, created a Jira ticket, published a page, created a GitHub issue")
  API_CALL       @description("Agent created or permanently mutated a record in an external system of record via an API call — e.g. updated a database, created a deployment, posted to Slack")
  REPORT         @description("Agent produced a structured report or analysis deliverable, either saved to disk or persisted inline from the final response")
  NONE           @description("No deliverable artifact was produced")
}

class DataField {
  key string @description("Snake_case field key, e.g. 'recipe_name', 'video_url', 'total_price'")
  value string @description("The extracted value as a string. For arrays/lists, join with ', '.")
  type string @description("Display type: text, number, date, link, email, status, tags, boolean, currency")
  label string @description("Human-readable column header, e.g. 'Recipe Name', 'Video URL'")
}

class ExtractedOutput {
  kind OutputKind
  title string @description("Short human-readable title, e.g. 'Created README.md'")
  path string? @description("Local file path if a file was created or modified")
  uri string? @description("If the tool result contains a 'url', 'html_url', or 'permalink' field for a resource the agent CREATED, copy it here. Never set this for URLs that were merely visited, read, or referenced.")
  summary string? @description("One-sentence description of the artifact that was produced")
  content string? @description("Full human-readable body for REPORT outputs when the deliverable only exists inline in the response rather than as a saved file")
  artifact_key string? @description("Stable artifact family key for dashboards/views, e.g. 'recipe-pdf', 'drive-link', 'extracted-recipes'. Reuse the same key across runs for the same family. Never include instance-specific values like dates, UUIDs, or recipe titles.")
  artifact_label string? @description("Human-readable plural label for the artifact family, e.g. 'Recipe PDFs', 'Drive Links', 'Extracted Recipes'")
  artifact_kind string? @description("Short subtype like pdf, html, drive-link, report, recipe, csv")
  tags string[] @description("Categorical tags for browsing and filtering, e.g. ['italian', 'pasta', 'dinner']. Use lowercase, short descriptors. Empty array if not applicable.")
  data_fields DataField[] @description("The 5-15 most important fields extracted from the tool result for dashboard display. Empty array if kind is NONE.")
}

function ExtractOutputs(tool_name: string, tool_input: string, tool_result: string, view_columns: string) -> ExtractedOutput[] {
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

    ARTIFACT METADATA — REQUIRED when kind is not NONE:
    - artifact_key: STABLE lowercase-dash-case family key that groups equivalent
      deliverables across runs. Examples: 'recipe-pdf', 'drive-link',
      'extracted-recipes', 'github-pr', 'email-sent'. Never include instance
      values (dates, UUIDs, titles). Derive from the output type and content.
    - artifact_label: Human-readable plural label for the family. Examples:
      'Recipe PDFs', 'Drive Links', 'Extracted Recipes', 'Pull Requests'.
    - artifact_kind: Short lowercase subtype. Examples: pdf, html, csv,
      drive-link, report, recipe, email, pr, issue. For files, use the
      file extension. For links, describe the destination.

    DATA FIELDS — extract the important fields from the tool result:
    {% if view_columns %}
    VIEW SCHEMA COLUMNS — these are the columns in the user's dashboard view.
    When extracting data_fields, PREFER these exact keys and labels over
    inventing your own. For each view column where a value exists in the tool
    result, include a data_field with that column's key and label. You may also
    extract additional fields beyond the view columns if they are clearly
    important (e.g. thread_id, message_id for emails).

    {{ view_columns }}
    {% endif %}
    - Pick the 5-15 most useful fields a user would want to see in a dashboard table
    - Assign display types semantically: URLs → "link", emails → "email",
      dates/timestamps → "date", monetary values → "currency", true/false → "boolean",
      comma-separated lists → "tags", counts/quantities → "number"
    - Provide clean human-readable labels (e.g. 'Recipe Name', not 'recipe_name')
    - Skip internal/debug fields, raw HTML, large text blobs, and IDs
    - EXCEPTION: for EMAIL_SENT and EMAIL_DRAFT, ALWAYS include thread_id,
      message_id, recipient (to), and subject as data fields when they appear
      in the tool result. These are correlation IDs required for follow-up
      monitoring — they are NOT internal/debug fields.
    - For arrays of strings, join values with ", " and set type to "tags"

    TAGS — add categorical tags useful for browsing and filtering:
    - Short lowercase descriptors like "recipe", "italian", "quick", "report"
    - Derive from the content/context, not from field names

    THESE ARE NOT OUTPUTS — return kind=NONE (empty array):
    - Opening, visiting, reading, or browsing a URL or webpage
    - Writing intermediate/scratch files, scripts, or configs as part of doing work
    - Writing local staging files that are only used as inputs to a later side effect, such as email/message draft bodies, generated prompts, or temporary payload files
    - Local draft files like email-body.txt, message.txt, draft.md, or similar files under the workspace when the real artifact is an eventual email/message/API action
    - Screenshots (saving a .png/.jpg to /tmp or anywhere) — these are internal tools
    - Any file written to /tmp/ — these are ephemeral scratch files, not deliverables
    - Editing source code files (these are routine development, not deliverables)
    - Installing packages or dependencies
    - Running shell commands (git add, npm install, docker build, etc.)
    - Running tests, linters, or build tools
    - Creating/modifying config files (package.json, .env, tsconfig, etc.)
    - Searching, listing, or querying anything
    - Logging, printing, or echoing to stdout
    - ANY read-only or navigational action
    - Internal state files (JSON trackers, next-actions files, etc.)
    - Downloading or fetching content from external sources (this is INPUT, not output)
    - Saving raw API responses or data dumps to files
    - Creating directories or folder structures

    When in doubt, return an empty array. Be very conservative.

    MULTI-ENTITY DECOMPOSITION:
    When a result contains data for multiple distinct entities, return
    one ExtractedOutput per entity — NOT one aggregate output.

    Common patterns to decompose:
    - Spreadsheet/CSV with rows for different companies, people, or items
    - Batch API response with multiple records
    - Research results covering multiple topics, companies, or subjects
    - Scraped data from multiple listings, profiles, or pages
    - File containing structured data about multiple distinct things

    Each entity output should have:
    - Its own title (e.g. "Global Pointe Pricing" not "Pricing Spreadsheet")
    - Its own data_fields with entity-specific values
    - The same artifact_key to group them (e.g. all get "company-research")

    Do NOT decompose a single narrative report into sections.
    Cap at 20 entities per tool call — aggregate if more.

    Tool name: {{ tool_name }}
    Tool input (truncated): {{ tool_input }}
    Tool result (truncated): {{ tool_result }}

    {{ ctx.output_format }}
  "#
}

function ExtractFinalResponseOutput(user_message: string?, assistant_message: string, view_columns: string) -> ExtractedOutput[] {
  client OutputClassifier
  prompt #"
    An AI agent has finished a task. Decide whether the FINAL ASSISTANT
    RESPONSE itself contains a durable deliverable that should be persisted
    as a task output, even though no file or external side effect was created.

    PERSIST THE RESPONSE ONLY WHEN:
    - The response itself is the work product a user would want to revisit in
      dashboards or views
    - The response contains substantive, reusable content such as an extracted
      recipe, research brief, analysis, summary report, plan, transcript
      synthesis, structured instructions, or another polished deliverable

    DO NOT PERSIST (return an empty array):
    - Brief confirmations or acknowledgements
    - "Done", "completed", or other status-only replies
    - Questions, clarification requests, or approval requests
    - Partial progress updates or notes that are not a finished deliverable
    - Generic conversational responses with little durable value

    If the response SHOULD be persisted:
    - Set kind = REPORT
    - Set title to a short, specific human-readable title
    - Set summary to a one-sentence description of the deliverable
    - Set content to the full deliverable content, cleaned up but faithful to
      the response, preserving important headings, lists, and structure
    - Set artifact_key to a stable lowercase-dash-case family key such as
      'extracted-recipes', 'research-briefs', or 'meeting-summaries' — never
      an instance title. Derive from the content type and purpose.
    - Set artifact_label to a human-readable plural family label such as
      'Extracted Recipes' or 'Research Briefs'
    - Set artifact_kind to a short lowercase subtype like recipe, report,
      brief, summary, analysis
    - Leave path and uri empty
    - Extract data_fields from the response content — pick the 5-15 most
      important structured values a user would want in a dashboard table
    - Add categorical tags for browsing/filtering

    {% if view_columns %}
    VIEW SCHEMA COLUMNS — these are the columns in the user's dashboard view.
    When extracting data_fields, PREFER these exact keys and labels over
    inventing your own. For each view column where a value exists in the
    response, include a data_field with that column's key and label. You may
    also extract additional fields beyond the view columns if clearly important.

    {{ view_columns }}
    {% endif %}

    If the response should NOT be persisted:
    - Return an empty array

    MULTI-ENTITY DECOMPOSITION:
    When the response contains structured data about multiple distinct entities,
    return one ExtractedOutput per entity — NOT one aggregate output.

    Common patterns to decompose:
    - A list of restaurants, each with name, cuisine, address, price range
    - Research results covering multiple companies, people, or topics
    - A collection of recipes, products, or listings with per-item details
    - Recommendations or reviews for multiple distinct items

    Each entity output should have:
    - Its own title (e.g. "Bistrot Ha" not "Restaurant Guide")
    - Its own data_fields with entity-specific values (e.g. cuisine, address,
      price_range for that one restaurant)
    - Its own content with just that entity's description/details
    - The same artifact_key to group them (e.g. all get "restaurant-guides")
    - The same artifact_label and artifact_kind across all entities

    Do NOT decompose:
    - A single narrative report, essay, or analysis that isn't a list of entities
    - Content where the value is in the whole, not in individual items
    Cap at 20 entities — if more, pick the 20 most notable and aggregate the rest.

    For non-multi-entity responses, return a single-element array.

    User message / task context:
    {{ user_message }}

    Final assistant response:
    {{ assistant_message }}

    {{ ctx.output_format }}
  "#
}
