// =============================================================================
// Smart Query BAML Functions
// Infer source-specific queries from folder/file names
// =============================================================================

class GmailQueryResult {
  gmail_query string @description("Gmail search query using Gmail's operators")
  limit int @description("Max results to return (default: 50)")
  filename_format string @description("Template for result filenames. Use {id}, {date}, {from}, {subject}. Example: {date}_{from}_{subject}_{id}.txt")
}

class GDriveQueryResult {
  gdrive_query string @description("Google Drive search query")
  limit int @description("Max results to return (default: 50)")
  filename_format string @description("Template for result filenames. Use {id}, {name}, {date}. Example: {name}_{id}")
}

class NotionQueryResult {
  notion_query string @description("Notion search query text")
  limit int @description("Max results to return (default: 50)")
  filename_format string @description("Template for result filenames. Use {id}, {title}, {date}. Example: {title}_{id}.md")
}

class GitHubQueryResult {
  github_query string @description("GitHub query - format depends on search_type")
  search_type string @description("Type: repos, issues, prs, commits, releases, workflows, files, branches")
  content_type string @description("Format: markdown (default), diff (PRs/commits), json (metadata), raw (files)")
  limit int @description("Max results (default: 50)")
  filename_format string @description("Filename template with placeholders like {number}, {title}, {sha}, {name}, {path}")
}

class SlackQueryResult {
  slack_query string @description("Slack search query using Slack's search syntax")
  limit int @description("Max results to return (default: 50)")
  filename_format string @description("Template for result filenames. Use {id}, {channel}, {user}, {date}, {text}. Example: {date}_{channel}_{user}_{id}.txt")
}

class LinearQueryResult {
  linear_query string @description("Linear issue filter query using Linear's filter syntax")
  search_type string @description("Type of search: 'issues' or 'projects'. Default: 'issues'")
  limit int @description("Max results to return (default: 50)")
  filename_format string @description("Template for result filenames. Use {id}, {identifier}, {title}, {state}, {assignee}, {team}, {priority}, {date}. Example: {identifier}_{title}.md")
}

class PostHogQueryResult {
  posthog_query string @description("Search query string for PostHog API")
  search_type string @description("Type of search: 'events', 'feature-flags', 'insights', 'cohorts'")
  project_id int @description("PostHog project ID to search in. Use 0 to auto-select the first available project.")
  limit int @description("Max results to return (default: 50)")
  filename_format string @description("Template for result filenames. Placeholders vary by search_type.")
}

// -----------------------------------------------------------------------------
// PostHog Query Inference
// -----------------------------------------------------------------------------

function InferPostHogQuery(name: string, guidance: string?) -> PostHogQueryResult {
  client AnthropicClient
  prompt #"
    Convert a folder/file name into a PostHog query.

    SEARCH TYPES:
    1. feature-flags — Feature flags (toggles). Use when name contains: flag, toggle, feature, rollout, experiment
    2. insights — Saved queries, funnels, retention, trends. Use when name contains: insight, funnel, retention, trend, dashboard, chart, analytics
    3. cohorts — User segments/groups. Use when name contains: cohort, segment, group, users, audience
    4. events — Raw event data (default). Use when name contains: event, pageview, click, action, or nothing specific

    POSTHOG STANDARD EVENTS (start with $):
    $pageview, $pageleave, $autocapture, $screen, $exception, $rageclick, $web_vitals

    QUERY FORMAT:
    - For events: the event name to filter on (e.g., "$pageview", "signup", "purchase"). Empty string = all recent events.
    - For feature-flags/insights/cohorts: a search term to filter by name/key (e.g., "beta", "onboarding", "retention").

    ACTIVE FLAGS:
    If the name implies "active" or "enabled" flags (e.g., "active-feature-flags", "enabled-flags"), set search_type to "feature-flags" and use a broad query (empty or relevant keyword). The provider will filter for active=true.

    DEFAULTS:
    - project_id: 0 (auto-select first project)
    - limit: 50

    FILENAME FORMATS by search_type:
    - events: {date}_{event}_{id}.json
    - feature-flags: {key}_{id}.json
    - insights: {name}_{id}.json
    - cohorts: {name}_{id}.json

    Query examples:
    - "active-feature-flags" → search_type: feature-flags, query: "" (all flags, provider filters active)
    - "pageview-events" → search_type: events, query: "$pageview"
    - "error-events" → search_type: events, query: "$exception"
    - "retention-insights" → search_type: insights, query: "retention"
    - "beta-users" → search_type: cohorts, query: "beta"
    - "signup-funnel" → search_type: insights, query: "signup funnel"
    - "recent-events" → search_type: events, query: ""
    - "experiment-flags" → search_type: feature-flags, query: "experiment"
    - "rage-clicks" → search_type: events, query: "$rageclick"
    - "web-vitals" → search_type: events, query: "$web_vitals"
    - "onboarding-cohort" → search_type: cohorts, query: "onboarding"

    Folder/file name: {{ name }}
    {% if guidance %}

    Additional guidance from user: {{ guidance }}
    {% endif %}

    {{ ctx.output_format }}
  "#
}

// -----------------------------------------------------------------------------
// Gmail Query Inference
// -----------------------------------------------------------------------------

function InferGmailQuery(name: string, guidance: string?) -> GmailQueryResult {
  client AnthropicClient
  prompt #"
    Convert a folder/file name into a Gmail search query.
    
    IMPORTANT: Gmail search is FUZZY by default!
    - Bare words (no operator) search across subject, body, AND sender name/email
    - Use bare words for most searches - they match everywhere!
    - Gmail automatically handles plurals, common misspellings, and related terms
    
    CRITICAL: PREFER BARE WORDS OVER from: FOR FUZZY SENDER MATCHING!
    - from:company only matches if that word appears exactly in sender name/email
    - Bare words match sender, subject, AND body - much more flexible!
    - Many companies use subdomains like noreply@mail.company.com - bare words catch these better
    - ONLY use from: when you need STRICT sender matching (rare)
    
    CRITICAL SYNTAX RULES:
    - Multi-word operator values MUST be quoted: from:"summit health" NOT from:summit health
    - Bare multi-word phrases should also be quoted: "lab results" NOT lab results
    - Unquoted spaces mean AND: lab results = lab AND results (both must appear)
    - Use OR to match alternatives: mychart OR "nyu langone" OR "summit health"
    
    Gmail search operators (use sparingly - bare words are usually better!):
    - from:"name or email" - STRICT sender match (avoid for fuzzy matching!)
    - subject:"phrase" - matches subject line only
    - has:attachment, filename:pdf
    - is:unread, is:starred, is:important
    - newer_than:1d, newer_than:7d, older_than:30d
    
    QUERY STRATEGY:
    1. For company/brand emails → USE BARE WORDS: nike OR adidas OR "north face"
       (This finds emails FROM them, ABOUT them, or MENTIONING them - much better coverage!)
    2. For topics → bare words or quoted phrases: "lab results" OR prescription OR appointment
    3. For people → from:firstname works but bare word also good: john OR from:john
    4. For mixed (topic + senders) → all bare words: mychart OR nyu OR "summit health" OR prescription
    5. For health/medical → medical OR health OR doctor OR appointment OR mychart OR prescription
    6. When in doubt → ADD MORE ORs with related terms, synonyms, and variations!
    
    DEFAULTS:
    - limit: 50
    - filename_format: {date}_{from}_{subject}_{id}.txt
    
    Query examples:
    - "invoices" → invoice OR receipt OR bill OR statement
    - "stripe-invoices" → stripe invoice OR stripe receipt OR stripe payment
    - "receipts" → receipt OR invoice OR order OR confirmation OR purchase
    - "meeting-notes" → meeting OR agenda OR "calendar invite" OR standup OR sync
    - "flight-confirmation" → flight OR itinerary OR "boarding pass" OR airline OR booking
    - "health-emails" → medical OR health OR doctor OR appointment OR mychart OR prescription OR lab
    - "from-john" → from:john (explicit "from" in name = use the operator)
    - "emails-from-acme-corp" → from:"acme corp" (explicit "from" + multi-word = quoted operator)
    - "unread" → is:unread
    - "project-alpha" → "project alpha" (quoted phrase)
    - "shipping" → shipping OR tracking OR delivery OR fedex OR ups OR usps OR package
    - "recent" → newer_than:7d
    - "amazon-orders" → amazon order OR amazon shipment OR amazon delivery OR amazon purchase
    - "uber-receipts" → uber receipt OR uber ride OR lyft receipt OR lyft ride
    
    Guidance examples:
    - "from nike" → nike (bare word for fuzzy matching - catches nike.com, news@nike.com, etc.)
    - "anything from summit health, nyu, mychart, health related" 
      → mychart OR nyu OR "summit health" OR medical OR appointment OR prescription OR lab
      (Use bare words with lots of ORs - they match sender names AND content!)
    - "strictly only emails where the sender is john@company.com" → from:john@company.com
      (Use from: ONLY when user explicitly wants strict sender matching)
    
    Filename format: {date}_{from}_{subject}_{id}.txt
    
    Folder/file name: {{ name }}
    {% if guidance %}
    
    Additional guidance from user: {{ guidance }}
    {% endif %}
    
    {{ ctx.output_format }}
  "#
}

// -----------------------------------------------------------------------------
// Gmail Query Evaluation and Refinement
// -----------------------------------------------------------------------------

class GmailQueryEvaluation {
  is_satisfactory bool @description("True if results match user intent well enough")
  refined_query string? @description("Improved Gmail query if not satisfactory, null if satisfactory")
  reasoning string @description("Brief explanation of evaluation (1-2 sentences)")
}

function EvaluateGmailQueryResults(
  original_guidance: string,
  generated_query: string,
  result_count: int,
  sample_results: string
) -> GmailQueryEvaluation {
  client AnthropicClient
  prompt #"
    You are a STRICT evaluator checking if Gmail search results actually match what the user asked for.
    Be critical - if results don't clearly match the user's intent, suggest a better query.
    
    USER'S REQUEST: {{ original_guidance }}
    
    CURRENT QUERY: {{ generated_query }}
    
    RESULTS: {{ result_count }} emails found
    
    SAMPLE RESULTS: {{ sample_results }}
    
    STRICT EVALUATION CHECKLIST:
    
    1. ZERO RESULTS = AUTOMATIC FAIL
       - If result_count is 0, the query is broken. Mark as NOT satisfactory.
       - Common causes:
         * Unquoted multi-word values: from:summit health → from:"summit health"
         * Using from: when bare word would match better (from:bonobos might miss noreply@e.bonobos.com)
    
    2. CHECK EACH THING THE USER MENTIONED:
       - If user mentioned specific senders/companies (e.g., "bonobos", "amazon", "stripe"):
         → Are emails FROM or ABOUT these in the results?
         → Note: bare words like "bonobos" are GOOD - they match sender, subject, AND body
         → If from: operator fails, suggest using bare word instead for fuzzy matching
       - If user mentioned topics (e.g., "health", "medical", "appointments"):
         → Do the subjects/snippets actually contain these words?
         → Generic results that don't mention the topics = NOT satisfactory.
    
    3. LOOK FOR OBVIOUS MISMATCHES:
       - Results about completely unrelated topics = NOT satisfactory
       - Missing the main thing the user asked for = NOT satisfactory
    
    4. QUERY IMPROVEMENTS TO SUGGEST:
       - from:bonobos (0 results) → bonobos (bare word matches subdomains like e.bonobos.com)
       - from:summit health → from:"summit health" (MUST quote multi-word)
       - Using from: when company uses weird email domains → use bare word instead
       - Not enough OR alternatives for fuzzy matching
    
    BE HARSH: If you're unsure whether results match, lean toward NOT satisfactory and suggest improvements.
    Only mark satisfactory if the results CLEARLY match what the user asked for.
    
    {{ ctx.output_format }}
  "#
}

// -----------------------------------------------------------------------------
// Google Drive Query Inference
// -----------------------------------------------------------------------------

function InferGDriveQuery(name: string, guidance: string?) -> GDriveQueryResult {
  client AnthropicClient
  prompt #"
    Convert a folder/file name into a Google Drive files.list query (the `q` parameter).
    
    Output MUST be ONLY the query expression (no `q=`, no URL encoding).
    It MUST be valid Drive query syntax.
    
    Drive query syntax:
    - name contains 'text'
    - fullText contains 'text'
    - mimeType = 'application/pdf'
    - mimeType = 'application/vnd.google-apps.document'
    - mimeType = 'application/vnd.google-apps.spreadsheet'
    - mimeType = 'application/vnd.google-apps.presentation'
    - mimeType contains 'image'
    - sharedWithMe = true
    - 'me' in owners
    - 'email@example.com' in owners
    - 'email@example.com' in writers
    - 'email@example.com' in readers
    - starred = true
    - trashed = false
    - modifiedTime > '2024-01-01T00:00:00'
    - createdTime > '2024-01-01T00:00:00'
    - 'FOLDER_ID' in parents
    
    Combine with: and, or, not
    
    IMPORTANT:
    - Always include `trashed = false` unless the user explicitly asks for trash / deleted files.
    - Prefer `fullText contains` for topic/keyword folders (it searches name + indexed content).
      Use `name contains` when the intent is specifically filename-based.
    - Use RFC3339 timestamps for createdTime/modifiedTime. Prefer including a timezone, e.g. '2026-01-22T00:00:00Z'.
    
    DEFAULTS:
    - limit: 50
    - filename_format: {name}_{id}
    
    Query examples:
    - "recent" → trashed = false and modifiedTime > '2026-01-22T00:00:00Z'
    - "shared" → trashed = false and sharedWithMe = true
    - "my-docs" → trashed = false and 'me' in owners and mimeType = 'application/vnd.google-apps.document'
    - "pdfs" → trashed = false and mimeType = 'application/pdf'
    - "documents" → trashed = false and mimeType = 'application/vnd.google-apps.document'
    - "spreadsheets" → trashed = false and mimeType = 'application/vnd.google-apps.spreadsheet'
    - "presentations" → trashed = false and mimeType = 'application/vnd.google-apps.presentation'
    - "images" → trashed = false and mimeType contains 'image/'
    - "starred" → trashed = false and starred = true
    - "college-docs" → trashed = false and (fullText contains 'college' or name contains 'college') and mimeType = 'application/vnd.google-apps.document'
    - Guidance: "created in the past week" → use createdTime > '...'
    - Guidance: "modified in the past week" → use modifiedTime > '...'
    
    Filename format: Choose a format that makes sense for the query intent.
    Available placeholders: {id}, {name}, {date}, {created}, {mime_type}, {ext}
    - {name} is the basename (no extension)
    - {ext} is the extension (includes leading dot)
    - For documents: {name}_{id}
    - For dated files: {date}_{name}_{id}
    - For created-time focused queries: {created}_{name}_{id}
    - Default: {name}_{id}
    
    Folder/file name: {{ name }}
    {% if guidance %}
    
    Additional guidance: {{ guidance }}
    {% endif %}
    
    {{ ctx.output_format }}
  "#
}

// -----------------------------------------------------------------------------
// Notion Query Inference
// -----------------------------------------------------------------------------

function InferNotionQuery(name: string, guidance: string?) -> NotionQueryResult {
  client AnthropicClient
  prompt #"
    Convert a folder/file name into a Notion search query.
    
    Notion search is simple text-based. Extract key search terms from the folder name.
    Remove common words like "my", "the", "all", etc.
    
    DEFAULTS:
    - limit: 50
    - filename_format: {title}_{id}.md
    
    Query examples:
    - "meeting-notes" → meeting notes
    - "project-docs" → project docs
    - "todo" → todo
    - "weekly-reports" → weekly reports
    
    Filename format: Choose a format that makes sense for the query intent.
    Available placeholders: {id}, {title}, {type}, {date}, {created}
    - For notes: {title}_{id}.md
    - For dated pages: {date}_{title}_{id}.md
    - Default: {title}_{id}.md
    
    Folder/file name: {{ name }}
    {% if guidance %}
    
    Additional guidance from user: {{ guidance }}
    {% endif %}
    
    {{ ctx.output_format }}
  "#
}

// -----------------------------------------------------------------------------
// Slack Query Inference
// -----------------------------------------------------------------------------

function InferSlackQuery(name: string, guidance: string?) -> SlackQueryResult {
  client AnthropicClient
  prompt #"
    Convert a folder/file name into a Slack search query.
    
    Slack search supports various operators for finding messages across channels.
    The query should find relevant messages based on the folder name intent.
    
    SLACK SEARCH OPERATORS:
    - in:#channel-name - messages in a specific channel
    - in:@username - direct messages with a user
    - from:@username - messages from a specific user
    - from:me - messages you sent
    - to:me - messages sent to you (DMs)
    - has:link - messages containing links
    - has:reaction - messages with reactions
    - has:star - starred messages
    - has:pin - pinned messages
    - before:YYYY-MM-DD - messages before a date
    - after:YYYY-MM-DD - messages after a date
    - during:month - messages from a specific month (e.g., during:january)
    - on:YYYY-MM-DD - messages on a specific date
    - is:saved - saved messages
    
    QUERY STRATEGY:
    1. For channel-specific queries → use in:#channel
    2. For user-specific queries → use from:@user
    3. For topic searches → use keywords directly
    4. For time-based queries → use before:, after:, during:, on:
    5. For content type → use has:link, has:reaction, etc.
    6. Combine operators with spaces (implicit AND) or OR
    
    DEFAULTS:
    - limit: 50
    - filename_format: {date}_{channel}_{user}_{id}.txt
    
    Query examples:
    - "engineering-updates" → in:#engineering OR in:#engineering-updates
    - "team-announcements" → in:#announcements OR in:#general announcement
    - "from-ceo" → from:@ceo OR from:@founder
    - "recent-links" → has:link after:2026-01-01
    - "pinned-messages" → has:pin
    - "design-feedback" → in:#design feedback OR in:#design-reviews
    - "standup-notes" → standup OR "daily standup" OR in:#standup
    - "customer-issues" → in:#support OR in:#customer-success issue OR bug
    - "product-discussions" → in:#product OR product roadmap OR feature
    - "onboarding" → onboarding OR "new hire" OR welcome
    - "decisions" → decision OR decided OR "we will" OR approved
    - "action-items" → "action item" OR TODO OR "follow up" OR assigned
    
    Filename format: Choose a format that makes sense for the query intent.
    Available placeholders: {id}, {channel}, {user}, {date}, {text}
    - For channel messages: {date}_{channel}_{user}_{id}.txt
    - For user messages: {date}_{user}_{channel}_{id}.txt
    - Default: {date}_{channel}_{user}_{id}.txt
    
    Folder/file name: {{ name }}
    {% if guidance %}
    
    Additional guidance from user: {{ guidance }}
    {% endif %}
    
    {{ ctx.output_format }}
  "#
}

// -----------------------------------------------------------------------------
// Linear Query Inference
// -----------------------------------------------------------------------------

function InferLinearQuery(name: string, guidance: string?) -> LinearQueryResult {
  client AnthropicClient
  prompt #"
    Convert natural language into a Linear search query.
    
    CORE PRINCIPLE: Text search is powerful - use it liberally. Only add filters when intent is unambiguous.
    
    SYNTAX:
    - Plain text: searches title + description (very flexible, use this a lot!)
    - assignee:name | assignee:me - who it's assigned to
    - state:backlog | state:todo | state:"in progress" | state:done | state:canceled
    - priority:1 (urgent) | priority:2 (high) | priority:3 (medium) | priority:4 (low)  
    - label:name - filter by label
    - team:KEY - filter by team key
    - creator:name - who created it
    - OR - combine alternatives (use generously!)
    
    STRATEGY - Think about what the user ACTUALLY wants:
    
    1. TOPIC/FEATURE searches → mostly text search
       "checkout bugs" → checkout bug
       "api performance" → api performance
       "mobile crashes" → mobile crash
       "auth issues" → auth authentication login
       
    2. PERSON searches → assignee filter + text backup
       "john's tickets" → assignee:john OR john
       "sarah jones work" → assignee:sarah OR assignee:jones OR sarah OR jones
       "assigned to mike" → assignee:mike
       
    3. STATUS/PRIORITY searches → filters
       "my open issues" → assignee:me state:todo OR state:"in progress"
       "urgent bugs" → priority:1 bug
       "blocked tickets" → blocked OR blocking
       "stuff in review" → review
       
    4. PROJECT/CUSTOMER searches → text search (names vary!)
       "acme corp" → acme OR "acme corp"
       "q1 roadmap" → q1 roadmap
       "enterprise features" → enterprise
       
    5. VAGUE/BROAD searches → text search with synonyms
       "tech debt" → "tech debt" OR refactor OR cleanup
       "security" → security vulnerability
       "onboarding" → onboarding OR "getting started" OR setup
    
    KEY RULES:
    - When in doubt, use text search - it's flexible
    - Use OR generously to catch variations  
    - For multi-word names: assignee:first OR assignee:last (never the full name)
    - Add synonyms for common concepts
    - Don't over-filter - better to return more results than miss things
    
    SEARCH TYPE: "issues" (default) or "projects" (only if explicitly asking for projects list)
    DEFAULTS: limit=50, filename_format={identifier}_{title}.md
    
    Folder/file name: {{ name }}
    {% if guidance %}
    Additional guidance: {{ guidance }}
    {% endif %}
    
    {{ ctx.output_format }}
  "#
}

// -----------------------------------------------------------------------------
// GitHub Query Inference
// -----------------------------------------------------------------------------

function InferGitHubQuery(name: string, guidance: string?) -> GitHubQueryResult {
  client AnthropicClient
  prompt #"
    Convert natural language into a GitHub query. Think about what an agent would need.
    
    ═══════════════════════════════════════════════════════════════════════════════
    SEARCH TYPES & QUERY FORMATS
    ═══════════════════════════════════════════════════════════════════════════════
    
    1. REPOS (search_type: "repos")
       Listing: list:org/ORGNAME or list:user/USERNAME [type:private|public]
       Search:  language:go stars:>100 topic:ml org:company
       
       Examples:
       - "beam-cloud repos" → list:org/beam-cloud
       - "private repos in acme" → list:org/acme type:private
       - "popular go projects" → language:go stars:>1000
       
    2. ISSUES (search_type: "issues")  
       Query: repo:owner/name [is:open|closed] [label:X] [assignee:X] [author:X]
       
       Examples:
       - "open bugs in beta9" → repo:beam-cloud/beta9 is:open label:bug
       - "my issues" → assignee:@me is:open
       - "security issues in react" → repo:facebook/react label:security
       
    3. PULL REQUESTS (search_type: "prs")
       Query: repo:owner/name [is:open|closed|merged] [author:X] [review:approved]
       
       Examples:
       - "open PRs in beta9" → repo:beam-cloud/beta9 is:open
       - "merged PRs this week" → repo:owner/repo is:merged merged:>2026-01-26
       - "PRs needing review" → repo:owner/repo is:open review:none
       
    4. COMMITS (search_type: "commits")
       Query: repo:owner/name [branch:X] [author:X] [since:DATE] [path:X]
       
       Examples:
       - "recent commits in beta9" → repo:beam-cloud/beta9
       - "commits to main this week" → repo:owner/repo branch:main since:2026-01-26
       - "john's commits" → repo:owner/repo author:john
       - "changes to src/api" → repo:owner/repo path:src/api
       
    5. RELEASES (search_type: "releases")
       Query: repo:owner/name [latest] [prerelease:true|false]
       
       Examples:
       - "beta9 releases" → repo:beam-cloud/beta9
       - "latest release of kubernetes" → repo:kubernetes/kubernetes latest
       - "stable releases only" → repo:owner/repo prerelease:false
       
    6. WORKFLOW RUNS (search_type: "workflows")
       Query: repo:owner/name [status:success|failure|in_progress] [branch:X] [event:push|pr]
       
       Examples:
       - "failed CI in beta9" → repo:beam-cloud/beta9 status:failure
       - "recent builds" → repo:owner/repo
       - "PR checks" → repo:owner/repo event:pull_request
       
    7. FILES (search_type: "files")
       Query: repo:owner/name path:PATH [ref:branch|tag|sha]
       
       Examples:
       - "beta9 readme" → repo:beam-cloud/beta9 path:README.md
       - "package.json in react" → repo:facebook/react path:package.json
       - "config files" → repo:owner/repo path:*.yaml ref:main
       - "src directory" → repo:owner/repo path:src/
       
    8. BRANCHES (search_type: "branches")
       Query: repo:owner/name [protected:true|false]
       
       Examples:
       - "beta9 branches" → repo:beam-cloud/beta9
       - "protected branches" → repo:owner/repo protected:true
    
    ═══════════════════════════════════════════════════════════════════════════════
    CONTENT TYPES (how to format the content)
    ═══════════════════════════════════════════════════════════════════════════════
    
    - "markdown" (default) - Rich formatted content, best for agents to read
    - "diff" - Code changes (for PRs/commits)
    - "json" - Structured metadata
    - "raw" - Unprocessed file content (for files)
    
    ═══════════════════════════════════════════════════════════════════════════════
    FILENAME FORMATS (use appropriate placeholders)
    ═══════════════════════════════════════════════════════════════════════════════
    
    Repos:     {name}.md or {full_name}.json
    Issues:    {number}_{title}.md
    PRs:       {number}_{title}.md or {number}_{title}_diff.md
    Commits:   {sha_short}_{message}.md
    Releases:  {tag}_{name}.md
    Workflows: {id}_{name}_{status}.md
    Files:     {path} (preserve original path/name)
    Branches:  {name}.md
    
    ═══════════════════════════════════════════════════════════════════════════════
    SMART DETECTION
    ═══════════════════════════════════════════════════════════════════════════════
    
    Detect intent from keywords:
    - "repos/repositories/projects" in org/user → repos listing
    - "prs/pulls/pull requests/merge requests" → prs
    - "issues/bugs/tickets/tasks" → issues
    - "commits/history/changes/log" → commits  
    - "releases/versions/tags" → releases
    - "ci/cd/builds/workflows/actions/checks" → workflows
    - "readme/config/file/code" → files
    - "branches" → branches
    - "diff/patch/changes" → content_type: diff
    
    Folder/file name: {{ name }}
    {% if guidance %}
    Additional guidance: {{ guidance }}
    {% endif %}
    
    {{ ctx.output_format }}
  "#
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

test TestGitHubOpenPRs {
  functions [InferGitHubQuery]
  args {
    name "open-prs-beta9"
    guidance null
  }
}

test TestGitHubIssues {
  functions [InferGitHubQuery]
  args {
    name "beta9-bugs"
    guidance "show open bugs in the beta9 repo"
  }
}

test TestGitHubRepos {
  functions [InferGitHubQuery]
  args {
    name "go-cli-tools"
    guidance null
  }
}

test TestGmailUnread {
  functions [InferGmailQuery]
  args {
    name "unread-emails"
    guidance null
  }
}

test TestGmailFromPerson {
  functions [InferGmailQuery]
  args {
    name "from-eli"
    guidance null
  }
}

test TestGmailWithGuidance {
  functions [InferGmailQuery]
  args {
    name "important-emails"
    guidance "Only from the last 7 days, max 100 results"
  }
}

test TestGmailInvoices {
  functions [InferGmailQuery]
  args {
    name "invoices"
    guidance null
  }
}

test TestGmailMeetingNotes {
  functions [InferGmailQuery]
  args {
    name "meeting-notes"
    guidance null
  }
}

test TestGmailFlightConfirmation {
  functions [InferGmailQuery]
  args {
    name "flight-confirmations"
    guidance null
  }
}

test TestGmailReceipts {
  functions [InferGmailQuery]
  args {
    name "receipts"
    guidance null
  }
}

test TestGmailProjectAlpha {
  functions [InferGmailQuery]
  args {
    name "project-alpha-emails"
    guidance null
  }
}

test TestGmailShippingUpdates {
  functions [InferGmailQuery]
  args {
    name "shipping-updates"
    guidance null
  }
}

test TestGmailHealthWithGuidance {
  functions [InferGmailQuery]
  args {
    name "Medical Documents"
    guidance "anything from: summit health, nyu, anything health related basically, mychart"
  }
}

test TestGmailMultiWordSender {
  functions [InferGmailQuery]
  args {
    name "acme-corp-emails"
    guidance "from acme corp and related companies"
  }
}

test TestGmailFuzzySenderNike {
  functions [InferGmailQuery]
  args {
    name "nike-emails"
    guidance "from nike"
  }
}

test TestGmailFuzzySenderAmazon {
  functions [InferGmailQuery]
  args {
    name "amazon-orders"
    guidance null
  }
}

test TestGDriveShared {
  functions [InferGDriveQuery]
  args {
    name "shared-with-me"
    guidance null
  }
}

test TestGDrivePdfs {
  functions [InferGDriveQuery]
  args {
    name "pdfs"
    guidance null
  }
}

test TestGDriveCollegeDocs {
  functions [InferGDriveQuery]
  args {
    name "college docs"
    guidance null
  }
}

test TestGDriveRecentDocsWithGuidance {
  functions [InferGDriveQuery]
  args {
    name "recent-docs"
    guidance "any docs created in the past week"
  }
}

test TestGDriveOwnedByMe {
  functions [InferGDriveQuery]
  args {
    name "my docs"
    guidance null
  }
}

test TestNotionMeetings {
  functions [InferNotionQuery]
  args {
    name "meeting-notes"
    guidance null
  }
}

test TestEvaluateGmailResults_NoResults {
  functions [EvaluateGmailQueryResults]
  args {
    original_guidance "anything from summit health, nyu, mychart, health related"
    generated_query "from:summit health OR from:nyu OR from:mychart"
    result_count 0
    sample_results "[]"
  }
}

test TestEvaluateGmailResults_GoodResults {
  functions [EvaluateGmailQueryResults]
  args {
    original_guidance "invoices from stripe"
    generated_query "invoice from:stripe"
    result_count 15
    sample_results "[{\"from\":\"Stripe\",\"subject\":\"Your invoice from Acme Corp\",\"snippet\":\"Invoice #12345...\"},{\"from\":\"Stripe Billing\",\"subject\":\"Invoice for January\",\"snippet\":\"Amount due: $99\"}]"
  }
}

test TestSlackEngineering {
  functions [InferSlackQuery]
  args {
    name "engineering-updates"
    guidance null
  }
}

test TestSlackFromCEO {
  functions [InferSlackQuery]
  args {
    name "from-ceo"
    guidance "messages from our CEO"
  }
}

test TestSlackRecentLinks {
  functions [InferSlackQuery]
  args {
    name "recent-links"
    guidance null
  }
}

test TestSlackStandups {
  functions [InferSlackQuery]
  args {
    name "standup-notes"
    guidance null
  }
}

test TestSlackDecisions {
  functions [InferSlackQuery]
  args {
    name "team-decisions"
    guidance "important decisions made by the team"
  }
}

test TestLinearMyIssues {
  functions [InferLinearQuery]
  args {
    name "my-issues"
    guidance null
  }
}

test TestLinearMyBugs {
  functions [InferLinearQuery]
  args {
    name "my-bugs"
    guidance "bugs assigned to me"
  }
}

test TestLinearUrgent {
  functions [InferLinearQuery]
  args {
    name "urgent-issues"
    guidance null
  }
}

test TestLinearTeamBacklog {
  functions [InferLinearQuery]
  args {
    name "eng-backlog"
    guidance "engineering team backlog"
  }
}

test TestLinearInProgress {
  functions [InferLinearQuery]
  args {
    name "in-progress"
    guidance null
  }
}

test TestPostHogActiveFlags {
  functions [InferPostHogQuery]
  args {
    name "active-feature-flags"
    guidance null
  }
}

test TestPostHogPageviewEvents {
  functions [InferPostHogQuery]
  args {
    name "pageview-events"
    guidance null
  }
}

test TestPostHogRetentionInsights {
  functions [InferPostHogQuery]
  args {
    name "retention-insights"
    guidance null
  }
}

test TestPostHogBetaUsers {
  functions [InferPostHogQuery]
  args {
    name "beta-users"
    guidance null
  }
}

test TestPostHogErrorEvents {
  functions [InferPostHogQuery]
  args {
    name "error-events"
    guidance "exception and error events from the past week"
  }
}

test TestPostHogSignupFunnel {
  functions [InferPostHogQuery]
  args {
    name "signup-funnel"
    guidance null
  }
}
