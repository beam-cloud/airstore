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
    
    CRITICAL SYNTAX RULES:
    - Multi-word operator values MUST be quoted: from:"summit health" NOT from:summit health
    - Bare multi-word phrases should also be quoted: "lab results" NOT lab results
    - Unquoted spaces mean AND: lab results = lab AND results (both must appear)
    - Use OR to match alternatives: mychart OR "nyu langone" OR "summit health"
    
    Gmail search operators (use sparingly):
    - from:"name or email" - matches sender (QUOTE multi-word values!)
    - subject:"phrase" - matches subject line only
    - has:attachment, filename:pdf
    - is:unread, is:starred, is:important
    - newer_than:1d, newer_than:7d, older_than:30d
    
    QUERY STRATEGY:
    1. For topics → bare words or quoted phrases: "lab results" OR prescription OR appointment
    2. For senders → from: with quotes for multi-word: from:mychart OR from:"nyu langone"
    3. For mixed (topic + senders) → combine: mychart OR nyu OR "summit health" OR prescription
       (bare words match sender names too, so you often don't need from:)
    4. For health/medical emails → just use: medical OR health OR doctor OR appointment OR prescription OR "lab results"
    
    DEFAULTS:
    - limit: 50
    - filename_format: {date}_{from}_{subject}_{id}.txt
    
    Query examples:
    - "invoices" → invoice
    - "stripe-invoices" → invoice from:stripe
    - "receipts" → receipt OR invoice OR order
    - "meeting-notes" → meeting OR agenda OR "calendar invite"
    - "flight-confirmation" → flight OR itinerary OR "boarding pass" OR airline
    - "health-emails" → medical OR health OR doctor OR appointment OR mychart OR prescription
    - "from-john" → from:john
    - "emails-from-acme-corp" → from:"acme corp" (QUOTED because multi-word)
    - "unread" → is:unread
    - "project-alpha" → "project alpha" (quoted phrase)
    - "shipping" → shipping OR tracking OR delivery OR fedex OR ups
    - "recent" → newer_than:7d
    
    Guidance examples:
    - "anything from summit health, nyu, mychart, health related" 
      → mychart OR nyu OR "summit health" OR medical OR appointment OR prescription
      (Use bare words - they match sender names! No need for from: on each one)
    
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
       - Common cause: unquoted multi-word values like from:summit health (should be from:"summit health")
    
    2. CHECK EACH THING THE USER MENTIONED:
       - If user mentioned specific senders (e.g., "mychart", "nyu", "summit health"):
         → Are emails FROM these senders actually in the results?
         → If user mentioned 3 senders but results only show 1, that's a problem.
       - If user mentioned topics (e.g., "health", "medical", "appointments"):
         → Do the subjects/snippets actually contain these words?
         → Generic results that don't mention the topics = NOT satisfactory.
    
    3. LOOK FOR OBVIOUS MISMATCHES:
       - Results about completely unrelated topics = NOT satisfactory
       - Missing the main thing the user asked for = NOT satisfactory
    
    4. QUERY SYNTAX PROBLEMS TO FIX:
       - from:summit health → from:"summit health" (MUST quote multi-word)
       - Using too many operators when bare words would be better
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
// Tests
// -----------------------------------------------------------------------------

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
