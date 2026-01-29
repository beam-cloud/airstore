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
    
    Gmail search operators:
    - from:, to:, subject:, cc:, bcc:
    - label:inbox, label:sent, label:draft, label:spam, label:trash
    - is:unread, is:read, is:starred, is:important, is:snoozed
    - has:attachment, has:drive, has:document, has:spreadsheet, has:youtube
    - filename:pdf, filename:doc
    - newer_than:1d, newer_than:7d, older_than:30d
    - after:2024/01/01, before:2024/12/31
    - category:primary, category:social, category:promotions, category:updates
    - in:anywhere, in:inbox, in:sent
    - larger:5M, smaller:1M
    
    Combine with AND (space), OR, NOT (-), grouping ()
    
    DEFAULTS:
    - limit: 50
    - filename_format: {date}_{from}_{subject}_{id}.txt
    
    Query examples:
    - "unread" → is:unread
    - "unread-emails" → is:unread
    - "starred" → is:starred
    - "from-john" → from:john
    - "emails-from-jane" → from:jane
    - "invoices" → subject:invoice OR label:invoice OR invoice
    - "recent" → newer_than:7d
    - "today" → newer_than:1d
    - "attachments" → has:attachment
    - "pdfs" → has:attachment filename:pdf
    
    Filename format: Choose a format that makes sense for the query intent.
    Available placeholders: {id}, {date}, {from}, {to}, {subject}, {snippet}
    - For invoices: {date}_invoice_{from}_{id}.txt
    - For emails from someone: {date}_{subject}_{id}.txt
    - For starred: {from}_{subject}_{id}.txt
    - Default: {date}_{from}_{subject}_{id}.txt
    
    Folder/file name: {{ name }}
    {% if guidance %}
    
    Additional guidance from user: {{ guidance }}
    {% endif %}
    
    {{ ctx.output_format }}
  "#
}

// -----------------------------------------------------------------------------
// Google Drive Query Inference
// -----------------------------------------------------------------------------

function InferGDriveQuery(name: string, guidance: string?) -> GDriveQueryResult {
  client AnthropicClient
  prompt #"
    Convert a folder/file name into a Google Drive search query.
    
    Drive query syntax:
    - name contains 'text'
    - fullText contains 'text'
    - mimeType = 'application/pdf'
    - mimeType = 'application/vnd.google-apps.document'
    - mimeType = 'application/vnd.google-apps.spreadsheet'
    - mimeType = 'application/vnd.google-apps.presentation'
    - mimeType contains 'image'
    - sharedWithMe = true
    - 'email@example.com' in owners
    - 'email@example.com' in writers
    - starred = true
    - trashed = false
    - modifiedTime > '2024-01-01T00:00:00'
    
    Combine with: and, or, not
    
    DEFAULTS:
    - limit: 50
    - filename_format: {name}_{id}
    
    Query examples:
    - "recent" → modifiedTime > '2024-01-01T00:00:00'
    - "shared" → sharedWithMe = true
    - "pdfs" → mimeType = 'application/pdf'
    - "documents" → mimeType = 'application/vnd.google-apps.document'
    - "spreadsheets" → mimeType = 'application/vnd.google-apps.spreadsheet'
    - "images" → mimeType contains 'image'
    - "starred" → starred = true
    
    Filename format: Choose a format that makes sense for the query intent.
    Available placeholders: {id}, {name}, {date}, {created}, {ext}
    - For documents: {name}_{id}
    - For dated files: {date}_{name}_{id}
    - Default: {name}_{id}
    
    Folder/file name: {{ name }}
    {% if guidance %}
    
    Additional guidance from user: {{ guidance }}
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

test TestNotionMeetings {
  functions [InferNotionQuery]
  args {
    name "meeting-notes"
    guidance null
  }
}
