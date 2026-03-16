class ApprovalSummary {
  summary string   @description("One plain-text sentence describing the action that needs approval. Examples: 'Send outreach email to Camila Dinamarca', 'Send 5 follow-up emails', 'Deploy v2.3.1 to production', 'Publish quarterly revenue report', 'Update CRM records for 12 contacts'")
  details string   @description("Clean, readable markdown of the content being approved, with generous spacing. Adapt the format to the content type — emails get To/Subject/Body, batch operations get a numbered list, reports get the full text, actions get a description of what will happen. No emoji, no markdown tables, no status updates, no preamble — only what the user needs to make an approve/reject decision.")
}

class ApprovalItem {
  item_key string   @description("Stable slug identifier derived from the item content, e.g. 'lisa-zehner', 'talamore-woodbury', 'quarterly-report'. Use lowercase with hyphens.")
  kind OutputKind   @description("The type of action: EMAIL_DRAFT, EMAIL_SENT, FILE_CREATED, API_CALL, REPORT, LINK_CREATED, etc.")
  title string      @description("Short descriptive title, e.g. 'Email to Lisa Zehner', 'Deploy staging v2.3'")
  description string @description("One-sentence description of what this item does or contains")
  details string    @description("Clean markdown of the item content — email body, report text, action description, etc. No preamble or commentary.")
  data_fields DataField[] @description("Structured key-value fields extracted from this item: to, subject, company, amount, etc.")
}

function ExtractApprovalSummary(context: string) -> ApprovalSummary {
  client ApprovalSummarizer
  prompt #"
    An AI agent is asking a human to approve something. Below is the
    agent's recent text output. Your job: extract the specific thing
    that needs approval and present it cleanly.

    Rules for `summary`:
    - One plain sentence, no markdown formatting
    - Describe the action and scope: "Send email to X", "Send 5 outreach
      emails", "Follow up with 3 contacts", "Publish quarterly report"
    - For batch operations, include the count

    Rules for `details`:
    - Show ONLY the actual content the user is approving
    - Use generous spacing between sections — never jam content together
    - Adapt format to the content type:

    SINGLE EMAIL — use this exact format:

        **To:** Name \<email\>

        **Subject:** subject line

        ---

        Body text with paragraph breaks.

    BATCH OF EMAILS — numbered list with each recipient and subject:

        **1.** Name \<email\> — subject line

        **2.** Name \<email\> — subject line

        **3.** Name \<email\> — subject line

        If all emails share the same body, show it once after the list
        separated by a ---. If bodies differ, show a brief excerpt for each.

    FOLLOW-UP ACTIONS — describe what will happen:

        **Action:** Follow up on previous outreach

        **Recipients:** Name1, Name2, Name3

        ---

        Describe the follow-up approach and any key details.

    REPORTS / DOCUMENTS — include the full text with paragraph breaks.

    OTHER ACTIONS — describe what will happen, key parameters, and
    any side effects. Use bold labels for structure.

    General rules:
    - Strip all emoji, markdown tables, tool output, status noise
    - Do NOT add preamble — start directly with the content
    - Do NOT add commentary — only what needs to be approved
    - When the agent lists multiple items, preserve the full list

    Agent output (most recent text):
    ---
    {{ context }}
    ---

    {{ ctx.output_format }}
  "#
}

function ExtractApprovalItems(context: string) -> ApprovalItem[] {
  client ApprovalSummarizer
  prompt #"
    An AI agent is asking a human to approve one or more actions. Below
    is the agent's recent text output. Your job: decompose the approval
    request into individual actionable items that can each be independently
    approved or rejected.

    DECOMPOSITION RULES:
    - Each distinct action the agent wants to perform gets its own ApprovalItem
    - For batch emails: one item per recipient
    - For batch operations: one item per target/entity
    - For a single action: return an array of one item
    - If the agent is asking about a single decision (not per-item), return
      one item representing the whole action

    ITEM FIELDS:
    - item_key: stable slug from the content (recipient name, entity ID, etc.)
    - kind: the OutputKind that best matches (EMAIL_DRAFT for emails to send,
      FILE_CREATED for files, API_CALL for API operations, etc.)
    - title: short, specific — "Email to Lisa Zehner", not "Draft email"
    - description: one sentence about what this item does
    - details: the actual content being approved (email body, report text, etc.)
      Use clean markdown, no preamble. For emails include To/Subject/Body.
    - data_fields: structured fields like to, subject, company, amount

    Agent output (most recent text):
    ---
    {{ context }}
    ---

    {{ ctx.output_format }}
  "#
}
