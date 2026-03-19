class ApprovalSummary {
  summary string   @description("One plain-text sentence describing the action that needs approval. Examples: 'Send outreach email to Camila Dinamarca', 'Send 5 follow-up emails', 'Deploy v2.3.1 to production', 'Publish quarterly revenue report', 'Update CRM records for 12 contacts'")
  details string   @description("Clean, readable markdown of the content being approved, with generous spacing. Adapt the format to the content type — emails get To/Subject/Body, batch operations get a numbered list, reports get the full text, actions get a description of what will happen. No emoji, no markdown tables, no status updates, no preamble — only what the user needs to make an approve/reject decision.")
}

function ExtractApprovalSummary(assistant_text: string) -> ApprovalSummary {
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
    {{ assistant_text }}
    ---

    {{ ctx.output_format }}
  "#
}

function ExtractApprovalOutput(assistant_text: string) -> ExtractedOutput[] {
  client OutputClassifier
  prompt #"
    An AI agent is blocked waiting for human approval. Extract the durable
    artifact(s) the user is being asked to approve so they can appear in task
    outputs before approval happens.

    ONLY return outputs when the message contains the actual artifact content,
    not just a bare yes/no question.

    RETURN AN EMPTY ARRAY when:
    - The agent only asks "should I proceed?" without showing the draft/content
    - The message is a status update, question, or short acknowledgement
    - There is no durable artifact the user would want to revisit

    RETURN OUTPUTS when the message includes the full content being approved,
    for example:
    - Draft emails or replies with recipient / subject / body
    - Draft outreach sequences or follow-up plans with concrete copy
    - Draft reports, briefs, or documents awaiting approval

    EMAIL DRAFTS:
    - Set kind = EMAIL_DRAFT
    - Title should identify the draft itself, ideally by recipient or subject
    - Content should include only the draft body / material being approved,
      not the approval request wrapper
    - Use stable artifact families such as 'email-draft', 'sales-email',
      'outreach-email', or another durable family key when clearly implied
    - Include useful data_fields like recipient, subject, channel, company

    NON-EMAIL DRAFTS:
    - Set kind = REPORT
    - Persist the full draft/report content in content
    - Title and summary should describe the artifact, not the approval ask

    MULTI-ENTITY:
    - If the approval covers multiple distinct drafted artifacts, return one
      ExtractedOutput per artifact
    - Cap at 20 artifacts

    General rules:
    - Strip conversational wrappers like "should I send this?"
    - Do not invent missing recipients, subjects, or fields
    - Prefer specific, stable artifact_key values over generic ones when the
      artifact family is clear from context

    Approval context:
    ---
    {{ assistant_text }}
    ---

    {{ ctx.output_format }}
  "#
}
