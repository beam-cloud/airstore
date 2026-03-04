import type { CoreClient, RequestOptions } from '../client.js';
import type {
  SendDirectAgentMessageParams,
  SendDirectAgentMessageResponse,
  SendDirectRunMessageParams,
  SendDirectRunMessageResponse,
} from '../types/channels.js';
import { toInputProvenanceBody, toPolicyBody, toRoutingBody } from './helpers.js';

/**
 * Send direct messages to agents and runs via named channels.
 *
 * Use `sendDirectAgentMessage` to start a new task by messaging an agent, and
 * `sendDirectRunMessage` to send follow-up input to an existing run (e.g.
 * follow-up, steer, or interrupt).
 */
export class Channels {
  constructor(private readonly client: CoreClient) {}

  /** Send a message to an agent, creating a new task. Equivalent to tasks.create via the direct channel. */
  async sendDirectAgentMessage(
    workspaceId: string,
    agentId: string,
    params: SendDirectAgentMessageParams,
    options?: RequestOptions,
  ): Promise<SendDirectAgentMessageResponse> {
    return this.client.request<SendDirectAgentMessageResponse>(
      'POST',
      `/workspaces/${workspaceId}/channels/direct/agents/${agentId}/messages`,
      {
        message: params.message,
        session_id: params.sessionId,
        session_key: params.sessionKey,
        deliver: params.deliver,
        timeout_ms: params.timeoutMs,
        policy: toPolicyBody(params.policy),
        lane: params.lane,
        extra_system_prompt: params.extraSystemPrompt,
        input_provenance: toInputProvenanceBody(params.inputProvenance),
        routing: toRoutingBody(params.routing),
        attachments: params.attachments,
        idempotency_key: params.idempotencyKey,
        label: params.label,
        spawned_by: params.spawnedBy,
      },
      undefined,
      options,
    );
  }

  /**
   * Send a follow-up message to an active run. The `queueMode` controls
   * delivery: `followup`, `steer`, `interrupt`, or `queue`.
   */
  async sendDirectRunMessage(
    workspaceId: string,
    runId: string,
    params: SendDirectRunMessageParams,
    options?: RequestOptions,
  ): Promise<SendDirectRunMessageResponse> {
    return this.client.request<SendDirectRunMessageResponse>(
      'POST',
      `/workspaces/${workspaceId}/channels/direct/runs/${runId}/messages`,
      {
        message: params.message,
        task_id: params.taskId,
        queue_mode: params.queueMode,
        idempotency_key: params.idempotencyKey,
      },
      undefined,
      options,
    );
  }
}
