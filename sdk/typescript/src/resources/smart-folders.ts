import type { CoreClient, RequestOptions } from '../client.js';
import type {
  SmartFolder,
  SmartFolderCreateParams,
  SmartFolderUpdateParams,
} from '../types/smart-folders.js';

/**
 * Manage smart folders (filesystem queries) within a workspace.
 *
 * Smart folders use LLM inference to automatically organize and filter
 * data from connected integrations into virtual folders or files.
 *
 * @example
 * ```ts
 * await airstore.smartFolders.create("ws_abc", {
 *   integration: "gmail",
 *   name: "Recent Emails",
 *   guidance: "Last 7 days of emails",
 * });
 * ```
 */
export class SmartFolders {
  constructor(private readonly client: CoreClient) {}

  /**
   * Create a new smart folder.
   *
   * @param workspaceId - Workspace external ID.
   * @param params - Smart folder creation parameters.
   * @param options - Per-request overrides.
   * @returns The created smart folder.
   */
  async create(
    workspaceId: string,
    params: SmartFolderCreateParams,
    options?: RequestOptions,
  ): Promise<SmartFolder> {
    const body: Record<string, unknown> = {
      integration: params.integration,
      name: params.name,
      output_format: params.outputFormat ?? 'folder',
    };
    if (params.guidance !== undefined) body['guidance'] = params.guidance;
    if (params.fileExt !== undefined) body['file_ext'] = params.fileExt;

    return this.client.request<SmartFolder>(
      'POST',
      `/workspaces/${workspaceId}/fs/queries`,
      body,
      undefined,
      options,
    );
  }

  /**
   * List all smart folders in a workspace.
   *
   * @param workspaceId - Workspace external ID.
   * @param options - Per-request overrides.
   * @returns Array of smart folders.
   */
  async list(
    workspaceId: string,
    options?: RequestOptions,
  ): Promise<SmartFolder[]> {
    // API may return { queries: [...] } or a direct array depending on version
    const result = await this.client.request<SmartFolder[] | { queries: SmartFolder[] }>(
      'GET',
      `/workspaces/${workspaceId}/fs/queries/list`,
      undefined,
      undefined,
      options,
    );
    if (Array.isArray(result)) return result;
    return (result as { queries: SmartFolder[] }).queries ?? [];
  }

  /**
   * Retrieve a smart folder by its virtual filesystem path.
   *
   * @param workspaceId - Workspace external ID.
   * @param queryPath - Virtual path of the smart folder.
   * @param options - Per-request overrides.
   * @returns The smart folder.
   *
   * @throws {NotFoundError} If the smart folder doesn't exist.
   */
  async retrieve(
    workspaceId: string,
    queryPath: string,
    options?: RequestOptions,
  ): Promise<SmartFolder> {
    return this.client.request<SmartFolder>(
      'GET',
      `/workspaces/${workspaceId}/fs/queries`,
      undefined,
      { path: queryPath },
      options,
    );
  }

  /**
   * Update an existing smart folder.
   *
   * @param workspaceId - Workspace external ID.
   * @param queryId - Smart folder external ID.
   * @param params - Fields to update.
   * @param options - Per-request overrides.
   * @returns The updated smart folder.
   */
  async update(
    workspaceId: string,
    queryId: string,
    params: SmartFolderUpdateParams,
    options?: RequestOptions,
  ): Promise<SmartFolder> {
    const body: Record<string, unknown> = {};
    if (params.name !== undefined) body['name'] = params.name;
    if (params.guidance !== undefined) body['guidance'] = params.guidance;

    return this.client.request<SmartFolder>(
      'PUT',
      `/workspaces/${workspaceId}/fs/queries/${queryId}`,
      body,
      undefined,
      options,
    );
  }

  /**
   * Delete a smart folder.
   *
   * @param workspaceId - Workspace external ID.
   * @param queryId - Smart folder external ID.
   * @param options - Per-request overrides.
   *
   * @throws {NotFoundError} If the smart folder doesn't exist.
   */
  async del(
    workspaceId: string,
    queryId: string,
    options?: RequestOptions,
  ): Promise<void> {
    await this.client.request<null>(
      'DELETE',
      `/workspaces/${workspaceId}/fs/queries/${queryId}`,
      undefined,
      undefined,
      options,
    );
  }
}
