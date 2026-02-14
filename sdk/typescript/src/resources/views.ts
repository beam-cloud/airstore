import type { CoreClient, RequestOptions } from '../client.js';
import type {
  SourceView,
  ViewCreateParams,
  ViewUpdateParams,
  SyncResult,
  IntegrationResource,
} from '../types/views.js';

/**
 * Manage source views within a workspace.
 *
 * Source views are materialized queries over connected data sources.
 * In **smart** mode the query is LLM-inferred from natural language guidance;
 * in **query** mode the query is built from a structured per-integration filter.
 *
 * @example Smart mode (LLM-inferred)
 * ```ts
 * await airstore.views.create("ws_abc", {
 *   integration: "gmail",
 *   name: "Recent Emails",
 *   guidance: "Last 7 days of emails",
 * });
 * ```
 *
 * @example Query mode (structured filter)
 * ```ts
 * await airstore.views.create("ws_abc", {
 *   integration: "gmail",
 *   name: "Unread from boss",
 *   filter: { from: "boss@company.com", is_unread: true },
 * });
 * ```
 */
export class Views {
  constructor(private readonly client: CoreClient) {}

  /**
   * Create a new source view.
   *
   * If `filter` is provided the view is created in query mode;
   * otherwise it uses smart mode with LLM inference from `guidance`.
   */
  async create(
    workspaceId: string,
    params: ViewCreateParams,
    options?: RequestOptions,
  ): Promise<SourceView> {
    const body: Record<string, unknown> = {
      integration: params.integration,
      name: params.name,
      output_format: params.outputFormat ?? 'folder',
    };
    if (params.guidance !== undefined) body['guidance'] = params.guidance;
    if (params.filter !== undefined) body['filter'] = params.filter;
    if (params.fileExt !== undefined) body['file_ext'] = params.fileExt;

    return this.client.request<SourceView>(
      'POST',
      `/workspaces/${workspaceId}/fs/views`,
      body,
      undefined,
      options,
    );
  }

  /** List all source views in a workspace. */
  async list(
    workspaceId: string,
    options?: RequestOptions,
  ): Promise<SourceView[]> {
    const result = await this.client.request<{ views: SourceView[] }>(
      'GET',
      `/workspaces/${workspaceId}/fs/views/list`,
      undefined,
      undefined,
      options,
    );
    return (result as { views: SourceView[] }).views ?? [];
  }

  /** Retrieve a source view by its virtual filesystem path. */
  async retrieve(
    workspaceId: string,
    viewPath: string,
    options?: RequestOptions,
  ): Promise<SourceView> {
    return this.client.request<SourceView>(
      'GET',
      `/workspaces/${workspaceId}/fs/views`,
      undefined,
      { path: viewPath },
      options,
    );
  }

  /** Update an existing source view. */
  async update(
    workspaceId: string,
    viewId: string,
    params: ViewUpdateParams,
    options?: RequestOptions,
  ): Promise<SourceView> {
    const body: Record<string, unknown> = {};
    if (params.name !== undefined) body['name'] = params.name;
    if (params.guidance !== undefined) body['guidance'] = params.guidance;
    if (params.filter !== undefined) body['filter'] = params.filter;

    return this.client.request<SourceView>(
      'PUT',
      `/workspaces/${workspaceId}/fs/views/${viewId}`,
      body,
      undefined,
      options,
    );
  }

  /** Delete a source view. */
  async del(
    workspaceId: string,
    viewId: string,
    options?: RequestOptions,
  ): Promise<void> {
    await this.client.request<null>(
      'DELETE',
      `/workspaces/${workspaceId}/fs/views/${viewId}`,
      undefined,
      undefined,
      options,
    );
  }

  /**
   * Sync a source view — re-execute its query and refresh cached metadata.
   *
   * Idempotent and safe to call repeatedly. Returns the count of total
   * and newly discovered results.
   */
  async sync(
    workspaceId: string,
    viewId: string,
    options?: RequestOptions,
  ): Promise<SyncResult> {
    return this.client.request<SyncResult>(
      'POST',
      `/workspaces/${workspaceId}/fs/views/${viewId}/sync`,
      undefined,
      undefined,
      options,
    );
  }

  /**
   * List available resources for an integration (repos, channels, etc.).
   *
   * Used to populate filter dropdowns with real data from connected sources.
   *
   * @example
   * ```ts
   * const repos = await airstore.views.listResources("ws_abc", "github");
   * // [{ id: "owner/repo", name: "owner/repo" }, ...]
   * ```
   */
  async listResources(
    workspaceId: string,
    integration: string,
    resourceType?: string,
    options?: RequestOptions,
  ): Promise<IntegrationResource[]> {
    const params: Record<string, string> = {};
    if (resourceType) params['type'] = resourceType;

    const result = await this.client.request<{ resources: IntegrationResource[] }>(
      'GET',
      `/workspaces/${workspaceId}/fs/sources/${integration}/resources`,
      undefined,
      params,
      options,
    );
    return (result as { resources: IntegrationResource[] }).resources ?? [];
  }
}
