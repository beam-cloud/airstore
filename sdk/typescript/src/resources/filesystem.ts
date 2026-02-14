import type { CoreClient, RequestOptions } from '../client.js';
import type { VirtualFile, DirectoryListing, TreeListing } from '../types/filesystem.js';

/**
 * Read-only access to the workspace virtual filesystem.
 *
 * The filesystem provides a unified view of all connected integrations,
 * source views, tools, and user-uploaded files.
 *
 * @example
 * ```ts
 * const listing = await airstore.fs.list("ws_abc", { path: "/" });
 * const content = await airstore.fs.read("ws_abc", {
 *   path: "/Sources/gmail/inbox/email.txt",
 * });
 * ```
 */
export class Filesystem {
  constructor(private readonly client: CoreClient) {}

  /**
   * List directory contents.
   *
   * @param workspaceId - Workspace external ID.
   * @param opts - Listing options.
   * @param opts.path - Directory path to list. Defaults to root.
   * @param options - Per-request overrides.
   * @returns Array of files and directories.
   */
  async list(
    workspaceId: string,
    opts: { path?: string } = {},
    options?: RequestOptions,
  ): Promise<VirtualFile[]> {
    const params: Record<string, string> = {};
    if (opts.path !== undefined) params['path'] = opts.path;

    const result = await this.client.request<DirectoryListing>(
      'GET',
      `/workspaces/${workspaceId}/fs/list`,
      undefined,
      params,
      options,
    );
    return (result as DirectoryListing).entries ?? [];
  }

  /**
   * Read file contents as a string.
   *
   * @param workspaceId - Workspace external ID.
   * @param opts - Read options.
   * @param opts.path - File path to read.
   * @param opts.offset - Byte offset to start reading from.
   * @param opts.length - Number of bytes to read.
   * @param opts.compression - Compression strategy: 'strip', 'distill', or 'chain'. Omit to disable.
   * @param options - Per-request overrides.
   * @returns File contents as a string.
   *
   * @throws {NotFoundError} If the file doesn't exist.
   */
  async read(
    workspaceId: string,
    opts: {
      path: string;
      offset?: number;
      length?: number;
      compression?: 'strip' | 'distill' | 'chain';
    },
    options?: RequestOptions,
  ): Promise<string> {
    const params: Record<string, string> = { path: opts.path };
    if (opts.offset !== undefined) params['offset'] = String(opts.offset);
    if (opts.length !== undefined) params['length'] = String(opts.length);
    if (opts.compression !== undefined) params['compression'] = opts.compression;

    const resp = await this.client.rawRequest(
      'GET',
      `/workspaces/${workspaceId}/fs/read`,
      { params, ...options },
    );
    return resp.text();
  }

  /**
   * Get a directory tree for efficient prefetching.
   *
   * Returns a flat list of all entries under the given path. Supports
   * pagination via `continuationToken` for large directories.
   *
   * @param workspaceId - Workspace external ID.
   * @param opts - Tree options.
   * @param opts.path - Root path for the tree. Defaults to root.
   * @param opts.maxKeys - Maximum number of entries to return.
   * @param opts.continuationToken - Token from a previous truncated response.
   * @param options - Per-request overrides.
   * @returns Tree listing with entries and pagination info.
   */
  async tree(
    workspaceId: string,
    opts: { path?: string; maxKeys?: number; continuationToken?: string } = {},
    options?: RequestOptions,
  ): Promise<TreeListing> {
    const params: Record<string, string> = {};
    if (opts.path !== undefined) params['path'] = opts.path;
    if (opts.maxKeys !== undefined) params['max_keys'] = String(opts.maxKeys);
    if (opts.continuationToken !== undefined) {
      params['continuation_token'] = opts.continuationToken;
    }

    return this.client.request<TreeListing>(
      'GET',
      `/workspaces/${workspaceId}/fs/tree`,
      undefined,
      params,
      options,
    );
  }

  /**
   * Get file or directory metadata.
   *
   * @param workspaceId - Workspace external ID.
   * @param path - Absolute path to stat.
   * @param options - Per-request overrides.
   * @returns File metadata.
   *
   * @throws {NotFoundError} If the path doesn't exist.
   */
  async stat(
    workspaceId: string,
    path: string,
    options?: RequestOptions,
  ): Promise<VirtualFile> {
    return this.client.request<VirtualFile>(
      'GET',
      `/workspaces/${workspaceId}/fs/stat`,
      undefined,
      { path },
      options,
    );
  }
}
