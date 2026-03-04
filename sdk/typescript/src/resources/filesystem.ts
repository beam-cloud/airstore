import type { CoreClient, RequestOptions } from '../client.js';
import type { VirtualFile, DirectoryListing, TreeListing } from '../types/filesystem.js';

/**
 * Read-only access to the workspace virtual filesystem.
 *
 * The filesystem provides a unified view of all connected integrations,
 * source views, tools, and user-uploaded files.
 */
export class Filesystem {
  constructor(private readonly client: CoreClient) {}

  /** List directory contents at the given path (defaults to root). */
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
   * Read file contents as a string. Supports byte-range reads
   * (`offset`/`length`) and server-side compression strategies.
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

  /** Get a flat directory tree. Supports pagination via `continuationToken`. */
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

  /** Get file or directory metadata. */
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
