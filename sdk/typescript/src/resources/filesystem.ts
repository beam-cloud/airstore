import type { CoreClient, RequestOptions } from '../client.js';
import type {
  VirtualFile,
  DirectoryListing,
  TreeListing,
  SearchResult,
  UploadUrlResponse,
  DownloadUrlResponse,
} from '../types/filesystem.js';

/**
 * Access the workspace virtual filesystem.
 *
 * The filesystem provides a unified view of all connected integrations,
 * source views, tools, and user-uploaded files.
 */
export class Filesystem {
  constructor(private readonly client: CoreClient) {}

  // ── Read operations ──────────────────────────────────────────────────

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

  /** Search files by name across the workspace. */
  async search(
    workspaceId: string,
    query: string,
    opts: { limit?: number } = {},
    options?: RequestOptions,
  ): Promise<VirtualFile[]> {
    const params: Record<string, string> = { q: query };
    if (opts.limit !== undefined) params['limit'] = String(opts.limit);

    const result = await this.client.request<SearchResult>(
      'GET',
      `/workspaces/${workspaceId}/fs/search`,
      undefined,
      params,
      options,
    );
    return (result as SearchResult).results ?? [];
  }

  /** Get a presigned download URL for a file. */
  async getDownloadUrl(
    workspaceId: string,
    path: string,
    options?: RequestOptions,
  ): Promise<string> {
    const result = await this.client.request<DownloadUrlResponse>(
      'GET',
      `/workspaces/${workspaceId}/fs/download-url`,
      undefined,
      { path },
      options,
    );
    return (result as DownloadUrlResponse).download_url;
  }

  // ── Write operations ─────────────────────────────────────────────────

  /** Get a presigned upload URL for a file. */
  async getUploadUrl(
    workspaceId: string,
    opts: { path: string; contentType?: string },
    options?: RequestOptions,
  ): Promise<UploadUrlResponse> {
    return this.client.request<UploadUrlResponse>(
      'POST',
      `/workspaces/${workspaceId}/fs/upload-url`,
      { path: opts.path, content_type: opts.contentType ?? 'application/octet-stream' },
      undefined,
      options,
    );
  }

  /** Notify the server that a presigned upload has completed. */
  async notifyUploadComplete(
    workspaceId: string,
    path: string,
    options?: RequestOptions,
  ): Promise<void> {
    await this.client.request<{ success: boolean }>(
      'POST',
      `/workspaces/${workspaceId}/fs/upload-complete`,
      { path },
      undefined,
      options,
    );
  }

  /**
   * Upload a file via presigned URL.
   * Gets a presigned URL, PUTs the data, then notifies the server.
   */
  async upload(
    workspaceId: string,
    opts: { path: string; data: Blob | ArrayBuffer; contentType?: string },
    options?: RequestOptions,
  ): Promise<void> {
    const ct = opts.contentType ?? 'application/octet-stream';
    const { upload_url } = await this.getUploadUrl(
      workspaceId,
      { path: opts.path, contentType: ct },
      options,
    );

    const res = await fetch(upload_url, {
      method: 'PUT',
      body: opts.data,
      headers: { 'Content-Type': ct },
    });
    if (!res.ok) {
      throw new Error(`Upload PUT failed (${res.status})`);
    }

    await this.notifyUploadComplete(workspaceId, opts.path, options);
  }

  /** Create a directory. */
  async mkdir(
    workspaceId: string,
    path: string,
    options?: RequestOptions,
  ): Promise<void> {
    await this.client.request<{ created: boolean; path: string }>(
      'POST',
      `/workspaces/${workspaceId}/fs/mkdir`,
      { path },
      undefined,
      options,
    );
  }

  /** Delete a file or directory. */
  async delete(
    workspaceId: string,
    path: string,
    opts: { recursive?: boolean } = {},
    options?: RequestOptions,
  ): Promise<void> {
    const params: Record<string, string> = { path };
    if (opts.recursive) params['recursive'] = 'true';

    await this.client.request<{ deleted: boolean }>(
      'DELETE',
      `/workspaces/${workspaceId}/fs/delete`,
      undefined,
      params,
      options,
    );
  }

  /** Rename or move a file or directory. */
  async rename(
    workspaceId: string,
    opts: { oldPath: string; newPath: string },
    options?: RequestOptions,
  ): Promise<void> {
    await this.client.request<{ renamed: boolean; old_path: string; new_path: string }>(
      'POST',
      `/workspaces/${workspaceId}/fs/rename`,
      { old_path: opts.oldPath, new_path: opts.newPath },
      undefined,
      options,
    );
  }
}
