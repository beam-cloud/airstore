/**
 * A virtual file or directory in the workspace filesystem.
 */
export interface VirtualFile {
  /** Unique identifier. */
  id: string;
  /** File or directory name. */
  name: string;
  /** Full path. */
  path: string;
  /** MIME type. */
  type: string;
  /** Whether this is a directory. */
  is_folder: boolean;
  /** File size in bytes. */
  size: number;
  /** ISO 8601 last modified timestamp. */
  modified_at?: string;
  /** Number of children (for directories). */
  child_count?: number;
  /** Provider-specific metadata. */
  metadata?: Record<string, unknown>;
}

/**
 * Directory listing result.
 */
export interface DirectoryListing {
  entries: VirtualFile[];
}

/**
 * Tree listing result.
 */
export interface TreeListing {
  path: string;
  entries: VirtualFile[];
  truncated: boolean;
  continuation_token?: string;
}
