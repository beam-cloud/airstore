import type { OutputFormat } from './shared.js';

/**
 * Parameters for creating a smart folder.
 */
export interface SmartFolderCreateParams {
  /** Integration source (e.g., "gmail", "gdrive"). */
  integration: string;
  /** Display name for the smart folder. */
  name: string;
  /** Natural language guidance for LLM inference. */
  guidance?: string;
  /** Output format: "folder" for directory of files, "file" for single aggregated file. @default "folder" */
  outputFormat?: OutputFormat;
  /** File extension for "file" output format (e.g., ".md", ".json"). */
  fileExt?: string;
}

/**
 * Parameters for updating a smart folder.
 */
export interface SmartFolderUpdateParams {
  /** New display name. */
  name?: string;
  /** New guidance for LLM inference. */
  guidance?: string;
}

/**
 * A smart folder (filesystem query) in a workspace.
 */
export interface SmartFolder {
  /** Unique external identifier. */
  external_id: string;
  /** Integration source. */
  integration: string;
  /** Virtual filesystem path. */
  path: string;
  /** Display name. */
  name: string;
  /** LLM guidance text. */
  guidance: string;
  /** Output format. */
  output_format: string;
  /** ISO 8601 creation timestamp. */
  created_at: string;
}
