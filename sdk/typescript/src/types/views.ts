import type { OutputFormat } from './shared.js';

// ── Enums ────────────────────────────────────────────────────────────────────

/**
 * View mode: "smart" uses LLM inference, "query" uses structured filters.
 */
export type ViewMode = 'smart' | 'query';

/** Supported integration identifiers. */
export type Integration =
  | 'gmail'
  | 'github'
  | 'gdrive'
  | 'notion'
  | 'slack'
  | 'linear'
  | 'posthog'
  | 'outlook'
  | 'web';

// Gmail
export type GmailLabel =
  | 'inbox' | 'sent' | 'starred' | 'important'
  | 'drafts' | 'spam' | 'trash';

// GitHub
export type GitHubResourceType =
  | 'issues' | 'prs' | 'commits' | 'files'
  | 'releases' | 'workflows' | 'branches';
export type GitHubState = 'open' | 'closed' | 'merged';
export type GitHubContentType = 'markdown' | 'diff' | 'json' | 'raw';

// Google Drive
export type GDriveMimeType =
  | 'pdf' | 'document' | 'spreadsheet' | 'presentation' | 'image';

// Linear
export type LinearResourceType = 'issues' | 'projects';
export type LinearState =
  | 'backlog' | 'todo' | 'in_progress' | 'done' | 'canceled';
export type LinearPriority = 'urgent' | 'high' | 'medium' | 'low';

// PostHog
export type PostHogResourceType =
  | 'events' | 'feature-flags' | 'insights' | 'cohorts';

// Web / Firecrawl
export type WebMode = 'map' | 'search';

// ── Const enum objects (for runtime enumeration / dropdowns) ─────────────────

export const ViewModes = { Smart: 'smart', Query: 'query' } as const;

export const GmailLabels = [
  'inbox', 'sent', 'starred', 'important', 'drafts', 'spam', 'trash',
] as const;

export const GitHubResourceTypes = [
  'issues', 'prs', 'commits', 'files', 'releases', 'workflows', 'branches',
] as const;
export const GitHubStates = ['open', 'closed', 'merged'] as const;
export const GitHubContentTypes = ['markdown', 'diff', 'json', 'raw'] as const;

export const GDriveMimeTypes = [
  'pdf', 'document', 'spreadsheet', 'presentation', 'image',
] as const;

export const LinearResourceTypes = ['issues', 'projects'] as const;
export const LinearStates = [
  'backlog', 'todo', 'in_progress', 'done', 'canceled',
] as const;
export const LinearPriorities = ['urgent', 'high', 'medium', 'low'] as const;

export const PostHogResourceTypes = [
  'events', 'feature-flags', 'insights', 'cohorts',
] as const;

export const WebModes = ['map', 'search'] as const;

export const Integrations = [
  'gmail', 'github', 'gdrive', 'notion', 'slack', 'linear', 'posthog', 'outlook', 'web',
] as const;

// ── Per-integration filter types ──────────────────────────────────────────────

export interface GmailFilter {
  from?: string;
  to?: string;
  subject?: string;
  label?: GmailLabel;
  newer_than?: string;
  older_than?: string;
  has_attachment?: boolean;
  is_unread?: boolean;
  is_starred?: boolean;
}

export interface GitHubFilter {
  repo: string;
  type?: GitHubResourceType;
  state?: GitHubState;
  label?: string;
  author?: string;
  content_type?: GitHubContentType;
}

export interface GDriveFilter {
  name_contains?: string;
  mime_type?: GDriveMimeType;
  shared_with_me?: boolean;
  starred?: boolean;
  modified_after?: string;
  modified_before?: string;
  folder_id?: string;
}

export interface NotionFilter {
  search?: string;
}

export interface SlackFilter {
  channel?: string;
  from?: string;
  after?: string;
  before?: string;
  has_link?: boolean;
  has_reaction?: boolean;
}

export interface LinearFilter {
  type?: LinearResourceType;
  team?: string;
  state?: LinearState;
  assignee?: string;
  priority?: LinearPriority;
  label?: string;
}

export interface PostHogFilter {
  type?: PostHogResourceType;
  query?: string;
  project_id?: number;
}

export interface OutlookFilter {
  from?: string;
  to?: string;
  subject?: string;
  folder?: string;
  newer_than?: string;
  older_than?: string;
  has_attachment?: boolean;
  is_unread?: boolean;
  is_flagged?: boolean;
}

export interface WebFilter {
  mode?: WebMode;
  url?: string;
  query?: string;
  include_paths?: string[];
}

/**
 * Discriminated union of all per-integration filter types.
 * Each filter is tagged by its `integration` field so the correct
 * shape is enforced at compile time.
 */
export type ViewFilter =
  | ({ integration: 'gmail' } & GmailFilter)
  | ({ integration: 'github' } & GitHubFilter)
  | ({ integration: 'gdrive' } & GDriveFilter)
  | ({ integration: 'notion' } & NotionFilter)
  | ({ integration: 'slack' } & SlackFilter)
  | ({ integration: 'linear' } & LinearFilter)
  | ({ integration: 'posthog' } & PostHogFilter)
  | ({ integration: 'outlook' } & OutlookFilter)
  | ({ integration: 'web' } & WebFilter);

// ── View CRUD types ──────────────────────────────────────────────────────────

/**
 * Parameters for creating a source view.
 *
 * Supply `guidance` for smart mode (LLM-inferred), or `filter` for
 * query mode (structured). If `filter` is provided, `mode` defaults
 * to `"query"`; otherwise it defaults to `"smart"`.
 */
export interface ViewCreateParams {
  /** Integration source. */
  integration: Integration;
  /** Display name for the view. */
  name: string;
  /** Natural language guidance for LLM inference (smart mode). */
  guidance?: string;
  /**
   * Structured filter object (query mode).
   * Shape depends on the integration — see per-integration filter types.
   */
  filter?: Record<string, unknown>;
  /** Output format: "folder" for directory of files, "file" for single aggregated file. @default "folder" */
  outputFormat?: OutputFormat;
  /** File extension for "file" output format (e.g., ".md", ".json"). */
  fileExt?: string;
}

/**
 * Parameters for updating a source view.
 */
export interface ViewUpdateParams {
  /** New display name. */
  name?: string;
  /** New guidance for LLM inference (smart mode). */
  guidance?: string;
  /** New structured filter (query mode). */
  filter?: Record<string, unknown>;
}

/**
 * A source view in a workspace.
 *
 * Source views are materialized queries over connected data sources.
 * In "smart" mode the query is LLM-inferred from guidance text;
 * in "query" mode the query is built from a structured filter.
 */
export interface SourceView {
  /** Unique external identifier. */
  external_id: string;
  /** Integration source. */
  integration: Integration;
  /** Virtual filesystem path. */
  path: string;
  /** Display name. */
  name: string;
  /** View mode ("smart" or "query"). */
  mode: ViewMode;
  /** LLM guidance text (smart mode). */
  guidance: string;
  /** The raw query spec JSON (always present). */
  query_spec?: string;
  /** Structured filter (query mode — enables round-trip editing). */
  filter?: Record<string, unknown>;
  /** Output format. */
  output_format: string;
  /** ISO 8601 creation timestamp. */
  created_at: string;
  /** ISO 8601 last update timestamp. */
  updated_at?: string;
}

/**
 * Result of a sync operation on a source view.
 */
export interface SyncResult {
  /** The view's external ID. */
  external_id: string;
  /** Integration source. */
  integration: Integration;
  /** Virtual filesystem path. */
  path: string;
  /** View mode. */
  mode: string;
  /** ISO 8601 timestamp of the last sync. */
  last_synced_at: string;
  /** Number of total results after sync. */
  results_count: number;
  /** Number of newly discovered results since the last sync. */
  new_results: number;
}

/**
 * A selectable resource from an integration (repo, channel, etc.).
 */
export interface IntegrationResource {
  /** Unique identifier (e.g., "owner/repo", "general"). */
  id: string;
  /** Display name (e.g., "owner/repo", "#general"). */
  name: string;
}
