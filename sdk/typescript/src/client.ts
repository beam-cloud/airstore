import { VERSION } from './version.js';
import {
  APIError,
  APIConnectionError,
  APIConnectionTimeoutError,
  AirstoreError,
} from './errors.js';

/** Per-request overrides. Every resource method accepts this as the last argument. */
export interface RequestOptions {
  /** Override the client-level timeout (ms). */
  timeout?: number;
  /** Override the client-level maxRetries. */
  maxRetries?: number;
  /** Abort signal for cancellation. */
  signal?: AbortSignal;
  /** Extra headers merged with client-level headers. */
  headers?: Record<string, string>;
}

/** Configuration for the Airstore client. */
export interface ClientOptions {
  /**
   * API key for authentication. Defaults to `process.env.AIRSTORE_API_KEY`.
   * Organization tokens start with `org_`, workspace tokens with `tok_`.
   */
  apiKey?: string;

  /**
   * Base URL for the Airstore REST API.
   * @default "https://api.airstore.ai/api/v1" or AIRSTORE_BASE_URL env
   */
  baseURL?: string;

  /**
   * Request timeout in milliseconds.
   * @default 60000
   */
  timeout?: number;

  /**
   * Maximum number of retries for failed requests (429, 5xx).
   * @default 2
   */
  maxRetries?: number;

  /** Default headers added to every request. */
  defaultHeaders?: Record<string, string>;
}

/** Metadata attached to every response object. */
export interface ResponseMeta {
  statusCode: number;
  headers: Headers;
  requestId: string | undefined;
}

export function attachResponseMeta<T extends object>(
  obj: T,
  meta: ResponseMeta,
): T & { lastResponse: ResponseMeta } {
  Object.defineProperty(obj, 'lastResponse', {
    value: meta,
    enumerable: false,
    configurable: true,
  });
  return obj as T & { lastResponse: ResponseMeta };
}

/** Retryable status codes. */
const RETRYABLE_STATUS_CODES = new Set([408, 409, 429, 500, 502, 503, 504]);

/** Base backoff in ms. */
const BASE_BACKOFF_MS = 500;
/** Max backoff in ms. */
const MAX_BACKOFF_MS = 8000;

/**
 * Core HTTP client for the Airstore API.
 *
 * Handles authentication, automatic retries with exponential backoff,
 * timeout, and per-request option overrides.
 */
export class CoreClient {
  readonly apiKey: string;
  readonly baseURL: string;
  readonly timeout: number;
  readonly maxRetries: number;
  readonly defaultHeaders: Record<string, string>;

  constructor(opts: ClientOptions = {}) {
    this.apiKey =
      opts.apiKey ??
      (typeof process !== 'undefined' ? process.env?.['AIRSTORE_API_KEY'] ?? '' : '');

    if (!this.apiKey) {
      throw new AirstoreError(
        'API key is required. Pass it via the `apiKey` option or set the AIRSTORE_API_KEY environment variable.',
      );
    }

    const envBaseURL =
      typeof process !== 'undefined'
        ? (process.env?.['AIRSTORE_BASE_URL'] ?? '')
        : '';
    this.baseURL = (
      opts.baseURL ?? (envBaseURL || 'https://api.airstore.ai/api/v1')
    ).replace(/\/+$/, '');

    this.timeout = opts.timeout ?? 60_000;
    this.maxRetries = opts.maxRetries ?? 2;
    this.defaultHeaders = opts.defaultHeaders ?? {};
  }

  /** Make an API request with retry and timeout. */
  async request<T>(
    method: string,
    path: string,
    body?: unknown,
    params?: Record<string, string>,
    reqOpts?: RequestOptions,
  ): Promise<T & { lastResponse: ResponseMeta }> {
    const url = this._buildURL(path, params);
    const timeout = reqOpts?.timeout ?? this.timeout;
    const maxRetries = reqOpts?.maxRetries ?? this.maxRetries;
    const headers = this._buildHeaders(reqOpts?.headers);

    let lastError: unknown;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      if (attempt > 0) {
        await this._sleep(this._backoffMs(attempt, lastError));
      }

      const controller = new AbortController();
      if (reqOpts?.signal) {
        reqOpts.signal.addEventListener('abort', () => controller.abort(), { once: true });
      }

      const timeoutId = setTimeout(() => controller.abort(), timeout);

      try {
        const fetchOpts: globalThis.RequestInit = {
          method,
          headers,
          signal: controller.signal,
        };

        if (body !== undefined && method !== 'GET' && method !== 'HEAD') {
          fetchOpts.body = JSON.stringify(body);
        }

        const response = await fetch(url, fetchOpts);
        clearTimeout(timeoutId);

        const meta: ResponseMeta = {
          statusCode: response.status,
          headers: response.headers,
          requestId:
            response.headers.get('x-request-id') ??
            response.headers.get('X-Request-Id') ??
            undefined,
        };

        if (response.ok) {
          const data = (await response.json()) as { success?: boolean; data?: T };
          // Airstore API wraps responses in { success, data }
          const result = (data.data ?? data) as T & object;
          return attachResponseMeta(result, meta);
        }

        // Non-retryable error
        if (!RETRYABLE_STATUS_CODES.has(response.status) || attempt === maxRetries) {
          let errorMessage = `Request failed with status ${response.status}`;
          try {
            const errBody = await response.json();
            errorMessage = (errBody as any)?.error ?? (errBody as any)?.message ?? errorMessage;
          } catch {
            // ignore parse errors
          }
          throw APIError.generate(response.status, null, errorMessage, response.headers);
        }

        // Retryable — store error and continue
        lastError = APIError.generate(
          response.status,
          null,
          `Retryable error (${response.status})`,
          response.headers,
        );
      } catch (err) {
        clearTimeout(timeoutId);

        if (err instanceof APIError) {
          if (!RETRYABLE_STATUS_CODES.has(err.status) || attempt === maxRetries) {
            throw err;
          }
          lastError = err;
          continue;
        }

        if (err instanceof DOMException && err.name === 'AbortError') {
          if (reqOpts?.signal?.aborted) {
            throw new AirstoreError('Request was cancelled');
          }
          throw new APIConnectionTimeoutError();
        }

        if (attempt === maxRetries) {
          throw new APIConnectionError(
            `Connection error: ${err instanceof Error ? err.message : String(err)}`,
            err,
          );
        }

        lastError = err;
      }
    }

    throw lastError ?? new AirstoreError('Request failed after retries');
  }

  /**
   * Build full URL with query params.
   *
   * Uses string concatenation to preserve the base path prefix (e.g. `/api/v1`).
   * `new URL(path, base)` would resolve absolute paths against the origin,
   * dropping the base path — so we avoid it.
   */
  private _buildURL(path: string, params?: Record<string, string>): string {
    const suffix = path.startsWith('/') ? path : `/${path}`;
    const url = new URL(`${this.baseURL}${suffix}`);
    if (params) {
      for (const [key, value] of Object.entries(params)) {
        if (value !== undefined && value !== '') {
          url.searchParams.set(key, value);
        }
      }
    }
    return url.toString();
  }

  /** Build request headers. */
  private _buildHeaders(extra?: Record<string, string>): Record<string, string> {
    return {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${this.apiKey}`,
      'User-Agent': `airstore-sdk-typescript/${VERSION}`,
      ...this.defaultHeaders,
      ...extra,
    };
  }

  /** Calculate backoff with jitter. */
  private _backoffMs(attempt: number, lastError: unknown): number {
    // Respect Retry-After header if present
    if (lastError instanceof APIError) {
      const retryAfter =
        lastError.headers.get('retry-after-ms') ??
        lastError.headers.get('retry-after');
      if (retryAfter) {
        const ms = Number(retryAfter);
        if (!isNaN(ms)) {
          return retryAfter.includes('.') || ms > 1000 ? ms : ms * 1000;
        }
      }
    }

    const base = BASE_BACKOFF_MS * Math.pow(2, attempt - 1);
    const jitter = Math.random() * base * 0.5;
    return Math.min(base + jitter, MAX_BACKOFF_MS);
  }

  /** Sleep helper. */
  private _sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Make a raw HTTP request to any API path.
   * Escape hatch for endpoints not yet covered by the SDK.
   */
  async rawRequest(
    method: string,
    path: string,
    opts?: { body?: unknown; params?: Record<string, string> } & RequestOptions,
  ): Promise<Response> {
    const url = this._buildURL(path, opts?.params);
    const headers = this._buildHeaders(opts?.headers);
    const controller = new AbortController();
    if (opts?.signal) {
      opts.signal.addEventListener('abort', () => controller.abort(), { once: true });
    }
    const timeoutMs = opts?.timeout ?? this.timeout;
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const fetchOpts: globalThis.RequestInit = {
        method,
        headers,
        signal: controller.signal,
      };
      if (opts?.body !== undefined) {
        fetchOpts.body = JSON.stringify(opts.body);
      }
      const resp = await fetch(url, fetchOpts);
      clearTimeout(timeoutId);
      return resp;
    } catch (err) {
      clearTimeout(timeoutId);
      throw new APIConnectionError(
        `Raw request failed: ${err instanceof Error ? err.message : String(err)}`,
        err,
      );
    }
  }
}
