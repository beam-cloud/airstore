/**
 * Base error for all Airstore SDK errors.
 */
export class AirstoreError extends Error {
  constructor(message: string, public readonly cause?: unknown) {
    super(message);
    this.name = 'AirstoreError';
  }
}

/**
 * Error returned by the Airstore API.
 *
 * Contains the HTTP status code, response headers, and optional request ID
 * for debugging. Use the semantic subclasses (AuthenticationError, NotFoundError, etc.)
 * for targeted catch blocks.
 */
export class APIError extends AirstoreError {
  readonly status: number;
  readonly headers: Headers;
  readonly requestId: string | undefined;

  constructor(
    status: number,
    message: string,
    headers: Headers,
    requestId: string | undefined,
  ) {
    super(message);
    this.name = 'APIError';
    this.status = status;
    this.headers = headers;
    this.requestId = requestId;
  }

  /**
   * Generate the appropriate APIError subclass for a given HTTP status.
   */
  static generate(
    status: number,
    errorBody: unknown,
    message: string,
    headers: Headers,
  ): APIError {
    const requestId =
      headers.get('x-request-id') ?? headers.get('X-Request-Id') ?? undefined;

    if (status === 401) return new AuthenticationError(message, headers, requestId);
    if (status === 403) return new PermissionDeniedError(message, headers, requestId);
    if (status === 404) return new NotFoundError(message, headers, requestId);
    if (status === 409) return new ConflictError(message, headers, requestId);
    if (status === 422) return new UnprocessableEntityError(message, headers, requestId);
    if (status === 429) return new RateLimitError(message, headers, requestId);
    if (status >= 500) return new InternalServerError(message, headers, requestId);

    return new APIError(status, message, headers, requestId);
  }
}

/** 401 - Invalid or missing API key. */
export class AuthenticationError extends APIError {
  constructor(message: string, headers: Headers, requestId: string | undefined) {
    super(401, message, headers, requestId);
    this.name = 'AuthenticationError';
  }
}

/** 403 - Token lacks permission for this operation. */
export class PermissionDeniedError extends APIError {
  constructor(message: string, headers: Headers, requestId: string | undefined) {
    super(403, message, headers, requestId);
    this.name = 'PermissionDeniedError';
  }
}

/** 404 - Resource not found. */
export class NotFoundError extends APIError {
  constructor(message: string, headers: Headers, requestId: string | undefined) {
    super(404, message, headers, requestId);
    this.name = 'NotFoundError';
  }
}

/** 409 - Conflicting operation. */
export class ConflictError extends APIError {
  constructor(message: string, headers: Headers, requestId: string | undefined) {
    super(409, message, headers, requestId);
    this.name = 'ConflictError';
  }
}

/** 422 - Validation failed. */
export class UnprocessableEntityError extends APIError {
  constructor(message: string, headers: Headers, requestId: string | undefined) {
    super(422, message, headers, requestId);
    this.name = 'UnprocessableEntityError';
  }
}

/** 429 - Rate limit exceeded. Retry after the period indicated by Retry-After header. */
export class RateLimitError extends APIError {
  constructor(message: string, headers: Headers, requestId: string | undefined) {
    super(429, message, headers, requestId);
    this.name = 'RateLimitError';
  }
}

/** 5xx - Server-side error. Safe to retry with backoff. */
export class InternalServerError extends APIError {
  constructor(message: string, headers: Headers, requestId: string | undefined) {
    super(500, message, headers, requestId);
    this.name = 'InternalServerError';
  }
}

/** Network-level connection failure (DNS, TCP, TLS). */
export class APIConnectionError extends AirstoreError {
  constructor(message: string, cause?: unknown) {
    super(message, cause);
    this.name = 'APIConnectionError';
  }
}

/** Request timed out. */
export class APIConnectionTimeoutError extends APIConnectionError {
  constructor(message: string = 'Request timed out') {
    super(message);
    this.name = 'APIConnectionTimeoutError';
  }
}
