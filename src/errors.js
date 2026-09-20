export class DbError extends Error {
  constructor(message, { code = 'DB_ERROR', cause } = {}) {
    super(message);
    this.name = 'DbError';
    this.code = code;
    if (cause !== undefined) this.cause = cause;
  }

  toJSON() {
    return serializeError(this);
  }
}

export class VersionError extends DbError {
  constructor(message, options = {}) {
    super(message, { code: 'VERSION_ERROR', ...options });
    this.name = 'VersionError';
  }
}

export class MigrationError extends DbError {
  constructor(message, options = {}) {
    super(message, { code: 'MIGRATION_FAILED', ...options });
    this.name = 'MigrationError';
  }
}

export class TransactionError extends DbError {
  constructor(message, options = {}) {
    super(message, { code: 'TRANSACTION_ERROR', ...options });
    this.name = 'TransactionError';
  }
}

export class AbortError extends DbError {
  constructor(message = 'Transaction was aborted', options = {}) {
    super(message, { code: 'ABORT_ERR', ...options });
    this.name = 'AbortError';
  }
}

export class WorkerError extends DbError {
  constructor(message, options = {}) {
    super(message, { code: 'WORKER_ERROR', ...options });
    this.name = 'WorkerError';
  }
}

export class TimeoutError extends DbError {
  constructor(message = 'Worker request timed out', options = {}) {
    super(message, { code: 'TIMEOUT', ...options });
    this.name = 'TimeoutError';
  }
}

export function normalizeError(error) {
  if (error instanceof DbError) return error;
  if (error instanceof Error) return new DbError(error.message, { code: error.name || 'DB_ERROR', cause: error });
  return new DbError(String(error));
}

export function serializeError(error) {
  return serializeValue(error, 0);
}

export function reviveError(value) {
  if (!value || typeof value !== 'object') return new DbError(String(value));
  const cause = value.cause ? reviveError(value.cause) : undefined;
  const message = value.message || 'Database operation failed';
  const ErrorClass = {
    VersionError,
    MigrationError,
    TransactionError,
    AbortError,
    WorkerError,
    TimeoutError,
    DbError
  }[value.name] || DbError;
  const revived = new ErrorClass(message, { code: value.code, cause });
  revived.stack = value.stack;
  return revived;
}

function serializeValue(value, depth) {
  if (value === null || typeof value !== 'object') return value;
  if (depth >= 5) return `[${value.constructor?.name || 'Object'}]`;
  if (value instanceof Error) {
    return {
      name: value.name || 'Error',
      message: value.message,
      code: value.code,
      stack: value.stack,
      cause: value.cause ? serializeValue(value.cause, depth + 1) : undefined
    };
  }
  if (Array.isArray(value)) return value.map((item) => serializeValue(item, depth + 1));
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [key, serializeValue(item, depth + 1)])
  );
}
