export const ERROR_CODES = Object.freeze({
  ABORT: 'ABORT',
  BACKUP_FAILED: 'BACKUP_FAILED',
  BLOCKED: 'BLOCKED',
  CLOSING: 'CLOSING',
  DOWNGRADE_UNSUPPORTED: 'DOWNGRADE_UNSUPPORTED',
  INVALID_CONFIG: 'INVALID_CONFIG',
  INVALID_OPERATION: 'INVALID_OPERATION',
  LOCK_TIMEOUT: 'LOCK_TIMEOUT',
  MIGRATION_CHAIN: 'MIGRATION_CHAIN',
  MIGRATION_FAILED: 'MIGRATION_FAILED',
  RECOVERY_FAILED: 'RECOVERY_FAILED',
  UNKNOWN_REQUEST: 'UNKNOWN_REQUEST',
  WORKER_RESTARTED: 'WORKER_RESTARTED'
})

export class ManagedDBError extends Error {
  constructor(message, options = {}) {
    super(message)
    this.name = 'ManagedDBError'
    this.code = options.code || 'ERROR'
    this.cause = options.cause
    this.retryable = Boolean(options.retryable)
    this.details = options.details
  }

  toJSON() {
    return {
      name: this.name,
      code: this.code,
      message: this.message,
      retryable: this.retryable,
      details: this.details,
      cause: serializeError(this.cause)
    }
  }
}

export function serializeError(error) {
  if (!error) return null
  if (error instanceof Error || error instanceof DOMException) {
    return {
      name: error.name,
      message: error.message,
      code: error.code
    }
  }
  return String(error)
}

export function toError(value) {
  if (value instanceof Error) return value
  if (value && typeof value === 'object') {
    const error = new ManagedDBError(value.message || 'IndexedDB operation failed', {
      code: value.code,
      retryable: value.retryable,
      details: value.details,
      cause: value.cause
    })
    error.name = value.name || 'ManagedDBError'
    return error
  }
  return new Error(String(value))
}

export function wrapError(error, fallbackMessage, options = {}) {
  if (error instanceof ManagedDBError) return error
  const message = error?.message || fallbackMessage
  return new ManagedDBError(message, { ...options, cause: error })
}
