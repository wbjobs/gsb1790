import { ManagedDatabase } from './core/managed-db.js'
import { serializeError } from './core/errors.js'

export function createManagedWorker(selfScope = globalThis, workerConfig = {}) {
  let database

  selfScope.onmessage = async event => {
    const message = event.data
    if (!message || message.type !== 'managed-db:request') return

    if (message.action === 'open') {
      try {
        database?.close?.()
        database = new ManagedDatabase({
          ...workerConfig,
          ...message.config,
          procedures: workerConfig.procedures
        })
        await database.open()
        selfScope.postMessage({
          type: 'managed-db:response',
          requestId: message.requestId,
          ok: true,
          result: { version: database.targetVersion }
        })
      } catch (error) {
        selfScope.postMessage({
          type: 'managed-db:response',
          requestId: message.requestId,
          ok: false,
          error: serializeError(error)
        })
      }
      return
    }

    if (!database) {
      selfScope.postMessage({
        type: 'managed-db:response',
        requestId: message.requestId,
        ok: false,
        error: { name: 'ManagedDBError', code: 'CLOSING', message: 'Database is not open' }
      })
      return
    }

    try {
      const result = await database.execute({
        type: message.action,
        ...message.payload
      })
      selfScope.postMessage({
        type: 'managed-db:response',
        requestId: message.requestId,
        ok: true,
        result
      })
    } catch (error) {
      selfScope.postMessage({
        type: 'managed-db:response',
        requestId: message.requestId,
        ok: false,
        error: serializeError(error)
      })
    }
  }

  return {
    get database() {
      return database
    }
  }
}
