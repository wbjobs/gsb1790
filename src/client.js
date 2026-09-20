import { ManagedDBError, ERROR_CODES, toError } from './core/errors.js'

let nextRequestId = 1

export class ManagedIndexedDB {
  constructor(config = {}) {
    if (!config.name) {
      throw new ManagedDBError('config.name is required', { code: ERROR_CODES.INVALID_CONFIG })
    }
    if (!config.worker && !config.workerUrl && typeof Worker === 'undefined') {
      throw new ManagedDBError('config.worker or config.workerUrl is required', {
        code: ERROR_CODES.INVALID_CONFIG
      })
    }

    this.config = config
    this.worker = null
    this.pending = new Map()
    this.opening = null
    this.closed = false
  }

  async open() {
    if (this.opening) return this.opening
    this.closed = false
    this.opening = this.startWorker()
      .then(() => this.request('open', undefined))
      .finally(() => {
        this.opening = null
      })
    return this.opening
  }

  startWorker() {
    if (this.worker) return Promise.resolve()
    return new Promise((resolve, reject) => {
      try {
        const worker = this.config.worker || new Worker(this.config.workerUrl, { type: 'module' })

        const onError = event => {
          reject(event.error || new Error(event.message || 'Worker failed to start'))
        }
        worker.addEventListener('error', onError, { once: true })

        this.attachWorker(worker)
        this.worker = worker
        queueMicrotask(() => {
          worker.removeEventListener('error', onError)
          resolve()
        })
      } catch (error) {
        reject(error)
      }
    })
  }

  attachWorker(worker) {
    worker.onmessage = event => this.handleMessage(event.data)
    worker.onmessageerror = () => this.failAll(new Error('Worker message could not be deserialized'), true)
    worker.onerror = event => {
      this.failAll(event.error || new Error(event.message || 'Worker crashed'), true)
    }
  }

  handleMessage(message) {
    if (!message || message.type !== 'managed-db:response') return
    const pending = this.pending.get(message.requestId)
    if (!pending) return
    this.pending.delete(message.requestId)
    if (message.ok) pending.resolve(message.result)
    else pending.reject(toError(message.error))
  }

  failAll(error, retryable = false) {
    const wrapped = new ManagedDBError(error.message || 'Worker restarted; operation did not commit safely', {
      code: ERROR_CODES.WORKER_RESTARTED,
      retryable,
      cause: error
    })
    for (const [, pending] of this.pending) pending.reject(wrapped)
    this.pending.clear()
    this.worker = null
  }

  async request(action, payload) {
    if (this.closed) {
      throw new ManagedDBError('Client is closed', { code: ERROR_CODES.CLOSING })
    }
    if (!this.worker) await this.open()

    const requestId = nextRequestId++
    return new Promise((resolve, reject) => {
      this.pending.set(requestId, { resolve, reject })
      this.worker.postMessage({
        type: 'managed-db:request',
        requestId,
        action,
        payload,
        config: action === 'open' ? this.workerConfig() : undefined
      })
    })
  }

  workerConfig() {
    return {
      name: this.config.name,
      backupMode: this.config.backupMode,
      lockTimeoutMs: this.config.lockTimeoutMs
    }
  }

  batch(operations) {
    return this.request('batch', { operations })
  }

  readwrite(store, operations) {
    return this.batch(operations.map(operation => ({ store, mode: 'readwrite', ...operation })))
  }

  async get(store, key) {
    const [result] = await this.batch([{ store, action: 'get', key }])
    return result
  }

  async getAll(store, range) {
    const [result] = await this.batch([{ store, action: 'getAll', range }])
    return result
  }

  async put(store, value, key) {
    const [result] = await this.batch([{ store, action: 'put', value, key }])
    return result
  }

  async add(store, value, key) {
    const [result] = await this.batch([{ store, action: 'add', value, key }])
    return result
  }

  async delete(store, key) {
    const [result] = await this.batch([{ store, action: 'delete', key }])
    return result
  }

  async count(store, range) {
    const [result] = await this.batch([{ store, action: 'count', range }])
    return result
  }

  procedure(name, input) {
    return this.request('procedure', { name, input })
  }

  async close() {
    this.closed = true
    this.failAll(new ManagedDBError('Client closed', { code: ERROR_CODES.CLOSING }), false)
    this.worker?.terminate?.()
    this.worker = null
  }
}
