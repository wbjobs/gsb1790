import { ManagedDBError, ERROR_CODES } from './errors.js'

export class LocalLockManager {
  constructor() {
    this.resources = new Map()
  }

  async read(resources, mode = 'exclusive') {
    if (mode === 'shared') return this.acquire(resources, 'shared')
    return this.acquire(resources, 'exclusive')
  }

  async acquire(resources, type) {
    const normalized = [...new Set(resources)].sort()
    let waiter
    await new Promise((resolve, reject) => {
      waiter = { type, resources: normalized, resolve, reject, granted: false }
      for (const resource of normalized) {
        if (!this.resources.has(resource)) {
          this.resources.set(resource, { holders: [], queue: [] })
        }
        this.resources.get(resource).queue.push(waiter)
      }
      this.pump()
    })

    let released = false
    return () => {
      if (released) return
      released = true
      for (const resource of normalized) {
        const entry = this.resources.get(resource)
        const index = entry.holders.indexOf(waiter)
        if (index >= 0) entry.holders.splice(index, 1)
        if (!entry.holders.length && !entry.queue.length) this.resources.delete(resource)
      }
      this.pump()
    }
  }

  pump() {
    let progressed = true
    while (progressed) {
      progressed = false
      for (const [, entry] of this.resources) {
        while (entry.queue.length) {
          const waiter = entry.queue[0]
          if (waiter.granted) {
            entry.queue.shift()
            continue
          }

          const canGrant = waiter.resources.every(resource => {
            const target = this.resources.get(resource)
            if (target.queue[0] !== waiter) return false
            if (waiter.type === 'exclusive') return target.holders.length === 0
            return target.holders.every(holder => holder.type === 'shared')
          })

          if (!canGrant) break
          waiter.granted = true
          for (const resource of waiter.resources) {
            const target = this.resources.get(resource)
            target.queue.shift()
            target.holders.push(waiter)
          }
          progressed = true
          waiter.resolve()
        }
      }
    }
  }
}

export async function withWebLock(locks, resource, options, callback) {
  if (!locks) return callback()
  const timeoutMs = options?.lockTimeoutMs ?? 15000
  const controller = new AbortController()
  const timer = setTimeout(() => {
    controller.abort(new DOMException(`Timed out waiting for lock ${resource}`, 'TimeoutError'))
  }, timeoutMs)

  try {
    return await locks.request(resource, { signal: controller.signal, mode: options?.mode || 'exclusive' }, callback)
  } catch (error) {
    if (error.name === 'AbortError' || error.name === 'TimeoutError') {
      throw new ManagedDBError(`Database lock timed out: ${resource}`, {
        code: ERROR_CODES.LOCK_TIMEOUT,
        retryable: true,
        cause: error
      })
    }
    throw error
  } finally {
    clearTimeout(timer)
  }
}
