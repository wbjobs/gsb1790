import { ManagedDBError } from './errors.js'

export function requestAsPromise(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error)
  })
}

export function transactionDone(transaction) {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve()
    transaction.onerror = () => reject(transaction.error)
    transaction.onabort = () => reject(transaction.error || new DOMException('Transaction aborted', 'AbortError'))
  })
}

export function openDatabase({
  idb,
  name,
  version,
  blocked,
  upgrade
}) {
  return new Promise((resolve, reject) => {
    const request = idb.open(name, version)
    let upgradePromise
    let settled = false

    const finishResolve = value => {
      if (settled) return
      settled = true
      resolve(value)
    }

    const finishReject = error => {
      if (settled) return
      settled = true
      reject(error)
    }

    request.onupgradeneeded = event => {
      try {
        const result = upgrade?.(event, request.result, request.transaction)
        upgradePromise = Promise.resolve(result)
      } catch (error) {
        request.transaction?.abort()
        finishReject(error)
      }
    }

    request.onsuccess = async () => {
      const connection = request.result
      try {
        if (upgradePromise) await upgradePromise
        finishResolve(connection)
      } catch (error) {
        try {
          request.transaction?.abort()
        } catch {}
        finishReject(error)
      }
    }

    request.onerror = () => {
      finishReject(request.error)
    }

    request.onblocked = event => {
      blocked?.(event)
    }
  })
}

export function deleteDatabase(idb, name, blocked) {
  return new Promise((resolve, reject) => {
    const request = idb.deleteDatabase(name)
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error)
    request.onblocked = event => blocked?.(event)
  })
}

export function makeKeyRange(idb, range) {
  if (range === undefined || range === null) return null
  if (typeof range !== 'object' || Array.isArray(range)) return range
  if (typeof range.includes === 'function') return range
  if (typeof globalThis.IDBKeyRange === 'undefined' && !idb) return range

  if ('only' in range) return globalThis.IDBKeyRange.only(range.only)
  if ('lower' in range && 'upper' in range) {
    return globalThis.IDBKeyRange.bound(
      range.lower,
      range.upper,
      range.lowerOpen ?? false,
      range.upperOpen ?? false
    )
  }
  if ('lower' in range) {
    return globalThis.IDBKeyRange.lowerBound(range.lower, range.lowerOpen ?? false)
  }
  if ('upper' in range) {
    return globalThis.IDBKeyRange.upperBound(range.upper, range.upperOpen ?? false)
  }
  if ('startsWith' in range) {
    const prefix = range.startsWith
    return globalThis.IDBKeyRange.bound(prefix, prefix + '\uffff', false, false)
  }

  throw new ManagedDBError('Unsupported key range', { code: 'INVALID_OPERATION' })
}

export async function getAllRecords(storeOrIndex, range) {
  const keyRange = makeKeyRange(globalThis.indexedDB, range)
  const recordsRequest = keyRange ? storeOrIndex.getAll(keyRange) : storeOrIndex.getAll()
  const keysRequest = keyRange ? storeOrIndex.getAllKeys(keyRange) : storeOrIndex.getAllKeys()
  const [values, keys] = await Promise.all([
    requestAsPromise(recordsRequest),
    requestAsPromise(keysRequest)
  ])
  return values.map((value, index) => ({ key: keys[index], value }))
}

export async function countRecords(storeOrIndex, range) {
  const keyRange = makeKeyRange(globalThis.indexedDB, range)
  return requestAsPromise(keyRange ? storeOrIndex.count(keyRange) : storeOrIndex.count())
}
