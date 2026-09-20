import { ManagedDBError, ERROR_CODES } from './errors.js'
import { requestAsPromise, transactionDone, makeKeyRange } from './idb-utils.js'

export function wrapStore(store, idb, mode, transaction) {
  return {
    name: store.name,
    keyPath: store.keyPath,
    autoIncrement: store.autoIncrement,
    createIndex(name, keyPath, options = {}) {
      store.createIndex(name, keyPath, options)
    },
    deleteIndex(name) {
      store.deleteIndex(name)
    },
    index(name) {
      return wrapIndex(store.index(name), idb)
    },
    add(...args) {
      return requestAsPromise(store.add(...args))
    },
    put(...args) {
      return requestAsPromise(store.put(...args))
    },
    get(key) {
      return requestAsPromise(store.get(key))
    },
    getAll(range) {
      const keyRange = makeKeyRange(idb, range)
      return requestAsPromise(keyRange ? store.getAll(keyRange) : store.getAll())
    },
    getAllKeys(range) {
      const keyRange = makeKeyRange(idb, range)
      return requestAsPromise(keyRange ? store.getAllKeys(keyRange) : store.getAllKeys())
    },
    delete(keyOrRange) {
      const keyRange = makeKeyRange(idb, keyOrRange)
      return requestAsPromise(store.delete(keyRange ?? keyOrRange))
    },
    clear() {
      return requestAsPromise(store.clear())
    },
    count(range) {
      const keyRange = makeKeyRange(idb, range)
      return requestAsPromise(keyRange ? store.count(keyRange) : store.count())
    },
    openCursor(range, direction) {
      return store.openCursor(makeKeyRange(idb, range), direction)
    },
    _raw: store,
    _transaction: transaction,
    _mode: mode
  }
}

function wrapIndex(index, idb) {
  return {
    name: index.name,
    keyPath: index.keyPath,
    multiEntry: index.multiEntry,
    unique: index.unique,
    get(key) {
      return requestAsPromise(index.get(key))
    },
    getAll(range) {
      const keyRange = makeKeyRange(idb, range)
      return requestAsPromise(keyRange ? index.getAll(keyRange) : index.getAll())
    },
    getAllKeys(range) {
      const keyRange = makeKeyRange(idb, range)
      return requestAsPromise(keyRange ? index.getAllKeys(keyRange) : index.getAllKeys())
    },
    count(range) {
      const keyRange = makeKeyRange(idb, range)
      return requestAsPromise(keyRange ? index.count(keyRange) : index.count())
    },
    openCursor(range, direction) {
      return index.openCursor(makeKeyRange(idb, range), direction)
    },
    _raw: index
  }
}

export function wrapVersionTransaction(transaction, database, idb) {
  return {
    db: database,
    oldVersion: null,
    newVersion: database.version,
    createObjectStore(name, options = {}) {
      const store = database.createObjectStore(name, options)
      return wrapStore(store, idb, 'versionchange', transaction)
    },
    deleteObjectStore(name) {
      database.deleteObjectStore(name)
    },
    objectStore(name) {
      return wrapStore(transaction.objectStore(name), idb, 'versionchange', transaction)
    },
    abort() {
      transaction.abort()
    },
    _raw: transaction
  }
}

export async function withTransaction(database, idb, stores, mode, callback) {
  const storeNames = [...new Set(stores)].sort()
  if (!storeNames.length) {
    throw new ManagedDBError('A transaction requires at least one object store', {
      code: ERROR_CODES.INVALID_OPERATION
    })
  }

  const missing = storeNames.filter(name => !database.objectStoreNames.contains(name))
  if (missing.length) {
    throw new ManagedDBError(`Object stores do not exist: ${missing.join(', ')}`, {
      code: ERROR_CODES.INVALID_OPERATION,
      details: { missing }
    })
  }

  const transaction = database.transaction(storeNames, mode)
  let finished = false
  const completion = transactionDone(transaction).finally(() => {
    finished = true
  })
  const storesProxy = Object.fromEntries(
    storeNames.map(name => [name, wrapStore(transaction.objectStore(name), idb, mode, transaction)])
  )

  let callbackResult
  try {
    callbackResult = await callback(storesProxy, transaction)
  } catch (error) {
    if (!finished && !error?.name?.includes?.('Abort')) {
      try {
        transaction.abort()
      } catch {}
    }
    await completion.catch(() => {})
    throw error
  }

  await completion
  return callbackResult
}
