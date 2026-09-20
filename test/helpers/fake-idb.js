class FakeIDBKeyRange {
  constructor(spec) { Object.assign(this, spec) }
  static only(value) { return new FakeIDBKeyRange({ only: value }) }
  static lowerBound(value, open = false) { return new FakeIDBKeyRange({ lower: value, lowerOpen: open }) }
  static upperBound(value, open = false) { return new FakeIDBKeyRange({ upper: value, upperOpen: open }) }
  static bound(lower, upper, lowerOpen = false, upperOpen = false) {
    return new FakeIDBKeyRange({ lower, upper, lowerOpen, upperOpen })
  }
}

const clone = value => structuredClone(value)
const compare = (left, right) => left < right ? -1 : left > right ? 1 : 0

function inRange(key, range) {
  if (range?.only !== undefined) return key === range.only
  if (range?.lower !== undefined) {
    const result = compare(key, range.lower)
    if (result < 0 || (result === 0 && range.lowerOpen)) return false
  }
  if (range?.upper !== undefined) {
    const result = compare(key, range.upper)
    if (result > 0 || (result === 0 && range.upperOpen)) return false
  }
  return true
}

class FakeRequest {
  constructor(source = null) { this.source = source; this.result = null; this.error = null }
  succeed(result) { this.result = result; queueMicrotask(() => this.onsuccess?.({ target: this })) }
  fail(error) { this.error = error; queueMicrotask(() => this.onerror?.({ target: this })) }
}

class FakeObjectStore {
  constructor(transaction, schema, database) {
    this.transaction = transaction
    this.schema = schema
    this.database = database
    this.keyPath = schema.keyPath
    this.autoIncrement = schema.autoIncrement
    this.name = schema.name
    this.indexNames = {
      contains: name => schema.indexes.some(index => index.name === name),
      [Symbol.iterator]: () => schema.indexes.map(index => index.name)[Symbol.iterator]()
    }
  }
  createIndex(name, keyPath, options = {}) {
    this.schema.indexes.push({ name, keyPath, unique: !!options.unique, multiEntry: !!options.multiEntry })
  }
  deleteIndex(name) { this.schema.indexes = this.schema.indexes.filter(index => index.name !== name) }
  index(name) { const schema = this.schema.indexes.find(item => item.name === name); return { name, keyPath: schema.keyPath, unique: schema.unique, multiEntry: schema.multiEntry } }
  resolveKey(value, explicitKey) {
    if (this.keyPath) return this.keyPath.split('.').reduce((target, part) => target[part], value)
    if (explicitKey !== undefined) return explicitKey
    return ++this.schema.nextKey
  }
  add(value, key) { return this.write(value, key, false) }
  put(value, key) { return this.write(value, key, true) }
  write(value, explicitKey, overwrite) {
    const request = new FakeRequest(this)
    try {
      const key = this.resolveKey(value, explicitKey)
      if (!overwrite && this.transaction.data.get(this.name)?.has(key)) throw new DOMException('ConstraintError', 'ConstraintError')
      this.transaction.ensureStore(this.name).set(key, clone(value))
      request.succeed(key)
      this.transaction.scheduleCommit()
    } catch (error) {
      request.fail(error)
      this.transaction.abort(error)
    }
    return request
  }
  get(key) {
    const request = new FakeRequest(this)
    const value = this.transaction.data.get(this.name)?.get(key)
    request.succeed(value === undefined ? undefined : clone(value))
    this.transaction.scheduleCommit()
    return request
  }
  entries(range) {
    const entries = [...(this.transaction.data.get(this.name) || [])].sort((a, b) => compare(a[0], b[0]))
    return range ? entries.filter(([key]) => inRange(key, range)) : entries
  }
  getAll(range) { const request = new FakeRequest(this); request.succeed(this.entries(range).map(([, value]) => clone(value))); this.transaction.scheduleCommit(); return request }
  getAllKeys(range) { const request = new FakeRequest(this); request.succeed(this.entries(range).map(([key]) => key)); this.transaction.scheduleCommit(); return request }
  count(range) { const request = new FakeRequest(this); request.succeed(this.entries(range).length); this.transaction.scheduleCommit(); return request }
  clear() { const request = new FakeRequest(this); this.transaction.ensureStore(this.name).clear(); request.succeed(undefined); this.transaction.scheduleCommit(); return request }
  delete(keyOrRange) {
    const request = new FakeRequest(this)
    if (keyOrRange instanceof FakeIDBKeyRange) for (const [key] of this.entries(keyOrRange)) this.transaction.ensureStore(this.name).delete(key)
    else this.transaction.ensureStore(this.name).delete(keyOrRange)
    request.succeed(undefined)
    this.transaction.scheduleCommit()
    return request
  }
}

class FakeTransaction {
  constructor(factory, database, storeNames, mode) {
    this.factory = factory
    this.db = database
    this.storeNames = [...storeNames]
    this.mode = mode
    this.error = null
    this.data = new Map()
    this.stores = new Map()
    this.finished = false
    this.commitToken = 0
    for (const name of this.storeNames) {
      if (!this.data.has(name)) {
        this.data.set(name, new Map([...(database.data.get(name) || [])].map(([key, value]) => [key, clone(value)])))
        this.stores.set(name, new FakeObjectStore(this, database.schemas.get(name), database))
      }
    }
  }
  objectStore(name) { return this.stores.get(name) }
  ensureStore(name) { if (!this.data.has(name)) this.data.set(name, new Map()); return this.data.get(name) }
  scheduleCommit() {
    if (this.commitScheduled || this.finished) return
    this.commitScheduled = true
    setTimeout(() => {
      this.commitScheduled = false
      if (!this.finished) this.commit()
    }, 0)
  }
  abort(error = new DOMException('AbortError', 'AbortError')) {
    if (this.finished) return
    this.finished = true
    this.error = error
    queueMicrotask(() => this.onabort?.({ target: this }))
  }
  commit() {
    if (this.finished) return
    this.finished = true
    if (this.mode === 'readwrite' || this.mode === 'versionchange') {
      for (const [name, rows] of this.data) {
        const target = this.db.data.get(name) || new Map()
        target.clear()
        for (const [key, value] of rows) target.set(key, value)
        this.db.data.set(name, target)
      }
    }
    queueMicrotask(() => this.oncomplete?.({ target: this }))
  }
}

class FakeIDBFactory {
  constructor() { this.databasesMap = new Map() }
  databases() { return Promise.resolve([...this.databasesMap.values()].map(db => ({ name: db.name, version: db.version }))) }
  createDatabaseRecord(name, version = 0) {
    return { name, version, schemas: new Map(), data: new Map(), connections: 0 }
  }
  objectStoreNames(db) {
    return {
      contains: name => db.schemas.has(name),
      [Symbol.iterator]: () => [...db.schemas.keys()][Symbol.iterator]()
    }
  }
  createConnection(db) {
    db.connections += 1
    return {
      name: db.name,
      version: db.version,
      objectStoreNames: this.objectStoreNames(db),
      createObjectStore: (name, options = {}) => {
        const schema = { name, keyPath: options.keyPath ?? null, autoIncrement: !!options.autoIncrement, nextKey: 0, indexes: [] }
        db.schemas.set(name, schema)
        db.data.set(name, new Map())
        const store = new FakeObjectStore(null, schema, db)
        if (this.pendingVersionTransaction) {
          this.pendingVersionTransaction.stores.set(name, new FakeObjectStore(this.pendingVersionTransaction, schema, db))
        }
        return store
      },
      deleteObjectStore: name => { db.schemas.delete(name); db.data.delete(name) },
      transaction: (names, mode = 'readonly') => {
        const transaction = new FakeTransaction(this, db, Array.isArray(names) ? names : [names], mode)
        return transaction
      },
      close: () => { db.connections -= 1 }
    }
  }
  open(name, version) {
    const request = new FakeRequest()
    queueMicrotask(() => {
      const oldDb = this.databasesMap.get(name)
      const oldVersion = oldDb?.version || 0
      const targetVersion = version ?? oldVersion
      const db = oldDb || this.createDatabaseRecord(name, 0)
      this.databasesMap.set(name, db)
      const connection = this.createConnection(db)
      request.result = connection
      if (version !== undefined && targetVersion > oldVersion) {
        db.version = targetVersion
        const transaction = new FakeTransaction(this, db, [...db.schemas.keys()], 'versionchange')
        this.pendingVersionTransaction = transaction
        transaction.createObjectStore = (name, options = {}) => {
          const schema = { name, keyPath: options.keyPath ?? null, autoIncrement: !!options.autoIncrement, nextKey: 0, indexes: [] }
          db.schemas.set(name, schema)
          db.data.set(name, new Map())
          const store = new FakeObjectStore(transaction, schema, db)
          transaction.stores.set(name, store)
          return store
        }
        transaction.deleteObjectStore = connection.deleteObjectStore.bind(connection)
        request.transaction = transaction
        Promise.resolve(request.onupgradeneeded?.({ oldVersion, newVersion: targetVersion, target: request, transaction }))
          .then(() => { this.pendingVersionTransaction = null; transaction.commit() }, error => { this.pendingVersionTransaction = null; transaction.abort(error) })
          .then(() => request.onsuccess?.({ target: request }), error => { request.error = error; request.onerror?.({ target: request }) })
      } else {
        request.succeed(connection)
      }
    })
    return request
  }
  deleteDatabase(name) {
    const request = new FakeRequest()
    queueMicrotask(() => { this.databasesMap.delete(name); request.succeed(undefined) })
    return request
  }
}

globalThis.IDBKeyRange = FakeIDBKeyRange
globalThis.FakeIDBKeyRange = FakeIDBKeyRange
globalThis.FakeIDBFactory = FakeIDBFactory
