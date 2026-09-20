export class FakeIndexedDB {
  constructor() {
    this.databases = new Map();
  }

  open(name, version) {
    return new FakeOpenRequest(this, name, version);
  }

  deleteDatabase(name) {
    this.databases.delete(name);
    return new FakeRequest(undefined);
  }
}

class FakeRequest {
  constructor(result = null) {
    this.result = result;
    this.error = null;
    this.onsuccess = null;
    this.onerror = null;
  }

  succeed(result) {
    this.result = result;
    dispatch(this, 'success');
  }

  fail(error) {
    this.error = error;
    dispatch(this, 'error');
  }
}

class FakeOpenRequest extends FakeRequest {
  constructor(factory, name, requestedVersion) {
    super(null);
    queueMicrotask(() => {
      let database = factory.databases.get(name);
      const oldVersion = database?.version || 0;
      const newVersion = requestedVersion ?? Math.max(oldVersion, 1);
      if (oldVersion > newVersion) {
        this.fail(makeError('VersionError', 'Cannot open an older database version'));
        return;
      }

      database = database || new FakeDatabase(name, 0);
      database.closed = false;
      factory.databases.set(name, database);
      this.result = database;

      if (newVersion > oldVersion) {
        const transaction = new FakeTransaction(database, '*', 'versionchange', newVersion);
        database.activeTransaction = transaction;
        this.transaction = transaction;
        transaction.oncomplete = () => this.succeed(database);
        transaction.onabort = () => this.fail(transaction.error);
        transaction.onerror = () => {
          if (this.readyState !== 'done') this.fail(transaction.error);
        };
        dispatch(this, 'upgradeneeded', { oldVersion, newVersion });
        queueMicrotask(() => transaction.commitIfIdle());
      } else {
        this.succeed(database);
      }
    });
  }
}

class FakeDatabase {
  constructor(name, version) {
    this.name = name;
    this.version = version;
    this.objectStoreNames = new FakeStringList();
    this.objectStores = new Map();
    this.closed = false;
    this.onversionchange = null;
  }

  transaction(stores, mode) {
    if (this.closed) throw makeError('InvalidStateError', 'Database is closed');
    const transaction = new FakeTransaction(this, stores, mode);
    queueMicrotask(() => transaction.commitIfIdle());
    return transaction;
  }

  createObjectStore(name, options) {
    if (this.objectStoreNames.contains(name)) {
      throw makeError('ConstraintError', `Object store already exists: ${name}`);
    }
    const store = new FakeObjectStore(name, options);
    this.objectStoreNames.add(name);
    this.objectStores.set(name, store);
    return new FakeObjectStoreView(store, this.activeTransaction || null);
  }

  deleteObjectStore(name) {
    if (!this.objectStoreNames.contains(name)) {
      throw makeError('NotFoundError', `Object store does not exist: ${name}`);
    }
    this.objectStoreNames.delete(name);
    this.objectStores.delete(name);
  }

  close() {
    this.closed = true;
  }
}

class FakeStringList {
  constructor(items = []) {
    this.items = [...items];
  }

  get length() {
    return this.items.length;
  }

  [Symbol.iterator]() {
    return this.items[Symbol.iterator]();
  }

  contains(name) {
    return this.items.includes(name);
  }

  add(name) {
    this.items.push(name);
  }

  delete(name) {
    this.items = this.items.filter((item) => item !== name);
  }
}

class FakeObjectStore {
  constructor(name, options = {}) {
    this.name = name;
    this.keyPath = options?.keyPath ?? null;
    this.indexNames = new FakeStringList();
    this.data = new Map();
  }

  cloneData() {
    return new Map(Array.from(this.data, ([key, value]) => [key, structuredClone(value)]));
  }
}

class FakeIndex {
  constructor(name, keyPath) {
    this.name = name;
    this.keyPath = keyPath;
  }
}

class FakeObjectStoreView {
  constructor(store, transaction) {
    this.store = store;
    this.transaction = transaction;
    this.keyPath = store.keyPath;
    this.indexNames = store.indexNames;
  }

  get(key) {
    return this.request(() => structuredClone(this.store.data.get(key)));
  }

  getAll() {
    return this.request(() => Array.from(this.store.data.values(), (value) => structuredClone(value)));
  }

  getAllKeys() {
    return this.request(() => Array.from(this.store.data.keys()));
  }

  count() {
    return this.request(() => this.store.data.size);
  }

  put(value, explicitKey, delay = 0) {
    return this.request(() => {
      const key = resolveKey(this.keyPath, value, explicitKey);
      this.store.data.set(key, structuredClone(value));
      return key;
    }, delay);
  }

  add(value, explicitKey) {
    return this.request(() => {
      const key = resolveKey(this.keyPath, value, explicitKey);
      if (this.store.data.has(key)) throw makeError('ConstraintError', `Key already exists: ${key}`);
      this.store.data.set(key, structuredClone(value));
      return key;
    });
  }

  delete(key) {
    return this.request(() => {
      this.store.data.delete(key);
      return undefined;
    });
  }

  clear() {
    return this.request(() => {
      this.store.data.clear();
      return undefined;
    });
  }

  openCursor() {
    const entries = Array.from(this.store.data.entries());
    return new CursorRequest(entries, this);
  }

  createIndex(name, keyPath) {
    if (this.transaction?.state !== 'active') {
      throw makeError('InvalidStateError', 'Cannot create an index outside an active transaction');
    }
    const index = new FakeIndex(name, keyPath);
    this.store.indexNames.add(name);
    this.store.indexes ??= new Map();
    this.store.indexes.set(name, index);
    return index;
  }

  deleteIndex(name) {
    if (this.transaction?.state !== 'active') {
      throw makeError('InvalidStateError', 'Cannot delete an index outside an active transaction');
    }
    this.store.indexNames.delete(name);
    this.store.indexes?.delete(name);
  }

  request(execute, delay = 0) {
    if (this.transaction) this.transaction.requestStarted();
    const request = new FakeStoreRequest(execute, this.transaction, delay);
    if (this.transaction) this.transaction.activeRequests += 1;
    return request;
  }
}

class FakeStoreRequest {
  constructor(execute, transaction, delay = 0) {
    this.result = null;
    this.error = null;
    this.onsuccess = null;
    this.onerror = null;
    const run = () => {
      try {
        this.result = execute();
        dispatch(this, 'success');
        if (transaction) settle('success');
      } catch (error) {
        this.error = error;
        const event = {
          defaultPrevented: false,
          preventDefault() {
            this.defaultPrevented = true;
          }
        };
        dispatch(this, 'error', event);
        if (event.defaultPrevented) {
          if (transaction) settle('handled');
        } else if (transaction) {
          settle('error');
        }
      }
    };
    if (delay > 0) setTimeout(run, delay);
    else queueMicrotask(run);

    function settle(outcome) {
      transaction.activeRequests -= 1;
      transaction.requestSettled(outcome);
    }
  }
}

class CursorRequest {
  constructor(entries, view) {
    this.entries = entries;
    this.view = view;
    this.index = 0;
    this.result = null;
    this.onsuccess = null;
    this.onerror = null;
    queueMicrotask(() => this.emit());
  }

  emit() {
    const entry = this.entries[this.index];
    this.result = entry ? new FakeCursor(entry[0], entry[1], this) : null;
    dispatch(this, 'success');
  }
}

class FakeCursor {
  constructor(key, value, request) {
    this.key = key;
    this.value = value;
    this.request = request;
    this.done = false;
  }

  update(value) {
    this.request.entries[this.request.index][1] = value;
    const key = this.request.view.keyPath ? value[this.request.view.keyPath] : this.key;
    this.request.view.store.data.set(key, structuredClone(value));
  }

  delete() {
    this.request.entries.splice(this.request.index, 1);
    this.request.index -= 1;
  }

  continue() {
    if (this.done) return;
    this.request.index += 1;
    queueMicrotask(() => this.request.emit());
  }
}

class FakeTransaction {
  constructor(database, stores, mode, upgradeVersion = null) {
    this.db = database;
    this.mode = mode;
    this.error = null;
    this.oncomplete = null;
    this.onerror = null;
    this.onabort = null;
    this.state = 'active';
    this.upgradeVersion = upgradeVersion;
    this.dynamicScope = stores === '*' || stores === undefined;
    this.objectStoreNames = this.dynamicScope ? Array.from(database.objectStoreNames) : resolveScope(database, stores);
    this.snapshot = new Map(
      this.objectStoreNames.map((name) => [name, database.objectStores.get(name)?.cloneData() || new Map()])
    );
    this.phase = 'idle';
    this.activeRequests = 0;
  }

  objectStore(name) {
    const scope = this.dynamicScope ? Array.from(this.db.objectStoreNames) : this.objectStoreNames;
    if (!scope.includes(name)) {
      throw makeError('NotFoundError', `Object store is outside transaction scope: ${name}`);
    }
    return new FakeObjectStoreView(this.db.objectStores.get(name), this);
  }

  abort() {
    if (this.state !== 'active') return;
    this.state = 'aborting';
    queueMicrotask(() => this.finish('aborted'));
  }

  commitIfIdle() {
    if (this.state !== 'active' || this.phase === 'waiting' || this.activeRequests > 0) return;
    this.phase = 'waiting';
    setTimeout(() => {
      if (this.state === 'active' && this.phase === 'waiting') this.finish('complete');
    }, 0);
  }

  requestStarted() {
    if (this.state !== 'active') throw makeError('TransactionInactiveError', 'Transaction is not active');
    this.phase = 'request';
  }

  requestSettled(outcome) {
    if (outcome === 'error') {
      this.error = this.error || makeError('AbortError', 'Transaction failed');
      if (this.mode === 'readwrite' || this.mode === 'versionchange') {
        this.state = 'errored';
        queueMicrotask(() => this.finish('aborted'));
        return false;
      }
    }
    this.phase = 'idle';
    this.commitIfIdle();
    return true;
  }

  finish(kind) {
    if (this.state === 'finished' || this.state === 'aborted') return;
    if (kind === 'aborted') {
      this.state = 'aborted';
      this.db.activeTransaction = null;
      for (const [name, data] of this.snapshot) {
        if (this.db.objectStores.has(name)) this.db.objectStores.get(name).data = data;
      }
      if (this.upgradeVersion !== null) this.rollbackUpgrade();
      this.error = this.error || makeError('AbortError', 'Transaction was aborted');
      dispatch(this, 'abort');
      dispatch(this, 'error');
    } else {
      this.state = 'finished';
      this.db.activeTransaction = null;
      if (this.upgradeVersion !== null) this.db.version = this.upgradeVersion;
      dispatch(this, 'complete');
    }
  }

  rollbackUpgrade() {
    for (const name of Array.from(this.db.objectStoreNames)) {
      if (!this.snapshot.has(name)) {
        this.db.objectStoreNames.delete(name);
        this.db.objectStores.delete(name);
      }
    }
    for (const [name, data] of this.snapshot) {
      if (this.db.objectStores.has(name)) this.db.objectStores.get(name).data = data;
    }
  }
}

function dispatch(target, type, event = {}) {
  const handler = target[`on${type}`];
  if (typeof handler === 'function') handler({ type, target, ...event });
}

function makeError(name, message) {
  const error = new Error(message);
  error.name = name;
  return error;
}

function resolveKey(keyPath, value, explicitKey) {
  if (keyPath !== null) return value[keyPath];
  if (explicitKey === undefined) throw makeError('DataError', 'An explicit key is required');
  return explicitKey;
}

function resolveScope(database, stores) {
  if (stores === '*' || stores === undefined) return Array.from(database.objectStoreNames);
  return Array.isArray(stores) ? stores : [stores];
}
