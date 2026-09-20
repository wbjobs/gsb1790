import { AsyncReadWriteLock } from './lock.js';
import {
  AbortError,
  MigrationError,
  TransactionError,
  VersionError,
  normalizeError
} from './errors.js';

export class IndexedDatabase {
  constructor({
    name,
    migrations = [],
    indexedDB = globalThis.indexedDB,
    closeOnVersionChange = true,
    onBlocked = null
  } = {}) {
    if (!name) throw new TypeError('Database name is required');
    if (!indexedDB) throw new TypeError('indexedDB is not available');
    this.name = name;
    this.indexedDB = indexedDB;
    this.closeOnVersionChange = closeOnVersionChange;
    this.onBlocked = onBlocked;
    this.version = 0;
    this.migrations = normalizeMigrations(migrations);
    this.targetVersion = this.migrations.at(-1)?.version || 1;
    validateMigrationChain(this.migrations);
    this.#lock = new AsyncReadWriteLock();
  }

  #lock;
  #db = null;
  #state = 'closed';
  #opening = null;
  #closedByUser = false;

  async open() {
    if (this.#state === 'open') return this;
    if (this.#opening) return this.#opening;
    if (this.#closedByUser) throw new VersionError('Database was explicitly closed');

    this.#opening = this.#openWithLock().finally(() => {
      this.#opening = null;
    });
    return this.#opening;
  }

  async transaction(handler, options = {}) {
    if (typeof handler !== 'function') throw new TypeError('Transaction handler must be a function');
    const { stores, mode = 'readonly' } = options;
    if (mode !== 'readonly' && mode !== 'readwrite') {
      throw new TransactionError(`Unsupported transaction mode: ${mode}`);
    }
    await this.#ensureOpen();
    return this.#lock.run(mode === 'readonly' ? 'read' : 'write', () => {
      if (!this.#db || this.#state !== 'open') {
        return Promise.reject(new TransactionError('Database closed before transaction started'));
      }
      const scope = resolveStoreNames(this.#db, stores);
      const idbTransaction = this.#db.transaction(scope, mode);
      const context = new TransactionContext(idbTransaction, mode);
      return executeTransaction(idbTransaction, () => handler(context));
    });
  }

  async close({ permanent = true } = {}) {
    if (permanent) this.#closedByUser = true;
    await this.#lock.run('write', async () => {
      if (this.#db) {
        this.#db.close();
        this.#db = null;
      }
      this.#state = 'closed';
      this.version = 0;
    });
  }

  async #ensureOpen() {
    if (this.#state === 'open') return;
    if (this.#closedByUser) throw new VersionError('Database was explicitly closed');
    await this.open();
  }

  async #openWithLock() {
    return this.#lock.run('write', () => this.#openUnlocked());
  }

  #openUnlocked() {
    return new Promise((resolve, reject) => {
      this.#state = 'opening';
      const request = this.indexedDB.open(this.name, this.targetVersion);
      let upgradeOutcome = Promise.resolve();
      let openError = null;
      let upgradeSettled = false;

      request.onupgradeneeded = (event) => {
        const transaction = request.transaction;
        const context = new MigrationContext(
          request.result,
          transaction,
          event.oldVersion,
          event.newVersion
        );
        upgradeOutcome = runMigrationChain(
          this.migrations,
          context,
          event.oldVersion,
          event.newVersion
        ).catch((error) => {
          const migrationError = normalizeMigrationError(error, context);
          openError = migrationError;
          upgradeSettled = true;
          try {
            transaction.abort();
          } catch {
          }
          throw migrationError;
        });
        upgradeOutcome.then(() => {
          upgradeSettled = true;
        }, () => {});
      };

      request.onsuccess = async () => {
        try {
          await upgradeOutcome;
          if (openError) throw openError;
          this.#useConnection(request.result);
          resolve(this);
        } catch (error) {
          try {
            request.result.close();
          } catch {
          }
          this.#state = 'closed';
          reject(normalizeError(error));
        }
      };

      request.onerror = () => {
        this.#state = 'closed';
        const fail = () => reject(openError || normalizeError(request.error));
        if (upgradeSettled) fail();
        else upgradeOutcome.then(fail, fail);
      };

      request.onblocked = () => {
        if (typeof this.onBlocked === 'function') this.onBlocked();
      };
    });
  }

  #useConnection(db) {
    if (this.#db && this.#db !== db) this.#db.close();
    this.#db = db;
    this.version = db.version;
    this.#state = 'open';
    if (this.closeOnVersionChange) {
      db.onversionchange = () => {
        this.close({ permanent: false }).catch(() => {});
      };
    }
  }
}

class TransactionContext {
  constructor(transaction, mode) {
    this.transaction = transaction;
    this.mode = mode;
    this.db = transaction.db;
    this.#stores = new Map();
  }

  #stores;

  get storeNames() {
    return Array.from(this.transaction.db.objectStoreNames);
  }

  store(name) {
    if (!this.#stores.has(name)) {
      this.#stores.set(name, new Store(this.transaction.objectStore(name), this));
    }
    return this.#stores.get(name);
  }

  abort() {
    this.transaction.abort();
  }
}

class MigrationContext {
  constructor(db, transaction, fromVersion, toVersion) {
    this.db = db;
    this.transaction = transaction;
    this.fromVersion = fromVersion;
    this.toVersion = toVersion;
    this.#stores = new Map();
  }

  #stores;

  get storeNames() {
    return Array.from(this.db.objectStoreNames);
  }

  store(name) {
    if (!this.#stores.has(name)) {
      this.#stores.set(name, new Store(this.transaction.objectStore(name), this));
    }
    return this.#stores.get(name);
  }

  createStore(name, options = null) {
    const objectStore = this.db.createObjectStore(name, options || undefined);
    const wrapper = new Store(objectStore, this);
    this.#stores.set(name, wrapper);
    return wrapper;
  }

  deleteStore(name) {
    this.db.deleteObjectStore(name);
    this.#stores.delete(name);
  }

  createIndex(storeName, indexName, keyPath, options = {}) {
    this.transaction.objectStore(storeName).createIndex(indexName, keyPath, options);
  }

  deleteIndex(storeName, indexName) {
    this.transaction.objectStore(storeName).deleteIndex(indexName);
  }

  abort() {
    this.transaction.abort();
  }
}

class Store {
  constructor(objectStore, context) {
    this.objectStore = objectStore;
    this.context = context;
  }

  get keyPath() {
    return this.objectStore.keyPath;
  }

  get indexNames() {
    return Array.from(this.objectStore.indexNames);
  }

  get(key) {
    return requestToPromise(this.objectStore.get(key));
  }

  getAll(query, count) {
    return requestToPromise(this.objectStore.getAll(query, count));
  }

  getAllKeys(query, count) {
    return requestToPromise(this.objectStore.getAllKeys(query, count));
  }

  count(query) {
    return requestToPromise(this.objectStore.count(query));
  }

  put(value, key) {
    return requestToPromise(this.objectStore.put(value, key));
  }

  add(value, key) {
    return requestToPromise(this.objectStore.add(value, key));
  }

  delete(key) {
    return requestToPromise(this.objectStore.delete(key));
  }

  clear() {
    return requestToPromise(this.objectStore.clear());
  }

  forEach(callback, options = {}) {
    const request = this.objectStore.openCursor(options.query, options.direction);
    const shouldContinue = typeof callback === 'function' ? callback : options.callback;
    return iterateCursor(request, shouldContinue);
  }
}

function requestToPromise(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = (event) => {
      event.preventDefault();
      reject(normalizeError(request.error));
    };
  });
}

function iterateCursor(request, callback) {
  return new Promise((resolve, reject) => {
    request.onsuccess = async () => {
      const cursor = request.result;
      if (!cursor) return resolve();
      try {
        const command = callback ? await callback(cursor) : undefined;
        if (command === 'stop') return resolve();
        if (command === 'delete') {
          cursor.delete();
        } else if (command && typeof command === 'object' && 'update' in command) {
          cursor.update(command.update);
        } else if (command && typeof command === 'object' && 'continueTo' in command) {
          cursor.continue(command.continueTo);
          return;
        }
        cursor.continue();
      } catch (error) {
        reject(normalizeError(error));
      }
    };
    request.onerror = (event) => {
      event.preventDefault();
      reject(normalizeError(request.error));
    };
  });
}

function executeTransaction(idbTransaction, operation) {
  return new Promise((resolve, reject) => {
    let operationState = 'pending';
    let operationResult;
    let operationError;
    let transactionState = 'pending';
    let transactionError;

    const finish = () => {
      if (operationState === 'pending' || transactionState === 'pending') return;
      if (transactionState === 'aborted') {
        reject(operationError || transactionError || new AbortError());
      } else if (transactionState === 'errored') {
        reject(operationError || transactionError || new TransactionError('Transaction failed'));
      } else if (operationState === 'errored') {
        reject(operationError);
      } else {
        resolve(operationResult);
      }
    };

    idbTransaction.oncomplete = () => {
      transactionState = 'completed';
      finish();
    };

    idbTransaction.onerror = () => {
      transactionState = 'errored';
      transactionError = normalizeError(idbTransaction.error);
      finish();
    };

    idbTransaction.onabort = () => {
      transactionState = 'aborted';
      transactionError = transactionError || normalizeAbort(idbTransaction.error);
      finish();
    };

    Promise.resolve()
      .then(operation)
      .then(
        (result) => {
          operationState = 'completed';
          operationResult = result;
          finish();
        },
        (error) => {
          operationState = 'errored';
          operationError = normalizeError(error);
          try {
            idbTransaction.abort();
          } catch {
          }
          finish();
        }
      );
  });
}

function normalizeAbort(error) {
  if (!error) return new AbortError();
  if (error.name === 'AbortError') return new AbortError(error.message, { cause: error });
  return new TransactionError(error.message || 'Transaction aborted', { cause: error });
}

function resolveStoreNames(db, stores) {
  const available = Array.from(db.objectStoreNames);
  if (available.length === 0) throw new TransactionError('Cannot start a transaction before any object store exists');
  if (stores === undefined || stores === '*') return available;
  const names = Array.isArray(stores) ? stores : [stores];
  if (names.length === 0) throw new TransactionError('Transaction store scope is empty');
  for (const name of names) {
    if (!available.includes(name)) throw new TransactionError(`Object store does not exist: ${name}`);
  }
  return names;
}

function normalizeMigrations(input) {
  const entries = Array.isArray(input)
    ? input
    : Object.entries(input).map(([version, upgrade]) => ({ version: Number(version), upgrade }));
  const migrations = entries.map((migration) => ({
    version: Number(migration.version),
    upgrade: migration.upgrade,
    description: migration.description
  }));

  const seen = new Set();
  for (const migration of migrations) {
    if (!Number.isSafeInteger(migration.version) || migration.version < 1) {
      throw new VersionError('Migration versions must be positive safe integers');
    }
    if (typeof migration.upgrade !== 'function') {
      throw new VersionError(`Migration ${migration.version} must provide an upgrade function`);
    }
    if (seen.has(migration.version)) throw new VersionError(`Duplicate migration version: ${migration.version}`);
    seen.add(migration.version);
  }

  return migrations.sort((a, b) => a.version - b.version);
}

async function runMigrationChain(migrations, context, oldVersion, newVersion) {
  if (oldVersion > newVersion) {
    throw new VersionError(`Cannot downgrade database from ${oldVersion} to ${newVersion}`);
  }
  if (oldVersion === 0 && newVersion !== migrations.at(-1)?.version) {
    throw new VersionError('A new database must migrate to the latest schema version');
  }

  validateMigrationChain(migrations, newVersion);

  for (const migration of migrations) {
    if (migration.version > oldVersion && migration.version <= newVersion) {
      await migration.upgrade(context, migration);
    }
  }
}

function validateMigrationChain(migrations, targetVersion = migrations.at(-1)?.version) {
  const required = new Set(migrations.map((migration) => migration.version));
  for (let version = 1; version <= targetVersion; version += 1) {
    if (!required.has(version)) throw new VersionError(`Missing migration for version ${version}`);
  }
}

function normalizeMigrationError(error, context) {
  if (error instanceof MigrationError) return error;
  const normalized = normalizeError(error);
  const range = `from ${context.fromVersion} to ${context.toVersion}`;
  return new MigrationError(`Migration failed ${range}: ${normalized.message}`, { cause: normalized });
}
