import { getIndexedDB, getNavigatorLocks } from './env.js'
import { ManagedDBError, ERROR_CODES, wrapError } from './errors.js'
import { LocalLockManager, withWebLock } from './locks.js'
import {
  assertMigrationChain,
  migrationsFrom,
  normalizeMigrations
} from './migration.js'
import { deleteDatabase, openDatabase } from './idb-utils.js'
import { createSnapshot, deleteSnapshot, restoreSnapshot } from './snapshot.js'
import { catalogExists, getCatalogRecord, putCatalogRecord } from './catalog.js'
import { withTransaction, wrapVersionTransaction } from './transactions.js'

const DATA_LOCK_NAME = 'data'

export class ManagedDatabase {
  constructor(options = {}) {
    this.name = options.name
    this.migrations = normalizeMigrations(options.migrations || [])
    this.targetVersion = this.migrations.at(-1)?.version || 0
    this.backupMode = options.backupMode === 'native' ? 'native' : 'snapshot'
    this.lockTimeoutMs = options.lockTimeoutMs ?? 15000
    this.procedures = new Map(Object.entries(options.procedures || {}))
    this.idb = getIndexedDB(options.indexedDB)
    this.webLocks = options.navigator?.locks || getNavigatorLocks()
    this.localLocks = new LocalLockManager()
    this.connection = null
    this.opening = null
    this.runningMigration = null
  }

  get ready() {
    return this.opening
  }

  async open() {
    if (this.connection) return this.connection
    if (this.opening) return this.opening
    this.opening = this.connect().finally(() => {
      this.opening = null
    })
    return this.opening
  }

  async connect() {
    if (!this.name) {
      throw new ManagedDBError('database name is required', { code: ERROR_CODES.INVALID_CONFIG })
    }

    return this.withGlobalLock(DATA_LOCK_NAME, async () => {
      if (await catalogExists(this.idb, this.name)) {
        const record = await getCatalogRecord(this.idb, this.name)
        if (record?.state === 'migrating') {
          await this.recoverFromInterruptedMigration(record)
        } else if (record) {
          await deleteSnapshot(this.idb, this.name)
        }
      }

      const current = await this.peekCurrentVersion()
      if (current > this.targetVersion) {
        throw new ManagedDBError(
          `Database version ${current} is newer than code version ${this.targetVersion}`,
          { code: ERROR_CODES.DOWNGRADE_UNSUPPORTED }
        )
      }

      const pending = migrationsFrom(current, this.migrations)
      if (!pending.length) {
        this.connection = await this.openAt(current, false)
        return this.connection
      }

      assertMigrationChain(current, this.targetVersion, this.migrations)
      if (this.backupMode === 'native') {
        await this.migrateNatively(current, pending)
      } else {
        await this.migrateWithSnapshot(current, pending)
      }
      return this.connection
    })
  }

  async peekCurrentVersion() {
    if (typeof this.idb.databases === 'function') {
      const databases = await this.idb.databases()
      return databases.find(database => database.name === this.name)?.version || 0
    }
    const connection = await openDatabase({ idb: this.idb, name: this.name, version: undefined })
    const version = connection.version
    connection.close()
    return version
  }

  openAt(version, allowUpgrade, blocked) {
    return openDatabase({
      idb: this.idb,
      name: this.name,
      version: allowUpgrade ? version : (version || undefined),
      blocked,
      upgrade: () => {
        if (!allowUpgrade) throw new Error('Unexpected database upgrade')
      }
    }).then(connection => {
      connection.onversionchange = () => {
        connection.close()
        if (this.connection === connection) this.connection = null
      }
      return connection
    })
  }

  async migrateWithSnapshot(currentVersion, pending) {
    const before = await this.openAt(currentVersion, false)
    try {
      const snapshotRecord = await createSnapshot(this.idb, before, this.name)
      await putCatalogRecord(this.idb, this.name, {
        ...snapshotRecord,
        state: 'migrating',
        startedAt: Date.now()
      })
    } finally {
      before.close()
    }

    let nextVersion = currentVersion
    let connection
    try {
      for (const migration of pending) {
      connection = await this.openVersion(nextVersion, migration)
        await this.runMigrationVerification(migration, connection)
        connection.close()
        connection = null
        nextVersion = migration.version
      }
      await deleteSnapshot(this.idb, this.name)
      this.connection = await this.openAt(this.targetVersion, false)
    } catch (error) {
      connection?.close?.()
      const record = await getCatalogRecord(this.idb, this.name)
      let recoveryError
      if (record?.state === 'migrating') {
        try {
          await this.recoverFromInterruptedMigration(record)
        } catch (rollbackError) {
          recoveryError = rollbackError
        }
      }
      const migrationError = wrapError(error, `Migration ${nextVersion} failed`, {
        code: ERROR_CODES.MIGRATION_FAILED
      })
      if (recoveryError) migrationError.cause = recoveryError
      throw migrationError
    }
  }

  async migrateNatively(currentVersion, pending) {
    let connection
    try {
      connection = await this.openNativeUpgrade(currentVersion, pending)
      await deleteSnapshot(this.idb, this.name).catch(() => {})
      this.connection = connection
    } catch (error) {
      connection?.close?.()
      throw wrapError(error, 'Native migration transaction failed', {
        code: ERROR_CODES.MIGRATION_FAILED
      })
    }
  }

  async openVersion(currentVersion, migration) {
    return openDatabase({
      idb: this.idb,
      name: this.name,
      version: migration.version,
      upgrade: async (event, database, transaction) => {
        const context = wrapVersionTransaction(transaction, database, this.idb)
        context.oldVersion = event.oldVersion
        await migration.up(context, {
          fromVersion: currentVersion,
          toVersion: migration.version,
          migration
        })
      }
    }).then(connection => {
      connection.onversionchange = () => connection.close()
      return connection
    })
  }

  async openNativeUpgrade(currentVersion, pending) {
    return openDatabase({
      idb: this.idb,
      name: this.name,
      version: this.targetVersion,
      upgrade: async (event, database, transaction) => {
        let current = currentVersion
        for (const migration of pending) {
          const context = wrapVersionTransaction(transaction, database, this.idb)
          context.oldVersion = event.oldVersion
          await migration.up(context, {
            fromVersion: current,
            toVersion: migration.version,
            migration
          })
          current = migration.version
        }
      }
    }).then(connection => {
      connection.onversionchange = () => connection.close()
      return connection
    })
  }

  async runMigrationVerification(migration, connection) {
    if (!migration.verify) return
    await withTransaction(
      connection,
      this.idb,
      [...connection.objectStoreNames],
      'readonly',
      stores => migration.verify(stores, { version: migration.version })
    )
  }

  async recoverFromInterruptedMigration(record) {
    try {
      await restoreSnapshot(this.idb, this.name, record, { reopen: false })
    } catch (error) {
      throw wrapError(error, 'Migration recovery failed', {
        code: ERROR_CODES.RECOVERY_FAILED
      })
    }
  }

  async withGlobalLock(resourceSuffix, callback) {
    const resource = `${this.name}:${resourceSuffix}`
    if (this.webLocks) {
      return withWebLock(this.webLocks, resource, { lockTimeoutMs: this.lockTimeoutMs }, callback)
    }
    const release = await this.localLocks.acquire([resource], 'exclusive')
    try {
      return await callback()
    } finally {
      release()
    }
  }

  async withDataLock(mode, callback) {
    const resource = `${this.name}:data`
    const run = async () => {
      if (!this.connection) {
        this.connection = await this.openAt(this.targetVersion, false)
      }
      return callback()
    }

    if (this.webLocks) {
      return withWebLock(
        this.webLocks,
        resource,
        { lockTimeoutMs: this.lockTimeoutMs, mode },
        run
      )
    }
    const release = await this.localLocks.acquire([resource], mode)
    try {
      return await run()
    } finally {
      release()
    }
  }

  async execute(request) {
    await this.open()
    if (request.type === 'batch') return this.executeBatch(request)
    if (request.type === 'procedure') return this.executeProcedure(request)
    throw new ManagedDBError(`Unknown request type ${request.type}`, {
      code: ERROR_CODES.UNKNOWN_REQUEST
    })
  }

  batch(operations) {
    return this.executeBatch({ operations })
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

  async executeBatch(request) {
    const operations = request.operations
    if (!Array.isArray(operations) || !operations.length) {
      throw new ManagedDBError('batch.operations must be a non-empty array', {
        code: ERROR_CODES.INVALID_OPERATION
      })
    }

    const stores = [...new Set(operations.map(operation => operation.store))]
    const mode = operations.some(isWrite) ? 'readwrite' : 'readonly'

    return this.withDataLock(mode === 'readonly' ? 'shared' : 'exclusive', () =>
      withTransaction(this.connection, this.idb, stores, mode, async storeProxies => {
        const pending = operations.map(operation =>
          runOperation(storeProxies[operation.store], operation)
        )
        return Promise.all(pending)
      })
    )
  }

  async executeProcedure(request) {
    const handler = this.procedures.get(request.name)
    if (typeof handler !== 'function') {
      throw new ManagedDBError(`Procedure ${request.name} is not registered`, {
        code: ERROR_CODES.INVALID_OPERATION
      })
    }

    return this.withDataLock('exclusive', () =>
      handler({
        db: this.connection,
        idb: this.idb,
        input: request.input,
        transaction: (stores, mode, callback) =>
          withTransaction(this.connection, this.idb, stores, mode, callback)
      })
    )
  }

  procedure(name, input) {
    return this.executeProcedure({ name, input })
  }

  async close() {
    await this.opening?.catch(() => {})
    this.connection?.close()
    this.connection = null
  }

  async destroy() {
    await this.close()
    await deleteSnapshot(this.idb, this.name).catch(() => {})
    await deleteDatabase(this.idb, this.name)
  }
}

function isWrite(operation) {
  if (operation.mode && operation.mode !== 'readonly' && operation.mode !== 'readwrite') {
    throw new ManagedDBError(`Invalid transaction mode ${operation.mode}`, {
      code: ERROR_CODES.INVALID_OPERATION
    })
  }
  return ['put', 'add', 'delete', 'clear'].includes(operation.action)
}

async function runOperation(store, operation) {
  if (!store) {
    throw new ManagedDBError('Operation is missing an object store name', {
      code: ERROR_CODES.INVALID_OPERATION
    })
  }

  switch (operation.action) {
    case 'get':
      return store.get(operation.key)
    case 'getAll':
      return store.getAll(operation.range)
    case 'getAllKeys':
      return store.getAllKeys(operation.range)
    case 'count':
      return store.count(operation.range)
    case 'put':
      return store.put(operation.value, operation.key)
    case 'add':
      return store.add(operation.value, operation.key)
    case 'delete':
      return store.delete(operation.key ?? operation.range)
    case 'clear':
      return store.clear()
    default:
      throw new ManagedDBError(`Unsupported operation ${operation.action}`, {
        code: ERROR_CODES.INVALID_OPERATION
      })
  }
}
