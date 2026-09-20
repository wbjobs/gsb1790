import { ManagedDBError, ERROR_CODES } from './errors.js'
import {
  deleteDatabase,
  getAllRecords,
  openDatabase,
  requestAsPromise,
  transactionDone
} from './idb-utils.js'
import { describeSchema } from './migration.js'
import { backupDatabaseName, deleteCatalogRecord, putCatalogRecord } from './catalog.js'

const METADATA_STORE = '__stores__'
const DATA_STORE = '__data__'
const SNAPSHOT_VERSION = 1

async function recreateBackup(idb, databaseName) {
  await deleteDatabase(idb, backupDatabaseName(databaseName))
  return openDatabase({
    idb,
    name: backupDatabaseName(databaseName),
    version: SNAPSHOT_VERSION,
    upgrade(event, database) {
      if (event.oldVersion < 1) {
        database.createObjectStore(METADATA_STORE, { keyPath: 'name' })
        database.createObjectStore(DATA_STORE)
      }
    }
  })
}

function backupKey(storeName, key) {
  return [storeName, key]
}

export async function createSnapshot(idb, sourceDatabase, databaseName, options = {}) {
  const id = options.id || 'primary'
  const schema = describeSchema(sourceDatabase)
  const createdAt = Date.now()
  let backup

  await putCatalogRecord(idb, databaseName, {
    id,
    state: 'preparing',
    databaseName,
    sourceVersion: sourceDatabase.version,
    createdAt,
    schema
  })

  try {
    backup = await recreateBackup(idb, databaseName)
    for (const store of schema.objectStores) {
      const readTransaction = sourceDatabase.transaction(store.name, 'readonly')
      const records = await getAllRecords(readTransaction.objectStore(store.name))
      const writeTransaction = backup.transaction([METADATA_STORE, DATA_STORE], 'readwrite')
      writeTransaction.objectStore(METADATA_STORE).put({ ...store, recordCount: records.length })
      const dataStore = writeTransaction.objectStore(DATA_STORE)
      for (const record of records) {
        dataStore.put(record.value, backupKey(store.name, record.key))
      }
      await transactionDone(writeTransaction)
    }
    backup.close()

    const record = {
      id,
      state: 'complete',
      databaseName,
      sourceVersion: sourceDatabase.version,
      createdAt,
      completedAt: Date.now(),
      schema
    }
    await putCatalogRecord(idb, databaseName, record)
    return record
  } catch (error) {
    backup?.close?.()
    await deleteDatabase(idb, backupDatabaseName(databaseName)).catch(() => {})
    await putCatalogRecord(idb, databaseName, {
      id,
      state: 'failed',
      databaseName,
      sourceVersion: sourceDatabase.version,
      createdAt,
      failedAt: Date.now(),
      schema,
      error: error?.message || String(error)
    }).catch(() => {})
    throw new ManagedDBError('Snapshot backup failed', {
      code: ERROR_CODES.BACKUP_FAILED,
      cause: error
    })
  }
}

export async function restoreSnapshot(idb, databaseName, catalogRecord, options = {}) {
  const backupName = backupDatabaseName(databaseName)
  const backup = await openDatabase({ idb, name: backupName, version: SNAPSHOT_VERSION })
  const schema = catalogRecord.schema
  let restoredConnection

  try {
    const metadata = await getAllRecords(
      backup.transaction(METADATA_STORE, 'readonly').objectStore(METADATA_STORE)
    )
    const recordCounts = Object.fromEntries(metadata.map(item => [item.key, item.value.recordCount]))
    const dataTransaction = backup.transaction(DATA_STORE, 'readonly')
    const allRecords = await getAllRecords(dataTransaction.objectStore(DATA_STORE))

    await deleteDatabase(idb, databaseName)
    restoredConnection = await openDatabase({
      idb,
      name: databaseName,
      version: 1,
      upgrade(event, database) {
        for (const store of schema.objectStores) {
          const storeOptions = { autoIncrement: Boolean(store.autoIncrement) }
          if (store.keyPath !== null) storeOptions.keyPath = store.keyPath
          const objectStore = database.createObjectStore(store.name, storeOptions)
          for (const index of store.indexes) {
            objectStore.createIndex(index.name, index.keyPath, {
              unique: index.unique,
              multiEntry: index.multiEntry
            })
          }
        }
      }
    })

    for (const store of schema.objectStores) {
      const records = allRecords.filter(record => record.key[0] === store.name)
      const transaction = restoredConnection.transaction(store.name, 'readwrite')
      const objectStore = transaction.objectStore(store.name)
      for (const record of records) {
        const key = record.key[1]
        if (store.keyPath) objectStore.put(record.value)
        else objectStore.put(record.value, key)
      }
      await transactionDone(transaction)
    }

    const counts = await countRows(restoredConnection, schema)
    const mismatch = counts.find(item => {
      return item.count !== recordCounts[item.name]
    })
    if (mismatch) {
      throw new ManagedDBError(`Snapshot verification failed for ${mismatch.name}`, {
        code: ERROR_CODES.RECOVERY_FAILED,
        details: counts
      })
    }

    restoredConnection.close()
    restoredConnection = null
    if (catalogRecord.sourceVersion === 0) {
      await deleteDatabase(idb, databaseName)
      if (options.reopen !== false) {
        restoredConnection = await openDatabase({ idb, name: databaseName })
      }
    } else if (options.reopen === false) {
      restoredConnection = null
    } else {
      restoredConnection = await openDatabase({
        idb,
        name: databaseName,
        version: catalogRecord.sourceVersion
      })
    }

    if (!options.keepBackup) {
      await deleteCatalogRecord(idb, databaseName, catalogRecord.id)
      await deleteDatabase(idb, backupName)
    }
  } catch (error) {
    restoredConnection?.close?.()
    throw new ManagedDBError('Snapshot restore failed', {
      code: ERROR_CODES.RECOVERY_FAILED,
      cause: error,
      details: { databaseName }
    })
  } finally {
    backup.close()
    if (options.reopen === false) restoredConnection?.close?.()
  }
}

async function countRows(database, schema) {
  const counts = []
  for (const store of schema.objectStores) {
    const transaction = database.transaction(store.name, 'readonly')
    counts.push({
      name: store.name,
      count: await requestAsPromise(transaction.objectStore(store.name).count())
    })
  }
  return counts
}

export async function deleteSnapshot(idb, databaseName) {
  await deleteCatalogRecord(idb, databaseName).catch(() => {})
  await deleteDatabase(idb, backupDatabaseName(databaseName)).catch(() => {})
}
