import { openDatabase, requestAsPromise } from './idb-utils.js'

export const CATALOG_DB_VERSION = 1

export function catalogName(databaseName) {
  return `__managed_idb_catalog_${databaseName}`
}

export function backupDatabaseName(databaseName) {
  return `__managed_idb_backup_${databaseName}`
}

export async function openCatalog(idb, databaseName) {
  return openDatabase({
    idb,
    name: catalogName(databaseName),
    version: CATALOG_DB_VERSION,
    upgrade(event, database) {
      if (event.oldVersion < 1) {
        database.createObjectStore('backups', { keyPath: 'id' })
      }
    }
  })
}

async function completeTransaction(transaction) {
  await new Promise((resolve, reject) => {
    transaction.oncomplete = resolve
    transaction.onerror = () => reject(transaction.error)
    transaction.onabort = () => reject(transaction.error)
  })
}

export async function putCatalogRecord(idb, databaseName, record) {
  const catalog = await openCatalog(idb, databaseName)
  try {
    const transaction = catalog.transaction('backups', 'readwrite')
    transaction.objectStore('backups').put(record)
    await completeTransaction(transaction)
  } finally {
    catalog.close()
  }
}

export async function getCatalogRecord(idb, databaseName, id = 'primary') {
  const catalog = await openCatalog(idb, databaseName)
  try {
    const transaction = catalog.transaction('backups', 'readonly')
    return requestAsPromise(transaction.objectStore('backups').get(id))
  } finally {
    catalog.close()
  }
}

export async function deleteCatalogRecord(idb, databaseName, id = 'primary') {
  if (typeof idb.databases === 'function') {
    const databases = await idb.databases()
    if (!databases.some(database => database.name === catalogName(databaseName))) return
  }

  const catalog = await openCatalog(idb, databaseName)
  try {
    const transaction = catalog.transaction('backups', 'readwrite')
    transaction.objectStore('backups').delete(id)
    await completeTransaction(transaction)
  } finally {
    catalog.close()
  }
}

export async function catalogExists(idb, databaseName) {
  if (typeof idb.databases === 'function') {
    const databases = await idb.databases()
    return databases.some(database => database.name === catalogName(databaseName))
  }
  try {
    const catalog = await openCatalog(idb, databaseName)
    catalog.close()
    return true
  } catch {
    return false
  }
}
