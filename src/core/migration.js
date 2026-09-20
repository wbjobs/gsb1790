import { ManagedDBError, ERROR_CODES } from './errors.js'

export function normalizeMigrations(migrations) {
  if (!Array.isArray(migrations)) {
    throw new ManagedDBError('migrations must be an array', { code: ERROR_CODES.INVALID_CONFIG })
  }

  const normalized = migrations
    .map(migration => ({
      version: Number(migration.version),
      description: migration.description || `Migration ${migration.version}`,
      up: migration.up,
      verify: migration.verify
    }))
    .sort((a, b) => a.version - b.version)

  for (const migration of normalized) {
    if (!Number.isInteger(migration.version) || migration.version < 1) {
      throw new ManagedDBError('migration.version must be a positive integer', {
        code: ERROR_CODES.INVALID_CONFIG
      })
    }
    if (typeof migration.up !== 'function') {
      throw new ManagedDBError(`migration ${migration.version} requires an up function`, {
        code: ERROR_CODES.INVALID_CONFIG
      })
    }
  }

  for (let index = 1; index < normalized.length; index += 1) {
    if (normalized[index].version === normalized[index - 1].version) {
      throw new ManagedDBError(`duplicate migration version ${normalized[index].version}`, {
        code: ERROR_CODES.MIGRATION_CHAIN
      })
    }
  }

  return normalized
}

export function migrationsFrom(currentVersion, migrations) {
  return migrations.filter(migration => migration.version > currentVersion)
}

export function assertMigrationChain(currentVersion, targetVersion, migrations) {
  const expected = []
  for (let version = currentVersion + 1; version <= targetVersion; version += 1) {
    expected.push(version)
  }
  const actual = migrationsFrom(currentVersion, migrations).map(migration => migration.version)
  const missing = expected.filter(version => !actual.includes(version))
  if (missing.length || actual.some(version => version > targetVersion)) {
    throw new ManagedDBError(
      `Incomplete migration chain from ${currentVersion} to ${targetVersion}`,
      {
        code: ERROR_CODES.MIGRATION_CHAIN,
        details: { missing, actual, expected }
      }
    )
  }
}

export function describeSchema(database) {
  return {
    name: database.name,
    version: database.version,
    objectStores: [...database.objectStoreNames].map(storeName => {
      const store = database.transaction(storeName, 'readonly').objectStore(storeName)
      return {
        name: storeName,
        keyPath: store.keyPath,
        autoIncrement: store.autoIncrement,
        indexes: [...store.indexNames].map(indexName => {
          const index = store.index(indexName)
          return {
            name: indexName,
            keyPath: index.keyPath,
            multiEntry: index.multiEntry,
            unique: index.unique
          }
        })
      }
    })
  }
}
