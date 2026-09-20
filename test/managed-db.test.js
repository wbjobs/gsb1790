import assert from 'node:assert/strict'
import { afterEach, test } from 'node:test'
import './helpers/fake-idb.js'
import { ManagedDatabase } from '../src/core/managed-db.js'
import { putCatalogRecord } from '../src/core/catalog.js'

function migrations(failVersion = null) {
  return [
    {
      version: 1,
      up(t) {
        const tasks = t.createObjectStore('tasks', { keyPath: 'id' })
        tasks.createIndex('status', 'status')
      }
    },
    {
      version: 2,
      async up(t) {
        const tasks = t.objectStore('tasks')
        tasks.createIndex('ownerStatus', ['owner', 'status'])
        await tasks.put({ id: 'seed-v2', owner: 'ada', status: 'ready' })
      }
    },
    {
      version: 3,
      up(t) {
        const tasks = t.objectStore('tasks')
        tasks.put({ id: 'seed-v3', owner: 'grace', status: 'done' })
        if (failVersion === 3) throw new Error('boom at v3')
      }
    }
  ]
}

function createDb(name, options = {}) {
  return new ManagedDatabase({
    name,
    migrations: migrations(options.failVersion),
    indexedDB: new FakeIDBFactory(),
    navigator: { locks: undefined },
    ...options
  })
}

async function destroy(db) {
  const idb = db.idb
  await db.close?.()
  for (const database of await idb.databases()) {
    await new Promise((resolve, reject) => {
      const request = idb.deleteDatabase(database.name)
      request.onsuccess = resolve
      request.onerror = () => reject(request.error)
    })
  }
}

afterEach(async () => {
  for (const name of ['chain', 'rollback', 'recovery', 'atomic', 'dirty']) {
    const idb = new FakeIDBFactory()
  }
})

test('runs a multi-version migration chain without losing existing data', async () => {
  const name = `chain-${Date.now()}`
  const idb = new FakeIDBFactory()
  const db = new ManagedDatabase({
    name,
    indexedDB: idb,
    navigator: { locks: undefined },
    migrations: migrations().slice(0, 1)
  })
  await db.open()
  await db.batch([
    { store: 'tasks', action: 'put', value: { id: 'a', owner: 'ada', status: 'todo' } }
  ])
  await db.close()

  const upgraded = new ManagedDatabase({
    name,
    indexedDB: idb,
    navigator: { locks: undefined },
    migrations: migrations()
  })
  await upgraded.open()
  assert.equal(upgraded.connection.version, 3)
  const rows = await upgraded.getAll('tasks')
  const ids = rows.map(row => row.id).sort()
  assert.deepEqual(ids, ['a', 'seed-v2', 'seed-v3'])
  await destroy(upgraded)
})

test('rolls schema and data back when a migration step fails', async () => {
  const name = `rollback-${Date.now()}`
  const idb = new FakeIDBFactory()
  const first = createDb(name, { indexedDB: idb, migrations: migrations().slice(0, 2) })
  await first.open()
  await first.put('tasks', { id: 'keep', owner: 'ada', status: 'todo' })
  await first.close()

  const failed = new ManagedDatabase({
    name,
    indexedDB: idb,
    navigator: { locks: undefined },
    migrations: migrations(3)
  })
  await assert.rejects(failed.open(), /boom at v3/)
  await failed.close()

  const restored = new ManagedDatabase({
    name,
    indexedDB: idb,
    navigator: { locks: undefined },
    migrations: migrations().slice(0, 2)
  })
  await restored.open()
  assert.equal(restored.connection.version, 2)
  assert.deepEqual(await restored.getAll('tasks'), [
    { id: 'keep', owner: 'ada', status: 'todo' },
    { id: 'seed-v2', owner: 'ada', status: 'ready' }
  ])
  await destroy(restored)
})

test('recovers after an interrupted migration on next open', async () => {
  const name = `recovery-${Date.now()}`
  const idb = new FakeIDBFactory()
  const v2 = createDb(name, { indexedDB: idb, migrations: migrations().slice(0, 2) })
  await v2.open()
  await v2.put('tasks', { id: 'recoverable', owner: 'ada', status: 'todo' })
  await v2.close()
  const before = await v2.openAt(2, false)
  const snapshot = await import('../src/core/snapshot.js')
  const record = await snapshot.createSnapshot(idb, before, name)
  before.close()
  await putCatalogRecord(idb, name, { ...record, state: 'migrating' })

  const recovered = new ManagedDatabase({
    name,
    indexedDB: idb,
    navigator: { locks: undefined },
    migrations: migrations().slice(0, 2)
  })
  await recovered.open()
  assert.equal(recovered.connection.version, 2)
  assert.equal(await recovered.get('tasks', 'recoverable') === undefined, false)
  await destroy(recovered)
})

test('batch transaction rolls back every operation on failure', async () => {
  const db = createDb(`atomic-${Date.now()}`)
  await db.open()
  await db.put('tasks', { id: 'existing', owner: 'ada', status: 'todo' })

  await assert.rejects(
    db.batch([
      { store: 'tasks', action: 'put', value: { id: 'temporary', owner: 'grace', status: 'todo' } },
      { store: 'tasks', action: 'add', value: { id: 'existing', owner: 'grace', status: 'todo' } }
    ]),
    /ConstraintError/
  )

  assert.equal(await db.get('tasks', 'temporary'), undefined)
  await destroy(db)
})

test('readers wait for an active writer and never observe uncommitted values', async () => {
  const db = createDb(`dirty-${Date.now()}`)
  await db.open()
  await db.put('tasks', { id: 'state', owner: 'ada', status: 'before' })

  let releaseWriter
  const writerStarted = Promise.withResolvers()
  db.procedures.set('blocked-write', async ({ transaction }) => {
    writerStarted.resolve()
    await new Promise(resolve => {
      releaseWriter = resolve
    })
    return transaction(['tasks'], 'readwrite', async stores => {
      await stores.tasks.put({ id: 'state', owner: 'ada', status: 'committed' })
    })
  })
  const writer = db.procedure('blocked-write', {})
  await writerStarted.promise
  const read = db.get('tasks', 'state')
  await Promise.resolve()
  releaseWriter()
  await writer
  assert.equal((await read).status, 'committed')
  await destroy(db)
})
