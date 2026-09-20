# Managed IndexedDB

Zero-dependency IndexedDB wrapper built around a dedicated Web Worker. It provides:

- ordered, gap-checked schema migration chains;
- full snapshot backup and automatic rollback per migration version;
- IndexedDB atomic batches with explicit transaction boundaries;
- read/write isolation through Web Locks and a single Worker connection;
- crash recovery for interrupted migrations on the next open;
- structured, serializable error codes across the Worker boundary.

## Architecture

`ManagedIndexedDB` runs on the page and sends RPC messages to a Worker. The Worker owns one `ManagedDatabase` connection and serializes all data access.

- Reads acquire a shared lock and use a `readonly` IndexedDB transaction.
- Writes acquire an exclusive lock and use one `readwrite` transaction.
- Migration acquires the same exclusive data lock before any snapshot or version change.
- Every migration version is opened separately so a failure can roll back to the exact prior version.
- Before a snapshot migration, all stores and records are copied to `__managed_idb_backup_<db>`.
- A catalog at `__managed_idb_catalog_<db>` records backup state. A `migrating` record found on startup triggers automatic restore.

A `native` mode exists for storage-constrained cases. It runs migrations inside IndexedDB's native upgrade transaction, but only the default `snapshot` mode provides crash recovery and per-version rollback.

## Define the Worker

Migration functions and procedures must be registered in Worker code because functions cannot be structured-cloned through `postMessage`.

```js
import { createManagedWorker } from '../src/worker-entry.js'

const migrations = [
  {
    version: 1,
    up(t) {
      const tasks = t.createObjectStore('tasks', { keyPath: 'id' })
      tasks.createIndex('status', 'status')
    }
  },
  {
    version: 2,
    up(t) {
      t.objectStore('tasks').createIndex('ownerStatus', ['owner', 'status'])
    }
  },
  {
    version: 3,
    async up(t) {
      await t.objectStore('tasks').put({
        id: 'seed',
        owner: 'ada',
        status: 'ready'
      })
    },
    verify: async stores => {
      if ((await stores.tasks.count()) < 1) throw new Error('missing seed task')
    }
  }
]

createManagedWorker(globalThis, {
  name: 'app-db',
  migrations,
  backupMode: 'snapshot'
})
```

## Use the client

```js
import { ManagedIndexedDB } from './src/index.js'

const db = new ManagedIndexedDB({
  name: 'app-db',
  workerUrl: new URL('./app-worker.js', import.meta.url)
})

await db.open()

await db.batch([
  { store: 'tasks', action: 'put', value: { id: '1', status: 'todo', owner: 'ada' } },
  { store: 'tasks', action: 'put', value: { id: '2', status: 'todo', owner: 'grace' } }
])

await db.getAll('tasks')
```

All operations in one `batch` call share one IndexedDB transaction. If any request rejects, the transaction is aborted and earlier writes in the batch are rolled back.

## Atomic procedures

Procedures allow reusable multi-request logic while keeping transactions explicit.

```js
const procedures = {
  completeTask({ input, transaction }) {
    return transaction(['tasks'], 'readwrite', async stores => {
      const task = await stores.tasks.get(input.id)
      if (!task) throw new Error('not found')
      task.status = 'done'
      await stores.tasks.put(task)
      return task
    })
  }
}

await db.procedure('completeTask', { id: '1' })
```

Do not wait on unrelated async work between transaction requests. IndexedDB can auto-commit an inactive transaction, just as it does with direct API usage.

## Migration rules

- Versions must be positive integers.
- Pending versions must be contiguous; a missing version fails before touching the database.
- A newer on-disk version than code version fails with `DOWNGRADE_UNSUPPORTED`.
- Each migration may be async and can use `createObjectStore`, `deleteObjectStore`, indexes, gets, puts and deletes.
- An optional `verify(stores, context)` runs after that version commits.
- A failed migration restores the schema, records and previous version from the pre-migration snapshot.

## Error recovery

Errors use `ManagedDBError` with stable codes:

- `MIGRATION_CHAIN`: invalid or non-contiguous migration definitions.
- `MIGRATION_FAILED`: a migration script or verification failed.
- `RECOVERY_FAILED`: snapshot restoration failed; the database must not be considered migrated.
- `BACKUP_FAILED`: the pre-migration backup could not be created.
- `LOCK_TIMEOUT`: another tab, Worker or long transaction retained the database lock.
- `WORKER_RESTARTED`: a Worker crash interrupted in-flight work.

The next successful `open()` inspects the catalog. A complete snapshot marked `migrating` is restored before normal connections are allowed.

## Demo and tests

```sh
npm test
npm run serve
```

Open `http://localhost:5173/example/` and use the buttons to exercise migration, atomic writes, post-write failure and reads.

The Node tests use a small in-memory IndexedDB harness and cover:

- multi-version migration with pre-existing data;
- migration failure restoring the prior version and records;
- interrupted-migration recovery;
- all-or-nothing batch rollback;
- FIFO read/write isolation.
