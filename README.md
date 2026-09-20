# Worker IndexedDB

A small, dependency-free IndexedDB wrapper designed for Web Workers.

It provides:

- Ordered schema migrations from every old version to the latest version.
- Atomic migration execution inside the IndexedDB `versionchange` transaction.
- Automatic abort and rollback when a migration or write transaction fails.
- Worker-wide fair read/write scheduling so queued readers cannot observe uncommitted writes.
- Serializable transaction handlers that execute entirely inside one Worker.
- Worker crash and request-timeout recovery through a reconnectable Worker factory.

## Model

Database code is split into two parts:

1. The Worker owns the `IndexedDatabase`, migrations, and named transaction handlers.
2. The main thread owns a `WorkerDatabaseClient` and sends structured input only.

Do not split one database transaction into several `postMessage` calls. Functions cannot be serialized across threads, and IndexedDB transactions can auto-commit while waiting for the next message. Named transaction handlers keep reads, validation, and writes in the same Worker-side transaction.

## Define The Worker

```js
import { attachWorkerHost, transaction } from 'worker-indexeddb';

const migrations = [
  {
    version: 1,
    upgrade(db) {
      db.createStore('accounts', { keyPath: 'id' });
    }
  },
  {
    version: 2,
    async upgrade(db) {
      await db.store('accounts').forEach((cursor) => {
        cursor.value.enabled = true;
        cursor.update(cursor.value);
      });
    }
  }
];

const transactions = {
  getAccount: transaction((tx, id) => tx.store('accounts').get(id), 'readonly', 'accounts'),
  putAccount: transaction((tx, account) => tx.store('accounts').put(account), 'readwrite', 'accounts'),
  transfer: transaction(async (tx, input) => {
    const accounts = tx.store('accounts');
    const from = await accounts.get(input.from);
    const to = await accounts.get(input.to);

    if (from.balance < input.amount) throw new Error('Insufficient balance');
    from.balance -= input.amount;
    to.balance += input.amount;
    await accounts.put(from);
    await accounts.put(to);
    return { from, to };
  }, 'readwrite', 'accounts')
};

attachWorkerHost(self, {
  name: 'bank',
  migrations,
  transactions,
  onBlocked() {
    self.postMessage({ type: 'schema-upgrade-blocked' });
  }
});
```

## Use The Client

```js
import { WorkerDatabaseClient } from 'worker-indexeddb';

const db = new WorkerDatabaseClient(
  () => new Worker(new URL('./db.worker.js', import.meta.url), { type: 'module' }),
  { timeout: 15000 }
);

await db.write('putAccount', { id: 1, balance: 100 });
const account = await db.read('getAccount', 1);
const result = await db.write('transfer', { from: 1, to: 2, amount: 40 });
```

The factory is required for recovery. If a Worker crashes or a request times out, all pending calls fail instead of guessing whether a write committed. The next call reconnects using the factory; failed reads or idempotent writes can be retried by the application.

## Migration Rules

- Versions must be positive integers and form a chain without gaps.
- Each version runs in order when the existing database version is older.
- The newest Worker opens the latest schema version.
- Existing stores and records are not cleared by the library.
- Data transformations should use only synchronous code and IndexedDB requests.
- Do not fetch from the network or wait on timers inside a migration; IndexedDB does not keep a transaction alive across unrelated work.
- A thrown migration error calls `transaction.abort()`, rolls back schema and data changes, and leaves the previous database version recoverable.

Migration context methods include `createStore`, `deleteStore`, `createIndex`, `deleteIndex`, and `store(name)`. Store methods include `get`, `getAll`, `getAllKeys`, `count`, `put`, `add`, `delete`, `clear`, and `forEach`.

## Concurrency

The Worker uses a fair read/write lock:

- Multiple readers may run together.
- A writer waits for existing readers and writers.
- New readers wait behind a queued writer, preventing writer starvation.
- Readers therefore never observe a half-finished write transaction.

This is stricter and easier to reason about than relying only on IndexedDB request scheduling.

## Direct Use

The core database can also be used without RPC inside a Worker:

```js
import { IndexedDatabase } from 'worker-indexeddb';

const db = new IndexedDatabase({ name: 'bank', migrations });
await db.open();

await db.transaction(async (tx) => {
  await tx.store('accounts').put({ id: 1, balance: 100 });
}, { stores: 'accounts', mode: 'readwrite' });
```

## Test

```sh
npm test
```

The Node tests use an in-memory IndexedDB-compatible fake that models upgrade rollback, transaction rollback, auto-commit, and delayed requests.
