import test from 'node:test';
import assert from 'node:assert/strict';
import { WorkerDatabaseClient, attachWorkerHost } from '../src/index.js';
import { FakeIndexedDB } from './fake-indexeddb.mjs';

const migrations = [
  {
    version: 1,
    upgrade(db) {
      db.createStore('items', { keyPath: 'id' });
    }
  }
];

const migrationsV2 = [
  ...migrations,
  {
    version: 2,
    upgrade(db) {
      db.createStore('history', { keyPath: 'id' });
    }
  }
];

class FakeWorker {
  constructor() {
    this.listeners = new Map();
    this.started = false;
    this.terminated = false;
    this.host = null;
  }

  start() {
    this.started = true;
  }

  addEventListener(type, listener) {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type).push(listener);
  }

  postMessage(message) {
    if (!this.host) return;
    queueMicrotask(async () => {
      try {
        const payload = await this.host.handle(message);
        this.emit('message', { id: message.id, ok: true, payload });
      } catch (error) {
        this.emit('message', {
          id: message.id,
          ok: false,
          error: { name: error.name, message: error.message, code: error.code }
        });
      }
    });
  }

  terminate() {
    this.terminated = true;
  }

  emit(type, payload = {}) {
    for (const listener of this.listeners.get(type) || []) {
      listener(type === 'message' ? { data: payload } : payload);
    }
  }

  dispatch(type, payload) {
    for (const listener of this.listeners.get(type) || []) listener(payload);
  }
}

function createWorker({
  indexedDB = new FakeIndexedDB(),
  failOpen = false,
  transactions,
  migrations: overrideMigrations
} = {}) {
  const worker = new FakeWorker();
  const selectedMigrations = overrideMigrations || (failOpen ? brokenMigrationsV2() : migrationsV2);
  worker.host = attachWorkerHost(worker, {
    name: 'worker-db',
    indexedDB,
    send: (message) => worker.dispatch('message', message),
    migrations: selectedMigrations,
    transactions: transactions || {
      getItem: async (tx, id) => tx.store('items').get(id),
      putItem: (tx, item) => tx.store('items').put(item),
      slowPutItem: (tx, item) => tx.store('items').objectStore.put(item, undefined, 20)
    }
  });
  return worker;
}

function brokenMigrationsV2() {
  return [
    ...migrations,
    {
      version: 2,
      upgrade(db) {
        db.createStore('broken', { keyPath: 'id' });
        throw new Error('bad migration');
      }
    }
  ];
}

test('runs registered read and write transactions through the worker RPC boundary', async () => {
  const indexedDB = new FakeIndexedDB();
  const client = new WorkerDatabaseClient(() => createWorker({ indexedDB }));

  await client.write('putItem', { id: 1, value: 'first' });
  const item = await client.read('getItem', 1);
  assert.deepEqual(item, { id: 1, value: 'first' });
  await client.close();
});

test('reports migration failure and allows a fresh worker to recover the old schema', async () => {
  const indexedDB = new FakeIndexedDB();
  const seed = new WorkerDatabaseClient(() => createWorker({ indexedDB, migrations }));
  await seed.write('putItem', { id: 'seed', value: true });
  await seed.close({ permanent: false });

  let attempts = 0;
  const client = new WorkerDatabaseClient(() => {
    attempts += 1;
    return createWorker({
      indexedDB,
      failOpen: attempts === 1
    });
  });

  await assert.rejects(client.open(), /bad migration/);
  assert.equal(attempts, 1);

  const recovered = await client.open();
  assert.equal(recovered, 2);
  await client.write('putItem', { id: 'ok', value: true });
  const item = await client.read('getItem', 'ok');
  assert.equal(item.value, true);
  await client.close();
});

test('fails pending work when the worker crashes and reconnects for the next transaction', async () => {
  let workers = 0;
  let currentWorker = null;
  const client = new WorkerDatabaseClient(() => {
    workers += 1;
    const worker = createWorker();
    currentWorker = worker;
    return worker;
  });

  await client.open();
  assert.equal(workers, 1);
  const pending = client.write('slowPutItem', { id: 1 });
  setTimeout(() => currentWorker.emit('error', { message: 'worker crashed' }), 5);

  const result = await pending.then(
    () => 'committed',
    (error) => error.message
  );
  assert.match(result, /worker crashed/);

  const afterReconnect = await client.write('putItem', { id: 2, value: 'recovered' });
  assert.equal(afterReconnect, 2);
  assert.equal(workers, 2);
  await client.close();
});

test('disconnects a timed out worker and uses the factory to reconnect', async () => {
  const indexedDB = new FakeIndexedDB();
  const seed = new WorkerDatabaseClient(() => createWorker({ indexedDB, migrations }));
  await seed.write('putItem', { id: 'seed', value: true });
  await seed.close({ permanent: false });

  let workers = 0;
  const client = new WorkerDatabaseClient(
    () => {
      workers += 1;
      if (workers === 1) return new FakeWorker();
      return createWorker({ indexedDB });
    },
    { autoOpen: false, timeout: 10 }
  );

  await assert.rejects(client.open(), /timed out/);
  assert.equal(workers, 1);
  const version = await client.open();
  assert.equal(version, 2);
  assert.equal(workers, 2);
  await client.close();
});
