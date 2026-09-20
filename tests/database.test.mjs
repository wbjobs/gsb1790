import test from 'node:test';
import assert from 'node:assert/strict';
import { IndexedDatabase, MigrationError, VersionError } from '../src/index.js';
import { FakeIndexedDB } from './fake-indexeddb.mjs';

const migrationsV1 = [
  {
    version: 1,
    upgrade(db) {
      const users = db.createStore('users', { keyPath: 'id' });
      users.put({ id: 'u1', name: 'Ada', tags: ['v1'] });
    }
  }
];

const migrationsV3 = [
  ...migrationsV1,
  {
    version: 2,
    async upgrade(db) {
      db.createIndex('users', 'name', 'name');
      await db.store('users').forEach((cursor) => {
        cursor.value.schema = 2;
        cursor.update(cursor.value);
      });
    }
  },
  {
    version: 3,
    upgrade(db) {
      const settings = db.createStore('settings', { keyPath: 'key' });
      settings.put({ key: 'ready', value: true });
    }
  }
];

test('runs a complete migration chain and preserves existing data', async () => {
  const indexedDB = new FakeIndexedDB();
  const v1 = new IndexedDatabase({ name: 'app', migrations: migrationsV1, indexedDB });
  await v1.open();
  await v1.transaction(
    async (tx) => {
      await tx.store('users').put({ id: 'u2', name: 'Grace', tags: ['v1'] });
    },
    { stores: 'users', mode: 'readwrite' }
  );
  await v1.close({ permanent: false });

  const v3 = new IndexedDatabase({ name: 'app', migrations: migrationsV3, indexedDB });
  await v3.open();

  assert.equal(v3.version, 3);
  const users = await v3.transaction((tx) => tx.store('users').getAll());
  const settings = await v3.transaction((tx) => tx.store('settings').get('ready'));
  assert.equal(users.length, 2);
  assert.deepEqual(users.find((user) => user.id === 'u2'), {
    id: 'u2',
    name: 'Grace',
    tags: ['v1'],
    schema: 2
  });
  assert.deepEqual(settings, { key: 'ready', value: true });
});

test('rolls back schema and data changes when any migration in the chain fails', async () => {
  const indexedDB = new FakeIndexedDB();
  const v1 = new IndexedDatabase({ name: 'app', migrations: migrationsV1, indexedDB });
  await v1.open();
  await v1.close({ permanent: false });

  const brokenMigrations = [
    ...migrationsV1,
    {
      version: 2,
      async upgrade(db) {
        db.createStore('audit', { keyPath: 'id' });
        await db.store('users').put({ id: 'u1', name: 'Ada', schema: 2, tags: ['v1'] });
        throw new Error('data migration failed');
      }
    },
    {
      version: 3,
      upgrade(db) {
        db.createStore('never', { keyPath: 'id' });
      }
    }
  ];

  const broken = new IndexedDatabase({ name: 'app', migrations: brokenMigrations, indexedDB });
  await assert.rejects(broken.open(), (error) => error instanceof MigrationError);

  const recovered = new IndexedDatabase({ name: 'app', migrations: migrationsV3, indexedDB });
  await recovered.open();
  assert.equal(recovered.version, 3);
  const user = await recovered.transaction((tx) => tx.store('users').get('u1'));
  assert.deepEqual(user, { id: 'u1', name: 'Ada', tags: ['v1'], schema: 2 });
});

test('serializes writers and prevents dirty reads', async () => {
  const indexedDB = new FakeIndexedDB();
  const db = new IndexedDatabase({ name: 'app', migrations: migrationsV3, indexedDB });
  await db.open();
  const reads = [];

  const writer1 = db.transaction(
    async (tx) => {
      await tx.store('users').objectStore.put({ id: 'status', name: 'writer-1' }, undefined, 20);
    },
    { stores: 'users', mode: 'readwrite' }
  );

  const reader = db.transaction(async (tx) => {
    reads.push(await tx.store('users').get('status'));
  });
  const writer2 = db.transaction(
    async (tx) => {
      await tx.store('users').objectStore.put({ id: 'status', name: 'writer-2' }, undefined, 5);
    },
    { stores: 'users', mode: 'readwrite' }
  );

  await writer1;
  await reader;
  assert.deepEqual(reads, [{ id: 'status', name: 'writer-1' }]);
  await writer2;
  const finalValue = await db.transaction((tx) => tx.store('users').get('status'));
  assert.deepEqual(finalValue, { id: 'status', name: 'writer-2' });
});

test('aborts a failed write transaction without committing partial writes', async () => {
  const indexedDB = new FakeIndexedDB();
  const db = new IndexedDatabase({ name: 'app', migrations: migrationsV1, indexedDB });
  await db.open();

  await assert.rejects(
    db.transaction(
      async (tx) => {
        await tx.store('users').put({ id: 'partial', name: 'Partial' });
        throw new Error('business rule failed');
      },
      { stores: 'users', mode: 'readwrite' }
    ),
    (error) => /business rule failed/.test(error.message)
  );

  const count = await db.transaction((tx) => tx.store('users').count());
  assert.equal(count, 1);
  const partial = await db.transaction((tx) => tx.store('users').get('partial'));
  assert.equal(partial, undefined);
});

test('rejects malformed migration chains before opening the database', () => {
  const indexedDB = new FakeIndexedDB();
  assert.throws(
    () => new IndexedDatabase({
      name: 'duplicate-version',
      indexedDB,
      migrations: [
        { version: 1, upgrade() {} },
        { version: 1, upgrade() {} }
      ]
    }),
    (error) => error instanceof VersionError
  );

  assert.throws(
    () => new IndexedDatabase({
      name: 'missing-version',
      indexedDB,
      migrations: [
        { version: 1, upgrade() {} },
        { version: 3, upgrade() {} }
      ]
    }),
    (error) => error instanceof VersionError
  );
});
