import { attachWorkerHost, transaction } from '../src/index.js';

const migrations = [
  {
    version: 1,
    upgrade(db) {
      db.createStore('users', { keyPath: 'id' });
    }
  },
  {
    version: 2,
    async upgrade(db) {
      db.createIndex('users', 'email', 'email', { unique: true });
      await db.store('users').forEach((cursor) => {
        cursor.value.email = cursor.value.email || `${cursor.value.id}@example.com`;
        cursor.update(cursor.value);
      });
    }
  },
  {
    version: 3,
    upgrade(db) {
      const settings = db.createStore('settings', { keyPath: 'key' });
      settings.put({ key: 'schemaReady', value: true });
    }
  }
];

const transactions = {
  getUser: transaction((stores, id) => stores.store('users').get(id), 'readonly', 'users'),
  putUser: transaction((stores, user) => stores.store('users').put(user), 'readwrite', 'users'),
  transfer: transaction(
    async (stores, input) => {
      const users = stores.store('users');
      const from = await users.get(input.from);
      const to = await users.get(input.to);
      if (!from || !to) throw new Error('Both accounts must exist');
      if (from.balance < input.amount) throw new Error('Insufficient balance');
      from.balance -= input.amount;
      to.balance += input.amount;
      await users.put(from);
      await users.put(to);
      return { from, to };
    },
    'readwrite',
    'users'
  )
};

attachWorkerHost(self, { name: 'app-db', migrations, transactions });
