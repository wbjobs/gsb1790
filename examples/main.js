import { WorkerDatabaseClient } from '../src/index.js';

const db = new WorkerDatabaseClient(
  () => new Worker(new URL('./db.worker.js', import.meta.url), { type: 'module' }),
  { timeout: 15000 }
);

await db.write('putUser', { id: 1, email: 'a@example.com', balance: 100 });
const user = await db.read('getUser', 1);
console.log(user);
