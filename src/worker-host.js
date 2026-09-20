import { IndexedDatabase } from './database.js';
import { normalizeError, serializeError } from './errors.js';
import { REQUEST_CLOSE, REQUEST_OPEN, REQUEST_TRANSACTION } from './protocol.js';

export function attachWorkerHost(target, options) {
  const host = new WorkerDatabaseHost(options);
  const send = options.send || ((envelope) => target.postMessage(envelope));
  target.addEventListener('message', (event) => {
    const envelope = event.data;
    if (!envelope || typeof envelope.id !== 'string' || !envelope.type) return;
    host.handle(envelope).then(
      (payload) => {
        send({ id: envelope.id, ok: true, payload });
      },
      (error) => {
        send({ id: envelope.id, ok: false, error: serializeError(normalizeError(error)) });
      }
    );
  });
  return host;
}

export class WorkerDatabaseHost {
  constructor({
    name,
    migrations = [],
    transactions = {},
    indexedDB = globalThis.indexedDB,
    closeOnVersionChange = true,
    onBlocked = null
  } = {}) {
    this.db = new IndexedDatabase({ name, migrations, indexedDB, closeOnVersionChange, onBlocked });
    this.transactions = new Map(Object.entries(transactions));
  }

  registerTransaction(name, handler) {
    if (typeof handler !== 'function') throw new TypeError('Transaction handler must be a function');
    this.transactions.set(name, handler);
  }

  async handle(envelope) {
    switch (envelope.type) {
      case REQUEST_OPEN:
        await this.db.open();
        return { version: this.db.version };
      case REQUEST_TRANSACTION:
        return this.runTransaction(envelope.payload || {});
      case REQUEST_CLOSE:
        await this.db.close({ permanent: Boolean(envelope.payload?.permanent) });
        return { closed: true };
      default:
        throw new TypeError(`Unknown worker request: ${envelope.type}`);
    }
  }

  runTransaction({ name, input, mode, stores }) {
    const handler = this.transactions.get(name);
    if (!handler) throw new TypeError(`Transaction is not registered: ${name}`);
    return this.db.transaction((context) => handler(context, input), {
      mode: mode || inferMode(handler),
      stores: stores || handler.stores || '*'
    });
  }
}

export function transaction(definition, mode = 'readonly', stores = '*') {
  if (typeof definition === 'function') {
    definition.mode = mode;
    definition.stores = stores;
    return definition;
  }
  const handler = definition.handler;
  handler.mode = definition.mode || mode;
  handler.stores = definition.stores || stores;
  return handler;
}

function inferMode(handler) {
  return handler.mode === 'readwrite' ? 'readwrite' : 'readonly';
}

if (typeof self !== 'undefined' && typeof WorkerGlobalScope !== 'undefined' && self instanceof WorkerGlobalScope) {
  self.attachWorkerHost = attachWorkerHost;
}
