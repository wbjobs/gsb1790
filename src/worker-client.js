import { TimeoutError, WorkerError, reviveError } from './errors.js';
import { REQUEST_CLOSE, REQUEST_OPEN, REQUEST_TRANSACTION } from './protocol.js';

export class WorkerDatabaseClient {
  constructor(worker, { autoOpen = true, timeout = 0, terminateOnClose = false } = {}) {
    this.autoOpen = autoOpen;
    this.defaultTimeout = timeout;
    this.terminateOnClose = terminateOnClose;
    this.#createWorker = typeof worker === 'function' ? worker : () => worker;
    this.#pending = new Map();
  }

  #createWorker;
  #worker = null;
  #pending;
  #opening = null;
  #ready = false;
  #closed = false;
  #autoOpenFailed = false;
  #requestCounter = 0;

  async open(options = {}) {
    if (this.#ready) return this.version;
    if (this.#opening) return this.#opening;
    this.#opening = this.#request(REQUEST_OPEN, {}, options).then((result) => {
      this.#ready = true;
      this.version = result.version;
      this.#opening = null;
      this.#autoOpenFailed = false;
      return this.version;
    }).catch((error) => {
      this.#resetConnection(null);
      this.#autoOpenFailed = error.code === 'MIGRATION_FAILED';
      this.#opening = null;
      throw error;
    });
    return this.#opening;
  }

  run(name, input, options = {}) {
    return this.#readyRequest(
      REQUEST_TRANSACTION,
      {
        name,
        input,
        mode: options.mode,
        stores: options.stores
      },
      options
    );
  }

  read(name, input, options = {}) {
    return this.run(name, input, { ...options, mode: 'readonly' });
  }

  write(name, input, options = {}) {
    return this.run(name, input, { ...options, mode: 'readwrite' });
  }

  async close({ permanent = true } = {}) {
    if (!this.#worker) return;
    await this.#request(REQUEST_CLOSE, { permanent });
    if (permanent) this.#closed = true;
    this.#autoOpenFailed = false;
    if (this.terminateOnClose && typeof this.#worker.terminate === 'function') {
      this.#worker.terminate();
    }
    this.#resetConnection(permanent ? new WorkerError('Database client was closed') : null);
  }

  async #readyRequest(type, payload, options) {
    if (this.autoOpen && !this.#ready && !this.#closed && !this.#autoOpenFailed) await this.open(options);
    return this.#request(type, payload, options);
  }

  #connect() {
    if (this.#worker) return this.#worker;
    const worker = this.#createWorker();
    if (!worker || typeof worker.postMessage !== 'function' || typeof worker.addEventListener !== 'function') {
      throw new TypeError('Worker must implement addEventListener and postMessage');
    }
    this.#worker = worker;
    if (typeof worker.start === 'function') worker.start();
    worker.addEventListener('message', (event) => this.#handleMessage(event.data));
    worker.addEventListener('error', (event) => {
      const message = event?.message || event?.error?.message || 'Worker crashed';
      this.#resetConnection(new WorkerError(message, { cause: event?.error || event }));
    });
    worker.addEventListener?.('messageerror', () => {
      this.#failAll(new WorkerError('Worker message could not be deserialized'));
    });
    return worker;
  }

  #request(type, payload, { timeout = this.defaultTimeout } = {}) {
    if (this.#closed) return Promise.reject(new WorkerError('Database client was closed'));
    const worker = this.#connect();
    const id = this.#nextId();
    const entry = {};
    const promise = new Promise((resolve, reject) => {
      entry.resolve = resolve;
      entry.reject = reject;
    });
    entry.promise = promise;
    this.#pending.set(id, entry);
    worker.postMessage({ id, type, payload });

    if (timeout > 0) {
      entry.timer = setTimeout(() => {
        this.#pending.delete(id);
        entry.reject(new TimeoutError(`Request ${type} timed out after ${timeout}ms`));
        this.#resetConnection(new WorkerError(`Worker request ${type} timed out and was disconnected`));
      }, timeout);
    }
    return promise;
  }

  #handleMessage(envelope) {
    if (!envelope || typeof envelope.id !== 'string') return;
    const entry = this.#pending.get(envelope.id);
    if (!entry) return;
    clearTimeout(entry.timer);
    this.#pending.delete(envelope.id);
    if (envelope.ok) entry.resolve(envelope.payload);
    else entry.reject(reviveError(envelope.error));
  }

  #failAll(error) {
    for (const [id, entry] of this.#pending) {
      clearTimeout(entry.timer);
      entry.reject(error);
      this.#pending.delete(id);
    }
  }

  #resetConnection(error) {
    this.#ready = false;
    this.#worker = null;
    if (error) this.#failAll(error);
  }

  #nextId() {
    this.#requestCounter += 1;
    return `${Date.now().toString(36)}-${this.#requestCounter}-${Math.random().toString(36).slice(2, 10)}`;
  }
}
