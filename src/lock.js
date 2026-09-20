export class AsyncReadWriteLock {
  #readers = 0;
  #writer = false;
  #waiters = [];

  async acquire(mode = 'read') {
    if (!this.#canEnter(mode)) {
      await new Promise((resolve) => this.#waiters.push({ mode, resolve }));
    }
    if (mode === 'read') this.#readers += 1;
    else this.#writer = true;
  }

  release(mode = 'read') {
    if (mode === 'read') this.#readers = Math.max(0, this.#readers - 1);
    else this.#writer = false;
    this.#pump();
  }

  async run(mode, operation) {
    await this.acquire(mode);
    try {
      return await operation();
    } finally {
      this.release(mode);
    }
  }

  #canEnter(mode) {
    if (this.#writer) return false;
    if (mode === 'read') return !this.#hasWaitingWriter();
    return this.#readers === 0;
  }

  #hasWaitingWriter() {
    return this.#waiters.some((waiter) => waiter.mode === 'write');
  }

  #pump() {
    if (this.#waiters[0]?.mode === 'write') {
      if (this.#readers === 0 && !this.#writer) {
        const waiter = this.#waiters.shift();
        waiter.resolve();
      }
      return;
    }

    while (this.#waiters[0]?.mode === 'read' && !this.#writer) {
      const waiter = this.#waiters.shift();
      waiter.resolve();
    }
  }
}
