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
      const tasks = t.objectStore('tasks')
      tasks.createIndex('ownerStatus', ['owner', 'status'])
    }
  },
  {
    version: 3,
    async up(t) {
      const tasks = t.objectStore('tasks')
      tasks.createIndex('updatedAt', 'updatedAt')
      await tasks.put({
        id: 'welcome',
        owner: 'system',
        status: 'ready',
        title: 'Recoverable IndexedDB is ready',
        updatedAt: Date.now()
      })
    }
  }
]

const procedures = {
  async completeTask({ input, transaction }) {
    return transaction(['tasks'], 'readwrite', async stores => {
      const task = await stores.tasks.get(input.id)
      if (!task) return { found: false }
      task.status = 'done'
      task.completedAt = Date.now()
      await stores.tasks.put(task)
      return { found: true, task }
    })
  },

  async transferTask({ input, transaction }) {
    return transaction(['tasks'], 'readwrite', async stores => {
      const task = await stores.tasks.get(input.id)
      if (!task) throw new Error(`Task ${input.id} does not exist`)
      task.owner = input.owner
      await stores.tasks.put(task)
      if (input.failAfterWrite) throw new Error('Simulated post-write failure')
      return task
    })
  }
}

createManagedWorker(globalThis, {
  name: 'acceptance-tasks',
  migrations,
  procedures,
  backupMode: 'snapshot',
  lockTimeoutMs: 15000
})
