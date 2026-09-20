import { ManagedIndexedDB } from '../src/index.js'

const db = new ManagedIndexedDB({
  name: 'acceptance-tasks',
  workerUrl: new URL('./tasks-worker.js', import.meta.url),
  lockTimeoutMs: 15000
})

const output = document.querySelector('#output')

function log(title, value) {
  const item = document.createElement('pre')
  item.textContent = `${title}\n${typeof value === 'string' ? value : JSON.stringify(value, null, 2)}`
  output.prepend(item)
}

async function action(callback) {
  try {
    const result = await callback()
    log('OK', result ?? 'done')
  } catch (error) {
    log(`${error.code || error.name}: ${error.message}`, error.cause?.message || '')
  }
}

document.querySelector('#open').addEventListener('click', () => action(async () => {
  await db.open()
  return db.getAll('tasks')
}))

document.querySelector('#write').addEventListener('click', () => action(async () => {
  const id = crypto.randomUUID()
  await db.batch([
    { store: 'tasks', action: 'put', value: { id, title: `Task ${id}`, owner: 'ada', status: 'todo', updatedAt: Date.now() } },
    { store: 'tasks', action: 'put', value: { id: `${id}-audit`, title: `Audit ${id}`, owner: 'ada', status: 'queued', updatedAt: Date.now() } }
  ])
  return db.getAll('tasks')
}))

document.querySelector('#atomic').addEventListener('click', () => action(async () => {
  await db.procedure('transferTask', {
    id: 'welcome',
    owner: 'grace',
    failAfterWrite: true
  })
}))

document.querySelector('#read').addEventListener('click', () => action(() => db.getAll('tasks')))

document.querySelector('#reset').addEventListener('click', () => action(async () => {
  indexedDB.deleteDatabase('acceptance-tasks')
  indexedDB.deleteDatabase('__managed_idb_catalog_acceptance-tasks')
  indexedDB.deleteDatabase('__managed_idb_backup_acceptance-tasks')
  location.reload()
}))
