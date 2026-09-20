import assert from 'node:assert/strict'
import { test } from 'node:test'
import { LocalLockManager } from '../src/core/locks.js'
import { assertMigrationChain, normalizeMigrations } from '../src/core/migration.js'

test('rejects a migration chain with a missing version', () => {
  const migrations = normalizeMigrations([
    { version: 1, up: () => {} },
    { version: 3, up: () => {} }
  ])
  assert.throws(
    () => assertMigrationChain(0, 3, migrations),
    /Incomplete migration chain/
  )
})

test('local read/write lock grants readers concurrently and writers in FIFO order', async () => {
  const locks = new LocalLockManager()
  const events = []

  let releaseFirstReader
  const firstReader = locks.read(['db'], 'shared').then(release => {
    events.push('reader-1')
    releaseFirstReader = release
  })
  const writerStarted = new Promise(resolve => {
    locks.acquire(['db'], 'exclusive').then(release => {
      events.push('writer')
      release()
      resolve()
    })
  })

  await firstReader
  const secondReader = locks.read(['db'], 'shared').then(release => {
    events.push('reader-2')
    release()
  })
  await new Promise(resolve => setTimeout(resolve, 0))
  assert.deepEqual(events, ['reader-1'])
  releaseFirstReader()
  await writerStarted
  await secondReader
  assert.deepEqual(events, ['reader-1', 'writer', 'reader-2'])
})
