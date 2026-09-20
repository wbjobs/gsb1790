export function getIndexedDB(idb) {
  const impl = idb || globalThis.indexedDB || globalThis.mozIndexedDB || globalThis.webkitIndexedDB
  if (!impl) {
    throw new Error('IndexedDB is not available in this environment')
  }
  return impl
}

export function getNavigatorLocks() {
  return globalThis.navigator?.locks || null
}

export function getBroadcastChannel() {
  return globalThis.BroadcastChannel || null
}
