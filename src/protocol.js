export const REQUEST_OPEN = 'open';
export const REQUEST_TRANSACTION = 'transaction';
export const REQUEST_CLOSE = 'close';

export function createEnvelope(type, name, payload, id) {
  return { id, type, name, payload };
}
