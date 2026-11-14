/**
 * Ordered, bounded-concurrency prefetch of canonical chunk bodies (self-host
 * READ_API_CANONICAL_FETCH_CONCURRENCY, service-canonical.ts). Yields
 * `fetch(keys[i])` strictly in input order with at most `concurrency` fetches
 * in flight or buffered: fetch i+N starts only after the consumer has taken
 * item i and asked for the next one, so at most N bodies are held at once
 * (the one being consumed plus N-1 prefetched), each still capped by the
 * caller (readCapped(entry.stored_bytes)).
 *
 * concurrency <= 1 is literally sequential: each fetch starts when its item is
 * requested, exactly the pre-prefetch `for (...) await store.getObject(key)`.
 *
 * A rejection surfaces at its own position (items before it are yielded first).
 * Every prefetched promise carries a no-op catch, so a sibling that rejects
 * after the consumer stopped (refusal, abort, break) is never an unhandled
 * rejection. On stop the `signal` passed to `fetch` aborts, so a fetch still in
 * flight can drop its body instead of reading it (ObjectStore.getObject itself
 * takes no signal; the request's own deadline signal is on the store's fetch).
 */
import { readCapped } from "../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { readerChunkBody } from "../../packages/pipeline/src/lib/scrub-engine/reader-policy.ts";
import type { ObjectKey, ObjectStore } from "../../packages/object-store/src/index.ts";

export async function* boundedPrefetch<K, V>(
  keys: readonly K[],
  concurrency: number,
  fetch: (key: K, signal: AbortSignal) => Promise<V>,
): AsyncGenerator<V, void, undefined> {
  const stop = new AbortController();
  try {
    if (!(concurrency > 1)) {
      for (const key of keys) yield await fetch(key, stop.signal);
      return;
    }
    // FIFO of started fetches: started and taken in index order, so window[0] is item i.
    const window: Array<Promise<V>> = [];
    const start = (i: number) => {
      // new Promise: a synchronous throw from fetch becomes this item's rejection.
      const p = new Promise<V>((resolve) => resolve(fetch(keys[i]!, stop.signal)));
      p.catch(() => {});
      window.push(p);
    };
    for (let i = 0; i < Math.min(concurrency, keys.length); i++) start(i);
    for (let i = 0; i < keys.length; i++) {
      const value = await window.shift()!;
      yield value;
      if (i + concurrency < keys.length) start(i + concurrency);
    }
  } finally {
    stop.abort();
  }
}

/**
 * One chunk body exactly as the sequential readers read it: getObject, then
 * readerChunkBody (scrub-marker metadata under `maximum`), then
 * readCapped(limit) (null past the cap). Once the prefetch stopped, a body that
 * arrives late is cancelled unread rather than buffered.
 */
export async function readChunkBody(
  store: ObjectStore, key: ObjectKey, marker: unknown, maximum: number | undefined, limit: number, stop: AbortSignal,
): Promise<Uint8Array | null> {
  const object = await store.getObject(key);
  if (stop.aborted) {
    await object.body.cancel().catch(() => {});
    stop.throwIfAborted();
  }
  return readCapped(await readerChunkBody(object, marker, maximum), limit);
}
