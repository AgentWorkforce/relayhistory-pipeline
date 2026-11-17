/**
 * One seal-pool worker thread (seal-pool.ts). It runs the pinned sealer unchanged
 * (`sealRawChunk`, the scrub-engine seam over raw-chunks.ts) against a capturing
 * store: the store records the single create-only PUT the sealer makes, and acks
 * it the way the object-store core does (sha256 of the body, its length,
 * created). The main thread then sends that exact PUT to the real store. All CPU
 * work, the source hash, the scrub, zstd and the hash of the scrubbed bytes,
 * happens here and off the main thread.
 *
 * Nothing here touches a network or a credential. Any call besides one `put`
 * is refused, and an error goes back as `{ ok: false }` with its class name only (never a message, which could quote content): the main thread then
 * re-runs the pinned sealer itself, so every error is exactly today's.
 */
import { parentPort } from "node:worker_threads";
import { sealRawChunk } from "@rhc/lib/scrub-engine/index.ts";
import { sha256Hex } from "../../packages/object-store/src/crypto.ts";
import type { ObjectStore, PutOpts } from "../../packages/object-store/src/types.ts";
import type { SealPoolRequest, SealPoolResponse } from "./seal-pool.ts";

function capturingStore(captured: { put?: { key: string; body: Uint8Array; opts: PutOpts } }): ObjectStore {
  const refuse = (op: string) => () => { throw new Error(`seal_pool_unexpected_${op}`); };
  return new Proxy({} as ObjectStore, {
    get(_target, prop) {
      if (prop === "put")
        return async (key: string, body: Uint8Array, opts: PutOpts = {}) => {
          if (captured.put) throw new Error("seal_pool_second_put");
          // The signal cannot cross threads; the sealer never passes one.
          if (opts.signal) throw new Error("seal_pool_signal");
          captured.put = { key: String(key), body, opts };
          return { key, version: "seal-pool", contentSha256: await sha256Hex(body), size: body.length, created: true };
        };
      if (prop === "backend") return "fs";
      if (prop === "then") return undefined;
      return refuse(String(prop));
    },
  });
}

parentPort!.on("message", async (request: SealPoolRequest) => {
  const captured: { put?: { key: string; body: Uint8Array; opts: PutOpts } } = {};
  let response: SealPoolResponse;
  try {
    const sealed = await sealRawChunk(capturingStore(captured), request.input);
    if (!captured.put || !sealed.created) throw new Error("seal_pool_no_put");
    response = { id: request.id, ok: true, sealed, put: captured.put };
  } catch (error) {
    response = { id: request.id, ok: false, code: error instanceof Error ? error.name : "unknown" };
  }
  const transfer = response.ok ? [response.put.body.buffer as ArrayBuffer] : [];
  parentPort!.postMessage(response, transfer);
});
