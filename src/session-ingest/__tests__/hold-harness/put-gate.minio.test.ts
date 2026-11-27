/**
 * Opt-in disposable MinIO (SESSION_INGEST_TEST_S3_*, loopback only; run.ts starts one): self-test of the hold
 * harness's object-store pieces through the repository's own S3 client (createS3Store). A second create-only PUT is
 * refused; a PUT paused at the gate is delivered after a later read and lands; a paused create-only PUT delivered
 * after a later writer is refused and the later object survives.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { isObjectStoreError, objectKey, readAll } from "../../../shared/vendor.ts";
import { fixtureS3, scratchBucket, type ScratchBucket } from "./s3.ts";
import { startPutGate, type PutGate } from "./put-gate.ts";

const fx = fixtureS3();
const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array) => new TextDecoder().decode(b);

describe.skipIf(!fx)("hold harness: create-only and paused PUTs (MinIO)", () => {
  let b: ScratchBucket, gate: PutGate;
  beforeAll(async () => { b = await scratchBucket(fx!); gate = await startPutGate({ upstream: fx!.endpoint }); });
  afterAll(async () => { await gate?.close(); await b?.drop(); });

  const read = async (store: ReturnType<ScratchBucket["store"]>, key: string) => {
    try { return dec(await readAll(await store.get(objectKey(key)))); }
    catch (e) { if (isObjectStoreError(e, "not_found")) return null; throw e; }
  };

  test("a second create-only PUT is refused: 412 on the wire, already_exists from the client, first bytes kept", async () => {
    const store = b.store(), key = "org-a/marker.json";
    expect((await store.put(objectKey(key), enc('{"v":1}'), { ifNoneMatch: true })).created).toBe(true);
    const wire = await b.raw("PUT", key, '{"v":2}', { "if-none-match": "*" });
    expect(wire.status).toBe(412);
    const err = await store.put(objectKey(key), enc('{"v":2}'), { ifNoneMatch: true }).catch((e) => e);
    expect(isObjectStoreError(err, "already_exists")).toBe(true);
    expect(await read(store, key)).toBe('{"v":1}');
    // Without the precondition the same PUT overwrites: the refusal is the header's, not a bucket policy.
    expect((await b.raw("PUT", "org-a/plain.json", "1")).status).toBe(200);
    expect((await b.raw("PUT", "org-a/plain.json", "2")).status).toBe(200);
  });

  test("a paused create-only PUT whose client gave up is delivered after a later read, and lands", async () => {
    const via = b.store(gate.url), direct = b.store(), key = "org-b/marker.json";
    const held = gate.hold({ method: "PUT", key });
    const put = via.put(objectKey(key), enc("old"), { ifNoneMatch: true });
    put.catch(() => {});
    const req = await held.arrived;
    expect(req.headers["if-none-match"]).toBe("*");
    held.abandon();
    expect(await put.catch((e) => e)).toBeInstanceOf(Error); // the writer saw a failure: outcome unknown to it
    // The later read, after the PUT was issued and "failed", sees nothing (through the gate and directly).
    expect(await read(via, key)).toBeNull();
    expect(await read(direct, key)).toBeNull();
    const late = await held.deliver();
    expect(late.status).toBe(200); // SigV4 computed for the gate's Host still verifies at MinIO
    expect(await read(direct, key)).toBe("old");
    expect(gate.log.filter((e) => e.key === key && e.method === "PUT")).toEqual([{ method: "PUT", key, held: true, status: 200 }]);
  });

  test("a paused create-only PUT delivered after a later writer is refused, and the later object survives", async () => {
    const via = b.store(gate.url), direct = b.store(), key = "org-c/marker.json";
    const held = gate.hold({ method: "PUT", key });
    const put = via.put(objectKey(key), enc("old"), { ifNoneMatch: true });
    put.catch(() => {});
    await held.arrived;
    expect((await direct.put(objectKey(key), enc("barrier"), { ifNoneMatch: true })).created).toBe(true);
    const late = await held.deliver();
    expect(late.status).toBe(412);
    // The waiting client gets the refusal (different bytes: not an idempotent replay).
    expect(isObjectStoreError(await put.catch((e) => e), "already_exists")).toBe(true);
    expect(await read(direct, key)).toBe("barrier");
  });
});
