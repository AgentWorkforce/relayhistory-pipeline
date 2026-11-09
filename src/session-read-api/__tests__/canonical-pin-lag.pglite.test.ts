/**
 * READ_API_CANONICAL_PIN_LAGGING_POINTER (canonical.ts pinnedToPointer, wired only by
 * service-canonical.ts). Self-host pointer rows come from the consumer's reconcile poll, so with
 * async admission a tick can index revision N while admission then CASes the manifest to N+k: an
 * immediate read saw content_sha256 and revision ahead of the pointer and answered 501
 * `*_canonical_manifest_mismatch`. Over PGlite (every migration, the read role) with the real
 * writers (deliverBatch / prepareDelivery -> inbox -> deliverPrepared), the consumer's
 * indexPointer and canonicalSnapshot through sqlCanonicalPointers:
 *   - sync and async writers produce the same pointer fields once indexed;
 *   - flag off, the lag is still `manifest_mismatch` (regression pin);
 *   - flag on, the lag is served pinned to the indexed generation (its rows only);
 *   - flag on, it still fails closed: rewritten earlier chunk, pointer revision at or ahead of the
 *     manifest, another user, an erased session (tombstoned pointer, deleted manifest).
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { computeContentSha256, objectKey, validateManifest, type ChunkEntry, type ManifestV1 } from "../../../packages/object-store/src/index.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { canonicalSnapshot, CanonicalReadError, pinnedToPointer, type CanonicalDeps } from "../canonical.ts";
import { poolTenantRunner } from "../liveness.ts";
import { sqlCanonicalPointers } from "../session-timing-source.ts";
import { indexPointer, droppedFanout } from "../../session-pipeline/pointer.ts";
import { objectStoreManifestReader, prefixOrgResolver, prefixAllowed } from "../../shared/vendor.ts";
import { deliverBatch, deliverPrepared } from "../../session-ingest/deliver.ts";
import { decodeInbox, encodeInbox, inboxKey } from "../../session-ingest/async-admission.ts";
import { prepareDelivery } from "../../session-ingest/vendor/delivery/prepare.ts";
import { canonicalManifestIdentity } from "../../shared/manifest-identity.ts";
import { manifestUserId } from "../canonical-projection.ts";
import { MemoryLedger, TENANT, batch, record, scratchStore } from "../../session-ingest/__tests__/support.ts";

const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8");
const psql = (p: string) => read(p).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const BINDINGS = { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" };
const ID = { orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, source: "claude", sessionId: "sess-1" };
const digest = (v: string) => v.replace(/^\\x/u, "").toLowerCase();

let supa: PGlite;
beforeAll(async () => {
  supa = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await supa.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await supa.exec(read(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).sort()) await supa.exec(read(`supabase/migrations/${f}`));
  for (const f of readdirSync(join(root, "src/hoopdb/migrations")).filter((f) => f.endsWith(".sql")).sort()) await supa.exec(read(`src/hoopdb/migrations/${f}`));
  await supa.exec(psql("supabase/bootstrap/session_read_api_role.sql"));
}, 240_000);
afterAll(async () => {
  await supa?.close();
});

/** The read role, as the service's tenant runner uses it. */
const asRead = () => ({
  async connect() {
    await supa.exec("SET ROLE bulls96_read_api");
    return { query: (t: string, p?: unknown[]) => supa.query(t, p as unknown[]) as Promise<{ rows: unknown[] }>, release: () => void supa.exec("RESET ROLE") };
  },
});
const db = { query: (t: string, p?: unknown[]) => supa.query(t, p as never) } as never;
const reset = () => supa.exec("TRUNCATE sessions.session_index CASCADE; TRUNCATE sessions.session_index_ext CASCADE");

type Snap = { ok: true; exists: boolean; rows: string[] } | { ok: false; reason: string };

function world() {
  const s = scratchStore();
  const reader = objectStoreManifestReader(s.store, { provider: "s3", bucket: "bucket", maxBytes: 1024 * 1024 });
  const deps = { ledger: new MemoryLedger(), acceptanceMode: "canonical", target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} } } as never;
  async function manifestKeys() {
    const out: string[] = [];
    let cursor: string | undefined;
    do {
      const page = (await s.store.list("96bulls/org-a" as never, cursor)) as { items: Array<{ key: unknown }>; cursor?: string };
      for (const o of page.items) if (String(o.key).endsWith("/manifest.json")) out.push(String(o.key));
      cursor = page.cursor ?? undefined;
    } while (cursor);
    return out;
  }
  /** One reconcile tick: the consumer's indexPointer over every manifest. */
  async function index() {
    for (const key of await manifestKeys())
      await indexPointer(
        { provider: "s3", bucket: "bucket", key },
        { db, lock: (_s, w) => w(), reader, exists: async (k) => !!(await s.store.head(k as never)), orgs: prefixOrgResolver, allowPrefix: (p, o) => prefixAllowed(BINDINGS, p, o), fanout: droppedFanout },
      );
  }
  async function snapshot(extra: Partial<CanonicalDeps> = {}): Promise<Snap> {
    const cd = {
      store: s.store, provider: "s3", bucket: "bucket", prefix: BINDINGS,
      pointers: sqlCanonicalPointers(poolTenantRunner(asRead() as never)).pointers,
      decompress: (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b)), ...extra,
    } as CanonicalDeps;
    try {
      const r = await canonicalSnapshot(cd, ID);
      return { ok: true, exists: r.exists, rows: r.rows.filter((x) => x.table === "convergence_events").map((x) => String(x.row.delivery_record_id)).sort() };
    } catch (e) {
      if (e instanceof CanonicalReadError) return { ok: false, reason: e.reason };
      throw e;
    }
  }
  async function manifest(): Promise<{ key: string; m: ManifestV1 }> {
    const [key] = await manifestKeys();
    return { key: key!, m: (await reader.read(key!))!.manifest };
  }
  async function overwrite(key: string, m: ManifestV1) {
    await s.store.put(objectKey(key) as never, new TextEncoder().encode(JSON.stringify(m)));
  }
  async function fields() {
    const { m } = await manifest();
    const c = canonicalManifestIdentity(m);
    const p = (await sqlCanonicalPointers(poolTenantRunner(asRead() as never)).pointers(ID))[0]!;
    return {
      manifest: { content_sha256: m.raw.content_sha256, user: manifestUserId(m), revision: String(m.revision), session: c.sessionId, machine: c.machineId, ws: m.session.workspace_id, harness: m.session.harness },
      pointer: { content_sha256: digest(p.content_sha256), user: p.user_id, revision: String(p.revision), session: p.session_id, machine: p.machine_id, ws: p.workspace_id, harness: p.harness },
    };
  }
  const sync = async (b: unknown) => deliverBatch(deps, TENANT, b as never);
  /** The async path: prepare, the inbox round trip (encode/zstd/decode), then the admission writer. */
  const viaInbox = async (b: unknown) => {
    const prepared = await prepareDelivery(TENANT, b as never);
    const T = Date.now();
    const key = inboxKey("96bulls", TENANT, prepared.originId, prepared.receipt.receiptId, T, "0123456789abcdef");
    const decoded = (await decodeInbox("96bulls", key, new Uint8Array(zstdDecompressSync(encodeInbox(TENANT, prepared, T, 0)))))!;
    return deliverPrepared(deps, decoded.tenant, decoded.prepared);
  };
  return { s, index, snapshot, manifest, overwrite, fields, sync, viaInbox };
}

const B1 = () => batch([record({ record_id: "u1", payload: { text: "hi", ts_ms: 1_761_955_200_000, message_id: "m1" } })], { batch_id: "b1" });
const B2 = () => batch([record({ record_id: "a1", payload: { text: "yo", ts_ms: 1_761_955_201_000, message_id: "m2" } })], { batch_id: "b2" });
const PIN = { pinLaggingPointer: true } as const;
const MISMATCH = { ok: false, reason: "manifest_mismatch" };

/** Async b1, a reconcile tick, then async b2 admitted after it: pointer at revision 1, manifest ahead. */
async function lagging() {
  await reset();
  const w = world();
  await w.viaInbox(await B1());
  await w.index();
  await w.viaInbox(await B2());
  return w;
}

describe("canonical reads with a lagging pointer (READ_API_CANONICAL_PIN_LAGGING_POINTER)", () => {
  test("sync and async writers index to the same pointer fields, each equal to its manifest", async () => {
    const out: Record<string, Awaited<ReturnType<ReturnType<typeof world>["fields"]>>> = {};
    for (const mode of ["sync", "async"] as const) {
      await reset();
      const w = world();
      const put = mode === "sync" ? w.sync : w.viaInbox;
      await put(await B1());
      await put(await B2());
      await w.index();
      const f = await w.fields();
      expect(f.pointer).toEqual(f.manifest);
      expect(await w.snapshot()).toEqual({ ok: true, exists: true, rows: ["a1", "u1"] });
      out[mode] = f;
      w.s.done();
    }
    // content_sha256 chains over chunk bytes, which carry each delivery's receipt time, so it is
    // compared within a mode above; every other field is identical across the two writers.
    const sans = (f: (typeof out)[string]) => ({ manifest: { ...f!.manifest, content_sha256: undefined }, pointer: { ...f!.pointer, content_sha256: undefined } });
    expect(sans(out.async!)).toEqual(sans(out.sync!));
    for (const f of Object.values(out)) expect(f.pointer.content_sha256).toMatch(/^[0-9a-f]{64}$/u);
  }, 180_000);

  test("flag off: a pointer indexed mid-admission is still manifest_mismatch, and a re-index heals it", async () => {
    const w = await lagging();
    const f = await w.fields();
    expect([f.pointer.revision, f.manifest.revision]).toEqual(["1", "2"]);
    expect(f.pointer.content_sha256).not.toBe(f.manifest.content_sha256);
    expect(await w.snapshot()).toEqual(MISMATCH);
    expect(await w.snapshot({ pinLaggingPointer: false })).toEqual(MISMATCH);
    await w.index();
    expect(await w.snapshot()).toEqual({ ok: true, exists: true, rows: ["a1", "u1"] });
    w.s.done();
  }, 120_000);

  test("flag on: the lag is served pinned to the indexed generation (its rows only); the next tick serves the rest", async () => {
    const w = await lagging();
    expect(await w.snapshot(PIN)).toEqual({ ok: true, exists: true, rows: ["u1"] });
    await w.index();
    expect(await w.snapshot(PIN)).toEqual({ ok: true, exists: true, rows: ["a1", "u1"] });
    w.s.done();
  }, 120_000);

  test("flag on, fail closed: an earlier chunk rewritten (same chunk count, different sha256) is not a prefix", async () => {
    const w = await lagging();
    const { key, m } = await w.manifest();
    // Same two chunks, chunk 0 claims other bytes; the chain is recomputed so the manifest is self-consistent.
    const sha = "f".repeat(64);
    const chunks = m.raw.chunks.map((c, i) => (i === 0 ? { ...c, sha256: sha, sha256_8: sha.slice(0, 8) } : c));
    const rewritten = { ...m, revision: m.revision + 1, raw: { ...m.raw, chunks, content_sha256: await computeContentSha256(chunks) } } as ManifestV1;
    expect(await validateManifest(rewritten)).toEqual([]);
    await w.overwrite(key, rewritten);
    expect(await w.snapshot(PIN)).toEqual(MISMATCH);
    // And with the manifest's chunk count equal to the pointer's (one chunk, rewritten).
    const one = rewritten.raw.chunks.slice(0, 1);
    const single = { ...rewritten, raw: { ...rewritten.raw, chunks: one, chunk_count: 1, raw_bytes: one[0]!.bytes, stored_bytes: one[0]!.stored_bytes, next_offset: one[0]!.bytes, content_sha256: await computeContentSha256(one) } } as ManifestV1;
    expect(await validateManifest(single)).toEqual([]);
    await w.overwrite(key, single);
    expect(await w.snapshot(PIN)).toEqual(MISMATCH);
    w.s.done();
  }, 120_000);

  test("flag on, fail closed: a pointer revision at or ahead of the manifest (regression) is not pinned", async () => {
    const w = await lagging();
    for (const revision of [2, 9]) {
      await supa.exec(`UPDATE sessions.session_index SET revision = ${revision}`);
      expect(await w.snapshot(PIN)).toEqual(MISMATCH);
    }
    w.s.done();
  }, 120_000);

  test("flag on, fail closed: a digest that is no prefix, or another user's pointer, is refused", async () => {
    const w = await lagging();
    await supa.exec(`UPDATE sessions.session_index SET user_id = 'someone-else'`);
    expect(await w.snapshot(PIN)).toEqual(MISMATCH);
    await w.index();
    await supa.exec(`UPDATE sessions.session_index SET revision = 1, content_sha256 = decode(repeat('ab', 32), 'hex')`);
    expect(await w.snapshot(PIN)).toEqual(MISMATCH);
    w.s.done();
  }, 120_000);

  test("flag on, erased session: a tombstoned pointer serves nothing; erased bytes under a live pointer are object_missing", async () => {
    const w = await lagging();
    const { key } = await w.manifest();
    // Operator erase of the bytes (session-erase deletes the objects) before the consumer tombstones the row.
    await w.s.store.delete(objectKey(key) as never);
    expect(await w.snapshot(PIN)).toEqual({ ok: false, reason: "object_missing" });
    // The consumer's tombstone (upsertRawPointer deleted): no live pointer, nothing pinned or served.
    await supa.exec(`UPDATE sessions.session_index SET deleted_at = now()`);
    expect(await w.snapshot(PIN)).toEqual({ ok: true, exists: false, rows: [] });
    w.s.done();
  }, 120_000);
});

describe("pinnedToPointer", () => {
  const sha = (n: number) => n.toString(16).padStart(2, "0").repeat(32);
  const iso = "2026-11-01T10:00:00.000Z";
  const chunk = (seq: number, epoch: number, start_offset: number, bytes: number): ChunkEntry =>
    ({ seq, epoch, start_offset, bytes, stored_bytes: bytes + 1, sha256: sha(seq + 1), sha256_8: sha(seq + 1).slice(0, 8), stored_sha256: sha(0xa0 + seq), compression: "zstd" }) as ChunkEntry;
  // Epoch 0: chunks 0 and 1; epoch 1 (truncated) opens at chunk 2.
  const chunks = [chunk(0, 0, 0, 10), chunk(1, 0, 10, 5), chunk(2, 1, 0, 7)];
  async function manifest(): Promise<ManifestV1> {
    return {
      schema_v: 1, layout: "v1", trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18", revision: 5, created_at: iso, updated_at: iso,
      session: { workspace_id: "ws", machine_id: "m", harness: "claude", session_id: "s", started_at: iso },
      writer: { delivery_instance_id: "m", plugin_version: "test" },
      raw: {
        owner: "m", state: "open", updated_at: iso, format: "ndjson", compression: "zstd", chunk_count: 3, raw_bytes: 22, stored_bytes: 25,
        content_sha256: await computeContentSha256(chunks), epoch: 1, next_offset: 7, chunks,
        epochs: [
          { epoch: 0, opened_at: iso, reason: "initial", first_seq: 0, opened_by: "m" },
          { epoch: 1, opened_at: iso, reason: "truncated", first_seq: 2, opened_by: "m" },
        ],
      },
    } as ManifestV1;
  }

  test("each strict prefix pins to a self-consistent manifest at the pointer revision", async () => {
    const m = await manifest();
    expect(await validateManifest(m)).toEqual([]);
    const one = (await pinnedToPointer(m, await computeContentSha256(chunks.slice(0, 1)), 2))!;
    expect(one.revision).toBe(2);
    expect(one.raw).toMatchObject({ chunk_count: 1, raw_bytes: 10, stored_bytes: 11, next_offset: 10, epoch: 0 });
    expect(one.raw.epochs.map((e) => e.epoch)).toEqual([0]);
    const two = (await pinnedToPointer(m, await computeContentSha256(chunks.slice(0, 2)), 4))!;
    expect(two.raw).toMatchObject({ chunk_count: 2, raw_bytes: 15, stored_bytes: 17, next_offset: 15, epoch: 0 });
    // Epoch 1 opens at chunk index 2 (first_seq is a chunk index): not part of a two-chunk generation.
    expect(two.raw.epochs.map((e) => e.epoch)).toEqual([0]);
    for (const p of [one, two]) expect(await validateManifest(p)).toEqual([]);
    // The input is not mutated.
    expect(m.raw.chunks.length).toBe(3);
    expect(m.revision).toBe(5);
  });

  test("fails closed: no prefix match, empty chain, revision bounds, malformed digest", async () => {
    const m = await manifest();
    const prefix = await computeContentSha256(chunks.slice(0, 1));
    expect(await pinnedToPointer(m, "ab".repeat(32), 2)).toBeNull();
    expect(await pinnedToPointer(m, await computeContentSha256([]), 2)).toBeNull();
    for (const revision of [0, -1, 5, 6, 1.5, Number.NaN]) expect(await pinnedToPointer(m, prefix, revision)).toBeNull();
    expect(await pinnedToPointer(m, prefix.toUpperCase(), 2)).toBeNull();
    expect(await pinnedToPointer(m, `\\x${prefix}`, 2)).toBeNull();
    // A prefix whose cut is not a valid manifest (here: server.normalized from a later epoch) is refused too.
    const normalized = { ...m, server: { updated_at: iso, normalized: { source_epoch: 1 } } } as unknown as ManifestV1;
    expect(await pinnedToPointer(normalized, prefix, 2)).toBeNull();
  });
});
