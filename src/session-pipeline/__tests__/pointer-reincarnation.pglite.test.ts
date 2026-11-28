/**
 * T139: a session erased by a plain delete, then re-created reusing revision number 1, becomes visible to an
 * AUTHORIZED caller from the re-create's own event, with no second append. Delayed pre-erase work cannot republish
 * the erased content, a stale read cannot commit, and an operator erasure still refuses the same sequence.
 *
 * Real: the consumer (processBatch, standard S3-notification path), the pointer stage (indexPointer, the hoopdb
 * upsert) on PGlite with EVERY migration including 20261105062360, the search stage (indexSearchScope) on that same
 * database, and the read API (createReadApi) over the registered Postgres projection. Pointer liveness for the read
 * API is answered from the real session_index.
 * Mocked: S3 is the fs object store (its ETag stands in for the S3 versionId); the scope lock is the harness's
 * recorded mutex (pointer-lock-harness.ts); the projection lives in its own PGlite; the read API's token verifier is
 * a fixture. The FIFO path is on review/latency-session-coalesce-fifo (T126) and is proved there against this fix.
 * Synthetic, secret-free data only.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { objectKey } from "../../shared/vendor.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { createSearchIndexRegistry } from "@rhc/adapters/defaults.ts";
import { TENANT_SETTINGS_SQL, type TenantSqlRunner } from "@rhc/search-index/postgres.ts";
import { composeSearchService } from "../../shared/search-composition.ts";
import { createReadApi, AuthError } from "../../session-read-api/app.ts";
import type { SearchProjectionState } from "../search-adapter.ts";
import type { SearchIndexDeps } from "../search-index.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import type { PointerDeps } from "../pointer.ts";
import { pointerRig, loc } from "./pointer-lock-harness.ts";

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanup.splice(0).reverse()) await c(); });

const NOW = new Date("2026-11-02T00:00:00.000Z");
const scope = { orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: "sess_1" };
const tenant = { orgId: scope.orgId, workspaceId: scope.workspaceId };

/** Capture the JSON log lines one act writes. */
async function logged<T>(act: () => Promise<T>): Promise<{ result: T; lines: Record<string, unknown>[] }> {
  const lines: Record<string, unknown>[] = [];
  const out = process.stdout.write, err = process.stderr.write;
  const tap = (w: typeof process.stdout.write, s: NodeJS.WriteStream) => ((chunk: unknown, ...rest: unknown[]) => {
    for (const l of String(chunk).split("\n")) if (l.startsWith("{")) try { lines.push(JSON.parse(l)); } catch {}
    return (w as (...a: unknown[]) => boolean).call(s, chunk, ...rest);
  }) as typeof process.stdout.write;
  process.stdout.write = tap(out, process.stdout);
  process.stderr.write = tap(err, process.stderr);
  try { return { result: await act(), lines }; } finally { process.stdout.write = out; process.stderr.write = err; }
}
const named = (lines: Record<string, unknown>[], msg: string) => lines.filter((l) => l.message === msg);

async function rig(opts: { reincarnation: boolean }) {
  const r = await pointerRig();
  cleanup.push(r.close);
  // The search projection in its own PGlite, as pipeline.test.ts registers it.
  const proj = new PGlite({ extensions: { pg_trgm } });
  await proj.exec("CREATE EXTENSION pg_trgm");
  cleanup.push(() => proj.close());
  const tenants: TenantSqlRunner = (orgId, workspaceId, work) =>
    proj.transaction(async (tx) => { await tx.query(TENANT_SETTINGS_SQL, [orgId, workspaceId]); return work(tx); });
  let complete = false;
  const state: SearchProjectionState = {
    async fromMs() { return 0; },
    async record(_tx, _scope, value) { complete = value.complete; },
    async partials() { return { sessions: complete ? [] : [scope.sessionId], overflow: false }; },
  };
  const adapter = composeSearchService({ registry: createSearchIndexRegistry(), state: () => state }, { env: { SEARCH_INDEX_BACKEND: "postgres" }, sql: proj }, "writer", tenants);
  await adapter.index.ensureSchema();
  // The search stage's metadata transaction runs on the REAL pointer database (session_index, directory, locks).
  const transact: SearchIndexDeps["transact"] = (work) =>
    r.pg.transaction(async (tx) => work({ query: async (t: string, p?: unknown[]) => ({ rows: (await tx.query(t, p as unknown[])).rows }) } as SqlExecutor));
  const search: SearchIndexDeps = { transact, reader: r.reader, store: r.store, adapter, prefixFor: (org) => `96bulls/${org}`, now: () => NOW };
  const stamp = async (key: string) => {
    const meta = await r.store.head(objectKey(key));
    return meta ? { ...(meta.version.eTag ? { etag: meta.version.eTag } : {}), ...(meta.version.version ? { version: meta.version.version } : {}) } : null;
  };
  const pointerDeps = (): PointerDeps => {
    const base = r.pointer();
    return { ...base, db: { ...base.db, pool: r.pool } as PointerDeps["db"], ...(opts.reincarnation ? { reincarnation: { stamp } } : {}) };
  };
  const deps = (): ConsumerDeps => ({ pointer: pointerDeps(), search, requeue: async (k) => { r.requeued.push(k); } });
  let n = 0;
  const put = (key: string, eTag: string) => ({ messageId: `m${++n}`, body: r.body(key, "ObjectCreated:Put", String(n).padStart(4, "0"), eTag) });
  const del = (key: string) => ({ messageId: `m${++n}`, body: r.body(key, "ObjectRemoved:Delete", String(n).padStart(4, "0")) });
  const run = (records: { messageId: string; body: string }[]) => logged(() => processBatch(records, deps()));
  // The AUTHORIZED read: the real read API, a fixture token, and pointer liveness from the real session_index.
  const app = createReadApi({ adapter, completeSinceMs: 0,
    verify: async (token) => { if (token !== "fixture") throw new AuthError(401, "invalid_token", "invalid token"); return { ...tenant, userId: "user_1", scopes: ["rth:read"] }; },
    liveness: {
      async erased() { return new Set(); },
      async sessionStart() { return { pointers: 1, live: 1, createdAtMs: Date.parse("2026-11-01T10:00:00.000Z") }; },
      async livePointerSessions(org, ws, ids) {
        const live = (await r.pg.query<{ session_id: string }>(
          "SELECT DISTINCT session_id FROM sessions.session_index WHERE org_id=$1 AND workspace_id=$2 AND session_id=ANY($3) AND deleted_at IS NULL", [org, ws, [...ids]])).rows;
        return new Set(live.map((x) => x.session_id));
      },
    },
  });
  /** Marker ids an authorized caller gets back for a text search. */
  const hits = async (q: string) => {
    const res = await app.request(`/v1/events?workspace=${scope.workspaceId}&q=${encodeURIComponent(q)}&limit=50`, { headers: { authorization: "Bearer fixture" } });
    if (res.status !== 200) return { status: res.status, ids: [] as string[] };
    const body = (await res.json()) as { events: { text?: string; content?: string }[] };
    return { status: 200, ids: body.events.map((e) => String(e.text ?? e.content ?? "")) };
  };
  const etag = async (key: string) => (await r.store.head(objectKey(key)))!.version.eTag!.replace(/^"|"$/g, "");
  return { r, run, put, del, hits, etag, row: r.row };
}

describe("T139 standard path: erase, then re-create at revision 1", () => {
  test("FLAG OFF (control, the pre-fix behaviour): the re-created marker stays invisible until revision 2", async () => {
    const t = await rig({ reincarnation: false });
    const key = await t.r.writeManifest(1, loc, "markerold");
    await t.run([t.put(key, await t.etag(key))]);
    expect((await t.hits("markerold")).ids).toEqual(["markerold"]);
    await t.r.store.delete(objectKey(key));
    await t.run([t.del(key)]);
    await t.r.writeManifest(1, loc, "markernew");
    const { lines } = await t.run([t.put(key, await t.etag(key))]);
    expect(named(lines, "pointer indexed")).toEqual([expect.objectContaining({ outcome: "unchanged", revision: 1 })]);
    expect(await t.row()).toEqual([expect.objectContaining({ revision: "1", deleted_at: expect.anything() })]);
    expect((await t.hits("markernew")).ids).toEqual([]);
  }, 60_000);

  test("FLAG ON: the re-create's own event makes the new marker visible to an authorized caller at revision 1; the old marker never returns", async () => {
    const t = await rig({ reincarnation: true });
    const key = await t.r.writeManifest(1, loc, "markerold");
    await t.run([t.put(key, await t.etag(key))]);
    expect((await t.hits("markerold")).ids).toEqual(["markerold"]);
    await t.r.store.delete(objectKey(key));
    await t.run([t.del(key)]);
    expect((await t.hits("markerold")).ids).toEqual([]);
    await t.r.writeManifest(1, loc, "markernew");
    const { result, lines } = await t.run([t.put(key, await t.etag(key))]);
    expect(result).toEqual({ batchItemFailures: [] });
    expect(named(lines, "pointer indexed")).toEqual([expect.objectContaining({ outcome: "indexed", revision: 1 })]);
    expect(await t.row()).toEqual([expect.objectContaining({ revision: "1", deleted_at: null })]);
    if (process.env.T139_DEBUG) {
      console.error("DIR", JSON.stringify((await t.r.pg.query("SELECT session_id, deleted_at, event_projection_ready, event_projection_epoch, event_projection_sources FROM sessions.session_directory")).rows));
      console.error("PENDING", JSON.stringify((await t.r.pg.query("SELECT * FROM sessions.session_directory_event_pending")).rows));
      console.error("IDX", JSON.stringify((await t.r.pg.query("SELECT session_id, revision, deleted_at, indexed_at, canonical_session_id FROM sessions.session_index")).rows));
      const again = await t.run([t.put(key, await t.etag(key))]);
      console.error("AGAIN", JSON.stringify(named(again.lines, "search index reconciled")));
    }
    expect((await t.hits("markernew")).ids).toEqual(["markernew"]);
    expect((await t.hits("markerold")).ids).toEqual([]);
  }, 60_000);

  test("paused pre-erase work: a pass that read the old manifest, paused inside the lock across the erase and re-create, then resumed, cannot republish the old marker; the new one stays visible", async () => {
    const t = await rig({ reincarnation: true });
    const key = await t.r.writeManifest(1, loc, "markerold");
    await t.run([t.put(key, await t.etag(key))]);
    const oldTag = await t.etag(key);
    // A redelivered event of the old incarnation reads it, then pauses inside the lock just before its upsert.
    let release!: () => void;
    const gate = new Promise<void>((ok) => (release = ok));
    let paused!: () => void;
    const reached = new Promise<void>((ok) => (paused = ok));
    t.r.hooks.beforeUpsert = async () => { paused(); await gate; };
    const old = t.run([t.put(key, oldTag)]);
    await reached;
    // While it is paused: the plain delete and the re-create at revision 1 land in S3.
    await t.r.store.delete(objectKey(key));
    await t.r.writeManifest(1, loc, "markernew");
    release();
    const resumed = await old;
    expect(resumed.result).toEqual({ batchItemFailures: [] });
    // Its commit wrote nothing old: the row still names the old ETag at revision 1 (same object as stored), and its
    // search pass read whatever is LIVE, which is the new incarnation.
    expect((await t.hits("markerold")).ids).toEqual([]);
    // The delete's and the re-create's own events, delivered after: the row moves to the new incarnation.
    const { result } = await t.run([t.del(key), t.put(key, await t.etag(key))]);
    expect(result).toEqual({ batchItemFailures: [] });
    expect(await t.row()).toEqual([expect.objectContaining({ revision: "1", deleted_at: null })]);
    expect((await t.hits("markernew")).ids).toEqual(["markernew"]);
    expect((await t.hits("markerold")).ids).toEqual([]);
    // And once more, the old event delivered late after everything: still nothing old.
    await t.run([t.put(key, oldTag)]);
    expect((await t.hits("markernew")).ids).toEqual(["markernew"]);
    expect((await t.hits("markerold")).ids).toEqual([]);
  }, 60_000);

  test("commit-time refusal: a pass that read incarnation B, paused before commit while B is replaced by C, rolls back (pointer_not_current) and the retry publishes C", async () => {
    const t = await rig({ reincarnation: true });
    const key = await t.r.writeManifest(1, loc, "markerold");
    await t.run([t.put(key, await t.etag(key))]);
    await t.r.store.delete(objectKey(key));
    await t.run([t.del(key)]);
    await t.r.writeManifest(1, loc, "markerbee");
    const tagB = await t.etag(key);
    // The pass reads B, then (before its upsert commits) B is replaced by C at the same revision number.
    t.r.hooks.beforeUpsert = async () => { await t.r.store.delete(objectKey(key)); await t.r.writeManifest(1, loc, "markersea"); };
    const stale = await t.run([t.put(key, tagB)]);
    expect(stale.result.batchItemFailures).toHaveLength(1);
    expect(named(stale.lines, "pointer write not current, rolled back")).toHaveLength(1);
    // Nothing of B committed: the row is still the tombstone, and an authorized caller sees neither B nor the old.
    expect(await t.row()).toEqual([expect.objectContaining({ deleted_at: expect.anything() })]);
    expect((await t.hits("markerbee")).ids).toEqual([]);
    // SQS redelivers; the retry reads C under the lock and publishes it.
    const retry = await t.run([t.put(key, await t.etag(key))]);
    expect(retry.result).toEqual({ batchItemFailures: [] });
    expect((await t.hits("markersea")).ids).toEqual(["markersea"]);
    expect((await t.hits("markerbee")).ids).toEqual([]);
  }, 60_000);

  test("OPERATOR ERASE: the same sequence after a permanent erasure fence is refused; an authorized caller sees nothing", async () => {
    const t = await rig({ reincarnation: true });
    const key = await t.r.writeManifest(1, loc, "markerold");
    await t.run([t.put(key, await t.etag(key))]);
    // The operator erasure's permanent fence (20261105050000), then the delete.
    await t.r.pg.query("INSERT INTO sessions.session_erasure_fences (org_id, workspace_id, source, session_id) VALUES ($1,$2,$3,$4)",
      [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId]);
    await t.r.store.delete(objectKey(key));
    await t.run([t.del(key)]);
    await t.r.writeManifest(1, loc, "markernew");
    await t.run([t.put(key, await t.etag(key))]);
    expect(await t.row()).toEqual([expect.objectContaining({ deleted_at: expect.anything() })]);
    expect((await t.hits("markernew")).ids).toEqual([]);
    expect((await t.hits("markerold")).ids).toEqual([]);
  }, 60_000);

  test("a lower revision while the row is LIVE (no tombstone) is still refused, and counted", async () => {
    const t = await rig({ reincarnation: true });
    const key = await t.r.writeManifest(3, loc, "markerthree");
    await t.run([t.put(key, await t.etag(key))]);
    // S3 now holds revision 1 of a different incarnation, but no delete was processed: the fence keeps revision 3.
    await t.r.store.delete(objectKey(key));
    await t.r.writeManifest(1, loc, "markerone");
    const { lines } = await t.run([t.put(key, await t.etag(key))]);
    expect(await t.row()).toEqual([expect.objectContaining({ revision: "3", deleted_at: null })]);
    expect(named(lines, "pointer fence refused live write")).toEqual([expect.objectContaining({ revision: 1, stored_revision: 3, stored_deleted: false, incarnation_sha256: expect.stringMatching(/^[0-9a-f]{64}$/) })]);
  }, 60_000);
});
