/**
 * T139 OFFLINE FIXTURE, checkpoint 2 (design f959bd92 / rev 3 fb6c7fd4, Pipeline acceptance d1cea47d043d): PARSED
 * AUTHORIZED reads end to end. PGlite with every migration, plus the two prototype SQL files applied inline (NOT
 * migrations): fixtures/t139-incarnation.prototype.sql (authority: k, per-owner incarnations, single-use receipts) and
 * fixtures/t139-pointer-upsert.prototype.sql (upsert_session_index with the incarnation-verified branch).
 *
 * Real: the consumer, the pointer stage and hoopdb upsert (IndexDeps.reincarnation: the authority check under k SHARED
 * in the publication transaction, then the current-key HEAD), indexSearchScope on the same database, the registered
 * Postgres search projection, and the read API (createReadApi GET /v1/events, fixture token; pointer liveness from the
 * real session_index). Mocked: S3 is the fs store; the scope lock is the harness mutex; PGlite is ONE connection, so
 * concurrency is modelled by pausing a pass at a hook (afterRead: after its manifest read and before its transaction,
 * i.e. before the shared k). Real two-backend lock evidence is t139-incarnation.postgres.test.ts.
 * Owner = (user_1, machine srv_bb): the production owner mapping is open (design rev 3 Q4).
 *
 * The delivery path is a parameter: this file runs the STANDARD queue (processBatch). The FIFO run is on the T126
 * composition branch (see the checkpoint report).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
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

const AUTHORITY = readFileSync(join(import.meta.dir, "fixtures/t139-incarnation.prototype.sql"), "utf8");
const UPSERT = readFileSync(join(import.meta.dir, "fixtures/t139-pointer-upsert.prototype.sql"), "utf8");
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const c of cleanup.splice(0).reverse()) await c(); });

const NOW = new Date("2026-11-02T00:00:00.000Z");
const scope = { orgId: "org_1", workspaceId: "ws_1", harness: "claude", sessionId: "sess_1" };
const tenant = { orgId: scope.orgId, workspaceId: scope.workspaceId };
const OWNER = { user: "user_1", origin: "srv_bb" };

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

async function rig() {
  const r = await pointerRig();
  cleanup.push(r.close);
  await r.pg.exec(AUTHORITY);
  await r.pg.exec(UPSERT);
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
  const transact: SearchIndexDeps["transact"] = (work) =>
    r.pg.transaction(async (tx) => work({ query: async (t: string, p?: unknown[]) => ({ rows: (await tx.query(t, p as unknown[])).rows }) } as SqlExecutor));
  const search: SearchIndexDeps = { transact, reader: r.reader, store: r.store, adapter, prefixFor: (org) => `96bulls/${org}`, now: () => NOW };
  /** The CURRENT-key HEAD (not version-specific); `afterHead` runs once right after it, before COMMIT. */
  const head: { afterHead?: () => Promise<void> } = {};
  const stamp = async (key: string) => {
    const meta = await r.store.head(objectKey(key));
    if (head.afterHead) { const h = head.afterHead; head.afterHead = undefined; await h(); }
    return meta ? { ...(meta.version.eTag ? { etag: meta.version.eTag } : {}), ...(meta.version.version ? { version: meta.version.version } : {}) } : null;
  };
  const pointerDeps = (reader?: PointerDeps["reader"]): PointerDeps => {
    const base = r.pointer();
    return { ...base, ...(reader ? { reader } : {}), db: { ...base.db, pool: r.pool } as PointerDeps["db"], reincarnation: { stamp } };
  };
  const deps = (reader?: PointerDeps["reader"]): ConsumerDeps => ({ pointer: pointerDeps(reader), search, requeue: async (k) => { r.requeued.push(k); } });
  let n = 0;
  const put = (key: string, eTag: string) => ({ messageId: `m${++n}`, body: r.body(key, "ObjectCreated:Put", String(n).padStart(4, "0"), eTag) });
  const del = (key: string) => ({ messageId: `m${++n}`, body: r.body(key, "ObjectRemoved:Delete", String(n).padStart(4, "0")) });
  const run = (records: { messageId: string; body: string }[], reader?: PointerDeps["reader"]) => logged(() => processBatch(records, deps(reader)));
  /**
   * A pointer-stage reader that serves the object as it was when the pass read it: every read of `key` by this pass
   * returns that snapshot (the publication path's cached read). Models a pass that read A and then paused.
   */
  const snapshotReader = async (key: string): Promise<PointerDeps["reader"]> => {
    const base = r.pointer().reader;
    const held = await base.read(key);
    return { ...base, read: async (k: string) => (k === key ? held : base.read(k)) } as PointerDeps["reader"];
  };
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
  /** The PARSED authorized response: the marker texts an authorized caller gets back. */
  /** The full authorized response: status and, on 200, the marker texts. */
  const read = async (q: string) => {
    const res = await app.request(`/v1/events?workspace=${scope.workspaceId}&q=${encodeURIComponent(q)}&limit=50`, { headers: { authorization: "Bearer fixture" } });
    if (res.status !== 200) return { status: res.status, ids: [] as string[] };
    const body = (await res.json()) as { events: { text?: string; content?: string }[] };
    return { status: 200, ids: body.events.map((e) => String(e.text ?? e.content ?? "")) };
  };
  const hits = async (q: string) => {
    const res = await app.request(`/v1/events?workspace=${scope.workspaceId}&q=${encodeURIComponent(q)}&limit=50`, { headers: { authorization: "Bearer fixture" } });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { events: { text?: string; content?: string }[] };
    return body.events.map((e) => String(e.text ?? e.content ?? ""));
  };
  const etag = async (key: string) => (await r.store.head(objectKey(key)))!.version.eTag!.replace(/^"|"$/g, "");
  // The authority writers (prototype SQL): ordinary deletion, reissue, operator erase.
  const authorityDelete = () => r.pg.query("SELECT sessions.t139_delete($1,$2,$3,$4,$5,$6)", [scope.orgId, scope.workspaceId, OWNER.user, OWNER.origin, scope.harness, scope.sessionId]);
  const reissue = async () => (await r.pg.query<{ n: string | null }>("SELECT sessions.t139_reissue($1,$2,$3,$4,$5,$6)::text n", [scope.orgId, scope.workspaceId, OWNER.user, OWNER.origin, scope.harness, scope.sessionId])).rows[0]!.n;
  const erase = () => r.pg.query("SELECT sessions.t139_erase($1,$2,$3,$4)", [scope.orgId, scope.workspaceId, scope.harness, scope.sessionId]);
  return { r, run, put, del, hits, read, etag, head, authorityDelete, reissue, erase, snapshotReader };
}
type Rig = Awaited<ReturnType<typeof rig>>;

/** A indexed at incarnation 0, then an ordinary delete (S3 object gone, its event, and the authority's receipt). */
async function aThenDelete(t: Rig) {
  const key = await t.r.writeManifest(1, loc, "markerold");
  const aTag = await t.etag(key);
  await t.run([t.put(key, aTag)]);
  expect(await t.hits("markerold")).toEqual(["markerold"]);
  return { key, aTag };
}

describe("T139 standard path: parsed authorized reads", () => {
  test("ordinary re-create: delete, reissue, B at revision 1 (incarnation 1): B's own event makes B visible to an authorized caller; A never returns", async () => {
    const t = await rig();
    const { key } = await aThenDelete(t);
    await t.r.store.delete(objectKey(key));
    await t.authorityDelete();
    await t.run([t.del(key)]);
    expect(await t.hits("markerold")).toEqual([]);
    expect(await t.reissue()).toBe("1");
    await t.r.writeManifest(1, loc, "markernew", 1);
    const { result } = await t.run([t.put(key, await t.etag(key))]);
    expect(result).toEqual({ batchItemFailures: [] });
    expect(await t.hits("markernew")).toEqual(["markernew"]);
    expect(await t.hits("markerold")).toEqual([]);
  }, 60_000);

  test("paused predecessor, B's event WITHHELD: A's pass paused after its read and before the shared k, across delete + reissue + B's PUT; resumed, it is refused; then B's event makes B visible; a late A after that changes nothing", async () => {
    const t = await rig();
    const { key, aTag } = await aThenDelete(t);
    // A's pass reads A, then pauses BEFORE its publication transaction (before the shared k). PGlite is one
    // connection, so the pause is modelled by the pass's held read: it publishes from the A object it read.
    // The predecessor holds a LATER revision of the old incarnation (A's append, revision 2, written just before the
    // delete). The revision fence alone would admit it over a revision-1 tombstone: only the incarnation check refuses it.
    await t.r.writeManifest(2, loc, "markerold");
    const heldA = await t.snapshotReader(key);
    const aTwoTag = await t.etag(key);
    // While A's pass is paused: the ordinary delete (S3, its own event, the authority's receipt), the reissue, and B's
    // PUT. B's event is WITHHELD. (Processed after B's PUT, the delete event would itself re-read and publish live B.)
    await t.r.store.delete(objectKey(key));
    await t.run([t.del(key)]);
    await t.authorityDelete();
    expect(await t.reissue()).toBe("1");
    await t.r.writeManifest(1, loc, "markernew", 1);
    // Resumed: its check runs under the shared k after the reissue committed: refused.
    const resumed = await t.run([t.put(key, aTwoTag)], heldA);
    expect(resumed.result).toEqual({ batchItemFailures: [] });
    expect(named(resumed.lines, "pointer write refused by incarnation authority")).toEqual([expect.objectContaining({ reason: "incarnation_moved" })]);
    // The authorized read with B still withheld: neither marker.
    expect(await t.hits("markerold")).toEqual([]);
    expect(await t.hits("markernew")).toEqual([]);
    // B's event released: B at revision 1 is visible to an authorized caller.
    expect((await t.run([t.put(key, await t.etag(key))])).result).toEqual({ batchItemFailures: [] });
    expect(await t.hits("markernew")).toEqual(["markernew"]);
    // After predecessor completion, once more a late A (revision 2, the held read): B stays, A never returns.
    await t.run([t.put(key, aTwoTag)], await (async () => heldA)());
    await t.run([t.put(key, aTag)]);
    expect(await t.hits("markernew")).toEqual(["markernew"]);
    expect(await t.hits("markerold")).toEqual([]);
  }, 60_000);

  test("permanent erase: reissue is refused; a re-created B (incarnation 0 or 1) is refused (erased); the marker stays absent", async () => {
    const t = await rig();
    const { key } = await aThenDelete(t);
    await t.erase();
    await t.r.store.delete(objectKey(key));
    await t.authorityDelete();
    await t.run([t.del(key)]);
    expect(await t.reissue()).toBeNull();
    for (const inc of [0, 1]) {
      await t.r.writeManifest(1, loc, "markernew", inc);
      const { lines } = await t.run([t.put(key, await t.etag(key))]);
      expect(named(lines, "pointer write refused by incarnation authority")).toEqual([expect.objectContaining({ reason: "erased" })]);
      expect(await t.hits("markernew")).toEqual([]);
      expect(await t.hits("markerold")).toEqual([]);
    }
  }, 60_000);

  test("RESIDUAL WINDOW (same incarnation, reported separately): a PUT after the current-key HEAD and before COMMIT, its event withheld; record the commit and the parsed read before repair, then the repair by its own event", async () => {
    const t = await rig();
    const key = await t.r.writeManifest(1, loc, "markerone");
    await t.run([t.put(key, await t.etag(key))]);
    await t.r.writeManifest(2, loc, "markertwo");
    const twoTag = await t.etag(key);
    // Right after the HEAD (which still sees revision 2) and before COMMIT, revision 3 is PUT; its event is withheld.
    t.head.afterHead = async () => { await t.r.writeManifest(3, loc, "markerthree"); };
    const { result, lines } = await t.run([t.put(key, twoTag)]);
    expect(result).toEqual({ batchItemFailures: [] });
    const committed = (await t.r.pg.query<{ revision: string }>("SELECT revision::text FROM sessions.session_index")).rows;
    const beforeRepair = { two: await t.read("markertwo"), three: await t.read("markerthree") };
    console.error("T139_RESIDUAL", JSON.stringify({ committed, beforeRepair, pointer: named(lines, "pointer indexed").map((l) => l.outcome) }));
    // The pointer committed revision 2 (the HEAD saw it): the residual window is real at the pointer row. The search
    // pass then read the LIVE object (revision 3) against a revision-2 pointer: sources mismatch, the pass is incomplete,
    // and the read API refuses to serve the scope (501) until repair. No stale marker is served in the window.
    expect(committed).toEqual([{ revision: "2" }]);
    expect(beforeRepair).toEqual({ two: { status: 501, ids: [] }, three: { status: 501, ids: [] } });
    // Repair: revision 3's own event.
    await t.run([t.put(key, await t.etag(key))]);
    expect((await t.r.pg.query<{ revision: string }>("SELECT revision::text FROM sessions.session_index")).rows).toEqual([{ revision: "3" }]);
    expect(await t.hits("markerthree")).toEqual(["markerthree"]);
    expect(await t.hits("markertwo")).toEqual([]);
  }, 60_000);
});
