/**
 * INGEST_S3_FENCE_MIRROR with ORDERED controls against a STRICT SQL model of sessions.ingest_apply_ordered_control
 * (20261104055000_lazy_current_custody.sql): a foreign epoch refuses (55000), a digest mismatch refuses (40001), a
 * version above last+1 is a gap and refuses (40001), an equal version replays (another digest refuses), a lower
 * one is stale. SQL is the ordering authority; the S3 fence must never be live where SQL is deleted, and a control
 * SQL refuses must leave no S3 effect (and never wedge the legitimate control).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { createIngestApp, SYNC_SCOPE } from "../app.ts";
import type { OrderedControlResult } from "../control-protocol.ts";
import { LedgerUnavailableError, type OrderedControlInput } from "../ledger.ts";
import { s3ControlMirror, s3Ledger, s3LedgerKeys } from "../ledger-s3.ts";
import { MemoryLedger, SIGNING_KEY, TENANT, batch, scratchStore, signedHeaders } from "./support.ts";

const ROOT = "96bulls";
const EPOCH = "e".repeat(64);
const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const enc = (s: string) => new TextEncoder().encode(s);
const fenceKey = s3LedgerKeys(ROOT, TENANT).fence;
const refusal = (sqlstate: string) => new LedgerUnavailableError("ledger_sql_refused", { db_error_class: "sql", db_error_code: sqlstate } as never);

/** The strict SQL model: one control head per user, deletions per session. `bad` marks a control whose digest SQL refuses. */
function strictSql() {
  const ledger = new MemoryLedger();
  let last = 0n;
  const receipts = new Map<string, string>();
  const deleted = new Set<string>();
  ledger.applyOrderedControl = async (i: OrderedControlInput): Promise<OrderedControlResult> => {
    const c = i.control;
    if (c.epoch !== EPOCH) throw refusal("55000"); // foreign control epoch
    if (c.digest.startsWith("bad")) throw refusal("40001"); // control digest mismatch
    const v = BigInt(c.version);
    let outcome: "applied" | "replay" | "stale";
    if (v <= last) {
      if (receipts.get(c.version) !== c.digest) throw refusal("40001"); // conflicting control replay
      outcome = v === last ? "replay" : "stale";
    } else {
      if (v !== last + 1n) throw refusal("40001"); // control version gap
      last = v;
      receipts.set(c.version, c.digest);
      if (!("requestId" in i)) for (const [, id] of i.sessions) { if (i.enabled) deleted.delete(id); else deleted.add(id); }
      outcome = "applied";
    }
    return "requestId" in i ? { control: { ...c, outcome }, deleted: 0 } : { control: { ...c, outcome }, updated: 1 };
  };
  return { ledger, deleted };
}

function setup(mirror: "control" | "ledger" = "control") {
  const s = scratchStore();
  cleanups.push(s.done);
  const sql = strictSql();
  const s3FenceMirror = mirror === "control" ? s3ControlMirror(s.store, ROOT) : s3Ledger(s.store, ROOT, { resetBeginOnly: true });
  const app = createIngestApp({ deliver: { ledger: sql.ledger, target: { store: s.store, keyRoot: ROOT } }, log: () => {}, signingKey: SIGNING_KEY, s3FenceMirror: s3FenceMirror as never });
  const post = async (path: string, value: unknown, control: Record<string, unknown>) => {
    const body = JSON.stringify(value);
    return app.fetch(new Request(`https://x${path}`, { method: "POST", body, headers: signedHeaders({ ...TENANT, scopes: [SYNC_SCOPE], control } as never, "POST", path, enc(body)) }));
  };
  return { store: s.store, sql, post };
}
const ctl = (version: string, o: { digest?: string; epoch?: string; kind?: "selection" | "reset" } = {}) =>
  ({ epoch: o.epoch ?? EPOCH, version, digest: o.digest ?? `${o.kind === "reset" ? "a" : "d"}${version.padStart(63, "0")}`, kind: o.kind ?? "selection" });
const sel = async (sessionId: string, enabled: boolean) => ({ account_id: (await batch([])).account_id, origin_id: "origin-1", sessions: [{ source: "claude", session_id: sessionId }], enabled });
async function s3Deleted(store: ObjectStore): Promise<string[]> {
  const head = await store.head(objectKey(fenceKey));
  if (!head) return [];
  const f = JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(fenceKey))).body)));
  return ((f.deleted["origin-1"] ?? []) as [string, string][]).map(([, id]) => id).sort();
}
const sqlDeleted = (sql: { deleted: Set<string> }) => [...sql.deleted].sort();

describe("ordered controls: SQL decides, S3 follows (never live in S3 where SQL is deleted)", () => {
  test("P1: disable v2 arrives before v1 (a gap SQL refuses), then v1, then v2's retry: S3 and SQL both delete s1 and s2", async () => {
    const { store, sql, post } = setup();
    const path = "/v1/delivery/sessions/selection";
    expect((await post(path, await sel("s2", false), ctl("2"))).status).toBe(503); // the gap
    expect((await post(path, await sel("s1", false), ctl("1"))).status).toBe(200);
    expect((await post(path, await sel("s2", false), ctl("2"))).status).toBe(200);
    expect(sqlDeleted(sql)).toEqual(["s1", "s2"]);
    expect(await s3Deleted(store)).toEqual(["s1", "s2"]);
  });

  test("P2: a control SQL refuses (bad digest, foreign epoch) leaves no S3 tombstone and never wedges the legitimate control", async () => {
    const { store, sql, post } = setup();
    const path = "/v1/delivery/sessions/selection";
    expect((await post(path, await sel("s1", false), ctl("1", { digest: `bad${"0".repeat(61)}` }))).status).toBe(503);
    expect(await s3Deleted(store)).toEqual([]);
    expect((await post(path, await sel("s3", false), ctl("1", { epoch: "f".repeat(64) }))).status).toBe(503);
    expect(await s3Deleted(store)).toEqual([]);
    expect((await post(path, await sel("s1", false), ctl("1"))).status).toBe(200);
    expect(sqlDeleted(sql)).toEqual(["s1"]);
    expect(await s3Deleted(store)).toEqual(["s1"]);
  });

  test("a disable is fail-closed in S3 while SQL decides: a claim during it sees the session deleted", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const sql = strictSql();
    const inner = sql.ledger.applyOrderedControl!;
    let seenDuring: unknown;
    sql.ledger.applyOrderedControl = async (i) => {
      seenDuring = await s3Ledger(s.store, ROOT).claim({ tenant: TENANT, originId: "origin-1", batchId: "mid", batchDigest: "c".repeat(64), receiptId: `rhr_${"c".repeat(64)}`, sessions: [["claude", "s1"]],
        records: [{ source: "claude", sessionId: "s1", recordId: "r1", revisionId: "r1:r1", revision: 1, digest: "a".repeat(64) }] });
      return inner(i);
    };
    const app = createIngestApp({ deliver: { ledger: sql.ledger, target: { store: s.store, keyRoot: ROOT } }, log: () => {}, signingKey: SIGNING_KEY, s3FenceMirror: s3ControlMirror(s.store, ROOT) });
    const body = JSON.stringify(await sel("s1", false));
    const path = "/v1/delivery/sessions/selection";
    expect((await app.fetch(new Request(`https://x${path}`, { method: "POST", body, headers: signedHeaders({ ...TENANT, scopes: [SYNC_SCOPE], control: ctl("1") } as never, "POST", path, enc(body)) }))).status).toBe(200);
    expect(seenDuring).toMatchObject({ kind: "replay_fast", deleted: [["claude", "s1"]] });
  });

  test("out-of-order finalisation per session: disable v1 s1, enable v2 s1 both applied in SQL; S3 ends live like SQL", async () => {
    const { store, sql, post } = setup();
    const path = "/v1/delivery/sessions/selection";
    expect((await post(path, await sel("s1", false), ctl("1"))).status).toBe(200);
    expect((await post(path, await sel("s1", true), ctl("2"))).status).toBe(200);
    expect(sqlDeleted(sql)).toEqual([]);
    expect(await s3Deleted(store)).toEqual([]);
    // A late retry of v1 (SQL: stale) changes nothing in S3.
    expect((await post(path, await sel("s1", false), ctl("1"))).status).toBe(200);
    expect(await s3Deleted(store)).toEqual([]);
  });

  test("an ambiguous SQL failure (no SQLSTATE) leaves the disable pending: fail closed until the retry's verdict", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const sql = strictSql();
    const inner = sql.ledger.applyOrderedControl!;
    let drop = true;
    sql.ledger.applyOrderedControl = async (i) => { const r = await inner(i); if (drop) { drop = false; throw new Error("connection reset"); } return r; };
    const app = createIngestApp({ deliver: { ledger: sql.ledger, target: { store: s.store, keyRoot: ROOT } }, log: () => {}, signingKey: SIGNING_KEY, s3FenceMirror: s3ControlMirror(s.store, ROOT) });
    const path = "/v1/delivery/sessions/selection";
    const send = async () => { const body = JSON.stringify(await sel("s1", false)); return app.fetch(new Request(`https://x${path}`, { method: "POST", body, headers: signedHeaders({ ...TENANT, scopes: [SYNC_SCOPE], control: ctl("1") } as never, "POST", path, enc(body)) })); };
    expect((await send()).status).toBe(503);
    // SQL applied it, the answer was lost: S3 still treats s1 as deleted (pending), never live.
    const f = JSON.parse(new TextDecoder().decode(await readAll((await s.store.getObject(objectKey(fenceKey))).body)));
    expect(Object.values(f.pending)).toEqual([expect.objectContaining({ kind: "selection", originId: "origin-1", sessions: [["claude", "s1"]] })]);
    expect((await s3Ledger(s.store, ROOT).claim({ tenant: TENANT, originId: "origin-1", batchId: "b", batchDigest: "c".repeat(64), receiptId: `rhr_${"c".repeat(64)}`, sessions: [["claude", "s1"]], records: [] })).kind).toBe("replay_fast");
    expect((await send()).status).toBe(200); // the retry: SQL replays, the verdict lands
    expect(await s3Deleted(s.store)).toEqual(["s1"]);
    const after = JSON.parse(new TextDecoder().decode(await readAll((await s.store.getObject(objectKey(fenceKey))).body)));
    expect(after.pending).toEqual({});
  });

  test("a pending ordered reset refuses every claim (a retry) until SQL's verdict lands", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const mirror = s3ControlMirror(s.store, ROOT);
    const input = { tenant: TENANT, requestId: "req-1", control: ctl("1", { kind: "reset" }) };
    await mirror.beginOrdered(input as never);
    const claim = () => s3Ledger(s.store, ROOT).claim({ tenant: TENANT, originId: "origin-1", batchId: "b", batchDigest: "c".repeat(64), receiptId: `rhr_${"c".repeat(64)}`, sessions: [["claude", "s1"]],
      records: [{ source: "claude", sessionId: "s1", recordId: "r1", revisionId: "r1:r1", revision: 1, digest: "a".repeat(64) }] });
    await expect(claim()).rejects.toMatchObject({ code: "ledger_control_pending" });
    await mirror.finishOrdered(input as never, "applied");
    expect((await claim()).kind).toBe("claimed");
  });

  test("an ordered reset SQL refuses begins nothing in S3; the applied one begins it once", async () => {
    const { store, post } = setup();
    const path = "/v1/delivery/sessions/reset";
    const body = { account_id: (await batch([])).account_id, request_id: "req-1", confirmation: "DELETE ALL SESSIONS" };
    expect((await post(path, body, ctl("3", { kind: "reset" }))).status).toBe(503); // a gap
    expect(await store.head(objectKey(fenceKey)).then(async (h) => h ? JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(fenceKey))).body))) : { resets: 0, pending: {} }))
      .toMatchObject({ resets: 0 });
    expect((await post(path, body, ctl("1", { kind: "reset" }))).status).toBe(200);
    const f = JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(fenceKey))).body)));
    expect(f).toMatchObject({ resets: 1, resetRequests: { "req-1": null } });
    expect(f.pending ?? {}).toEqual({});
  });
});
