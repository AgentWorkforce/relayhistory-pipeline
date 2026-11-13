/**
 * INGEST_S3_FENCE_MIRROR (app.ts s3FenceMirror, decision 0005 "Controls"): with the SQL ledger still the
 * Worker's authority, the selection and reset routes also write the S3 upload fence (ledger-s3.ts). Deletions:
 * S3 first, a failed S3 write fails the control and SQL is never called. Re-enables: after SQL, and never for a
 * stale ordered control. Off (absent): no S3 fence object is ever written.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { createIngestApp, SYNC_SCOPE } from "../app.ts";
import type { Ledger, LedgerTenant, OrderedControlInput, SelectionInput } from "../ledger.ts";
import type { OrderedControlResult } from "../control-protocol.ts";
import { s3ControlMirror, s3Ledger, s3LedgerKeys, type S3FenceMirror } from "../ledger-s3.ts";
import { journalKey } from "../async-admission.ts";
import { s3FenceMirrorFlag } from "../worker.ts";
import { MemoryLedger, SIGNING_KEY, TENANT, batch, scratchStore, signedHeaders } from "./support.ts";

const ROOT = "96bulls";
const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const enc = (s: string) => new TextEncoder().encode(s);
const fenceKey = s3LedgerKeys(ROOT, TENANT).fence;

function setup(mirror: "s3" | "failing" | "off", ledger = new MemoryLedger(), mirrorOpts: Parameters<typeof s3Ledger>[2] = {}, appExtra: Record<string, unknown> = {}) {
  const s = scratchStore();
  cleanups.push(s.done);
  const order: string[] = [];
  const sqlSelect = ledger.setSessionUploads.bind(ledger);
  ledger.setSessionUploads = async (i: SelectionInput) => { order.push(`sql:${i.enabled ? "enable" : "disable"}`); return sqlSelect(i); };
  const sqlReset = ledger.resetUploadedSessions.bind(ledger);
  ledger.resetUploadedSessions = async (t: LedgerTenant, r: string) => { order.push("sql:reset"); return sqlReset(t, r); };
  const real = mirrorOpts.resetBeginOnly === undefined ? s3ControlMirror(s.store, ROOT) : s3Ledger(s.store, ROOT, mirrorOpts);
  const logs: Record<string, unknown>[] = [];
  const kindOf = (i: OrderedControlInput) => "requestId" in i ? "reset" : i.enabled ? "enable" : "disable";
  const fail = async (): Promise<never> => { order.push("s3:fail"); throw new Error("secret store path"); };
  const ordered = real as S3FenceMirror;
  const s3FenceMirror: S3FenceMirror = mirror === "failing"
    ? { setSessionUploads: fail, resetUploadedSessions: fail, beginOrdered: fail, abortOrdered: fail, finishOrdered: fail }
    : { setSessionUploads: async (i) => { order.push(`s3:${i.enabled ? "enable" : "disable"}`); return real.setSessionUploads(i); },
        resetUploadedSessions: async (t, r) => { order.push("s3:reset"); return real.resetUploadedSessions(t, r); },
        beginOrdered: async (i) => { order.push(`s3:begin:${kindOf(i)}:v${i.control.version}`); return ordered.beginOrdered(i); },
        abortOrdered: async (i) => { order.push(`s3:abort:${kindOf(i)}:v${i.control.version}`); return ordered.abortOrdered(i); },
        finishOrdered: async (i, outcome) => { order.push(`s3:ordered:${kindOf(i)}:v${i.control.version}:${outcome}`); return ordered.finishOrdered(i, outcome); } };
  const app = createIngestApp({ deliver: { ledger, target: { store: s.store, keyRoot: ROOT } }, log: (f) => void logs.push(f), signingKey: SIGNING_KEY, ...(mirror === "off" ? {} : { s3FenceMirror }), ...appExtra });
  const post = async (path: string, value: unknown, identity: Record<string, unknown> = { ...TENANT, scopes: [SYNC_SCOPE] }) => {
    const body = JSON.stringify(value);
    return app.fetch(new Request(`https://x${path}`, { method: "POST", body, headers: signedHeaders(identity as never, "POST", path, enc(body)) }));
  };
  return { store: s.store, ledger, order, post, logs };
}
const fence = async (store: ObjectStore) => JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(fenceKey))).body)));
const selection = async (enabled: boolean) => ({ account_id: (await batch([])).account_id, origin_id: "origin-1", sessions: [{ source: "claude", session_id: "s1" }], enabled });

describe("s3FenceMirror", () => {
  test("disable: S3 first, then SQL; the S3 fence holds the deletion", async () => {
    const { store, order, post } = setup("s3");
    expect((await post("/v1/delivery/sessions/selection", await selection(false))).status).toBe(200);
    expect(order).toEqual(["s3:disable", "sql:disable"]);
    expect((await fence(store)).deleted).toEqual({ "origin-1": [["claude", "s1"]] });
  });

  test("a failed S3 write fails the control with a coded 503, and SQL is never called", async () => {
    const { ledger, order, post } = setup("failing");
    const res = await post("/v1/delivery/sessions/selection", await selection(false));
    expect(res.status).toBe(503);
    expect(await res.text()).not.toContain("secret store path");
    expect(order).toEqual(["s3:fail"]);
    expect(ledger.selections).toEqual([]);
    const reset = await post("/v1/delivery/sessions/reset", { account_id: (await batch([])).account_id, request_id: "req-1", confirmation: "DELETE ALL SESSIONS" });
    expect(reset.status).toBe(503);
    expect(order).toEqual(["s3:fail", "s3:fail"]);
  });

  test("re-enable: SQL first, then the S3 tombstone is lifted", async () => {
    const { store, order, post } = setup("s3");
    await post("/v1/delivery/sessions/selection", await selection(false));
    expect((await post("/v1/delivery/sessions/selection", await selection(true))).status).toBe(200);
    expect(order).toEqual(["s3:disable", "sql:disable", "sql:enable", "s3:enable"]);
    expect((await fence(store)).deleted).toEqual({});
  });

  test("a stale ordered re-enable never lifts the S3 tombstone of the newer disable that superseded it", async () => {
    const ledger = new MemoryLedger();
    const v = (version: string) => ({ epoch: "e".repeat(64), version, digest: "d".repeat(63) + version, kind: "selection" as const });
    // SQL: disable v4 applied; enable v3 is stale (applied earlier, superseded by v4).
    ledger.applyOrderedControl = async (i: OrderedControlInput) => ({ control: { ...i.control, outcome: i.control.version === "4" ? "applied" as const : "stale" as const }, updated: 1 });
    const { store, order, post } = setup("s3", ledger);
    expect((await post("/v1/delivery/sessions/selection", await selection(false), { ...TENANT, scopes: [SYNC_SCOPE], control: v("4") })).status).toBe(200);
    expect((await post("/v1/delivery/sessions/selection", await selection(true), { ...TENANT, scopes: [SYNC_SCOPE], control: v("3") })).status).toBe(200);
    // The stale verdict lands under per-session order: v4 already landed for s1, so v3's lift does not.
    expect(order).toContain("s3:ordered:enable:v3:stale");
    expect((await fence(store)).deleted).toEqual({ "origin-1": [["claude", "s1"]] });
  });

  test("reset: S3 first (counted once by request id), then SQL", async () => {
    const { store, order, post } = setup("s3");
    const body = { account_id: (await batch([])).account_id, request_id: "req-1", confirmation: "DELETE ALL SESSIONS" };
    expect((await post("/v1/delivery/sessions/reset", body)).status).toBe(200);
    expect((await post("/v1/delivery/sessions/reset", body)).status).toBe(200);
    expect(order).toEqual(["s3:reset", "sql:reset", "s3:reset", "sql:reset"]);
    // The Worker's mirror only begins a reset (the next S3-ledger claim finishes it).
    expect(await fence(store)).toMatchObject({ resets: 1, resetsCompleted: 0, resetRequests: { "req-1": null } });
  });

  test("absent (the default): no S3 fence object is written", async () => {
    const { store, order, post } = setup("off");
    expect((await post("/v1/delivery/sessions/selection", await selection(false))).status).toBe(200);
    expect(order).toEqual(["sql:disable"]);
    expect(await store.head(objectKey(fenceKey))).toBeUndefined();
  });

  test("INGEST_S3_FENCE_MIRROR parses off | on only (an invalid value refuses, never off)", () => {
    for (const off of [undefined, "", "off", " off "]) expect(s3FenceMirrorFlag(off)).toBe(false);
    expect(s3FenceMirrorFlag("on")).toBe(true);
    for (const bad of ["yes", "ON", "1", "true"]) expect(() => s3FenceMirrorFlag(bad)).toThrow(/INGEST_S3_FENCE_MIRROR must be off or on/);
  });
});

/** A SQL ledger whose ordered controls are judged by version (higher applies, equal replays, lower is stale), as SQL does. */
function orderedSql(hooks: { beforeAnswer?: (version: string, enabled: boolean | null) => Promise<void> } = {}) {
  const ledger = new MemoryLedger();
  let last = 0n;
  const deleted = new Set<string>();
  ledger.applyOrderedControl = async (i: OrderedControlInput): Promise<OrderedControlResult> => {
    const v = BigInt(i.control.version);
    const outcome = v > last ? "applied" : v === last ? "replay" : "stale";
    if (outcome === "applied") {
      last = v;
      if ("requestId" in i) { /* SQL reset: nothing owned in this fixture */ }
      else for (const [, id] of i.sessions) { if (i.enabled) deleted.delete(id); else deleted.add(id); }
    }
    await hooks.beforeAnswer?.(i.control.version, "requestId" in i ? null : i.enabled);
    return "requestId" in i ? { control: { ...i.control, outcome }, deleted: 0 } : { control: { ...i.control, outcome }, updated: 1 };
  };
  return { ledger, deleted };
}
const ctl = (version: string, kind: "selection" | "reset" = "selection") => ({ epoch: "e".repeat(64), version, digest: (kind === "reset" ? "a" : "d").repeat(62) + version.padStart(2, "0"), kind });
const as = (control: ReturnType<typeof ctl>) => ({ ...TENANT, scopes: [SYNC_SCOPE], control });

describe("s3FenceMirror: ordered controls go through the fence's ordered mark (review of #581)", () => {
  test("P1: a concurrent ordered enable v5 and disable v6 (the disable completes while v5's S3 lift is pending) leaves BOTH S3 and SQL deleted", async () => {
    let post: ReturnType<typeof setup>["post"];
    let raced = false;
    const sql = orderedSql({ beforeAnswer: async (version, enabled) => {
      // Enable v5 is applied in SQL; before its S3 lift runs, disable v6 runs to completion (S3 and SQL).
      if (version === "5" && enabled === true && !raced) { raced = true; expect((await post("/v1/delivery/sessions/selection", await selection(false), as(ctl("6")))).status).toBe(200); }
    } });
    const env = setup("s3", sql.ledger);
    post = env.post;
    expect((await post("/v1/delivery/sessions/selection", await selection(false), as(ctl("4")))).status).toBe(200);
    expect((await post("/v1/delivery/sessions/selection", await selection(true), as(ctl("5")))).status).toBe(200);
    expect(sql.deleted.has("s1")).toBe(true);
    expect((await fence(env.store)).deleted).toEqual({ "origin-1": [["claude", "s1"]] });
    // SQL applied v5 before v6; S3 keeps per-session order, so v5's late lift does not land under v6.
    expect(env.order).toContain("s3:ordered:enable:v5:applied");
  });

  test("P2: a stale ordered disable (after enable v7) writes nothing to S3", async () => {
    const sql = orderedSql();
    const env = setup("s3", sql.ledger);
    expect((await env.post("/v1/delivery/sessions/selection", await selection(true), as(ctl("7")))).status).toBe(200);
    expect((await env.post("/v1/delivery/sessions/selection", await selection(false), as(ctl("6")))).status).toBe(200);
    expect(env.order).toContain("s3:ordered:disable:v6:stale");
    expect((await fence(env.store)).deleted).toEqual({});
    expect(sql.deleted.has("s1")).toBe(false);
  });

  test("a stale ordered reset (SQL applied it earlier) begins it in S3, idempotently by request id", async () => {
    const sql = orderedSql();
    const env = setup("s3", sql.ledger);
    expect((await env.post("/v1/delivery/sessions/selection", await selection(false), as(ctl("9")))).status).toBe(200);
    const body = { account_id: (await batch([])).account_id, request_id: "req-late", confirmation: "DELETE ALL SESSIONS" };
    expect((await env.post("/v1/delivery/sessions/reset", body, as(ctl("8", "reset")))).status).toBe(200);
    expect(env.order).toContain("s3:ordered:reset:v8:stale");
    expect(await fence(env.store)).toMatchObject({ resets: 1, resetRequests: { "req-late": null } });
    expect((await env.post("/v1/delivery/sessions/reset", body, as(ctl("8", "reset")))).status).toBe(200);
    expect(await fence(env.store)).toMatchObject({ resets: 1 });
  });

  test("P2: a mis-kinded control is refused before any S3 write", async () => {
    const sql = orderedSql();
    const env = setup("s3", sql.ledger);
    const res = await env.post("/v1/delivery/sessions/selection", await selection(false), as(ctl("3", "reset")));
    expect(res.status).toBe(503);
    expect(env.order).toEqual([]);
    expect(await env.store.head(objectKey(fenceKey))).toBeUndefined();
    const body = { account_id: (await batch([])).account_id, request_id: "req-1", confirmation: "DELETE ALL SESSIONS" };
    expect((await env.post("/v1/delivery/sessions/reset", body, as(ctl("3")))).status).toBe(503);
    expect(await env.store.head(objectKey(fenceKey))).toBeUndefined();
  });

  test("P2: the reset's S3 write is inside the privacy-journal bracket: a journal refusal changes nothing in S3", async () => {
    const asyncOn = { asyncAdmission: { mode: "on", enqueue: async () => {}, selfhost: { identityOf: () => undefined, reEnableRefusedByLedger: false } } };
    const body = { account_id: (await batch([])).account_id, request_id: "req-1", confirmation: "DELETE ALL SESSIONS" };
    // The journal refuses an unordered control with async admission on.
    const unordered = setup("s3", new MemoryLedger(), {}, asyncOn);
    expect((await unordered.post("/v1/delivery/sessions/reset", body)).status).toBe(503);
    expect(unordered.order).toEqual([]);
    expect(await unordered.store.head(objectKey(fenceKey))).toBeUndefined();
    // An unreadable journal refuses an ordered one.
    const sql = orderedSql();
    const broken = setup("s3", sql.ledger, {}, asyncOn);
    await broken.store.put(objectKey(journalKey(ROOT, TENANT)), enc("not a journal"), { contentType: "application/json" });
    expect((await broken.post("/v1/delivery/sessions/reset", body, as(ctl("3", "reset")))).status).toBe(503);
    expect(broken.order).toEqual([]);
    expect(await broken.store.head(objectKey(fenceKey))).toBeUndefined();
  });

  test("P3: an S3 failure answers a coded 503 and logs the ledger's own code; an id the fence refuses is a 400 before SQL", async () => {
    const env = setup("s3");
    await env.store.put(objectKey(fenceKey), enc("garbage"), { contentType: "application/json" });
    expect((await env.post("/v1/delivery/sessions/selection", await selection(false))).status).toBe(503);
    expect(env.logs).toContainEqual(expect.objectContaining({ route: "delivery.s3_fence_mirror", outcome: "ledger_fence_invalid" }));
    expect(env.order).toEqual(["s3:disable"]); // attempted, failed; SQL never called
    const ok = setup("s3");
    const res = await ok.post("/v1/delivery/sessions/selection", { ...(await selection(false)), origin_id: "__proto__" });
    expect(res.status).toBe(400);
    expect(ok.order.filter((o) => o.startsWith("sql:"))).toEqual([]);
  });
});

describe("s3FenceMirror: the Worker's reset only begins in S3 (subrequest budget)", () => {
  test("resetBeginOnly: one fence CAS and no LIST however many sessions; SQL still runs; a full ledger's claim finishes it", async () => {
    const env = setup("s3", new MemoryLedger(), { resetBeginOnly: true });
    const full = s3Ledger(env.store, ROOT, { sleep: async () => {} });
    for (let i = 0; i < 30; i++) {
      const b = { tenant: TENANT, originId: "origin-1", batchId: `b${i}`, batchDigest: "c".repeat(62) + String(i).padStart(2, "0"), receiptId: `rhr_${"f".repeat(62)}${String(i).padStart(2, "0")}`,
        sessions: [["claude", `s${i}`]] as [string, string][],
        records: [{ source: "claude", sessionId: `s${i}`, recordId: `r${i}`, revisionId: `r${i}:r1`, revision: 1, digest: "a".repeat(64) }] };
      await full.claim(b); // registers the owner stub
    }
    let lists = 0, puts = 0;
    const counted: ObjectStore = { ...env.store, backend: env.store.backend, head: (k) => env.store.head(k), get: (k, r) => env.store.get(k, r), getObject: (k, r) => env.store.getObject(k, r),
      presignGet: (k, t) => env.store.presignGet(k, t), delete: (k) => env.store.delete(k), list: (p, c) => { lists++; return env.store.list(p, c); }, put: (k, b, o) => { puts++; return env.store.put(k, b, o); } };
    expect(await s3Ledger(counted, ROOT, { resetBeginOnly: true }).resetUploadedSessions(TENANT, "req-1")).toBe(0);
    expect(lists).toBe(0);
    expect(puts).toBe(1);
    expect(await fence(env.store)).toMatchObject({ resets: 1, resetsCompleted: 0, resetRequests: { "req-1": null } });
    const body = { account_id: (await batch([])).account_id, request_id: "req-2", confirmation: "DELETE ALL SESSIONS" };
    expect((await env.post("/v1/delivery/sessions/reset", body)).status).toBe(200);
    expect(env.order).toEqual(["s3:reset", "sql:reset"]);
    // The next claim through a full ledger finishes every begun reset.
    await full.claim({ tenant: TENANT, originId: "origin-1", batchId: "after", batchDigest: "9".repeat(64), receiptId: `rhr_${"9".repeat(64)}`, sessions: [["claude", "new"]], records: [] });
    expect(await fence(env.store)).toMatchObject({ resets: 2, resetsCompleted: 2, resetRequests: { "req-1": 30, "req-2": 30 } });
  });
});
