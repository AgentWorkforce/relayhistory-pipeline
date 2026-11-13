/**
 * What INGEST_S3_FENCE_MIRROR=on needs beyond #593 (review of f40b7d04, N1-N7), against the STRICT SQL model of
 * sessions.ingest_apply_ordered_control: `stale` is returned only for a version at or below the head WITH a
 * same-digest receipt, so it means "applied earlier, since superseded", never "not applied". The invariant: the S3
 * fence is never live (for admission) where SQL is deleted.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { createIngestApp, SYNC_SCOPE } from "../app.ts";
import type { OrderedControlResult } from "../control-protocol.ts";
import { LedgerUnavailableError, type OrderedControlInput } from "../ledger.ts";
import { isDeletedForAdmission, s3ControlMirror, s3Ledger, s3LedgerKeys, validFence, type FenceDoc, type S3FenceMirror } from "../ledger-s3.ts";
import { MemoryLedger, SIGNING_KEY, TENANT, batch, scratchStore, signedHeaders } from "./support.ts";

const ROOT = "96bulls";
const E1 = "e".repeat(64), E2 = "f".repeat(64);
const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const enc = (s: string) => new TextEncoder().encode(s);
const fenceKey = s3LedgerKeys(ROOT, TENANT).fence;
const refusal = (sqlstate: string) => new LedgerUnavailableError("ledger_sql_refused", { db_error_class: "sql", db_error_code: sqlstate } as never);

/** The strict SQL model, with a rotatable admitted epoch. */
function strictSql() {
  const ledger = new MemoryLedger();
  const state = { epoch: E1, last: 0n, receipts: new Map<string, string>(), deleted: new Set<string>(), resets: [] as string[] };
  ledger.applyOrderedControl = async (i: OrderedControlInput): Promise<OrderedControlResult> => {
    const c = i.control;
    if (c.epoch !== state.epoch) throw refusal("55000");
    const v = BigInt(c.version);
    let outcome: "applied" | "replay" | "stale";
    if (v <= state.last) {
      if (state.receipts.get(c.version) !== c.digest) throw refusal("40001");
      outcome = v === state.last ? "replay" : "stale";
    } else {
      if (v !== state.last + 1n) throw refusal("40001");
      state.last = v;
      state.receipts.set(c.version, c.digest);
      if ("requestId" in i) state.resets.push(i.requestId);
      else for (const [, id] of i.sessions) { if (i.enabled) state.deleted.delete(id); else state.deleted.add(id); }
      outcome = "applied";
    }
    return "requestId" in i ? { control: { ...c, outcome }, deleted: 0 } : { control: { ...c, outcome }, updated: 1 };
  };
  return { ledger, state };
}

/** The real mirror, whose next `failFinish` finishOrdered calls fail (a dropped S3 write after SQL applied). */
function flakyMirror(store: ObjectStore) {
  const real = s3ControlMirror(store, ROOT);
  const ctl = { failFinish: 0 };
  const mirror: S3FenceMirror = { ...real, finishOrdered: async (i, o) => { if (ctl.failFinish > 0) { ctl.failFinish--; throw new Error("s3 write dropped"); } return real.finishOrdered(i, o); } };
  return { mirror, ctl };
}

function setup() {
  const s = scratchStore();
  cleanups.push(s.done);
  const sql = strictSql();
  const { mirror, ctl } = flakyMirror(s.store);
  const app = createIngestApp({ deliver: { ledger: sql.ledger, target: { store: s.store, keyRoot: ROOT } }, log: () => {}, signingKey: SIGNING_KEY, s3FenceMirror: mirror });
  const post = async (path: string, value: unknown, control: Record<string, unknown>) => {
    const body = JSON.stringify(value);
    return app.fetch(new Request(`https://x${path}`, { method: "POST", body, headers: signedHeaders({ ...TENANT, scopes: [SYNC_SCOPE], control } as never, "POST", path, enc(body)) }));
  };
  return { store: s.store, sql, post, ctl };
}
const control = (version: string, o: { epoch?: string; kind?: "selection" | "reset" } = {}) =>
  ({ epoch: o.epoch ?? E1, version, digest: `${o.kind === "reset" ? "a" : "d"}${version.padStart(63, "0")}`, kind: o.kind ?? "selection" });
const sel = async (sessionId: string, enabled: boolean) => ({ account_id: (await batch([])).account_id, origin_id: "origin-1", sessions: [{ source: "claude", session_id: sessionId }], enabled });
const resetBody = async (requestId: string) => ({ account_id: (await batch([])).account_id, request_id: requestId, confirmation: "DELETE ALL SESSIONS" });
async function fenceOf(store: ObjectStore): Promise<FenceDoc & { v: number }> {
  return JSON.parse(new TextDecoder().decode(await readAll((await store.getObject(objectKey(fenceKey))).body)));
}
const admissionDeleted = async (store: ObjectStore, id: string) => {
  const f = await fenceOf(store);
  return validFence(f) && isDeletedForAdmission(f, "origin-1", "claude", id);
};
const SELECT = "/v1/delivery/sessions/selection", RESET = "/v1/delivery/sessions/reset";

describe("N1: SQL `stale` means applied earlier, so its effect lands in S3", () => {
  test("disable v1 applied in SQL, its S3 finish lost; v2 lands; v1's retry is stale: S3 deletes s1 too", async () => {
    const { store, sql, post, ctl } = setup();
    ctl.failFinish = 1;
    expect((await post(SELECT, await sel("s1", false), control("1"))).status).toBe(503);
    expect((await post(SELECT, await sel("s2", false), control("2"))).status).toBe(200);
    expect((await post(SELECT, await sel("s1", false), control("1"))).status).toBe(200); // SQL: stale
    expect([...sql.state.deleted].sort()).toEqual(["s1", "s2"]);
    const f = await fenceOf(store);
    expect((f.deleted["origin-1"] ?? []).map(([, id]) => id).sort()).toEqual(["s1", "s2"]);
    expect(f.pending ?? {}).toEqual({});
  });

  test("a stale reset (applied earlier, S3 finish lost) begins the S3 reset", async () => {
    const { store, sql, post, ctl } = setup();
    ctl.failFinish = 1;
    expect((await post(RESET, await resetBody("req-1"), control("1", { kind: "reset" }))).status).toBe(503);
    expect((await post(SELECT, await sel("s9", false), control("2"))).status).toBe(200);
    expect((await post(RESET, await resetBody("req-1"), control("1", { kind: "reset" }))).status).toBe(200); // stale
    expect(sql.state.resets).toEqual(["req-1"]);
    expect(await fenceOf(store)).toMatchObject({ resets: 1, resetRequests: { "req-1": null } });
  });

  test("a stale retry for sessions no newer control touched still lands (N7)", async () => {
    const { store, post, ctl } = setup();
    ctl.failFinish = 1;
    expect((await post(SELECT, await sel("s1", false), control("1"))).status).toBe(503);
    expect((await post(SELECT, await sel("s7", true), control("2"))).status).toBe(200);
    expect((await post(SELECT, await sel("s1", false), control("1"))).status).toBe(200);
    expect(await admissionDeleted(store, "s1")).toBe(true);
  });

  test("applied, S3 finish lost, the epoch rotates, the retry is refused (55000): still deleted in S3 (never aborted)", async () => {
    const { store, sql, post, ctl } = setup();
    ctl.failFinish = 1;
    expect((await post(SELECT, await sel("s1", false), control("1"))).status).toBe(503);
    sql.state.epoch = E2;
    expect((await post(SELECT, await sel("s1", false), control("1"))).status).toBe(503);
    expect(sql.state.deleted.has("s1")).toBe(true);
    expect(await admissionDeleted(store, "s1")).toBe(true);
  });
});

describe("N2: only a request's own pending hold is released by its abort", () => {
  test("duplicate requests: one applied in SQL and died, the other refused: the pending entry stays (fail closed)", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const mirror = s3ControlMirror(s.store, ROOT);
    const input = { tenant: TENANT, originId: "origin-1", sessions: [["claude", "s1"]] as [string, string][], enabled: false, control: control("1") } as OrderedControlInput;
    const a = await mirror.beginOrdered(input);
    const b = await mirror.beginOrdered(input);
    expect(a).not.toBe(b);
    // B is applied in SQL and dies before its finish; A is refused definitely and aborts its own hold only.
    await mirror.abortOrdered(input, a);
    expect(await admissionDeleted(s.store, "s1")).toBe(true);
    await mirror.abortOrdered(input, b);
    expect(await admissionDeleted(s.store, "s1")).toBe(false);
  });

  test("an abort without a hold (or with another request's) releases nothing", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const mirror = s3ControlMirror(s.store, ROOT);
    const input = { tenant: TENANT, originId: "origin-1", sessions: [["claude", "s1"]] as [string, string][], enabled: false, control: control("1") } as OrderedControlInput;
    await mirror.beginOrdered(input);
    await mirror.abortOrdered(input, undefined);
    await mirror.abortOrdered(input, "00000000-0000-4000-8000-000000000000");
    expect(await admissionDeleted(s.store, "s1")).toBe(true);
  });
});

describe("N3: older fence readers fail closed while a control is pending", () => {
  test("the fence is v2 while anything is pending (pre-#593 readers accept only v1: they refuse, a retry), v1 again after", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const mirror = s3ControlMirror(s.store, ROOT);
    const input = { tenant: TENANT, originId: "origin-1", sessions: [["claude", "s1"]] as [string, string][], enabled: false, control: control("1") } as OrderedControlInput;
    await mirror.beginOrdered(input);
    expect((await fenceOf(s.store)).v).toBe(2);
    await expect(s3Ledger(s.store, ROOT).claim({ tenant: TENANT, originId: "origin-1", batchId: "b", batchDigest: "c".repeat(64), receiptId: `rhr_${"c".repeat(64)}`, sessions: [["claude", "s1"]], records: [] }))
      .resolves.toMatchObject({ kind: "replay_fast", deleted: [["claude", "s1"]] }); // a current reader handles it
    await mirror.finishOrdered(input, "applied");
    expect((await fenceOf(s.store)).v).toBe(1);
  });
});

describe("N5: across an epoch rotation the order of two controls is unknown: S3 fails closed", () => {
  const sessions = [["claude", "s1"]] as [string, string][];
  test("an old-epoch disable finishing after a new-epoch enable still deletes", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const mirror = s3ControlMirror(s.store, ROOT);
    await mirror.finishOrdered({ tenant: TENANT, originId: "origin-1", sessions, enabled: true, control: control("1", { epoch: E2 }) } as OrderedControlInput, "applied");
    await mirror.finishOrdered({ tenant: TENANT, originId: "origin-1", sessions, enabled: false, control: control("5", { epoch: E1 }) } as OrderedControlInput, "applied");
    expect(await admissionDeleted(s.store, "s1")).toBe(true);
  });
  test("an old-epoch enable finishing after a new-epoch disable does not lift", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    const mirror = s3ControlMirror(s.store, ROOT);
    await mirror.finishOrdered({ tenant: TENANT, originId: "origin-1", sessions, enabled: false, control: control("1", { epoch: E2 }) } as OrderedControlInput, "applied");
    await mirror.finishOrdered({ tenant: TENANT, originId: "origin-1", sessions, enabled: true, control: control("5", { epoch: E1 }) } as OrderedControlInput, "applied");
    expect(await admissionDeleted(s.store, "s1")).toBe(true);
  });
});
