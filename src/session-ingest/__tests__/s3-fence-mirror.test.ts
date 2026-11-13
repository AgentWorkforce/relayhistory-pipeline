/**
 * INGEST_S3_FENCE_MIRROR (app.ts s3FenceMirror, decision 0005 "Controls"): with the SQL ledger still the
 * Worker's authority, the selection and reset routes also write the S3 upload fence (ledger-s3.ts). Deletions:
 * S3 first, a failed S3 write fails the control and SQL is never called. Re-enables: after SQL, and never for a
 * stale ordered control. Off (absent): no S3 fence object is ever written.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { createIngestApp, SYNC_SCOPE } from "../app.ts";
import type { Ledger, LedgerTenant, SelectionInput } from "../ledger.ts";
import { s3Ledger, s3LedgerKeys } from "../ledger-s3.ts";
import { s3FenceMirrorFlag } from "../worker.ts";
import { MemoryLedger, SIGNING_KEY, TENANT, batch, scratchStore, signedHeaders } from "./support.ts";

const ROOT = "96bulls";
const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const enc = (s: string) => new TextEncoder().encode(s);
const fenceKey = s3LedgerKeys(ROOT, TENANT).fence;

function setup(mirror: "s3" | "failing" | "off", ledger = new MemoryLedger()) {
  const s = scratchStore();
  cleanups.push(s.done);
  const order: string[] = [];
  const sqlSelect = ledger.setSessionUploads.bind(ledger);
  ledger.setSessionUploads = async (i: SelectionInput) => { order.push(`sql:${i.enabled ? "enable" : "disable"}`); return sqlSelect(i); };
  const sqlReset = ledger.resetUploadedSessions.bind(ledger);
  ledger.resetUploadedSessions = async (t: LedgerTenant, r: string) => { order.push("sql:reset"); return sqlReset(t, r); };
  const real = s3Ledger(s.store, ROOT);
  const s3FenceMirror: Pick<Ledger, "setSessionUploads" | "resetUploadedSessions"> = mirror === "failing"
    ? { setSessionUploads: async () => { order.push("s3:fail"); throw new Error("secret store path"); }, resetUploadedSessions: async () => { order.push("s3:fail"); throw new Error("x"); } }
    : { setSessionUploads: async (i) => { order.push(`s3:${i.enabled ? "enable" : "disable"}`); return real.setSessionUploads(i); },
        resetUploadedSessions: async (t, r) => { order.push("s3:reset"); return real.resetUploadedSessions(t, r); } };
  const app = createIngestApp({ deliver: { ledger, target: { store: s.store, keyRoot: ROOT } }, log: () => {}, signingKey: SIGNING_KEY, ...(mirror === "off" ? {} : { s3FenceMirror }) });
  const post = async (path: string, value: unknown, identity: Record<string, unknown> = { ...TENANT, scopes: [SYNC_SCOPE] }) => {
    const body = JSON.stringify(value);
    return app.fetch(new Request(`https://x${path}`, { method: "POST", body, headers: signedHeaders(identity as never, "POST", path, enc(body)) }));
  };
  return { store: s.store, ledger, order, post };
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

  test("a stale ordered re-enable never lifts the S3 tombstone", async () => {
    const ledger = new MemoryLedger();
    const control = { epoch: "e".repeat(64), version: "3", digest: "d".repeat(64), kind: "selection" as const };
    ledger.applyOrderedControl = async () => ({ control: { ...control, outcome: "stale" as const }, updated: 0 });
    const { store, order, post } = setup("s3", ledger);
    await post("/v1/delivery/sessions/selection", await selection(false));
    const res = await post("/v1/delivery/sessions/selection", await selection(true), { ...TENANT, scopes: [SYNC_SCOPE], control });
    expect(res.status).toBe(200);
    expect(order).toEqual(["s3:disable", "sql:disable"]);
    expect((await fence(store)).deleted).toEqual({ "origin-1": [["claude", "s1"]] });
  });

  test("reset: S3 first (counted once by request id), then SQL", async () => {
    const { store, order, post } = setup("s3");
    const body = { account_id: (await batch([])).account_id, request_id: "req-1", confirmation: "DELETE ALL SESSIONS" };
    expect((await post("/v1/delivery/sessions/reset", body)).status).toBe(200);
    expect((await post("/v1/delivery/sessions/reset", body)).status).toBe(200);
    expect(order).toEqual(["s3:reset", "sql:reset", "s3:reset", "sql:reset"]);
    expect(await fence(store)).toMatchObject({ resets: 1, resetRequests: { "req-1": 0 } });
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
