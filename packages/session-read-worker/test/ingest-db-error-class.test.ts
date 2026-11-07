/** Formatter and route behaviour of the private ingest Worker's closed DB-error
 * classification INSIDE workerd: the real logger (src/shared/log.ts) writes the line, with
 * the closed class, safe code and basis, and nothing from the error's message/stack/URL or
 * any opaque code. The lines are captured by intercepting process.stdout/stderr.write; this
 * is NOT proof of delivery into real Cloudflare tail logs. Diagnostic only; no DB is needed. */
import { afterEach, describe, expect, test, vi } from "vitest";
import { dbClientErrorFields } from "../../../src/session-ingest/db-error-class.ts";
import { log } from "../../../src/shared/log.ts";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../../../src/session-ingest/app.ts";
import { sqlLedger } from "../../../src/session-ingest/ledger-sql.ts";
import { TENANT, batch, record } from "../../../src/session-ingest/__tests__/support.ts";

const SECRET = "password=hunter2 postgres://alice:s3cr3t@db.internal:5432/sessions token=eyJhbGciOi";
const NEEDLES = ["hunter2", "s3cr3t", "alice", "db.internal", "eyJhbGciOi", "postgres://", "Connection terminated"];

function capture() {
  const lines: string[] = [];
  const grab = (chunk: unknown) => { lines.push(String(chunk)); return true; };
  vi.spyOn(process.stdout, "write").mockImplementation(grab as never);
  vi.spyOn(process.stderr, "write").mockImplementation(grab as never);
  return lines;
}
afterEach(() => vi.restoreAllMocks());

describe("workerd: session ingest db error classification in the formatted log line (stdout interception)", () => {
  test("the pg Client 'error' line carries db_error_class and an allowlisted error_name, never the message", () => {
    const lines = capture();
    const e = Object.assign(new Error(`Connection terminated unexpectedly ${SECRET}`), { name: "Error" });
    log.error("session ingest db error", dbClientErrorFields(e));
    const line = lines.find(l => l.includes("session ingest db error"));
    expect(line).toBeDefined();
    const parsed = JSON.parse(line!) as Record<string, unknown>;
    expect(parsed).toMatchObject({ level: "error", error_code: "db_error", db_error_class: "terminated", error_name: "Error" });
    for (const n of NEEDLES) expect(line).not.toContain(n);
    // workerd's own network failure text classifies as reset.
    log.error("session ingest db error", dbClientErrorFields(new Error("Network connection lost.")));
    expect(JSON.parse(lines.at(-1)!)).toMatchObject({ db_error_class: "reset", db_error_basis: "message" });
  });

  test("opaque codes, inherited keys and throwing getters: closed fallbacks, nothing echoed, nothing thrown", () => {
    const lines = capture();
    const boom = () => { throw new Error(SECRET); };
    // [input, its allowlisted error_name]: a plain Error keeps "Error"; anything else is "other".
    const inputs: [unknown, string][] = [
      [Object.assign(new Error("x"), { code: "AbCd1234tokenXYZ" }), "Error"],
      [Object.assign(new Error("x"), { code: "ERR_TLS_AbCd1234tokenXYZ" }), "Error"],
      [Object.assign(new Error("x"), { code: "constructor", name: "__proto__" }), "other"],
      [Object.assign(new Error("x"), { code: "toString" }), "Error"],
      [Object.defineProperties(new Error(), { code: { get: boom }, message: { get: boom }, name: { get: boom } }), "other"],
      [new Proxy({}, { get: boom }), "other"],
    ];
    for (const [e] of inputs) expect(() => log.error("session ingest db error", dbClientErrorFields(e))).not.toThrow();
    const parsed = lines.filter(l => l.includes("session ingest db error")).map(l => JSON.parse(l) as Record<string, unknown>);
    expect(parsed).toHaveLength(inputs.length);
    parsed.forEach((p, i) => expect(p).toMatchObject({ error_code: "db_error", db_error_class: "unknown", db_error_basis: "none", error_name: inputs[i]![1] }));
    for (const l of lines) for (const n of [...NEEDLES, "AbCd1234"]) expect(l).not.toContain(n);
  });

  test("the /v1/delivery/batches 503 route line carries the same class and the correlation id", async () => {
    const lines = capture();
    // The real SQL ledger over a run that fails like a dropped Hyperdrive socket (no code).
    const ledger = sqlLedger(async () => { throw new Error(`Connection terminated unexpectedly ${SECRET}`); });
    const objects = new Map<string, Uint8Array>();
    const store = {
      async head() { return undefined; },
      async getObject() { throw new Error("not used"); },
      async putObject(key: string, body: Uint8Array) { objects.set(String(key), body); return { etag: "e" }; },
      async list() { return { items: [] }; },
      async deleteObject() {},
    };
    const app = createIngestApp({
      deliver: { ledger, target: { store: store as never, keyRoot: "96bulls", sleep: async () => {} } } as never,
      identify: (r) => bindingIdentity(r),
    });
    const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "e1" })]) });
    const res = await app.fetch(new Request("https://x/v1/delivery/batches", {
      method: "POST", body,
      headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] }), "x-correlation-id": "corr-workerd-503" },
    }));
    const route = lines.map(l => { try { return JSON.parse(l) as Record<string, unknown>; } catch { return undefined; } })
      .find(l => l?.route === "delivery.batches");
    expect(res.status).toBe(503);
    expect(route).toMatchObject({ outcome: "delivery_unavailable", correlation_id: "corr-workerd-503", db_error_class: "terminated", db_error_code: "db_error", db_error_basis: "message" });
    for (const l of lines) for (const n of NEEDLES) expect(l).not.toContain(n);
  });
});
