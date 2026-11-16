/** The batch route line's closed diagnostics: `db_refusal` for a ledger 55000 (from the message's known
 * prefix, never the message) and `client_*` from the User-Agent (never its text). Diagnostic only. */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../app.ts";
import { CLIENT_KINDS, CLIENT_OSES, CLIENT_VERSION, clientFields } from "../client-agent.ts";
import { dbErrorDiagnosis } from "../db-error-class.ts";
import { DB_REFUSALS, dbRefusalFromMessage, GENERATION_REFUSALS } from "../db-refusal-class.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { LedgerUnavailableError } from "../ledger.ts";
import { CLIENT_KIND_VALUES, CLIENT_OS_VALUES, CLIENT_VERSION_SHAPE, DB_REFUSAL_VALUES, logLine, sanitizeLogField } from "../../shared/log.ts";
import { batch, record, scratchStore, TENANT } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));

const pg = (code: string, message: string) => Object.assign(new Error(message), { code });

/** Representative messages, verbatim from supabase/migrations (pinned below), with their class. */
const CASES: readonly (readonly [string, string])[] = [
  ["session ingest: generation not ready", "generation_not_ready"],
  ["session ingest: policy expired during operation", "policy_expired"],
  ["session ingest: current custody policy unavailable", "policy_unavailable"],
  ["session ingest: retained org admission closed or conflicting", "org_admission_closed"],
  ["session ingest: buffered admission closed", "org_admission_closed"],
  ["session ingest: unbound prior org authority requires recovery", "org_prior_authority_unbound"],
  ["session ingest: existing user privacy authority changed", "user_privacy_authority"],
  ["session ingest: unsealed user authority cannot be adopted", "user_privacy_authority"],
  ["session ingest: prior user authority requires reviewed recovery", "user_privacy_authority"],
  ["session ingest: complete bounded cold privacy required", "user_privacy_authority"],
  ["session ingest: actual privacy seal required for ACK", "ack_seal"],
  ["session ingest: real user seal required before any receipt", "ack_seal"],
  ["session ingest: native custody identity changed", "native_custody_identity_changed"],
  ["session ingest: source current row is already privacy fenced", "source_row_privacy_fenced"],
  ["session ingest: inconsistent current custody privacy", "custody_privacy_inconsistent"],
  ["session ingest: user privacy seal missing", "user_seal_missing"],
  ["session ingest: buffered receipt proof missing", "receipt_proof_missing"],
  ["session ingest: explicit publication transition required", "publication_transition"],
];

describe("db_refusal: closed class of a 55000 from its message prefix", () => {
  test("every representative message is raised with ERRCODE 55000 by a migration (drift guard)", () => {
    const dir = join(import.meta.dir, "../../../supabase/migrations");
    const sql = readdirSync(dir).filter((f) => f.endsWith(".sql")).map((f) => readFileSync(join(dir, f), "utf8")).join("\n");
    for (const [message] of CASES) expect(sql).toContain(`ERRCODE='55000',MESSAGE='${message}'`);
  });

  test.each(CASES)("%s -> %s", (message, refusal) => {
    expect(dbRefusalFromMessage(message.toLowerCase())).toBe(refusal as never);
    expect(dbErrorDiagnosis(pg("55000", message))).toEqual({ db_error_class: "unknown", db_error_code: "55000", db_error_basis: "none", db_refusal: refusal as never });
  });

  test("every class but `other` is reachable; unknown, empty and absent messages are `other`", () => {
    expect(new Set([...CASES.map(([, r]) => r), "other"])).toEqual(new Set(DB_REFUSALS));
    expect(dbErrorDiagnosis(pg("55000", "frozen source: control pending")).db_refusal).toBe("other");
    expect(dbErrorDiagnosis(pg("55000", "")).db_refusal).toBe("other");
    expect(dbErrorDiagnosis(Object.assign(Object.create(null), { code: "55000" })).db_refusal).toBe("other");
    // Prefix, not substring: a known text quoted inside another message is not that class.
    expect(dbRefusalFromMessage("x session ingest: generation not ready")).toBe("other");
  });

  test("only 55000 carries db_refusal", () => {
    for (const code of ["40001", "57014", "55P03", "XX000"]) expect("db_refusal" in dbErrorDiagnosis(pg(code, "session ingest: generation not ready"))).toBe(false);
    expect("db_refusal" in dbErrorDiagnosis(new Error("session ingest: generation not ready"))).toBe(false);
  });
});

describe("the buffered generation retry reports the first non-generation refusal", () => {
  let n = 0;
  /** A buffered SQL ledger whose claim refuses at each accepted generation with the given messages. */
  async function claimRefusals(byGeneration: (generation: string, first: boolean) => string) {
    // Fresh generations per case: the module-level admitted hint is keyed by the generation set.
    n += 1;
    const G = n.toString(16).padStart(64, "a"), H = n.toString(16).padStart(64, "b");
    const seen: string[] = [];
    const ledger = sqlLedger(async (text: string, params: unknown[]) => {
      if (!text.includes("ingest_claim_buffered_batch")) throw pg("XX000", "unexpected");
      const generation = JSON.parse(String(params[0])).generation as string;
      seen.push(generation);
      throw pg("55000", byGeneration(generation, seen.length === 1));
    }, { acceptanceMode: "buffered", expectedGeneration: G, acceptGenerations: [G, H] });
    const error = await ledger.claim({ tenant: TENANT } as never).then(() => undefined, (e: unknown) => e);
    expect(seen.length).toBe(2);
    expect(error).toBeInstanceOf(LedgerUnavailableError);
    expect((error as LedgerUnavailableError).code).toBe("ledger_sqlstate_55000");
    return (error as LedgerUnavailableError).db?.db_refusal;
  }

  test("real refusal first, generation refusal last: the real one", async () => {
    expect(await claimRefusals((_, first) => first ? "session ingest: retained org admission closed or conflicting" : "session ingest: generation not ready")).toBe("org_admission_closed");
  });
  test("generation refusal first, real refusal last: the real one", async () => {
    expect(await claimRefusals((_, first) => first ? "session ingest: policy expired during operation" : "session ingest: source current row is already privacy fenced")).toBe("source_row_privacy_fenced");
  });
  test("two real refusals: the first", async () => {
    expect(await claimRefusals((_, first) => first ? "session ingest: user privacy seal missing" : "session ingest: native custody identity changed")).toBe("user_seal_missing");
  });
  test("an unknown 55000 is not a generation refusal", async () => {
    expect(await claimRefusals((_, first) => first ? "session ingest: generation not ready" : "frozen source: control held")).toBe("other");
  });
  test("both generation refusals: the last, as before", async () => {
    expect(GENERATION_REFUSALS.has("generation_not_ready")).toBe(true);
    expect(await claimRefusals((_, first) => first ? "session ingest: generation not ready" : "session ingest: current custody policy unavailable")).toBe("policy_unavailable");
  });
});

describe("client_*: closed values from the User-Agent", () => {
  test.each([
    ["agent-relay-desktop/1.4.2 macos", { client_kind: "desktop", client_version: "1.4.2", client_os: "macos" }],
    ["agent-relay-desktop/0.9.0-beta.3+build.7 linux", { client_kind: "desktop", client_version: "0.9.0-beta.3+build.7", client_os: "linux" }],
    ["agent-relay-desktop/2.0.0 Windows", { client_kind: "desktop", client_version: "2.0.0", client_os: "windows" }],
    ["agent-relay-desktop/2.0.0 darwin", { client_kind: "desktop", client_version: "2.0.0", client_os: "macos" }],
    ["agent-relay-desktop/2.0.0 freebsd", { client_kind: "desktop", client_version: "2.0.0", client_os: "other" }],
    ["agent-relay-desktop/2.0.0 macos (arm64; alice@example.com)", { client_kind: "desktop", client_version: "2.0.0", client_os: "macos" }],
    // A version off the shape (too long, or with other characters) is omitted, never truncated.
    [`agent-relay-desktop/${"1".repeat(33)} macos`, { client_kind: "desktop", client_os: "macos" }],
    ["agent-relay-desktop/1.0;rm_-rf macos", { client_kind: "desktop", client_os: "macos" }],
  ] as const)("%s", (ua, expected) => {
    expect(clientFields(ua)).toEqual(expected);
  });

  test.each([
    [undefined], [null], [""], ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"], ["curl/8.4.0"], ["agent-relay-desktop/1.0.0"],
    ["agent-relay-desktop"], [" agent-relay-desktop/1.0.0 macos"], ["AGENT-RELAY-DESKTOP/1.0.0 macos"], [`agent-relay-desktop/1.0.0 macos ${"x".repeat(300)}`],
  ] as const)("%p -> other only", (ua) => {
    expect(clientFields(ua)).toEqual({ client_kind: "other" });
  });
});

describe("the log allowlist mirrors the closed sets", () => {
  test("db_refusal, client_kind, client_os enums and the client_version shape", () => {
    expect([...DB_REFUSAL_VALUES].sort()).toEqual([...DB_REFUSALS].sort());
    expect([...CLIENT_KIND_VALUES].sort()).toEqual([...CLIENT_KINDS].sort());
    expect([...CLIENT_OS_VALUES].sort()).toEqual([...CLIENT_OSES].sort());
    expect(CLIENT_VERSION_SHAPE.source).toBe(CLIENT_VERSION.source);
    expect(sanitizeLogField("db_refusal", "ack_seal")).toBe("ack_seal");
    for (const bad of ["session ingest: generation not ready", "ACK_SEAL", "", 1, null]) expect(sanitizeLogField("db_refusal", bad)).toBeUndefined();
    expect(sanitizeLogField("client_version", "1.4.2")).toBe("1.4.2");
    for (const bad of ["1.0 macos", "x".repeat(33), "alice@example.com", "", 1]) expect(sanitizeLogField("client_version", bad)).toBeUndefined();
    expect(sanitizeLogField("client_kind", "Mozilla/5.0")).toBeUndefined();
    expect(sanitizeLogField("client_os", "darwin")).toBeUndefined();
  });
});

describe("the /v1/delivery/batches route line", () => {
  async function post(userAgent: string | undefined, message: string) {
    const s = scratchStore();
    cleanups.push(s.done);
    const ledger = sqlLedger(async () => { throw pg("55000", `${message} org=${TENANT.orgId} hunter2`); });
    const logged: Record<string, unknown>[] = [];
    const app = createIngestApp({
      deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} } } as never,
      log: (fields) => logged.push(fields),
      identify: (r) => bindingIdentity(r),
    });
    const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "r1" })]) });
    const res = await app.fetch(new Request("https://x/v1/delivery/batches", {
      method: "POST", body,
      headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] }), ...(userAgent ? { "user-agent": userAgent } : {}) },
    }));
    const route = logged.find((f) => f.route === "delivery.batches")!;
    return { status: res.status, route, line: JSON.parse(logLine("info", "session ingest", route)) as Record<string, unknown> };
  }

  test("a 55000 claim: unchanged 503; db_refusal and the desktop client survive the allowlist; no message or UA text", async () => {
    const r = await post("agent-relay-desktop/1.4.2 macos", "session ingest: retained org admission closed or conflicting");
    expect(r.status).toBe(503);
    expect(r.line).toMatchObject({
      route: "delivery.batches", outcome: "delivery_unavailable", error_code: "ledger_sqlstate_55000", db_error_code: "55000",
      db_error_class: "unknown", db_error_basis: "none", db_refusal: "org_admission_closed",
      client_kind: "desktop", client_version: "1.4.2", client_os: "macos",
    });
    const text = JSON.stringify(r.line);
    for (const needle of ["retained org admission", "hunter2", "agent-relay-desktop"]) expect(text).not.toContain(needle);
  });

  test("another User-Agent is client_kind other, and nothing of it is logged", async () => {
    const r = await post("Mozilla/5.0 alice@example.com", "session ingest: generation not ready");
    expect(r.line).toMatchObject({ db_refusal: "generation_not_ready", client_kind: "other" });
    expect("client_version" in r.line || "client_os" in r.line).toBe(false);
    expect(JSON.stringify(r.line)).not.toContain("Mozilla");
  });
});
