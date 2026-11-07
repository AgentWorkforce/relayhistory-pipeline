/** Closed DB-client error classification for the private ingest Worker's 503 attribution.
 * Diagnostic only: status/retry/timeout/pool/connection behavior is unchanged. */
import { afterEach, describe, expect, test } from "bun:test";
import { DB_ERROR_BASES, DB_ERROR_CLASSES, DB_ERROR_CODE_FALLBACK, classifyDbError, dbClientErrorFields, dbErrorDiagnosis, dbErrorName } from "../db-error-class.ts";
import { ALLOWED_LOG_FIELDS, log, logLine } from "../../shared/log.ts";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../app.ts";
import { ledgerErrorCode, sqlLedger } from "../ledger-sql.ts";
import { LedgerUnavailableError } from "../ledger.ts";
import { DeliveryError } from "../vendor/delivery/delivery-contracts.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

const err = (fields: { code?: unknown; message?: string; name?: string }) =>
  Object.assign(new Error(fields.message ?? ""), fields.code === undefined ? {} : { code: fields.code }, fields.name ? { name: fields.name } : {});
const SECRET = "password=hunter2 postgres://alice:s3cr3t@db.internal:5432/sessions token=eyJhbGciOi user alice@example.com";
const OPAQUE = "AbCd1234tokenXYZ";
const INHERITED = ["constructor", "__proto__", "toString", "valueOf", "hasOwnProperty", "isPrototypeOf", "propertyIsEnumerable", "toLocaleString", "__defineGetter__"];
function throwingErrors(): unknown[] {
  const boom = () => { throw new Error(SECRET); };
  const getters = Object.create(null, { code: { get: boom }, message: { get: boom }, name: { get: boom } });
  const onError = Object.defineProperties(new Error(), { code: { get: boom }, message: { get: boom }, name: { get: boom } });
  const proxy = new Proxy({}, { get: boom, has: boom, getPrototypeOf: boom });
  const { proxy: revoked, revoke } = Proxy.revocable({}, {});
  revoke();
  return [getters, onError, proxy, revoked];
}

describe("classifyDbError: closed classes from codes, then message patterns (in RAM)", () => {
  const cases: [string, Parameters<typeof err>[0], string][] = [
    ["57P01 admin shutdown", { code: "57P01" }, "terminated"],
    ["08006 connection failure", { code: "08006" }, "terminated"],
    ["pg 'Connection terminated unexpectedly' (no code)", { message: "Connection terminated unexpectedly" }, "terminated"],
    ["server closed the connection", { message: "server closed the connection unexpectedly" }, "terminated"],
    ["ECONNRESET", { code: "ECONNRESET" }, "reset"],
    ["EPIPE", { code: "EPIPE" }, "reset"],
    ["workerd 'Network connection lost.'", { message: "Network connection lost." }, "reset"],
    ["socket hang up", { message: "socket hang up" }, "reset"],
    ["ETIMEDOUT", { code: "ETIMEDOUT" }, "timeout"],
    ["57014 statement timeout", { code: "57014" }, "timeout"],
    ["timeout message", { message: "Query read timeout" }, "timeout"],
    ["53300 too many connections", { code: "53300" }, "pool"],
    ["ECONNREFUSED", { code: "ECONNREFUSED" }, "pool"],
    ["remaining connection slots", { message: "remaining connection slots are reserved" }, "pool"],
    ["CERT_HAS_EXPIRED", { code: "CERT_HAS_EXPIRED" }, "tls"],
    ["ERR_TLS_CERT_ALTNAME_INVALID", { code: "ERR_TLS_CERT_ALTNAME_INVALID" }, "tls"],
    ["certificate message", { message: "self signed certificate in certificate chain" }, "tls"],
    ["no code, no message", {}, "unknown"],
    ["unmatched SQLSTATE and message", { code: "42P01", message: "relation does not exist" }, "unknown"],
  ];
  for (const [name, input, expected] of cases)
    test(name, () => expect(classifyDbError(err(input))).toBe(expected as never));

  test("non-error inputs are unknown; output is always one of the closed set", () => {
    for (const v of [undefined, null, 0, "ECONNRESET", { code: 5 }, { message: 7 }, [] as unknown])
      expect(DB_ERROR_CLASSES).toContain(classifyDbError(v));
    expect(classifyDbError(undefined)).toBe("unknown");
  });
});

describe("dbErrorName / dbClientErrorFields: only closed values leave", () => {
  test("allowlisted names pass; any other name is 'other'", () => {
    expect(dbErrorName(err({ name: "DatabaseError" }))).toBe("DatabaseError");
    expect(dbErrorName(err({ name: "TypeError" }))).toBe("TypeError");
    expect(dbErrorName(err({ name: SECRET }))).toBe("other");
    expect(dbErrorName(err({ name: "PgCustomInternalError" }))).toBe("other");
    expect(dbErrorName({})).toBe("other");
  });

  test("error_code keeps only a valid SQLSTATE or a reviewed errno/TLS code, else db_error", () => {
    expect(dbClientErrorFields(err({ code: "ECONNRESET" })).error_code).toBe("ECONNRESET");
    expect(dbClientErrorFields(err({ code: "57P01" })).error_code).toBe("57P01");
    expect(dbClientErrorFields(err({ code: "42P01" })).error_code).toBe("42P01");
    expect(dbClientErrorFields(err({ code: "CERT_HAS_EXPIRED" })).error_code).toBe("CERT_HAS_EXPIRED");
    expect(dbClientErrorFields(err({ message: "Connection terminated unexpectedly" })).error_code).toBe("db_error");
    expect(dbClientErrorFields(err({ code: SECRET })).error_code).toBe("db_error");
  });

  // Finding 1: a regex-valid token-shaped code was passed through verbatim by cd825a34.
  test("opaque token-shaped codes (valid under the old [A-Za-z0-9_]{1,64} test) are normalized, never logged", () => {
    for (const opaque of [OPAQUE, "ERR_TLS_AbCd1234tokenXYZ", "CERT_AbCd1234tokenXYZ", "E" + "ABCDEFGHTOKEN", "57p01", "123456", "ERR_SSL_x"]) {
      const f = dbClientErrorFields(err({ code: opaque }));
      expect(f.error_code).toBe(DB_ERROR_CODE_FALLBACK);
      expect(dbErrorDiagnosis(err({ code: opaque })).db_error_code).toBe(DB_ERROR_CODE_FALLBACK);
      const line = logLine("error", "session ingest db error", f);
      expect(line).not.toContain(opaque);
      expect(line).not.toContain("AbCd1234");
    }
    // An arbitrary TLS-prefixed suffix no longer earns the tls class from its code.
    expect(dbErrorDiagnosis(err({ code: "ERR_TLS_AbCd1234tokenXYZ" }))).toEqual({ db_error_class: "unknown", db_error_code: "db_error", db_error_basis: "none" });
  });

  // Finding 2: CODE_CLASS was a plain object indexed without an own-property check.
  test("inherited Object keys as codes stay inside the closed sets", () => {
    for (const key of INHERITED) {
      const d = dbErrorDiagnosis(err({ code: key }));
      expect(d).toEqual({ db_error_class: "unknown", db_error_code: "db_error", db_error_basis: "none" });
      expect(classifyDbError(err({ code: key }))).toBe("unknown");
      const f = dbClientErrorFields(err({ code: key, name: key }));
      expect(f).toEqual({ error_code: "db_error", db_error_class: "unknown", db_error_basis: "none", error_name: "other" });
      const parsed = JSON.parse(logLine("error", "session ingest db error", f)) as Record<string, unknown>;
      for (const k of ["error_code", "db_error_class", "db_error_basis", "error_name"]) expect(typeof parsed[k]).toBe("string");
    }
  });

  // Finding 3: message/name/code getters (or proxy traps) can throw.
  test("throwing getters, proxies and revoked proxies never throw; they fall back to unknown/other/db_error", () => {
    const fallback = { error_code: "db_error", db_error_class: "unknown", db_error_basis: "none", error_name: "other" };
    for (const e of throwingErrors()) {
      expect(() => dbClientErrorFields(e)).not.toThrow();
      expect(dbClientErrorFields(e)).toEqual(fallback);
      expect(classifyDbError(e)).toBe("unknown");
      expect(dbErrorName(e)).toBe("other");
      expect(() => log.error("session ingest db error", dbClientErrorFields(e))).not.toThrow();
    }
    // Only the throwing property is lost: a safe code next to a throwing message still classifies.
    const partial = Object.defineProperty(Object.assign(new Error(), { code: "ECONNRESET" }), "message", { get() { throw new Error(SECRET); } });
    expect(dbClientErrorFields(partial)).toEqual({ error_code: "ECONNRESET", db_error_class: "reset", db_error_basis: "code", error_name: "Error" });
  });

  test("statement timeout (57014) and idle-in-transaction kill (25P03) keep distinct safe codes, apart from code-less drops", () => {
    expect(dbErrorDiagnosis(err({ code: "57014", message: "canceling statement due to statement timeout" }))).toEqual({ db_error_class: "timeout", db_error_code: "57014", db_error_basis: "code" });
    expect(dbErrorDiagnosis(err({ code: "25P03", message: "terminating connection due to idle-in-transaction timeout" }))).toEqual({ db_error_class: "timeout", db_error_code: "25P03", db_error_basis: "code" });
    expect(dbErrorDiagnosis(err({ code: "57P05", message: "terminating connection due to idle-session timeout" }))).toEqual({ db_error_class: "timeout", db_error_code: "57P05", db_error_basis: "code" });
    // The NEXT query on a killed session: pg's code-less client error, not a timeout code.
    expect(dbErrorDiagnosis(err({ message: "Connection terminated unexpectedly" }))).toEqual({ db_error_class: "terminated", db_error_code: "db_error", db_error_basis: "message" });
    for (const code of ["57014", "25P03"]) {
      const f = dbClientErrorFields(err({ code }));
      expect(f).toMatchObject({ error_code: code, db_error_class: "timeout", db_error_basis: "code" });
      expect(JSON.parse(logLine("error", "session ingest db error", f))).toMatchObject({ error_code: code });
    }
  });

  test("the class travels with its safe code and basis (a heuristic, not a cause)", () => {
    expect(dbErrorDiagnosis(err({ code: "57014" }))).toEqual({ db_error_class: "timeout", db_error_code: "57014", db_error_basis: "code" });
    expect(dbErrorDiagnosis(err({ code: "ECONNREFUSED" }))).toEqual({ db_error_class: "pool", db_error_code: "ECONNREFUSED", db_error_basis: "code" });
    expect(dbErrorDiagnosis(err({ code: "ENOTFOUND", message: "socket hang up" }))).toEqual({ db_error_class: "reset", db_error_code: "ENOTFOUND", db_error_basis: "message" });
    expect(dbErrorDiagnosis(err({ message: "Connection terminated unexpectedly" }))).toEqual({ db_error_class: "terminated", db_error_code: "db_error", db_error_basis: "message" });
    for (const v of [undefined, null, 1, "x", {}]) {
      const d = dbErrorDiagnosis(v);
      expect(DB_ERROR_CLASSES).toContain(d.db_error_class);
      expect(DB_ERROR_BASES).toContain(d.db_error_basis);
    }
  });

  test("a secret-looking message, name or code never appears in the emitted log line", () => {
    for (const e of [err({ message: `Connection terminated unexpectedly ${SECRET}` }), err({ name: SECRET, message: SECRET }), err({ code: SECRET, message: SECRET })]) {
      const line = logLine("error", "session ingest db error", dbClientErrorFields(e));
      for (const needle of ["hunter2", "s3cr3t", "alice", "db.internal", "eyJhbGciOi", "postgres://"]) expect(line).not.toContain(needle);
      const parsed = JSON.parse(line) as Record<string, unknown>;
      expect(DB_ERROR_CLASSES).toContain(parsed.db_error_class as never);
      expect(["other", "Error"]).toContain(parsed.error_name as string);
    }
    expect(ALLOWED_LOG_FIELDS.has("db_error_class")).toBe(true);
    expect(ALLOWED_LOG_FIELDS.has("error_name")).toBe(true);
    expect(ALLOWED_LOG_FIELDS.has("error_message")).toBe(false);
  });
});

describe("the /v1/delivery/batches 503 path logs the same closed class with the correlation id", () => {
  const cleanups: (() => void)[] = [];
  afterEach(() => cleanups.splice(0).forEach((f) => f()));

  test("a ledger DB failure without a code: unchanged 503 envelope; log line carries stage, error_code, db_error_class, correlation_id; no message text", async () => {
    const s = scratchStore();
    cleanups.push(s.done);
    // The real SQL ledger, over a run that fails like a dropped Hyperdrive socket.
    const ledger = sqlLedger(async () => { throw err({ message: `Connection terminated unexpectedly ${SECRET}` }); });
    const logged: Record<string, unknown>[] = [];
    const app = createIngestApp({
      deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} } } as never,
      log: (fields) => logged.push(fields),
      identify: (r) => bindingIdentity(r),
    });
    const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "e1" })]) });
    const res = await app.fetch(new Request("https://x/v1/delivery/batches", {
      method: "POST", body,
      headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] }), "x-correlation-id": "corr-503" },
    }));
    // Status/envelope unchanged.
    expect(res.status).toBe(503);
    const json = (await res.json()) as { error: { code: string }; correlationId: string };
    expect(json.error.code).toBe("delivery_unavailable");
    expect(json.correlationId).toBe("corr-503");
    // The route's log line, rendered through the real allowlist/scrubber.
    const fields = logged.at(-1)!;
    expect(fields).toMatchObject({ route: "delivery.batches", outcome: "delivery_unavailable", correlation_id: "corr-503", error_code: "ledger_error", db_error_class: "terminated", db_error_code: "db_error", db_error_basis: "message" });
    expect(typeof fields.stage).toBe("string");
    const line = logLine("info", "session ingest", fields);
    expect(JSON.parse(line)).toMatchObject({ db_error_class: "terminated", correlation_id: "corr-503" });
    for (const needle of ["hunter2", "s3cr3t", "alice", "db.internal", "eyJhbGciOi", "postgres://", "Connection terminated"]) {
      expect(line).not.toContain(needle);
      expect(JSON.stringify(json)).not.toContain(needle);
    }
  });

  /** One POST through the real SQL ledger whose run throws `thrown`; the response and the route log line. */
  async function post(thrown: () => unknown, correlation: string) {
    const s = scratchStore();
    cleanups.push(s.done);
    let calls = 0;
    const ledger = sqlLedger(async () => { calls++; throw thrown(); });
    const logged: Record<string, unknown>[] = [];
    const app = createIngestApp({
      deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} } } as never,
      log: (fields) => logged.push(fields),
      identify: (r) => bindingIdentity(r),
    });
    const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "e1" })]) });
    const res = await app.fetch(new Request("https://x/v1/delivery/batches", {
      method: "POST", body,
      headers: { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] }), "x-correlation-id": correlation },
    }));
    const json = (await res.json()) as Record<string, unknown>;
    const route = logged.find((f) => f.route === "delivery.batches")!;
    return { status: res.status, retryAfter: res.headers.get("retry-after"), json, calls, route, line: logLine("info", "session ingest", route) };
  }

  test("ledger catch path: throwing code/name getters and proxies become LedgerUnavailableError, never escape", async () => {
    const boom = () => { throw new Error(`getter escaped ${SECRET}`); };
    const inputs: unknown[] = [
      Object.defineProperty({}, "code", { get: boom }),
      Object.defineProperty(new Error("x"), "name", { get: boom }),
      new Proxy({}, { get: boom }),
      (() => { const r = Proxy.revocable({}, {}); r.revoke(); return r.proxy; })(),
    ];
    for (const e of inputs) {
      expect(() => ledgerErrorCode(e)).not.toThrow();
      expect(ledgerErrorCode(e)).toBe("ledger_error");
      const ledger = sqlLedger(async () => { throw e; });
      let caught: unknown;
      try { await ledger.releaseSlot(TENANT as never, "h"); } catch (x) { caught = x; }
      expect(caught).toBeInstanceOf(LedgerUnavailableError);
      expect((caught as LedgerUnavailableError).code).toBe("ledger_error");
      expect((caught as LedgerUnavailableError).db).toEqual({ db_error_class: "unknown", db_error_code: "db_error", db_error_basis: "none" });
    }
    // The P0W01 ownership refusal still maps to 403 through the same snapshot.
    let owned: unknown;
    try { await sqlLedger(async () => { throw err({ code: "P0W01" }); }).releaseSlot(TENANT as never, "h"); } catch (x) { owned = x; }
    expect(owned).toBeInstanceOf(DeliveryError);
    expect((owned as DeliveryError).status).toBe(403);
  });

  test("route boundary: throwing code/name getters, a throwing Proxy trap and a revoked Proxy: unchanged 503/body/Retry-After/calls, closed fields", async () => {
    const base = await post(() => new Error("plain"), "corr-x");
    const boom = () => { throw new Error(`getter escaped ${SECRET}`); };
    const variants: [string, () => unknown][] = [
      ["throwing code getter", () => Object.defineProperty({}, "code", { get: boom })],
      ["throwing name getter", () => Object.defineProperty(new Error("x"), "name", { get: boom })],
      ["throwing Proxy trap", () => new Proxy({}, { get: boom })],
      ["revoked Proxy", () => { const r = Proxy.revocable({}, {}); r.revoke(); return r.proxy; }],
    ];
    for (const [name, thrown] of variants) {
      const r = await post(thrown, "corr-x");
      expect([name, r.status, r.retryAfter, r.json, r.calls]).toEqual([name, base.status, base.retryAfter, base.json, base.calls]);
      expect(r.route).toMatchObject({ outcome: "delivery_unavailable", error_code: "ledger_error", db_error_class: "unknown", db_error_code: "db_error", db_error_basis: "none" });
      for (const needle of ["getter escaped", "hunter2", "s3cr3t"]) {
        expect(r.line).not.toContain(needle);
        expect(JSON.stringify(r.json)).not.toContain(needle);
      }
    }
  });

  test("route boundary: 57014 and 25P03 reach the 503 line with distinct ledger and safe codes; response unchanged", async () => {
    const base = await post(() => new Error("plain"), "corr-x");
    const st = await post(() => err({ code: "57014", message: "canceling statement due to statement timeout" }), "corr-x");
    const idle = await post(() => err({ code: "25P03", message: "terminating connection due to idle-in-transaction timeout" }), "corr-x");
    for (const r of [st, idle]) expect([r.status, r.retryAfter, r.json, r.calls]).toEqual([base.status, base.retryAfter, base.json, base.calls]);
    expect(st.route).toMatchObject({ error_code: "ledger_timeout", db_error_class: "timeout", db_error_code: "57014", db_error_basis: "code" });
    expect(idle.route).toMatchObject({ error_code: "ledger_sqlstate_25P03", db_error_class: "timeout", db_error_code: "25P03", db_error_basis: "code" });
  });

  test("route boundary: opaque codes, inherited keys and throwing getters change only closed log fields; status, body and retry are unchanged", async () => {
    const base = await post(() => new Error("plain"), "corr-x");
    const variants: [string, () => unknown, Record<string, unknown>][] = [
      ["opaque code", () => err({ code: OPAQUE }), { db_error_class: "unknown", db_error_code: "db_error", db_error_basis: "none" }],
      ["opaque TLS-prefixed code", () => err({ code: "ERR_TLS_" + OPAQUE }), { db_error_class: "unknown", db_error_code: "db_error" }],
      ["throwing message getter + safe code", () => Object.defineProperty(Object.assign(new Error(), { code: "ECONNRESET" }), "message", { get() { throw new Error(SECRET); } }), { db_error_class: "reset", db_error_code: "ECONNRESET", db_error_basis: "code" }],
      ["throwing message getter, no code", () => Object.defineProperty(new Error(), "message", { get() { throw new Error(SECRET); } }), { db_error_class: "unknown", db_error_code: "db_error", db_error_basis: "none" }],
      ...INHERITED.map((k): [string, () => unknown, Record<string, unknown>] => [`inherited key ${k}`, () => err({ code: k }), { db_error_class: "unknown", db_error_code: "db_error", db_error_basis: "none" }]),
    ];
    for (const [name, thrown, expected] of variants) {
      const r = await post(thrown, "corr-x");
      expect([name, r.status, r.retryAfter, r.json, r.calls]).toEqual([name, base.status, base.retryAfter, base.json, base.calls]);
      expect(r.status).toBe(503);
      expect(r.route).toMatchObject({ outcome: "delivery_unavailable", ...expected });
      expect(DB_ERROR_CLASSES).toContain(r.route.db_error_class as never);
      for (const k of ["db_error_class", "db_error_code", "db_error_basis"]) expect(typeof JSON.parse(r.line)[k]).toBe("string");
      for (const needle of [OPAQUE, "AbCd1234", "hunter2", "s3cr3t"]) {
        expect(r.line).not.toContain(needle);
        expect(JSON.stringify(r.json)).not.toContain(needle);
      }
    }
  });
});
