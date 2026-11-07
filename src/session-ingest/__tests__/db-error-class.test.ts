/** Closed DB-client error classification for the private ingest Worker's 503 attribution.
 * Diagnostic only: status/retry/timeout/pool/connection behavior is unchanged. */
import { afterEach, describe, expect, test } from "bun:test";
import { DB_ERROR_CLASSES, classifyDbError, dbClientErrorFields, dbErrorName } from "../db-error-class.ts";
import { ALLOWED_LOG_FIELDS, logLine } from "../../shared/log.ts";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../app.ts";
import { sqlLedger } from "../ledger-sql.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

const err = (fields: { code?: unknown; message?: string; name?: string }) =>
  Object.assign(new Error(fields.message ?? ""), fields.code === undefined ? {} : { code: fields.code }, fields.name ? { name: fields.name } : {});
const SECRET = "password=hunter2 postgres://alice:s3cr3t@db.internal:5432/sessions token=eyJhbGciOi user alice@example.com";

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

  test("error_code keeps the client's code, else db_error; a non-token code is db_error", () => {
    expect(dbClientErrorFields(err({ code: "ECONNRESET" })).error_code).toBe("ECONNRESET");
    expect(dbClientErrorFields(err({ message: "Connection terminated unexpectedly" })).error_code).toBe("db_error");
    expect(dbClientErrorFields(err({ code: SECRET })).error_code).toBe("db_error");
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
    expect(fields).toMatchObject({ route: "delivery.batches", outcome: "delivery_unavailable", correlation_id: "corr-503", error_code: "ledger_error", db_error_class: "terminated" });
    expect(typeof fields.stage).toBe("string");
    const line = logLine("info", "session ingest", fields);
    expect(JSON.parse(line)).toMatchObject({ db_error_class: "terminated", correlation_id: "corr-503" });
    for (const needle of ["hunter2", "s3cr3t", "alice", "db.internal", "eyJhbGciOi", "postgres://", "Connection terminated"]) {
      expect(line).not.toContain(needle);
      expect(JSON.stringify(json)).not.toContain(needle);
    }
  });
});
