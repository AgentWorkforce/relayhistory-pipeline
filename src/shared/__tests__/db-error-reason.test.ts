/**
 * `db_error_reason`: a 40001's closed reason. The map is pinned to the SQL: every `ERRCODE='40001'` RAISE MESSAGE in
 * supabase/migrations and supabase/bootstrap (regenerated here from the files) has exactly one slug, so a new 40001
 * RAISE without a slug fails this test. Synthetic values only.
 */
import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import {
  DB_ERROR_REASONS, DB_INTERNAL_REASONS, DB_REASON_MESSAGE_MAX, DB_SERIALIZATION_REASON_BY_MESSAGE, PG_BUILTIN_SERIALIZATION_MESSAGES,
  dbErrorReasonField, dbInternalReason, dbSerializationReason,
} from "../db-error-reason.ts";
import { DB_ERROR_REASON_VALUES, DB_INTERNAL_REASON_VALUES, errorCodeFields, errorFields, logLine, sanitizeLogField } from "../log.ts";

const ROOT = join(import.meta.dir, "../../../supabase");
const SQL = ["migrations", "bootstrap"].flatMap((d) => readdirSync(join(ROOT, d)).filter((f) => f.endsWith(".sql")).sort()
  .map((f) => readFileSync(join(ROOT, d, f), "utf8")));
/**
 * Every spelling that raises SQLSTATE 40001, case-insensitive and whitespace-tolerant: ERRCODE = '40001',
 * ERRCODE = 'serialization_failure', RAISE [level] serialization_failure, RAISE [level] SQLSTATE '40001'.
 */
const FORTY001_SPELLINGS: readonly RegExp[] = [
  /ERRCODE\s*=\s*'40001'/gi,
  /ERRCODE\s*=\s*'serialization_failure'/gi,
  /\bRAISE\s+(?:EXCEPTION\s+)?serialization_failure\b/gi,
  /\bRAISE\s+(?:EXCEPTION\s+)?SQLSTATE\s+'40001'/gi,
];
/** The one form the map is keyed by. */
const CANONICAL = /ERRCODE\s*=\s*'40001'\s*,\s*MESSAGE\s*=\s*'([^']*)'/gi;

/** A file's 40001 raises: how many (any spelling) and the messages of those in the canonical form. */
function scan40001(sql: string): { raises: number; messages: string[] } {
  const raises = FORTY001_SPELLINGS.reduce((n, re) => n + [...sql.matchAll(re)].length, 0);
  return { raises, messages: [...sql.matchAll(CANONICAL)].map((m) => m[1]!) };
}
/** The pin's guarantee: every 40001 raise is in the canonical form (so its message is extracted and needs a slug). */
function pinHolds(sql: string): boolean {
  const { raises, messages } = scan40001(sql);
  return raises === messages.length;
}

/** Every 40001 RAISE's MESSAGE, from the SQL itself. */
function sqlMessages(): Set<string> {
  const out = new Set<string>();
  let raises = 0, matched = 0;
  for (const sql of SQL) {
    const r = scan40001(sql);
    raises += r.raises; matched += r.messages.length;
    for (const m of r.messages) out.add(m);
  }
  // Every 40001 raise, in any spelling, is in the one form the extractor reads, so none can evade the slug check.
  expect(matched).toBe(raises);
  expect(raises).toBe(332);
  return out;
}

describe("db_error_reason", () => {
  test("the map's SQL keys are exactly the 40001 RAISE messages in supabase/migrations and supabase/bootstrap", () => {
    const fromSql = sqlMessages();
    const keys = new Set([...DB_SERIALIZATION_REASON_BY_MESSAGE.keys()].filter((k) => !PG_BUILTIN_SERIALIZATION_MESSAGES.includes(k)));
    expect([...fromSql].filter((m) => !keys.has(m))).toEqual([]); // a 40001 RAISE with no slug
    expect([...keys].filter((m) => !fromSql.has(m))).toEqual([]); // a slug for a message the SQL no longer raises
    expect(fromSql.size).toBe(150);
    for (const m of PG_BUILTIN_SERIALIZATION_MESSAGES) expect(DB_SERIALIZATION_REASON_BY_MESSAGE.has(m)).toBe(true);
  });

  test("the pin fails for each evading 40001 spelling (case- and whitespace-tolerant), and holds for the canonical form", () => {
    expect(pinHolds("RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: control version gap';")).toBe(true);
    expect(pinHolds("raise exception using errcode = '40001' ,\n  message = 'x: y';")).toBe(true);
    for (const evading of [
      "RAISE EXCEPTION USING ERRCODE='serialization_failure',MESSAGE='x: new';",
      "raise exception using errcode  =\n 'SERIALIZATION_FAILURE', message = 'x: new';",
      "RAISE serialization_failure USING MESSAGE='x: new';",
      "raise   exception\n serialization_failure using message = 'x: new';",
      "RAISE SQLSTATE '40001' USING MESSAGE='x: new';",
      "raise exception sqlstate\t'40001' using message = 'x: new';",
      "RAISE EXCEPTION 'x: new' USING ERRCODE='40001';",
      "RAISE EXCEPTION USING MESSAGE='x: new',ERRCODE='40001';",
    ]) expect(pinHolds(evading)).toBe(false);
    // Not a raise: a comment or a CHECK list naming 40001 is not counted.
    expect(scan40001("-- 40001 propagates\nCHECK(reason IN('40001','55P03'))").raises).toBe(0);
  });

  test("slugs are unique per message, snake_case, and the enum is exactly the slugs plus other", () => {
    const slugs = [...DB_SERIALIZATION_REASON_BY_MESSAGE.values()];
    expect(new Set(slugs).size).toBe(slugs.length);
    for (const s of slugs) expect(s).toMatch(/^[a-z][a-z0-9_]{0,63}$/);
    expect(slugs).not.toContain("other");
    expect([...DB_ERROR_REASON_VALUES].sort()).toEqual([...new Set([...slugs, "other"])].sort());
    expect([...DB_ERROR_REASONS].sort()).toEqual([...DB_ERROR_REASON_VALUES].sort());
    expect(DB_ERROR_REASONS.length).toBe(150 + 3 + 1);
  });

  test("each known message maps to its slug, exactly", () => {
    expect(dbSerializationReason("publication: pointer mutation busy")).toBe("pointer_mutation_busy");
    expect(dbSerializationReason("publication: pointer mutation requires fresh snapshots")).toBe("pointer_mutation_requires_fresh_snapshots");
    expect(dbSerializationReason("session ingest: claim generation changed")).toBe("claim_generation_changed");
    expect(dbSerializationReason("session ingest: unknown claim attempt")).toBe("unknown_claim_attempt");
    expect(dbSerializationReason("session ingest: session outside claim attempt")).toBe("session_outside_claim_attempt");
    expect(dbSerializationReason("babysitter retention: session outside claim attempt")).toBe("babysitter_session_outside_claim_attempt");
    expect(dbSerializationReason("session ingest: buffered ordinal coverage changed")).toBe("buffered_ordinal_coverage_changed");
    expect(dbSerializationReason("buffer gc: identity changed")).toBe("identity_changed");
    expect(dbSerializationReason("could not serialize access due to concurrent update")).toBe("pg_concurrent_update");
    expect(dbSerializationReason("could not serialize access due to read/write dependencies among transactions")).toBe("pg_rw_dependency");
    for (const [m, s] of DB_SERIALIZATION_REASON_BY_MESSAGE) expect(dbSerializationReason(m)).toBe(s);
  });

  test("unknown, near-miss or absent messages are other", () => {
    for (const m of ["", "pointer mutation busy", "publication: pointer mutation busy ", "PUBLICATION: POINTER MUTATION BUSY",
      "publication: pointer mutation busy; PRIVATE-MARKER", "something else", undefined, null, 40001, {}]) expect(dbSerializationReason(m)).toBe("other");
  });

  test("only a 40001 carries the field; never the raw message", () => {
    const MARKER = "PRIVATE-MARKER-session_secret";
    const f = (e: unknown) => dbErrorReasonField(e, errorCodeFields(e).code);
    expect(f({ code: "40001", message: "publication: pointer mutation busy" })).toEqual({ db_error_reason: "pointer_mutation_busy" });
    expect(f({ code: "40001", message: `${MARKER} could not serialize` })).toEqual({ db_error_reason: "other" });
    expect(f({ code: "40001" })).toEqual({ db_error_reason: "other" });
    for (const e of [{ code: "55P03", message: "publication: pointer mutation busy" }, { code: "40P01" }, { code: 40001, message: "publication: pointer mutation busy" },
      new Error("publication: pointer mutation busy"), undefined, null, "40001",
      Object.assign(new Error("publication: pointer mutation busy"), { name: "40001" }), Object.assign(new Error("x"), { name: "XX000" })]) expect(f(e)).toEqual({});
    // errorFields' display/retry fallback to the name is unchanged; only the diagnostic code ignores it.
    expect(errorFields(Object.assign(new Error("x"), { name: "40001" }))).toEqual({ error_code: "40001" });
    expect(errorCodeFields(Object.assign(new Error("x"), { name: "40001" }))).toEqual({ fields: { error_code: "40001" }, code: undefined });
    expect(errorCodeFields({ code: "40001", retryable: true })).toEqual({ fields: { error_code: "40001", retryable: true }, code: "40001" });
    // The caller's captured code decides, not a second read of `code`.
    expect(dbErrorReasonField({ message: "publication: pointer mutation busy" }, "40001")).toEqual({ db_error_reason: "pointer_mutation_busy" });
    expect(dbErrorReasonField({ code: "40001", message: "publication: pointer mutation busy" }, "55P03")).toEqual({});
    const err = Object.assign(new Error(`${MARKER} serialization`), { code: "40001" });
    const line = logLine("error", "pointer index failed", { ...errorFields(err), ...f(err) });
    expect(JSON.parse(line)).toMatchObject({ error_code: "40001", db_error_reason: "other" });
    expect(line).not.toContain(MARKER);
  });

  test("an unreadable, non-string or over-long message is other and extraction never throws", () => {
    const throwing = { code: "40001", get message(): string { throw new Error("PRIVATE-MARKER getter"); } };
    for (const code of ["40001", "XX000"]) {
      const field = code === "40001" ? "db_error_reason" : "db_internal_reason";
      expect(dbErrorReasonField({ ...{ code }, get message(): string { throw new Error("boom"); } }, code)).toEqual({ [field]: "other" });
      expect(dbErrorReasonField(throwing, code)).toEqual({ [field]: "other" });
      expect(dbErrorReasonField({ code, message: 42 }, code)).toEqual({ [field]: "other" });
      expect(dbErrorReasonField({ code, message: { toString: () => "publication: pointer mutation busy" } }, code)).toEqual({ [field]: "other" });
      expect(dbErrorReasonField({ code, message: "cache lookup failed for type " + "9".repeat(DB_REASON_MESSAGE_MAX) }, code)).toEqual({ [field]: "other" });
      const proxy = new Proxy({}, { get() { throw new Error("trap"); }, has() { throw new Error("trap"); } });
      expect(dbErrorReasonField(proxy, code)).toEqual({ [field]: "other" });
      expect(dbErrorReasonField(undefined, code)).toEqual({ [field]: "other" });
    }
  });

  test("XX000: each internal message family maps to its slug; unknown is other; no raw text", () => {
    const CASES: [string, string][] = [
      ["cache lookup failed for relation 123456", "cache_lookup_failed"],
      ["cache lookup failed for type 99", "cache_lookup_failed"],
      ["cached plan must not change result type", "cached_plan_result_type"],
      ["could not open relation with OID 4242", "could_not_open_relation"],
      ["could not read block 7 in file \"base/1/2\": read only 0 of 8192 bytes", "could_not_read_block"],
      ["could not find trigger 777", "could_not_find"],
      ["could not find tuple for constraint 12", "could_not_find"],
      ["tuple concurrently updated", "tuple_concurrently_modified"],
      ["tuple concurrently deleted", "tuple_concurrently_modified"],
      ["unrecognized node type: 321", "unrecognized_node_type"],
    ];
    for (const [m, slug] of CASES) {
      expect(dbInternalReason(m)).toBe(slug);
      expect(dbErrorReasonField({ code: "XX000", message: m }, "XX000")).toEqual({ db_internal_reason: slug });
    }
    expect(new Set([...CASES.map(([, s]) => s), "other"])).toEqual(new Set(DB_INTERNAL_REASONS));
    for (const m of ["", "Cache lookup failed for x", " cache lookup failed for x", "cached plan must not change result type!", "PRIVATE-MARKER internal",
      "publication: pointer mutation busy", undefined, null, 7]) expect(dbInternalReason(m)).toBe("other");
    // XX000 never carries the 40001 field and vice versa.
    expect(dbErrorReasonField({ message: "publication: pointer mutation busy" }, "XX000")).toEqual({ db_internal_reason: "other" });
    expect(dbErrorReasonField({ message: "cache lookup failed for type 9" }, "40001")).toEqual({ db_error_reason: "other" });
    const MARKER = "PRIVATE-MARKER-relation-secret_table";
    const err = Object.assign(new Error(`could not open relation ${MARKER}`), { code: "XX000" });
    const line = logLine("error", "pointer index failed", { ...errorFields(err), ...dbErrorReasonField(err, "XX000") });
    expect(JSON.parse(line)).toMatchObject({ error_code: "XX000", db_internal_reason: "could_not_open_relation" });
    expect(line).not.toContain(MARKER);
    expect([...DB_INTERNAL_REASON_VALUES].sort()).toEqual([...DB_INTERNAL_REASONS].sort());
    for (const v of DB_INTERNAL_REASONS) expect(sanitizeLogField("db_internal_reason", v)).toBe(v);
    for (const v of ["cache lookup failed for type 9", "pointer_mutation_busy", "", 1, null]) expect(sanitizeLogField("db_internal_reason", v)).toBeUndefined();
  });

  test("the serializer keeps only enum values", () => {
    for (const v of DB_ERROR_REASONS) expect(sanitizeLogField("db_error_reason", v)).toBe(v);
    for (const v of ["publication: pointer mutation busy", "pointer mutation busy", "POINTER_MUTATION_BUSY", "", 1, true, null, {}])
      expect(sanitizeLogField("db_error_reason", v)).toBeUndefined();
    const line = JSON.parse(logLine("error", "pointer index failed", { db_error_reason: "publication: pointer mutation busy" }));
    expect("db_error_reason" in line).toBe(false);
  });
});
