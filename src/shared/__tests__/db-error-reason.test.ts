/**
 * `db_error_reason`: a 40001's closed reason. The map is pinned to the SQL: every `ERRCODE='40001'` RAISE MESSAGE in
 * supabase/migrations and supabase/bootstrap (regenerated here from the files) has exactly one slug, so a new 40001
 * RAISE without a slug fails this test. Synthetic values only.
 */
import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import {
  DB_ERROR_REASONS, DB_SERIALIZATION_REASON_BY_MESSAGE, PG_BUILTIN_SERIALIZATION_MESSAGES, dbErrorReasonField, dbSerializationReason,
} from "../db-error-reason.ts";
import { DB_ERROR_REASON_VALUES, errorFields, logLine, sanitizeLogField } from "../log.ts";

const ROOT = join(import.meta.dir, "../../../supabase");
const SQL = ["migrations", "bootstrap"].flatMap((d) => readdirSync(join(ROOT, d)).filter((f) => f.endsWith(".sql")).sort()
  .map((f) => readFileSync(join(ROOT, d, f), "utf8")));
/** Every 40001 RAISE's MESSAGE, from the SQL itself. */
function sqlMessages(): Set<string> {
  const out = new Set<string>();
  let errcodes = 0, matched = 0;
  for (const sql of SQL) {
    errcodes += [...sql.matchAll(/ERRCODE\s*=\s*'40001'/gi)].length;
    for (const m of sql.matchAll(/ERRCODE\s*=\s*'40001'\s*,\s*MESSAGE\s*=\s*'([^']*)'/gi)) { matched++; out.add(m[1]!); }
  }
  // Every 40001 ERRCODE is in the one form the extractor reads, so none can be missed (a new form fails here first).
  expect(matched).toBe(errcodes);
  expect(errcodes).toBeGreaterThan(0);
  return out;
}

describe("db_error_reason", () => {
  test("the map's SQL keys are exactly the 40001 RAISE messages in supabase/migrations and supabase/bootstrap", () => {
    const fromSql = sqlMessages();
    const keys = new Set([...DB_SERIALIZATION_REASON_BY_MESSAGE.keys()].filter((k) => !PG_BUILTIN_SERIALIZATION_MESSAGES.includes(k)));
    expect([...fromSql].filter((m) => !keys.has(m))).toEqual([]); // a 40001 RAISE with no slug
    expect([...keys].filter((m) => !fromSql.has(m))).toEqual([]); // a slug for a message the SQL no longer raises
    expect(fromSql.size).toBe(148);
    for (const m of PG_BUILTIN_SERIALIZATION_MESSAGES) expect(DB_SERIALIZATION_REASON_BY_MESSAGE.has(m)).toBe(true);
  });

  test("slugs are unique per message, snake_case, and the enum is exactly the slugs plus other", () => {
    const slugs = [...DB_SERIALIZATION_REASON_BY_MESSAGE.values()];
    expect(new Set(slugs).size).toBe(slugs.length);
    for (const s of slugs) expect(s).toMatch(/^[a-z][a-z0-9_]{0,63}$/);
    expect(slugs).not.toContain("other");
    expect([...DB_ERROR_REASON_VALUES].sort()).toEqual([...new Set([...slugs, "other"])].sort());
    expect([...DB_ERROR_REASONS].sort()).toEqual([...DB_ERROR_REASON_VALUES].sort());
    expect(DB_ERROR_REASONS.length).toBe(148 + 3 + 1);
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
    expect(dbErrorReasonField({ code: "40001", message: "publication: pointer mutation busy" })).toEqual({ db_error_reason: "pointer_mutation_busy" });
    expect(dbErrorReasonField({ code: "40001", message: `${MARKER} could not serialize` })).toEqual({ db_error_reason: "other" });
    expect(dbErrorReasonField({ code: "40001" })).toEqual({ db_error_reason: "other" });
    for (const e of [{ code: "55P03", message: "publication: pointer mutation busy" }, { code: "40P01" }, { code: 40001, message: "publication: pointer mutation busy" },
      new Error("publication: pointer mutation busy"), undefined, null, "40001"]) expect(dbErrorReasonField(e)).toEqual({});
    const err = Object.assign(new Error(`${MARKER} serialization`), { code: "40001" });
    const line = logLine("error", "pointer index failed", { ...errorFields(err), ...dbErrorReasonField(err) });
    expect(JSON.parse(line)).toMatchObject({ error_code: "40001", db_error_reason: "other" });
    expect(line).not.toContain(MARKER);
  });

  test("the serializer keeps only enum values", () => {
    for (const v of DB_ERROR_REASONS) expect(sanitizeLogField("db_error_reason", v)).toBe(v);
    for (const v of ["publication: pointer mutation busy", "pointer mutation busy", "POINTER_MUTATION_BUSY", "", 1, true, null, {}])
      expect(sanitizeLogField("db_error_reason", v)).toBeUndefined();
    const line = JSON.parse(logLine("error", "pointer index failed", { db_error_reason: "publication: pointer mutation busy" }));
    expect("db_error_reason" in line).toBe(false);
  });
});
