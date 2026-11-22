/**
 * Static guard: no CREATE/ALTER FUNCTION (or PROCEDURE/ROUTINE) in any shipped SQL may carry a function-level SET of
 * a CUSTOM parameter (a dotted name, `SET <ident>.<ident>`, quoted or not; bulls96.* is one such prefix).
 *
 * Why: PostgreSQL 15+ refuses a function-level SET of a custom (placeholder) parameter to a non-superuser owner
 * (42501 "permission denied to set parameter") unless a superuser granted SET on it. A real owner is a non-superuser,
 * and this repo grants no parameter privilege, so such a migration applies only where the owner is a superuser.
 * Session-level reads and writes (current_setting, set_config, SET LOCAL inside a body) are not function-level and
 * are not refused; built-in parameters (SET search_path=...) have no dot and are not refused.
 *
 * Scanned, recursively: supabase/ (migrations, rollbacks, bootstrap incl. rds_activation, history-migrations,
 * pending, operations, tests) and src/hoopdb/migrations, plus every SESSION_BOOTSTRAP_FILES entry.
 *
 * Method (comments stripped first): split the text into statements outside every dollar-quoted body and single-quoted
 * string; in each statement, from any CREATE [OR REPLACE] FUNCTION|PROCEDURE or ALTER FUNCTION|PROCEDURE|ROUTINE on,
 * a SET of a dotted name is a finding (this covers a header, the trailing options after a dollar- or single-quoted
 * body, and an ALTER). Then the same scan runs recursively inside every dollar-quoted body and every single-quoted
 * string (with '' unescaped), so static DDL in a plpgsql or DO body and DDL in an EXECUTE string (including a nested
 * dollar-quoted body inside it) are covered. Accepted miss: DDL text assembled by concatenation or format() pieces
 * at run time (for example 'SET ' || name) is not visible to a static scan.
 */
import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";

const ROOT = new URL("../../../", import.meta.url).pathname;
const IDENT = String.raw`(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)`;
const SET_CUSTOM = new RegExp(String.raw`\bSET\s+${IDENT}\s*\.\s*${IDENT}`, "i");
const DDL = /\b(?:CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)|ALTER\s+(?:FUNCTION|PROCEDURE|ROUTINE))\b/i;

/** Strip -- line comments and block comments (a marker inside a string is stripped too, which only widens the check). */
const stripComments = (sql: string) => sql.replace(/\/\*[\s\S]*?\*\//g, " ").replace(/--[^\n]*/g, "");

/** Split into the outer text (bodies and strings replaced by placeholders) and the inner texts to scan recursively. */
function split(sql: string): { outer: string; inner: string[] } {
  let outer = "", i = 0;
  const inner: string[] = [];
  const open = /\$([A-Za-z_][A-Za-z0-9_]*)?\$|'/g;
  for (;;) {
    open.lastIndex = i;
    const m = open.exec(sql);
    if (!m) return { outer: outer + sql.slice(i), inner };
    if (m[0] === "'") {
      let j = m.index + 1, body = "";
      for (; j < sql.length; j++) {
        if (sql[j] === "'") { if (sql[j + 1] === "'") { body += "'"; j++; continue; } break; }
        body += sql[j];
      }
      outer += sql.slice(i, m.index) + " 'str' ";
      inner.push(body);
      i = j + 1;
    } else {
      const close = sql.indexOf(m[0], m.index + m[0].length);
      if (close < 0) return { outer: outer + sql.slice(i), inner };
      outer += sql.slice(i, m.index) + " $body$ ";
      inner.push(sql.slice(m.index + m[0].length, close));
      i = close + m[0].length;
    }
  }
}

interface Finding { text: string }
function functionLevelCustomSets(source: string, depth = 0): Finding[] {
  if (depth > 8) return [];
  const { outer, inner } = split(depth === 0 ? stripComments(source) : source);
  const found: Finding[] = [];
  for (const stmt of outer.split(";")) {
    const at = stmt.search(DDL);
    if (at >= 0 && SET_CUSTOM.test(stmt.slice(at))) found.push({ text: stmt.slice(at).replace(/\s+/g, " ").trim().slice(0, 160) });
  }
  for (const s of inner) found.push(...functionLevelCustomSets(s, depth + 1));
  return found;
}

const walk = (dir: string): string[] => readdirSync(join(ROOT, dir)).sort().flatMap((f) => {
  const p = join(dir, f);
  return statSync(join(ROOT, p)).isDirectory() ? walk(p) : f.endsWith(".sql") ? [p] : [];
});
const SCANNED = [...new Set([...walk("supabase"), ...walk("src/hoopdb/migrations"), ...SESSION_BOOTSTRAP_FILES])];

describe("no function-level SET of a custom parameter", () => {
  test("the scan covers every shipped SQL directory, recursively", () => {
    const count = (prefix: string) => SCANNED.filter((f) => f.startsWith(prefix)).length;
    expect(count("supabase/migrations/")).toBeGreaterThan(100);
    expect(count("supabase/rollbacks/")).toBeGreaterThan(10);
    for (const d of ["supabase/bootstrap/rds_activation/", "supabase/history-migrations/", "supabase/pending/", "src/hoopdb/migrations/"])
      expect({ d, n: count(d) > 0 }).toEqual({ d, n: true });
    for (const f of SESSION_BOOTSTRAP_FILES) expect(SCANNED).toContain(f);
    expect(SCANNED).toContain("supabase/migrations/20261105062210_lazy_ack_t2.sql");
    expect(SCANNED).toContain("supabase/rollbacks/20261105062210_lazy_ack_t2.down.sql");
  });
  test("no scanned file has one", () => {
    const hits = SCANNED.flatMap((f) => functionLevelCustomSets(readFileSync(join(ROOT, f), "utf8")).map((h) => ({ file: f, ...h })));
    expect(hits).toEqual([]);
  });
  test("the guard flags each refused form (including the unchanged r3 statements) and passes the close allowed forms", () => {
    const refused = [
      // r3's ACK header and gate setter, verbatim shapes.
      "CREATE OR REPLACE FUNCTION sessions.ingest_lazy_ack_batch(p jsonb) RETURNS void\nLANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp SET bulls96.lazy_ack_t2='off' AS $lazy_ack_batch$\nBEGIN END $lazy_ack_batch$;",
      "CREATE FUNCTION s.g(p text)RETURNS text LANGUAGE plpgsql AS $g$ BEGIN\n IF p='fenced' THEN ALTER FUNCTION sessions.ingest_lazy_ack_batch(jsonb) SET bulls96.lazy_ack_t2='fenced';\n END IF; RETURN p; END $g$;",
      // Any custom prefix, quoted or not.
      "ALTER FUNCTION s.f(jsonb) SET app.x = 'on';",
      "alter routine s.f set BULLS96.x to 'on';",
      "ALTER PROCEDURE s.p() SET \"my app\".\"x\"='1';",
      "CREATE FUNCTION s.f() RETURNS int LANGUAGE sql SET other.knob FROM CURRENT BEGIN ATOMIC SELECT 1; END;",
      // Trailing options after a dollar-quoted body.
      "CREATE FUNCTION s.f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$ SET app.x='1';",
      // Trailing options after a single-quoted body that contains ';'.
      "CREATE FUNCTION s.f() RETURNS int LANGUAGE plpgsql AS 'BEGIN PERFORM 1; RETURN 1; END' SET app.x='1';",
      // DDL inside EXECUTE strings, including a nested dollar-quoted body with trailing options.
      "DO $d$ BEGIN EXECUTE 'ALTER FUNCTION s.f() SET bulls96.x=''1'''; END $d$;",
      "DO $d$ BEGIN EXECUTE format('CREATE FUNCTION s.f() RETURNS int LANGUAGE sql SET app.x=%L AS ''SELECT 1''','1'); END $d$;",
      "DO $x$ BEGIN EXECUTE $q$CREATE FUNCTION s.f() RETURNS int LANGUAGE sql AS $y$ SELECT 1; $y$ SET app.x='1'$q$; END $x$;",
    ];
    for (const sql of refused) expect({ sql, flagged: functionLevelCustomSets(sql).length > 0 }).toEqual({ sql, flagged: true });
    const allowed = [
      "CREATE FUNCTION s.f() RETURNS text LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $f$ BEGIN RETURN current_setting('app.x',true); END $f$;",
      "CREATE FUNCTION s.f() RETURNS void LANGUAGE plpgsql AS $f$ BEGIN PERFORM set_config('app.x','1',true); SET LOCAL app.y='2'; END $f$;",
      "CREATE FUNCTION s.f() RETURNS void LANGUAGE plpgsql AS 'BEGIN PERFORM set_config(''app.x'',''1'',true); END' SET search_path=pg_catalog;",
      "DO $x$ BEGIN EXECUTE $q$CREATE FUNCTION s.f() RETURNS int LANGUAGE sql AS $y$ SELECT 1 $y$ SET search_path=pg_catalog$q$; END $x$;",
      "ALTER FUNCTION s.f() SET search_path=pg_catalog;",
      "ALTER FUNCTION s.f() OWNER TO app_owner; SET LOCAL app.x='1';",
      "-- ALTER FUNCTION s.f() SET bulls96.x='1';\nSELECT 1;",
      "UPDATE sessions.lazy_ack_t2_mode SET gate='fenced' WHERE singleton;",
      "UPDATE s.t AS a SET x=1 FROM s.u b WHERE a.id=b.id;",
      // Accepted miss (documented above): assembled by concatenation at run time.
      "DO $d$ BEGIN EXECUTE 'ALTER FUNCTION s.f() SET ' || 'app' || '.x=1'; END $d$;",
    ];
    for (const sql of allowed) expect({ sql, hits: functionLevelCustomSets(sql) }).toEqual({ sql, hits: [] });
  });
});
