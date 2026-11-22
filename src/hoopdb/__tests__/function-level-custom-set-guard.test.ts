/**
 * Static guard: no CREATE/ALTER FUNCTION (or PROCEDURE/ROUTINE) under supabase/migrations, supabase/rollbacks or the
 * bootstrap files may carry a function-level SET of a bulls96.* parameter.
 *
 * Why: PostgreSQL 15+ refuses a function-level SET of a custom (placeholder) parameter to a non-superuser owner
 * (42501 "permission denied to set parameter") unless a superuser granted SET on it. A real owner is a non-superuser,
 * and this repo grants no parameter privilege, so such a migration applies only where the owner is a superuser.
 * Session-level reads and writes (current_setting, set_config, SET LOCAL inside a body) are not function-level and
 * are not refused here.
 *
 * Checked forms (comments stripped first):
 *   - every CREATE [OR REPLACE] FUNCTION|PROCEDURE header and its trailing options (the text outside its body);
 *   - every ALTER FUNCTION|PROCEDURE|ROUTINE statement, including one written inside a function or DO body (static
 *     plpgsql DDL) or inside a string for EXECUTE;
 *   - every CREATE FUNCTION|PROCEDURE header that appears inside a body or string.
 */
import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";

const ROOT = new URL("../../../", import.meta.url).pathname;
const SET_BULLS = /\bSET\s+(?:"bulls96"|bulls96)\s*\./i;

/** Strip -- line comments and block comments (outside nothing: a comment marker inside a string is also stripped, which only widens the check). */
const stripComments = (sql: string) => sql.replace(/\/\*[\s\S]*?\*\//g, " ").replace(/--[^\n]*/g, "");

/** Replace every dollar-quoted body with a placeholder, returning the outer text. */
function outerText(sql: string): string {
  let out = "", i = 0;
  const tag = /\$([A-Za-z_][A-Za-z0-9_]*)?\$/g;
  for (;;) {
    tag.lastIndex = i;
    const m = tag.exec(sql);
    if (!m) return out + sql.slice(i);
    const close = sql.indexOf(m[0], m.index + m[0].length);
    if (close < 0) return out + sql.slice(i);
    out += sql.slice(i, m.index) + " $body$ ";
    i = close + m[0].length;
  }
}

interface Finding { kind: string; text: string }
function functionLevelBullsSets(source: string): Finding[] {
  const sql = stripComments(source);
  const found: Finding[] = [];
  const snip = (s: string) => s.replace(/\s+/g, " ").trim().slice(0, 160);
  // (1) Outer statements: CREATE FUNCTION headers plus trailing options, ALTER FUNCTION statements.
  for (const stmt of outerText(sql).split(";")) {
    if (/^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)\b/i.test(stmt) && SET_BULLS.test(stmt)) found.push({ kind: "create", text: snip(stmt) });
    if (/^\s*ALTER\s+(?:FUNCTION|PROCEDURE|ROUTINE)\b/i.test(stmt) && SET_BULLS.test(stmt)) found.push({ kind: "alter", text: snip(stmt) });
  }
  // (2) Anywhere, bodies and strings included: an ALTER FUNCTION ... SET bulls96. up to the statement end, and a
  // CREATE FUNCTION header up to its AS / BEGIN ATOMIC / RETURN.
  for (const m of sql.matchAll(/\bALTER\s+(?:FUNCTION|PROCEDURE|ROUTINE)\b[^;]*/gi))
    if (SET_BULLS.test(m[0])) found.push({ kind: "alter-any", text: snip(m[0]) });
  for (const m of sql.matchAll(/\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)\b[\s\S]*?(?=\bAS\s*(?:\$|')|\bBEGIN\s+ATOMIC\b|\bRETURN\s|;|$)/gi))
    if (SET_BULLS.test(m[0])) found.push({ kind: "create-any", text: snip(m[0]) });
  return found;
}

const sqlFiles = (dir: string) => readdirSync(join(ROOT, dir)).filter((f) => f.endsWith(".sql")).sort().map((f) => join(dir, f));
const SCANNED = [...new Set([...sqlFiles("supabase/migrations"), ...sqlFiles("supabase/rollbacks"), ...sqlFiles("supabase/bootstrap"), ...SESSION_BOOTSTRAP_FILES])];

describe("no function-level SET of a bulls96.* parameter", () => {
  test("the scan covers every migration, rollback and bootstrap file", () => {
    expect(SCANNED.filter((f) => f.startsWith("supabase/migrations/")).length).toBeGreaterThan(100);
    expect(SCANNED.filter((f) => f.startsWith("supabase/rollbacks/")).length).toBeGreaterThan(10);
    for (const f of SESSION_BOOTSTRAP_FILES) expect(SCANNED).toContain(f);
    expect(SCANNED).toContain("supabase/migrations/20261105062210_lazy_ack_t2.sql");
    expect(SCANNED).toContain("supabase/rollbacks/20261105062210_lazy_ack_t2.down.sql");
  });
  test("no scanned file has one", () => {
    const hits = SCANNED.flatMap((f) => functionLevelBullsSets(readFileSync(join(ROOT, f), "utf8")).map((h) => ({ file: f, ...h })));
    expect(hits).toEqual([]);
  });
  test("the guard flags each refused form (including the unchanged r3 statements) and passes session-level uses", () => {
    const refused = [
      // r3's ACK header and gate setter, verbatim shapes.
      "CREATE OR REPLACE FUNCTION sessions.ingest_lazy_ack_batch(p jsonb) RETURNS void\nLANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp SET bulls96.lazy_ack_t2='off' AS $lazy_ack_batch$\nBEGIN END $lazy_ack_batch$;",
      "CREATE FUNCTION s.g(p text)RETURNS text LANGUAGE plpgsql AS $g$ BEGIN\n IF p='fenced' THEN ALTER FUNCTION sessions.ingest_lazy_ack_batch(jsonb) SET bulls96.lazy_ack_t2='fenced';\n END IF; RETURN p; END $g$;",
      "ALTER FUNCTION s.f(jsonb) SET bulls96.x = 'on';",
      "alter routine s.f set BULLS96.x to 'on';",
      "ALTER PROCEDURE s.p() SET \"bulls96\".x='1';",
      "CREATE FUNCTION s.f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$ SET bulls96.x='1';",
      "CREATE FUNCTION s.f() RETURNS int LANGUAGE sql SET bulls96.x FROM CURRENT BEGIN ATOMIC SELECT 1; END;",
      "DO $d$ BEGIN EXECUTE 'ALTER FUNCTION s.f() SET bulls96.x=''1'''; END $d$;",
      "DO $d$ BEGIN EXECUTE format('CREATE FUNCTION s.f() RETURNS int LANGUAGE sql SET bulls96.x=%L AS ''SELECT 1''','1'); END $d$;",
    ];
    for (const sql of refused) expect({ sql, flagged: functionLevelBullsSets(sql).length > 0 }).toEqual({ sql, flagged: true });
    const allowed = [
      "CREATE FUNCTION s.f() RETURNS text LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $f$ BEGIN RETURN current_setting('bulls96.x',true); END $f$;",
      "CREATE FUNCTION s.f() RETURNS void LANGUAGE plpgsql AS $f$ BEGIN PERFORM set_config('bulls96.x','1',true); SET LOCAL bulls96.y='2'; END $f$;",
      "ALTER FUNCTION s.f() SET search_path=pg_catalog;",
      "-- ALTER FUNCTION s.f() SET bulls96.x='1';\nSELECT 1;",
      "UPDATE sessions.lazy_ack_t2_mode SET gate='fenced' WHERE singleton;",
    ];
    for (const sql of allowed) expect({ sql, hits: functionLevelBullsSets(sql) }).toEqual({ sql, hits: [] });
  });
});
