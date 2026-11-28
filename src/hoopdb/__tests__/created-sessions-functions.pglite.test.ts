/**
 * createdSessionsFunctions against a live catalogue (PGlite). For each CREATE FUNCTION spelling, the parsed key is either
 * exactly the regprocedure key of the function that statement creates, or no catalogue key at all (under-credit, the
 * safe direction). It may never be another existing overload's key (over-credit). Overloads of the same name that
 * already exist are in the catalogue for every case. Plus: the real 062360 migration's parsed keys, after a full
 * migrate, are its live catalogue keys (the reader with its DEFAULTs included).
 */
import { afterAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { createdSessionsFunctions, pendingExecuteGrants } from "../migrate-guards.ts";
import { REPO_ROOT, applySupabaseSchema } from "../../session-ingest/__tests__/pglite-schema.ts";

setDefaultTimeout(300_000);
const open: PGlite[] = [];
afterAll(async () => { await Promise.all(open.map((d) => d.close())); });

const catalogue = async (db: PGlite) => new Set((await db.query<{ f: string }>(
  "SELECT p.oid::regprocedure::text f FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='sessions'")).rows.map((r) => r.f));

// Argument lists that PostgreSQL accepts. Every case is created next to existing overloads g(text) and g(text,integer).
const CASES = [
  "a text, b integer DEFAULT 1",
  "text, integer = 1",
  "a text DEFAULT 'x', b integer DEFAULT 1",
  "a text DEFAULT 'x,y', b integer DEFAULT 1",
  "a text DEFAULT '=', b integer DEFAULT 1",
  "a text DEFAULT 'a=b', b integer DEFAULT 1",
  "a text DEFAULT $$q,r$$, b integer DEFAULT 1",
  "a text DEFAULT E'it''s', b integer DEFAULT -1",
  "a text DEFAULT NULL::text, b integer DEFAULT 1",
  "a text = coalesce(null,'d'), b integer = 2",
  "a text DEFAULT '''', b integer DEFAULT 1",
  "a text\n DEFAULT\n NULL, b integer DEFAULT 1",
  "a text DEFAULT current_setting('x'), b integer DEFAULT 5",
  "a text DEFAULT '--', b integer DEFAULT 1",
  // hard-delete-owner's repro at 9db388ec: the balanced-paren scan closes the argument list at the quoted ')', and a
  // stripped DEFAULT then yielded "(text)", another existing overload's key.
  "a text DEFAULT ')', b integer DEFAULT 1",
  "a text DEFAULT '(', b integer DEFAULT 1",
  "a text DEFAULT $$)$$, b integer DEFAULT 1",
  // A double-quoted parameter name holding ')' also ends the balanced-paren scan early; the quotes are removed before
  // normalizing, so the truncated remainder must still name no catalogue signature.
  '"a)" text, b integer DEFAULT 1',
  '"text" text, b integer DEFAULT 1',
  // Pipeline (5a8970329576): both conditions together. The quoted name ends the scan at its ')', and the dequoted
  // prefix is itself a catalogue type ("text", "jsonb"), so a careless parser would credit g(text) or g(jsonb).
  '"text)" integer, b integer DEFAULT 1',
  '"jsonb)" integer, b integer DEFAULT 1',
  '"text," integer, b integer DEFAULT 1',
  "a text DEFAULT '/*', b integer DEFAULT 1",
  "a text DEFAULT 'x', b integer DEFAULT (1)",
  "a text, b integer DEFAULT 1, c jsonb DEFAULT '{}'::jsonb",
  "a text, c jsonb DEFAULT '{\"k\":1,\"j\":2}'::jsonb",
  "org text, workspace text, after_cursor jsonb DEFAULT NULL, page_size integer DEFAULT 512",
];

describe("createdSessionsFunctions, catalogue-backed", () => {
  test("every spelling parses to its own catalogue key or to no catalogue key: never another overload", async () => {
    const db = new PGlite();
    open.push(db);
    await db.exec("CREATE SCHEMA sessions");
    for (const [i, c] of CASES.entries()) {
      const name = `sessions.g${i}`;
      // The overloads an over-credit could name: same name, one and two arguments, created first.
      await db.exec(`CREATE FUNCTION ${name}(a text) RETURNS void LANGUAGE sql AS 'SELECT'; CREATE FUNCTION ${name}(a jsonb) RETURNS void LANGUAGE sql AS 'SELECT';`);
      const sql = `CREATE FUNCTION ${name}(${c}) RETURNS void LANGUAGE sql AS 'SELECT';`;
      const before = await catalogue(db);
      await db.exec(sql);
      const own = [...(await catalogue(db))].filter((k) => !before.has(k));
      expect([c, own.length]).toEqual([c, 1]);
      const all = await catalogue(db);
      for (const k of createdSessionsFunctions([sql])) {
        // Exactly the created function, or nothing the catalogue knows.
        expect([c, k === own[0] || !all.has(k)]).toEqual([c, true]);
      }
    }
  });

  test("GRANT targets too: a quoted parameter name in a GRANT signature credits its own key or nothing, never another overload", async () => {
    const db = new PGlite();
    open.push(db);
    await db.exec("CREATE SCHEMA sessions; CREATE ROLE grant_probe;");
    // hard-delete-owner (ae668e917048): GRANT parsing has the same quoted-name hazard as CREATE.
    const GRANTS = [
      ['k1', '"text)" integer'], ['k2', '"jsonb)" integer'], ['k3', '"text," integer, b integer'],
      ['k4', 'a text'], ['k5', 'a text, b integer'],
    ] as const;
    for (const [name, sig] of GRANTS) {
      // Overloads an over-split or truncated key could name: (text), (jsonb), and (text,integer,integer), which is what a
      // comma inside a quoted name ("text,") splits (integer,integer) into.
      await db.exec(`CREATE FUNCTION sessions.${name}(a text) RETURNS void LANGUAGE sql AS 'SELECT';
        CREATE FUNCTION sessions.${name}(a jsonb) RETURNS void LANGUAGE sql AS 'SELECT';
        CREATE FUNCTION sessions.${name}(a text, b integer, c integer) RETURNS void LANGUAGE sql AS 'SELECT';`);
      await db.exec(`CREATE FUNCTION sessions.${name}(${sig}) RETURNS void LANGUAGE sql AS 'SELECT';`).catch(() => {}); // k4: already exists
      const grant = `GRANT EXECUTE ON FUNCTION sessions.${name}(${sig}) TO grant_probe;`;
      await db.exec(grant);
      // The one function PostgreSQL resolved for that GRANT.
      const granted = (await db.query<{ f: string }>(`SELECT p.oid::regprocedure::text f FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname='sessions' AND p.proname=$1 AND has_function_privilege('grant_probe', p.oid, 'EXECUTE')
          AND EXISTS(SELECT 1 FROM aclexplode(p.proacl) x WHERE x.grantee='grant_probe'::regrole)`, [name])).rows.map((r) => r.f);
      expect([sig, granted.length]).toEqual([sig, 1]);
      const all = await catalogue(db);
      for (const g of pendingExecuteGrants([grant])) {
        const k = g.slice(0, g.lastIndexOf(" "));
        expect([sig, k === granted[0] || !all.has(k)]).toEqual([sig, true]);
      }
      // Quote-free signatures are still credited exactly.
      if (!/["'$]/u.test(sig)) expect([sig, [...pendingExecuteGrants([grant])]]).toEqual([sig, [`${granted[0]} grant_probe:EXECUTE`]]);
    }
  });

  test("the defaults without quotes are credited exactly (the case the DEFAULT strip exists for)", async () => {
    const db = new PGlite();
    open.push(db);
    await db.exec("CREATE SCHEMA sessions");
    for (const [i, c] of ["a text, b integer DEFAULT 1", "text, integer = 1", "a text DEFAULT NULL::text, b integer DEFAULT 1",
      "org text, workspace text, after_cursor jsonb DEFAULT NULL, page_size integer DEFAULT 512"].entries()) {
      const sql = `CREATE FUNCTION sessions.h${i}(${c}) RETURNS void LANGUAGE sql AS 'SELECT';`;
      await db.exec(sql);
      const own = (await db.query<{ f: string }>(`SELECT 'sessions.h${i}'::regproc::regprocedure::text f`)).rows[0]!.f;
      expect([c, [...createdSessionsFunctions([sql])]]).toEqual([c, [own]]);
    }
  });

  test("the real 062360 migration: every parsed key is a live catalogue key after a full migrate (the reader included)", async () => {
    const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
    open.push(db);
    await applySupabaseSchema(db, {});
    const live = await catalogue(db);
    const parsed = [...createdSessionsFunctions([readFileSync(join(REPO_ROOT, "supabase/migrations/20261105062360_session_erasure_replay_manifest.sql"), "utf8")])];
    expect(parsed).toContain("sessions.session_erasure_replay_read(text,text,text,text,jsonb,integer)");
    for (const k of parsed) expect([k, live.has(k)]).toEqual([k, true]);
  });
});
