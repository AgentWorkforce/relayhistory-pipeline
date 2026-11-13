/**
 * Server-owned upload consent (20261105057010_upload_consent.sql) in PGlite with every Supabase
 * migration and the role bootstrap, called through `sqlUploadConsentStore` AS bulls96_session_ingest
 * (SET ROLE), as production logs in. Holds are inserted as the owner, the way an operator does.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { sqlUploadConsentStore } from "../upload-consent-sql.ts";
import { ConsentWriteRefused, sessionKey, type UploadConsentStore } from "../upload-consent.ts";
import { migrationFiles } from "./pglite-schema.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
/** psql meta-commands (\set) are not SQL. */
function sql(path: string): string {
  return read(path).split("\n").map((line) => (line.startsWith("\\ir ") ? sql(join(dirname(path), line.slice(4).trim())) : line.startsWith("\\") ? "" : line)).join("\n");
}
const ROLE = sql("supabase/bootstrap/session_ingest_role.sql");
setDefaultTimeout(120000);

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

async function database(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const file of migrationFiles(root)) await db.exec(read(`supabase/migrations/${file}`));
  await db.exec(ROLE);
  return db;
}

/** Each call runs as `role`, the way one pooled login would. */
function runAs(db: PGlite, role = "bulls96_session_ingest") {
  return async (text: string, params: unknown[]) => {
    await db.exec(`SET ROLE ${role}`);
    try {
      return (await db.query(text, params)) as { rows: Record<string, unknown>[] };
    } finally {
      await db.exec("RESET ROLE");
    }
  };
}

const T1 = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const T2 = { orgId: "org-a", workspaceId: "ws-a", userId: "user-2" };
const S1 = { source: "claude", sessionId: "s-1" };
const S2 = { source: "codex", sessionId: "s-2" };
const NOW = 1_800_000_000_000;

async function fixture() {
  const db = await database();
  return { db, store: sqlUploadConsentStore(runAs(db)) };
}
const refusal = async (p: Promise<unknown>) => {
  try { await p; } catch (e) { if (e instanceof ConsentWriteRefused) return e.code; throw e; }
  return null;
};
const policy = async (store: UploadConsentStore, t = T1) => (await store.snapshot(t, "origin-1", [], NOW)).policy;

describe("upload consent SQL as bulls96_session_ingest", () => {
  test("no policy: snapshot answers policy null, not held, origin unconfirmed, and records first sight", async () => {
    const { store } = await fixture();
    const snap = await store.snapshot(T1, "origin-1", [{ ...S1, firstActivityMs: NOW - 5000 }], NOW);
    expect(snap.policy).toBeNull();
    expect(snap.held).toBe(false);
    expect(snap.originConfirmed).toBe(false);
    expect(snap.sessions.get(sessionKey(S1.source, S1.sessionId))).toEqual({ firstSeenMs: NOW, firstActivityMs: NOW - 5000 });
  });

  test("untrusted callers can only narrow; broadening is refused and changes nothing", async () => {
    const { store } = await fixture();
    expect(await refusal(store.apply(T1, { mode: "all" }, { trusted: false, nowMs: NOW }))).toBe("upload_consent_untrusted");
    expect(await policy(store)).toBeNull();
    expect(await store.apply(T1, { mode: "all", confirmOrigins: ["origin-1"] }, { trusted: true, nowMs: NOW })).toEqual({ revision: 1 });
    // Each broadening form is refused untrusted, and the revision stays put.
    for (const write of [{ allow: [S1] }, { confirmOrigins: ["origin-2"] }, { releaseHold: true }])
      expect(await refusal(store.apply(T1, write, { trusted: false, nowMs: NOW }))).toBe("upload_consent_untrusted");
    expect(await policy(store)).toEqual({ mode: "all", revision: 1, newSessionBoundaryMs: null });
    expect((await store.snapshot(T1, "origin-2", [], NOW)).originConfirmed).toBe(false);
    // Narrowing works untrusted: all -> selected, and a deny.
    expect(await store.apply(T1, { mode: "selected", deny: [S2] }, { trusted: false, nowMs: NOW })).toEqual({ revision: 2 });
    expect(await policy(store)).toEqual({ mode: "selected", revision: 2, newSessionBoundaryMs: null });
    expect((await store.snapshot(T1, "origin-1", [S2], NOW)).sessions.get(sessionKey(S2.source, S2.sessionId))?.decision).toBe("deny");
    // selected -> new is a broadening again.
    expect(await refusal(store.apply(T1, { mode: "new" }, { trusted: false, nowMs: NOW }))).toBe("upload_consent_untrusted");
    expect((await policy(store))?.revision).toBe(2);
  });

  test("trusted all with confirm_origins; a deny survives a change to all", async () => {
    const { store } = await fixture();
    await store.apply(T1, { mode: "selected", deny: [S1], allow: [S2] }, { trusted: true, nowMs: NOW });
    await store.apply(T1, { mode: "all", confirmOrigins: ["origin-1"] }, { trusted: true, nowMs: NOW });
    const snap = await store.snapshot(T1, "origin-1", [S1, S2], NOW);
    expect(snap.policy).toEqual({ mode: "all", revision: 2, newSessionBoundaryMs: null });
    expect(snap.originConfirmed).toBe(true);
    expect(snap.sessions.get(sessionKey(S1.source, S1.sessionId))?.decision).toBe("deny");
    expect(snap.sessions.get(sessionKey(S2.source, S2.sessionId))?.decision).toBe("allow");
    expect((await store.snapshot(T1, "origin-other", [], NOW)).originConfirmed).toBe(false);
  });

  test("new mode sets its boundary on transition and keeps it while already new", async () => {
    const { store } = await fixture();
    await store.apply(T1, { mode: "new" }, { trusted: true, nowMs: NOW });
    expect(await policy(store)).toEqual({ mode: "new", revision: 1, newSessionBoundaryMs: NOW });
    await store.apply(T1, { mode: "new", allow: [S1] }, { trusted: true, nowMs: NOW + 60_000 });
    expect(await policy(store)).toEqual({ mode: "new", revision: 2, newSessionBoundaryMs: NOW });
    await store.apply(T1, { deny: [S2] }, { trusted: false, nowMs: NOW + 120_000 });
    expect(await policy(store)).toEqual({ mode: "new", revision: 3, newSessionBoundaryMs: NOW });
    // Leaving new clears it; returning sets a fresh one.
    await store.apply(T1, { mode: "selected" }, { trusted: false, nowMs: NOW + 180_000 });
    expect(await policy(store)).toEqual({ mode: "selected", revision: 4, newSessionBoundaryMs: null });
    await store.apply(T1, { mode: "new" }, { trusted: true, nowMs: NOW + 240_000 });
    expect(await policy(store)).toEqual({ mode: "new", revision: 5, newSessionBoundaryMs: NOW + 240_000 });
  });

  test("first_seen keeps the earliest server time; first_activity keeps the minimum claim", async () => {
    const { store } = await fixture();
    const key = sessionKey(S1.source, S1.sessionId);
    await store.snapshot(T1, "origin-1", [S1], NOW);
    expect((await store.snapshot(T1, "origin-1", [{ ...S1, firstActivityMs: NOW - 100 }], NOW + 10)).sessions.get(key)).toEqual({ firstSeenMs: NOW, firstActivityMs: NOW - 100 });
    expect((await store.snapshot(T1, "origin-1", [{ ...S1, firstActivityMs: NOW + 500 }], NOW + 20)).sessions.get(key)).toEqual({ firstSeenMs: NOW, firstActivityMs: NOW - 100 });
    expect((await store.snapshot(T1, "origin-1", [{ ...S1, firstActivityMs: NOW - 900 }], NOW + 30)).sessions.get(key)).toEqual({ firstSeenMs: NOW, firstActivityMs: NOW - 900 });
    // An absent claim keeps the recorded one; duplicates in one batch keep their minimum.
    expect((await store.snapshot(T1, "origin-1", [S1], NOW + 40)).sessions.get(key)).toEqual({ firstSeenMs: NOW, firstActivityMs: NOW - 900 });
    const s2 = sessionKey(S2.source, S2.sessionId);
    expect((await store.snapshot(T1, "origin-1", [{ ...S2, firstActivityMs: NOW - 5 }, { ...S2, firstActivityMs: NOW - 50 }, S2], NOW + 50)).sessions.get(s2)).toEqual({ firstSeenMs: NOW + 50, firstActivityMs: NOW - 50 });
    // A decision written before first sight keeps its own first_seen (the apply time).
    await store.apply(T1, { deny: [{ source: "amp", sessionId: "s-3" }] }, { trusted: false, nowMs: NOW + 60 });
    expect((await store.snapshot(T1, "origin-1", [{ source: "amp", sessionId: "s-3" }], NOW + 70)).sessions.get(sessionKey("amp", "s-3"))).toEqual({ decision: "deny", firstSeenMs: NOW + 60 });
  });

  test("expected_revision mismatch refuses without writing; a match applies", async () => {
    const { store } = await fixture();
    expect(await refusal(store.apply(T1, { mode: "selected", expectedRevision: 1 }, { trusted: true, nowMs: NOW }))).toBe("upload_consent_revision");
    expect(await policy(store)).toBeNull();
    expect(await store.apply(T1, { mode: "selected", expectedRevision: 0 }, { trusted: true, nowMs: NOW })).toEqual({ revision: 1 });
    expect(await refusal(store.apply(T1, { mode: "all", expectedRevision: 0 }, { trusted: true, nowMs: NOW }))).toBe("upload_consent_revision");
    expect(await policy(store)).toEqual({ mode: "selected", revision: 1, newSessionBoundaryMs: null });
    expect(await store.apply(T1, { mode: "all", expectedRevision: 1 }, { trusted: true, nowMs: NOW })).toEqual({ revision: 2 });
  });

  test("an org-wide hold holds every user and only an operator releases it; a principal hold is released by its person", async () => {
    const { db, store } = await fixture();
    await db.query("INSERT INTO sessions.upload_consent_holds (org_id, reason_code) VALUES ('org-a', 'incident_1')");
    expect((await store.snapshot(T1, "origin-1", [], NOW)).held).toBe(true);
    expect((await store.snapshot(T2, "origin-1", [], NOW)).held).toBe(true);
    expect((await store.snapshot({ ...T1, orgId: "org-b" }, "origin-1", [], NOW)).held).toBe(false);
    // A person's trusted release does not touch the org-wide hold.
    expect(await store.apply(T1, { releaseHold: true }, { trusted: true, nowMs: NOW })).toEqual({ revision: 1 });
    expect((await store.snapshot(T1, "origin-1", [], NOW)).held).toBe(true);
    expect((await db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.upload_consent_holds WHERE released_at IS NULL")).rows[0]!.n).toBe(1);
    await db.query("UPDATE sessions.upload_consent_holds SET released_at = now(), released_by = 'operator'");
    expect((await store.snapshot(T1, "origin-1", [], NOW)).held).toBe(false);
    // A principal-level hold on user-1 only.
    await db.query("INSERT INTO sessions.upload_consent_holds (org_id, workspace_id, user_id, reason_code) VALUES ('org-a', 'ws-a', 'user-1', 'lost_device')");
    expect((await store.snapshot(T1, "origin-1", [], NOW)).held).toBe(true);
    expect((await store.snapshot(T2, "origin-1", [], NOW)).held).toBe(false);
    expect(await refusal(store.apply(T1, { releaseHold: true }, { trusted: false, nowMs: NOW }))).toBe("upload_consent_untrusted");
    expect((await store.snapshot(T1, "origin-1", [], NOW)).held).toBe(true);
    // user-2 cannot release user-1's hold.
    await store.apply(T2, { releaseHold: true }, { trusted: true, nowMs: NOW });
    expect((await store.snapshot(T1, "origin-1", [], NOW)).held).toBe(true);
    await store.apply(T1, { releaseHold: true }, { trusted: true, nowMs: NOW });
    expect((await store.snapshot(T1, "origin-1", [], NOW)).held).toBe(false);
    expect((await db.query<{ released_by: string }>("SELECT released_by FROM sessions.upload_consent_holds WHERE user_id = 'user-1'")).rows).toEqual([{ released_by: "person" }]);
  });

  test("principals are isolated: user-2's policy, allow and origin never show for user-1", async () => {
    const { store } = await fixture();
    await store.apply(T2, { mode: "all", allow: [S1], confirmOrigins: ["origin-1"] }, { trusted: true, nowMs: NOW });
    const snap = await store.snapshot(T1, "origin-1", [S1], NOW);
    expect(snap.policy).toBeNull();
    expect(snap.originConfirmed).toBe(false);
    expect(snap.sessions.get(sessionKey(S1.source, S1.sessionId))?.decision).toBeUndefined();
    const other = await store.snapshot({ ...T2, workspaceId: "ws-b" }, "origin-1", [S1], NOW);
    expect(other.policy).toBeNull();
    expect((await store.snapshot(T2, "origin-1", [S1], NOW)).sessions.get(sessionKey(S1.source, S1.sessionId))?.decision).toBe("allow");
  });

  test("an allow and a deny of one session in one write is invalid and writes nothing", async () => {
    const { db, store } = await fixture();
    const raw = await runAs(db)("SELECT sessions.upload_consent_apply_v1($1,$2,$3,$4::jsonb,$5,$6) AS result",
      ["org-a", "ws-a", "user-1", JSON.stringify({ allow: [{ source: "claude", session_id: "s-1" }], deny: [{ source: "claude", session_id: "s-1" }] }), true, NOW]);
    expect(raw.rows[0]!.result).toEqual({ ok: false, code: "invalid" });
    expect(await policy(store)).toBeNull();
  });

  test("the ingest role cannot read or write the consent tables directly", async () => {
    const { db } = await fixture();
    const run = runAs(db);
    for (const t of ["policies", "sessions", "origins", "holds"]) {
      await expect(run(`SELECT * FROM sessions.upload_consent_${t}`, [])).rejects.toMatchObject({ code: "42501" });
    }
    await expect(run("INSERT INTO sessions.upload_consent_holds (org_id, reason_code) VALUES ('org-a', 'x')", [])).rejects.toMatchObject({ code: "42501" });
    await expect(run("INSERT INTO sessions.upload_consent_policies (org_id, workspace_id, user_id, mode, revision) VALUES ('org-a','ws-a','user-1','all',1)", [])).rejects.toMatchObject({ code: "42501" });
    await expect(run("UPDATE sessions.upload_consent_holds SET released_at = now()", [])).rejects.toMatchObject({ code: "42501" });
    const acl = (await db.query<{ t: string; n: number }>(
      "SELECT c.relname t, count(a.*)::int n FROM pg_class c JOIN pg_namespace s ON s.oid=c.relnamespace LEFT JOIN LATERAL aclexplode(c.relacl) a ON a.grantee <> c.relowner WHERE s.nspname='sessions' AND c.relname LIKE 'upload_consent_%' AND c.relkind='r' GROUP BY 1 ORDER BY 1",
    )).rows;
    expect(acl).toEqual([
      { t: "upload_consent_holds", n: 0 },
      { t: "upload_consent_origins", n: 0 },
      { t: "upload_consent_policies", n: 0 },
      { t: "upload_consent_sessions", n: 0 },
    ]);
    // Only the owner and the ingest role may EXECUTE the two entry points.
    for (const fn of ["sessions.upload_consent_snapshot_v1(text,text,text,text,jsonb,bigint)", "sessions.upload_consent_apply_v1(text,text,text,jsonb,boolean,bigint)"])
      for (const [role, ok] of [["bulls96_session_ingest", true], ["anon", false], ["authenticated", false], ["service_role", false], ["bulls96_session_ingest_seeder", false]] as const)
        expect((await db.query<{ e: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') e", [role, fn])).rows[0]!.e).toBe(ok);
  });
});
