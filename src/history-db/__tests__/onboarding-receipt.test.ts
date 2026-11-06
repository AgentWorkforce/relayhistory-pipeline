import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../../session-ingest/app.ts";
import { historyRetentionViolations, runHistoryMigrations, type HistoryClient } from "../migrate.ts";
import type { Run } from "../onboarding.ts";

setDefaultTimeout(120_000);
const root = join(import.meta.dir, "../../..");
const open: PGlite[] = [];
afterEach(async () => { await Promise.all(open.splice(0).map((d) => d.close())); });
const TENANT = { userId: "user-1", orgId: "org-1", workspaceId: "ws-1" };

function client(db: PGlite): HistoryClient {
  return {
    async query<R>(text: string, params?: unknown[]) {
      if (params?.length) return (await db.query<R>(text, params)) as { rows: R[] };
      const results = await db.exec(text);
      return { rows: (results.at(-1)?.rows ?? []) as R[] };
    },
  };
}

/** The real ingest track (as the ingest PGlite tests load it), then the real history track. */
async function setup(opts: { ingestTrack?: boolean } = {}) {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } }); open.push(db);
  if (opts.ingestTrack !== false) {
    const read = (f: string) => readFileSync(join(root, f), "utf8");
    await db.exec(read("supabase/tests/supabase_shim.sql"));
    for (const f of ["session_directory_search_worker", "session_token_verifier", "session_ingest_roles"])
      await db.exec(read("supabase/bootstrap/" + f + ".sql"));
    for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort())
      await db.exec(read("supabase/migrations/" + f));
  }
  await runHistoryMigrations(client(db));
  // The Worker's own role, exactly as production runs it.
  const asIngest: Run = async (text, params) => {
    if (opts.ingestTrack === false) return (await db.query(text, params)) as { rows: Record<string, unknown>[] };
    await db.exec("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
    try {
      const r = await db.query(text, params);
      await db.exec("COMMIT");
      return r as { rows: Record<string, unknown>[] };
    } catch (e) {
      await db.exec("ROLLBACK");
      throw e;
    }
  };
  const app = createIngestApp({ deliver: {} as never, log: () => {}, identify: (r) => bindingIdentity(r), onboarding: { run: asIngest } });
  const call = (path: string, scopes: string[], tenant = TENANT) =>
    app.fetch(new Request(`https://x${path}`, { headers: { [IDENTITY_HEADER]: encodeIdentity({ ...tenant, scopes }) } }));
  const revision = (t: { org?: string; ws?: string; user?: string }, origin: string, record: string, source: string, session: string | null, at: string) =>
    db.query(
      `INSERT INTO sessions.ingest_record_revisions (org_id, workspace_id, origin_id, record_id, revision, revision_id, digest, user_id, source, session_id, updated_at)
       VALUES ($1, $2, $3, $4, 1, $4, repeat('a', 64), $5, $6, $7, $8::timestamptz)`,
      [t.org ?? TENANT.orgId, t.ws ?? TENANT.workspaceId, origin, record, t.user ?? TENANT.userId, source, session, at]);
  const importRows = async (rows: Record<string, string>[]) =>
    (await db.query<{ n: number }>("SELECT history.probe_session_imports_import($1::jsonb) AS n", [JSON.stringify(rows)])).rows[0]!.n;
  const imported = (source: string, session: string, at: string, t: Partial<typeof TENANT> = {}) => ({
    org_id: t.orgId ?? TENANT.orgId, workspace_id: t.workspaceId ?? TENANT.workspaceId, user_id: t.userId ?? TENANT.userId,
    source, session_id: session, last_received_at: at,
  });
  return { db, call, revision, importRows, imported };
}

const receipt = async (res: Response) => { expect(res.status).toBe(200); return res.json(); };

describe("onboarding receipt: relayhistory status counters off Neon (lane D / W4)", () => {
  test("an identity with nothing received is zero, null, []", async () => {
    const { call } = await setup();
    expect(await receipt(await call("/v1/onboarding/receipt", ["rth:read"]))).toEqual({ sessionCount: 0, lastReceivedAt: null, sources: [] });
  });

  test("ledger: distinct (source, session) of exactly this org/workspace/user; latest updated_at; sorted sources", async () => {
    const { call, revision } = await setup();
    await revision({}, "o1", "r1", "codex", "s1", "2026-11-05T10:00:00Z");
    await revision({}, "o1", "r2", "codex", "s1", "2026-11-05T11:00:00Z"); // same session, later record
    await revision({}, "o2", "r3", "codex", "s1", "2026-11-05T09:00:00Z"); // same session from another origin
    await revision({}, "o1", "r4", "claude", "s1", "2026-11-05T08:00:00Z"); // same id, other source: another session
    await revision({}, "o1", "r5", "claude", null, "2026-11-05T12:00:00Z"); // no session: not counted
    await revision({ user: "user-2" }, "o1", "r6", "cursor", "s9", "2026-11-05T13:00:00Z");
    await revision({ ws: "ws-2" }, "o1", "r7", "cursor", "s9", "2026-11-05T13:00:00Z");
    await revision({ org: "org-2" }, "o1", "r8", "cursor", "s9", "2026-11-05T13:00:00Z");
    expect(await receipt(await call("/v1/onboarding/receipt", ["rth:read"]))).toEqual({
      sessionCount: 2, lastReceivedAt: "2026-11-05T11:00:00.000Z", sources: ["claude", "codex"],
    });
    expect(await receipt(await call("/v1/onboarding/receipt", ["rth:read"], { ...TENANT, userId: "user-2" }))).toEqual({
      sessionCount: 1, lastReceivedAt: "2026-11-05T13:00:00.000Z", sources: ["cursor"],
    });
  });

  test("imported pre-cutover rollups count once with the ledger; deletions and later resets hide them", async () => {
    const { db, call, revision, importRows, imported } = await setup();
    expect(await importRows([
      imported("codex", "old-1", "2026-10-01T00:00:00Z"),
      imported("codex", "s1", "2026-10-02T00:00:00Z"), // also in the ledger: one session
      imported("grok", "old-2", "2026-10-03T00:00:00Z"),
      imported("codex", "elsewhere", "2026-10-03T00:00:00Z", { userId: "user-2" }),
    ])).toBe(4);
    // Re-import is idempotent and only ever moves a time later.
    expect(await importRows([imported("codex", "old-1", "2026-09-01T00:00:00Z")])).toBe(0);
    await revision({}, "o1", "r1", "codex", "s1", "2026-11-05T10:00:00Z");
    expect(await receipt(await call("/v1/onboarding/receipt", ["rth:read"]))).toEqual({
      sessionCount: 3, lastReceivedAt: "2026-11-05T10:00:00.000Z", sources: ["codex", "grok"],
    });
    // A deletion of an imported session (from any origin) hides it.
    await db.query("INSERT INTO sessions.upload_deletions (org_id, workspace_id, user_id, origin_id, source, session_id) VALUES ($1, $2, $3, 'o9', 'grok', 'old-2')",
      [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
    expect(await receipt(await call("/v1/onboarding/receipt", ["rth:read"]))).toMatchObject({ sessionCount: 2, sources: ["codex"] });
    // A reset hides every imported session received before it (the ledger rows are deleted by the reset itself).
    await db.query("INSERT INTO sessions.upload_resets (org_id, workspace_id, user_id, request_id, reset_at) VALUES ($1, $2, $3, 'req', '2026-10-15T00:00:00Z')",
      [TENANT.orgId, TENANT.workspaceId, TENANT.userId]);
    expect(await receipt(await call("/v1/onboarding/receipt", ["rth:read"]))).toEqual({
      sessionCount: 1, lastReceivedAt: "2026-11-05T10:00:00.000Z", sources: ["codex"],
    });
    expect(await importRows([imported("codex", "old-1", "2026-10-20T00:00:00Z")])).toBe(1);
    expect(await receipt(await call("/v1/onboarding/receipt", ["rth:read"]))).toMatchObject({ sessionCount: 2 });
    expect(await historyRetentionViolations()).toEqual([]);
  });

  test("the ingest role may only call the receipt: no import, no direct table read", async () => {
    const { db } = await setup();
    const asRole = async (sql: string) => {
      await db.exec("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
      try { return await db.query(sql); } finally { await db.exec("ROLLBACK"); }
    };
    expect(await asRole("SELECT history.probe_session_receipt('org-1', 'ws-1', 'user-1') AS r")).toBeTruthy();
    await expect(asRole("SELECT history.probe_session_imports_import('[]'::jsonb)")).rejects.toMatchObject({ code: "42501" });
    await expect(asRole("SELECT * FROM history.probe_session_imports")).rejects.toMatchObject({ code: "42501" });
    await expect(asRole("SELECT history.probe_session_receipt('', 'ws-1', 'user-1')")).rejects.toMatchObject({ code: "22023" });
  });

  test("scopes and workspace are enforced like the connection read", async () => {
    const { call } = await setup();
    expect((await call("/v1/onboarding/receipt", [])).status).toBe(403);
    expect((await call("/v1/onboarding/receipt", [SYNC_SCOPE])).status).toBe(403);
    expect((await call("/v1/onboarding/receipt", ["rth:read"], { ...TENANT, workspaceId: "" })).status).toBe(403);
  });

  test("fails closed with 503: no onboarding dependency, or the ingest track absent at call time", async () => {
    const bare = createIngestApp({ deliver: {} as never, log: () => {}, identify: (r) => bindingIdentity(r) });
    const headers = { [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: ["rth:read"] }) };
    expect((await bare.fetch(new Request("https://x/v1/onboarding/receipt", { headers }))).status).toBe(503);
    // The history track applies without the ingest track (plpgsql), but the read refuses.
    const { call } = await setup({ ingestTrack: false });
    expect((await call("/v1/onboarding/receipt", ["rth:read"])).status).toBe(503);
  });
});
