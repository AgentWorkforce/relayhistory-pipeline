import { afterEach, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { readFileSync } from "node:fs";
import { createReportDatabase } from "../database.ts";
import {
  canonicalPointerGeneration,
  type CanonicalIdentity,
  type CanonicalPointer,
} from "../../session-read-api/canonical.ts";
import type { HoopDb } from "../../shared/db.ts";
const handles: PGlite[] = [];
afterEach(async () => {
  for (const db of handles.splice(0)) await db.close();
});
const request = {
  scope: { organizationId: "o", workspaceId: "w" },
  window: {
    since: "2026-11-01T00:00:00.000Z",
    until: "2026-11-02T00:00:00.000Z",
  },
  maxSessions: 10,
  maxEvents: 100,
};
async function setup() {
  const p = new PGlite();
  handles.push(p);
  await p.exec(`CREATE ROLE anon;CREATE ROLE authenticated;CREATE ROLE service_role;CREATE SCHEMA sessions;
 CREATE TABLE sessions.session_index(org_id text,workspace_id text,harness text,session_id text,canonical_session_id text,machine_id text,user_id text,storage_provider text,bucket text,manifest_key text,content_sha256 bytea,raw_format text,revision bigint,deleted_at timestamptz);
 CREATE TABLE sessions.session_directory(org_id text,workspace_id text,harness text,session_id text,deleted_at timestamptz,event_projection_ready boolean,event_projection_version int);
 CREATE TABLE sessions.session_directory_event_pending(org_id text,workspace_id text,harness text,source_session_id text,dirty boolean);
 CREATE TABLE sessions.session_directory_event_times(org_id text,workspace_id text,harness text,session_id text,event_ts timestamptz);`);
  await p.exec(
    readFileSync(
      new URL(
        "../../../supabase/migrations/20261105047000_coordination_report_jobs.sql",
        import.meta.url,
      ),
      "utf8",
    ),
  );
  const queries: string[] = [];
  const query = async <R = Record<string, unknown>>(
    sql: string,
    params?: unknown[],
  ) => {
    queries.push(sql);
    const r = await p.query(sql, params);
    return { rows: r.rows as R[] };
  };
  const db = {
    query,
    pool: { connect: async () => ({ query, release() {} }) },
    end: async () => {},
  } as unknown as HoopDb;
  return { p, queries, api: createReportDatabase(db, request.scope) };
}
const artifact = (
  dependencies: Array<{ identity: CanonicalIdentity; generation: string }> = [],
) => ({ sourceSnapshot: { sessions: dependencies }, safe: "report" });
async function addHistorical(
  p: PGlite,
  api: ReturnType<typeof createReportDatabase>,
) {
  await p.query(
    "INSERT INTO sessions.session_directory VALUES('o','w','codex','historical',NULL,true,1)",
  );
  await p.query(
    "INSERT INTO sessions.session_directory_event_times VALUES('o','w','codex','historical','2026-11-01T12:00:00Z')",
  );
  await p.query(
    "INSERT INTO sessions.session_index VALUES('o','w','codex','historical',NULL,'m','u','s3','b','key',decode(repeat('a',64),'hex'),'ndjson',1,NULL)",
  );
  const identity = {
    orgId: "o",
    workspaceId: "w",
    source: "codex",
    sessionId: "historical",
  };
  const pointers = await api.pointers(identity);
  return {
    identity,
    generation: canonicalPointerGeneration(identity, pointers),
  };
}
test("durable idempotency, lease ownership and unchanged historical dependencies", async () => {
  const { p, api } = await setup();
  const dependency = await addHistorical(p, api);
  const id = await api.enqueue(request, "daily-1");
  expect(await api.enqueue(request, "daily-1")).toBe(id);
  await expect(
    api.enqueue({ ...request, maxEvents: 101 }, "daily-1"),
  ).rejects.toThrow("idempotency_conflict");
  const claim = await api.claim(id);
  expect(claim).not.toBeNull();
  expect(await api.claim(id)).toBeNull();
  expect(
    await api.complete(
      { ...claim!, leaseId: "00000000-0000-0000-0000-000000000000" },
      "0",
      artifact([dependency]),
    ),
  ).toBe(false);
  // A stale workspace epoch alone is harmless when exact historical dependencies match.
  expect(await api.complete(claim!, "0", artifact([dependency]))).toBe(true);
  expect((await api.read(id))?.status).toBe("ready");
  const inventoryBefore = await api.listSessions(request);
  // Unrelated current activity in the SAME workspace must not erase this historical report.
  await p.query(
    "INSERT INTO sessions.session_directory VALUES('o','w','codex','current',NULL,true,1)",
  );
  await p.query(
    "INSERT INTO sessions.session_directory_event_times VALUES('o','w','codex','current','2026-11-03T12:00:00Z')",
  );
  await p.query(
    "INSERT INTO sessions.session_index VALUES('o','w','codex','current',NULL,'m','u','s3','b','current',decode(repeat('b',64),'hex'),'ndjson',1,NULL)",
  );
  expect((await api.read(id))?.status).toBe("ready");
  expect((await api.listSessions(request)).generation).toBe(
    inventoryBefore.generation,
  );
  await p.query(
    "UPDATE sessions.session_index SET deleted_at=now() WHERE session_id='historical'",
  );
  expect(await api.read(id)).toMatchObject({
    status: "invalidated",
    result: null,
  });
  const retry = await api.claim(id);
  expect(retry).not.toBeNull();
  expect(await api.complete(retry!, "999", artifact([dependency]))).toBe(false);
});
test("event-time inventory excludes outside window; dirty metadata fails closed", async () => {
  const { p, api } = await setup();
  await addHistorical(p, api);
  expect((await api.listSessions(request)).sessions).toEqual([
    { orgId: "o", workspaceId: "w", source: "codex", sessionId: "historical" },
  ]);
  await p.query(
    "INSERT INTO sessions.session_directory_event_pending VALUES('o','w','codex','s',true)",
  );
  expect((await api.listSessions(request)).complete).toBe(false);
  await expect(
    api.listSessions({
      ...request,
      scope: { organizationId: "other", workspaceId: "w" },
    }),
  ).rejects.toThrow("scope_denied");
});
test("expired claims and changed window membership cannot publish or retain content", async () => {
  const { p, api } = await setup();
  const id = await api.enqueue(request, "daily-erasure");
  const old = await api.claim(id);
  await p.query(
    "UPDATE sessions.coordination_report_jobs SET lease_expires_at=now()-interval '1 second' WHERE id=$1",
    [id],
  );
  expect(await api.complete(old!, "0", artifact())).toBe(false);
  const fresh = await api.claim(id);
  expect(fresh?.leaseId).not.toBe(old?.leaseId);
  await addHistorical(p, api);
  // No transcript publication from a snapshot that missed newly visible historical work.
  expect(await api.complete(fresh!, "0", artifact())).toBe(false);
});
test("a ready empty report is invalidated when new event-time membership enters its window", async () => {
  const { p, api } = await setup();
  const id = await api.enqueue(request, "empty");
  const claim = await api.claim(id);
  expect(await api.complete(claim!, "0", artifact())).toBe(true);
  await addHistorical(p, api);
  expect(await api.read(id)).toMatchObject({
    status: "invalidated",
    result: null,
  });
});

test("ingestion uses shared fences without counter writes; publication uses matching exclusive fence", async () => {
  const { p, api, queries } = await setup();
  const dependency = await addHistorical(p, api);
  expect(
    (await p.query("SELECT * FROM sessions.coordination_report_epochs")).rows,
  ).toHaveLength(0);
  const id = await api.enqueue(request, "fence");
  const claim = await api.claim(id);
  expect(await api.complete(claim!, "initial", artifact([dependency]))).toBe(
    true,
  );
  expect(
    queries.some((q) =>
      q.includes(
        "pg_advisory_xact_lock(hashtextextended('coordination-report:'||length($1::text)::text||':'||$1::text||':'||$2::text,0))",
      ),
    ),
  ).toBe(true);
  const body = await p.query<{ prosrc: string }>(
    "SELECT prosrc FROM pg_proc WHERE oid='sessions.coordination_report_source_changed()'::regprocedure",
  );
  expect(body.rows[0]!.prosrc).toContain(
    "pg_advisory_xact_lock_shared(hashtextextended('coordination-report:'||length(scope_org)::text||':'||scope_org||':'||scope_workspace,0))",
  );
  expect(body.rows[0]!.prosrc).toContain(
    "ORDER BY value->>'org_id',value->>'workspace_id'",
  );
  expect(body.rows[0]!.prosrc).not.toContain(
    "INSERT INTO sessions.coordination_report_epochs",
  );
  expect(
    (
      await p.query<{ generation: number }>(
        "SELECT generation FROM sessions.coordination_report_epochs",
      )
    ).rows[0]!.generation,
  ).toBe(1);
  // A rejected lease cannot advance publication metadata.
  expect(await api.complete(claim!, "initial", artifact([dependency]))).toBe(
    false,
  );
  expect(
    (
      await p.query<{ generation: number }>(
        "SELECT generation FROM sessions.coordination_report_epochs",
      )
    ).rows[0]!.generation,
  ).toBe(1);
});


test("report migration down removes every artifact and up reapplies", async () => {
  const { p } = await setup();
  const down = readFileSync(new URL("../../../supabase/rollbacks/20261105047000_coordination_report_jobs.down.sql",import.meta.url),"utf8");
  const up = readFileSync(new URL("../../../supabase/migrations/20261105047000_coordination_report_jobs.sql",import.meta.url),"utf8");
  await p.exec(down);
  expect((await p.query(`SELECT relname FROM pg_class WHERE relnamespace='sessions'::regnamespace AND relname LIKE 'coordination_report_%'`)).rows).toEqual([]);
  expect((await p.query(`SELECT proname FROM pg_proc WHERE pronamespace='sessions'::regnamespace AND proname='coordination_report_source_changed'`)).rows).toEqual([]);
  expect((await p.query(`SELECT tgname FROM pg_trigger WHERE tgname LIKE 'coordination_report_%'`)).rows).toEqual([]);
  // Existing source tables remain writable after trigger removal.
  await p.query("INSERT INTO sessions.session_directory VALUES('o','w','codex','survives',NULL,true,1)");
  await p.exec(up);
  expect((await p.query(`SELECT relname FROM pg_class WHERE relnamespace='sessions'::regnamespace AND relname IN ('coordination_report_epochs','coordination_report_jobs') ORDER BY relname`)).rows).toEqual([{relname:"coordination_report_epochs"},{relname:"coordination_report_jobs"}]);
  expect((await p.query(`SELECT tgname FROM pg_trigger WHERE tgname LIKE 'coordination_report_%'`)).rows).toHaveLength(4);
  await p.query("UPDATE sessions.session_directory SET event_projection_ready=true WHERE session_id='survives'");
});
