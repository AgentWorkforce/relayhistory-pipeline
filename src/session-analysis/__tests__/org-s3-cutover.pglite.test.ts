/**
 * 20261105062120 on PGlite with the full migration plan: the scoped cutover proof
 * sessions.session_analysis_cutover_free (condition (d) of loadSessionSource), the immutable
 * sessions.org_s3_cutover attestation, the caller pin and grants, and the operator recorder
 * (src/session-analysis/org-s3-cutover.ts, scripts/record-org-s3-cutover.ts) in dry-run and execute.
 */
import { afterEach, beforeEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import { database, pointer, read, KEY } from "../../session-brief/__tests__/harness.ts";
import {
  ORG_S3_CUTOVER_ATTESTATION,
  OrgS3CutoverRefusal,
  parseOrgS3CutoverArgs,
  recordOrgS3Cutover,
} from "../org-s3-cutover.ts";

setDefaultTimeout(60_000);
let pg: PGlite;

beforeEach(async () => {
  pg = await database();
  await pg.exec(read("supabase/bootstrap/session_read_api_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  await pg.exec(read("supabase/bootstrap/session_analysis_admit_role.sql").split("\n").filter((l) => !l.startsWith("\\")).join("\n"));
  await pointer(pg); // KEY's live pointer, created_at = now()
});
afterEach(async () => {
  await pg.close();
});

const cutover = (org: string, uploadsAgo: string, turnsAgo = uploadsAgo) =>
  pg.query(
    `INSERT INTO sessions.org_s3_cutover (pointer_fix_deployed_at, org_id, uploads_cutover_at, turns_cutover_at, attestation, recorded_by)
     VALUES ('2026-01-01T00:00:00Z', $1, now() - $2::interval, now() - $3::interval, $4, 'operator-a')`,
    [org, uploadsAgo, turnsAgo, ORG_S3_CUTOVER_ATTESTATION],
  );
/** The proof for KEY with the snapshot's earliest evidence `evidenceAgo` before now (null: none). */
const free = async (evidenceAgo: string | null = "1 minute", key = KEY) =>
  (
    await pg.query<{ f: boolean }>(
      "SELECT sessions.session_analysis_cutover_free($1, $2, $3, $4, CASE WHEN $5::text IS NULL THEN NULL ELSE now() - $5::interval END) AS f",
      [key.orgId, key.workspaceId, key.harness, key.sessionId, evidenceAgo],
    )
  ).rows[0]!.f;
const ageFirstPointer = (interval: string) =>
  pg.query("UPDATE sessions.session_index SET created_at = created_at - $1::interval WHERE org_id = $2 AND session_id = $3", [
    interval,
    KEY.orgId,
    KEY.sessionId,
  ]);

// Every proof compares against cutover_at + 1 hour (the fixed clock-skew margin). The usual fixture
// records the cutover 3 hours ago, so first-seen and evidence must be later than 2 hours ago.
describe("session_analysis_cutover_free", () => {
  test("no cutover row: false for every session (inert until an operator records one)", async () => {
    expect(await free()).toBe(false);
    // Another org's cutover does not apply.
    await cutover("org-other", "3 hours");
    expect(await free()).toBe(false);
  });

  test("first seen and earliest evidence after the later of the two cutovers + 1h: true; otherwise false", async () => {
    await cutover(KEY.orgId, "3 hours");
    expect(await free("1 minute")).toBe(true);
    // Evidence after the cutover but inside the margin refuses even when the pointer's first-seen is fine.
    expect(await free("2 hours 30 minutes")).toBe(false);
    // No earliest evidence time: doubt.
    expect(await free(null)).toBe(false);
    // First seen inside the margin (after cutover_at, before cutover_at + 1h): refused.
    await ageFirstPointer("2 hours 30 minutes");
    expect(await free("1 minute")).toBe(false);
  });

  test("the effective cutover is the LATER of uploads and turns: a session between them is refused", async () => {
    // Uploads fenced 5h ago, but Neon turn writes only stopped 2h ago; the session began 90 minutes ago:
    // after uploads + 1h, but not after turns + 1h.
    await cutover(KEY.orgId, "5 hours", "2 hours");
    await ageFirstPointer("90 minutes");
    expect(await free("1 minute")).toBe(false);
    expect((await pg.query<{ ok: boolean }>("SELECT cutover_at = turns_cutover_at AS ok FROM sessions.org_s3_cutover")).rows[0]!.ok).toBe(true);
  });

  test("the 1 hour margin is exact and strict: cutover_at + 1h is false, + 1h + 1us is true (first-seen and evidence)", async () => {
    await cutover(KEY.orgId, "3 hours");
    const at = async (firstSeen: string, evidence: string) => {
      await pg.query(`UPDATE sessions.session_index SET created_at = (SELECT cutover_at + $1::interval FROM sessions.org_s3_cutover)`, [firstSeen]);
      return (
        await pg.query<{ f: boolean }>(
          "SELECT sessions.session_analysis_cutover_free($1, $2, $3, $4, (SELECT cutover_at + $5::interval FROM sessions.org_s3_cutover)) AS f",
          [KEY.orgId, KEY.workspaceId, KEY.harness, KEY.sessionId, evidence],
        )
      ).rows[0]!.f;
    };
    const EDGE = "1 hour", PAST = "1 hour 1 microsecond";
    expect(await at(PAST, PAST)).toBe(true);
    expect(await at(EDGE, PAST)).toBe(false);
    expect(await at(PAST, EDGE)).toBe(false);
    expect(await at(EDGE, EDGE)).toBe(false);
    expect(await at("0", PAST)).toBe(false); // equal to the bare cutover
  });

  test("tombstoned pointers: an older tombstoned pointer of the session blocks; only tombstones (no live pointer) is false", async () => {
    await cutover(KEY.orgId, "3 hours");
    expect(await free()).toBe(true);
    // A second machine's pointer, created before the cutover and since tombstoned: first-seen is its time.
    await pointer(pg, { machine: "m-2", manifestKey: "org-a/ws-a/m-2/claude/2026-10-29/s-1/manifest.json" });
    await pg.query("UPDATE sessions.session_index SET created_at = now() - interval '4 hours', deleted_at = now() WHERE machine_id = 'm-2'");
    expect(await free()).toBe(false);
    // Now every pointer is tombstoned and all are after the cutover: still false (no live pointer).
    await pg.query("UPDATE sessions.session_index SET created_at = now(), deleted_at = now()");
    expect(await free()).toBe(false);
  });

  test("first-seen is org-wide: the same physical session id in another workspace or harness before the cutover blocks", async () => {
    await cutover(KEY.orgId, "3 hours");
    await pointer(pg, { key: { ...KEY, workspaceId: "ws-b", harness: "codex" }, manifestKey: "org-a/ws-b/m-1/codex/2026-10-29/s-1/manifest.json" });
    await pg.query("UPDATE sessions.session_index SET created_at = now() - interval '4 hours' WHERE workspace_id = 'ws-b'");
    expect(await free()).toBe(false);
    await pg.query("DELETE FROM sessions.session_index WHERE workspace_id = 'ws-b'");
    expect(await free()).toBe(true);
  });

  test("first-seen matches the canonical id org-wide: an older pointer under another physical id sharing the canonical id blocks", async () => {
    await cutover(KEY.orgId, "3 hours");
    expect(await free()).toBe(true);
    await pointer(pg, { key: { ...KEY, workspaceId: "ws-c", sessionId: "phys-x" }, manifestKey: "org-a/ws-c/m-1/claude/2026-10-29/phys-x/manifest.json" });
    await pg.query("UPDATE sessions.session_index SET canonical_session_id = $1, created_at = now() - interval '4 hours' WHERE session_id = 'phys-x'", [KEY.sessionId]);
    expect(await free()).toBe(false);
  });

  test("first-seen is immune to later revisions: an upsert with a later start keeps the earliest created_at", async () => {
    await cutover(KEY.orgId, "3 hours");
    await ageFirstPointer("4 hours");
    expect(await free()).toBe(false);
    // A later revision of the same manifest claims a start after the cutover: least() keeps the original.
    const row = (await pg.query<Record<string, unknown>>("SELECT * FROM sessions.session_index WHERE org_id = $1 AND session_id = $2", [KEY.orgId, KEY.sessionId])).rows[0]!;
    const next = { ...row, revision: Number(row.revision) + 1, created_at: new Date().toISOString(), last_activity_at: new Date().toISOString(),
      content_sha256: "\\x" + "cd".repeat(32) };
    await pg.query("SELECT sessions.upsert_session_index($1::jsonb)", [JSON.stringify([next])]);
    const stored = (await pg.query<{ revision: string; old: boolean }>("SELECT revision::text, created_at < now() - interval '210 minutes' AS old FROM sessions.session_index WHERE org_id = $1 AND session_id = $2", [KEY.orgId, KEY.sessionId])).rows[0]!;
    expect(stored.revision).toBe(String(next.revision));
    expect(stored.old).toBe(true);
    expect(await free()).toBe(false);
  });

  test("guard: a plain UPDATE or DELETE that would flip a granted verdict is refused; only the audited correction path changes a row", async () => {
    await cutover(KEY.orgId, "3 hours");
    expect(await free()).toBe(true);
    const refused = async (sql: string) => {
      try {
        await pg.exec(sql);
        return "ok";
      } catch (error) {
        return (error as Error).message;
      }
    };
    // Each of these would retract (later cutover, delete) or widen (earlier cutover) a verdict outside the reviewed path.
    expect(await refused("UPDATE sessions.org_s3_cutover SET turns_cutover_at = now(), uploads_cutover_at = now()")).toContain("immutable");
    expect(await refused("UPDATE sessions.org_s3_cutover SET turns_cutover_at = turns_cutover_at - interval '9 hours', uploads_cutover_at = uploads_cutover_at - interval '9 hours'")).toContain("immutable");
    expect(await refused("DELETE FROM sessions.org_s3_cutover")).toContain("immutable");
    // Setting the correction marker by hand is not enough to lower it.
    expect(await refused("BEGIN; SELECT set_config('sessions.org_s3_cutover_correction', 'later', true); UPDATE sessions.org_s3_cutover SET turns_cutover_at = turns_cutover_at - interval '9 hours', uploads_cutover_at = uploads_cutover_at - interval '9 hours'; COMMIT"))
      .toContain("only move cutover_at later");
    await pg.exec("ROLLBACK").catch(() => {});
    expect(await free()).toBe(true);
  });
});

describe("sessions.org_s3_cutover is an immutable, owner-only attestation", () => {
  test("UPDATE, DELETE and TRUNCATE are refused (owner included); one row per org; both times in the past; the fixed attestation", async () => {
    await cutover(KEY.orgId, "1 hour");
    const fails = async (sql: string) => {
      try {
        await pg.exec(sql);
        return "ok";
      } catch (error) {
        return (error as Error).message;
      }
    };
    expect(await fails("UPDATE sessions.org_s3_cutover SET turns_cutover_at = now() - interval '9 hours'")).toContain("immutable");
    expect(await fails("DELETE FROM sessions.org_s3_cutover")).toContain("immutable");
    expect(await fails("TRUNCATE sessions.org_s3_cutover")).toContain("immutable");
    expect(await fails("INSERT INTO sessions.org_s3_cutover_history (org_id, uploads_cutover_at, turns_cutover_at, cutover_at, recorded_at, recorded_by, superseded_by, lowering_approved, superseded_at, pointer_fix_deployed_at) VALUES ('o', now(), now(), now(), now(), 'a', 'b', false, now() + interval '9 days', now())")).toBe("ok");
    // A forged superseded_at is overwritten with the database clock.
    expect((await pg.query<{ ok: boolean }>("SELECT superseded_at <= clock_timestamp() AS ok FROM sessions.org_s3_cutover_history WHERE org_id = 'o'")).rows[0]!.ok).toBe(true);
    expect(await fails("UPDATE sessions.org_s3_cutover_history SET note = 'x'")).toContain("immutable");
    expect(await fails("DELETE FROM sessions.org_s3_cutover_history")).toContain("immutable");
    expect(await fails("TRUNCATE sessions.org_s3_cutover_history")).toContain("immutable");
    expect(await fails(`INSERT INTO sessions.org_s3_cutover (pointer_fix_deployed_at, org_id, uploads_cutover_at, turns_cutover_at, attestation, recorded_by)
      VALUES ('2026-01-01T00:00:00Z', '${KEY.orgId}', now() - interval '5 hours', now() - interval '5 hours', '${ORG_S3_CUTOVER_ATTESTATION}', 'b')`)).toContain("duplicate key");
    expect(await fails(`INSERT INTO sessions.org_s3_cutover (pointer_fix_deployed_at, org_id, uploads_cutover_at, turns_cutover_at, attestation, recorded_by)
      VALUES ('2026-01-01T00:00:00Z', 'org-f', now() + interval '1 hour', now() - interval '1 hour', '${ORG_S3_CUTOVER_ATTESTATION}', 'b')`)).toContain("org_s3_cutover_not_future");
    expect(await fails(`INSERT INTO sessions.org_s3_cutover (pointer_fix_deployed_at, org_id, uploads_cutover_at, turns_cutover_at, attestation, recorded_by)
      VALUES ('2026-01-01T00:00:00Z', 'org-g', now() - interval '1 hour', now() - interval '1 hour', 'uploads_only', 'b')`)).toContain("check constraint");
    expect(await fails(`INSERT INTO sessions.org_s3_cutover (pointer_fix_deployed_at, org_id, uploads_cutover_at, attestation, recorded_by)
      VALUES ('2026-01-01T00:00:00Z', 'org-h', now() - interval '1 hour', '${ORG_S3_CUTOVER_ATTESTATION}', 'b')`)).toContain("turns_cutover_at");
    expect((await pg.query("SELECT 1 FROM sessions.org_s3_cutover")).rows).toHaveLength(1);
    // recorded_at is always the database clock: an owner INSERT's future value is overwritten.
    await pg.exec(`INSERT INTO sessions.org_s3_cutover (pointer_fix_deployed_at, org_id, uploads_cutover_at, turns_cutover_at, attestation, recorded_by, recorded_at)
      VALUES ('2026-01-01T00:00:00Z', 'org-r', now() + interval '1 day', now() + interval '1 day', '${ORG_S3_CUTOVER_ATTESTATION}', 'b', now() + interval '2 days')`).catch((e: Error) => {
      expect(e.message).toContain("org_s3_cutover_not_future");
    });
    expect((await pg.query("SELECT 1 FROM sessions.org_s3_cutover WHERE org_id = 'org-r'")).rows).toHaveLength(0);
    await pg.exec(`INSERT INTO sessions.org_s3_cutover (pointer_fix_deployed_at, org_id, uploads_cutover_at, turns_cutover_at, attestation, recorded_by, recorded_at)
      VALUES ('2026-01-01T00:00:00Z', 'org-s', now() - interval '1 hour', now() - interval '1 hour', '${ORG_S3_CUTOVER_ATTESTATION}', 'b', now() + interval '2 days')`);
    expect((await pg.query<{ ok: boolean }>("SELECT recorded_at <= clock_timestamp() AS ok FROM sessions.org_s3_cutover WHERE org_id = 'org-s'")).rows[0]!.ok).toBe(true);
  });

  /** Run `sql` as `role` in a rolled-back transaction; returns the error message or "ok". */
  async function as(role: string, sql: string) {
    await pg.exec("BEGIN");
    try {
      await pg.exec(`SET LOCAL ROLE ${role}`);
      await pg.exec(sql);
      return "ok";
    } catch (error) {
      return (error as Error).message;
    } finally {
      await pg.exec("ROLLBACK");
    }
  }

  test("grants: the worker (service_role) executes the proof; read-API, admission and client roles cannot; nobody but the owner touches the table", async () => {
    for (const role of ["service_role", "bulls96_read_api", "bulls96_analysis_admit", "anon", "authenticated", "intruder"])
      await pg.exec(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${role}') THEN CREATE ROLE ${role}; END IF; END $$`);
    await pg.exec("GRANT USAGE ON SCHEMA sessions TO service_role, bulls96_read_api, bulls96_analysis_admit, anon, authenticated, intruder");
    const call = `SELECT sessions.session_analysis_cutover_free('${KEY.orgId}', '${KEY.workspaceId}', '${KEY.harness}', '${KEY.sessionId}', now())`;
    expect(await as("service_role", call)).toBe("ok");
    for (const role of ["bulls96_read_api", "bulls96_analysis_admit", "anon", "authenticated", "intruder"])
      expect(await as(role, call)).toContain("permission denied");
    // Even with EXECUTE drifted onto them, the caller pin refuses roles the proof does not serve.
    await pg.exec("GRANT EXECUTE ON FUNCTION sessions.session_analysis_cutover_free(text,text,text,text,timestamptz) TO bulls96_read_api, intruder");
    expect(await as("bulls96_read_api", call)).toContain("caller is not a role this function serves");
    expect(await as("intruder", call)).toContain("caller is not a role this function serves");
    for (const role of ["service_role", "bulls96_read_api", "bulls96_analysis_admit", "anon", "authenticated"]) {
      expect(await as(role, "SELECT 1 FROM sessions.org_s3_cutover")).toContain("permission denied");
      expect(await as(role, "SELECT 1 FROM sessions.org_s3_cutover_history")).toContain("permission denied");
      expect(await as(role, `SELECT sessions.org_s3_cutover_correct_v1('${KEY.orgId}', now(), now(), 'x', NULL, true)`)).toContain("permission denied");
      expect(await as(role, `INSERT INTO sessions.org_s3_cutover (pointer_fix_deployed_at, org_id, uploads_cutover_at, turns_cutover_at, attestation, recorded_by)
        VALUES ('2026-01-01T00:00:00Z', 'x', now(), now(), '${ORG_S3_CUTOVER_ATTESTATION}', 'x')`)).toContain("permission denied");
    }
    const acl = (await pg.query<{ g: string; p: string }>(`
      SELECT r.rolname g, a.privilege_type p FROM pg_class c
        CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a JOIN pg_roles r ON r.oid = a.grantee
       WHERE c.oid IN ('sessions.org_s3_cutover'::regclass, 'sessions.org_s3_cutover_history'::regclass) AND a.grantee <> c.relowner`)).rows;
    expect(acl).toEqual([]);
    const fnAcl = (await pg.query<{ f: string; g: string }>(`
      SELECT p.oid::regprocedure::text f, r.rolname g FROM pg_proc p
        CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a JOIN pg_roles r ON r.oid = a.grantee
       WHERE p.proname IN ('session_analysis_cutover_free', 'org_s3_cutover_immutable_v1', 'org_s3_cutover_correct_v1', 'org_s3_cutover_stamp_v1', 'session_analysis_first_seen_v1') AND a.grantee <> p.proowner
         AND r.rolname NOT IN ('bulls96_read_api', 'intruder') ORDER BY 1, 2`)).rows;
    expect(fnAcl).toEqual([{ f: "sessions.session_analysis_cutover_free(text,text,text,text,timestamp with time zone)", g: "service_role" }]);
    const pinned = (await pg.query<{ c: string[] | null; d: boolean }>(
      "SELECT proconfig c, prosecdef d FROM pg_proc WHERE oid = 'sessions.session_analysis_cutover_free(text,text,text,text,timestamptz)'::regprocedure")).rows[0]!;
    expect(pinned).toEqual({ c: ["search_path=pg_catalog, sessions, pg_temp"], d: true });
    // Both sentinels are registered, ordered after 062110, and report applied.
    const { MIGRATION_SENTINELS } = await import("../../hoopdb/migration-sentinels.ts");
    for (const step of ["upstream:20261105062120_analysis_org_s3_cutover.sql", "upstream:20261105062130_analysis_cutover_source_revision.sql"]) {
      expect(step > "upstream:20261105062110").toBe(true);
      expect(MIGRATION_SENTINELS[step]).toBeDefined();
      expect((await pg.query<{ applied: boolean }>(MIGRATION_SENTINELS[step]!.sql)).rows[0]!.applied).toBe(true);
    }
  });
});

describe("operator recorder (scripts/record-org-s3-cutover.ts)", () => {
  const NOW = new Date("2026-11-17T12:00:00Z");
  const base = [
    "--org", KEY.orgId,
    "--uploads-cutover-at", "2026-11-17T10:00:00Z",
    "--turns-cutover-at", "2026-11-17T11:00:00Z",
    "--recorded-by", "operator-a",
    "--pointer-fix-deployed-at", "2026-11-17T09:00:00Z",
    "--attest-uploads-fenced", "--attest-turns-forwarded",
  ];
  const refusal = (fn: () => unknown) => {
    try {
      fn();
      return "ok";
    } catch (error) {
      return error instanceof OrgS3CutoverRefusal ? error.code : String(error);
    }
  };

  test("arguments: dry-run by default; both attestations required (no turns forwarding, no row); times explicit and not in the future", () => {
    expect(parseOrgS3CutoverArgs(base, NOW)).toEqual({
      orgId: KEY.orgId, uploadsCutoverAt: "2026-11-17T10:00:00Z", turnsCutoverAt: "2026-11-17T11:00:00Z",
      recordedBy: "operator-a", note: null, execute: false, correct: false, approvedLowering: false,
      pointerFixDeployedAt: "2026-11-17T09:00:00Z",
    });
    // The pointer-move fix must be deployed no later than the effective cutover; the flag is required.
    expect(refusal(() => parseOrgS3CutoverArgs(base.map((a) => (a === "2026-11-17T09:00:00Z" ? "2026-11-17T11:30:00Z" : a)), NOW))).toBe("cutover_before_pointer_fix");
    expect(parseOrgS3CutoverArgs(base.map((a) => (a === "2026-11-17T09:00:00Z" ? "2026-11-17T11:00:00Z" : a)), NOW).pointerFixDeployedAt).toBe("2026-11-17T11:00:00Z");
    const without = [...base]; without.splice(without.indexOf("--pointer-fix-deployed-at"), 2);
    expect(refusal(() => parseOrgS3CutoverArgs(without, NOW))).toBe("cutover_pointer_fix_time");
    expect(parseOrgS3CutoverArgs([...base, "--correct", "--khaliq-approved-lowering"], NOW)).toMatchObject({ correct: true, approvedLowering: true });
    // Approval for a lowering only means something in a correction.
    expect(refusal(() => parseOrgS3CutoverArgs([...base, "--khaliq-approved-lowering"], NOW))).toBe("cutover_args");
    expect(parseOrgS3CutoverArgs([...base, "--execute", "--note", "ticket 12"], NOW)).toMatchObject({ execute: true, note: "ticket 12" });
    expect(refusal(() => parseOrgS3CutoverArgs(base.filter((a) => a !== "--attest-turns-forwarded"), NOW))).toBe("cutover_turns_not_attested");
    expect(refusal(() => parseOrgS3CutoverArgs(base.filter((a) => a !== "--attest-uploads-fenced"), NOW))).toBe("cutover_uploads_not_attested");
    expect(refusal(() => parseOrgS3CutoverArgs(base.map((a) => (a === "2026-11-17T11:00:00Z" ? "2026-11-17T13:00:00Z" : a)), NOW))).toBe("cutover_time_future");
    expect(refusal(() => parseOrgS3CutoverArgs(base.map((a) => (a === "2026-11-17T11:00:00Z" ? "2026-11-17 11:00" : a)), NOW))).toBe("cutover_time");
    expect(refusal(() => parseOrgS3CutoverArgs(base.map((a) => (a === "operator-a" ? "bad actor" : a)), NOW))).toBe("cutover_recorded_by");
    expect(refusal(() => parseOrgS3CutoverArgs([...base, "--bogus"], NOW))).toBe("cutover_args");
    expect(refusal(() => parseOrgS3CutoverArgs([...base, "--org", "again"], NOW))).toBe("cutover_args");
    expect(refusal(() => parseOrgS3CutoverArgs(base.map((a) => (a === KEY.orgId ? "" : a)), NOW))).toBe("cutover_org");
  });

  const client = () => ({ query: <T,>(text: string, params?: unknown[]) => pg.query(text, params) as Promise<{ rows: T[] }> });
  const input = (over: Partial<Parameters<typeof recordOrgS3Cutover>[1]> = {}) => ({
    orgId: KEY.orgId, uploadsCutoverAt: new Date(Date.now() - 4 * 3_600_000).toISOString(), turnsCutoverAt: new Date(Date.now() - 3 * 3_600_000).toISOString(),
    recordedBy: "operator-a", note: null, execute: false, correct: false, approvedLowering: false,
    pointerFixDeployedAt: new Date(Date.now() - 6 * 3_600_000).toISOString(), ...over,
  });

  test("dry-run writes nothing and reports counts only; execute inserts one row; a second execute is refused", async () => {
    // A second session first seen before the cutover.
    await pointer(pg, { key: { ...KEY, sessionId: "s-old" }, manifestKey: "org-a/ws-a/m-1/claude/2026-10-29/s-old/manifest.json" });
    await pg.query("UPDATE sessions.session_index SET created_at = now() - interval '150 minutes' WHERE session_id = 's-old'"); // after cutover, inside the margin
    const dry = await recordOrgS3Cutover(client(), input());
    expect(dry).toEqual({ mode: "dry-run", existing: 0, inserted: 0, sessionsAfter: 1, sessionsAtOrBefore: 1 });
    expect(JSON.stringify(dry)).not.toContain(KEY.orgId);
    expect((await pg.query("SELECT 1 FROM sessions.org_s3_cutover")).rows).toHaveLength(0);
    expect(await free()).toBe(false);
    const done = await recordOrgS3Cutover(client(), input({ execute: true }));
    expect(done).toEqual({ mode: "execute", existing: 0, inserted: 1, sessionsAfter: 1, sessionsAtOrBefore: 1 });
    const row = (await pg.query<{ attestation: string; recorded_by: string; later: boolean }>(
      "SELECT attestation, recorded_by, cutover_at = turns_cutover_at AS later FROM sessions.org_s3_cutover")).rows;
    expect(row).toEqual([{ attestation: ORG_S3_CUTOVER_ATTESTATION, recorded_by: "operator-a", later: true }]);
    expect(await free()).toBe(true);
    await expect(recordOrgS3Cutover(client(), input({ execute: true }))).rejects.toMatchObject({ code: "cutover_already_recorded" });
    expect((await pg.query("SELECT 1 FROM sessions.org_s3_cutover")).rows).toHaveLength(1);
    // The transaction was rolled back: the connection is usable.
    expect((await recordOrgS3Cutover(client(), input())).existing).toBe(1);
  });

  test("pointer fix: the recorder refuses a cutover before --pointer-fix-deployed-at at record time; the row stores it; the database refuses it too", async () => {
    const hours = (h: number) => new Date(Date.now() - h * 3_600_000).toISOString();
    // Effective cutover 3h ago, fix deployed 2h ago: refused before anything is written (dry-run and execute).
    await expect(recordOrgS3Cutover(client(), input({ pointerFixDeployedAt: hours(2) }))).rejects.toMatchObject({ code: "cutover_before_pointer_fix" });
    await expect(recordOrgS3Cutover(client(), input({ pointerFixDeployedAt: hours(2), execute: true }))).rejects.toMatchObject({ code: "cutover_before_pointer_fix" });
    expect((await pg.query("SELECT 1 FROM sessions.org_s3_cutover")).rows).toHaveLength(0);
    // Fix deployed exactly at the effective cutover: accepted and stored.
    const fix = hours(3);
    expect(await recordOrgS3Cutover(client(), input({ pointerFixDeployedAt: fix, execute: true }))).toMatchObject({ inserted: 1 });
    const stored = (await pg.query<{ ok: boolean }>("SELECT pointer_fix_deployed_at = $1::timestamptz AS ok FROM sessions.org_s3_cutover", [fix])).rows[0]!;
    expect(stored.ok).toBe(true);
    // The database enforces it for any owner insert, and for a correction below the stored fix time.
    await expect(pg.query(`INSERT INTO sessions.org_s3_cutover (org_id, uploads_cutover_at, turns_cutover_at, attestation, recorded_by, pointer_fix_deployed_at)
      VALUES ('org-p', now() - interval '3 hours', now() - interval '3 hours', $1, 'op', now() - interval '1 hour')`, [ORG_S3_CUTOVER_ATTESTATION])).rejects.toThrow("org_s3_cutover_after_pointer_fix");
    await expect(pg.query("SELECT sessions.org_s3_cutover_correct_v1($1, now() - interval '5 hours', now() - interval '5 hours', 'op', NULL, true)", [KEY.orgId]))
      .rejects.toThrow("org_s3_cutover_after_pointer_fix");
  });

  test("--correct: later is applied (prior row to history); lowering is refused without approval, applied with it; no row refuses", async () => {
    const hours = (h: number) => new Date(Date.now() - h * 3_600_000).toISOString();
    await expect(recordOrgS3Cutover(client(), input({ correct: true, execute: true }))).rejects.toMatchObject({ code: "cutover_not_recorded" });
    await recordOrgS3Cutover(client(), input({ execute: true })); // cutover 3h ago
    expect(await free()).toBe(true);
    // Dry-run of a stricter correction: direction reported, nothing written.
    const later = { uploadsCutoverAt: hours(4), turnsCutoverAt: hours(1), recordedBy: "operator-b" };
    expect(await recordOrgS3Cutover(client(), input({ ...later, correct: true }))).toMatchObject({ mode: "dry-run", direction: "later", corrected: 0 });
    expect((await pg.query("SELECT 1 FROM sessions.org_s3_cutover_history")).rows).toHaveLength(0);
    // Execute it: stricter, so the session (first seen now, evidence 1 minute ago) is now inside the margin.
    expect(await recordOrgS3Cutover(client(), input({ ...later, correct: true, execute: true }))).toMatchObject({ direction: "later", corrected: 1 });
    expect(await free()).toBe(false);
    const history = (await pg.query<{ recorded_by: string; superseded_by: string; lowering_approved: boolean }>(
      "SELECT recorded_by, superseded_by, lowering_approved FROM sessions.org_s3_cutover_history")).rows;
    expect(history).toEqual([{ recorded_by: "operator-a", superseded_by: "operator-b", lowering_approved: false }]);
    // Lowering without approval: refused by the recorder (dry-run and execute) and by the database function itself.
    const lower = { uploadsCutoverAt: hours(5), turnsCutoverAt: hours(5), recordedBy: "operator-c" };
    await expect(recordOrgS3Cutover(client(), input({ ...lower, correct: true }))).rejects.toMatchObject({ code: "cutover_lowering_not_approved" });
    await expect(recordOrgS3Cutover(client(), input({ ...lower, correct: true, execute: true }))).rejects.toMatchObject({ code: "cutover_lowering_not_approved" });
    await expect(pg.query("SELECT sessions.org_s3_cutover_correct_v1($1, $2, $3, 'x', NULL, false)", [KEY.orgId, lower.uploadsCutoverAt, lower.turnsCutoverAt]))
      .rejects.toThrow("only move cutover_at later");
    expect((await pg.query("SELECT 1 FROM sessions.org_s3_cutover_history")).rows).toHaveLength(1);
    expect(await free()).toBe(false);
    // With Khaliq's approval: applied and recorded as an approved lowering.
    expect(await recordOrgS3Cutover(client(), input({ ...lower, correct: true, approvedLowering: true, execute: true }))).toMatchObject({ direction: "lowered", corrected: 1 });
    expect(await free()).toBe(true);
    expect((await pg.query<{ n: number }>("SELECT count(*)::int n FROM sessions.org_s3_cutover_history WHERE lowering_approved")).rows[0]!.n).toBe(1);
    // Deletion is not offered anywhere: still refused.
    await expect(pg.query("DELETE FROM sessions.org_s3_cutover")).rejects.toThrow("immutable");
  });
});
