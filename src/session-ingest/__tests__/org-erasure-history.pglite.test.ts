/**
 * 20261105062340_ingest_org_erasure_history on PGlite with every migration: the durable erasure history that 062320's
 * release counts. Single-connection semantics (what is recorded, what the release and bind refuse); the two-connection
 * lock orders (a writer first, a release first, a writer that resumes after the release) are the PostgreSQL suite's
 * (org-erasure-history.postgres.test.ts).
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ORG_ERASURE_HISTORY_SOURCES } from "../../hoopdb/org-erasure-history-sources.ts";
import { TENANT } from "./support.ts";
import { authorityFixture } from "./privacy-authority-support.ts";

setDefaultTimeout(180_000);
const open: Array<() => unknown> = [];
afterEach(async () => { for (const c of open.splice(0).reverse()) await c(); });
type Fixture = Awaited<ReturnType<typeof authorityFixture>>;
const ROOT = join(import.meta.dir, "../../..");
const MIGRATION = readFileSync(join(ROOT, "supabase/migrations/20261105062340_ingest_org_erasure_history.sql"), "utf8");
const EVIDENCE = "e".repeat(64);
const ORG = TENANT.orgId;
const OTHER = "org-b";

async function fixture() {
  const f = await authorityFixture();
  open.push(f.close);
  await f.db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text)");
  for (const n of ["upstream:20261105062320_ingest_org_admission_fences.sql", "upstream:20261105062340_ingest_org_erasure_history.sql"])
    await f.db.query("INSERT INTO sessions.__96bulls_migrations(name) VALUES($1) ON CONFLICT DO NOTHING", [n]);
  return f;
}
const one = async (f: Fixture, sql: string, p: unknown[] = []) => (await f.db.query<Record<string, any>>(sql, p)).rows[0]!;
const code = (p: Promise<unknown>) => p.then(() => "ok", (e: { code?: string }) => String(e.code));
const status = async (f: Fixture, org = ORG) => (await one(f, "SELECT sessions.ingest_org_admission_hold_status($1) s", [org])).s;
const deletion = (f: Fixture, org: string, session: string) => f.db.query(
  "INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,'u','o','claude',$3)",
  [org, TENANT.workspaceId, session]);

const SEAL = "b".repeat(64);
/** A FULL start strictly between the epoch's install and seal. */
/** The bridge's selected FULL: started (integer ms) 1 ms after the install (the install-seal gap is >= 2 ms). */
const MID = "(SELECT floor(extract(epoch FROM installed_at) * 1000)::bigint + 1 FROM sessions.ingest_org_admission_holds WHERE epoch=$2::uuid)";
const FULL_ARGS = `'full-run-1',${MID}`;
/** install, seal (the FULL receipt), bind that FULL, attempt the marker: ready for bridge_complete. */
async function toAttempt(f: Fixture, org = ORG) {
  const e = (await one(f, "SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [org])).e as string;
  await f.db.query("SELECT pg_sleep(0.002)"); // a FULL start strictly between install and seal
  await f.db.query(`SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,'${SEAL}')`, [org, e]);
  await f.db.query(`SELECT sessions.ingest_org_erasure_history_bind($1,$2::uuid,${FULL_ARGS},$3)`, [org, e, SEAL]);
  await f.db.query(`SELECT sessions.ingest_org_admission_hold_marker_attempt($1,$2::uuid,'${"c".repeat(64)}')`, [org, e]);
  return e;
}
const release = (f: Fixture, e: string, n: number | null, org = ORG, detail: string | null = null) =>
  f.db.query("SELECT sessions.ingest_org_admission_hold_release($1,$2::uuid,'bridge_complete',$3,$5,$4)", [org, e, EVIDENCE, n, detail]);

describe("062340 the shared source list", () => {
  test("the migration's recorders, truncate refusals and source CHECK are exactly ORG_ERASURE_HISTORY_SOURCES; each has org_id", async () => {
    const f = await fixture();
    const trig = async (name: string) => (await f.db.query<{ t: string }>(
      "SELECT c.relname::text t FROM pg_trigger g JOIN pg_class c ON c.oid=g.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='sessions' AND g.tgname=$1 AND g.tgenabled='O' ORDER BY 1", [name])).rows.map((r) => r.t);
    const sorted = [...ORG_ERASURE_HISTORY_SOURCES].sort();
    expect(await trig("ingest_org_erasure_history_record")).toEqual(sorted);
    expect(await trig("ingest_org_erasure_history_refuse_truncate")).toEqual(sorted);
    const check = /source text NOT NULL CHECK \(source IN \(([^)]*)\)\)/u.exec(MIGRATION)![1]!.split(", ").map((s) => s.slice(1, -1));
    expect(check).toEqual([...ORG_ERASURE_HISTORY_SOURCES]);
    for (const t of ORG_ERASURE_HISTORY_SOURCES)
      expect((await one(f, "SELECT count(*)::int n FROM information_schema.columns WHERE table_schema='sessions' AND table_name=$1 AND column_name='org_id'", [t])).n).toBe(1);
    expect((await one(f, "SELECT sessions.ingest_org_erasure_history_recording() r")).r).toBe(true);
    expect((await one(f, "SELECT sessions.ingest_org_erasure_history_coverage_start() IS NOT NULL c")).c).toBe(true);
  });
});

describe("062340 what the release counts", () => {
  test("the 0 -> 1 race: an erasure after the watermark read refuses the release; the re-read count releases", async () => {
    const f = await fixture();
    const e = await toAttempt(f);
    const read = await status(f);
    expect(read.erasures).toBe(0);
    expect(read.erasure_history).toMatchObject({ covered: true, recording: true, by_source: {} });
    await deletion(f, ORG, "s-race");
    expect(await code(release(f, e, read.erasures))).toBe("55000");
    const again = await status(f);
    expect(again.erasures).toBe(1);
    expect(again.erasure_history.by_source).toEqual({ upload_deletions: 1 });
    expect(await code(release(f, e, again.erasures))).toBe("ok");
  });

  test("a transient row (inserted, then deleted) is still counted: a surviving-row count would miss it", async () => {
    const f = await fixture();
    const e = await toAttempt(f);
    const read = await status(f);
    await deletion(f, ORG, "s-transient");
    await f.db.query("DELETE FROM sessions.upload_deletions WHERE org_id=$1 AND session_id='s-transient'", [ORG]);
    expect((await one(f, "SELECT count(*)::int n FROM sessions.upload_deletions WHERE org_id=$1", [ORG])).n).toBe(0);
    expect(await code(release(f, e, read.erasures))).toBe("55000");
    expect((await f.db.query("SELECT op FROM sessions.ingest_org_erasure_history WHERE erased_org=$1 ORDER BY at", [ORG])).rows.map((r: any) => r.op)).toEqual(["INSERT", "DELETE"]);
  });

  test("a same-count replacement and an old row updated into the window both change the count", async () => {
    const f = await fixture();
    await deletion(f, ORG, "s-old"); // before the hold: outside the window
    const e = await toAttempt(f);
    const read = await status(f);
    expect(read.erasures).toBe(0);
    await f.db.query("DELETE FROM sessions.upload_deletions WHERE org_id=$1 AND session_id='s-old'", [ORG]);
    await deletion(f, ORG, "s-new"); // one surviving row, as before
    expect((await one(f, "SELECT count(*)::int n FROM sessions.upload_deletions WHERE org_id=$1", [ORG])).n).toBe(1);
    expect((await status(f)).erasures).toBe(2);
    await f.db.query("UPDATE sessions.upload_deletions SET deleted_at=clock_timestamp() WHERE org_id=$1", [ORG]);
    expect((await status(f)).erasures).toBe(3);
    expect(await code(release(f, e, read.erasures))).toBe("55000");
  });

  test("an UPDATE that moves a row to another org is recorded for both orgs", async () => {
    const f = await fixture();
    await deletion(f, ORG, "s-move");
    await f.db.query("UPDATE sessions.upload_deletions SET org_id=$2 WHERE org_id=$1 AND session_id='s-move'", [ORG, OTHER]);
    const rows = (await f.db.query("SELECT erased_org, op FROM sessions.ingest_org_erasure_history WHERE op='UPDATE' ORDER BY erased_org")).rows;
    expect(rows).toEqual([{ erased_org: ORG, op: "UPDATE" }, { erased_org: OTHER, op: "UPDATE" }]);
  });

  test("another org's erasures are recorded under that org and do not move this org's count", async () => {
    const f = await fixture();
    const e = await toAttempt(f);
    const read = await status(f);
    await deletion(f, OTHER, "s-b");
    expect((await status(f)).erasures).toBe(read.erasures);
    expect((await one(f, "SELECT count(*)::int n FROM sessions.ingest_org_erasure_history WHERE erased_org=$1", [OTHER])).n).toBeGreaterThan(0);
    expect(await code(release(f, e, read.erasures))).toBe("ok");
  });
});

describe("062340 bind and release refusals", () => {
  test("bind: the fence is the install (never a caller value); the selected FULL is the sealed one, started between install and seal, once", async () => {
    const f = await fixture();
    const e = (await one(f, "SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [ORG])).e as string;
    const bind = (startMs: string, ev = SEAL, run = "full-run-1", org = ORG) =>
      code(f.db.query(`SELECT sessions.ingest_org_erasure_history_bind($1,$2::uuid,$4,${startMs},$3)`, [org, e, ev, run]));
    const ms = (expr: string) => `(SELECT floor(extract(epoch FROM ${expr}) * 1000)::bigint FROM sessions.ingest_org_admission_holds WHERE epoch=$2::uuid)`;
    expect(await bind(MID)).toBe("55000"); // held, not sealed
    await f.db.query("SELECT pg_sleep(0.002)");
    await f.db.query(`SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,'${SEAL}')`, [ORG, e]);
    expect(await bind(MID, "d".repeat(64))).toBe("55000"); // not the sealed FULL's receipt
    expect(await bind(ms("installed_at"))).toBe("55000"); // started at or before the install
    expect(await bind(`${ms("sealed_at")} + 1`)).toBe("55000"); // after the seal
    expect(await bind("0")).toBe("22023");
    expect(await bind("253402300800000")).toBe("22023");
    expect(await bind(MID, SEAL, "bad run!")).toBe("22023");
    expect(await bind(MID)).toBe("ok");
    expect(await bind(MID)).toBe("ok"); // the same FULL again
    expect(await bind(MID, SEAL, "full-run-2")).toBe("55000"); // another FULL after a bind
    expect(await bind(MID, SEAL, "full-run-1", OTHER)).toBe("55000");
    const st = await status(f);
    const h = await one(f, "SELECT installed_at, floor(extract(epoch FROM installed_at) * 1000)::bigint + 1 AS ms FROM sessions.ingest_org_admission_holds WHERE epoch=$1::uuid", [e]);
    // The fence at stored precision, and the selected FULL exactly as given, in separate fields.
    expect((await one(f, "SELECT ($1::jsonb->'erasure_history'->>'fence_at')::timestamptz = installed_at AS eq FROM sessions.ingest_org_admission_holds WHERE epoch=$2::uuid", [JSON.stringify(st), e])).eq).toBe(true);
    expect(st.selected_full).toEqual({ run: "full-run-1", started_ms: Number(h.ms), started_at: st.selected_full.started_at, evidence_sha256: SEAL });
    expect((await one(f, "SELECT ($1::jsonb->'selected_full'->>'started_at')::timestamptz = timestamptz 'epoch' + $2::bigint * interval '1 ms' AS eq", [JSON.stringify(st), h.ms])).eq).toBe(true);
  });

  test("release refuses with no binding, and when coverage restarted after the cutoff (a down and reapply)", async () => {
    const f = await fixture();
    const e = (await one(f, "SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text e", [ORG])).e as string;
    await f.db.query("SELECT pg_sleep(0.002)");
    await f.db.query(`SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,'${SEAL}')`, [ORG, e]);
    await f.db.query(`SELECT sessions.ingest_org_admission_hold_marker_attempt($1,$2::uuid,'${"c".repeat(64)}')`, [ORG, e]);
    expect(await code(release(f, e, 0))).toBe("55000"); // not bound (and after the attempt it can no longer be: not sealed)
    const g = await fixture();
    const e2 = await toAttempt(g);
    // What a down then a reapply leave: an 'ended' and a later 'started' row. The bound cutoff predates the new start.
    await g.db.query("INSERT INTO sessions.ingest_org_erasure_history_coverage(kind) VALUES('ended')");
    await g.db.query("INSERT INTO sessions.ingest_org_erasure_history_coverage(kind) VALUES('started')");
    expect((await status(g)).erasure_history.covered).toBe(false);
    expect(await code(release(g, e2, 0))).toBe("55000");
  });

  test("a disarm, even one re-armed since, refuses every later release; a permitted truncate restarts coverage", async () => {
    const f = await fixture();
    expect(await code(f.db.query("TRUNCATE sessions.upload_resets"))).toBe("ok"); // no hold: allowed, and recorded
    expect((await f.db.query("SELECT kind FROM sessions.ingest_org_erasure_history_coverage ORDER BY at")).rows.map((r: any) => r.kind)).toEqual(["started", "truncated"]);
    const e = await toAttempt(f);
    for (const t of ORG_ERASURE_HISTORY_SOURCES) expect(await code(f.db.query(`TRUNCATE sessions.${t} CASCADE`))).toBe("55000");
    expect((await status(f)).erasure_history.recording).toBe(true);
    // The reproduced bypass: disable, a transient insert and delete, re-enable.
    await f.db.exec("ALTER TABLE sessions.upload_deletions DISABLE TRIGGER ingest_org_erasure_history_record");
    await deletion(f, ORG, "s-dark");
    await f.db.query("DELETE FROM sessions.upload_deletions WHERE org_id=$1 AND session_id='s-dark'", [ORG]);
    await f.db.exec("ALTER TABLE sessions.upload_deletions ENABLE TRIGGER ingest_org_erasure_history_record");
    const st = await status(f);
    expect(st.erasures).toBe(0); // nothing was recorded...
    expect(st.erasure_history.recording).toBe(false); // ...but the re-arm is visible
    expect(await code(release(f, e, 0))).toBe("55000");
    expect(await code(release(f, e, 0, ORG, "erasure_window_empty"))).toBe("55000");
  });

  test("a TRUNCATE outside READ COMMITTED is refused 40001, hold or not (its snapshot could predate an install)", async () => {
    const f = await fixture();
    for (const iso of ["REPEATABLE READ", "SERIALIZABLE"]) {
      await f.db.exec(`BEGIN ISOLATION LEVEL ${iso}`);
      expect(await code(f.db.query("TRUNCATE sessions.upload_resets"))).toBe("40001");
      await f.db.exec("ROLLBACK");
    }
  });

  test("a covered truncate gap: a cutoff before a permitted truncate is not covered", async () => {
    const f = await fixture();
    const before = (await one(f, "SELECT clock_timestamp()::text t")).t as string;
    await f.db.query("TRUNCATE sessions.upload_resets");
    expect((await one(f, "SELECT sessions.ingest_org_erasure_history_covers($1::timestamptz) c", [before])).c).toBe(false);
  });

  test("an explicit empty-window release needs zero history since the install", async () => {
    const f = await fixture();
    const e = await toAttempt(f);
    await deletion(f, OTHER, "s-other");
    expect(await code(release(f, e, 1, ORG, "erasure_window_empty"))).toBe("55000"); // count mismatch
    await deletion(f, ORG, "s-1");
    expect(await code(release(f, e, 1, ORG, "erasure_window_empty"))).toBe("55000"); // matching, but not empty
    expect(await code(release(f, e, 1, ORG, "erasures_replayed"))).toBe("ok");
  });

  test("the history, coverage and bindings are immutable; the runtime roles reach none of it", async () => {
    const f = await fixture();
    await toAttempt(f); // a binding row
    await deletion(f, ORG, "s-1"); // a history row; coverage has its 'started' row
    for (const t of ["ingest_org_erasure_history", "ingest_org_erasure_history_coverage", "ingest_org_erasure_history_bindings"]) {
      expect((await one(f, `SELECT count(*)::int n FROM sessions.${t}`)).n).toBeGreaterThan(0);
      const col = t.endsWith("bindings") ? "fence_at" : "at";
      expect(await code(f.db.query(`UPDATE sessions.${t} SET ${col}=${col}`))).toBe("55000");
      expect(await code(f.db.query(`DELETE FROM sessions.${t}`))).toBe("55000");
      expect(await code(f.db.query(`TRUNCATE sessions.${t}`))).toBe("55000");
    }
    for (const role of ["bulls96_session_ingest", "bulls96_session_ingest_seeder"]) {
      for (const t of ["ingest_org_erasure_history", "ingest_org_erasure_history_coverage", "ingest_org_erasure_history_bindings"])
        expect((await one(f, `SELECT has_table_privilege($1,'sessions.${t}','SELECT,INSERT,UPDATE,DELETE') p`, [role])).p).toBe(false);
      expect((await one(f, "SELECT has_function_privilege($1,'sessions.ingest_org_erasure_history_bind(text,uuid,text,bigint,text)','EXECUTE') p", [role])).p).toBe(false);
    }
  });
});
