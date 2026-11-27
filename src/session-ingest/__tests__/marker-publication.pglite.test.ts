/**
 * marker-publication.ts against the REAL slot 062320 (and 062340) functions on PGlite with every migration, through
 * scripts/backlog-marker.ts pgMarkerSession (the tool's own transaction shapes): the marker attempt's evidence, the
 * SHARED key in a READ COMMITTED READ ONLY transaction, an unsealed or absent hold, retry (idempotent attempt, adopt),
 * a release committed between the attempt and the guard, and an earlier epoch's attempt blocking adoption. Single
 * connection: the two-connection lock ordering is the in-memory model's (marker-publication.test.ts) and the
 * PostgreSQL suite's for the hold itself.
 */
import { afterEach, expect, setDefaultTimeout, test } from "bun:test";
import { evidenceHash } from "../authority-evidence.ts";
import { orgFenceAuthorityDoc } from "../backlog-authority.ts";
import { orgFenceAuthorityKey, validOrgFenceAuthority } from "../ledger-s3.ts";
import { MARKER_SQL, markerRun, type MarkerRunDeps, type MarkerSqlSession } from "../marker-publication.ts";
import { objectKey, readAll } from "../../shared/vendor.ts";
import { pgMarkerSession } from "../../../scripts/backlog-marker.ts";
import { authorityFixture } from "./privacy-authority-support.ts";
import { scratchStore } from "./support.ts";
import { CATCHUP_EVIDENCE, ROOT, RUNS, SHA, baseline, bound, evidenceReport, putJson, scopeIdOf, seedSweeper, spec } from "./marker-support.ts";

// The database's clock stamps marker_attempted_at, so the run's clock is the real one here (adoption compares them).
const NOW = Date.now();

setDefaultTimeout(240_000);
const open: (() => unknown)[] = [];
afterEach(async () => { for (const c of open.splice(0).reverse()) await c(); });
const ORG = "5e5e5e5e-0000-4000-8000-000000000001";
const SEAL_EVIDENCE = "b".repeat(64);

async function fixture() {
  const f = await authorityFixture();
  open.push(f.close);
  await f.seal();
  await f.db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text)");
  for (const slot of ["upstream:20261105062320_ingest_org_admission_fences.sql", "upstream:20261105062340_ingest_org_erasure_history.sql"])
    await f.db.query("INSERT INTO sessions.__96bulls_migrations(name) VALUES($1) ON CONFLICT DO NOTHING", [slot]);
  const s = scratchStore();
  open.push(s.done);
  await seedSweeper(s.store, [ORG], { [ORG]: NOW - 3_600_000 }, spec([ORG], { observedAtMs: NOW - 60_000 }));
  const one = async (sql: string, p: unknown[] = []) => (await f.db.query<Record<string, unknown>>(sql, p)).rows[0]!;
  const install = async () => (await one("SELECT sessions.ingest_org_admission_hold_install($1, gen_random_uuid())::text AS e", [ORG])).e as string;
  const seal = async (e: string) => {
    await f.db.query("SELECT pg_sleep(0.002)");
    await f.db.query("SELECT sessions.ingest_org_admission_hold_seal($1,$2::uuid,$3)", [ORG, e, SEAL_EVIDENCE]);
  };
  const bind = (e: string) => f.db.query(`SELECT sessions.ingest_org_erasure_history_bind($1,$2::uuid,'full-run-1',(SELECT floor(extract(epoch FROM installed_at) * 1000)::bigint + 1
    FROM sessions.ingest_org_admission_holds WHERE epoch=$2::uuid),$3)`, [ORG, e, SEAL_EVIDENCE]);
  const release = async (e: string) => {
    const st = (await one("SELECT sessions.ingest_org_admission_hold_status($1) s", [ORG])).s as { erasures: number };
    await f.db.query("SELECT sessions.ingest_org_admission_hold_release($1,$2::uuid,'bridge_complete',$3,NULL,$4)", [ORG, e, "d".repeat(64), st.erasures]);
  };
  const session = pgMarkerSession(f.db as never);
  const deps = (mode: "dry_run" | "execute", over: Partial<MarkerRunDeps> = {}, sess: MarkerSqlSession = session): MarkerRunDeps => ({
    mode, orgs: [ORG], sha: SHA, scopeId: scopeIdOf([ORG]), runs: RUNS, sources: {} as never, baseline: baseline([ORG]), bound: bound({}, [ORG]),
    store: s.store, keyRoot: ROOT, bucket: "transcripts", ledgerBucket: null, withSession: (fn) => fn(sess), now: () => NOW,
    evidence: (async () => evidenceReport([ORG], NOW - 1000)) as never, concurrency: 1, ...over });
  const run = (mode: "dry_run" | "execute", over: Partial<MarkerRunDeps> = {}, sess?: MarkerSqlSession) => markerRun(deps(mode, over, sess));
  const waveOf = (d: Awaited<ReturnType<typeof run>>) => ({ id: d.wave_id!, orgs: d.per_org.filter((p) => p.verdict === "ready").map((p) => ({ orgHash: p.org_hash, epochHash: p.epoch_hash! })) });
  const events = async (e: string) => (await f.db.query<{ kind: string; evidence_sha256: string | null }>(
    "SELECT kind, evidence_sha256 FROM sessions.ingest_org_admission_hold_events WHERE epoch=$1::uuid ORDER BY at", [e])).rows;
  const state = async (e: string) => (await one("SELECT state FROM sessions.ingest_org_admission_holds WHERE epoch=$1::uuid", [e])).state;
  const markerDoc = async () => { try { return JSON.parse(new TextDecoder().decode(await readAll((await s.store.getObject(objectKey(orgFenceAuthorityKey(ROOT, ORG)))).body))); } catch { return null; } };
  return { f, s, install, seal, bind, release, session, run, waveOf, events, state, markerDoc, one };
}

test("real 062320: absent, then held, then sealed; dry reads only; execute commits the attempt with the catch-up's evidence, then publishes under the key", async () => {
  const x = await fixture();
  expect((await x.run("dry_run")).per_org[0]).toMatchObject({ refused: "hold_absent", hold: "absent" });
  const e = await x.install();
  expect((await x.run("dry_run")).per_org[0]).toMatchObject({ refused: "hold_not_sealed", hold: "held" });
  await x.seal(e);
  const d = await x.run("dry_run");
  expect(d.per_org[0]).toMatchObject({ verdict: "ready", hold: "sealed", marker: "absent", attempted: false });
  expect(await x.state(e)).toBe("sealed");
  const r = await x.run("execute", { wave: x.waveOf(d) });
  expect(r.per_org[0]).toMatchObject({ verdict: "published", marker: "written", attempted: true, hold: "marker_attempted" });
  expect(await x.state(e)).toBe("marker_attempted");
  expect((await x.events(e)).map((v) => [v.kind, v.evidence_sha256])).toEqual([["installed", null], ["sealed", SEAL_EVIDENCE], ["marker_attempted", CATCHUP_EVIDENCE]]);
  const m = await x.markerDoc();
  expect(validOrgFenceAuthority(m)).toBe(true);
  expect(m.orgId).toBe(ORG);
  // While marker_attempted the org's admission is refused by the hold itself (the enforcement the marker relies on).
  await x.f.db.exec("BEGIN ISOLATION LEVEL READ COMMITTED");
  await expect(x.f.db.query("SELECT sessions.ingest_org_admission_hold_check($1,'admission')", [ORG])).rejects.toMatchObject({ code: "RH042" });
  await x.f.db.exec("ROLLBACK");
  // Retry: the attempt is idempotent (still one event), the marker adopted, never rewritten.
  const again = await x.run("execute", { wave: x.waveOf(d), now: () => NOW + 30_000, evidence: (async () => evidenceReport([ORG], NOW + 29_000)) as never });
  expect(again.per_org[0]).toMatchObject({ verdict: "adopted", marker: "adopted" });
  expect((await x.events(e)).filter((v) => v.kind === "marker_attempted")).toHaveLength(1);
  expect(await x.markerDoc()).toEqual(m);
});

test("real 062320: the guard takes the key SHARED in a READ ONLY READ COMMITTED transaction; erasure-class callers are never refused", async () => {
  const x = await fixture();
  const e = await x.install();
  await x.seal(e);
  const out = await x.session.guarded(ORG, async (inTx) => {
    expect((await inTx("SELECT current_setting('transaction_read_only') AS ro, current_setting('transaction_isolation') AS iso", []))[0]).toEqual({ ro: "on", iso: "read committed" });
    // The shared advisory lock on the org key is held by this transaction now.
    const held = await inTx(`SELECT count(*)::int AS n FROM pg_locks WHERE locktype = 'advisory' AND mode = 'ShareLock' AND pid = pg_backend_pid()`, []);
    expect(held[0]!.n).toBeGreaterThanOrEqual(1);
    const st = await inTx(MARKER_SQL.status(ORG).text, MARKER_SQL.status(ORG).params);
    return (st[0]!.status as { hold: { state: string } }).hold.state;
  });
  expect(out).toBe("sealed");
  // And a write inside the guard is refused (READ ONLY).
  await expect(x.session.guarded(ORG, async (inTx) => inTx("UPDATE sessions.ingest_org_admission_holds SET state = state", []))).rejects.toBeTruthy();
  expect(await x.state(e)).toBe("sealed");
});

test("real 062320: a release committed between the attempt and the guard refuses with nothing written", async () => {
  const x = await fixture();
  const e = await x.install();
  await x.seal(e);
  await x.bind(e);
  const d = await x.run("dry_run");
  const racing: MarkerSqlSession = { ...x.session, write: async (t, p) => { const rows = await x.session.write(t, p); await x.release(e); return rows; } };
  const r = await x.run("execute", { wave: x.waveOf(d) }, racing);
  expect(r.per_org[0]).toMatchObject({ verdict: "incomplete", refused: "hold_released_before_put", attempted: true, hold: "released" });
  expect(await x.markerDoc()).toBeNull();
  expect(await x.state(e)).toBe("released");
});

test("real 062320: an earlier epoch that attempted a marker blocks adoption by a later epoch (conflict, never overwritten)", async () => {
  const x = await fixture();
  const e1 = await x.install();
  await x.seal(e1);
  await x.bind(e1);
  await x.f.db.query("SELECT sessions.ingest_org_admission_hold_marker_attempt($1,$2::uuid,$3)", [ORG, e1, CATCHUP_EVIDENCE]);
  await x.release(e1);
  const e2 = await x.install();
  await x.seal(e2);
  // A marker left by epoch 1's cutover (newer than epoch 2's attempt would be: the timestamp alone cannot tell).
  await putJson(x.s.store, orgFenceAuthorityKey(ROOT, ORG), orgFenceAuthorityDoc(ORG, NOW));
  const d = await x.run("dry_run");
  expect(d.per_org[0]).toMatchObject({ refused: "marker_present", marker: "present" });
  const s = MARKER_SQL.otherAttempts(ORG, e2);
  expect((await x.session.read(s.text, s.params))[0]).toEqual({ n: 1 });
  // An execute pinned to epoch 2 (as a dry before the marker appeared would have) refuses before any attempt.
  const { epochHash } = await import("../erasure-window.ts");
  const { waveId } = await import("../org-wave.ts");
  const orgs = [{ orgHash: evidenceHash("org", ORG), epochHash: epochHash(e2) }];
  const r = await x.run("execute", { wave: { id: waveId({ sha: SHA, scope: scopeIdOf([ORG]), runs: RUNS, orgs }), orgs } });
  expect(r.per_org[0]).toMatchObject({ refused: "marker_conflict", attempted: false });
  expect(await x.state(e2)).toBe("sealed");
});
