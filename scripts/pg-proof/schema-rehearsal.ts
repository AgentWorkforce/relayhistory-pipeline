/**
 * DESTRUCTIVE, DISPOSABLE-CI-ONLY schema release rehearsal. It runs only inside
 * .github/workflows/rehearse-session-schema.yml against the job's own throwaway PostgreSQL
 * service on 127.0.0.1:5432 (no secrets, no environment, no production database), and
 * refuses anywhere else. It is prerequisite proof only and authorizes nothing: no apply,
 * deploy or rebind of any real database follows from a PASS
 * (runbooks/schema-release-rehearsal.md).
 *
 * It replays the production release (scripts/schema-release-database.ts) with the
 * release checkout's own modules, imported from the working directory (the release sha):
 *   R0 seed      the database the release starts from: exactly the schema steps of the
 *                commit that landed the baseline, applied by runMigrations with a release
 *                target binding, marker PASS at that commit's generation, and the
 *                previous release's event-trigger body (a stale reviewed wrapper)
 *   R1 plan      inspectRelease (READ ONLY; refuses function_acl_drift, ledger_gap, ...),
 *                migrationTargetSplit checked against an independent window, the target
 *                commit found on the release sha's first-parent history at the dispatched
 *                generation, validateReleasePlan on the plan exactly as production builds it
 *   R2 boundary  the planned boundary is safe and still the previous body (prosrc not exact)
 *   R4 apply     the production apply order (anchor, inspection, ledger, boundary, window,
 *                generation, manifest, runMigrations with beforeWrite, read-backs)
 *   R3 boundary  the boundary refreshed under the migrator lock is the pinned body (runs
 *                after R4: it reads the refresh R4's beforeWrite performed)
 *   R5 read-back ledger = seed + approved window, nothing held or later applied, marker
 *                PASS at the dispatched generation
 *   R6 ingest    the session-ingest Worker vars during (A,B overlap) and after (B only)
 *   R7 fence     old/next/both Workers around the owner rebind (needs the overlap, 044030)
 *   R8 read      the 047010 mixed-claim shape 17/170/6939/177 on real PostgreSQL
 * Injections (REHEARSAL_INJECT) prove each refusal: ledger-gap, boundary-drift, late-step,
 * wrong-generation, stale-plan. Every stage writes rehearsal-out/<stage>.json; the first
 * FAIL stops the run. REHEARSAL_EXPECT_REFUSAL (`<stage>:<code>`) marks a known-negative
 * row: the run then passes only if exactly that stage FAILs with exactly that code.
 * The driver prints only `stage status code` lines and the verdict; the release
 * checkout's own modules (migrate.ts) also log their progress lines to stdout. The job
 * has no secrets, and evidence is screened (resultRecord) before it is written.
 */
import pg from "pg";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  disposableEnvRefusal,
  errorCode,
  expectedWindow,
  flipHex,
  INJECTION_REFUSAL,
  laneOf,
  legacyForecast,
  MAX_POSTMASTER_AGE_SECONDS,
  RehearsalRefusal,
  rehearsalPassed,
  rehearsalRefuse,
  resultRecord,
  sameNameOid,
  seedOrder,
  seedSet,
  validateInputs,
  type RehearsalInputs,
  type StageResult,
  type StageStatus,
} from "./schema-rehearsal-core.ts";

const ROOT = process.cwd();
const OUT = join(ROOT, "rehearsal-out");
const BOUNDARY = "global-default-revoke" as const;
/** The schema lanes as release-target.ts reads them (checked against RELEASE_TREE). */
const LANE_DIRS: Record<string, string> = { "upstream:": "supabase/migrations", "96bulls:": "src/hoopdb/migrations" };
/** Synthetic owner-fixture values (as src/session-ingest/__tests__/generation-overlap.postgres.test.ts). */
const EPOCH = "3".repeat(64);
const IDENTITY = "c".repeat(64);
const PRIVACY_BASELINE = "a".repeat(64);
const APPROVAL = "9".repeat(64);
const READ_047010 = /^upstream:20261105047010_/;
const PG = { host: "127.0.0.1", port: 5432, database: "postgres" } as const;

const results: StageResult[] = [];
class Stop extends Error {}
/** The stage the run is in (or about to start): a refusal outside stage() is recorded against it. */
let current = "R0";

async function emit(stage: string, status: StageStatus, evidence: Record<string, unknown> = {}, code?: string) {
  let r: StageResult;
  try {
    r = resultRecord(stage, status, evidence, code);
  } catch (e) {
    r = resultRecord(stage, "FAIL", {}, errorCode(e));
  }
  results.push(r);
  await writeFile(join(OUT, `${r.stage}.json`), JSON.stringify(r, null, 2) + "\n");
  console.log(`${r.stage} ${r.status}${r.code ? " " + r.code : ""}`);
  if (r.status === "FAIL") throw new Stop();
}
async function stage<T extends Record<string, unknown>>(name: string, fn: () => Promise<T>): Promise<T> {
  current = name;
  let evidence: T;
  try {
    evidence = await fn();
  } catch (e) {
    if (e instanceof Stop) throw e;
    await emit(name, "FAIL", e instanceof RehearsalRefusal ? e.detail : {}, errorCode(e));
    throw new Stop();
  }
  await emit(name, "PASS", evidence);
  return evidence;
}
/** PASS only on the exact refusal code; anything else (or no refusal) is a mismatch. */
async function expectRefusal(expected: string, fn: () => Promise<unknown> | unknown) {
  let got: string | null = null;
  try {
    await fn();
  } catch (e) {
    got = errorCode(e);
  }
  if (got !== expected) rehearsalRefuse("rehearsal_refusal_mismatch", { expected, got });
  return { refused: got };
}

function git(args: string[]): string {
  const r = spawnSync("git", args, {
    cwd: ROOT,
    encoding: "utf8",
    maxBuffer: 256 * 1024 * 1024,
    env: { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "" },
  });
  if (r.status !== 0) rehearsalRefuse("rehearsal_git", { command: args[0] });
  return r.stdout;
}
/** name -> blob oid of both lanes' *.sql at a commit, lane order, each lane sorted. */
function laneTree(sha: string): Map<string, string> {
  const m = new Map<string, string>();
  for (const [prefix, dir] of Object.entries(LANE_DIRS)) {
    const rows: [string, string][] = [];
    for (const ent of git(["ls-tree", "-z", sha, dir + "/"]).split("\0")) {
      if (!ent) continue;
      const tab = ent.indexOf("\t");
      const path = ent.slice(tab + 1);
      if (path.endsWith(".sql")) rows.push([prefix + path.slice(dir.length + 1), ent.slice(0, tab).split(" ")[2]!]);
    }
    for (const [n, oid] of rows.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) m.set(n, oid);
  }
  return m;
}
const stepPath = (name: string) => `${LANE_DIRS[laneOf(name)]}/${name.slice(laneOf(name).length)}`;
/**
 * The first-parent commit of the release sha that added a step's file (the oldest such).
 * --diff-merges=first-parent turns on patch output (git 2.43), so --no-patch is required
 * and only full commit ids are kept.
 */
export function landingCommit(sha: string, name: string): string {
  const out = git([
    "log",
    "--first-parent",
    "--diff-merges=first-parent",
    "--no-patch",
    "--diff-filter=A",
    "--format=%H",
    sha,
    "--",
    stepPath(name),
  ]);
  const c = out
    .split("\n")
    .filter((l) => /^[0-9a-f]{40}$/.test(l))
    .at(-1);
  if (!c) rehearsalRefuse("rehearsal_landing_missing", { step: name });
  return c;
}
/**
 * The target commit: on the release sha's first-parent history (newest first, down to the
 * commit that landed the target), whose two schema lanes are exactly the release tree
 * without `held` (by name and blob), bound by the release checkout's own
 * releaseTargetBinding, at the dispatched generation. No such commit:
 * rehearsal_target_anchor_missing; only other generations: rehearsal_generation_mismatch.
 */
export async function discoverAnchor(x: {
  RT: { releaseTargetBinding: (i: Record<string, unknown>) => any };
  M: { schemaStepDigests: (held: readonly string[]) => Promise<[string, string][]> };
  sha: string;
  target: string;
  generation: string;
  held: readonly string[];
}) {
  const prefix = await x.M.schemaStepDigests(x.held);
  const names = new Set(prefix.map(([n]) => n));
  const want = new Map([...laneTree(x.sha)].filter(([n]) => names.has(n)));
  const stop = landingCommit(x.sha, x.target);
  const found: any[] = [];
  let scanned = 0;
  for (const c of git(["rev-list", "--first-parent", "--max-count=3000", x.sha]).trim().split("\n")) {
    scanned++;
    if (sameNameOid(laneTree(c), want)) {
      try {
        found.push(x.RT.releaseTargetBinding({ repo: ROOT, releaseSha: x.sha, targetSha: c, target: x.target, prefix }));
      } catch {}
    }
    if (c === stop) break;
  }
  const hit = found.find((a) => a.targetGeneration === x.generation);
  if (!hit)
    found.length
      ? rehearsalRefuse("rehearsal_generation_mismatch", { candidates: [...new Set(found.map((a) => a.targetGeneration))], scanned })
      : rehearsalRefuse("rehearsal_target_anchor_missing", { scanned });
  return hit;
}
const sameNames = (a: readonly string[], b: readonly string[]) => a.length === b.length && a.every((n, i) => n === b[i]);
const psql = (text: string) => text.split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const mod = (p: string) => import(join(ROOT, p));

async function main() {
  await mkdir(OUT, { recursive: true });
  let inputs: RehearsalInputs | undefined;
  let pgMajor = "";
  try {
    await run((i, m) => {
      inputs = i;
      pgMajor = m;
    });
  } catch (e) {
    if (!(e instanceof Stop)) {
      // A refusal outside stage(): recorded against the stage the run is in or about to
      // start (R0 for the guards); never over a stage that already has a record.
      const name = results.some((r) => r.stage === current) ? `${current}-after` : current;
      try {
        await emit(name, "FAIL", e instanceof RehearsalRefusal ? e.detail : {}, errorCode(e));
      } catch {}
    }
  }
  const passed = rehearsalPassed(results, inputs?.expect ?? null);
  await writeFile(
    join(OUT, "summary.json"),
    JSON.stringify(
      {
        sha: inputs?.sha ?? null,
        pg_major: pgMajor || null,
        baseline: inputs?.baseline ?? null,
        target: inputs?.target ?? null,
        generation: inputs?.generation ?? null,
        inject: inputs?.inject ?? null,
        expect_refusal: inputs?.expect ? `${inputs.expect.stage}:${inputs.expect.code}` : null,
        stages: results.map((r) => ({ stage: r.stage, status: r.status, ...(r.code ? { code: r.code } : {}) })),
        passed,
      },
      null,
      2,
    ) + "\n",
  );
  console.log(
    passed ? (inputs?.expect ? "rehearsal PASS (expected refusal observed)" : "rehearsal PASS") : "rehearsal FAIL",
  );
  process.exit(passed ? 0 : 1);
}

async function run(report: (i: RehearsalInputs, pgMajor: string) => void) {
  // ---- Guards: disposable CI only, exact inputs, the exact clean full checkout.
  const envRefusal = disposableEnvRefusal(process.env);
  if (envRefusal) rehearsalRefuse(envRefusal);
  const inputs = validateInputs({
    sha: process.env.REHEARSAL_SHA,
    baseline: process.env.REHEARSAL_BASELINE,
    target: process.env.REHEARSAL_TARGET,
    generation: process.env.REHEARSAL_GENERATION,
    inject: process.env.REHEARSAL_INJECT,
    expect: process.env.REHEARSAL_EXPECT_REFUSAL,
  });
  const pgMajor = process.env.REHEARSAL_PG_MAJOR ?? "";
  report(inputs, pgMajor);
  if (!/^(15|16|17)$/.test(pgMajor)) rehearsalRefuse("rehearsal_pg_major");
  const { sha, baseline, target, inject } = inputs;
  const B = inputs.generation;
  if (git(["rev-parse", "HEAD"]).trim() !== sha) rehearsalRefuse("rehearsal_checkout_sha");
  if (spawnSync("git", ["diff", "--quiet", "HEAD"], { cwd: ROOT }).status !== 0) rehearsalRefuse("rehearsal_checkout_dirty");
  if (git(["rev-parse", "--is-shallow-repository"]).trim() !== "false") rehearsalRefuse("rehearsal_checkout_shallow");
  const RUN_ID = process.env.GITHUB_RUN_ID || "1";

  // ---- The release checkout's own production modules.
  const M = await mod("src/hoopdb/migrate.ts");
  const hasApi =
    existsSync(join(ROOT, "src/hoopdb/release-target.ts")) &&
    existsSync(join(ROOT, "src/hoopdb/schema-release-boundary.ts")) &&
    typeof M.migrationTargetSplit === "function" &&
    typeof M.schemaStepDigests === "function";
  if (!hasApi) {
    // A pre-#534 tree (no release-target API) can only run a legacy full apply: report exactly what it would apply.
    const tree = [...laneTree(sha).keys()];
    const landed = new Set(laneTree(landingCommit(sha, baseline)).keys());
    const f = legacyForecast(tree, landed, target);
    rehearsalRefuse("rehearsal_release_api_missing", {
      fullApplyPending: f.fullApplyPending,
      beyondTarget: f.beyondTarget,
    });
  }
  const RT = await mod("src/hoopdb/release-target.ts");
  const C = await mod("src/hoopdb/schema-release-contract.ts");
  const I = await mod("src/hoopdb/schema-release-inspect.ts");
  const BD = await mod("src/hoopdb/schema-release-boundary.ts");
  const D = await mod("scripts/schema-release-database.ts");
  const O = await mod("src/hoopdb/owner-operator-apply.ts");
  const G = await mod("src/hoopdb/migrate-guards.ts");
  const DB = await mod("src/shared/db.ts");
  const { SESSION_BOOTSTRAP_FILES } = await mod("src/hoopdb/session-bootstrap-chain.ts");
  // The anchor helpers are production's own (exported with this rehearsal); a release
  // sha without them cannot be rehearsed against production's anchor code.
  if (
    typeof D.validateReleasePlan !== "function" ||
    typeof BD.refreshReleaseBoundary !== "function" ||
    typeof D.targetAnchor !== "function" ||
    typeof D.sameAnchor !== "function"
  )
    rehearsalRefuse("rehearsal_release_api_missing", {});
  const lanes = (M.RELEASE_TREE.lanes as { prefix: string; dir: string }[]).map((l) => [l.prefix, l.dir]);
  if (JSON.stringify(lanes) !== JSON.stringify(Object.entries(LANE_DIRS))) rehearsalRefuse("rehearsal_lane_layout");
  const P = C.SCHEMA_RELEASE;
  const profile = I.releaseProfile(BOUNDARY);
  const fullGen: string = await M.generation();
  const tree: string[] = await M.expectedMigrationNames();
  if (!sameNames(tree, [...laneTree(sha).keys()])) rehearsalRefuse("rehearsal_checkout_tree");
  const read = (p: string) => readFile(join(ROOT, p), "utf8");

  // ---- Connections: the job's disposable service only.
  const admin = new pg.Client({ ...PG, user: "supabase_admin", application_name: "96bulls-schema-rehearsal-admin" });
  admin.on("error", () => {});
  await admin.connect();
  const ownerClient = () => {
    const c = new pg.Client({
      ...PG,
      user: P.owner,
      application_name: "96bulls-schema-rehearsal-owner",
      options: "-c statement_timeout=300000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=60000",
    });
    c.on("error", () => {});
    return c;
  };
  const env = (
    await admin.query(
      "SELECT current_setting('server_version_num')::int/10000 AS major, r.rolsuper, current_database() AS db, to_regnamespace('sessions') IS NULL AS no_sessions, NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=$1) AS no_owner, extract(epoch FROM now() - pg_postmaster_start_time())::int AS age FROM pg_roles r WHERE r.rolname=current_user",
      [P.owner],
    )
  ).rows[0];
  if (String(env.major) !== pgMajor) rehearsalRefuse("rehearsal_pg_major_mismatch", { server: env.major });
  if (env.rolsuper !== true || env.db !== P.database) rehearsalRefuse("rehearsal_admin_identity");
  if (!env.no_sessions || !env.no_owner) rehearsalRefuse("rehearsal_database_not_fresh");
  // The job's service container starts with the job: a server up for longer is not it.
  if (!(env.age >= 0 && env.age <= MAX_POSTMASTER_AGE_SECONDS)) rehearsalRefuse("rehearsal_database_not_fresh", { postmasterAge: "stale" });

  const schemaLedger = async () =>
    new Map(
      (await admin.query(`SELECT name, sha256 FROM ${M.MIGRATION_LEDGER} ORDER BY name`)).rows
        .filter((r: { name: string }) => r.name.startsWith("upstream:") || r.name.startsWith("96bulls:"))
        .map((r: { name: string; sha256: string }) => [r.name, r.sha256]),
    );
  const ledgerDigest = async () => JSON.stringify([...(await admin.query(`SELECT name, sha256 FROM ${M.MIGRATION_LEDGER} ORDER BY name`)).rows]);
  const marker = async () =>
    (await admin.query(`SELECT generation, status FROM ${DB.ROLE_VALIDATION_TABLE} WHERE name=$1`, [DB.ROLE_VALIDATION_NAME])).rows[0] as
      | { generation: string; status: string }
      | undefined;
  const boundaryDigest = async () => BD.releaseBoundaryDigest((await G.functionBoundaryEvidence(admin, BOUNDARY, "supabase", P.runtime)).event_trigger);

  // ---- R0: the database the release starts from.
  const owner = ownerClient();
  const ownerOps = ownerClient();
  let seed!: ReturnType<typeof seedSet>;
  let A = "";
  await stage("R0", async () => {
    await admin.query(
      `CREATE ROLE ${P.owner} LOGIN CREATEROLE CREATEDB BYPASSRLS REPLICATION; ALTER ROLE ${P.owner} SUPERUSER; GRANT CREATE ON DATABASE ${P.database} TO ${P.owner}; SET ROLE ${P.owner}; CREATE SCHEMA sessions; GRANT CREATE ON SCHEMA public TO ${P.owner}; REVOKE TEMPORARY ON DATABASE ${P.database} FROM PUBLIC; ALTER DEFAULT PRIVILEGES FOR ROLE ${P.owner} REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;`,
    );
    await admin.query(psql(await read("supabase/tests/supabase_shim.sql")));
    for (const f of SESSION_BOOTSTRAP_FILES) await admin.query(psql(await read(f)));
    await admin.query(
      `DO $$BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='${P.runtime}') THEN CREATE ROLE ${P.runtime} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION; END IF; END $$`,
    );
    await admin.query("CREATE EXTENSION IF NOT EXISTS pg_trgm; CREATE EXTENSION IF NOT EXISTS btree_gin;");
    await admin.query(
      psql(await read("supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql")).replaceAll(":'runtime_role'", `'${P.runtime}'`),
    );
    await admin.query(
      `RESET ROLE; ALTER ROLE ${P.owner} NOSUPERUSER; ALTER DEFAULT PRIVILEGES FOR ROLE supabase_admin REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC`,
    );
    await owner.connect();
    await ownerOps.connect();
    const landing = landingCommit(sha, baseline);
    const landed = (RT.treeSchemaSteps(ROOT, landing) as [string, string][]).map(([n]) => n);
    seed = seedSet({ landing: landed, tree, baseline, target });
    const steps = await M.plan(profile);
    const byName = new Map(steps.map((s: { name: string }) => [s.name, s]));
    const ordered = [
      ...seedOrder(seed.seed, seed.unseeded).map((n) => byName.get(n) ?? rehearsalRefuse("rehearsal_seed_step", { step: n })),
      ...steps.filter((s: { name: string }) => s.name.startsWith("verify:")),
    ];
    const anchor = RT.releaseTargetBinding({
      repo: ROOT,
      releaseSha: sha,
      targetSha: landing,
      target: seed.lastSeed,
      prefix: await M.schemaStepDigests(seed.unseeded),
    });
    A = anchor.targetGeneration;
    if (A === B) rehearsalRefuse("rehearsal_seed_generation_is_target");
    await M.runMigrations(owner, {
      profile,
      steps: ordered,
      target: seed.lastSeed,
      expectedPending: seed.seed,
      targetGeneration: A,
      targetBinding: { targetSha: landing, manifestDigest: anchor.manifestDigest, verifiers: anchor.verifiers },
    });
    const ledger = await schemaLedger();
    if (!sameNames([...ledger.keys()].sort(), [...seed.seed].sort())) rehearsalRefuse("rehearsal_seed_ledger");
    const m = await marker();
    if (m?.status !== "pass" || m.generation !== A) rehearsalRefuse("rehearsal_seed_marker");
    // The previous release's event-trigger body: the reviewed wrapper with a different source.
    const wrapper = await O.wrapperSource();
    const delimiter = "$owner_apply_boundary$;";
    if (!wrapper.sql.includes(delimiter)) rehearsalRefuse("rehearsal_wrapper_delimiter");
    await owner.query(wrapper.sql.replace(delimiter, "\n-- fixture stale source\n" + delimiter));
    return { landingSha: landing, seedGeneration: A, seeded: seed.seed.length, unseeded: seed.unseeded.length, lastSeed: seed.lastSeed };
  });

  // ---- Ingest fixtures (R7): the real Worker ledger and deliver path as bulls96_session_ingest.
  const rt = new pg.Pool({ ...PG, user: "supabase_admin", max: 8, application_name: "96bulls-schema-rehearsal-ingest" });
  rt.on("error", () => {});
  const ingest = async () => {
    const { sqlLedger } = await mod("src/session-ingest/ledger-sql.ts");
    const { deliverBatch } = await mod("src/session-ingest/deliver.ts");
    const { prepareDelivery, deliveryAccount } = await mod("src/session-ingest/vendor/delivery/prepare.ts");
    const S = await ingestFixtures(deliveryAccount);
    const runAs = async (t: string, p: unknown[] = []) => {
      const c = await rt.connect();
      try {
        await c.query("SET ROLE bulls96_session_ingest");
        return await c.query(t, p);
      } finally {
        await c.query("RESET ROLE").catch(() => {});
        c.release();
      }
    };
    const fs = S.scratchStore();
    const worker = (expected: string, accept?: string[]) => ({
      ledger: sqlLedger(runAs, { acceptanceMode: "buffered", expectedGeneration: expected, ...(accept ? { acceptGenerations: accept } : {}) }),
      acceptanceMode: "buffered" as const,
      target: { store: fs.store, keyRoot: "96bulls" },
    });
    let n = 0;
    const upload = async (deps: ReturnType<typeof worker>) => {
      n++;
      const raw = await S.batch([S.record({ record_id: "rehearsal-r" + n })], { batch_id: "rehearsal-b" + n });
      const p = await prepareDelivery(S.TENANT, raw);
      const custody = (
        await runAs("SELECT sessions.ingest_frozen_source_witness($1::jsonb) AS r", [
          JSON.stringify({
            kind: "batch",
            tenant: S.TENANT,
            originId: p.originId,
            batchId: p.batchId,
            batchDigest: p.batchDigest,
            originalRecordCount: raw.records.length,
            candidates: p.records.map((r: any, i: number) => ({
              preparedOrd: i,
              recordId: r.record_id,
              revision: String(r.revision),
              revisionId: r.revision_id,
              candidateDigest: r.digest,
              source: r.source,
              sessionId: r.session_id,
            })),
          }),
        ])
      ).rows[0].r;
      return deliverBatch({ ...deps, custody }, S.TENANT, raw);
    };
    const ok = async (label: string, deps: ReturnType<typeof worker>) => {
      const r = await upload(deps);
      if (!/^[0-9a-f]{64}$/.test(r?.custodyAck?.sourceDigest ?? "")) rehearsalRefuse("rehearsal_ingest_not_admitted", { worker: label });
    };
    const refusedCode = async (deps: ReturnType<typeof worker>) => {
      try {
        await upload(deps);
        return null;
      } catch (e) {
        const x = e as { cause_code?: string; code?: string };
        return x.cause_code ?? x.code ?? "unknown";
      }
    };
    return { worker, ok, refusedCode, fs, deliveryAccount, S };
  };
  const ownerFn = async (fn: string, p: unknown) => (await ownerOps.query(`SELECT sessions.${fn}($1::jsonb) AS r`, [JSON.stringify(p)])).rows[0].r;

  let fence: Awaited<ReturnType<typeof ingest>> | undefined;
  let overlapOpen = false;
  if (inject === "none") {
    const OV = existsSync(join(ROOT, "src/hoopdb/ingest-generation-overlap.ts")) ? await mod("src/hoopdb/ingest-generation-overlap.ts") : undefined;
    if (OV && (await OV.overlapInstalled(ownerOps))) {
      await stage("R7-pre", async () => {
        await ownerFn("ingest_lazy_policy_set", {
          baselineMode: "lazy-current",
          generation: A,
          sourceIdentitySha256: IDENTITY,
          controlEpoch: EPOCH,
          privacyBaselineSha256: PRIVACY_BASELINE,
          retentionContractSha256: "b".repeat(64),
          sourceProofSha256: "d".repeat(64),
          validUntil: new Date(Date.now() + 3600000).toISOString(),
          issuer: "relayhistory-cloud",
        });
        await ownerFn("ingest_receipt_seat_initialize", {
          protocol: "delivery-acceptance-floor-v1",
          controlEpoch: EPOCH,
          capturedAt: "2026-11-04T00:00:00.000000Z",
          sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({
            sequenceRegclass,
            sequenceOid: String(i + 1),
            allocatedHighWater: "9007199254740993",
            isCalled: true,
            incrementBy: "1",
            cycle: false,
          })),
          allocatedHighWater: "9007199254740993",
          generation: A,
          sourceIdentitySha256: IDENTITY,
          sourceProofSha256: "d".repeat(64),
        });
        await ownerOps.query(
          "INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')",
          [IDENTITY, EPOCH, PRIVACY_BASELINE, "e".repeat(64)],
        );
        fence = await ingest();
        await fence.ok("old", fence.worker(A));
        await fence.ok("both", fence.worker(B, [B, A]));
        await OV.openGenerationOverlap(ownerOps, { from: A, to: B, ttlSeconds: 3600, admitDuringApply: true, approvalSha256: APPROVAL });
        overlapOpen = true;
        return { old: "admitted", both: "admitted", overlap: "open", ttlSeconds: 3600 };
      });
    } else await emit("R7-pre", "SKIP", { reason: "overlap_not_installed_at_baseline" });
  }

  // ---- R1: the read-only plan, exactly as production builds and validates it.
  current = "R1";
  const stepByName = new Map((await M.plan(profile)).map((s: { name: string; path: string }) => [s.name, s]));
  // Production's own anchor helpers (scripts/schema-release-database.ts targetAnchor,
  // sameAnchor): the repo is process.cwd(), which is ROOT.
  const sameAnchor = (a: unknown, b: unknown): boolean => D.sameAnchor(a, b);
  const anchorAt = async (targetSha: string, held: string[]) => D.targetAnchor({ sha, target, targetSha }, held);
  const makePlan = async () => {
    const inspection = await I.inspectRelease(owner, BOUNDARY);
    const split = M.migrationTargetSplit(inspection.inventory, target);
    const ledgered = new Set<string>(inspection.inventory.filter((s: any) => !s.pending).map((s: any) => s.name));
    const window = expectedWindow(tree, ledgered, target);
    if (!sameNames(window.expectedPending, split.expectedPending) || !sameNames(window.held, split.held))
      rehearsalRefuse("rehearsal_expected_pending", { expected: window.expectedPending.length, got: split.expectedPending.length });
    const anchor = await discoverAnchor({ RT, M, sha, target, generation: B, held: split.held });
    if (!split.held.length && anchor.targetGeneration !== fullGen) C.refuse("release_target_generation_changed");
    // As production writes release-plan/plan.json and reads it back.
    const plan = JSON.parse(
      JSON.stringify({
        version: 1,
        project: P.project,
        sha,
        boundary: BOUNDARY,
        target,
        ...split,
        targetSha: anchor.targetSha,
        targetGeneration: anchor.targetGeneration,
        manifestDigest: anchor.manifestDigest,
        targetManifest: anchor.manifest,
        runId: RUN_ID,
        ...inspection,
      }),
    );
    const input = {
      sha,
      generation: fullGen,
      boundary: BOUNDARY,
      target,
      targetSha: anchor.targetSha,
      targetGeneration: anchor.targetGeneration,
      manifestDigest: anchor.manifestDigest,
      planRun: RUN_ID,
    };
    const approved = D.validateReleasePlan(plan, input);
    return { plan, input, approved, inspection };
  };

  if (inject === "ledger-gap") {
    const up = seed.seed.filter((n) => n.startsWith("upstream:"));
    if (up.length < 3) rehearsalRefuse("rehearsal_injection_unavailable", { inject });
    const victim = up[Math.floor(up.length / 2)]!;
    await admin.query(`DELETE FROM ${M.MIGRATION_LEDGER} WHERE name=$1`, [victim]);
    await stage("R1-ledger-gap", async () => ({ step: victim, ...(await expectRefusal(INJECTION_REFUSAL["ledger-gap"], makePlan)) }));
    return;
  }

  let planned!: Awaited<ReturnType<typeof makePlan>>;
  await stage("R1", async () => {
    planned = await makePlan();
    const a = planned.approved;
    return {
      project: "<redacted>",
      fullGeneration: fullGen,
      targetSha: a.targetSha,
      targetGeneration: a.targetGeneration,
      manifestDigest: a.manifestDigest,
      expectedPending: a.expectedPending,
      held: a.held.length,
      inventory: planned.plan.inventory.length,
    };
  });
  const firstPending: string[] = planned.approved.expectedPending;

  await stage("R2", async () => {
    const b = BD.requireSafeReleaseBoundary(planned.approved.existingBoundary);
    if (b.prosrc_exact !== false || b.prosrc_sha256 === G.SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256)
      rehearsalRefuse("rehearsal_boundary_not_previous");
    return { safe: true, prosrcExact: false };
  });

  // ---- R4: the production apply, in production order (schema-release-database.ts releaseDatabase).
  const apply = async (p: typeof planned, client: { query: (...a: any[]) => Promise<any> }, overrides: { targetGeneration?: string } = {}) => {
    const approved = p.approved;
    if (!sameAnchor(await anchorAt(approved.targetSha, approved.held), approved)) C.refuse("release_target_manifest_changed");
    const inspection = await I.inspectRelease(client, BOUNDARY);
    const split = M.migrationTargetSplit(inspection.inventory, target);
    const anchor = await anchorAt(approved.targetSha, split.held);
    if (!split.held.length && anchor.targetGeneration !== fullGen) C.refuse("release_target_generation_changed");
    if (inspection.ledgerSha256 !== approved.ledgerSha256) C.refuse("release_ledger_changed");
    if (BD.releaseBoundaryDigest(inspection.existingBoundary) !== BD.releaseBoundaryDigest(approved.existingBoundary))
      C.refuse("release_boundary_changed");
    if (!sameNames(split.expectedPending, approved.expectedPending) || !sameNames(split.held, approved.held))
      C.refuse("release_target_pending_changed");
    if (anchor.targetGeneration !== approved.targetGeneration) C.refuse("release_target_generation_changed");
    if (!sameAnchor(anchor, approved)) C.refuse("release_target_manifest_changed");
    let refreshed: any;
    const result = await M.runMigrations(client, {
      profile,
      expectedLedgerSha256: approved.ledgerSha256,
      target,
      expectedPending: approved.expectedPending,
      targetGeneration: overrides.targetGeneration ?? approved.targetGeneration,
      targetBinding: { targetSha: approved.targetSha, manifestDigest: approved.manifestDigest, verifiers: anchor.verifiers },
      // Production also re-runs the GitHub release gate here; the rehearsal has no approval to recheck.
      beforeWrite: async () => {
        if (!sameAnchor(await anchorAt(approved.targetSha, approved.held), approved)) C.refuse("release_target_manifest_changed");
        refreshed = await BD.refreshReleaseBoundary(client, approved.existingBoundary);
      },
    });
    if (!sameNames(result.held, approved.held)) C.refuse("release_target_readback");
    const ledgered = new Set(
      (await client.query(`SELECT name FROM ${M.MIGRATION_LEDGER} WHERE name = ANY($1::text[])`, [[...approved.expectedPending, ...approved.held]])).rows.map(
        (r: { name: string }) => r.name,
      ),
    );
    if (approved.expectedPending.some((n: string) => !ledgered.has(n)) || approved.held.some((n: string) => ledgered.has(n)))
      C.refuse("release_target_readback");
    const m = (await client.query(`SELECT generation,status FROM ${DB.ROLE_VALIDATION_TABLE} WHERE name=$1`, [DB.ROLE_VALIDATION_NAME])).rows[0];
    if (m?.status !== "pass" || m.generation !== approved.targetGeneration) C.refuse("release_marker");
    const verified = await BD.verifyReleaseBoundary(client);
    return { refreshed, verified, held: result.held as string[] };
  };
  const unchanged = async (before: { ledger: string; marker: string; boundary: string }) => {
    const after = { ledger: await ledgerDigest(), marker: JSON.stringify(await marker()), boundary: await boundaryDigest() };
    if (after.ledger !== before.ledger) rehearsalRefuse("rehearsal_ledger_written");
    if (after.marker !== before.marker) rehearsalRefuse("rehearsal_marker_written");
    if (after.boundary !== before.boundary) rehearsalRefuse("rehearsal_boundary_written");
    return { ledgerUnchanged: true, markerUnchanged: true, boundaryUnchanged: true };
  };
  const snapshot = async () => ({ ledger: await ledgerDigest(), marker: JSON.stringify(await marker()), boundary: await boundaryDigest() });

  if (inject === "boundary-drift") {
    await stage("R4-boundary-drift", async () => {
      const ledgerBefore = await ledgerDigest();
      await admin.query(`ALTER DEFAULT PRIVILEGES FOR ROLE ${P.owner} GRANT EXECUTE ON FUNCTIONS TO PUBLIC`);
      const r = await expectRefusal(INJECTION_REFUSAL["boundary-drift"], () => apply(planned, owner));
      if ((await ledgerDigest()) !== ledgerBefore) rehearsalRefuse("rehearsal_ledger_written");
      return { ...r, ledgerUnchanged: true };
    });
    return;
  }
  if (inject === "stale-plan") {
    await stage("R4-stale-plan", async () => {
      await admin.query(`INSERT INTO ${M.MIGRATION_LEDGER}(name, sha256) VALUES ('rehearsal:stale-plan', $1)`, ["0".repeat(64)]);
      const before = await snapshot();
      // Production's own refusal: runMigrations re-reads the ledger under the migrator lock
      // and refuses the approved (now stale) ledger digest before any write, exactly as
      // releaseDatabase passes it; beforeWrite is a tripwire.
      const verifiers = (await anchorAt(planned.approved.targetSha, planned.approved.held)).verifiers;
      const migrator = await expectRefusal(INJECTION_REFUSAL["stale-plan"], () =>
        M.runMigrations(owner, {
          profile,
          expectedLedgerSha256: planned.approved.ledgerSha256,
          target,
          expectedPending: planned.approved.expectedPending,
          targetGeneration: planned.approved.targetGeneration,
          targetBinding: {
            targetSha: planned.approved.targetSha,
            manifestDigest: planned.approved.manifestDigest,
            verifiers,
          },
          beforeWrite: async () => rehearsalRefuse("rehearsal_reached_before_write"),
        }),
      );
      // The release script's earlier check (its order, replayed by apply()).
      const replay = await expectRefusal(INJECTION_REFUSAL["stale-plan"], () => apply(planned, owner));
      return { migrator: migrator.refused, replay: replay.refused, ...(await unchanged(before)) };
    });
    return;
  }
  if (inject === "wrong-generation") {
    await stage("R4-wrong-generation", async () => {
      const flipped = flipHex(planned.approved.targetGeneration);
      const before = await snapshot();
      const artifact = await expectRefusal("release_plan_artifact", () =>
        D.validateReleasePlan(planned.plan, { ...planned.input, targetGeneration: flipped }),
      );
      const verifiers = (await anchorAt(planned.approved.targetSha, planned.approved.held)).verifiers;
      const migrator = await expectRefusal(INJECTION_REFUSAL["wrong-generation"], () =>
        M.runMigrations(owner, {
          profile,
          expectedLedgerSha256: planned.approved.ledgerSha256,
          target,
          expectedPending: planned.approved.expectedPending,
          targetGeneration: flipped,
          targetBinding: {
            targetSha: planned.approved.targetSha,
            manifestDigest: planned.approved.manifestDigest,
            verifiers,
          },
          beforeWrite: async () => rehearsalRefuse("rehearsal_reached_before_write"),
        }),
      );
      return { plan: artifact.refused, migrator: migrator.refused, ...(await unchanged(before)) };
    });
    return;
  }

  let applied!: Awaited<ReturnType<typeof apply>>;
  if (inject === "late-step") {
    await stage("R4-late-step", async () => {
      const targetSql = await readFile((stepByName.get(target) as { path: string }).path, "utf8");
      let fired = false;
      const late = {
        query: (text: any, params?: any) => {
          if (!fired && text === targetSql) {
            fired = true;
            return Promise.reject(Object.assign(new Error("injected late step failure"), { code: INJECTION_REFUSAL["late-step"] }));
          }
          return owner.query(text, params);
        },
      };
      const r = await expectRefusal(INJECTION_REFUSAL["late-step"], () => apply(planned, late));
      const ledger = await schemaLedger();
      const earlier = firstPending.slice(0, -1);
      if (earlier.some((n) => !ledger.has(n))) rehearsalRefuse("rehearsal_late_step_earlier_missing");
      if (ledger.has(target)) rehearsalRefuse("rehearsal_late_step_target_ledgered");
      const m = await marker();
      if (m?.status !== "pending" || m.generation !== B) rehearsalRefuse("rehearsal_late_step_marker");
      return { ...r, earlierLedgered: earlier.length, targetLedgered: false, marker: "pending" };
    });
    // The rerun: a fresh plan over the partially applied database completes the release.
    await stage("R1-rerun", async () => {
      planned = await makePlan();
      return { expectedPending: planned.approved.expectedPending, targetGeneration: planned.approved.targetGeneration };
    });
  }
  await stage("R4", async () => {
    applied = await apply(planned, owner);
    return { held: applied.held.length, verified: true };
  });
  await stage("R3", async () => {
    const r = applied.refreshed;
    if (!r || r.prosrc_exact !== true || r.prosrc_sha256 !== G.SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256)
      rehearsalRefuse("rehearsal_boundary_not_pinned");
    if (applied.verified?.prosrc_exact !== true) rehearsalRefuse("rehearsal_boundary_not_pinned");
    return { prosrcExact: true, prosrcMatchesPin: true };
  });
  let ledgerAfter!: Map<string, string>;
  await stage("R5", async () => {
    ledgerAfter = await schemaLedger();
    const expected = new Set([...seed.seed, ...firstPending]);
    const got = [...ledgerAfter.keys()];
    if (got.length !== expected.size || got.some((n) => !expected.has(n)))
      rehearsalRefuse("rehearsal_ledger_readback", { expected: expected.size, got: got.length });
    if (!ledgerAfter.has(target)) rehearsalRefuse("rehearsal_target_not_ledgered");
    const later = tree.slice(tree.indexOf(target) + 1).filter((n) => laneOf(n) === laneOf(target) && ledgerAfter.has(n));
    if (later.length) rehearsalRefuse("rehearsal_later_step_applied", { later });
    const heldLedgered = planned.approved.held.filter((n: string) => ledgerAfter.has(n));
    if (heldLedgered.length) rehearsalRefuse("rehearsal_held_ledgered", { heldLedgered });
    const m = await marker();
    if (m?.status !== "pass" || m.generation !== B) rehearsalRefuse("rehearsal_marker_readback");
    return { ledgered: got.length, applied: firstPending.length, held: planned.approved.held.length, marker: "pass", generation: B };
  });

  await stage("R6", async () => {
    const S = await mod("stacks/cloudflare/session-ingest.ts");
    const W = await mod("src/session-ingest/worker.ts");
    // Placeholder ids only: R6 reads the generation vars, nothing else is emitted.
    const base = {
      accountId: "rehearsal-account",
      hyperdriveId: "rehearsal-hyperdrive",
      secretsStoreId: "rehearsal-store",
      kmsKeyArn: "rehearsal-kms-key",
      acceptanceMode: "buffered",
      inlineMaxSessions: 0,
      replicaMode: "off",
    };
    const during = S.sessionIngestVars({ ...base, expectedGeneration: A, overlapGeneration: B });
    const accepted = W.acceptedGenerations(during, "buffered");
    if (JSON.stringify(accepted) !== JSON.stringify([A, B])) rehearsalRefuse("rehearsal_ingest_vars_during");
    const after = S.sessionIngestVars({ ...base, expectedGeneration: B });
    if (after.INGEST_EXPECT_GENERATION !== B || "INGEST_ACCEPT_GENERATIONS" in after || W.acceptedGenerations(after, "buffered") !== undefined)
      rehearsalRefuse("rehearsal_ingest_vars_after");
    return {
      during: { INGEST_EXPECT_GENERATION: during.INGEST_EXPECT_GENERATION, INGEST_ACCEPT_GENERATIONS: during.INGEST_ACCEPT_GENERATIONS },
      after: { INGEST_EXPECT_GENERATION: after.INGEST_EXPECT_GENERATION },
    };
  });

  if (inject !== "none") {
    await emit("R7", "SKIP", { reason: "injection_run" });
    await emit("R8", "SKIP", { reason: "injection_run" });
    return;
  }
  if (!overlapOpen || !fence) {
    await emit("R7", "SKIP", { reason: "overlap_not_installed_at_baseline" });
    await emit("R8", "SKIP", { reason: "needs_r7" });
    return;
  }
  const f = fence;
  await stage("R7", async () => {
    const REB = await mod("src/hoopdb/ingest-generation-rebind.ts");
    const old = f.worker(A);
    const both = f.worker(B, [B, A]);
    const next = f.worker(B);
    await f.ok("old", old);
    await f.ok("both", both);
    const rebind = await REB.rebindIngestGeneration(ownerOps, { from: A, to: B });
    if (rebind.policy !== "rebound" || rebind.overlapClosed !== true) rehearsalRefuse("rehearsal_rebind", { policy: rebind.policy ?? null });
    const oldCode = await f.refusedCode(old);
    if (oldCode !== "ledger_sqlstate_55000") rehearsalRefuse("rehearsal_old_worker_not_fenced", { got: oldCode });
    await f.ok("next", next);
    await f.ok("both", both);
    return { beforeRebind: "old+both admitted", rebind: "rebound", overlapClosed: true, old: oldCode, after: "next+both admitted" };
  });

  if (![...ledgerAfter.keys()].some((n) => READ_047010.test(n))) {
    await emit("R8", "SKIP", { reason: "047010_not_applied" });
    return;
  }
  await stage("R8", async () => {
    await admin.query(psql(await read("supabase/bootstrap/session_read_api_role.sql")));
    const pick = (sql: string, re: RegExp) => sql.match(re)?.[0] ?? rehearsalRefuse("rehearsal_reader_body_missing");
    const list = pick(
      await read("supabase/migrations/20261105044040_buffered_stages_session_first.sql"),
      /CREATE OR REPLACE FUNCTION sessions\.buffered_sessions_for\([\s\S]*?END\n\$buffered_sessions\$;/,
    ).replace("CREATE OR REPLACE FUNCTION sessions.buffered_sessions_for(", "CREATE FUNCTION ref044090.list(");
    const stagesRef = pick(
      await read("supabase/migrations/20261105044090_claim_sessions_reader.sql"),
      /CREATE OR REPLACE FUNCTION sessions\.buffered_session_stages_for\([\s\S]*?END\n\$buffered_stages\$;/,
    ).replace("CREATE OR REPLACE FUNCTION sessions.buffered_session_stages_for(", "CREATE FUNCTION ref044090.stages(");
    await admin.query(
      "CREATE SCHEMA ref044090;GRANT USAGE ON SCHEMA ref044090 TO bulls96_read_api;" +
        list +
        stagesRef +
        "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA ref044090 FROM PUBLIC;GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ref044090 TO bulls96_read_api",
    );
    const counts = async () =>
      (
        await admin.query(
          "SELECT (SELECT count(*)::int FROM sessions.ingest_claim_attempts) AS claims, (SELECT coalesce(sum(jsonb_array_length(records)),0)::int FROM sessions.ingest_claim_attempts) AS records, (SELECT coalesce(sum(jsonb_array_length(group_ordinals)),0)::int FROM sessions.ingest_stage_intents) AS groups",
        )
      ).rows[0] as { claims: number; records: number; groups: number };
    const before = await counts();
    const { createIngestApp } = await mod("src/session-ingest/app.ts");
    const { S } = f;
    const app = createIngestApp({
      identify: () => ({ ...S.TENANT, scopes: ["rth:sync"] }),
      deliver: f.worker(B),
      log: () => {},
      custodySource: "frozen-supabase",
    });
    let serial = 0;
    let unrelated = 0;
    for (let b = 0; b < 17; b++) {
      // Synthetic values matching the aggregate incident shape only: 16*10+17 target records.
      const groups: [string, string, string[]][] = [
        ["droid", "rehearsal-wide-target", Array.from({ length: b === 16 ? 17 : 10 }, (_, i) => `${b}-target-${i}`)],
      ];
      // 153 unrelated groups: 30*45 + 123*44 records = 6762.
      for (let s = 0; s < 9; s++) {
        const count = unrelated++ < 30 ? 45 : 44;
        groups.push([s % 2 ? "codex" : "droid", `rehearsal-wide-other-${s}`, Array.from({ length: count }, (_, r) => `${b}-${s}-${r}`)]);
      }
      serial++;
      const records = groups.flatMap(([source, session_id, ids]) =>
        ids.map((id) => S.record({ record_id: "rehearsal-" + id, revision: serial, revision_id: `rehearsal-${id}:r${serial}`, source, session_id })),
      );
      const body = await S.batch(records, { batch_id: "rehearsal-wide-" + serial, account_id: await f.deliveryAccount(S.TENANT) });
      const r = await app.request("/v1/delivery/batches", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ protocolVersion: 1, batch: body }),
      });
      if (r.status !== 200) rehearsalRefuse("rehearsal_r8_deliver", { status: r.status });
    }
    const after = await counts();
    const delta = { claims: after.claims - before.claims, records: after.records - before.records, groups: after.groups - before.groups };
    if (delta.claims !== 17 || delta.records !== 6939 || delta.groups !== 170) rehearsalRefuse("rehearsal_r8_shape", delta);
    // The operator sequence: writer on, backfill to completion, reader on.
    const { backfillClaimSessions } = await mod("src/session-ingest/claim-sessions-backfill.ts");
    await admin.query("UPDATE sessions.ingest_claim_sessions_policy SET write_enabled=true");
    const bf = await backfillClaimSessions({ sql: { query: (s: string, p?: unknown[]) => admin.query(s, p) }, batchSize: 3, statementTimeoutMs: 30000, dryRun: false });
    if (bf.completionMarked !== true) rehearsalRefuse("rehearsal_r8_backfill");
    await admin.query("UPDATE sessions.ingest_claim_sessions_policy SET read_enabled=true");
    // Owner G2 proof with no head, synthetic (as exact-session-stages.fixture.ts prove()).
    const key = [S.TENANT.orgId, S.TENANT.workspaceId, S.TENANT.userId];
    await admin.query("DELETE FROM sessions.owner_buffered_parity WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3", key);
    await admin.query(
      `INSERT INTO sessions.owner_buffered_parity(org_id,workspace_id,user_id,cutover_epoch,head_epoch,head_version,head_digest,head_kind,no_head,privacy_baseline_sha256,deletion_set_digest,reset_set_digest,target_lineage_digest,target_generation)
       SELECT $1,$2,$3,$4,NULL,NULL,NULL,NULL,true,$5,e.deletion_set_digest,e.reset_set_digest,e.target_lineage_digest,e.target_generation
         FROM sessions.owner_buffered_parity_expected($1,$2,$3,$4,NULL::bigint,NULL,NULL::text) AS e`,
      [...key, EPOCH, PRIVACY_BASELINE],
    );
    const STAGES =
      "SELECT batch_id,stage_key,stage_sha256,stage_bytes::text AS stage_bytes,group_indexes,group_ordinals,kept_ordinals,reset_epoch::text AS reset_epoch,accepted_at,admission_epoch,admission_version FROM ";
    const asReader = async (fn: string) => {
      const c = await rt.connect();
      try {
        await c.query("BEGIN READ ONLY");
        await c.query(
          "SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true),set_config('bulls96_read.user_id',$3,true)",
          key,
        );
        await c.query("SET LOCAL ROLE bulls96_read_api");
        const r = await c.query(STAGES + fn + "($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)", [
          S.TENANT.workspaceId,
          "droid",
          "rehearsal-wide-target",
          EPOCH,
          null,
          null,
          null,
          null,
          false,
          PRIVACY_BASELINE,
        ]);
        await c.query("COMMIT");
        return r.rows as any[];
      } catch (e) {
        await c.query("ROLLBACK").catch(() => {});
        throw e;
      } finally {
        c.release();
      }
    };
    const frozen = await asReader("ref044090.stages");
    const current = await asReader("sessions.buffered_session_stages_for_target");
    if (JSON.stringify(current) !== JSON.stringify(frozen)) rehearsalRefuse("rehearsal_r8_not_equivalent");
    const kept = current.reduce((n, r) => n + r.kept_ordinals.length, 0);
    if (current.length !== 17 || kept !== 177 || !current.every((r) => r.group_indexes.length === 1 && r.group_indexes[0] === 0))
      rehearsalRefuse("rehearsal_r8_read_shape", { stages: current.length, kept });
    return { ...delta, targetStages: current.length, targetOrdinals: kept, equivalentTo044090: true };
  });
  await rt.end().catch(() => {});
  f.fs.done();
}

/**
 * The session-ingest fixtures the R7/R8 deliveries need (the shapes of
 * src/session-ingest/__tests__/support.ts, kept here so the driver imports no test
 * module), built from the release checkout's own object store and delivery contract.
 */
async function ingestFixtures(deliveryAccount: (t: typeof TENANT) => Promise<string>) {
  const { createFsStore } = await mod("packages/object-store/src/fs.ts");
  const { DELIVERY_MAPPING_VERSION } = await mod("src/session-ingest/vendor/delivery/delivery-contracts.ts");
  const scratchStore = () => {
    const base = process.env.RUNNER_TEMP || tmpdir();
    mkdirSync(base, { recursive: true, mode: 0o700 });
    const root = mkdtempSync(join(base, "rehearsal-ingest-"));
    return { store: createFsStore({ root }), done: () => rmSync(root, { recursive: true, force: true }) };
  };
  const record = (over: Record<string, unknown> & { record_id: string }) => ({
    schema_version: 1,
    origin_id: "origin-1",
    revision_id: `${over.record_id}:r${(over.revision as number | undefined) ?? 1}`,
    revision: 1,
    kind: "session_event",
    source: "claude",
    session_id: "sess-1",
    operation: "upsert",
    payload: { text: "hello", ts_ms: 1_761_955_200_000 },
    ...over,
  });
  const batch = async (records: unknown[], over: Record<string, unknown> = {}) => ({
    schema_version: 1,
    origin_id: "origin-1",
    batch_id: "batch-1",
    job_id: "job-1",
    generation: 1,
    destination_id: "dest-1",
    instance_id: "inst-1",
    account_id: await deliveryAccount(TENANT),
    mapping_version: DELIVERY_MAPPING_VERSION,
    records,
    ...over,
  });
  return { TENANT, scratchStore, record, batch };
}
const TENANT = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };

if (import.meta.main) await main();
