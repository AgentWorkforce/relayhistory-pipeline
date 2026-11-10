/** Real PG lock proof in an isolated database on an ALREADY RUNNING test container.
 * No VM/container starts, role changes, live data, or connection URLs in output.
 * Usage: bun scripts/proof-native-agent-catalog-activation.ts CONTEXT CONTAINER
 */
import { execFileSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import pg from "pg";
import { nativeAgentCatalogActivation } from "../src/hoopdb/native-agent-catalog-activation.ts";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";

const [context, container] = process.argv.slice(2);
if (!context || !container || context.startsWith("-") || container.startsWith("-") || process.argv.length !== 4) {
  throw new Error("proof_usage_context_container");
}
const docker = (...args: string[]) => execFileSync("docker", ["--context", context, ...args], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
const check: (ok: unknown, code: string) => asserts ok = (ok, code) => { if (!ok) throw new Error(code); };
const inspected = JSON.parse(docker("inspect", container));
check(inspected.length === 1 && inspected[0].State.Running, "proof_existing_container_required");
const port = docker("port", container, "5432/tcp").trim().match(/^127\.0\.0\.1:(\d+)$/);
check(port, "proof_loopback_test_port_required");
// Existing test credentials stay in RAM. No writes to Docker or its configuration.
const env = Object.fromEntries((inspected[0].Config.Env as string[]).map((v) => {
  const i = v.indexOf("="); return [v.slice(0, i), v.slice(i + 1)];
}));
const connection = { host: "127.0.0.1", port: Number(port[1]), user: env.POSTGRES_USER ?? "postgres", password: env.POSTGRES_PASSWORD, ssl: false as const };
const database = "native_agent_catalog_proof_" + randomBytes(8).toString("hex");
const clients: pg.Client[] = [];
const connect = async (name: string) => {
  const client = new pg.Client({ ...connection, database: name, application_name: "native-agent-catalog-lock-proof", connectionTimeoutMillis: 5000 });
  await client.connect(); clients.push(client); return client;
};
const transaction = (client: pg.Client, before?: (sql: string) => Promise<void>) => async <T>(work: (tx: SqlExecutor) => Promise<T>) => {
  await client.query("BEGIN");
  try {
    const result = await work({ async query<R>(sql: string, values?: unknown[]) {
      await before?.(sql); return { rows: (await client.query(sql, values)).rows as R[] };
    } });
    await client.query("COMMIT"); return result;
  } catch (e) { await client.query("ROLLBACK").catch(() => {}); throw e; }
};
const code = async (promise: Promise<unknown>) => promise.then(() => "success", (e: { code?: string }) => e.code ?? "unknown");
const bounded = async <T>(promise: Promise<T>) => {
  let timer: ReturnType<typeof setTimeout>;
  try { return await Promise.race([promise, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("proof_barrier_timeout")), 10_000); })]); }
  finally { clearTimeout(timer!); }
};
const generation = "a".repeat(64), options = { mode: "selfhosted" as const, generation };
const report: Record<string, unknown> = { scope: "isolated_synthetic_pg_lock_proof", liveDatabaseUsed: false, newVm: false, newContainer: false };
let admin: pg.Client | undefined, created = false, release: (() => void) | undefined;
try {
  admin = await connect("postgres");
  const version = (await admin.query("SHOW server_version_num")).rows[0].server_version_num;
  check(Number(version) >= 170000 && Number(version) < 180000, "proof_pg17_required");
  await admin.query(`CREATE DATABASE ${database}`); created = true;
  const owner = await connect(database), writer = await connect(database), other = await connect(database), reader = await connect(database);
  await owner.query(`CREATE SCHEMA sessions;
    CREATE TABLE sessions.session_directory(org_id text,workspace_id text,harness text,session_id text,
      deleted_at timestamptz,event_projection_epoch bigint,last_activity_at timestamptz,event_projection_ready boolean,
      event_projected_at timestamptz,catalog_projected_at timestamptz,agent_names text[],PRIMARY KEY(org_id,workspace_id,harness,session_id));
    CREATE TABLE sessions.session_index(org_id text,workspace_id text,harness text,session_id text,machine_id text,canonical_machine_id text,canonical_session_id text,deleted_at timestamptz);
    CREATE TABLE sessions.session_directory_agents(org_id text,workspace_id text,harness text,session_id text,agent_name_normalized text,last_activity_at timestamptz);
    CREATE TABLE sessions.session_directory_users(user_id text);
    CREATE TABLE sessions.session_directory_machines(org_id text,workspace_id text,harness text,session_id text,machine_id text);
    CREATE TABLE sessions.session_directory_relationships(org_id text,workspace_id text,harness text,parent_session_id text);
    CREATE TABLE sessions.session_directory_catalog_rollout(singleton boolean PRIMARY KEY,completed_at timestamptz,updated_at timestamptz);
    CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text);
    INSERT INTO sessions.session_directory VALUES('org-test','ws-test','codex','s1',NULL,1,'2026-01-01',true,'2026-01-01','2026-01-01',ARRAY['reviewer']);
    INSERT INTO sessions.session_index VALUES('org-test','ws-test','codex','s1','physical-m1','canonical-m1',NULL,NULL);
    INSERT INTO sessions.session_directory_agents VALUES('org-test','ws-test','codex','s1','reviewer','2026-01-01');
    INSERT INTO sessions.session_directory_machines VALUES('org-test','ws-test','codex','s1','canonical-m1');
    INSERT INTO sessions.session_directory_catalog_rollout VALUES(true,NULL,NULL)`);
  await owner.query("INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker',$1,'pass')", [generation]);
  const plan = await nativeAgentCatalogActivation(transaction(owner), options);
  check(plan.eligible, "proof_fixture_ineligible");
  await writer.query("BEGIN; UPDATE sessions.session_directory SET event_projection_epoch=event_projection_epoch");
  const blockedActivation = await code(nativeAgentCatalogActivation(transaction(owner), { ...options, applyFingerprint: plan.fingerprint }));
  check(blockedActivation === "55P03", "proof_writer_did_not_block_activation");
  await writer.query("ROLLBACK");
  check((await owner.query("SELECT completed_at FROM sessions.session_directory_catalog_rollout")).rows[0].completed_at === null, "proof_failed_activation_changed_marker");
  report.writerBlocksActivation = true;
  report.failedActivationPreservesMarker = true;
  let reached!: () => void;
  const markerReached = new Promise<void>((resolve) => { reached = resolve; });
  const hold = new Promise<void>((resolve) => { release = resolve; });
  const activation = nativeAgentCatalogActivation(transaction(owner, async (sql) => {
    if (sql.startsWith("UPDATE sessions.session_directory_catalog_rollout")) { reached(); await hold; }
  }), { ...options, applyFingerprint: plan.fingerprint });
  // Attach a rejection observer before waiting for the barrier.
  const observed = activation.then((result) => ({ result }), (error: unknown) => ({ error }));
  await bounded(Promise.race([markerReached, observed.then((v) => { if ("error" in v) throw v.error; })]));
  await writer.query("SET statement_timeout='500ms'");
  check(await code(writer.query("UPDATE sessions.session_directory SET event_projection_epoch=event_projection_epoch+1")) === "57014", "proof_activation_did_not_block_writer");
  check(await code(writer.query("UPDATE sessions.__96bulls_role_validation SET status='pending'")) === "57014", "proof_activation_did_not_block_migration_admission");
  check(await code(nativeAgentCatalogActivation(transaction(other), { ...options, applyFingerprint: plan.fingerprint })) === "55P03", "proof_concurrent_activation_not_serialized");
  check((await reader.query("SELECT count(*)::integer AS n FROM sessions.session_directory")).rows[0].n === 1, "proof_reader_blocked");
  check(release, "proof_barrier_missing");
  release();
  const activated = await bounded(activation);
  check(activated.activated, "proof_activation_failed");
  check((await reader.query("SELECT completed_at IS NOT NULL AS marked FROM sessions.session_directory_catalog_rollout")).rows[0].marked, "proof_marker_not_committed");
  Object.assign(report, { postgresVersion: version, activationBlocksWriter: true, activationBlocksMigrationAdmission: true, concurrentActivationSerialized: true, ordinaryReadersContinue: true, activationCommits: true, success: true });
} catch (e) {
  const error = e as { message?: string; code?: string };
  report.success = false;
  report.error = error.message && /^proof_[a-z0-9_]+$/.test(error.message) ? error.message : error.code ?? "proof_operation_failed";
  process.exitCode = 1;
} finally {
  release?.();
  for (const client of clients.filter((c) => c !== admin)) await client.end().catch(() => {});
  if (created && admin) {
    try { await admin.query(`DROP DATABASE ${database}`); report.isolatedDatabaseRemoved = true; }
    catch { report.isolatedDatabaseRemoved = false; process.exitCode = 1; }
  }
  await admin?.end().catch(() => {});
  console.log(JSON.stringify(report));
}
