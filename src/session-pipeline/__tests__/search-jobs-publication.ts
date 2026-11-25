/**
 * A real managed publication for the search-index wake red tests (design §2, R1c, R5a, R5c), offline and
 * synthetic. The setup is the one of src/session-ingest/__tests__/publication-portable.pglite.test.ts
 * (real migrations and verifiers on PGlite, a buffered stage from the real ingest path, the portable
 * publisher against a MinIO-shaped fake), then the real queue consumers drive the committed head through
 * `object-landed` and the managed authority path of `session-indexed` (hoopDbSessionIndex with the
 * publication transaction), as publication-pickup.ts does. Nothing here reads a credential or the network.
 *
 * Rev 6 (R5f, R5g): `beforeAuthority` mutates the committed publication state BEFORE the retired case and the
 * current authority transaction run, so the real §2 writer sees it; `siblingHead` seeds a second live head of the
 * same canonical scope (cloned from the real one, triggers off) for the clock aggregate.
 */
import { expect } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import { createS3Store, type ObjectStore } from "../../../packages/object-store/src/index.ts";
import { objectStoreManifestReader, type StageMessage } from "../../shared/vendor.ts";
import { pollPostgresStage, postgresPipelineQueue, type PollResult } from "@rhc/lib/pipeline/queue.ts";
import { objectLandedHandler, sessionIndexedHandler } from "@rhc/lib/pipeline/stages.ts";
import { InMemoryPipelineEventSink } from "@rhc/lib/pipeline/events.ts";
import { InMemoryDeadLetterStore, type StageQueueDeps } from "@rhc/lib/pipeline/dlq.ts";
import { hoopDbSessionIndex } from "../../hoopdb/index.ts";
import type { Transact } from "../../shared/db.ts";
import { derivedSchemaAssets } from "../derived-schema.ts";
import { sqlLedger } from "../../session-ingest/ledger-sql.ts";
import { deliverBatch } from "../../session-ingest/deliver.ts";
import { runPortablePublicationRuntime } from "../../session-ingest/publication-portable.ts";
import { batch, record, TENANT } from "../../session-ingest/__tests__/support.ts";
import { orderedFixtureLedger, initializeFixtureSeatClock } from "../../session-ingest/__tests__/ordered-support.ts";
import { pgliteTestPool, MinioShapedS3 } from "../../session-ingest/__tests__/portable-fakes.ts";

const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const GEN = "a".repeat(64), BUCKET = "selfhost-sessions", REGION = "selfhost-1", ORIGIN = "http://minio.selfhost.svc:9000";
const ACCESS = "publisher-fixture", SECRET = "publisher-fixture-secret-only";
const FILES: Record<string, string> = {
  "/run/secrets/publication/access-key-id": ACCESS + "\n",
  "/run/secrets/publication/secret-access-key": SECRET + "\n",
  "/run/secrets/publication/database-url": "postgresql://bulls96_session_ingest:fixture-only@postgres.selfhost.svc:5432/sessions?sslmode=disable\n",
};
const ENV = {
  INGEST_PUBLICATION_TARGET: "portable", INGEST_PUBLISH_ENABLED: "true", INGEST_PUBLICATION_GC_ENABLED: "true",
  INGEST_PUBLISH_BUCKET: BUCKET, INGEST_PUBLISH_EXPECT_GENERATION: GEN, INGEST_PUBLISH_S3_ENDPOINT: ORIGIN, ALLOW_IN_CLUSTER_HTTP: "true",
  INGEST_PUBLISH_S3_REGION: REGION, INGEST_PUBLISH_S3_ACCESS_KEY_ID_FILE: "/run/secrets/publication/access-key-id",
  INGEST_PUBLISH_S3_SECRET_ACCESS_KEY_FILE: "/run/secrets/publication/secret-access-key", INGEST_PUBLISH_DATABASE_URL_FILE: "/run/secrets/publication/database-url",
  HOOPDB_DB_MODE: "selfhosted", SESSION_POSTGRES_TLS_PROFILE: "supabase", INGEST_PUBLISH_BUDGET_MS: "60000",
};

export interface PublishedHead {
  db: PGlite;
  store: ObjectStore;
  key: string;
  scope: { orgId: string; workspaceId: string; harness: string; sessionId: string; canonicalSessionId: string };
  /** ingest_publication_attempts.published_at of the attempt that made the head (ISO, ms precision). */
  publishedAt: string;
  /** The session-indexed message the authority path handles. */
  indexed: StageMessage;
  /** What a retired-generation pass wrote (counted inside its own, rolled back, transaction). */
  retired: { outcome: unknown; jobsWritten: number };
  /** All pipeline_jobs rows after the current authority path committed. */
  jobsAfter: Array<{ stage: string; message: StageMessage }>;
  /** session_directory epoch of the canonical scope after the authority transaction (null when none committed). */
  epoch: string | null;
  /** The real consumer's result for the current `session-indexed` delivery (the authority transaction). */
  authority: PollResult;
  close(): Promise<void>;
}

export interface PublishedHeadOptions {
  /**
   * Runs after the publish and `object-landed`, BEFORE the retired case and the current authority transaction:
   * the committed state the §2 writer then reads. `key` is the real head's current_key.
   */
  beforeAuthority?: (fixture: { db: PGlite; key: string; publishedAt: string }) => Promise<void>;
  /**
   * Default true: the current authority delivery must ack, and the fan-out soundness checks run. false (R5g): the
   * delivery's poll result is returned unchecked, for the test to assert.
   */
  expectAuthorityAck?: boolean;
  /**
   * The writer's explicit opt-in (hoopDbSessionIndex publication.enqueueSearchWake; only exactly true writes the
   * managed wake). Default true, for the retired case and the current authority transaction alike; `undefined`
   * passes no field at all (the writer's default, off).
   */
  enqueueSearchWake?: boolean | undefined;
}

/**
 * A sibling head of the real one (heads, generations, attempts, images, session_index cloned, each column overridden
 * as given), seeded with triggers off (session_replication_role = replica): the rows are the state the §2 clock
 * query reads, not a path under test. Its key is `<real key>.<tag>`, its machine `srv_<tag with '-' as '_'>`.
 * A VALID second origin: `origin-<tag>` on its head, its generation AND its attempt (§2: each sibling's attempt
 * matches its own head's origin). `attemptOrigin` forges a mismatch (the negative case).
 */
export async function siblingHead(db: PGlite, realKey: string, tag: string,
  o: { publishedAt: string; headState?: "live" | "retired"; deleted?: boolean; canonical?: string; attemptOrigin?: string }) {
  const generation = crypto.randomUUID(), attempt = crypto.randomUUID();
  const machine = `srv_${tag.replace(/-/g, "_")}`, origin = `origin-${tag}`;
  await db.transaction(async (tx) => {
    await tx.query("SET LOCAL session_replication_role = replica");
    const key = (await tx.query<{ key: string }>("SELECT current_key || '.' || $1 AS key FROM sessions.ingest_publication_heads WHERE current_key = $2", [tag, realKey])).rows[0]!.key;
    const clone = (table: string, where: string, patch: Record<string, unknown>) => tx.query(
      `INSERT INTO sessions.${table} SELECT (jsonb_populate_record(NULL::sessions.${table}, to_jsonb(t) || $1::jsonb)).* FROM sessions.${table} t WHERE ${where}`,
      [JSON.stringify(patch)]);
    const one = async (p: Promise<{ affectedRows?: number }>) => expect((await p).affectedRows).toBe(1);
    await one(clone("ingest_publication_heads", `current_key = '${realKey}'`, { origin_id: origin, machine_id: machine, generation, current_key: key, state: o.headState ?? "live" }));
    await one(clone("ingest_publication_generations", `generation = (SELECT generation FROM sessions.ingest_publication_heads WHERE current_key = '${realKey}')`,
      { generation, prefix: `${tag}-${generation}`, origin_id: origin, state: o.headState ?? "live" }));
    await one(clone("ingest_publication_attempts", "state = 'published' AND attempt_token = (SELECT attempt_token FROM sessions.ingest_publication_images WHERE manifest_key = '" + realKey + "')",
      { attempt_token: attempt, published_at: o.publishedAt, origin_id: o.attemptOrigin ?? origin }));
    await one(clone("ingest_publication_images", `manifest_key = '${realKey}'`, { attempt_token: attempt, generation, manifest_key: key }));
    await one(clone("session_index", `manifest_key = '${realKey}'`, {
      machine_id: machine, manifest_key: key, deleted_at: o.deleted ? o.publishedAt : null,
      ...(o.canonical ? { session_id: o.canonical, canonical_session_id: o.canonical } : {}),
    }));
  });
}

/** Publishes one session, then runs `object-landed` and the managed `session-indexed` authority path. */
export async function publishedHead(opts: PublishedHeadOptions = {}): Promise<PublishedHead> {
  const optIn = "enqueueSearchWake" in opts ? (opts.enqueueSearchWake === undefined ? {} : { enqueueSearchWake: opts.enqueueSearchWake }) : { enqueueSearchWake: true };
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  const owner = async (sql: string, args: unknown[] = []) => db.query<any>(sql, args as never);
  const asIngest = async (sql: string, args: unknown[] = []) => {
    await db.exec("BEGIN; SET LOCAL ROLE bulls96_session_ingest");
    try { const r = await db.query<any>(sql, args as never); await db.exec("COMMIT"); return r; } catch (e) { await db.exec("ROLLBACK"); throw e; }
  };
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const f of readdirSync(join(root, "supabase/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read("supabase/migrations/" + f));
  for (const f of readdirSync(join(root, "src/hoopdb/migrations")).filter((f) => f.endsWith(".sql")).sort()) await db.exec(read("src/hoopdb/migrations/" + f));
  await db.exec("REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions FROM PUBLIC");
  await db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text,updated_at timestamptz DEFAULT now())");
  await owner("INSERT INTO sessions.__96bulls_role_validation(name,generation,status)VALUES('session_directory_search_worker',$1,'pass')", [GEN]);
  await owner("INSERT INTO sessions.ingest_buffer_admission(org_id,privacy_baseline_sha256,retention_contract_sha256,schema_generation,valid_until,control_epoch)VALUES($1,$2,$2,$3,clock_timestamp()+interval'1 hour',repeat('3',64))", [TENANT.orgId, "a".repeat(64), GEN]);
  await initializeFixtureSeatClock({ query: owner }, GEN);
  await owner("INSERT INTO sessions.ingest_publication_frontiers VALUES($1,$2,$3,'origin-1',$4,$4,clock_timestamp()+interval'1 hour',$5,$6,'96bulls',$7)", [TENANT.orgId, TENANT.workspaceId, TENANT.userId, "b".repeat(64), BUCKET, REGION, GEN]);
  const s3 = new MinioShapedS3(ORIGIN, BUCKET, REGION, ACCESS);
  const store = createS3Store({ bucket: BUCKET, region: REGION, endpoint: ORIGIN, credentials: { accessKeyId: ACCESS, secretAccessKey: SECRET }, fetch: s3.fetch }) as unknown as ObjectStore;
  const ledger = orderedFixtureLedger(sqlLedger(asIngest, { acceptanceMode: "buffered", expectedGeneration: GEN }), { query: owner });
  const accepted = await deliverBatch({ ledger, target: { store, keyRoot: "96bulls" }, acceptanceMode: "buffered" }, TENANT,
    await batch([record({ record_id: "search-wake-r1", payload: { text: "search wake fixture needle", ts_ms: 1761955200000 } })]));
  expect(accepted.outcome).toBe("accepted_buffered");
  const assets = async () => { const real = await derivedSchemaAssets(); return { expected: { ...real.expected, generation: GEN }, verifiers: real.verifiers }; };
  const published = await runPortablePublicationRuntime(ENV, "publish", {
    assets, fetcher: s3.fetch,
    readFile: (p: string) => { if (!(p in FILES)) throw Object.assign(new Error("ENOENT"), { code: "ENOENT" }); return FILES[p]!; },
    pool: (c: any) => pgliteTestPool(db, "bulls96_session_ingest", c),
  } as never);
  // Fixture soundness: one published attempt, one live pointer, one object-landed job.
  expect(published).toMatchObject({ enabled: true, examined: 1, published: 1 });
  const attempt = (await owner(`SELECT state, to_char(published_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS published_at,
      published_at IS NOT NULL AS stamped FROM sessions.ingest_publication_attempts`)).rows;
  expect(attempt).toHaveLength(1);
  expect(attempt[0]).toMatchObject({ state: "published", stamped: true });
  const pointer = (await owner("SELECT org_id, workspace_id, harness, session_id, coalesce(canonical_session_id, session_id) AS canonical, manifest_key FROM sessions.session_index")).rows;
  expect(pointer).toHaveLength(1);
  const key = pointer[0].manifest_key as string;
  expect(key).toStartWith(`96bulls/${TENANT.orgId}/upload-generations/g=`);
  const landed = (await owner("SELECT message FROM sessions.pipeline_jobs WHERE stage='object-landed'")).rows;
  expect(landed).toHaveLength(1);

  const job = landed[0].message as StageMessage;
  const transact: Transact = (work) => db.transaction((tx) => work(tx));
  const reader = objectStoreManifestReader(store, { provider: "s3", bucket: (job.payload as { bucket: string }).bucket });
  const queue = postgresPipelineQueue(db);
  const index = hoopDbSessionIndex({ db: db as never, reader, queue, current: () => undefined, publication: { schemaGeneration: GEN, transact, ...optIn } });
  const deps: StageQueueDeps = { sink: new InMemoryPipelineEventSink(), deadLetters: new InMemoryDeadLetterStore(), handlers: {
    "object-landed": objectLandedHandler({ senders: { send: (_s, msg) => queue.send(msg) }, quarantine: async () => { throw Error("unexpected quarantine"); }, transformEnabled: false }),
    "session-indexed": sessionIndexedHandler(index),
  } };
  const options = { stage: "object-landed" as const, sql: db, deps, batchSize: 1, leaseSeconds: 30 };
  expect(await pollPostgresStage(options)).toMatchObject({ claimed: 1, acked: 1, retried: 0 });
  const indexedRows = (await owner("SELECT message FROM sessions.pipeline_jobs WHERE stage='session-indexed'")).rows;
  expect(indexedRows).toHaveLength(1);
  const indexed = indexedRows[0].message as StageMessage;
  await owner("UPDATE sessions.pipeline_jobs SET visible_at='2000-01-01' WHERE stage='session-indexed'");
  await opts.beforeAuthority?.({ db, key, publishedAt: attempt[0].published_at });

  // The retired case: a retirement ordered before the worker's final lock. Count what the authority path
  // writes inside that transaction, then roll the whole scenario back (publication-pickup.ts's pattern).
  const rollback = Error("rollback retirement fixture");
  let retired: PublishedHead["retired"] = { outcome: undefined, jobsWritten: -1 };
  await expect(db.transaction(async (tx) => {
    const before = (await tx.query<{ n: number }>("SELECT count(*)::int n FROM sessions.pipeline_jobs")).rows[0]!.n;
    await tx.query("UPDATE sessions.ingest_publication_generations SET state='retired',retired_at=clock_timestamp() WHERE generation=(SELECT generation FROM sessions.ingest_publication_heads WHERE current_key=$1)", [key]);
    const fenced = hoopDbSessionIndex({ db: tx as never, reader, queue: postgresPipelineQueue(tx), current: () => indexed, publication: { schemaGeneration: GEN, transact: (work) => work(tx), ...optIn } });
    const outcome = await sessionIndexedHandler(fenced)(indexed, {} as never, 1);
    const after = (await tx.query<{ n: number }>("SELECT count(*)::int n FROM sessions.pipeline_jobs")).rows[0]!.n;
    retired = { outcome, jobsWritten: after - before };
    throw rollback;
  })).rejects.toBe(rollback);

  // The current case: the real authority transaction.
  const authority = await pollPostgresStage({ ...options, stage: "session-indexed" });
  const ack = opts.expectAuthorityAck ?? true;
  if (ack) expect(authority).toMatchObject({ claimed: 1, acked: 1, retried: 0 });
  const jobsAfter = (await owner("SELECT stage, message FROM sessions.pipeline_jobs ORDER BY id")).rows as PublishedHead["jobsAfter"];
  const p = pointer[0];
  const dir = (await owner("SELECT event_projection_epoch::text AS epoch FROM sessions.session_directory WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4",
    [p.org_id, p.workspace_id, p.harness, p.canonical])).rows;
  // Fixture soundness: the authority transaction projected the directory row and fanned out.
  if (ack) {
    expect(dir).toHaveLength(1);
    expect(jobsAfter.some((j) => j.stage === "summary-short")).toBe(true);
  }
  return {
    db, store, key,
    scope: { orgId: p.org_id, workspaceId: p.workspace_id, harness: p.harness, sessionId: p.session_id, canonicalSessionId: p.canonical },
    publishedAt: attempt[0].published_at, indexed, retired, jobsAfter, epoch: dir[0]?.epoch ?? null, authority,
    close: () => db.close(),
  };
}
