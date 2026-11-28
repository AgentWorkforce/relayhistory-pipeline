/**
 * The qualification's two backends, behind one shape:
 *
 *   pglite+fs     PGlite with every migration in the checkout (privacy-authority-support authorityFixture) and a
 *                 filesystem object store under $TMPDIR. Single connection, superuser session: fast, for CI.
 *   pg<major>+minio  a real PostgreSQL (a scratch database on the given server, migrated by the real migrator as the
 *                 owner role, hold-harness/pg.ts) and a versioned MinIO scratch bucket (hold-harness/s3.ts). Run it
 *                 under `bun src/session-ingest/__tests__/hold-harness/run.ts --pg 17 -- <file>`, which starts both
 *                 disposable containers and passes their coordinates in the environment.
 *
 * Connections: `owner` is the migration owner (the lifecycle and erase entry points, the backlog tools' reads, the
 * owner-only tables); `admin` is the fixture superuser, used ONLY for the seeding the fixture documents (rows written
 * with session_replication_role = replica); `ingest` runs one statement as bulls96_session_ingest in its own
 * transaction, as the edge's SQL ledger does. Everything synthetic; nothing reads a production endpoint.
 */
import pg from "pg";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { ObjectStore } from "../../src/shared/vendor.ts";
import { authorityFixture, baseline, epoch, generation, identity, INDEX_SQL, ROOT } from "../../src/session-ingest/__tests__/privacy-authority-support.ts";
import { migrateAsOwner, scratchDatabase, serverMajor } from "../../src/session-ingest/__tests__/hold-harness/pg.ts";
import { scratchBucket, type S3Fixture } from "../../src/session-ingest/__tests__/hold-harness/s3.ts";

export interface Sql { query<T = Record<string, any>>(text: string, params?: unknown[]): Promise<{ rows: T[] }> }
export interface Backend {
  kind: string;
  owner: Sql;
  /** Run `fn` with the superuser session in session_replication_role = replica (the fixture's documented seeding). */
  replica<T>(fn: (admin: Sql) => Promise<T>): Promise<T>;
  /** One statement as bulls96_session_ingest, in its own transaction (the SQL ledger's runner). */
  ingest(text: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
  store: ObjectStore;
  close(): Promise<void>;
}

const LAZY_POLICY = () => ({ baselineMode: "lazy-current", generation, sourceIdentitySha256: identity, controlEpoch: epoch, privacyBaselineSha256: baseline,
  retentionContractSha256: "b".repeat(64), sourceProofSha256: "d".repeat(64), validUntil: new Date(Date.now() + 6 * 3600_000).toISOString(), issuer: "relayhistory-cloud" });
const SEAT_INIT = () => ({ protocol: "delivery-acceptance-floor-v1", controlEpoch: epoch, capturedAt: "2026-11-04T00:00:00.000000Z",
  sourceSequences: ["sessions.delivery_catalog_acceptance_seq", "sessions.delivery_projection_acceptance_seq"].map((sequenceRegclass, i) => ({ sequenceRegclass, sequenceOid: String(i + 1), allocatedHighWater: "9007199254740993", isCalled: true, incrementBy: "1", cycle: false })),
  allocatedHighWater: "9007199254740993", generation, sourceIdentitySha256: identity, sourceProofSha256: "d".repeat(64) });
const SEAL_SQL = "INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot) VALUES($1,$2,$3,0,0,$4,'{}')";
const SEAL_PARAMS = () => [identity, epoch, baseline, "e".repeat(64)];

export async function pgliteBackend(): Promise<Backend> {
  const f = await authorityFixture();
  await f.seal();
  // The lifecycle functions require their slots' ledger rows (this fixture applies migrations without the migrator).
  await f.db.exec("CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations(name text PRIMARY KEY, sha256 text)");
  for (const n of ["upstream:20261105062320_ingest_org_admission_fences.sql", "upstream:20261105062340_ingest_org_erasure_history.sql"])
    await f.db.query("INSERT INTO sessions.__96bulls_migrations(name) VALUES($1) ON CONFLICT DO NOTHING", [n]);
  const owner: Sql = { query: (t, p) => f.db.query(t, (p ?? []) as never) as never };
  return {
    kind: "pglite+fs", owner,
    async replica(fn) {
      await f.db.exec("SET session_replication_role = replica");
      try { return await fn(owner); } finally { await f.db.exec("SET session_replication_role = origin"); }
    },
    ingest: (t, p) => f.run(t, (p ?? []) as unknown[]),
    store: f.deps.target.store, close: f.close,
  };
}

export async function postgresBackend(opts: { pgUrl: string; s3: S3Fixture; expectMajor?: number }): Promise<Backend> {
  if (opts.expectMajor !== undefined && (await serverMajor(opts.pgUrl)) !== opts.expectMajor) throw Error("postgres_major_mismatch");
  const db = await scratchDatabase(opts.pgUrl, { bootstrap: true, prefix: "cutover" });
  const clients: pg.Client[] = [];
  const bucket = await scratchBucket(opts.s3, { prefix: "cutover", versioned: true }).catch(async (e) => { await db.drop(); throw e; });
  const close = async () => {
    for (const c of clients.splice(0)) await c.end().catch(() => {});
    await bucket.drop().catch(() => {});
    await db.drop().catch(() => {});
  };
  try {
    await migrateAsOwner(db.ownerUrl);
    const connect = async (url: string) => { const c = new pg.Client({ connectionString: url }); await c.connect(); clients.push(c); return c; };
    const ownerC = await connect(db.ownerUrl), adminC = await connect(db.fixtureUrl), ingestC = await connect(db.fixtureUrl);
    const owner: Sql = { query: (t, p) => ownerC.query(t, (p ?? []) as unknown[]) as never };
    // The C3 post-apply index job (non-transactional in production), as the owner.
    await ownerC.query(readFileSync(join(ROOT, INDEX_SQL), "utf8"));
    // The reader's gate reads the role-validation PASS. The migrator's role step writes it with the schema's real
    // generation; the fixture's lazy policy (and the PGlite fixture) use the synthetic `generation`, so the row is
    // re-pointed to it here: seeding below the entry point, named in the report's fixture notes.
    await ownerC.query("CREATE TABLE IF NOT EXISTS sessions.__96bulls_role_validation(name text PRIMARY KEY,generation text,status text)");
    await ownerC.query("INSERT INTO sessions.__96bulls_role_validation VALUES('session_directory_search_worker',$1,'pass') ON CONFLICT (name) DO UPDATE SET generation=EXCLUDED.generation,status='pass'", [generation]);
    await ownerC.query("SELECT sessions.ingest_lazy_policy_set($1::jsonb)", [JSON.stringify(LAZY_POLICY())]);
    await ownerC.query("SELECT sessions.ingest_receipt_seat_initialize($1::jsonb)", [JSON.stringify(SEAT_INIT())]);
    await ownerC.query(SEAL_SQL, SEAL_PARAMS());
    const major = (await serverMajor(opts.pgUrl));
    return {
      kind: `pg${major}+minio`, owner,
      async replica(fn) {
        await adminC.query("SET session_replication_role = replica");
        try { return await fn({ query: (t, p) => adminC.query(t, (p ?? []) as unknown[]) as never }); }
        finally { await adminC.query("SET session_replication_role = origin"); }
      },
      async ingest(t, p) {
        await ingestC.query("BEGIN ISOLATION LEVEL READ COMMITTED");
        try {
          await ingestC.query("SET LOCAL ROLE bulls96_session_ingest");
          const r = await ingestC.query(t, (p ?? []) as unknown[]);
          await ingestC.query("COMMIT");
          return r as never;
        } catch (e) { await ingestC.query("ROLLBACK").catch(() => {}); throw e; }
      },
      store: bucket.store(), close,
    };
  } catch (e) { await close(); throw e; }
}
