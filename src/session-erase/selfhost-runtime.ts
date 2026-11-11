/** Self-host composition only. No imports from this module enter the hosted Worker. */
import { createHash } from "node:crypto";
import pg from "pg";
import { createPostgresSearchIndex } from "../../packages/pipeline/src/search-index/postgres.ts";
import { createOpenSearchSearchIndex, DEFAULT_OPENSEARCH_INDEX_PREFIX } from "../../packages/pipeline/src/search-index/opensearch.ts";
import { createS3Eraser } from "../migration/neon-s3-erasure/eraser.ts";
import { postgresConnectionString, postgresTlsOptions } from "../shared/postgres-tls.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import type { SqlRun } from "../session-ingest/ledger-sql.ts";
import type { SessionEraseConfig } from "../session-ingest/session-erase.ts";
import { INBOX_NAMESPACE } from "../session-ingest/async-admission.ts";
import { eraseDiscoveredSessionBytes } from "./selfhost-eraser.ts";
import { sessionEraseConfig as bytesConfig, sessionEraseCredentials } from "./selfhost-eraser-main.ts";
import { sessionEraseWorker, sqlEraseLedger, sqlSessionWriteGuard, type EraseSearchPort, type EraseStoreIdentity, type EraseWork } from "./runtime.ts";
import { SessionEraseServiceError } from "./receipt.ts";

function writerEndpoint(env: NodeJS.ProcessEnv): string | null {
  if (!env.LOCKER_ENDPOINT) return null;
  try { return assertDestinationUrl("LOCKER_ENDPOINT", env.LOCKER_ENDPOINT, destinationPolicy(env), { allowPath: false }).origin; }
  catch { throw Error("96bulls: operator erase object-store endpoint invalid"); }
}
export function eraseStoreIdentity(env: NodeJS.ProcessEnv, keyRoot: string): EraseStoreIdentity {
  if (env.LOCKER_PROVIDER && env.LOCKER_PROVIDER !== "s3") throw Error("96bulls: operator erase requires versioned S3");
  const bucket = env.LOCKER_BUCKET;
  if (!bucket) throw Error("96bulls: LOCKER_BUCKET required for operator erase");
  const endpoint = writerEndpoint(env);
  const backend = env.SEARCH_INDEX_BACKEND || "postgres";
  let searchIdentity: unknown;
  if (backend === "postgres") searchIdentity = [backend, env.SEARCH_INDEX_POSTGRES_SCHEMA || "search_index"];
  else if (backend === "opensearch") {
    if (!env.SEARCH_INDEX_OPENSEARCH_URL) throw Error("96bulls: operator erase search endpoint required");
    try { searchIdentity = [backend, assertDestinationUrl("SEARCH_INDEX_OPENSEARCH_URL", env.SEARCH_INDEX_OPENSEARCH_URL, destinationPolicy(env)).href,
      env.SEARCH_INDEX_OPENSEARCH_INDEX_PREFIX ?? DEFAULT_OPENSEARCH_INDEX_PREFIX]; }
    catch { throw Error("96bulls: operator erase search endpoint invalid"); }
  } else throw Error("96bulls: operator erase search backend unsupported");
  // Credentials may rotate. Resource identity must not change behind an outstanding receipt.
  const storeRef = createHash("sha256").update(JSON.stringify(["s3", bucket, env.LOCKER_REGION || "us-east-1", endpoint, keyRoot, searchIdentity])).digest("hex");
  return { bucket, keyRoot, storeRef };
}

const SEARCH_ROLE = "bulls96_session_erase_search";
function searchTarget(raw: string) {
  try {
  const url = new URL(postgresConnectionString(raw));
  if (url.hash || !url.username || !url.password || !url.pathname || url.pathname === "/") throw Error("96bulls: operator erase search database configuration invalid");
  return { host: url.hostname.replace(/^\[(.*)\]$/, "$1"), port: url.port ? Number(url.port) : 5432,
    user: decodeURIComponent(url.username), password: decodeURIComponent(url.password), database: decodeURIComponent(url.pathname.slice(1)) };
  } catch { throw Error("96bulls: operator erase search database configuration invalid"); }
}

/** A separate delete-only search login; the ingest login gains no search table access. */
async function postgresSearch(env: NodeJS.ProcessEnv, metadata: pg.PoolConfig): Promise<{ port: EraseSearchPort; close(): Promise<void> }> {
  if (!env.SESSION_ERASE_SEARCH_POSTGRES_URL) throw Error("96bulls: SESSION_ERASE_SEARCH_POSTGRES_URL required for operator erase");
  const target = searchTarget(env.SESSION_ERASE_SEARCH_POSTGRES_URL);
  if (target.user !== SEARCH_ROLE || target.host !== metadata.host || target.port !== (metadata.port ?? 5432) || target.database !== metadata.database)
    throw Error("96bulls: operator erase search must use its dedicated login on the metadata database");
  const pool = new pg.Pool({ ...target, ssl: postgresTlsOptions(env), max: 2,
    application_name: "96bulls-session-erase-search", options: "-c statement_timeout=5s -c lock_timeout=2s -c idle_in_transaction_session_timeout=10s",
    connectionTimeoutMillis: 2_000, idleTimeoutMillis: 60_000 });
  pool.on("error", () => {});
  try {
    const { rows } = await pool.query(`SELECT current_user=session_user AND current_user=$1 AND NOT r.rolsuper
      AND NOT r.rolbypassrls AND NOT r.rolcreaterole AND NOT r.rolcreatedb AND NOT r.rolreplication AND NOT r.rolinherit
      AND NOT EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid OR roleid=r.oid) AS safe FROM pg_roles r WHERE r.rolname=current_user`, [SEARCH_ROLE]);
    if (rows.length !== 1 || rows[0].safe !== true) throw Error("96bulls: operator erase search login refused");
    const acl = await pool.query(`SELECT
      NOT has_table_privilege(current_user,'search_index.transcript_events','SELECT,INSERT,UPDATE,TRUNCATE,REFERENCES,TRIGGER')
      AND NOT has_table_privilege(current_user,'search_index.projection_state','SELECT,INSERT,UPDATE,TRUNCATE,REFERENCES,TRIGGER')
      AND has_table_privilege(current_user,'search_index.transcript_events','DELETE')
      AND has_table_privilege(current_user,'search_index.projection_state','DELETE')
      AND NOT EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='search_index' AND c.relname IN('transcript_events','projection_state') AND a.attnum>0 AND NOT a.attisdropped
        AND(has_column_privilege(current_user,c.oid,a.attnum,'INSERT,UPDATE') OR(
          has_column_privilege(current_user,c.oid,a.attnum,'SELECT') AND a.attname NOT IN('org_id','workspace_id','source','session_id','canonical_session_id'))))
      AND(SELECT count(*)=2 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='search_index' AND c.relname IN('transcript_events','projection_state') AND c.relrowsecurity AND c.relforcerowsecurity)
      AS safe`);
    if (acl.rows[0]?.safe !== true) throw Error("96bulls: operator erase search privileges refused");
    const tenant = async <T>(orgId: string, workspaceId: string, work: (client: any) => Promise<T>): Promise<T> => {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await client.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true)", [orgId, workspaceId]);
        const result = await work(client); await client.query("COMMIT"); return result;
      } catch (error) { await client.query("ROLLBACK").catch(() => {}); throw error; }
      finally { client.release(); }
    };
    const adapter = createPostgresSearchIndex({ client: pool as any, tenantRunner: tenant });
    return { port: { async erase(key, sessionIds) {
      await adapter.eraseScope({ ...key, sessionIds });
      await tenant(key.orgId, key.workspaceId, async client => {
        const values = [key.orgId, key.workspaceId, key.source, [...sessionIds]];
        await client.query("DELETE FROM search_index.projection_state WHERE org_id=$1 AND workspace_id=$2 AND source=$3 AND(session_id=ANY($4::text[]) OR canonical_session_id=ANY($4::text[]))", values);
        const proof = await client.query("SELECT EXISTS(SELECT 1 FROM search_index.projection_state WHERE org_id=$1 AND workspace_id=$2 AND source=$3 AND(session_id=ANY($4::text[]) OR canonical_session_id=ANY($4::text[]))) remains", values);
        if (proof.rows[0]?.remains !== false) throw new SessionEraseServiceError();
      });
    } }, close: () => pool.end() };
  } catch { await pool.end(); throw Error("96bulls: operator erase search startup refused"); }
}

async function searchPort(env: NodeJS.ProcessEnv, metadata: pg.PoolConfig) {
  if (env.SEARCH_INDEX_POSTGRES_SCHEMA && env.SEARCH_INDEX_POSTGRES_SCHEMA !== "search_index") throw Error("96bulls: operator erase requires the search_index schema");
  if (!env.SEARCH_INDEX_BACKEND || env.SEARCH_INDEX_BACKEND === "postgres") return postgresSearch(env, metadata);
  if (env.SEARCH_INDEX_BACKEND !== "opensearch") throw Error("96bulls: operator erase search backend unsupported");
  const url = env.SEARCH_INDEX_OPENSEARCH_URL;
  if (!url) throw Error("96bulls: SEARCH_INDEX_OPENSEARCH_URL required for operator erase");
  assertDestinationUrl("SEARCH_INDEX_OPENSEARCH_URL", url, destinationPolicy(env));
  const username = env.SESSION_ERASE_OPENSEARCH_USERNAME, password = env.SESSION_ERASE_OPENSEARCH_PASSWORD;
  if (!username || !password) throw Error("96bulls: dedicated operator erase OpenSearch credentials required");
  const adapter = createOpenSearchSearchIndex({ url, indexPrefix: env.SEARCH_INDEX_OPENSEARCH_INDEX_PREFIX,
    auth: { kind: "basic", username, password }, requestTimeoutMs: 10_000 });
  return { port: { async erase(key: EraseWork, sessionIds: readonly string[]) { await adapter.eraseScope({ ...key, sessionIds }); } } as EraseSearchPort,
    close: async () => {} };
}

export async function buildSelfhostEraseRuntime(input: {
  env: NodeJS.ProcessEnv; keyRoot: string; api: SessionEraseConfig; enforced?: true; bytesEnabled?: true;
  run: SqlRun; pool: pg.Pool; metadata: pg.PoolConfig; log?: (fields: Record<string, unknown>) => void;
}) {
  if (!input.enforced) {
    if (input.api.enabled || input.bytesEnabled) throw Error("96bulls: operator erase requires fence enforcement");
    return undefined;
  }
  const { env, run } = input;
  const required = ["ingest_operator_runtime_begin(text,text,text,text,text,text,text,text,text,text)", "ingest_operator_runtime_write_begin(jsonb)",
    "ingest_operator_runtime_write_finish(text,text,uuid)", "ingest_operator_runtime_next(uuid)", "ingest_operator_runtime_authorize(text,text,text,text,uuid)",
    "ingest_operator_runtime_release(text,text,text,text,uuid)", "ingest_operator_erase_receipt(text,text,text,text)",
    "ingest_operator_runtime_purge(text,text,text,text,uuid)", "ingest_operator_runtime_step(text,text,text,text,uuid,text,text,bigint,text)"];
  const ready = await run("SELECT bool_and(to_regprocedure(fn) IS NOT NULL AND has_function_privilege(current_user,to_regprocedure(fn),'EXECUTE')) ready FROM unnest($1::text[]) fn", [required.map(fn => `sessions.${fn}`)]);
  if (ready.rows[0]?.ready !== true) throw Error("96bulls: operator erase runtime migration and grants required");
  const identity = eraseStoreIdentity(env, input.keyRoot);
  const ledger = sqlEraseLedger(run, identity);
  const guard = sqlSessionWriteGuard(run);
  if (!input.bytesEnabled) return { service: ledger, withSessionWrites: guard, start() {}, close: async () => {} };
  // Holding the consumer's search lock needs one connection while lease/purge RPCs use another.
  if ((input.metadata.max ?? 4) < 2) throw Error("96bulls: operator erase requires an ingest pool of at least two connections");
  const config = bytesConfig(env);
  if (config.dryRun || config.bucket !== identity.bucket || config.keyRoot !== identity.keyRoot || config.region !== (env.LOCKER_REGION || "us-east-1")
    || (config.endpoint ?? null) !== writerEndpoint(env)) throw Error("96bulls: operator eraser must target the writer store, without dry run");
  const eraser = createS3Eraser({ bucket: config.bucket, region: config.region, endpoint: config.endpoint, credentials: sessionEraseCredentials(env),
    fetch: (url, init) => fetch(url, { ...init, signal: init?.signal ? AbortSignal.any([init.signal, AbortSignal.timeout(10_000)]) : AbortSignal.timeout(10_000) }) });
  const search = await searchPort(env, input.metadata);
  const worker = sessionEraseWorker({ ledger, identity, search: search.port, log: input.log,
    bytes: {
      erase: (key, authorize) => eraseDiscoveredSessionBytes(eraser, key, { keyRoot: config.keyRoot, bucket: config.bucket, concurrency: config.concurrency,
        authorize, log: input.log }),
      async stagesAbsent(key, authorize) {
        for (const prefix of [`${config.keyRoot}/${key.orgId}/live-batches/v1/ws=${key.workspaceId}/`,
          `${config.keyRoot}/${key.orgId}/buffered-batches/`, `${config.keyRoot}/${INBOX_NAMESPACE}/`]) {
          const proof = await eraser.verifyErased(prefix, { authorize });
          if (proof.status !== "erased") return false;
        }
        return true;
      },
    },
    async searchLock(work, action) {
      const client = await input.pool.connect();
      try {
        await client.query("BEGIN");
        // Exact key format of session-pipeline/pointer.ts scopeLockKey, in global C order.
        for (const id of [...work.sessionIds].sort())
          await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", [["96bulls:search", work.orgId, work.workspaceId, work.source, id].join("\u001f")]);
        const result = await action(); await client.query("COMMIT"); return result;
      } catch (error) { await client.query("ROLLBACK").catch(() => {}); throw error; }
      finally { client.release(); }
    },
  });
  return { service: ledger, withSessionWrites: guard, start: worker.start,
    async close() { await worker.stop(); await search.close(); } };
}
