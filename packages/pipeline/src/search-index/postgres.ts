/**
 * Default SearchIndexAdapter: PostgreSQL + pg_trgm.
 *
 * The self-hosted default transcript search profile (helm-charts
 * docs/self-hosted-reference-architecture.md, "Standard (default)"): the
 * Postgres already in the stack, with `pg_trgm` GIN indexes over normalized
 * content prefixes and titles. Text and queries use the contract's JavaScript
 * lowercase operation before literal LIKE matching under COLLATE "C";
 * database-locale ILIKE can disagree with that operation for Unicode text.
 * q is escaped, so `%` and `_` are literal.
 *
 * The table is a derived, rebuildable index in its own schema
 * (`search_index` by default). It is NOT `sessions.session_directory`:
 * decision 0002 keeps transcript text out of the metadata directory. Nothing
 * here runs DDL except `ensureSchema`, an explicit operator step, and it
 * never creates the extension (the HoopDb bootstrap does).
 *
 * Tenancy is enforced twice: every statement filters on org_id/workspace_id,
 * and the table has forced row-level security with the same `tenant_only`
 * policy as `projection_state` (session-pipeline/postgres-search-state.ts):
 * rows are visible and writable only where org_id/workspace_id equal the
 * transaction's `bulls96_read.org_id`/`bulls96_read.workspace_id` settings.
 * Each tenant-scoped statement runs with those settings bound to its own
 * tenant (see `tenantRunner`); with none bound, the policy shows no rows and
 * refuses every write. A superuser or BYPASSRLS role skips the policy, so the
 * read service refuses such a login at startup (session-read-api/lambda.ts).
 *
 * The client is anything with `query(text, params) -> { rows }`: a `pg` Pool
 * or PGlite. Ordering uses COLLATE "C" so the cursor order is code-point
 * order, identical to the other adapters.
 */
import {
  CONTENT_PREFIX_CHARS,
  MAX_UPSERT_BATCH,
  SearchIndexError,
  assertDocument,
  assertPosition,
  assertQuery,
  assertScope,
  assertTenant,
  positionOf,
  type SearchCallContext,
  type SearchEventDocument,
  type SearchFingerprint,
  type SearchHit,
  type SearchIndexAdapter,
  type SearchIndexCapabilities,
  type SearchIndexHealth,
  type SearchPage,
  type SearchQuery,
  type SearchScope,
  type SearchTenant,
} from "./contracts.js";

export interface SqlClient {
  query<T = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: T[] }>;
}

/** Runs `work` in one transaction with the tenant settings bound to exactly
 * this tenant (structurally session-read-api/liveness.ts TenantRunner). */
export type TenantSqlRunner = <T>(orgId: string, workspaceId: string, work: (client: SqlClient) => Promise<T>) => Promise<T>;

/** The same settings, transaction-local, that the `tenant_only` policy reads. */
export const TENANT_SETTINGS_SQL =
  "SELECT set_config('bulls96_read.org_id', $1, true), set_config('bulls96_read.workspace_id', $2, true)";

export interface PostgresSearchIndexConfig {
  readonly id?: string;
  readonly client: SqlClient;
  /**
   * Runs each tenant-scoped statement (query, scan, upsert, delete, erase) in
   * its own tenant transaction, e.g. the read service's pool runner. Without
   * one, each such statement is preceded on `client` by TENANT_SETTINGS_SQL
   * for its tenant. That holds only when `client` is one open transaction (the
   * writer's scope pass via writerTransactionSql); on a bare pool or outside a
   * transaction the transaction-local settings lapse before the statement and
   * the policy shows nothing and refuses writes: it fails closed, never open.
   */
  readonly tenantRunner?: TenantSqlRunner;
  /** Schema of the derived index. Default `search_index`. */
  readonly schema?: string;
  readonly table?: string;
}

const IDENT = /^[a-z_][a-z0-9_]{0,62}$/;

const COLUMNS = [
  "org_id",
  "workspace_id",
  "ts_ms",
  "session_id",
  "source",
  "machine_id",
  "kind",
  "event_id",
  "type",
  "user_id",
  "actor_name",
  "actor_role",
  "subagent_id",
  "project_id",
  "task_title",
  "task_ref",
  "task_ref_id",
  "tags",
  "files_touched",
  "tool_name",
  "tool_status",
  "delivery_record_id",
] as const;

const KEY = "ts_ms, session_id, source, machine_id, kind, event_id";

interface Row {
  org_id: string;
  workspace_id: string;
  ts_ms: string | number;
  session_id: string;
  source: string;
  machine_id: string;
  kind: string;
  event_id: string;
  type: string;
  user_id: string;
  actor_name: string;
  actor_role: string;
  subagent_id: string;
  project_id: string;
  task_title: string;
  task_ref: string;
  task_ref_id: string;
  tags: string[];
  files_touched: string[];
  tool_name: string;
  tool_status: string;
  delivery_record_id: string;
  content: string;
}

function toRecord(d: SearchEventDocument) {
  return {
    org_id: d.orgId,
    workspace_id: d.workspaceId,
    ts_ms: d.tsMs,
    session_id: d.sessionId,
    source: d.source,
    machine_id: d.machineId,
    kind: d.kind,
    event_id: d.eventId,
    type: d.type,
    user_id: d.userId,
    actor_name: d.actorName,
    actor_role: d.actorRole,
    subagent_id: d.subagentId,
    project_id: d.projectId,
    task_title: d.taskTitle,
    task_ref: d.taskRef,
    task_ref_id: d.taskRefId,
    tags: d.tags,
    files_touched: d.filesTouched,
    tool_name: d.toolName,
    tool_status: d.toolStatus,
    delivery_record_id: d.deliveryRecordId,
    content: d.content,
    content_bytes: new TextEncoder().encode(d.content).length,
    content_prefix_folded: [...d.content].slice(0, CONTENT_PREFIX_CHARS).join("").toLowerCase(),
    task_title_folded: d.taskTitle.toLowerCase(),
    fingerprint: d.fingerprint,
    version: d.version,
  };
}

function toHit(r: Row): SearchHit {
  return {
    orgId: r.org_id,
    workspaceId: r.workspace_id,
    tsMs: Number(r.ts_ms),
    sessionId: r.session_id,
    source: r.source,
    machineId: r.machine_id,
    kind: r.kind,
    eventId: r.event_id,
    type: r.type,
    userId: r.user_id,
    actorName: r.actor_name,
    actorRole: r.actor_role,
    subagentId: r.subagent_id,
    projectId: r.project_id,
    taskTitle: r.task_title,
    taskRef: r.task_ref,
    tags: r.tags,
    filesTouched: r.files_touched,
    toolName: r.tool_name,
    toolStatus: r.tool_status,
    content: r.content,
  };
}

/** Escape a literal for LIKE ... ESCAPE '\'. */
export function likeLiteral(q: string): string {
  return `%${q.replace(/[\\%_]/g, (c) => `\\${c}`)}%`;
}

function checkSignal(context?: SearchCallContext): void {
  if (context?.signal?.aborted) throw new SearchIndexError("aborted");
}

export function createPostgresSearchIndex(config: PostgresSearchIndexConfig): SearchIndexAdapter {
  const schema = config.schema ?? "search_index";
  const tableName = config.table ?? "transcript_events";
  if (!IDENT.test(schema) || !IDENT.test(tableName)) throw new SearchIndexError("invalid_argument");
  const table = `"${schema}"."${tableName}"`;
  const db = config.client;

  /** `tenant` set: the statement runs with that tenant's settings bound
   * (config.tenantRunner, else TENANT_SETTINGS_SQL first on the same client). */
  async function run<T>(text: string, params: unknown[], context?: SearchCallContext, tenant?: SearchTenant): Promise<T[]> {
    checkSignal(context);
    try {
      if (!tenant) return (await db.query<T>(text, params)).rows;
      if (config.tenantRunner)
        return (await config.tenantRunner(tenant.orgId, tenant.workspaceId, (client) => client.query<T>(text, params))).rows;
      await db.query(TENANT_SETTINGS_SQL, [tenant.orgId, tenant.workspaceId]);
      return (await db.query<T>(text, params)).rows;
    } catch (error) {
      const code = (error as { code?: unknown })?.code;
      if (code === "42P01" || code === "3F000") throw new SearchIndexError("schema_missing");
      // Backend text can quote row content: only the SQLSTATE class leaves.
      throw new SearchIndexError("unavailable", true, typeof code === "string" ? `sqlstate_${code.slice(0, 2)}` : undefined);
    }
  }

  const capabilities: SearchIndexCapabilities = Object.freeze({
    backend: "postgres",
    filters: Object.freeze(["session", "source", "kind", "project", "since", "until"] as const),
    minIndexedQueryChars: 3,
    readAfterWrite: "immediate",
    synchronousErasure: true,
    versionedUpserts: true,
  });

  return {
    id: config.id ?? "postgres",
    backend: "postgres",
    capabilities: () => capabilities,

    async ensureSchema(context) {
      const ext = await run<{ nspname: string }>(
        `SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'pg_trgm'`,
        [],
        context,
      );
      const trgmSchema = ext[0]?.nspname;
      if (!trgmSchema || !IDENT.test(trgmSchema)) throw new SearchIndexError("schema_missing", false, "pg_trgm");
      const ops = `"${trgmSchema}".gin_trgm_ops`;
      const text = `text COLLATE "C" NOT NULL`;
      const statements = [
        `CREATE SCHEMA IF NOT EXISTS "${schema}"`,
        `CREATE TABLE IF NOT EXISTS ${table} (
          org_id ${text}, workspace_id ${text}, ts_ms bigint NOT NULL,
          session_id ${text}, source ${text}, machine_id ${text}, kind ${text}, event_id ${text},
          type text NOT NULL, user_id text NOT NULL, actor_name text NOT NULL, actor_role text NOT NULL,
          subagent_id text NOT NULL, project_id text NOT NULL, task_title text NOT NULL,
          task_ref text NOT NULL, task_ref_id text NOT NULL,
          tags text[] NOT NULL, files_touched text[] NOT NULL,
          tool_name text NOT NULL, tool_status text NOT NULL, delivery_record_id text NOT NULL,
          content text NOT NULL, content_bytes integer NOT NULL,
          content_prefix_folded ${text}, task_title_folded ${text},
          fingerprint text NOT NULL, version bigint NOT NULL,
          indexed_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (org_id, workspace_id, ${KEY})
        )`,
        `CREATE INDEX IF NOT EXISTS "${tableName}_session_idx" ON ${table} (org_id, workspace_id, session_id, ${KEY})`,
        `CREATE INDEX IF NOT EXISTS "${tableName}_scope_idx" ON ${table} (org_id, workspace_id, source, session_id)`,
        `CREATE INDEX IF NOT EXISTS "${tableName}_content_trgm" ON ${table} USING gin (content_prefix_folded ${ops})`,
        `CREATE INDEX IF NOT EXISTS "${tableName}_title_trgm" ON ${table} USING gin (task_title_folded ${ops})`,
      ];
      // Forced RLS binds the owner too; the policy is created once and then
      // verified exactly, so a tampered or added policy refuses the bootstrap.
      const tenantExpr = `org_id=current_setting('bulls96_read.org_id',true) AND workspace_id=current_setting('bulls96_read.workspace_id',true)`;
      statements.push(
        `ALTER TABLE ${table} ENABLE ROW LEVEL SECURITY`,
        `ALTER TABLE ${table} FORCE ROW LEVEL SECURITY`,
        `DO $policy$ BEGIN
          IF NOT EXISTS(SELECT 1 FROM pg_policies WHERE schemaname='${schema}' AND tablename='${tableName}' AND policyname='tenant_only') THEN
            CREATE POLICY tenant_only ON ${table} USING(${tenantExpr}) WITH CHECK(${tenantExpr});
          END IF;
        END $policy$`,
      );
      for (const statement of statements) await run(statement, [], context);
      const flags = await run<{ enabled: boolean; forced: boolean }>(
        `SELECT relrowsecurity AS enabled, relforcerowsecurity AS forced FROM pg_class WHERE oid=$1::regclass`,
        [table],
        context,
      );
      const policies = await run<{ name: string; command: string; roles: string; permissive: boolean; qual: string | null; check: string | null }>(
        `SELECT polname AS name, polcmd::text AS command, polroles::text AS roles, polpermissive AS permissive,
                pg_get_expr(polqual, polrelid) AS qual, pg_get_expr(polwithcheck, polrelid) AS check
           FROM pg_policy WHERE polrelid=$1::regclass`,
        [table],
        context,
      );
      const normalize = (value: string) => value.replace(/\s+|::text|\(|\)/g, "");
      const expected = normalize(tenantExpr);
      const [policy] = policies;
      if (!flags[0]?.enabled || !flags[0].forced || policies.length !== 1 || policy!.name !== "tenant_only" ||
          policy!.command !== "*" || policy!.roles !== "{0}" || !policy!.permissive ||
          normalize(policy!.qual ?? "") !== expected || normalize(policy!.check ?? "") !== expected)
        throw new SearchIndexError("schema_missing", false, "tenant_policy_mismatch");
    },

    async upsert(docs, context) {
      if (docs.length > MAX_UPSERT_BATCH) throw new SearchIndexError("invalid_argument");
      if (docs.length === 0) return { written: 0 };
      for (const doc of docs) assertDocument(doc);
      // One row per key (the newest version) so ON CONFLICT touches a row once.
      const newest = new Map<string, SearchEventDocument>();
      for (const doc of docs) {
        const key = JSON.stringify([doc.orgId, doc.workspaceId, doc.tsMs, doc.sessionId, doc.source, doc.machineId, doc.kind, doc.eventId]);
        const held = newest.get(key);
        if (!held || held.version <= doc.version) newest.set(key, doc);
      }
      // The policy admits one tenant per statement: one statement per tenant.
      const byTenant = new Map<string, { tenant: SearchTenant; records: ReturnType<typeof toRecord>[] }>();
      for (const doc of newest.values()) {
        const key = JSON.stringify([doc.orgId, doc.workspaceId]);
        const group = byTenant.get(key) ?? { tenant: { orgId: doc.orgId, workspaceId: doc.workspaceId }, records: [] };
        group.records.push(toRecord(doc));
        byTenant.set(key, group);
      }
      const assign = [...COLUMNS.slice(8), "content", "content_bytes", "content_prefix_folded", "task_title_folded", "fingerprint", "version"]
        .map((c) => `${c} = excluded.${c}`)
        .join(", ");
      let written = 0;
      for (const { tenant, records } of byTenant.values()) {
        const rows = await run<{ n: string | number }>(
          `WITH input AS (
             SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(
               org_id text, workspace_id text, ts_ms bigint, session_id text, source text, machine_id text,
               kind text, event_id text, type text, user_id text, actor_name text, actor_role text,
               subagent_id text, project_id text, task_title text, task_ref text, task_ref_id text,
               tags jsonb, files_touched jsonb, tool_name text, tool_status text, delivery_record_id text,
               content text, content_bytes integer, content_prefix_folded text, task_title_folded text,
               fingerprint text, version bigint)
           ), written AS (
             INSERT INTO ${table} (${COLUMNS.join(", ")}, content, content_bytes, content_prefix_folded, task_title_folded, fingerprint, version, indexed_at)
             SELECT ${COLUMNS.map((c) => (c === "tags" || c === "files_touched" ? `ARRAY(SELECT jsonb_array_elements_text(${c}))` : c)).join(", ")},
                    content, content_bytes, content_prefix_folded, task_title_folded, fingerprint, version, now()
               FROM input
             ON CONFLICT (org_id, workspace_id, ${KEY}) DO UPDATE SET ${assign}, indexed_at = now()
               WHERE ${table}.version <= excluded.version
             RETURNING 1
           )
           SELECT count(*) AS n FROM written`,
          [JSON.stringify(records)],
          context,
          tenant,
        );
        written += Number(rows[0]?.n ?? 0);
      }
      return { written };
    },

    async deleteEvents(tenant, positions, context) {
      assertTenant(tenant);
      if (positions.length > MAX_UPSERT_BATCH) throw new SearchIndexError("invalid_argument");
      if (positions.length === 0) return { deleted: 0 };
      for (const p of positions) assertPosition(p);
      const rows = await run<{ n: string | number }>(
        `WITH gone AS (
           DELETE FROM ${table} t USING jsonb_to_recordset($3::jsonb) AS x(
             ts_ms bigint, session_id text, source text, machine_id text, kind text, event_id text)
           WHERE t.org_id = $1 AND t.workspace_id = $2 AND t.ts_ms = x.ts_ms AND t.session_id = x.session_id
             AND t.source = x.source AND t.machine_id = x.machine_id AND t.kind = x.kind AND t.event_id = x.event_id
           RETURNING 1)
         SELECT count(*) AS n FROM gone`,
        [
          tenant.orgId,
          tenant.workspaceId,
          JSON.stringify(
            positions.map((p) => ({
              ts_ms: p.tsMs,
              session_id: p.sessionId,
              source: p.source,
              machine_id: p.machineId,
              kind: p.kind,
              event_id: p.eventId,
            })),
          ),
        ],
        context,
        tenant,
      );
      return { deleted: Number(rows[0]?.n ?? 0) };
    },

    async eraseScope(scope, context) {
      assertScope(scope);
      await run(
        `DELETE FROM ${table} WHERE org_id = $1 AND workspace_id = $2 AND source = $3 AND session_id = ANY($4::text[])`,
        [scope.orgId, scope.workspaceId, scope.source, [...scope.sessionIds]],
        context,
        scope,
      );
    },

    async scanFingerprints(scope: SearchScope, options, context) {
      assertScope(scope);
      if (!Number.isSafeInteger(options.fromMs) || !Number.isInteger(options.limit) || options.limit < 1) {
        throw new SearchIndexError("invalid_argument");
      }
      const rows = await run<Row & { fingerprint: string }>(
        `SELECT workspace_id, ${KEY}, fingerprint FROM ${table}
          WHERE org_id = $1 AND workspace_id = $2 AND source = $3 AND session_id = ANY($4::text[]) AND ts_ms >= $5
          ORDER BY ${KEY} LIMIT $6`,
        [scope.orgId, scope.workspaceId, scope.source, [...scope.sessionIds], options.fromMs, options.limit + 1],
        context,
        scope,
      );
      const truncated = rows.length > options.limit;
      return {
        truncated,
        rows: rows.slice(0, options.limit).map(
          (r): SearchFingerprint => ({
            tsMs: Number(r.ts_ms),
            sessionId: r.session_id,
            source: r.source,
            machineId: r.machine_id,
            kind: r.kind,
            eventId: r.event_id,
            workspaceId: r.workspace_id,
            fingerprint: r.fingerprint,
          }),
        ),
      };
    },

    async query(query: SearchQuery, context): Promise<SearchPage> {
      assertQuery(query);
      const params: unknown[] = [query.tenant.orgId, query.tenant.workspaceId];
      const bind = (v: unknown) => {
        params.push(v);
        return `$${params.length}`;
      };
      const where = ["org_id = $1", "workspace_id = $2"];
      if (query.sessionId !== undefined) where.push(`session_id = ${bind(query.sessionId)}`);
      if (query.source !== undefined) where.push(`source = ${bind(query.source)}`);
      if (query.kind !== undefined) where.push(`kind = ${bind(query.kind)}`);
      if (query.projectId !== undefined) where.push(`project_id = ${bind(query.projectId)}`);
      if (query.sinceMs !== undefined) where.push(`ts_ms >= ${bind(query.sinceMs)}`);
      if (query.untilMs !== undefined) where.push(`ts_ms <= ${bind(query.untilMs)}`);
      if (query.q !== undefined) {
        const needle = bind(likeLiteral(query.q.toLowerCase()));
        where.push(
          `(content_prefix_folded LIKE ${needle} ESCAPE '\\' OR task_title_folded LIKE ${needle} ESCAPE '\\')`,
        );
      }
      if (query.cursor !== undefined) {
        const c = query.cursor;
        const tuple = [c.tsMs, c.sessionId, c.source, c.machineId, c.kind, c.eventId].map(bind).join(", ");
        where.push(`(${KEY}) ${query.order === "asc" ? ">" : "<"} (${tuple})`);
      }
      const dir = query.order === "asc" ? "ASC" : "DESC";
      const order = KEY.split(", ").map((c) => `${c} ${dir}`).join(", ");
      const content =
        query.maxContentChars === undefined ? "content" : `left(content, ${bind(query.maxContentChars)}::integer)`;
      const limit = bind(query.limit + 1);
      const rows = await run<Row>(
        `SELECT ${COLUMNS.join(", ")}, ${content} AS content FROM ${table}
          WHERE ${where.join(" AND ")} ORDER BY ${order} LIMIT ${limit}`,
        params,
        context,
        query.tenant,
      );
      const hits = rows.slice(0, query.limit).map(toHit);
      const more = rows.length > query.limit;
      return more ? { hits, next: positionOf(hits[hits.length - 1]!) } : { hits };
    },

    async health(context): Promise<SearchIndexHealth> {
      try {
        const rows = await run<{ present: boolean }>(
          `SELECT to_regclass($1) IS NOT NULL AS present`,
          [`${schema}.${tableName}`],
          context,
        );
        return rows[0]?.present ? { ok: true, code: "ok" } : { ok: false, code: "schema_missing" };
      } catch {
        return { ok: false, code: "unreachable" };
      }
    },
  };
}

