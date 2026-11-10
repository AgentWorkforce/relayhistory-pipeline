/** Content-free portable index completeness. Explicit owner bootstrap only;
 * never initialized by a service implicitly. ensureSchema's only GRANTs are the
 * read API role's read-only access (schema USAGE, the nine projection_state
 * columns partials reads, and SELECT on transcript_events), issued when that role exists.
 * Readers compare the recorded metadata epoch with current pointer-derived
 * directory readiness, so a failed first pass or newer pointer cannot look
 * complete merely because an older pass once succeeded. */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { TenantRunner } from "../session-read-api/liveness.ts";
import type { SearchProjectionState } from "./search-adapter.ts";

/** A pg_policy-shaped row, as read from pg_policy or pg_policies. */
export interface TenantPolicyRow {
  name: string;
  command: string;
  /** "{0}" for TO PUBLIC, else the comma-joined grantee list. */
  roles: string;
  permissive: boolean;
  qual: string | null;
  check: string | null;
}

const normalizePolicyExpr = (value: string): string => value.replace(/\s+|::text|\(|\)/g, "");

/** The exact tenant gate: one permissive FOR ALL TO PUBLIC tenant_only policy binding both GUCs. */
export function isTenantOnlyPolicy(row: TenantPolicyRow): boolean {
  const expected = normalizePolicyExpr("org_id=current_setting('bulls96_read.org_id',true) AND workspace_id=current_setting('bulls96_read.workspace_id',true)");
  return (
    row.name === "tenant_only" &&
    row.command === "*" &&
    row.roles === "{0}" &&
    row.permissive &&
    normalizePolicyExpr(row.qual ?? "") === expected &&
    normalizePolicyExpr(row.check ?? "") === expected
  );
}

export function postgresSearchState(options: { reader?: TenantRunner; backendId: string; fromMs: number; schema?: string }): SearchProjectionState & { ensureSchema(owner: SqlExecutor): Promise<void> } {
  const schema = options.schema ?? "search_index";
  const { backendId, fromMs, reader } = options;
  if (!/^[a-z_][a-z0-9_]{0,62}$/.test(schema) || !/^[a-zA-Z0-9._-]{1,128}$/.test(options.backendId) || !Number.isSafeInteger(options.fromMs) || options.fromMs < 0) throw new Error("search state configuration invalid");
  const table = `"${schema}".projection_state`;
  return {
    async ensureSchema(owner) {
      await owner.query(`CREATE SCHEMA IF NOT EXISTS "${schema}"`);
      await owner.query(`CREATE TABLE IF NOT EXISTS ${table} (
        backend_id text NOT NULL, org_id text NOT NULL, workspace_id text NOT NULL,
        source text NOT NULL, session_id text NOT NULL, canonical_session_id text NOT NULL,
        source_epoch bigint NOT NULL, coverage_from_ms bigint NOT NULL,
        revision bigint NOT NULL CHECK(revision>=0), row_count bigint NOT NULL CHECK(row_count>=0),
        complete boolean NOT NULL, updated_at timestamptz NOT NULL,
        PRIMARY KEY(backend_id,org_id,workspace_id,source,session_id)
      )`);
      await owner.query(`ALTER TABLE ${table} ENABLE ROW LEVEL SECURITY`);
      await owner.query(`ALTER TABLE ${table} FORCE ROW LEVEL SECURITY`);
      await owner.query(`DO $policy$ BEGIN
        IF NOT EXISTS(SELECT 1 FROM pg_policies WHERE schemaname='${schema}' AND tablename='projection_state' AND policyname='tenant_only') THEN
          CREATE POLICY tenant_only ON ${table}
            USING(org_id=current_setting('bulls96_read.org_id',true) AND workspace_id=current_setting('bulls96_read.workspace_id',true))
            WITH CHECK(org_id=current_setting('bulls96_read.org_id',true) AND workspace_id=current_setting('bulls96_read.workspace_id',true));
        END IF;
      END $policy$`);
      const { rows } = await owner.query<{ name: string; command: string; roles: string; permissive: boolean; qual: string | null; check: string | null }>(
        `SELECT polname AS name,polcmd::text AS command,
          CASE WHEN polroles = '{0}' THEN '{0}'
               ELSE (SELECT string_agg(r.rolname, ',' ORDER BY r.rolname) FROM pg_roles r WHERE r.oid = ANY(polroles)) END AS roles,
          polpermissive AS permissive,
          pg_get_expr(polqual,polrelid) AS qual,pg_get_expr(polwithcheck,polrelid) AS check
         FROM pg_policy WHERE polrelid=$1::regclass`, [table]);
      const tenant = rows.find((r) => r.name === "tenant_only");
      if (!tenant || !isTenantOnlyPolicy(tenant)) throw new Error("search state tenant policy mismatch");
      // Exactly one policy may exist: a writer-targeted extra (the removed
      // search_consumer_trusted policy, or any other) would both collide
      // with #330's exact-one-policy check and widen the tenant gate, so it
      // fails closed here. The writer binds the tenant GUCs per scope
      // instead (every search write runs inside a tenant transaction).
      if (rows.length !== 1) throw new Error("search state tenant policy mismatch");
      const readRole = await owner.query<{ present: boolean }>(
        "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='bulls96_read_api') AS present",
      );
      if (readRole.rows[0]?.present) {
        const eventTable = `"${schema}".transcript_events`;
        const eventRelation = await owner.query<{ present: boolean; enabled: boolean | null; forced: boolean | null }>(
          `SELECT to_regclass($1) IS NOT NULL AS present,
                  c.relrowsecurity AS enabled,c.relforcerowsecurity AS forced
             FROM (SELECT to_regclass($1) AS oid) r
             LEFT JOIN pg_class c ON c.oid=r.oid`,
          [eventTable],
        );
        if (eventRelation.rows[0]?.present) {
          const eventPolicies = await owner.query<TenantPolicyRow>(
            `SELECT polname AS name,polcmd::text AS command,
              CASE WHEN polroles = '{0}' THEN '{0}'
                   ELSE (SELECT string_agg(r.rolname, ',' ORDER BY r.rolname) FROM pg_roles r WHERE r.oid = ANY(polroles)) END AS roles,
              polpermissive AS permissive,
              pg_get_expr(polqual,polrelid) AS qual,pg_get_expr(polwithcheck,polrelid) AS check
             FROM pg_policy WHERE polrelid=$1::regclass`,
            [eventTable],
          );
          if (eventRelation.rows[0]?.enabled !== true || eventRelation.rows[0]?.forced !== true ||
              eventPolicies.rows.length !== 1 || !isTenantOnlyPolicy(eventPolicies.rows[0]!))
            throw new Error("search state event tenant policy mismatch");
        }
        // The read API's completeness check (partials) reads this table as bulls96_read_api, under the
        // tenant_only policy above. This schema is created here at consumer start, after the role
        // bootstrap has run, so the grant lives here. Without it every /events read is a 503
        // ("permission denied for schema"). Only the columns partials reads; no writes. If the events
        // table exists, its SELECT grant is issued only after its forced tenant_only policy verifies.
        await owner.query(`GRANT USAGE ON SCHEMA "${schema}" TO bulls96_read_api`);
        await owner.query(`GRANT SELECT (backend_id,org_id,workspace_id,source,session_id,canonical_session_id,source_epoch,coverage_from_ms,complete)
          ON ${table} TO bulls96_read_api`);
        if (eventRelation.rows[0]?.present)
          await owner.query(`GRANT SELECT ON ${eventTable} TO bulls96_read_api`);
      }
    },
    async fromMs() { return fromMs; },
    async record(tx, scope, value) {
      if (![value.revision, value.rows, value.at.getTime()].every(n => Number.isSafeInteger(n) && n >= 0)) throw new Error("search state record invalid");
      await tx.query("SELECT set_config('bulls96_read.org_id',$1,true),set_config('bulls96_read.workspace_id',$2,true)", [scope.orgId, scope.workspaceId]);
      await tx.query(`INSERT INTO ${table}(backend_id,org_id,workspace_id,source,session_id,canonical_session_id,source_epoch,coverage_from_ms,revision,row_count,complete,updated_at)
        VALUES($1,$2,$3,$4,$5,$6,coalesce((SELECT event_projection_epoch FROM sessions.session_directory
          WHERE org_id=$2 AND workspace_id=$3 AND harness=$4 AND session_id=$6),-1),$7,$8,$9,$10,$11)
        ON CONFLICT(backend_id,org_id,workspace_id,source,session_id) DO UPDATE SET
          canonical_session_id=excluded.canonical_session_id,source_epoch=excluded.source_epoch,
          coverage_from_ms=excluded.coverage_from_ms,revision=excluded.revision,row_count=excluded.row_count,
          complete=excluded.complete,updated_at=excluded.updated_at`,
        [backendId, scope.orgId, scope.workspaceId, scope.harness, scope.sessionId, value.canonicalSessionId, fromMs, value.revision, value.rows, value.complete, value.at.toISOString()]);
    },
    async partials(tenant, input) {
      if (!reader) throw new Error("search state tenant reader missing");
      if (!tenant.orgId || !tenant.workspaceId || !Number.isInteger(input.limit) || input.limit < 1 || input.limit > 1000) throw new Error("search state query invalid");
      if (input.after !== undefined && (typeof input.after !== "string" || input.after.length === 0)) throw new Error("search state query invalid");
      const { rows } = await reader(tenant.orgId, tenant.workspaceId, tx => tx.query<{ session_id: string }>(
        `SELECT DISTINCT coalesce(p.canonical_session_id,p.session_id) AS session_id
         FROM sessions.session_index p
         LEFT JOIN ${table} s ON s.backend_id=$3 AND s.org_id=p.org_id AND s.workspace_id=p.workspace_id AND s.source=p.harness AND s.session_id=p.session_id
         LEFT JOIN sessions.session_directory d ON d.org_id=p.org_id AND d.workspace_id=p.workspace_id AND d.harness=p.harness AND d.session_id=coalesce(p.canonical_session_id,p.session_id)
         WHERE p.org_id=$1 AND p.workspace_id=$2 AND p.deleted_at IS NULL
           AND ($4::text IS NULL OR p.session_id=$4 OR coalesce(p.canonical_session_id,p.session_id)=$4)
           AND ($7::text IS NULL OR coalesce(p.canonical_session_id,p.session_id) > $7)
           AND (s.complete IS DISTINCT FROM true OR d.event_projection_ready IS DISTINCT FROM true
             OR s.canonical_session_id IS DISTINCT FROM coalesce(p.canonical_session_id,p.session_id)
             OR s.source_epoch IS DISTINCT FROM d.event_projection_epoch OR s.coverage_from_ms IS DISTINCT FROM $5::bigint)
         ORDER BY session_id LIMIT $6`,
        [tenant.orgId, tenant.workspaceId, backendId, input.sessionId ?? null, fromMs, input.limit + 1, input.after ?? null]));
      return { sessions: rows.slice(0, input.limit).map(r => r.session_id), overflow: rows.length > input.limit };
    },
  };
}
