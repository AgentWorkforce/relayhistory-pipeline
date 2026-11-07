/** Content-free portable index completeness. Explicit owner bootstrap only;
 * this helper issues no GRANT and is never initialized by a service implicitly.
 * Readers compare the recorded metadata epoch with current pointer-derived
 * directory readiness, so a failed first pass or newer pointer cannot look
 * complete merely because an older pass once succeeded. */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { TenantRunner } from "../session-read-api/liveness.ts";
import type { SearchProjectionState } from "./search-adapter.ts";

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
      const { rows } = await owner.query<{ name: string; command: string; roles: string; permissive: boolean; qual: string; check: string }>(
        `SELECT polname AS name,polcmd::text AS command,polroles::text AS roles,polpermissive AS permissive,
          pg_get_expr(polqual,polrelid) AS qual,pg_get_expr(polwithcheck,polrelid) AS check
         FROM pg_policy WHERE polrelid=$1::regclass`, [table]);
      const normalize = (value: string) => value.replace(/\s+|::text|\(|\)/g, "");
      const expected = normalize("org_id=current_setting('bulls96_read.org_id',true) AND workspace_id=current_setting('bulls96_read.workspace_id',true)");
      if (rows.length !== 1 || rows[0]!.name !== "tenant_only" || rows[0]!.command !== "*" || rows[0]!.roles !== "{0}" || !rows[0]!.permissive || normalize(rows[0]!.qual ?? "") !== expected || normalize(rows[0]!.check ?? "") !== expected) throw new Error("search state tenant policy mismatch");
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
      const { rows } = await reader(tenant.orgId, tenant.workspaceId, tx => tx.query<{ session_id: string }>(
        `SELECT DISTINCT coalesce(p.canonical_session_id,p.session_id) AS session_id
         FROM sessions.session_index p
         LEFT JOIN ${table} s ON s.backend_id=$3 AND s.org_id=p.org_id AND s.workspace_id=p.workspace_id AND s.source=p.harness AND s.session_id=p.session_id
         LEFT JOIN sessions.session_directory d ON d.org_id=p.org_id AND d.workspace_id=p.workspace_id AND d.harness=p.harness AND d.session_id=coalesce(p.canonical_session_id,p.session_id)
         WHERE p.org_id=$1 AND p.workspace_id=$2 AND p.deleted_at IS NULL
           AND ($4::text IS NULL OR p.session_id=$4 OR coalesce(p.canonical_session_id,p.session_id)=$4)
           AND (s.complete IS DISTINCT FROM true OR d.event_projection_ready IS DISTINCT FROM true
             OR s.canonical_session_id IS DISTINCT FROM coalesce(p.canonical_session_id,p.session_id)
             OR s.source_epoch IS DISTINCT FROM d.event_projection_epoch OR s.coverage_from_ms IS DISTINCT FROM $5::bigint)
         ORDER BY session_id LIMIT $6`,
        [tenant.orgId, tenant.workspaceId, backendId, input.sessionId ?? null, fromMs, input.limit + 1]));
      return { sessions: rows.slice(0, input.limit).map(r => r.session_id), overflow: rows.length > input.limit };
    },
  };
}
