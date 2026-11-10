/** Owner-only, bounded native backfill audit. Never repairs or invents catalog data. */
import { createHash } from "node:crypto";
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";

type Transact = <T>(work: (tx: SqlExecutor) => Promise<T>) => Promise<T>;
const TABLES = [
  "session_directory", "session_index", "session_directory_agents",
  "session_directory_users", "session_directory_machines",
  "session_directory_relationships", "session_directory_catalog_rollout",
  "__96bulls_role_validation",
] as const;
const MAX_ROWS = 10_000;
export type NativeAgentCatalogAudit = {
  version: 1;
  scope: "all_native_tenants";
  rows: number;
  fingerprint: string;
  counts: Record<string, number>;
  eligible: boolean;
  activated: boolean;
};

/**
 * `transact` owns BEGIN/COMMIT/ROLLBACK and one connection. Plan is READ ONLY.
 * Apply waits at most 1s for SHARE locks, reruns the audit, and accepts only the
 * reviewed plan's fingerprint. Only the pre-existing catalog rollout row is updated.
 * No scoped/excluded rows: that marker enables agent filters globally, not per tenant.
 *
 * This concerns the legacy list agent index, NOT the separately gated Desktop
 * relationship/model catalog. Its catalog_read_version/forget_tracking flags remain
 * unchanged. Empty agent arrays are valid for genuinely unnamed sessions; proving a
 * particular registered agent exists still requires the real caller's API/UI proof.
 */
export async function nativeAgentCatalogActivation(
  transact: Transact,
  options: { mode: "selfhosted"; generation: string; applyFingerprint?: string },
): Promise<NativeAgentCatalogAudit> {
  if (options.mode !== "selfhosted") throw new Error("native_catalog_selfhosted_only");
  if (!/^[0-9a-f]{64}$/.test(options.generation)) throw new Error("native_catalog_generation_invalid");
  if (options.applyFingerprint !== undefined && !/^[0-9a-f]{64}$/.test(options.applyFingerprint))
    throw new Error("native_catalog_fingerprint_invalid");
  const apply = options.applyFingerprint !== undefined;
  return transact(async (tx) => {
    await tx.query(apply ? "SET TRANSACTION ISOLATION LEVEL READ COMMITTED" : "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
    await tx.query("SET LOCAL statement_timeout='10s'");
    await tx.query("SET LOCAL lock_timeout='1s'");
    await tx.query("SET LOCAL TimeZone='UTC'");
    // Lock before taking the snapshot. Pointer/projector/tombstone/catalog writers
    // cannot straddle the verification and marker update. Canonical uploads continue.
    if (apply) {
      // Serialize activations before shared data locks, avoiding a lock-upgrade
      // deadlock between two operators. The validation lock blocks pending-first
      // migration admission until this transaction commits or rolls back.
      await tx.query("LOCK TABLE sessions.__96bulls_role_validation IN SHARE MODE");
      await tx.query("LOCK TABLE sessions.session_directory_catalog_rollout IN SHARE ROW EXCLUSIVE MODE");
      await tx.query(`LOCK TABLE ${TABLES.filter((t) => t !== "session_directory_catalog_rollout" && t !== "__96bulls_role_validation").map((t) => `sessions.${t}`).join(",")} IN SHARE MODE`);
    }
    const access = await tx.query<{ visible: boolean }>(
      `SELECT count(*)=$1::integer AND bool_and(
         r.rolsuper OR (c.relowner=r.oid AND NOT c.relforcerowsecurity)) AS visible
       FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
       CROSS JOIN pg_catalog.pg_roles r
       WHERE n.nspname='sessions' AND c.relname=ANY($2::text[]) AND r.rolname=current_user`,
      [TABLES.length, [...TABLES]],
    );
    if (access.rows[0]?.visible !== true) throw new Error("native_catalog_owner_visibility_required");
    const validation = await tx.query<{ valid: boolean }>(
      `SELECT EXISTS(SELECT 1 FROM sessions.__96bulls_role_validation
        WHERE name='session_directory_search_worker' AND status='pass' AND generation=$1) AS valid`,
      [options.generation],
    );
    if (validation.rows[0]?.valid !== true) throw new Error("native_catalog_schema_validation_required");
    const directory = await tx.query<Record<string, unknown>>(
      `SELECT org_id,workspace_id,harness,session_id,deleted_at::text,event_projection_epoch::text,
        last_activity_at::text,event_projection_ready,event_projected_at::text,catalog_projected_at::text,agent_names
       FROM sessions.session_directory
       ORDER BY org_id COLLATE "C",workspace_id COLLATE "C",harness COLLATE "C",session_id COLLATE "C"
       LIMIT $1`, [MAX_ROWS + 1],
    );
    if (directory.rows.length > MAX_ROWS) throw new Error("native_catalog_audit_row_limit");
    const count = await tx.query<{ counts: Record<string, number> }>(`SELECT json_build_object(
      'liveRows',(SELECT count(*) FROM sessions.session_directory WHERE deleted_at IS NULL),
      'unprojected',(SELECT count(*) FROM sessions.session_directory WHERE deleted_at IS NULL
         AND (catalog_projected_at IS NULL OR NOT event_projection_ready OR event_projected_at IS NULL
           OR catalog_projected_at < event_projected_at)),
      'livePointersWithoutDirectory',(SELECT count(*) FROM sessions.session_index p
        WHERE p.deleted_at IS NULL AND NOT EXISTS(SELECT 1 FROM sessions.session_directory d
          WHERE d.org_id=p.org_id AND d.workspace_id=p.workspace_id AND d.harness=p.harness
            AND d.session_id=coalesce(p.canonical_session_id,p.session_id) AND d.deleted_at IS NULL)),
      'directoryWithoutLivePointers',(SELECT count(*) FROM sessions.session_directory d
        WHERE d.deleted_at IS NULL AND NOT EXISTS(SELECT 1 FROM sessions.session_index p
          WHERE p.org_id=d.org_id AND p.workspace_id=d.workspace_id AND p.harness=d.harness
            AND coalesce(p.canonical_session_id,p.session_id)=d.session_id AND p.deleted_at IS NULL)),
      'missingAgents',(SELECT count(*) FROM sessions.session_directory d
        CROSS JOIN LATERAL unnest(d.agent_names) x(name) WHERE d.deleted_at IS NULL AND NOT EXISTS(
          SELECT 1 FROM sessions.session_directory_agents a WHERE a.org_id=d.org_id
            AND a.workspace_id=d.workspace_id AND a.harness=d.harness AND a.session_id=d.session_id
            AND a.agent_name_normalized=lower(btrim(x.name)) AND a.last_activity_at=d.last_activity_at)),
      'extraAgents',(SELECT count(*) FROM sessions.session_directory_agents a
        LEFT JOIN sessions.session_directory d USING(org_id,workspace_id,harness,session_id)
        WHERE d.session_id IS NULL OR d.deleted_at IS NOT NULL OR NOT EXISTS(
          SELECT 1 FROM unnest(d.agent_names) x(name) WHERE lower(btrim(x.name))=a.agent_name_normalized)),
      'unknownUsers',(SELECT count(*) FROM sessions.session_directory_users WHERE lower(btrim(user_id))='unknown'),
      'orphanMachines',(SELECT count(*) FROM sessions.session_directory_machines m
        WHERE NOT EXISTS(SELECT 1 FROM sessions.session_index p WHERE p.org_id=m.org_id
          AND p.workspace_id=m.workspace_id AND p.harness=m.harness AND p.machine_id=m.machine_id
          AND coalesce(p.canonical_session_id,p.session_id)=m.session_id AND p.deleted_at IS NULL)),
      'orphanRelationships',(SELECT count(*) FROM sessions.session_directory_relationships r
        WHERE NOT EXISTS(SELECT 1 FROM sessions.session_directory d WHERE d.org_id=r.org_id
          AND d.workspace_id=r.workspace_id AND d.harness=r.harness
          AND d.session_id=r.parent_session_id AND d.deleted_at IS NULL))
    ) AS counts`);
    const counts = count.rows[0]?.counts;
    if (!counts || Object.values(counts).some((n) => !Number.isSafeInteger(n) || n < 0))
      throw new Error("native_catalog_audit_result_invalid");
    const fingerprint = createHash("sha256").update(JSON.stringify({ version: 1, generation: options.generation, rows: directory.rows, counts })).digest("hex");
    const eligible = counts.liveRows > 0 && Object.entries(counts).every(([k, n]) => k === "liveRows" || n === 0);
    if (apply) {
      if (fingerprint !== options.applyFingerprint) throw new Error("native_catalog_plan_changed");
      if (!eligible) throw new Error("native_catalog_projection_incomplete");
      const marked = await tx.query<{ singleton: boolean }>(
        `UPDATE sessions.session_directory_catalog_rollout
         SET completed_at=coalesce(completed_at,now()),updated_at=now() WHERE singleton RETURNING singleton`,
      );
      if (marked.rows.length !== 1) throw new Error("native_catalog_rollout_row_missing");
    }
    return { version: 1, scope: "all_native_tenants", rows: directory.rows.length, fingerprint, counts, eligible, activated: apply };
  });
}
