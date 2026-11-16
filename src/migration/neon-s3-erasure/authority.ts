/**
 * Where the transcript eraser learns what users deleted (zero-Neon checklist E3).
 *
 *  - `neon` (default, today): Neon `sessions.deleted_upload_sessions`, plus the rollup/catalog presence sweep
 *    (a reset or deletion removes the group's rollups on Neon).
 *  - `supabase` (C3, after the owner activated the Supabase privacy authority): Supabase
 *    `sessions.ingest_frozen_source_deletions`, which the authority begin keeps exactly as Neon would keep
 *    `deleted_upload_sessions` (a disable inserts, a re-enable deletes, a reset inserts every owned session).
 *    Neon is not read. There is no presence sweep: under this authority Neon's delivery state no longer
 *    changes, so a vanished group can only be a deletion, and every deletion is a row here.
 *
 * The Supabase source refuses (and the sweep erases nothing) unless `sessions.ingest_privacy_authority` is
 * `active`: before that the frozen table is NOT the authority (a Neon re-enable never removes its row), and
 * erasing from it could destroy a session the user re-enabled.
 */
import type { Queryable } from "../relayhistory-export.ts";
import { legacyMachineId } from "../../shared/legacy-export-machine.ts";
import { selectedPublicTurnPredicateColumnSql } from "../../shared/legacy-turn-source.ts";
import {
  activityPage,
  catalogPage,
  fencedGroups,
  fencedLivePage,
  groupExistsInNeon,
  LIVE_FENCE_PAGE,
  lockWaiters,
  type LiveFencedGroup,
} from "../neon-s3-sync/neon-reader.ts";

export type ErasureAuthorityKind = "neon" | "supabase";

export interface ErasurePresence {
  activityPage(after: Parameters<typeof activityPage>[1], limit?: number): ReturnType<typeof activityPage>;
  catalogPage(after: Parameters<typeof catalogPage>[1], limit?: number): ReturnType<typeof catalogPage>;
  groupExists(orgId: string, sessionId: string, includeLegacy: boolean): Promise<boolean>;
}

/**
 * One legacy-exported scope whose privacy fence is still authoritative.  The
 * user is the owner bound by the legacy exporter, not necessarily the member
 * whose deletion created the fence; it derives the one `legacy:tables:v1`
 * machine which may be erased without reaching another user's copy.
 */
export interface LegacyFencedGroup {
  org_id: string;
  workspace_id: string;
  source: string;
  session_id: string;
  legacy_user_id: string;
  legacy_machine_id: string;
}

export interface ErasureAuthority {
  readonly kind: ErasureAuthorityKind;
  /** Refuses (throws) when this source is not the privacy authority right now. */
  ready(): Promise<void>;
  lockWaiters(): Promise<{ total: number; nonAdvisory: number }>;
  /** Neon only: the rollup/catalog presence sweep ("gone from Neon"). */
  readonly presence?: ErasurePresence;
  fencedGroups(): Promise<{ org_id: string; session_id: string }[]>;
  fencedLivePage(after: LiveFencedGroup | null, limit?: number): Promise<LiveFencedGroup[]>;
  /** Re-read right before an erasure: false once the user re-enabled the session. */
  stillFenced(g: LiveFencedGroup): Promise<boolean>;
  /**
   * Legacy-export scope fences, already joined to their exporter-bound owner.
   * Optional only for small in-process test authorities; both production
   * authority factories install it before legacy erasure is activated.
   */
  legacyFencedPage?(after: LegacyFencedGroup | null, limit?: number): Promise<LegacyFencedGroup[]>;
  /** Re-read the exact scope-level fence immediately before every delete page. */
  stillLegacyFenced?(g: LegacyFencedGroup): Promise<boolean>;
}

const LEGACY_FENCE_PAGE = 5_000;

/**
 * Neon-era legacy scopes.  The owner expression is deliberately identical to
 * the legacy exporter's binding order: one delivery-event user, otherwise one
 * outcome user, otherwise one owner across the selected public turns. A
 * deleting member's user id is never substituted for it.
 */
async function neonLegacyFencedPage(
  db: Queryable,
  after: LegacyFencedGroup | null,
  limit = LEGACY_FENCE_PAGE,
): Promise<LegacyFencedGroup[]> {
  const { rows } = await db.query<Omit<LegacyFencedGroup, "legacy_machine_id">>(
    `WITH fences AS (
       SELECT org_id,CASE WHEN workspace_id='' THEN 'default' ELSE workspace_id END AS workspace_id,source,session_id
         FROM sessions.deleted_upload_sessions
       UNION
       SELECT org_id,CASE WHEN workspace_id='' THEN 'default' ELSE workspace_id END AS workspace_id,source,session_id
         FROM sessions.conversation_turn_fences),
     bound AS (
       SELECT f.*,
         CASE
           WHEN EXISTS(SELECT 1 FROM sessions.convergence_events e
             WHERE e.org_id=f.org_id AND (e.workspace_id=f.workspace_id OR (f.workspace_id='default' AND e.workspace_id=''))
               AND e.source=f.source AND e.session_id=f.session_id AND e.user_id<>'')
           THEN (SELECT min(e.user_id) FROM sessions.convergence_events e
             WHERE e.org_id=f.org_id AND (e.workspace_id=f.workspace_id OR (f.workspace_id='default' AND e.workspace_id=''))
               AND e.source=f.source AND e.session_id=f.session_id AND e.user_id<>''
             HAVING count(DISTINCT e.user_id)=1)
           WHEN EXISTS(SELECT 1 FROM sessions.session_outcomes o
             WHERE o.org_id=f.org_id AND (o.workspace_id=f.workspace_id OR (f.workspace_id='default' AND o.workspace_id=''))
               AND o.source=f.source AND o.session_id=f.session_id AND o.user_id<>'')
           THEN (SELECT min(o.user_id) FROM sessions.session_outcomes o
             WHERE o.org_id=f.org_id AND (o.workspace_id=f.workspace_id OR (f.workspace_id='default' AND o.workspace_id=''))
               AND o.source=f.source AND o.session_id=f.session_id AND o.user_id<>''
             HAVING count(DISTINCT o.user_id)=1)
           WHEN EXISTS(SELECT 1 FROM sessions.conversation_turns t
             WHERE t.org_id=f.org_id AND (t.workspace_id=f.workspace_id OR (f.workspace_id='default' AND t.workspace_id='')) AND t.session_id=f.session_id
               AND ${selectedPublicTurnPredicateColumnSql("t", "f")} AND t.session_owner<>'')
           THEN (SELECT min(t.session_owner) FROM sessions.conversation_turns t
             WHERE t.org_id=f.org_id AND (t.workspace_id=f.workspace_id OR (f.workspace_id='default' AND t.workspace_id='')) AND t.session_id=f.session_id
               AND ${selectedPublicTurnPredicateColumnSql("t", "f")} AND t.session_owner<>''
             HAVING count(DISTINCT t.session_owner)=1)
           ELSE NULL
         END AS legacy_user_id
       FROM fences f)
     SELECT org_id,workspace_id,source,session_id,legacy_user_id
       FROM bound
      WHERE legacy_user_id IS NOT NULL
        AND ($1::text IS NULL OR (org_id,workspace_id,source,session_id,legacy_user_id) > ($1,$2,$3,$4,$5))
      ORDER BY org_id,workspace_id,source,session_id,legacy_user_id
      LIMIT $6`,
    after
      ? [after.org_id, after.workspace_id, after.source, after.session_id, after.legacy_user_id, Math.min(limit, LEGACY_FENCE_PAGE)]
      : [null, null, null, null, null, Math.min(limit, LEGACY_FENCE_PAGE)],
  );
  // `default` and the historical empty workspace are one privacy scope, but
  // old exports hashed the stored spelling. Queue both narrowly-derived
  // candidates under a default fence; a missing candidate is an idempotent
  // no-op and no arbitrary machine id is ever followed.
  return rows.flatMap((r) => {
    const workspaces = r.workspace_id === "default" ? ["", "default"] : [r.workspace_id];
    return workspaces.map((workspace) => ({
      ...r,
      legacy_machine_id: legacyMachineId(workspace, r.legacy_user_id),
    }));
  });
}

async function neonStillLegacyFenced(db: Queryable, g: LegacyFencedGroup): Promise<boolean> {
  const { rows } = await db.query(
    `SELECT 1 WHERE
       EXISTS(SELECT 1 FROM sessions.deleted_upload_sessions
         WHERE org_id=$1 AND source=$3 AND session_id=$4
           AND (workspace_id=$2 OR ($2='default' AND workspace_id='')))
       OR EXISTS(SELECT 1 FROM sessions.conversation_turn_fences
         WHERE org_id=$1 AND source=$3 AND session_id=$4
           AND (workspace_id=$2 OR ($2='default' AND workspace_id='')))
       LIMIT 1`,
    [g.org_id, g.workspace_id, g.source, g.session_id],
  );
  return rows.length > 0;
}

export function neonAuthority(db: Queryable): ErasureAuthority {
  return {
    kind: "neon",
    ready: async () => {},
    lockWaiters: () => lockWaiters(db),
    presence: {
      activityPage: (after, limit) => activityPage(db, after, limit),
      catalogPage: (after, limit) => catalogPage(db, after, limit),
      groupExists: (org, session, legacy) => groupExistsInNeon(db, org, session, legacy),
    },
    fencedGroups: () => fencedGroups(db),
    fencedLivePage: (after, limit) => fencedLivePage(db, after, limit),
    stillFenced: async (g) =>
      (
        await db.query(
          `SELECT 1 FROM sessions.deleted_upload_sessions
            WHERE org_id = $1 AND workspace_id = $2 AND user_id = $3 AND origin_id = $4 AND source = $5 AND session_id = $6`,
          [g.org_id, g.workspace_id, g.user_id, g.origin_id, g.source, g.session_id],
        )
      ).rows.length > 0,
    legacyFencedPage: (after, limit) => neonLegacyFencedPage(db, after, limit),
    stillLegacyFenced: (g) => neonStillLegacyFenced(db, g),
  };
}

export class AuthorityNotActive extends Error {
  readonly code = "supabase_privacy_authority_not_active";
  constructor() {
    super("supabase privacy authority not active");
    this.name = "AuthorityNotActive";
  }
}

/**
 * The Supabase authority's fenced set, through the least-privilege eraser RPCs (20261105041000): the deduplicated
 * UNION of the frozen-source deletions (Neon-era sessions) and the target's upload_deletions (every session the
 * ordered fence deleted, post-cutover uploads included). Each RPC refuses unless the authority is active. The
 * connection is expected to be bulls96_privacy_eraser (assertEraserIdentity), which can call nothing else beyond
 * the eraser RPC surface (ordinary and legacy-scope pages plus their rechecks).
 */
export function supabaseAuthority(db: Queryable): ErasureAuthority {
  const rpc = async <T>(sql: string, params: unknown[] = []): Promise<T> => {
    const r = (await db.query<{ r: T }>(sql, params)).rows[0];
    if (!r) throw Object.assign(new Error("eraser rpc returned no row"), { code: "eraser_rpc_empty" });
    return r.r;
  };
  const ready = async () => {
    const s = await rpc<{ active?: unknown }>("SELECT sessions.ingest_privacy_eraser_state() AS r");
    if (s?.active !== true) throw new AuthorityNotActive();
  };
  return {
    kind: "supabase",
    ready,
    lockWaiters: () => lockWaiters(db),
    fencedGroups: async () => rpc<{ org_id: string; session_id: string }[]>("SELECT sessions.ingest_privacy_eraser_groups() AS r"),
    fencedLivePage: async (after, limit = LIVE_FENCE_PAGE) =>
      rpc<LiveFencedGroup[]>("SELECT sessions.ingest_privacy_eraser_page($1::jsonb) AS r", [JSON.stringify({
        after: after ? [after.org_id, after.workspace_id, after.user_id, after.origin_id, after.source, after.session_id] : null,
        limit: Math.min(limit, 5000),
      })]),
    // The RPC re-checks the authority too: a release back to Neon makes these sets stale at once.
    stillFenced: async (g) =>
      (await rpc<boolean>("SELECT sessions.ingest_privacy_eraser_still_fenced($1::jsonb) AS r", [JSON.stringify({
        org_id: g.org_id, workspace_id: g.workspace_id, user_id: g.user_id, origin_id: g.origin_id, source: g.source, session_id: g.session_id,
      })])) === true,
    legacyFencedPage: async (after, limit = LIVE_FENCE_PAGE) =>
      rpc<LegacyFencedGroup[]>("SELECT sessions.legacy_export_privacy_eraser_page($1::jsonb) AS r", [JSON.stringify({
        after: after ? [after.org_id, after.workspace_id, after.source, after.session_id, after.legacy_user_id, after.legacy_machine_id] : null,
        limit: Math.min(limit, 5000),
      })]),
    stillLegacyFenced: async (g) =>
      (await rpc<boolean>("SELECT sessions.legacy_export_privacy_eraser_still_fenced($1::jsonb) AS r", [JSON.stringify({
        org_id: g.org_id, workspace_id: g.workspace_id, source: g.source, session_id: g.session_id,
        legacy_user_id: g.legacy_user_id, legacy_machine_id: g.legacy_machine_id,
      })])) === true,
  };
}

export class EraserIdentityRefused extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "EraserIdentityRefused";
  }
}

/** The authentication method the deployed eraser login uses (pinned): system_user must be `<method>:bulls96_privacy_eraser`. */
export const ERASER_AUTH_METHOD = "scram-sha-256";

/**
 * The Supabase eraser must run as bulls96_privacy_eraser and nothing stronger: never a superuser, a BYPASSRLS role,
 * the owner of the privacy tables, or a member of that owner (a compromised eraser would otherwise hold the
 * destructive S3 key AND an owner credential). Checked at startup, before any sweep:
 *  - system_user (PostgreSQL 16+), the AUTHENTICATED identity, which neither SET ROLE nor SET SESSION AUTHORIZATION
 *    (nor a startup `options=-c session_authorization=...`) can rewrite, must be exactly
 *    `${ERASER_AUTH_METHOD}:bulls96_privacy_eraser`; servers without system_user are refused (unverifiable);
 *  - session_user and current_user must both be bulls96_privacy_eraser (no SET ROLE substitution);
 *  - superuser / BYPASSRLS / owner membership are refused for the session identity and the effective role.
 */
export async function assertEraserIdentity(db: Queryable, opts: { authMethod?: string } = {}): Promise<void> {
  const method = opts.authMethod ?? ERASER_AUTH_METHOD;
  const v = (await db.query<{ n: number }>("SELECT current_setting('server_version_num')::int AS n")).rows[0]?.n;
  if (typeof v !== "number" || v < 160000) throw new EraserIdentityRefused("eraser_identity_unverifiable_server");
  const r = (await db.query<{
    system_who: string | null; session_who: string; who: string;
    session_super: boolean; session_bypass: boolean; session_owner_member: boolean;
    super: boolean; bypass: boolean; owner_member: boolean;
  }>(
    `SELECT system_user::text AS system_who, session_user::text AS session_who, current_user::text AS who,
            s.rolsuper AS session_super, s.rolbypassrls AS session_bypass, pg_has_role(session_user, c.relowner, 'MEMBER') AS session_owner_member,
            e.rolsuper AS super, e.rolbypassrls AS bypass, pg_has_role(current_user, c.relowner, 'MEMBER') AS owner_member
       FROM pg_roles s, pg_roles e, pg_class c
      WHERE s.rolname = session_user AND e.rolname = current_user
        AND c.oid = 'sessions.ingest_frozen_source_deletions'::regclass`,
  )).rows[0];
  if (!r) throw new EraserIdentityRefused("eraser_identity_unknown");
  if (r.system_who !== `${method}:bulls96_privacy_eraser`) throw new EraserIdentityRefused("eraser_identity_not_authenticated_as_privacy_eraser");
  if (r.session_super || r.super) throw new EraserIdentityRefused("eraser_identity_superuser");
  if (r.session_bypass || r.bypass) throw new EraserIdentityRefused("eraser_identity_bypassrls");
  if (r.session_owner_member || r.owner_member) throw new EraserIdentityRefused("eraser_identity_owner");
  if (r.session_who !== r.who) throw new EraserIdentityRefused("eraser_identity_role_substituted");
  if (r.session_who !== "bulls96_privacy_eraser") throw new EraserIdentityRefused("eraser_identity_not_privacy_eraser");
}

export function isErasureAuthority(v: unknown): v is ErasureAuthority {
  return !!v && typeof v === "object" && ((v as ErasureAuthority).kind === "neon" || (v as ErasureAuthority).kind === "supabase")
    && typeof (v as ErasureAuthority).fencedGroups === "function";
}
