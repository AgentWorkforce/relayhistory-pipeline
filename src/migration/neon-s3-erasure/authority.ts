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
 * connection is expected to be bulls96_privacy_eraser (assertEraserIdentity), which can call nothing else.
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
  };
}

export class EraserIdentityRefused extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "EraserIdentityRefused";
  }
}

/**
 * The Supabase eraser must run as bulls96_privacy_eraser and nothing stronger: never a superuser, a BYPASSRLS role,
 * the owner of the privacy tables, or a member of that owner (a compromised eraser would otherwise hold the
 * destructive S3 key AND an owner credential). Checked at startup, before any sweep, for BOTH the session identity
 * (the login) and the effective role: an elevated login that ran SET ROLE bulls96_privacy_eraser keeps its
 * credential (RESET ROLE restores it), so the login itself must be the eraser role and no role substitution is allowed.
 */
export async function assertEraserIdentity(db: Queryable): Promise<void> {
  const r = (await db.query<{
    session_who: string; who: string;
    session_super: boolean; session_bypass: boolean; session_owner_member: boolean;
    super: boolean; bypass: boolean; owner_member: boolean;
  }>(
    `SELECT session_user::text AS session_who, current_user::text AS who,
            s.rolsuper AS session_super, s.rolbypassrls AS session_bypass, pg_has_role(session_user, c.relowner, 'MEMBER') AS session_owner_member,
            e.rolsuper AS super, e.rolbypassrls AS bypass, pg_has_role(current_user, c.relowner, 'MEMBER') AS owner_member
       FROM pg_roles s, pg_roles e, pg_class c
      WHERE s.rolname = session_user AND e.rolname = current_user
        AND c.oid = 'sessions.ingest_frozen_source_deletions'::regclass`,
  )).rows[0];
  if (!r) throw new EraserIdentityRefused("eraser_identity_unknown");
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
