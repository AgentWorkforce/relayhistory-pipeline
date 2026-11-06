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

const COLS = "org_id, workspace_id, user_id, origin_id, source, session_id";

export function supabaseAuthority(db: Queryable): ErasureAuthority {
  const ready = async () => {
    const { rows } = await db.query<{ state: string | null }>(
      "SELECT (SELECT state FROM sessions.ingest_privacy_authority WHERE singleton) AS state",
    );
    if (rows[0]?.state !== "active") throw new AuthorityNotActive();
  };
  return {
    kind: "supabase",
    ready,
    lockWaiters: () => lockWaiters(db),
    fencedGroups: async () => {
      await ready();
      const { rows } = await db.query<{ org_id: string; session_id: string }>(
        "SELECT DISTINCT org_id, session_id FROM sessions.ingest_frozen_source_deletions LIMIT 100001",
      );
      if (rows.length > 100_000) throw Object.assign(new Error("too many fenced groups"), { code: "fence_overflow" });
      return rows;
    },
    fencedLivePage: async (after, limit = LIVE_FENCE_PAGE) => {
      await ready();
      const { rows } = after
        ? await db.query<LiveFencedGroup>(
            `SELECT ${COLS} FROM sessions.ingest_frozen_source_deletions
              WHERE (${COLS}) > ($1, $2, $3, $4, $5, $6) ORDER BY ${COLS} LIMIT $7`,
            [after.org_id, after.workspace_id, after.user_id, after.origin_id, after.source, after.session_id, limit],
          )
        : await db.query<LiveFencedGroup>(`SELECT ${COLS} FROM sessions.ingest_frozen_source_deletions ORDER BY ${COLS} LIMIT $1`, [limit]);
      return rows;
    },
    stillFenced: async (g) => {
      // Re-check the authority too: a release back to Neon makes this table stale at once.
      await ready();
      return (
        await db.query(
          `SELECT 1 FROM sessions.ingest_frozen_source_deletions
            WHERE org_id = $1 AND workspace_id = $2 AND user_id = $3 AND origin_id = $4 AND source = $5 AND session_id = $6`,
          [g.org_id, g.workspace_id, g.user_id, g.origin_id, g.source, g.session_id],
        )
      ).rows.length > 0;
    },
  };
}

export function isErasureAuthority(v: unknown): v is ErasureAuthority {
  return !!v && typeof v === "object" && ((v as ErasureAuthority).kind === "neon" || (v as ErasureAuthority).kind === "supabase")
    && typeof (v as ErasureAuthority).fencedGroups === "function";
}
