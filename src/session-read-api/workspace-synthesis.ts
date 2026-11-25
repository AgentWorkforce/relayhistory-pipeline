/**
 * GET /v1/session-intel/synthesis (docs/session-intel-workspace-synthesis.md, "Read route"): the stored
 * workspace-synthesis/1 artifact of one workspace and window, behind READ_API_WORKSPACE_SYNTHESIS_ENABLED (exactly
 * "true"). Off: the standard 501 envelope, reason `workspace_synthesis_disabled`, and nothing is read.
 *
 * Authority: the newest complete artifact for the exact workspace and window (session-analysis/workspace-synthesis.ts
 * latestWorkspaceSynthesis). Erasure: after the artifact is read, every session its envelope records (`considered`
 * and `related`: children and any other session its evidence belongs to) is checked against the pointer tombstones
 * (Liveness.erased) and, where one is bound (self-host SESSION_ERASE_FENCE_ENFORCED=1), the operator erase fence the
 * exact session routes use (sessions.ingest_operator_runtime_erased, ReadApiDeps.operatorErased); if any is erased or
 * fenced the artifact is not served (404, reason `synthesis_invalidated`) until the job recomputes it, so an erased
 * session never stays in a drilldown or a count. Only `synthesis` is served.
 *
 * Aliases: a session is addressed by its physical id (session_index.session_id) or its source RelayHistory id
 * (canonical_session_id), and a fence or tombstone may be written under either. Where an alias reader is bound, every
 * recorded session is expanded to the closure of the forms its pointer rows (live and tombstoned) hold under the same
 * source, both directions and through shared source ids (siblings), before both checks. The fence bound without an
 * alias reader is refused (503): it would see one form only. A fence lookup that fails, a row out of contract, more
 * forms than SYNTHESIS_FENCE_MAX_KEYS or a closure not complete within SYNTHESIS_ALIAS_MAX_ROUNDS is a 503: nothing is
 * served. All lookups run in batches of SYNTHESIS_ERASE_CHECK_BATCH keys.
 */
import {
  latestWorkspaceSynthesis, SynthesisArtifactError, type SynthesisArtifact, type SynthesisArtifactStore,
} from "../session-analysis/workspace-synthesis.ts";
import { SYNTHESIS_BOUNDS, type WorkspaceSynthesis } from "../session-intel/synthesis/synthesize.ts";
import { keyOf, TenantMismatchError, type Liveness, type SessionKey } from "./page.ts";
import type { TenantRunner } from "./liveness.ts";

export interface WorkspaceSynthesisSource {
  latest(orgId: string, workspaceId: string, window: { since: string; until: string }): Promise<SynthesisArtifact | null>;
}

/** The artifact store as the route's source; `keyRoot` is the job's key root (the tenant base, e.g. PIPELINE_KEY_PREFIX). */
export function storeWorkspaceSynthesis(store: SynthesisArtifactStore, keyRoot: string): WorkspaceSynthesisSource {
  return {
    latest: (orgId, workspace, window) => latestWorkspaceSynthesis(store, { keyRoot, orgId, workspace, window }),
  };
}

/**
 * The key root both readers use: the deployment's PIPELINE_KEY_PREFIX (the tenant base the job writes under), one
 * lowercase segment. Unset or not one segment: undefined, and the route answers 501 workspace_synthesis_not_configured.
 */
export function synthesisKeyRoot(raw: string | undefined): string | undefined {
  const root = (raw ?? "").replace(/^\/+|\/+$/gu, "");
  return /^[a-z0-9][a-z0-9-]{0,62}$/u.test(root) ? root : undefined;
}

/** Keys per Liveness.erased, alias and operator-fence call. */
export const SYNTHESIS_ERASE_CHECK_BATCH = 500;
/** At most this many (source, id) forms are checked for one artifact (every recorded session and its aliases); more is a 503. */
export const SYNTHESIS_FENCE_MAX_KEYS = 25_000;
/** At most this many pointer rows are read per alias batch; more is out of contract (a 503), never a silent cut. */
export const SYNTHESIS_ALIAS_ROWS_MAX = 4_000;
/** Alias closure rounds (each queries the forms the previous one found); still growing after this many is a 503. */
export const SYNTHESIS_ALIAS_MAX_ROUNDS = 8;

/**
 * The batched operator erase fence over a set of keys: the subset fenced, as `keyOf` strings. Any failure throws
 * (the route answers 503). Built from ReadApiDeps.operatorErasedBatch or, absent that, the per-key operatorErased.
 */
export type SynthesisOperatorFence = (orgId: string, workspaceId: string, keys: SessionKey[]) => Promise<Set<string>>;

/** One pointer row's id forms: its physical id and, where recorded, its source RelayHistory id. */
export interface SessionAliasRow {
  org_id: string;
  workspace_id: string;
  harness: string;
  session_id: string;
  canonical_session_id: string | null;
}

/** The pointer rows (live and tombstoned) whose physical or source id is one of `keys` under the key's source. */
export type SessionAliases = (orgId: string, workspaceId: string, keys: SessionKey[]) => Promise<SessionAliasRow[]>;

/**
 * Columns the read role holds (supabase/bootstrap/session_read_api_role.sql), under its tenant RLS policy, in the
 * tenant-bound READ ONLY transaction: no new function, grant or migration. Live and tombstoned rows both count (an
 * operator erase may have tombstoned some pointers already). The physical branch is si_session; the source-id branch
 * reads the workspace's pointers (bounded by the tenant), as session_analysis_first_seen_v1 does.
 */
export const SESSION_ALIASES_SQL = `SELECT p.org_id, p.workspace_id, p.harness, p.session_id, p.canonical_session_id
  FROM sessions.session_index AS p
  JOIN unnest($3::text[], $4::text[]) AS k(h, s)
    ON p.harness = k.h AND (p.session_id = k.s OR p.canonical_session_id = k.s)
 WHERE p.org_id = $1 AND p.workspace_id = $2
 GROUP BY p.org_id, p.workspace_id, p.harness, p.session_id, p.canonical_session_id
 LIMIT ${SYNTHESIS_ALIAS_ROWS_MAX + 1}`;

export function sqlSessionAliases(run: TenantRunner): SessionAliases {
  return async (orgId, workspaceId, keys) => {
    if (keys.length === 0) return [];
    const { rows } = await run(orgId, workspaceId, (q) => q.query<SessionAliasRow>(SESSION_ALIASES_SQL, [
      orgId, workspaceId, keys.map((k) => k.source), keys.map((k) => k.sessionId),
    ]));
    return rows;
  };
}

/** The per-key operatorErased as a batched fence: sequential, stopping at the first fenced key. */
export function perKeyOperatorFence(erased: (key: { orgId: string; workspaceId: string; source: string; sessionId: string }) => Promise<boolean>): SynthesisOperatorFence {
  return async (orgId, workspaceId, keys) => {
    for (const k of keys) if (await erased({ orgId, workspaceId, source: k.source, sessionId: k.sessionId })) return new Set([keyOf(k)]);
    return new Set();
  };
}

/** The checks a read applies to the artifact's recorded sessions. */
export interface SynthesisChecks {
  liveness: Liveness;
  orgId: string;
  workspaceId: string;
  /** The operator erase fence, where one is bound; then `aliases` is required. */
  operatorFence?: SynthesisOperatorFence;
  /** The alias reader; absent (and no fence), each session is checked under its recorded form only. */
  aliases?: SessionAliases;
}

/**
 * Every (source, id) form of `refs`: the same-tenant, same-source alias closure. Each round queries every form not yet
 * queried (a physical id finds its rows' source ids; a source id finds every physical id recorded under it, siblings
 * included), until no new form appears. So a recorded physical A whose source id C is shared by a physical B brings in
 * B (and any further source id B's rows hold). Bounded: SYNTHESIS_FENCE_MAX_KEYS forms, SYNTHESIS_ALIAS_ROWS_MAX rows a
 * batch and SYNTHESIS_ALIAS_MAX_ROUNDS rounds; a closure not complete within them is an error (the route's 503).
 */
async function aliasForms(input: SynthesisChecks, refs: readonly SessionKey[]): Promise<SessionKey[]> {
  const forms = new Map<string, SessionKey>();
  let frontier: SessionKey[] = [];
  const add = (k: SessionKey) => {
    if (forms.has(keyOf(k))) return;
    forms.set(keyOf(k), { source: k.source, sessionId: k.sessionId });
    if (forms.size > SYNTHESIS_FENCE_MAX_KEYS) throw new Error("synthesis_fence_bound");
    frontier.push({ source: k.source, sessionId: k.sessionId });
  };
  refs.forEach(add);
  if (!input.aliases) return [...forms.values()];
  for (let round = 0; frontier.length > 0; round++) {
    if (round >= SYNTHESIS_ALIAS_MAX_ROUNDS) throw new Error("synthesis_aliases_incomplete");
    const query = frontier;
    frontier = [];
    for (let i = 0; i < query.length; i += SYNTHESIS_ERASE_CHECK_BATCH) {
      const batch = query.slice(i, i + SYNTHESIS_ERASE_CHECK_BATCH);
      const asked = new Set(batch.map(keyOf));
      const rows = await input.aliases(input.orgId, input.workspaceId, batch);
      if (!Array.isArray(rows) || rows.length > SYNTHESIS_ALIAS_ROWS_MAX) throw new Error("synthesis_aliases_out_of_contract");
      for (const row of rows) {
        if (row.org_id !== input.orgId || row.workspace_id !== input.workspaceId) throw new TenantMismatchError();
        const canonical = row.canonical_session_id;
        if (typeof row.harness !== "string" || typeof row.session_id !== "string" || row.session_id === "" ||
            (canonical !== null && (typeof canonical !== "string" || canonical === "")) ||
            (!asked.has(keyOf({ source: row.harness, sessionId: row.session_id })) &&
              (canonical === null || !asked.has(keyOf({ source: row.harness, sessionId: canonical })))))
          throw new Error("synthesis_aliases_out_of_contract");
        add({ source: row.harness, sessionId: row.session_id });
        if (canonical !== null) add({ source: row.harness, sessionId: canonical });
      }
    }
  }
  return [...forms.values()];
}

/**
 * How many of the recorded sessions are erased (every pointer tombstoned) or operator-fenced, under every form; stops
 * at the first batch with any (the answer is then only "some"). Throws when a check cannot be made.
 */
export async function invalidatedCount(input: SynthesisChecks, refs: readonly SessionKey[]): Promise<number> {
  if (input.operatorFence && !input.aliases) throw new Error("synthesis_fence_aliases_unavailable");
  const keys = await aliasForms(input, refs);
  for (let i = 0; i < keys.length; i += SYNTHESIS_ERASE_CHECK_BATCH) {
    const erased = await input.liveness.erased(input.orgId, input.workspaceId, keys.slice(i, i + SYNTHESIS_ERASE_CHECK_BATCH));
    if (erased.size > 0) return erased.size;
  }
  if (input.operatorFence) {
    for (let i = 0; i < keys.length; i += SYNTHESIS_ERASE_CHECK_BATCH) {
      const fenced = await input.operatorFence(input.orgId, input.workspaceId, keys.slice(i, i + SYNTHESIS_ERASE_CHECK_BATCH));
      if (fenced.size > 0) return fenced.size;
    }
  }
  return 0;
}

const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?(?:Z|[+-]\d{2}:\d{2})$/u;

/** `since` and `until` as the artifact keys them: ISO-8601 with a zone, normalized to UTC ms; at most 30 days. */
export function synthesisWindow(since: string | undefined, until: string | undefined): { since: string; until: string } | string {
  if (since === undefined || until === undefined) return "since and until are required";
  if (!ISO.test(since) || !ISO.test(until)) return "since and until must be ISO-8601 timestamps with a zone";
  const s = Date.parse(since), u = Date.parse(until);
  if (!Number.isFinite(s) || !Number.isFinite(u) || s >= u) return "since must be before until";
  if (u - s > SYNTHESIS_BOUNDS.maxWindowMs) return "the window is longer than 30 days";
  return { since: new Date(s).toISOString(), until: new Date(u).toISOString() };
}

export type SynthesisRead =
  | { status: "ok"; synthesis: WorkspaceSynthesis }
  | { status: "not_found" }
  | { status: "invalidated"; erased: number };

/** Read, then the erase and fence checks (after the object IO, so an erase or fence during the read still wins). */
export async function readWorkspaceSynthesis(input: SynthesisChecks & {
  source: WorkspaceSynthesisSource;
  window: { since: string; until: string };
}): Promise<SynthesisRead> {
  let artifact: SynthesisArtifact | null;
  try {
    artifact = await input.source.latest(input.orgId, input.workspaceId, input.window);
  } catch (error) {
    if (error instanceof SynthesisArtifactError && error.code === "synthesis_cross_tenant") throw new TenantMismatchError();
    throw error;
  }
  if (!artifact) return { status: "not_found" };
  if (artifact.orgId !== input.orgId || artifact.workspace !== input.workspaceId) throw new TenantMismatchError();
  const erased = await invalidatedCount(input, [...artifact.considered, ...artifact.related]);
  if (erased > 0) return { status: "invalidated", erased };
  return { status: "ok", synthesis: artifact.synthesis };
}
