/**
 * C3: hand the privacy authority for selection/reset from Neon to Supabase (supabase/migrations/20261105041000_ingest_privacy_authority.sql).
 *
 *   census   READ-ONLY on both sides: Neon's privacy fingerprint vs Supabase's (heads, deletions, resets), and Neon's
 *            owned-session digest vs the Supabase owned copy (reset coverage), aggregate counts.
 *   sync-owned Supabase owner write (never once active): stream Neon's owned identities (exactly what Neon's reset would
 *            fence: owned_uploaded_session_identities for every user) and make the Supabase copy exactly that set.
 *   fence    Supabase owner write: the legacy Neon-first mirror begin refuses from now on (controls 503 BEFORE Neon).
 *   activate Supabase owner write, while fenced: two identical Neon snapshots >= settle apart with no pending head,
 *            then ONE Supabase transaction makes heads/frozen privacy exactly that snapshot (bound by the
 *            Neon-computed fingerprint), proves the heads against the target's control receipts, and activates.
 *            Neon is re-read afterwards: any change means a writer bypassed the fence (STOP, release at once).
 *   release  Supabase owner write: back to Neon authority (always from fenced; from active only while unchanged).
 *
 * Neon is only ever read (READ ONLY, REPEATABLE READ). Output is aggregate: counts, booleans and sha256 values,
 * never identifiers or content.
 */
import type { Queryable } from "./relayhistory-export.ts";

const HEX64 = /^[0-9a-f]{64}$/;

/**
 * The SAME fingerprint expression as sessions.ingest_privacy_authority_state(), over Neon's own tables. Both sides
 * are PostgreSQL jsonb text with every order pinned to COLLATE "C", so equal state gives byte-equal packets.
 */
export const NEON_PRIVACY_SNAPSHOT_SQL = `
WITH st AS (SELECT sessions.delivery_cutover_status() AS s),
h AS (SELECT * FROM sessions.delivery_cutover_control_heads),
d AS (SELECT org_id,workspace_id,user_id,origin_id,source,session_id FROM sessions.deleted_upload_sessions),
r AS (SELECT org_id,workspace_id,user_id,request_id,deleted FROM sessions.session_resets),
hp AS (SELECT coalesce(jsonb_agg(jsonb_build_array(org_id,workspace_id,user_id,epoch,version::text,digest,kind,pending,deleted::text)
  ORDER BY org_id COLLATE "C",workspace_id COLLATE "C",user_id COLLATE "C"),'[]'::jsonb) AS v FROM h),
dp AS (SELECT coalesce(jsonb_agg(jsonb_build_array(org_id,workspace_id,user_id,origin_id,source,session_id)
  ORDER BY org_id COLLATE "C",workspace_id COLLATE "C",user_id COLLATE "C",origin_id COLLATE "C",source COLLATE "C",session_id COLLATE "C"),'[]'::jsonb) AS v FROM d),
rp AS (SELECT coalesce(jsonb_agg(jsonb_build_array(org_id,workspace_id,user_id,request_id,deleted::text)
  ORDER BY org_id COLLATE "C",workspace_id COLLATE "C",user_id COLLATE "C",request_id COLLATE "C"),'[]'::jsonb) AS v FROM r)
SELECT (SELECT s FROM st) AS cutover,
 encode(sha256(convert_to(jsonb_build_array('privacy-authority-v1',(SELECT v FROM hp),(SELECT v FROM dp),(SELECT v FROM rp))::text,'UTF8')),'hex') AS fingerprint,
 (SELECT coalesce(jsonb_agg(jsonb_build_object('orgId',org_id,'workspaceId',workspace_id,'userId',user_id,'epoch',epoch,'version',version::text,
   'digest',digest,'kind',kind,'pending',pending,'deleted',deleted::text,'payload',payload,'privacyVersion',privacy_version::text)),'[]'::jsonb) FROM h) AS heads,
 (SELECT v FROM dp) AS deletions,(SELECT v FROM rp) AS resets,
 (SELECT count(*) FROM h)::int AS head_count,(SELECT count(*) FILTER (WHERE pending) FROM h)::int AS pending_count,
 (SELECT count(*) FROM d)::int AS deletion_count,(SELECT count(*) FROM r)::int AS reset_count`;

/**
 * Neon's sessions.owned_uploaded_session_identities() (relayhistory 0038) for EVERY user at once: the support identities
 * with a session, plus session ids recovered from null-session delete tombstones through neighborhood_claims. This is
 * exactly the set Neon's reset fences; the Supabase authority's reset must never cover less.
 */
export const NEON_OWNED_SESSIONS_SQL = `
SELECT org_id,workspace_id,user_id,origin_id,source,session_id FROM (
 SELECT org_id,workspace_id,user_id,origin_id,source,session_id FROM sessions.uploaded_support_identities WHERE session_id IS NOT NULL
 UNION
 SELECT r.org_id,r.workspace_id,r.user_id,r.origin_id,r.source,k->>'sessionId'
   FROM sessions.delivery_records r JOIN sessions.neighborhood_claims c ON c.org_id=r.org_id
   CROSS JOIN LATERAL jsonb_array_elements(c.support_event_ids) k
  WHERE r.session_id IS NULL AND r.operation='delete'
    AND k->>'machineId'='delivery:' || md5(jsonb_build_array(r.workspace_id,r.origin_id)::text)
    AND k->>'source'=r.source
    AND k->>'eventId'='delivery:' || md5(jsonb_build_array(r.workspace_id,r.origin_id)::text) || ':' || r.record_id
    AND length(k->>'sessionId') BETWEEN 1 AND 512
) o`;
const OWNED_ORDER = `org_id COLLATE "C",workspace_id COLLATE "C",user_id COLLATE "C",origin_id COLLATE "C",source COLLATE "C",session_id COLLATE "C"`;
/** The same digest text as sessions.ingest_privacy_authority_owned_digest(), over Neon's owned identities. */
export const NEON_OWNED_SESSIONS_DIGEST_SQL = `WITH o AS (${NEON_OWNED_SESSIONS_SQL})
SELECT count(*)::text AS rows,encode(sha256(convert_to(coalesce(string_agg(jsonb_build_array(org_id,workspace_id,user_id,origin_id,source,session_id)::text,E'\\n'
 ORDER BY ${OWNED_ORDER}),''),'UTF8')),'hex') AS sha256 FROM o`;
export interface OwnedDigest { rows: string; sha256: string }

export async function readNeonOwnedDigest(neon: Queryable): Promise<OwnedDigest> {
  return inReadOnly(neon, async () => {
    const r = (await neon.query<OwnedDigest>(NEON_OWNED_SESSIONS_DIGEST_SQL)).rows[0];
    if (!r || !/^(0|[1-9][0-9]*)$/.test(r.rows) || !HEX64.test(r.sha256)) throw new HandoffRefusal("neon_owned_digest_invalid");
    if (Number(r.rows) > 5_000_000) throw new HandoffRefusal("neon_owned_sessions_too_large");
    return { rows: r.rows, sha256: r.sha256 };
  });
}

export async function readSupabaseOwnedDigest(supa: Queryable): Promise<OwnedDigest> {
  return inReadOnly(supa, async () => {
    const r = (await supa.query<{ d: OwnedDigest }>("SELECT sessions.ingest_privacy_authority_owned_digest() AS d")).rows[0]?.d;
    if (!r || !HEX64.test(r.sha256)) throw new HandoffRefusal("supabase_owned_digest_invalid");
    return r;
  });
}

type OwnedRow = { org_id: string; workspace_id: string; user_id: string; origin_id: string; source: string; session_id: string };
const OWNED_COLS = "org_id,workspace_id,user_id,origin_id,source,session_id";
const KEY_C = "(org_id COLLATE \"C\",workspace_id COLLATE \"C\",user_id COLLATE \"C\",origin_id COLLATE \"C\",source COLLATE \"C\",session_id COLLATE \"C\")";

/**
 * Make the Supabase owned copy exactly Neon's owned identities. One Neon REPEATABLE READ READ ONLY cursor streams the
 * set in C order; each page is applied in its own Supabase transaction: insert the page, delete every Supabase row in
 * the page's key range that Neon does not have, and finally every row past Neon's last key. Run it while fenced so Neon
 * cannot change underneath; activation then refuses unless both digests are equal. Neon is only read.
 */
export async function syncOwned(
  neon: Queryable,
  supa: Queryable,
  opts: { pageRows?: number; hooks?: { beforePageCommit?: (page: number) => Promise<void> } } = {},
): Promise<{ rows: number; inserted: number; removed: number }> {
  const page = opts.pageRows ?? 5000;
  if (!Number.isInteger(page) || page < 1 || page > 20000) throw new HandoffRefusal("invalid_page_rows");
  // One sync at a time: an overlapping run's older Neon cursor could undo a newer run's pages. A SESSION lock held for
  // the whole run (each page additionally serializes with activation through the guard's authority lock). Session
  // locks need session affinity, which a transaction pooler breaks (another client can be handed this backend, where
  // the lock is reentrant; an unlock can land elsewhere). So the backend is pinned by pid: the lock is taken only on
  // the backend that just proved it does not already hold it, every page checks the pid before writing, and the
  // unlock runs only on that backend. Any mismatch refuses (sync_session_reassigned).
  const probe = (await supa.query<{ pid: number; held: boolean }>(
    "SELECT pg_backend_pid() AS pid, EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=1919249529 AND objid=97 AND objsubid=2 AND pid=pg_backend_pid() AND granted) AS held",
  )).rows[0];
  if (!probe || typeof probe.pid !== "number") throw new HandoffRefusal("sync_session_unknown");
  if (probe.held) throw new HandoffRefusal("sync_owned_in_progress");
  const pid = probe.pid;
  const got = (await supa.query<{ ok: boolean | null; pid: number }>(
    "SELECT CASE WHEN pg_backend_pid()=$1::int THEN pg_try_advisory_lock(1919249529,97) END AS ok, pg_backend_pid() AS pid", [pid],
  )).rows[0];
  if (!got || got.pid !== pid || got.ok === null) throw new HandoffRefusal("sync_session_reassigned");
  if (got.ok !== true) throw new HandoffRefusal("sync_owned_in_progress");
  let failure: unknown;
  try {
    return await syncOwnedLocked(neon, supa, page, pid, opts.hooks?.beforePageCommit);
  } catch (error) {
    failure = error;
    throw error;
  } finally {
    const un = await supa.query<{ ok: boolean | null; pid: number }>(
      "SELECT CASE WHEN pg_backend_pid()=$1::int THEN pg_advisory_unlock(1919249529,97) END AS ok, pg_backend_pid() AS pid", [pid],
    ).then((r) => r.rows[0], () => undefined);
    // An unlock that did not run on the locking backend leaves the lock to that session's end: report it, never hide it.
    if (failure === undefined && (!un || un.pid !== pid || un.ok !== true)) throw new HandoffRefusal("sync_session_reassigned");
  }
}

async function syncOwnedLocked(
  neon: Queryable,
  supa: Queryable,
  page: number,
  pid: number,
  beforePageCommit?: (page: number) => Promise<void>,
): Promise<{ rows: number; inserted: number; removed: number }> {
  const s = await readSupabaseState(supa);
  if (s.authority === "active") throw new HandoffRefusal("supabase_authority_active");
  let pages = 0;
  let rows = 0, inserted = 0, removed = 0;
  let after: OwnedRow | null = null;
  const applyPage = async (batch: OwnedRow[], last: boolean) => {
    await supa.query("BEGIN ISOLATION LEVEL READ COMMITTED");
    try {
      // Session affinity: the page must run on the backend that holds the sync lock, before any write.
      const here = (await supa.query<{ pid: number }>("SELECT pg_backend_pid() AS pid")).rows[0]?.pid;
      if (here !== pid) throw new HandoffRefusal("sync_session_reassigned");
      const doc = JSON.stringify(batch);
      const ins = await supa.query<{ n: string }>(
        `WITH x AS (INSERT INTO sessions.ingest_frozen_source_owned_sessions(${OWNED_COLS})
           SELECT ${OWNED_COLS} FROM jsonb_to_recordset($1::jsonb) AS r(org_id text,workspace_id text,user_id text,origin_id text,source text,session_id text)
           ON CONFLICT DO NOTHING RETURNING 1) SELECT count(*)::text AS n FROM x`, [doc]);
      inserted += Number(ins.rows[0]?.n ?? 0);
      const lo = after ? [after.org_id, after.workspace_id, after.user_id, after.origin_id, after.source, after.session_id] : null;
      const hi = batch.length ? batch[batch.length - 1]! : null;
      const conds: string[] = [];
      const params: unknown[] = [doc];
      if (lo) { params.push(...lo); conds.push(`${KEY_C} > ($2,$3,$4,$5,$6,$7)`); }
      if (hi && !last) { const b = params.length; params.push(hi.org_id, hi.workspace_id, hi.user_id, hi.origin_id, hi.source, hi.session_id); conds.push(`${KEY_C} <= ($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5},$${b + 6})`); }
      const del = await supa.query<{ n: string }>(
        `WITH x AS (DELETE FROM sessions.ingest_frozen_source_owned_sessions s WHERE ${conds.length ? conds.join(" AND ") : "true"}
           AND NOT EXISTS (SELECT 1 FROM jsonb_to_recordset($1::jsonb) AS r(org_id text,workspace_id text,user_id text,origin_id text,source text,session_id text)
             WHERE (r.org_id,r.workspace_id,r.user_id,r.origin_id,r.source,r.session_id)=(s.org_id,s.workspace_id,s.user_id,s.origin_id,s.source,s.session_id))
           RETURNING 1) SELECT count(*)::text AS n FROM x`, params);
      removed += Number(del.rows[0]?.n ?? 0);
      pages += 1;
      if (beforePageCommit) await beforePageCommit(pages);
      await supa.query("COMMIT");
    } catch (error) {
      await supa.query("ROLLBACK").catch(() => undefined);
      throw error;
    }
  };
  await neon.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    await neon.query(`DECLARE c3_owned NO SCROLL CURSOR FOR ${NEON_OWNED_SESSIONS_SQL} ORDER BY ${OWNED_ORDER}`);
    for (;;) {
      const batch = (await neon.query<OwnedRow>(`FETCH ${page} FROM c3_owned`)).rows;
      rows += batch.length;
      if (rows > 5_000_000) throw new HandoffRefusal("neon_owned_sessions_too_large");
      const last = batch.length < page;
      await applyPage(batch, last);
      if (last) break;
      after = batch[batch.length - 1]!;
    }
    await neon.query("COMMIT");
  } catch (error) {
    await neon.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
  return { rows, inserted, removed };
}

/** Bounds: the whole privacy state is tiny today (heads 0, deletions 8, resets 0); refuse anything implausible. */
export const SNAPSHOT_LIMITS = { heads: 100_000, deletions: 200_000, resets: 100_000 } as const;

export interface NeonPrivacySnapshot {
  fingerprint: string;
  epoch: string;
  heads: Record<string, unknown>[];
  deletions: unknown[];
  resets: unknown[];
  counts: { heads: number; pending: number; deletions: number; resets: number };
}

export class HandoffRefusal extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "HandoffRefusal";
  }
}

async function inReadOnly<T>(db: Queryable, fn: () => Promise<T>): Promise<T> {
  await db.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    const out = await fn();
    await db.query("COMMIT");
    return out;
  } catch (error) {
    await db.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

/** One consistent READ-ONLY Neon snapshot of the privacy authority state. */
export async function readNeonSnapshot(neon: Queryable, expectEpoch: string): Promise<NeonPrivacySnapshot> {
  if (!HEX64.test(expectEpoch)) throw new HandoffRefusal("invalid_epoch");
  return inReadOnly(neon, async () => {
    // Size first, so an implausible table never gets aggregated.
    const size = (await neon.query<{ h: number; d: number; r: number }>(
      `SELECT (SELECT count(*) FROM sessions.delivery_cutover_control_heads)::int AS h,
              (SELECT count(*) FROM sessions.deleted_upload_sessions)::int AS d,
              (SELECT count(*) FROM sessions.session_resets)::int AS r`,
    )).rows[0]!;
    if (size.h > SNAPSHOT_LIMITS.heads || size.d > SNAPSHOT_LIMITS.deletions || size.r > SNAPSHOT_LIMITS.resets)
      throw new HandoffRefusal("neon_privacy_state_too_large");
    const row = (await neon.query<Record<string, unknown>>(NEON_PRIVACY_SNAPSHOT_SQL)).rows[0];
    const cutover = row?.cutover as Record<string, unknown> | undefined;
    if (!row || !cutover || cutover.phase !== "active" || cutover.epoch !== expectEpoch) throw new HandoffRefusal("neon_cutover_not_active_epoch");
    if (typeof row.fingerprint !== "string" || !HEX64.test(row.fingerprint)) throw new HandoffRefusal("neon_fingerprint_invalid");
    const heads = row.heads as Record<string, unknown>[];
    if (heads.some((h) => h.epoch !== expectEpoch)) throw new HandoffRefusal("neon_head_other_epoch");
    return {
      fingerprint: row.fingerprint,
      epoch: expectEpoch,
      heads,
      deletions: row.deletions as unknown[],
      resets: row.resets as unknown[],
      counts: { heads: Number(row.head_count), pending: Number(row.pending_count), deletions: Number(row.deletion_count), resets: Number(row.reset_count) },
    };
  });
}

export interface SupabaseAuthorityState {
  authority: "neon" | "fenced" | "active";
  fingerprintSha256: string;
  heads: number;
  pendingHeads: number;
  deletions: number;
  resets: number;
  activatedFingerprintSha256: string | null;
}

export async function readSupabaseState(supa: Queryable): Promise<SupabaseAuthorityState> {
  return inReadOnly(supa, async () => {
    const r = (await supa.query<{ s: SupabaseAuthorityState }>("SELECT sessions.ingest_privacy_authority_state() AS s")).rows[0]?.s;
    if (!r || !HEX64.test(r.fingerprintSha256) || !["neon", "fenced", "active"].includes(r.authority)) throw new HandoffRefusal("supabase_state_invalid");
    return r;
  });
}

async function ownerCall(supa: Queryable, fn: string, p: unknown): Promise<Record<string, unknown>> {
  // READ COMMITTED (the authority lock refuses any other isolation), one statement, one transaction.
  await supa.query("BEGIN ISOLATION LEVEL READ COMMITTED");
  try {
    const r = (await supa.query<{ r: Record<string, unknown> }>(`SELECT sessions.${fn}($1::jsonb) AS r`, [JSON.stringify(p)])).rows[0]?.r;
    await supa.query("COMMIT");
    if (!r) throw new HandoffRefusal("owner_call_no_result");
    return r;
  } catch (error) {
    await supa.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

export interface Census {
  neon: { fingerprint: string; counts: NeonPrivacySnapshot["counts"]; owned: OwnedDigest };
  supabase: SupabaseAuthorityState & { owned: OwnedDigest };
  equal: boolean;
  ownedEqual: boolean;
}

export async function census(neon: Queryable, supa: Queryable, epoch: string): Promise<Census> {
  const n = await readNeonSnapshot(neon, epoch);
  const no = await readNeonOwnedDigest(neon);
  const s = await readSupabaseState(supa);
  const so = await readSupabaseOwnedDigest(supa);
  return {
    neon: { fingerprint: n.fingerprint, counts: n.counts, owned: no },
    supabase: { ...s, owned: so },
    equal: n.fingerprint === s.fingerprintSha256,
    ownedEqual: no.rows === so.rows && no.sha256 === so.sha256,
  };
}

export async function fence(supa: Queryable, epoch: string) {
  return ownerCall(supa, "ingest_privacy_authority_fence", { controlEpoch: epoch });
}

export async function release(supa: Queryable, epoch: string) {
  return ownerCall(supa, "ingest_privacy_authority_release", { controlEpoch: epoch });
}

export interface ActivateResult {
  activated: Record<string, unknown>;
  neonFingerprint: string;
  /** Neon re-read after the activation committed. false = a writer bypassed the fence: STOP and release. */
  neonStableAfter: boolean;
}

/**
 * Activation. Needs the fence already closed (authority 'fenced') and RHC at 100% on a mirror-first mode.
 * `expectNeonFingerprint` binds the run to the census the operator reviewed.
 */
export async function activate(
  neon: Queryable,
  supa: Queryable,
  opts: { epoch: string; expectNeonFingerprint: string; settleMs: number; sleep?: (ms: number) => Promise<void> },
): Promise<ActivateResult> {
  if (!HEX64.test(opts.expectNeonFingerprint)) throw new HandoffRefusal("invalid_expected_fingerprint");
  if (!Number.isInteger(opts.settleMs) || opts.settleMs < 0) throw new HandoffRefusal("invalid_settle");
  const sleep = opts.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const s = await readSupabaseState(supa);
  if (s.authority !== "fenced") throw new HandoffRefusal(`supabase_authority_${s.authority}_not_fenced`);
  if (s.pendingHeads !== 0) throw new HandoffRefusal("supabase_mirror_control_pending");
  const a = await readNeonSnapshot(neon, opts.epoch);
  if (a.fingerprint !== opts.expectNeonFingerprint) throw new HandoffRefusal("neon_changed_since_census");
  if (a.counts.pending !== 0) throw new HandoffRefusal("neon_control_pending");
  await sleep(opts.settleMs);
  const b = await readNeonSnapshot(neon, opts.epoch);
  if (b.fingerprint !== a.fingerprint) throw new HandoffRefusal("neon_not_stable");
  // Reset coverage: Neon's owned identities (exactly what Neon's reset fences). The SQL refuses unless the Supabase copy equals it.
  const owned = await readNeonOwnedDigest(neon);
  const activated = await ownerCall(supa, "ingest_privacy_authority_activate", {
    controlEpoch: opts.epoch,
    neonFingerprintSha256: b.fingerprint,
    heads: b.heads,
    deletions: b.deletions,
    resets: b.resets,
    neonOwned: owned,
  });
  if (activated.state !== "active" || activated.fingerprintSha256 !== b.fingerprint) throw new HandoffRefusal("activation_result_invalid");
  const c = await readNeonSnapshot(neon, opts.epoch);
  const ownedAfter = await readNeonOwnedDigest(neon);
  // Defense in depth: the committed Supabase copy must still be exactly what activation bound.
  const supaOwnedAfter = await readSupabaseOwnedDigest(supa);
  return {
    activated,
    neonFingerprint: b.fingerprint,
    neonStableAfter: c.fingerprint === b.fingerprint && ownedAfter.sha256 === owned.sha256 && supaOwnedAfter.sha256 === owned.sha256 && supaOwnedAfter.rows === owned.rows,
  };
}
