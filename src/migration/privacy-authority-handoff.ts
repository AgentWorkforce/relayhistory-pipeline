/**
 * C3: hand the privacy authority for selection/reset from Neon to Supabase (supabase/pending/ingest_privacy_authority.sql).
 *
 *   census   READ-ONLY on both sides: Neon's privacy fingerprint vs Supabase's (heads, deletions, resets), aggregate counts.
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
  neon: { fingerprint: string; counts: NeonPrivacySnapshot["counts"] };
  supabase: SupabaseAuthorityState;
  equal: boolean;
}

export async function census(neon: Queryable, supa: Queryable, epoch: string): Promise<Census> {
  const n = await readNeonSnapshot(neon, epoch);
  const s = await readSupabaseState(supa);
  return { neon: { fingerprint: n.fingerprint, counts: n.counts }, supabase: s, equal: n.fingerprint === s.fingerprintSha256 };
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
  const activated = await ownerCall(supa, "ingest_privacy_authority_activate", {
    controlEpoch: opts.epoch,
    neonFingerprintSha256: b.fingerprint,
    heads: b.heads,
    deletions: b.deletions,
    resets: b.resets,
  });
  if (activated.state !== "active" || activated.fingerprintSha256 !== b.fingerprint) throw new HandoffRefusal("activation_result_invalid");
  const c = await readNeonSnapshot(neon, opts.epoch);
  return { activated, neonFingerprint: b.fingerprint, neonStableAfter: c.fingerprint === b.fingerprint };
}
