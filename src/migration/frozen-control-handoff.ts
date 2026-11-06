/**
 * P1 option B: privacy/control HANDOFF from Neon to the Supabase mirror without a lost update.
 *
 * Option B runs the record copy with DELIVERY_CUSTODY_SOURCE_MODE=relayhistory (mirror OFF), so privacy
 * selection/reset keep working on Neon and the mirror drifts. The handoff makes the mirror EXACTLY equal to
 * Neon (pending heads included) at a moment when no Neon control can commit, and keeps that true until the
 * completeness seal opens the mirror path.
 *
 * Why the fence is real (deployed code, no change needed):
 *  - The fence is "mirror ON while UNSEALED". In RHC c7de216 mirror mode, routes/delivery.ts calls
 *    mirrorControlBegin BEFORE setSessionUploads/resetUploadedSessions (the only Neon control writes).
 *    That begin is sessions.ingest_source_control_begin, which calls ingest_frozen_source_policy() BEFORE it
 *    writes anything and raises "copy not sealed" until the completeness row exists. So every mirror-mode
 *    control 503s before it touches Neon or the mirror. Uploads are unaffected.
 *  - The fence opens on its own, atomically, at the moment the seal transaction commits.
 *
 * Why the Neon head vector is a complete capture cursor (relayhistory 0078):
 *  - In phase=active, every write to a frozen legacy table (delivery_records, deleted_upload_sessions,
 *    session_resets, delivery_origins, ...) needs a permit row for its xid. Only
 *    require_delivery_cutover_control_v1 inserts permits (the runtime role has no EXECUTE on it), and it is
 *    called only from begin_delivery_cutover_control_v1. That function inserts or version-bumps the user's
 *    control head in the SAME transaction. A pending replay (apply=false) writes nothing.
 *  - So: same status (phase/epoch/baseline) and same head set means no legacy privacy or record write
 *    committed in between. Two equal snapshots taken `settle` apart, plus a re-check inside the sync
 *    transaction, prove that no straggler control from a pre-fence isolate committed across the handoff.
 *  - Orgs with any head in the epoch are the only orgs whose frozen records can differ from the copy
 *    (controls delete/tombstone records). They are counted as affectedOrgs; --seal re-verifies them.
 *
 * What the sync writes (owner, one transaction, Neon never written):
 *  - every Neon head copied exactly (epoch/version/digest/kind/payload/pending/before_privacy/privacy_version),
 *    as a compare-and-set against the mirror row that was read;
 *  - privacy rows made exactly equal to Neon. A removal or a reset change is allowed ONLY for a tenant that has
 *    a Neon head (only a control can change frozen privacy). Any other difference is unexplained and refuses.
 *  - refuses: mirror head newer, same version with a different digest, another epoch, a mirror head Neon lacks,
 *    a sealed mirror (the fence would be open), a policy/phase/epoch mismatch, a live copy session, a busy lock,
 *    a plan hash that differs from the reviewed dry run, or a Neon change before COMMIT.
 */
import type { Queryable } from "../hoopdb/frozen-control-head-repair.ts";
import {
  byKey, COPY_ACTIVE_SQL, compare, delKey, headFull, headKey, LOCK_KEY, readMirror, readSource, refuse, resetKey, setSha, sha, sourceFingerprint, HEX64,
  type Census, type Deletion, type Head, type MirrorState, type ParityOptions, type Reset, type SourceState,
} from "./frozen-control-parity.ts";

export interface HandoffOptions extends ParityOptions { settleMs: number; sleep?: (ms: number) => Promise<void> }
export interface HandoffPlan {
  heads: { neon: Head; prior: { version: string; digest: string; pending: boolean } | null }[];
  insertDeletions: Deletion[]; removeDeletions: Deletion[]; insertResets: Reset[]; updateResets: Reset[]; removeResets: Reset[];
  affectedOrgs: number; affectedTenants: number; blocked: string[]; sha256: string;
}
export interface HandoffCensus {
  version: 1; stable: boolean; neonFingerprintSha256: string; exactParity: boolean; census: Census;
  handoff: { insertHeads: number; overwriteHeads: number; pendingHeads: number; insertDeletions: number; removeDeletions: number;
    insertResets: number; updateResets: number; removeResets: number; writes: number; affectedOrgs: number; affectedTenants: number;
    blocked: string[]; planSha256: string };
}

const resetLine = (r: Reset) => JSON.stringify([resetKey(r), r.deleted]);
/** Hash of the complete Neon privacy/control state, status included (the same fingerprint the #260 apply binds). */
export const neonFingerprint = sourceFingerprint;
const tenantOf = (r: { org_id: string; workspace_id: string; user_id: string }) => headKey(r);

/** Pure: the exact-sync plan that makes the mirror equal Neon, or the reasons it must not run. */
export function handoffPlan(src: SourceState, mir: MirrorState): HandoffPlan {
  const plan: HandoffPlan = { heads: [], insertDeletions: [], removeDeletions: [], insertResets: [], updateResets: [], removeResets: [],
    affectedOrgs: 0, affectedTenants: 0, blocked: [], sha256: "" };
  const block = (c: string) => { if (!plan.blocked.includes(c)) plan.blocked.push(c); };
  const pol = mir.policy;
  if (src.status.phase !== "active") block("neon_phase_not_active");
  if (!pol) block("supabase_policy_missing");
  else if (pol.controlEpoch !== src.status.epoch || pol.privacyBaselineSha256 !== src.status.privacyBaselineSha256) block("policy_mismatch");
  if (mir.sealed) block("sealed_fence_unavailable");

  const nh = byKey(src.heads, headKey), mh = byKey(mir.heads, headKey);
  const tenants = new Set<string>(), orgs = new Set<string>();
  for (const [k, n] of nh) {
    tenants.add(k); orgs.add(n.org_id);
    if (n.epoch !== src.status.epoch) { block("neon_head_other_epoch"); continue; }
    const m = mh.get(k);
    if (!m) { plan.heads.push({ neon: n, prior: null }); continue; }
    if (headFull(m) === headFull(n)) continue;
    if (m.epoch !== n.epoch) block("mirror_head_other_epoch");
    else if (BigInt(m.version) > BigInt(n.version)) block("mirror_head_newer");
    else if (m.version === n.version && m.digest !== n.digest) block("mirror_head_divergent");
    else plan.heads.push({ neon: n, prior: { version: m.version, digest: m.digest, pending: m.pending } });
  }
  for (const k of mh.keys()) if (!nh.has(k)) block("mirror_head_without_neon_head");

  const nd = byKey(src.deletions, delKey), md = byKey(mir.deletions, delKey);
  for (const [k, d] of nd) if (!md.has(k)) plan.insertDeletions.push(d); // frozen state never seeded, or a control's fence: always safe
  for (const [k, d] of md) if (!nd.has(k)) { if (tenants.has(tenantOf(d))) plan.removeDeletions.push(d); else block("unexplained_privacy_divergence"); }
  const nr = byKey(src.resets, resetKey), mr = byKey(mir.resets, resetKey);
  for (const [k, r] of nr) {
    const m = mr.get(k);
    if (!m) plan.insertResets.push(r);
    else if (m.deleted !== r.deleted) { if (tenants.has(tenantOf(r))) plan.updateResets.push(r); else block("unexplained_privacy_divergence"); }
  }
  for (const [k, r] of mr) if (!nr.has(k)) { if (tenants.has(tenantOf(r))) plan.removeResets.push(r); else block("unexplained_privacy_divergence"); }

  plan.affectedOrgs = orgs.size; plan.affectedTenants = tenants.size;
  plan.sha256 = sha(JSON.stringify({
    v: "handoff-1", neon: neonFingerprint(src), policy: pol, sealed: mir.sealed, blocked: plan.blocked,
    heads: plan.heads.map((h) => JSON.stringify([headFull(h.neon), h.prior])).sort(),
    insertDeletions: plan.insertDeletions.map(delKey).sort(), removeDeletions: plan.removeDeletions.map(delKey).sort(),
    insertResets: plan.insertResets.map(resetLine).sort(), updateResets: plan.updateResets.map(resetLine).sort(), removeResets: plan.removeResets.map(resetLine).sort(),
  }));
  return plan;
}

const exactParity = (src: SourceState, mir: MirrorState) =>
  setSha(src.heads.map(headFull)) === setSha(mir.heads.map(headFull))
  && setSha(src.deletions.map(delKey)) === setSha(mir.deletions.map(delKey))
  && setSha(src.resets.map(resetLine)) === setSha(mir.resets.map(resetLine));

function summarize(stable: boolean, src: SourceState, mir: MirrorState, plan: HandoffPlan, opts: ParityOptions): HandoffCensus {
  const blocked = stable ? plan.blocked : ["neon_not_stable", ...plan.blocked];
  const overwrite = plan.heads.filter((h) => h.prior).length;
  const writes = plan.heads.length + plan.insertDeletions.length + plan.removeDeletions.length + plan.insertResets.length + plan.updateResets.length + plan.removeResets.length;
  return {
    version: 1, stable, neonFingerprintSha256: neonFingerprint(src), exactParity: exactParity(src, mir), census: compare(src, mir, opts).census,
    handoff: { insertHeads: plan.heads.length - overwrite, overwriteHeads: overwrite, pendingHeads: plan.heads.filter((h) => h.neon.pending).length,
      insertDeletions: plan.insertDeletions.length, removeDeletions: plan.removeDeletions.length, insertResets: plan.insertResets.length,
      updateResets: plan.updateResets.length, removeResets: plan.removeResets.length, writes, affectedOrgs: plan.affectedOrgs,
      affectedTenants: plan.affectedTenants, blocked, planSha256: plan.sha256 },
  };
}

async function stableSource(neon: Queryable, opts: HandoffOptions): Promise<{ src: SourceState; stable: boolean }> {
  if (!Number.isFinite(opts.settleMs) || opts.settleMs < 0) refuse("settle_invalid");
  const a = await readSource(neon, opts);
  await (opts.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms))))(opts.settleMs);
  const b = await readSource(neon, opts);
  return { src: b, stable: neonFingerprint(a) === neonFingerprint(b) };
}

/** Read-only dry run: two Neon snapshots `settleMs` apart, one READ ONLY mirror snapshot. Aggregate output. */
export async function handoffCensus(neon: Queryable, supa: Queryable, opts: HandoffOptions): Promise<HandoffCensus> {
  const { src, stable } = await stableSource(neon, opts);
  await supa.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  let mir: MirrorState;
  try { mir = await readMirror(supa, opts); } finally { await supa.query("ROLLBACK"); }
  return summarize(stable, src, mir, handoffPlan(src, mir), opts);
}

/** Exact sync under the fence, bound to the reviewed handoff planSha256. Mirror only; Neon is never written. */
export async function handoffSync(neon: Queryable, supa: Queryable, expectPlanSha256: string, opts: HandoffOptions): Promise<HandoffCensus> {
  if (!HEX64.test(expectPlanSha256)) refuse("expect_plan_invalid");
  const { src, stable } = await stableSource(neon, opts);
  if (!stable) refuse("neon_not_stable");
  await supa.query("BEGIN");
  try {
    await supa.query("SET LOCAL lock_timeout='5s'");
    await supa.query("SET LOCAL statement_timeout='120s'");
    if ((await supa.query<any>(LOCK_KEY)).rows[0]?.ok !== true) refuse("parity_operator_busy");
    if ((await supa.query<any>(COPY_ACTIVE_SQL)).rows[0]?.n !== 0) refuse("frozen_copy_session_active");
    await supa.query("LOCK TABLE sessions.ingest_source_control_heads,sessions.ingest_frozen_source_deletions,sessions.ingest_frozen_source_resets IN SHARE ROW EXCLUSIVE MODE");
    // The seal row is what opens the fence; hold it still for the life of this transaction.
    await supa.query("LOCK TABLE sessions.ingest_frozen_source_completeness,sessions.ingest_lazy_policy IN SHARE MODE");
    const mir = await readMirror(supa, opts);
    const plan = handoffPlan(src, mir);
    if (plan.blocked.length) refuse(plan.blocked[0]!);
    if (plan.sha256 !== expectPlanSha256) refuse("plan_changed_rerun_handoff_census");

    for (const { neon: h, prior } of plan.heads) {
      const r = await supa.query(`INSERT INTO sessions.ingest_source_control_heads AS m(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending,before_privacy,privacy_version)
        VALUES($1,$2,$3,$4,$5::bigint,$6,$7,$8::jsonb,$9,$10::jsonb,$11::bigint)
        ON CONFLICT (org_id,workspace_id,user_id) DO UPDATE SET epoch=EXCLUDED.epoch,version=EXCLUDED.version,digest=EXCLUDED.digest,kind=EXCLUDED.kind,
          payload=EXCLUDED.payload,pending=EXCLUDED.pending,before_privacy=EXCLUDED.before_privacy,privacy_version=EXCLUDED.privacy_version,updated_at=clock_timestamp()
        WHERE m.version=$12::bigint AND m.digest=$13 AND m.pending=$14 RETURNING 1 AS x`,
        [h.org_id, h.workspace_id, h.user_id, h.epoch, h.version, h.digest, h.kind, h.payload, h.pending, h.before_privacy, h.privacy_version,
          prior?.version ?? null, prior?.digest ?? null, prior?.pending ?? null]);
      if (r.rows.length !== 1) refuse("handoff_postcondition_heads");
    }
    const delCols = ["org_id", "workspace_id", "user_id", "origin_id", "source", "session_id"] as const;
    const dcol = (rows: Deletion[]) => delCols.map((k) => rows.map((d) => d[k]));
    if (plan.insertDeletions.length && (await supa.query(`INSERT INTO sessions.ingest_frozen_source_deletions SELECT * FROM unnest($1::text[],$2::text[],$3::text[],$4::text[],$5::text[],$6::text[]) ON CONFLICT DO NOTHING RETURNING 1 AS x`, dcol(plan.insertDeletions))).rows.length !== plan.insertDeletions.length) refuse("handoff_postcondition_deletions");
    if (plan.removeDeletions.length && (await supa.query(`DELETE FROM sessions.ingest_frozen_source_deletions d USING unnest($1::text[],$2::text[],$3::text[],$4::text[],$5::text[],$6::text[]) AS u(a,b,c,e,f,g)
        WHERE d.org_id=u.a AND d.workspace_id=u.b AND d.user_id=u.c AND d.origin_id=u.e AND d.source=u.f AND d.session_id=u.g RETURNING 1 AS x`, dcol(plan.removeDeletions))).rows.length !== plan.removeDeletions.length) refuse("handoff_postcondition_deletions");
    const rcol = (rows: Reset[]) => (["org_id", "workspace_id", "user_id", "request_id", "deleted"] as const).map((k) => rows.map((r) => r[k]));
    if (plan.insertResets.length && (await supa.query(`INSERT INTO sessions.ingest_frozen_source_resets SELECT * FROM unnest($1::text[],$2::text[],$3::text[],$4::text[],$5::bigint[]) ON CONFLICT DO NOTHING RETURNING 1 AS x`, rcol(plan.insertResets))).rows.length !== plan.insertResets.length) refuse("handoff_postcondition_resets");
    if (plan.updateResets.length && (await supa.query(`UPDATE sessions.ingest_frozen_source_resets r SET deleted=u.e FROM unnest($1::text[],$2::text[],$3::text[],$4::text[],$5::bigint[]) AS u(a,b,c,d,e)
        WHERE r.org_id=u.a AND r.workspace_id=u.b AND r.user_id=u.c AND r.request_id=u.d RETURNING 1 AS x`, rcol(plan.updateResets))).rows.length !== plan.updateResets.length) refuse("handoff_postcondition_resets");
    if (plan.removeResets.length && (await supa.query(`DELETE FROM sessions.ingest_frozen_source_resets r USING unnest($1::text[],$2::text[],$3::text[],$4::text[],$5::bigint[]) AS u(a,b,c,d,e)
        WHERE r.org_id=u.a AND r.workspace_id=u.b AND r.user_id=u.c AND r.request_id=u.d RETURNING 1 AS x`, rcol(plan.removeResets))).rows.length !== plan.removeResets.length) refuse("handoff_postcondition_resets");

    // Inside the transaction: the mirror is now byte-exact to the Neon snapshot...
    const after = await readMirror(supa, opts);
    if (!exactParity(src, after)) refuse("handoff_verify_mismatch");
    // ...and Neon has not moved since (no straggler control committed during the sync).
    if (neonFingerprint(await readSource(neon, opts)) !== neonFingerprint(src)) refuse("neon_changed_during_sync");
    await supa.query("COMMIT");
  } catch (e) { await supa.query("ROLLBACK").catch(() => {}); throw e; }
  return handoffCensus(neon, supa, { ...opts, settleMs: 0 });
}
