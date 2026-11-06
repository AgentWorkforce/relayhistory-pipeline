/**
 * P1 privacy/control parity between Neon source custody and the Supabase frozen custody mirror.
 *
 *   census(neon, supa)                 read-only on both sides; AGGREGATE output only (counts + set hashes)
 *   applySeedRepair(neon, supa, sha)   seed-only + qualified repair, bound to the census planSha256
 *
 * Neon (never written):   sessions.deleted_upload_sessions, sessions.session_resets,
 *                         sessions.delivery_cutover_control_heads, sessions.delivery_cutover_status()
 * Supabase (owner only):  sessions.ingest_frozen_source_deletions, sessions.ingest_frozen_source_resets,
 *                         sessions.ingest_source_control_heads, sessions.ingest_lazy_policy,
 *                         sessions.ingest_frozen_source_completeness
 *
 * The seed is the privacy/control section of `frozen-source-copy --copy` WITHOUT any record copy:
 * missing rows are inserted with the exact Neon values (jsonb and bigint carried as text, so nothing
 * is re-encoded through JS numbers). It never updates or deletes an existing row. Missing heads are
 * seeded only when Neon's head is completed (not pending) and in the live policy epoch, unless the
 * operator explicitly asks for copy-mode pending seeding. Existing heads are changed only through the
 * reviewed repairControlHead rules (both sides completed, same epoch, Neon strictly newer), applied as a compare-and-set to
 * EXACTLY the planned Neon version/digest inside the same locked transaction (byte-exact jsonb, unlike the JS round trip in
 * frozen-control-head-repair.ts). Everything else is counted as HELD; any HELD item blocks the apply.
 *
 * Fail closed: Neon must be phase=active with the same epoch/baseline as the live Supabase policy; no
 * frozen-copy session may be connected; the apply runs under an advisory lock plus table locks (mirror tables SHARE ROW
 * EXCLUSIVE; seal and policy tables SHARE, so a seal or policy change cannot commit underneath it), re-reads both sides,
 * refuses unless the recomputed plan hash (which covers HELD counts and set hashes) equals the reviewed dry-run hash, and
 * requires a FRESH Neon read under the lock, before and again after the writes, to equal the planned Neon snapshot. Every
 * write must land exactly once and is re-verified before COMMIT. A rerun after success plans zero writes.
 */
import { createHash } from "node:crypto";
import type { HeadKey, Queryable } from "../hoopdb/frozen-control-head-repair.ts";

export interface Head {
  org_id: string; workspace_id: string; user_id: string; epoch: string; version: string; digest: string;
  kind: string; payload: string; pending: boolean; before_privacy: string | null; privacy_version: string | null;
}
export interface Deletion { org_id: string; workspace_id: string; user_id: string; origin_id: string; source: string; session_id: string }
export interface Reset { org_id: string; workspace_id: string; user_id: string; request_id: string; deleted: string }
export interface Policy { sourceIdentitySha256: string; controlEpoch: string; privacyBaselineSha256: string }
export interface SourceState { status: { phase: string; epoch: string; privacyBaselineSha256: string }; heads: Head[]; deletions: Deletion[]; resets: Reset[] }
export interface MirrorState { policy: Policy | null; sealed: boolean; heads: Head[]; deletions: Deletion[]; resets: Reset[] }
export interface ParityOptions { seedPendingMissing?: boolean; maxRows?: number }

export class ParityRefusal extends Error {
  constructor(readonly code: string) { super("control parity: " + code); }
}
export const refuse = (code: string): never => { throw new ParityRefusal(code); };
export const HEX64 = /^[0-9a-f]{64}$/u;
const MAX_ROWS = 500_000;
export const OPERATOR_APP = "p1-frozen-control-parity";
// Every frozen-source-copy owner connection uses application_name p1-frozen-copy (scripts/frozen-source-copy.ts).
export const COPY_ACTIVE_SQL = "SELECT count(*)::int AS n FROM pg_stat_activity WHERE application_name LIKE 'p1-frozen-copy%' AND pid<>pg_backend_pid()";
export const LOCK_KEY = "SELECT pg_try_advisory_xact_lock(hashtextextended('p1-frozen-control-parity',0)) AS ok";

const HEAD_COLS = "org_id,workspace_id,user_id,epoch,version::text AS version,digest,kind,payload::text AS payload,pending,before_privacy::text AS before_privacy,privacy_version::text AS privacy_version";
export const DEL_COLS = "org_id,workspace_id,user_id,origin_id,source,session_id";
const RESET_COLS = "org_id,workspace_id,user_id,request_id,deleted::text AS deleted";
export const NEON_SQL = {
  status: "SELECT sessions.delivery_cutover_status() AS s",
  heads: `SELECT ${HEAD_COLS} FROM sessions.delivery_cutover_control_heads`,
  deletions: `SELECT ${DEL_COLS} FROM sessions.deleted_upload_sessions`,
  resets: `SELECT ${RESET_COLS} FROM sessions.session_resets`,
};
export const MIRROR_SQL = {
  policy: "SELECT source_identity_sha256,control_epoch,privacy_baseline_sha256 FROM sessions.ingest_lazy_policy WHERE singleton",
  sealed: "SELECT EXISTS(SELECT 1 FROM sessions.ingest_frozen_source_completeness WHERE singleton) AS sealed",
  heads: `SELECT ${HEAD_COLS} FROM sessions.ingest_source_control_heads`,
  deletions: `SELECT ${DEL_COLS} FROM sessions.ingest_frozen_source_deletions`,
  resets: `SELECT ${RESET_COLS} FROM sessions.ingest_frozen_source_resets`,
};

async function bounded<T>(q: Queryable, sql: string, max: number): Promise<T[]> {
  const rows = (await q.query<T>(`${sql} LIMIT ${max + 1}`)).rows;
  if (rows.length > max) refuse("row_bound_exceeded");
  return rows;
}
const normHead = (r: any): Head => ({
  org_id: r.org_id, workspace_id: r.workspace_id, user_id: r.user_id, epoch: r.epoch, version: r.version, digest: r.digest,
  kind: r.kind, payload: r.payload, pending: r.pending === true, before_privacy: r.before_privacy ?? null, privacy_version: r.privacy_version ?? null,
});

/** One REPEATABLE READ, READ ONLY Neon snapshot. Refuses unless the cutover is active. */
export async function readSource(neon: Queryable, opts: ParityOptions = {}): Promise<SourceState> {
  const max = opts.maxRows ?? MAX_ROWS;
  await neon.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    const s = (await neon.query<any>(NEON_SQL.status)).rows[0]?.s;
    if (!s || s.phase !== "active") refuse("neon_phase_not_active");
    if (!HEX64.test(s.epoch ?? "") || !HEX64.test(s.privacyBaselineSha256 ?? "")) refuse("neon_status_invalid");
    return {
      status: { phase: s.phase, epoch: s.epoch, privacyBaselineSha256: s.privacyBaselineSha256 },
      heads: (await bounded<any>(neon, NEON_SQL.heads, max)).map(normHead),
      deletions: await bounded<Deletion>(neon, NEON_SQL.deletions, max),
      resets: await bounded<Reset>(neon, NEON_SQL.resets, max),
    };
  } finally { await neon.query("ROLLBACK"); }
}

/** Reads the mirror inside the caller's transaction. */
export async function readMirror(supa: Queryable, opts: ParityOptions = {}): Promise<MirrorState> {
  const max = opts.maxRows ?? MAX_ROWS;
  const p = (await supa.query<any>(MIRROR_SQL.policy)).rows[0];
  return {
    policy: p ? { sourceIdentitySha256: p.source_identity_sha256, controlEpoch: p.control_epoch, privacyBaselineSha256: p.privacy_baseline_sha256 } : null,
    sealed: (await supa.query<any>(MIRROR_SQL.sealed)).rows[0]?.sealed === true,
    heads: (await bounded<any>(supa, MIRROR_SQL.heads, max)).map(normHead),
    deletions: await bounded<Deletion>(supa, MIRROR_SQL.deletions, max),
    resets: await bounded<Reset>(supa, MIRROR_SQL.resets, max),
  };
}

// JSON arrays make keys unambiguous (no separator collisions with arbitrary identifier text).
export const headKey = (h: { org_id: string; workspace_id: string; user_id: string }) => JSON.stringify([h.org_id, h.workspace_id, h.user_id]);
export const delKey = (d: Deletion) => JSON.stringify([d.org_id, d.workspace_id, d.user_id, d.origin_id, d.source, d.session_id]);
export const resetKey = (r: Reset) => JSON.stringify([r.org_id, r.workspace_id, r.user_id, r.request_id]);
const headCore = (h: Head) => JSON.stringify([headKey(h), h.version, h.digest, h.pending]); // exactly what --seal compares
export const headFull = (h: Head) => JSON.stringify([headKey(h), h.epoch, h.version, h.digest, h.kind, h.payload, h.pending, h.before_privacy, h.privacy_version]);
export const sha = (s: string) => createHash("sha256").update(s).digest("hex");
export const setSha = (lines: string[]) => sha([...lines].sort().join("\n"));
export const byKey = <T>(rows: T[], key: (r: T) => string) => {
  const m = new Map<string, T>();
  for (const r of rows) { const k = key(r); if (m.has(k)) refuse("duplicate_key_in_read"); m.set(k, r); }
  return m;
};
// Fields that shape the witness/begin behaviour of a head; before_privacy/privacy_version only matter while pending.
const sameHead = (a: Head, b: Head) => a.epoch === b.epoch && a.version === b.version && a.digest === b.digest && a.kind === b.kind
  && a.payload === b.payload && a.pending === b.pending && (!a.pending || (a.before_privacy === b.before_privacy && a.privacy_version === b.privacy_version));

export type HeadCategory = "equal" | "equalVersionDigestFieldsDiffer" | "pendingStateDiffers" | "missingSeedable" | "missingPending"
  | "missingOtherEpoch" | "staleQualified" | "staleNeonNewerPending" | "staleOtherEpoch" | "epochMismatch" | "mirrorNewer"
  | "sameVersionDigestDiffers" | "extra";
export const HEAD_CATEGORIES: HeadCategory[] = ["equal", "equalVersionDigestFieldsDiffer", "pendingStateDiffers", "missingSeedable", "missingPending",
  "missingOtherEpoch", "staleQualified", "staleNeonNewerPending", "staleOtherEpoch", "epochMismatch", "mirrorNewer", "sameVersionDigestDiffers", "extra"];

export interface Plan {
  deletions: Deletion[]; resets: Reset[]; heads: Head[]; repairs: { key: HeadKey; mirror: [string, string]; neon: [string, string] }[];
  sha256: string; blocked: string[];
}
export interface Census {
  version: 1;
  policy: { neonPhase: string; supabasePolicyPresent: boolean; epochMatches: boolean; baselineMatches: boolean; sealed: boolean };
  heads: Record<HeadCategory, number> & { neon: number; mirror: number; neonCoreSetSha256: string; mirrorCoreSetSha256: string };
  deletions: { neon: number; mirror: number; missing: number; extra: number; neonSetSha256: string; mirrorSetSha256: string };
  resets: { neon: number; mirror: number; missing: number; extra: number; conflicting: number; neonSetSha256: string; mirrorSetSha256: string };
  parity: { sealHeadParity: boolean; privacyParity: boolean; held: number };
  plan: { seedDeletions: number; seedResets: number; seedHeads: number; seedHeadsPending: number; repairHeads: number; writes: number; blocked: string[]; planSha256: string };
}

/** Pure: compares the two snapshots. The census holds only counts and hashes; the plan holds rows and stays in RAM. */
export function compare(src: SourceState, mir: MirrorState, opts: ParityOptions = {}): { census: Census; plan: Plan } {
  const pol = mir.policy;
  const epochMatches = !!pol && pol.controlEpoch === src.status.epoch;
  const baselineMatches = !!pol && pol.privacyBaselineSha256 === src.status.privacyBaselineSha256;
  const epoch = pol?.controlEpoch;
  const counts = Object.fromEntries(HEAD_CATEGORIES.map((c) => [c, 0])) as Record<HeadCategory, number>;
  const plan: Plan = { deletions: [], resets: [], heads: [], repairs: [], sha256: "", blocked: [] };
  let seedHeadsPending = 0;

  const nh = byKey(src.heads, headKey), mh = byKey(mir.heads, headKey);
  for (const [k, n] of nh) {
    const m = mh.get(k);
    let c: HeadCategory;
    if (!m) {
      if (n.epoch !== epoch) c = "missingOtherEpoch";
      else if (n.pending) c = "missingPending";
      else c = "missingSeedable";
      if (c === "missingSeedable" || (c === "missingPending" && opts.seedPendingMissing)) {
        plan.heads.push(n); if (n.pending) seedHeadsPending++;
      }
    } else if (m.epoch !== n.epoch) c = "epochMismatch";
    else if (m.version === n.version && m.digest === n.digest) c = m.pending !== n.pending ? "pendingStateDiffers" : sameHead(m, n) ? "equal" : "equalVersionDigestFieldsDiffer";
    else if (m.version === n.version) c = "sameVersionDigestDiffers";
    else if (BigInt(m.version) > BigInt(n.version)) c = "mirrorNewer";
    else if (m.pending || n.pending) c = "staleNeonNewerPending";
    else if (n.epoch !== epoch) c = "staleOtherEpoch";
    else {
      c = "staleQualified";
      plan.repairs.push({ key: { org: n.org_id, workspace: n.workspace_id, user: n.user_id }, mirror: [m.version, m.digest], neon: [n.version, n.digest] });
    }
    counts[c]++;
  }
  for (const k of mh.keys()) if (!nh.has(k)) counts.extra++;

  const nd = byKey(src.deletions, delKey), md = byKey(mir.deletions, delKey);
  let delMissing = 0, delExtra = 0;
  for (const [k, d] of nd) if (!md.has(k)) { delMissing++; plan.deletions.push(d); }
  for (const k of md.keys()) if (!nd.has(k)) delExtra++;

  const nr = byKey(src.resets, resetKey), mr = byKey(mir.resets, resetKey);
  let rMissing = 0, rExtra = 0, rConflict = 0;
  for (const [k, r] of nr) {
    const m = mr.get(k);
    if (!m) { rMissing++; plan.resets.push(r); } else if (m.deleted !== r.deleted) rConflict++;
  }
  for (const k of mr.keys()) if (!nr.has(k)) rExtra++;

  if (src.status.phase !== "active") plan.blocked.push("neon_phase_not_active");
  if (!pol) plan.blocked.push("supabase_policy_missing");
  else if (!epochMatches || !baselineMatches) plan.blocked.push("policy_mismatch");
  if (mir.sealed && (plan.deletions.length || plan.resets.length)) plan.blocked.push("privacy_seed_after_seal");

  const held = counts.equalVersionDigestFieldsDiffer + counts.pendingStateDiffers + counts.missingOtherEpoch + counts.staleNeonNewerPending
    + counts.staleOtherEpoch + counts.epochMismatch + counts.mirrorNewer + counts.sameVersionDigestDiffers + counts.extra
    + (opts.seedPendingMissing ? 0 : counts.missingPending) + delExtra + rExtra + rConflict;
  if (held) plan.blocked.push("held_items_present");
  const nonEqualHeads = HEAD_CATEGORIES.filter((c) => c !== "equal" && c !== "equalVersionDigestFieldsDiffer").reduce((a, c) => a + counts[c], 0);
  const sets = {
    neonHeads: setSha(src.heads.map(headCore)), mirrorHeads: setSha(mir.heads.map(headCore)),
    neonDeletions: setSha([...nd.keys()]), mirrorDeletions: setSha([...md.keys()]),
    neonResets: setSha(src.resets.map((r) => JSON.stringify([resetKey(r), r.deleted]))), mirrorResets: setSha(mir.resets.map((r) => JSON.stringify([resetKey(r), r.deleted]))),
  };
  const sortKey = <T>(rows: T[], f: (r: T) => string) => rows.map(f).sort();
  // Binds the write set AND everything the operator reviewed: every category count, HELD total and all six set hashes.
  plan.sha256 = sha(JSON.stringify({
    v: 2, epoch: src.status.epoch, baseline: src.status.privacyBaselineSha256, policy: pol, sealed: mir.sealed, seedPendingMissing: !!opts.seedPendingMissing,
    deletions: sortKey(plan.deletions, delKey), resets: sortKey(plan.resets, (r) => JSON.stringify([resetKey(r), r.deleted])),
    heads: sortKey(plan.heads, headFull), repairs: plan.repairs.map((r) => JSON.stringify(r)).sort(), blocked: plan.blocked,
    counts, privacy: [delMissing, delExtra, rMissing, rExtra, rConflict], held, sets,
  }));
  const census: Census = {
    version: 1,
    policy: { neonPhase: src.status.phase, supabasePolicyPresent: !!pol, epochMatches, baselineMatches, sealed: mir.sealed },
    heads: { ...counts, neon: nh.size, mirror: mh.size, neonCoreSetSha256: sets.neonHeads, mirrorCoreSetSha256: sets.mirrorHeads },
    deletions: { neon: nd.size, mirror: md.size, missing: delMissing, extra: delExtra, neonSetSha256: sets.neonDeletions, mirrorSetSha256: sets.mirrorDeletions },
    resets: { neon: nr.size, mirror: mr.size, missing: rMissing, extra: rExtra, conflicting: rConflict, neonSetSha256: sets.neonResets, mirrorSetSha256: sets.mirrorResets },
    parity: { sealHeadParity: nonEqualHeads === 0, privacyParity: delMissing + delExtra + rMissing + rExtra + rConflict === 0, held },
    plan: { seedDeletions: plan.deletions.length, seedResets: plan.resets.length, seedHeads: plan.heads.length, seedHeadsPending,
      repairHeads: plan.repairs.length, writes: plan.deletions.length + plan.resets.length + plan.heads.length + plan.repairs.length,
      blocked: plan.blocked, planSha256: plan.sha256 },
  };
  return { census, plan };
}

/** Read-only census: Neon snapshot, then a READ ONLY REPEATABLE READ mirror snapshot. */
export async function census(neon: Queryable, supa: Queryable, opts: ParityOptions = {}): Promise<Census> {
  const src = await readSource(neon, opts);
  await supa.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try { return compare(src, await readMirror(supa, opts), opts).census; }
  finally { await supa.query("ROLLBACK"); }
}

export interface ApplyResult {
  seeded: { deletions: number; resets: number; heads: number }; repairs: { repaired: number; already: number; refused: Record<string, number> };
  after: Census;
}
/** Hash of the complete Neon privacy/control state, status included (aggregate only). */
export const sourceFingerprint = (s: SourceState) => sha(JSON.stringify([s.status, setSha(s.heads.map(headFull)), setSha(s.deletions.map(delKey)),
  setSha(s.resets.map((r) => JSON.stringify([resetKey(r), r.deleted])))]));

/**
 * Seed-only + qualified repair, bound to the reviewed dry-run planSha256. Seeds and repairs commit together in ONE locked
 * transaction. `onCommitted` fires immediately after COMMIT, so a later failure (the closing census) cannot hide that writes landed.
 */
export async function applySeedRepair(neon: Queryable, supa: Queryable, expectPlanSha256: string, opts: ParityOptions = {},
  hooks: { onCommitted?: (r: { seeded: ApplyResult["seeded"]; repaired: number }) => void } = {}): Promise<ApplyResult> {
  if (!HEX64.test(expectPlanSha256)) refuse("expect_plan_invalid");
  const src = await readSource(neon, opts), planned = sourceFingerprint(src);
  let plan: Plan, repairedCount = 0;
  await supa.query("BEGIN");
  try {
    await supa.query("SET LOCAL lock_timeout='5s'");
    await supa.query("SET LOCAL statement_timeout='120s'");
    if ((await supa.query<any>(LOCK_KEY)).rows[0]?.ok !== true) refuse("parity_operator_busy");
    if ((await supa.query<any>(COPY_ACTIVE_SQL)).rows[0]?.n !== 0) refuse("frozen_copy_session_active");
    // Serialise against mirror begin/complete and any copy seed for the short life of this transaction...
    await supa.query("LOCK TABLE sessions.ingest_source_control_heads,sessions.ingest_frozen_source_deletions,sessions.ingest_frozen_source_resets IN SHARE ROW EXCLUSIVE MODE");
    // ...and hold `sealed` and the live policy still, so privacy_seed_after_seal and the epoch checks are not a TOCTOU.
    await supa.query("LOCK TABLE sessions.ingest_frozen_source_completeness,sessions.ingest_lazy_policy IN SHARE MODE");
    const mir = await readMirror(supa, opts);
    ({ plan } = compare(src, mir, opts));
    if (plan.blocked.length) refuse(plan.blocked[0]!);
    if (plan.sha256 !== expectPlanSha256) refuse("plan_changed_rerun_census");
    // Fresh Neon read UNDER the lock: every seed and repair below targets exactly this planned Neon state.
    if (sourceFingerprint(await readSource(neon, opts)) !== planned) refuse("neon_changed_since_plan");

    if (plan.deletions.length) {
      const col = (k: keyof Deletion) => plan.deletions.map((d) => d[k]);
      const r = await supa.query(`INSERT INTO sessions.ingest_frozen_source_deletions(${DEL_COLS})
        SELECT * FROM unnest($1::text[],$2::text[],$3::text[],$4::text[],$5::text[],$6::text[]) ON CONFLICT DO NOTHING RETURNING 1 AS x`,
        (["org_id", "workspace_id", "user_id", "origin_id", "source", "session_id"] as const).map(col));
      if (r.rows.length !== plan.deletions.length) refuse("seed_postcondition_deletions");
    }
    if (plan.resets.length) {
      const col = (k: keyof Reset) => plan.resets.map((d) => d[k]);
      const r = await supa.query(`INSERT INTO sessions.ingest_frozen_source_resets(org_id,workspace_id,user_id,request_id,deleted)
        SELECT * FROM unnest($1::text[],$2::text[],$3::text[],$4::text[],$5::bigint[]) ON CONFLICT DO NOTHING RETURNING 1 AS x`,
        (["org_id", "workspace_id", "user_id", "request_id", "deleted"] as const).map(col));
      if (r.rows.length !== plan.resets.length) refuse("seed_postcondition_resets");
    }
    for (const h of plan.heads) {
      const r = await supa.query(`INSERT INTO sessions.ingest_source_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending,before_privacy,privacy_version)
        VALUES($1,$2,$3,$4,$5::bigint,$6,$7,$8::jsonb,$9,$10::jsonb,$11::bigint) ON CONFLICT DO NOTHING RETURNING 1 AS x`,
        [h.org_id, h.workspace_id, h.user_id, h.epoch, h.version, h.digest, h.kind, h.payload, h.pending, h.before_privacy, h.privacy_version]);
      if (r.rows.length !== 1) refuse("seed_postcondition_heads");
    }
    // Qualified repair (repairControlHead's rules, already enforced by compare: both completed, same epoch == policy, Neon
    // strictly newer) as a CAS from the planned mirror head to EXACTLY the planned Neon head, never "whatever Neon has now".
    const nh = byKey(src.heads, headKey);
    const repaired: Head[] = [];
    for (const r of plan.repairs) {
      const n = nh.get(JSON.stringify([r.key.org, r.key.workspace, r.key.user]))!;
      if (n.version !== r.neon[0] || n.digest !== r.neon[1] || n.pending) refuse("repair_plan_inconsistent");
      const u = await supa.query(`UPDATE sessions.ingest_source_control_heads SET epoch=$4,version=$5::bigint,digest=$6,kind=$7,payload=$8::jsonb,pending=false,
          before_privacy=$9::jsonb,privacy_version=$10::bigint,updated_at=clock_timestamp()
        WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3 AND epoch=$4 AND version=$11::bigint AND digest=$12 AND NOT pending RETURNING 1 AS x`,
        [n.org_id, n.workspace_id, n.user_id, n.epoch, n.version, n.digest, n.kind, n.payload, n.before_privacy, n.privacy_version, r.mirror[0], r.mirror[1]]);
      if (u.rows.length !== 1) refuse("repair_postcondition");
      repaired.push(n);
    }
    // Re-verify inside the transaction: every seeded/repaired row now equals its planned Neon source byte for byte.
    const after = await readMirror(supa, opts);
    const ah = byKey(after.heads, headKey), ad = byKey(after.deletions, delKey), ar = byKey(after.resets, resetKey);
    for (const h of [...plan.heads, ...repaired]) { const m = ah.get(headKey(h)); if (!m || headFull(m) !== headFull(h)) refuse("seed_verify_heads"); }
    for (const d of plan.deletions) if (!ad.has(delKey(d))) refuse("seed_verify_deletions");
    for (const r of plan.resets) if (ar.get(resetKey(r))?.deleted !== r.deleted) refuse("seed_verify_resets");
    // And Neon still is the planned state (heads AND privacy) at the end: no control committed during the apply.
    if (sourceFingerprint(await readSource(neon, opts)) !== planned) refuse("neon_changed_during_apply");
    await supa.query("COMMIT");
    repairedCount = repaired.length;
  } catch (e) { await supa.query("ROLLBACK").catch(() => {}); throw e; }
  const seeded = { deletions: plan.deletions.length, resets: plan.resets.length, heads: plan.heads.length };
  hooks.onCommitted?.({ seeded, repaired: repairedCount });
  return { seeded, repairs: { repaired: repairedCount, already: 0, refused: {} }, after: await census(neon, supa, opts) };
}
