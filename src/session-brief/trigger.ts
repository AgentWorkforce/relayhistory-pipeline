/**
 * T144 (under T130): the session-brief trigger, producer contract v1
 * (sha256 f01914a6cdc3db01353cb3a67f3e2cf1631b78da6a019d4582a532c6e7437d37), with the v2 scope extension proposed
 * by the T130 contract owner (adds `harness`; nothing else changes). Proposals pending acceptance are marked.
 *
 * Two different things, kept apart:
 *
 *   EVIDENCE: a committed manifest revision. Only committed evidence arms the durable wakeup
 *     (sessions.session_brief_wakeups, keyed (org, workspace, harness, session); store.ts armWakeup): it bumps the
 *     generation and keeps pending_since at the oldest uncovered evidence. planArm is that rule plus the
 *     first-brief bound.
 *
 *   A WAKE HINT: {v, kind: "append"|"end"|"sweep", org, workspace, [harness,] session, revision, digest,
 *     first_uncovered_at, trigger_id}. Not evidence and not publication authority: it can only bring an existing
 *     wakeup's due time EARLIER, to what the rules already allow (planHint). It never sets a due time later, never
 *     resets pending_since, never bumps the generation and never creates a wakeup; a hint whose revision the stored
 *     brief covers is a no-op. So duplicates, A/B/A, any reordering and redelivery after a lease or a retry converge
 *     on one row state (LEAST and the coverage check commute). trigger_id is for tracing, never for dedupe.
 *
 * Status of each rule: [Pipeline] = decided by Pipeline (contract owner of record; receipt 252419350106017792);
 * [owner] = proposed by the T130 owner (sessions-latency-owner); [builder] = proposed by this slice. Still open with
 * Pipeline: the producer hook point and the backoff floor.
 *
 * Due and claim eligibility (N = SESSION_BRIEF_MAX_DELAY_SECONDS 8, I = IDLE 600, W = MAX_WAIT 3600, T = THROTTLE):
 *   first brief (no ready brief): due = pending_since + N; an `end` makes it due now if earlier; never throttled. [owner]
 *   refresh, evidence or `append`: due = LEAST(evidence_at + I, pending_since + W). [existing armWakeup]
 *   refresh, `end`: due = now: a confirmed end removes the coalescing wait. [Pipeline]
 *   claim (refresh): now >= due AND now >= last_attempt + T: the refresh throttle applies (beats W). [Pipeline]
 *   v1 (no harness) [Pipeline]: wakes every candidate row by full key, each with its OWN debounce, throttle and
 *     retry floor; nothing (identity, time) is copied across harnesses, and an ambiguous v1 `end` marks no harness
 *     ended (it wakes like an append).
 *   a row in retry backoff or park is never lowered by a hint. [builder]
 *   coverage [Pipeline item 8]: a hint advances a row only when its trigger_id (the opaque commit reference)
 *     resolves to work armed and not consumed on that row; otherwise it is wake-only. Never decided from the max
 *     brief_revision. Caller credit still needs consumed membership (the Reader's, not this slice's).
 *   SCORING [Pipeline review]: a brief is a miss when the first authorized parsed brief proving consumption comes more
 *     than 60 s after the cohort's first upload HTTP attempt (briefMiss). Due-to-claim is diagnostic only.
 *
 * Delivery: hosted, a standard queue (SESSION_BRIEF_QUEUE_URL, DLQ, event source mapping); a consumer handed a hint
 * before its claim time hands it off durably (re-enqueue delayed, at most 900 s a hop) and acknowledges only after a
 * run or a successful handoff. The hosted one-minute tick checks due work every minute; SESSION_BRIEF_SWEEP_SECONDS
 * (hosted sets 300) gates only its lost-hint scan. [Pipeline] At dcbd14bb the scan cadence is PER INSTANCE (the
 * worker's lastSweep lives in its closure): concurrent or cold Lambda instances each keep their own, so the 300 s is
 * a per-instance minimum, not a shared schedule. A shared cadence would need an atomic shared sweep claim (e.g. a
 * CAS'd last-discovery row) and cold/concurrent tick tests; not built here. With EventBridge disabled there are no
 * ticks at all, so only hints drive claims. Self-host: the native worker polls the table; no queue; the existing SESSION_BRIEF_SWEEP_SECONDS default (0,
 * off) is unchanged for a direct worker (the native chart renders its own value).
 *
 * Nothing here logs; ids travel only inside hints and SQL parameters.
 */
import { createHash } from "node:crypto";

export const BRIEF_TRIGGER_CONTRACT = "f01914a6cdc3db01353cb3a67f3e2cf1631b78da6a019d4582a532c6e7437d37";

export type BriefWakeKind = "append" | "end" | "sweep";
export const BRIEF_WAKE_KINDS: readonly BriefWakeKind[] = ["append", "end", "sweep"];

interface HintBase {
  kind: BriefWakeKind;
  org: string;
  workspace: string;
  session: string;
  /** The authoritative pointer revision the producer committed (a sweep: the highest it saw). */
  revision: number;
  /**
   * sha256 (hex) of the committed object: session_index.content_sha256, the raw chain hash (packages/object-store
   * manifest.ts raw.content_sha256), which both hook paths hold; the manifest body hash exists only on publish.
   */
  digest: string;
  /** Epoch ms of the oldest committed evidence the producer knows is not yet covered by a brief. */
  first_uncovered_at: number;
  trigger_id: string;
}
/** Contract v1: no harness (the consumer wakes every harness's wakeup for the session). */
export interface BriefWakeHintV1 extends HintBase { v: 1 }
/** v2 [Pipeline]: the exact durable scope. Rollout: deploy a consumer that accepts v1 and v2 BEFORE emitting v2. */
export interface BriefWakeHintV2 extends HintBase { v: 2; harness: string }
export type BriefWakeHint = BriefWakeHintV1 | BriefWakeHintV2;

const ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/;
/** The sessions.session_brief_wakeups.harness CHECK. */
const HARNESS = /^[a-z0-9][a-z0-9._-]{0,63}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const V1_KEYS = ["v", "kind", "org", "workspace", "session", "revision", "digest", "first_uncovered_at", "trigger_id"] as const;
const V2_KEYS = [...V1_KEYS, "harness"] as const;
const isObj = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
/** Bounded epoch ms: after 2020-01-01 and before 2100-01-01 (a seconds value or a garbage number is refused). */
const epochMs = (v: unknown): v is number => Number.isSafeInteger(v) && (v as number) >= 1_577_836_800_000 && (v as number) < 4_102_444_800_000;
const exactKeys = (o: Record<string, unknown>, keys: readonly string[]) => {
  const k = Object.keys(o);
  return k.length === keys.length && keys.every((x) => k.includes(x));
};

/** A v1 or v2 hint exactly as defined, or null (unknown keys or version, a bad kind, id, revision, digest or time). */
export function parseBriefWakeHint(v: unknown): BriefWakeHint | null {
  if (!isObj(v)) return null;
  if (v.v === 1 ? !exactKeys(v, V1_KEYS) : v.v === 2 ? !exactKeys(v, V2_KEYS) : true) return null;
  if (v.v === 2 && (typeof v.harness !== "string" || !HARNESS.test(v.harness))) return null;
  if (!BRIEF_WAKE_KINDS.includes(v.kind as BriefWakeKind)) return null;
  for (const k of ["org", "workspace", "session"] as const) if (typeof v[k] !== "string" || !ID.test(v[k] as string)) return null;
  if (!Number.isSafeInteger(v.revision) || (v.revision as number) < 0) return null;
  if (typeof v.digest !== "string" || !HEX64.test(v.digest)) return null;
  if (!epochMs(v.first_uncovered_at)) return null;
  if (typeof v.trigger_id !== "string" || !HEX64.test(v.trigger_id)) return null;
  return v as unknown as BriefWakeHint;
}

/**
 * The committed object a producer wrote, as BOTH hook paths see it at the session_index upsert (canonical publish
 * in SQL; the legacy indexer in TS): the physical row (source = harness, session, machine) and the manifest key,
 * which is unique per publication attempt and so tells a recreated row (after the rekey DELETE) from the one it
 * replaced. The publication head generation is NOT used: it is a random uuid with no order and is absent on legacy
 * keys.
 */
export interface BriefPhysicalIncarnation {
  source: string;
  session: string;
  machine: string;
  manifestKey: string;
}

/**
 * trigger_id [builder, per Pipeline item 8]: the OPAQUE REFERENCE to the durable scoped commit identity (scope,
 * machine, head generation, pointer revision, manifest sha256), independent of the hint kind, so an append and an
 * end for the same commit share it and the consumer can resolve it against the arm record
 * (sessions.session_brief_arms). Equal for a retry of the same commit; different for a replacement at the same
 * revision (another generation or sha) and for another machine. Also the tracing join key.
 */
export function briefTriggerId(h: { org: string; workspace: string; harness: string; session: string; revision: number; digest: string },
  incarnation: BriefPhysicalIncarnation): string {
  return createHash("sha256").update(JSON.stringify([
    "session-brief-commit", 3, h.org, h.workspace, h.harness, h.session, h.revision, h.digest,
    incarnation.source, incarnation.session, incarnation.machine, incarnation.manifestKey,
  ])).digest("hex");
}

/** Build a v2 hint (the producer always knows its harness). Throws only for a programming error. */
export function briefWakeHint(kind: BriefWakeKind, f: Omit<BriefWakeHintV2, "v" | "kind" | "trigger_id">,
  incarnation: BriefPhysicalIncarnation): BriefWakeHintV2 {
  const hint = { v: 2 as const, kind, ...f, trigger_id: briefTriggerId(f, incarnation) };
  const ok = parseBriefWakeHint(hint);
  if (!ok || ok.v !== 2) throw new Error("session-brief trigger: invalid hint fields");
  return ok;
}

/** Does a wakeup row belong to this hint's scope? v2: exactly its harness; v1: every harness of the session. */
export const hintCovers = (h: BriefWakeHint, row: { org: string; workspace: string; harness: string; session: string }) =>
  row.org === h.org && row.workspace === h.workspace && row.session === h.session && (h.v === 1 || row.harness === h.harness);

// ---- the durable wakeup and its rules (pure; the SQL and queue paths apply exactly these) ----

export interface BriefTriggerTiming { maxDelaySeconds: number; idleSeconds: number; maxWaitSeconds: number }

/** One wakeup row (sessions.session_brief_wakeups), times in epoch ms. */
export interface BriefWakeState {
  pendingSince: number;
  evidenceAt: number;
  dueAt: number;
  generation: number;
  leased: boolean;
  /**
   * The row is in a retry backoff or a park (a claimed attempt was rescheduled: store.ts rescheduleWakeup, so
   * attempts > 0 and the lease released). A hint never lowers such a row's due time, or a replay would undo the
   * backoff (a failing first brief would retry at once, in a loop). New committed evidence still re-arms it
   * (armWakeup resets attempts to 0).
   */
  backoff?: boolean;
  /** The outstanding lease (store.ts lease_id): set by a claim, still set after its wall-clock expiry until reclaim. */
  leaseId?: string;
  /**
   * [Pipeline 80c6350a1113] The FIRST committed evidence armed while a lease was outstanding (expired or not): the
   * trailing work's own oldest uncovered time (B, kept when C lands too). Cleared only by a new claim, which sets
   * its own generation boundary. Column sessions.session_brief_wakeups.trailing_since (slot 20261105062520).
   */
  trailingSince?: number;
}

const secs = (s: number) => s * 1_000;

/**
 * Committed evidence arms the wakeup (store.ts armWakeup, plus the first-brief bound). Insert: pending_since =
 * evidence_at = now. Existing: pending_since kept (the oldest uncovered, also while leased), evidence_at = now,
 * generation + 1, due = LEAST(now + I, pending_since + W) for a refresh, or pending_since + N for a first brief
 * (never later than a due already promised). The generation bump keeps trailing work behind a run in flight, and
 * new evidence ends a backoff (armWakeup resets attempts).
 */
export function planArm(prev: BriefWakeState | null, nowMs: number, hasBrief: boolean, t: BriefTriggerTiming): BriefWakeState {
  const pendingSince = prev?.pendingSince ?? nowMs;
  let dueAt = hasBrief
    ? Math.min(nowMs + secs(t.idleSeconds), pendingSince + secs(t.maxWaitSeconds))
    : pendingSince + secs(t.maxDelaySeconds);
  if (prev && !hasBrief) dueAt = Math.min(dueAt, prev.dueAt);
  // Evidence armed while a lease is outstanding (fenced by lease_id, not by its wall-clock expiry) is trailing work.
  const trailingSince = prev?.leaseId !== undefined ? (prev.trailingSince ?? nowMs) : prev?.trailingSince;
  return {
    pendingSince, evidenceAt: nowMs, dueAt, generation: (prev?.generation ?? 0) + 1, leased: prev?.leased ?? false,
    ...(prev?.leaseId !== undefined ? { leaseId: prev.leaseId } : {}),
    ...(trailingSince !== undefined ? { trailingSince } : {}),
  };
}

export interface BriefHintContext {
  nowMs: number;
  /** A ready brief exists (a refresh); false: the first brief. */
  hasBrief: boolean;
  /**
   * The commit references (trigger_ids) of the CURRENT armed, unconsumed identity of each machine on this row,
   * resolved from the durable arm record (sessions.session_brief_arms, one identity per machine: a replacement at the
   * same revision retires the identity it replaces). A hint's eligibility rules apply only when its reference is one
   * of them: then it provably concerns current uncovered work (a delayed end for a replaced incarnation does not). Otherwise (a consumed commit, an unknown reference, or any v1
   * hint, which cannot name its scope) the hint is WAKE-ONLY: the consumer re-evaluates the row (its own due time,
   * retry floor and the armed uncovered evidence) and nothing is suppressed or advanced on the hint's word. Never
   * the max brief_revision alone (Pipeline item 8).
   */
  armedRefs?: ReadonlySet<string>;
  timing: BriefTriggerTiming;
}

/**
 * A hint over an existing wakeup: due := LEAST(due, the earliest time the rules allow), or null when it changes
 * nothing (no wakeup, a covered revision, nothing earlier). Never later, never pending_since, never the generation.
 * A `sweep` asks for nothing earlier than the row's own due time: it only makes the consumer re-evaluate the row
 * (a lost hint), so it is null here and the consumer goes on to claimAt.
 */
export function planHint(prev: BriefWakeState | null, hint: BriefWakeHint, ctx: BriefHintContext): BriefWakeState | null {
  if (!prev) return null; // nothing armed: nothing to wake (committed evidence arms; the sweep recovers)
  if (prev.backoff) return null; // a retry backoff or park stands until it ends or new evidence re-arms
  if (hint.kind === "sweep") return null;
  // Only a v2 hint whose reference resolves to work armed on THIS row advances it; anything else is wake-only.
  if (hint.v !== 2 || !ctx.armedRefs?.has(hint.trigger_id)) return null;
  // Only a v2 end is a confirmed end of THIS harness's session; a v1 end is ambiguous and wakes like an append.
  const ended = hint.kind === "end" && hint.v === 2;
  const t = ctx.timing;
  const firstBound = prev.pendingSince + secs(t.maxDelaySeconds);
  const eligible = !ctx.hasBrief
    ? (ended ? Math.min(ctx.nowMs, firstBound) : firstBound)
    : ended
      ? ctx.nowMs
      : Math.min(prev.evidenceAt + secs(t.idleSeconds), prev.pendingSince + secs(t.maxWaitSeconds));
  return eligible < prev.dueAt ? { ...prev, dueAt: eligible } : null;
}

/** When a claim may run: the due time, and for a refresh not before the throttle releases (it beats max-wait). */
export function claimAt(state: BriefWakeState, opts: { hasBrief: boolean; lastAttemptMs?: number; throttleSeconds: number }): number {
  if (!opts.hasBrief || opts.lastAttemptMs === undefined) return state.dueAt;
  return Math.max(state.dueAt, opts.lastAttemptMs + secs(opts.throttleSeconds));
}

/**
 * A worker claims a due wakeup under a new lease (also a reclaim after an expired one). The claim is a new
 * generation boundary: everything armed so far is within it, so trailing_since clears; pending_since is kept (a
 * failed earlier run retired nothing, so the oldest uncovered time still stands).
 */
export function claimPlan(s: BriefWakeState, leaseId = "lease"): BriefWakeState {
  const { trailingSince: _t, backoff: _b, ...rest } = s;
  return { ...rest, leased: true, leaseId };
}

/** The claim a run holds: what its finish or failure is fenced by. */
export interface BriefClaim { generation: number; leaseId: string }

/**
 * The run finished for the claim it holds (store.ts finishWakeup), fenced by lease_id. A stale finish (the lease was
 * reclaimed) changes nothing: the state is returned as is, so it can never clear a successor's times. Same lease and
 * generation: done (null). Same lease, newer generation: the trailing work stays, its window starting at its own
 * first commit (pending_since := trailing_since); with no trailing_since (a row armed during a lease before this
 * column existed) the conservative pending_since is kept, never now() (which would silently postpone known work).
 */
export function finishPlan(state: BriefWakeState, claim: BriefClaim, t: BriefTriggerTiming): BriefWakeState | null {
  if (state.leaseId !== claim.leaseId) return state; // stale completion
  if (state.generation === claim.generation) return null;
  const { trailingSince, leaseId: _l, ...rest } = state;
  const pendingSince = trailingSince ?? state.pendingSince;
  return { ...rest, leased: false, pendingSince, dueAt: Math.min(state.evidenceAt + secs(t.idleSeconds), pendingSince + secs(t.maxWaitSeconds)) };
}

/**
 * The run failed (store.ts rescheduleWakeup), fenced by lease_id like a finish. A failure retires no evidence: the
 * lease is released, pending_since and trailing_since are kept, and the row is due after the backoff (in backoff,
 * so a hint cannot pull it forward; new committed evidence still re-arms it).
 */
export function failPlan(state: BriefWakeState, claim: BriefClaim, nowMs: number, backoffMs: number): BriefWakeState {
  if (state.leaseId !== claim.leaseId) return state; // stale
  const { leaseId: _l, ...rest } = state;
  return { ...rest, leased: false, dueAt: nowMs + backoffMs, backoff: true };
}

// ---- durable commit identity (design r2: sessions.session_brief_arms; the SQL mirrors this) ----

/**
 * One machine's committed object for a session, as both hook paths derive it: the session_index revision (the only
 * durable, shared order: fenced strictly greater per row, monotone across publication head regenerations and
 * tombstones), the manifest key (unique per publication attempt) and the raw content sha256, plus whether the
 * manifest marks the session ended (an end is a new manifest revision: manifests are immutable).
 */
export interface BriefCommitIdentity { revision: number; manifestKey: string; contentSha256: string; ended: boolean }

const sameObject = (a: BriefCommitIdentity, b: BriefCommitIdentity) =>
  a.revision === b.revision && a.manifestKey === b.manifestKey && a.contentSha256 === b.contentSha256;

/**
 * Does a committed object add work over what this machine's row has armed and consumed (sessions.session_brief_arms:
 * the highest armed and consumed identities; it survives completion, and is erased with the row, so a recreate after
 * the rekey DELETE starts fresh)? Ordered by revision:
 *   below max(armed, consumed): an older replay (A after B, even after completion): no-op;
 *   equal, the same object: a replay or the other hook path seeing the same commit: no-op, except that `ended` is
 *     monotone (an end recorded for that object stays; a pre-end replay cannot clear it);
 *   equal, a different object: not authoritative (the session_index fence admits one object per revision within a
 *     row's lifetime): no-op;
 *   above: new evidence: arm.
 * Returns the identity to record as armed, or null.
 */
export function armIdentity(armed: BriefCommitIdentity | null, consumed: BriefCommitIdentity | null, incoming: BriefCommitIdentity): BriefCommitIdentity | null {
  const floor = Math.max(armed?.revision ?? -1, consumed?.revision ?? -1);
  if (incoming.revision < floor) return null;
  if (incoming.revision > floor) return incoming;
  const at = armed && armed.revision === floor ? armed : consumed!;
  if (!sameObject(at, incoming)) return null; // a different object at a fenced revision
  if (incoming.ended && !at.ended && !(consumed && sameObject(consumed, incoming) && consumed.ended)) return { ...at, ended: true };
  return null;
}

/** SQS standard queues delay a message by at most 15 minutes. */
export const SQS_MAX_DELAY_SECONDS = 900;

/** Hosted consumer: run once claimable, else hand off durably to that time (chained 15-minute hops). */
export type BriefHandoff = { action: "run" } | { action: "defer"; delaySeconds: number };
export function handoffPlan(claimableAt: number, nowMs: number): BriefHandoff {
  const wait = claimableAt - nowMs;
  if (wait <= 0) return { action: "run" };
  return { action: "defer", delaySeconds: Math.min(SQS_MAX_DELAY_SECONDS, Math.ceil(wait / 1_000)) };
}

/**
 * The score (Pipeline review): a miss when the first authorized, parsed brief proving consumption of a cohort is
 * served more than 60 s after that cohort's first upload HTTP attempt. Never measured from due or claim time (those
 * are diagnostics): a claim held 120 s by the throttle after an upload at 0 is already a miss when served at once.
 */
export const BRIEF_MISS_MS = 60_000;
export const briefMiss = (firstUploadAttemptMs: number, firstServedMs: number) => firstServedMs - firstUploadAttemptMs > BRIEF_MISS_MS;

// ---- settings ----

export interface BriefTriggerConfig {
  enabled: boolean;
  timing: BriefTriggerTiming;
  /** The recovery cadence (sweep hints on hosted, the worker's sweep on self-host); 0 = off. */
  sweepSeconds: number;
  /** Hosted only: the standard queue hints go to. */
  queueUrl?: string;
}

function int(env: NodeJS.ProcessEnv, name: string, fallback: number, min: number, max: number): number {
  const raw = env[name]?.trim();
  if (!raw || !/^\d{1,9}$/.test(raw)) return fallback;
  const value = Number(raw);
  return value >= min && value <= max ? value : fallback;
}

const QUEUE_URL = /^https:\/\/sqs\.[a-z0-9-]+\.amazonaws\.com\/\d{12}\/[A-Za-z0-9_-]{1,80}$/;

/**
 * SESSION_BRIEF_ENABLED ("true"; default off), SESSION_BRIEF_MAX_DELAY_SECONDS (8), SESSION_BRIEF_IDLE_SECONDS (600),
 * SESSION_BRIEF_MAX_WAIT_SECONDS (3600), SESSION_BRIEF_SWEEP_SECONDS (the EXISTING knob and default: 0 = off; hosted
 * sets 300, the native self-host chart renders its own) and SESSION_BRIEF_QUEUE_URL (hosted; SQS only).
 */
export function briefTriggerConfig(env: NodeJS.ProcessEnv = process.env): BriefTriggerConfig {
  const queueUrl = env.SESSION_BRIEF_QUEUE_URL?.trim();
  if (queueUrl && !QUEUE_URL.test(queueUrl)) throw new Error("SESSION_BRIEF_QUEUE_URL must be an SQS queue URL");
  return {
    enabled: env.SESSION_BRIEF_ENABLED === "true",
    timing: {
      maxDelaySeconds: int(env, "SESSION_BRIEF_MAX_DELAY_SECONDS", 8, 0, 3_600),
      idleSeconds: int(env, "SESSION_BRIEF_IDLE_SECONDS", 600, 0, 86_400),
      maxWaitSeconds: int(env, "SESSION_BRIEF_MAX_WAIT_SECONDS", 3_600, 0, 7 * 86_400),
    },
    sweepSeconds: int(env, "SESSION_BRIEF_SWEEP_SECONDS", 0, 0, 86_400),
    ...(queueUrl ? { queueUrl } : {}),
  };
}
