/**
 * T144 (under T130): the session-brief trigger, producer contract v1
 * (sha256 f01914a6cdc3db01353cb3a67f3e2cf1631b78da6a019d4582a532c6e7437d37).
 *
 * After the authoritative manifest CAS for a session revision, the producer emits a WAKE HINT:
 *
 *   {v:1, kind: "append"|"end"|"sweep", org, workspace, session, revision, digest, first_uncovered_at, trigger_id}
 *
 * A hint is not publication authority: it only says "look at this session again". The brief worker always reads
 * the authoritative pointers itself; a hint whose revision is older than what is already briefed changes nothing
 * but the debounce state, and a hint can never make a brief cover a revision the store does not hold.
 *
 * Delivery: hosted, one standard queue (SESSION_BRIEF_QUEUE_URL, with a DLQ and an event source mapping) plus a
 * recovery tick that enqueues `sweep` hints; self-host, the existing sessions.session_brief_wakeups table, polled
 * and leased by the native worker (no queue). Both apply the SAME debounce plan (planWakeup) to one durable state
 * per session, so the rules below hold on either path.
 *
 * Debounce (all from SESSION_BRIEF_*; everything off unless SESSION_BRIEF_ENABLED=true):
 *   - first brief (no ready brief yet): due at most SESSION_BRIEF_MAX_DELAY_SECONDS (8) after the session's first
 *     uncovered evidence, and it bypasses the throttle;
 *   - refresh: due SESSION_BRIEF_IDLE_SECONDS (600) after the newest evidence, never later than
 *     SESSION_BRIEF_MAX_WAIT_SECONDS (3600) after the oldest pending evidence, and the throttle applies at claim;
 *   - end: a refresh whose idle condition is met (due now; the throttle still applies);
 *   - sweep: recovers a lost trigger; it never delays a due wakeup, never resets pending_since and never creates
 *     extra work for a session that already has a wakeup;
 *   - the OLDEST pending_since wins across duplicates and reordering; evidence_at never moves backwards;
 *   - new evidence while a run is in flight bumps the generation, so the run's finish leaves the trailing work
 *     due instead of deleting it.
 *
 * Retry identity: trigger_id is derived from the physical incarnation the producer committed (the physical
 * source, physical session, machine and pointer generation), the revision, the digest and the kind, so a retried
 * producer (an ambiguous COMMIT, a redelivery) emits the same id and a different incarnation never collides.
 *
 * Nothing here logs; ids travel only inside hints and SQL parameters.
 */
import { createHash } from "node:crypto";

export const BRIEF_TRIGGER_CONTRACT = "f01914a6cdc3db01353cb3a67f3e2cf1631b78da6a019d4582a532c6e7437d37";

export type BriefWakeKind = "append" | "end" | "sweep";
export const BRIEF_WAKE_KINDS: readonly BriefWakeKind[] = ["append", "end", "sweep"];

export interface BriefWakeHint {
  v: 1;
  kind: BriefWakeKind;
  org: string;
  workspace: string;
  session: string;
  /** The authoritative pointer revision the producer committed (a sweep: the highest it saw). */
  revision: number;
  /** sha256 (hex) of the committed manifest. */
  digest: string;
  /** Epoch ms of the oldest evidence this hint knows is not yet covered by a brief. */
  first_uncovered_at: number;
  trigger_id: string;
}

const ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const HINT_KEYS = ["v", "kind", "org", "workspace", "session", "revision", "digest", "first_uncovered_at", "trigger_id"] as const;
const isObj = (v: unknown): v is Record<string, unknown> => v !== null && typeof v === "object" && !Array.isArray(v);
/** Bounded epoch ms: after 2020-01-01 and before 2100-01-01 (a seconds value or a garbage number is refused). */
const epochMs = (v: unknown): v is number => Number.isSafeInteger(v) && (v as number) >= 1_577_836_800_000 && (v as number) < 4_102_444_800_000;

/** A hint exactly as contract v1 defines it, or null (unknown keys, a bad kind, id, revision, digest or time). */
export function parseBriefWakeHint(v: unknown): BriefWakeHint | null {
  if (!isObj(v)) return null;
  const keys = Object.keys(v);
  if (keys.length !== HINT_KEYS.length || !HINT_KEYS.every((k) => keys.includes(k))) return null;
  if (v.v !== 1 || !BRIEF_WAKE_KINDS.includes(v.kind as BriefWakeKind)) return null;
  for (const k of ["org", "workspace", "session"] as const) if (typeof v[k] !== "string" || !ID.test(v[k] as string)) return null;
  if (!Number.isSafeInteger(v.revision) || (v.revision as number) < 0) return null;
  if (typeof v.digest !== "string" || !HEX64.test(v.digest)) return null;
  if (!epochMs(v.first_uncovered_at)) return null;
  if (typeof v.trigger_id !== "string" || !HEX64.test(v.trigger_id)) return null;
  return v as unknown as BriefWakeHint;
}

/** The physical incarnation a producer committed: what a retry of the same commit has in common, and no other. */
export interface BriefPhysicalIncarnation {
  /** The physical source (harness) and session the commit was for. */
  source: string;
  session: string;
  /** The writer machine. */
  machine: string;
  /** The publication head generation (or the pointer row's generation): a deleted and recreated head differs. */
  generation: string;
}

/** The stable retry identity of one hint: equal for a retry of the same commit, different for anything else. */
export function briefTriggerId(kind: BriefWakeKind, h: Pick<BriefWakeHint, "org" | "workspace" | "session" | "revision" | "digest">,
  incarnation: BriefPhysicalIncarnation): string {
  return createHash("sha256").update(JSON.stringify([
    "session-brief-trigger", 1, kind, h.org, h.workspace, h.session, h.revision, h.digest,
    incarnation.source, incarnation.session, incarnation.machine, incarnation.generation,
  ])).digest("hex");
}

/** Build a hint (validated: it throws only for a programming error, never for input it was given by a peer). */
export function briefWakeHint(kind: BriefWakeKind, fields: Omit<BriefWakeHint, "v" | "kind" | "trigger_id">,
  incarnation: BriefPhysicalIncarnation): BriefWakeHint {
  const hint = { v: 1 as const, kind, ...fields, trigger_id: briefTriggerId(kind, fields, incarnation) };
  const ok = parseBriefWakeHint(hint);
  if (!ok) throw new Error("session-brief trigger: invalid hint fields");
  return ok;
}

// ---- the debounce plan (pure; the SQL and queue paths apply exactly this) ----

export interface BriefTriggerTiming {
  maxDelaySeconds: number;
  idleSeconds: number;
  maxWaitSeconds: number;
}

/** One session's durable wakeup state, in epoch ms. */
export interface BriefWakeState {
  pendingSince: number;
  evidenceAt: number;
  dueAt: number;
  /** Bumped by new evidence: a run claimed at an older generation leaves the wakeup when it finishes. */
  generation: number;
  /** Set while a worker holds the wakeup (the run in flight). */
  leased: boolean;
  /** "sweep" only while every hint so far was a sweep (a sweep never downgrades a real trigger). */
  admission: "background" | "sweep";
  /** While leased: the oldest first_uncovered_at armed during the run (the trailing work's own window start). */
  trailingSince?: number;
  /** The newest hint applied: an exact duplicate (a producer retry, a redelivery) is a no-op. */
  lastTriggerId?: string;
  /** The highest revision any applied hint named (what a continuation re-presents). */
  revision: number;
}

export interface BriefWakeContext {
  nowMs: number;
  /** A ready brief already exists for the session (so this is a refresh, not the first brief). */
  hasBrief: boolean;
  /**
   * The highest revision the stored brief already covers (sessions.session_briefs.brief_revision), when known.
   * A hint at or below it is stale (a late duplicate, a redelivery after the work finished): it arms nothing, so a
   * finished wakeup is never recreated by an old message. Suppression only; never authority to skip new evidence.
   */
  coveredRevision?: number;
  timing: BriefTriggerTiming;
}

const secs = (s: number) => s * 1_000;

/**
 * The wakeup state after one hint. `prev` null: the session has no wakeup yet. Returns null when the hint changes
 * nothing worth storing (a sweep for a session that already has a wakeup and no older evidence).
 */
export function planWakeup(prev: BriefWakeState | null, hint: BriefWakeHint, ctx: BriefWakeContext): BriefWakeState | null {
  const { nowMs, hasBrief, timing } = ctx;
  if (prev?.lastTriggerId === hint.trigger_id) return null; // a duplicate: already applied
  if (ctx.coveredRevision !== undefined && hint.revision <= ctx.coveredRevision) return null; // already briefed
  const revision = Math.max(prev?.revision ?? 0, hint.revision);
  // A hint's own evidence time: the producer's first uncovered time, never in the future of this clock.
  const firstUncovered = Math.min(hint.first_uncovered_at, nowMs);
  const pendingSince = prev ? Math.min(prev.pendingSince, firstUncovered) : firstUncovered;
  if (hint.kind === "sweep") {
    // A lost-trigger sweep: create work only where there is none; otherwise keep the existing due time and
    // generation (a late, older sweep can neither delay nor duplicate), and only ever lower pending_since.
    if (prev) return pendingSince < prev.pendingSince ? { ...prev, pendingSince, revision, lastTriggerId: hint.trigger_id } : null;
    return { pendingSince, evidenceAt: firstUncovered, dueAt: nowMs, generation: 1, leased: false, admission: "sweep", revision, lastTriggerId: hint.trigger_id };
  }
  const evidenceAt = prev ? Math.max(prev.evidenceAt, nowMs) : nowMs;
  let due: number;
  if (hint.kind === "end") due = nowMs; // ended: the idle condition is met (the throttle still applies at claim)
  else if (!hasBrief) due = pendingSince + secs(timing.maxDelaySeconds); // first brief: fast, never pushed back
  else due = Math.min(evidenceAt + secs(timing.idleSeconds), pendingSince + secs(timing.maxWaitSeconds));
  // A first brief or an end never waits longer than an earlier due time already promised.
  if (prev && (!hasBrief || hint.kind === "end")) due = Math.min(due, prev.dueAt);
  const leased = prev?.leased ?? false;
  // Evidence that arrives while a run is in flight is the trailing work's own: it keeps its oldest time.
  const trailingSince = leased ? Math.min(prev?.trailingSince ?? Infinity, firstUncovered) : undefined;
  return {
    pendingSince,
    evidenceAt,
    dueAt: due,
    generation: (prev?.generation ?? 0) + 1,
    leased,
    admission: "background",
    ...(trailingSince !== undefined ? { trailingSince } : {}),
    lastTriggerId: hint.trigger_id,
    revision,
  };
}

/** A worker claims a due wakeup: it holds the lease for the generation it saw. */
export function claimPlan(state: BriefWakeState): BriefWakeState {
  const { trailingSince: _t, ...rest } = state;
  return { ...rest, leased: true };
}

/**
 * A run finished (or failed) for the generation it claimed. Same generation: the wakeup is done (null). A newer
 * generation armed while it ran: the trailing work stays, due by the refresh rule from the evidence that arrived
 * meanwhile (`trailingSince`, the oldest first_uncovered_at armed during the run), never from the finish time.
 */
export function finishPlan(state: BriefWakeState, claimedGeneration: number, ctx: Omit<BriefWakeContext, "hasBrief">): BriefWakeState | null {
  if (state.generation === claimedGeneration) return null;
  const { trailingSince, ...rest } = state;
  const pendingSince = Math.min(trailingSince ?? ctx.nowMs, ctx.nowMs);
  const dueAt = Math.min(state.evidenceAt + secs(ctx.timing.idleSeconds), pendingSince + secs(ctx.timing.maxWaitSeconds));
  return { ...rest, leased: false, pendingSince, dueAt };
}

/** SQS standard queues delay a message by at most 15 minutes. */
export const SQS_MAX_DELAY_SECONDS = 900;

/**
 * Hosted consumer: what to do with a hint whose state is now `state`. Due (or past due): run. Not yet due: hand
 * it off durably to the due time (re-enqueue with a delay, chained in 15-minute hops) and only THEN acknowledge
 * the original; a failed handoff keeps the original (the consumer reports a batch item failure and the queue
 * redelivers it). Never acknowledges work it has neither run nor handed off.
 */
export type BriefHandoff = { action: "run" } | { action: "defer"; delaySeconds: number };
export function handoffPlan(state: BriefWakeState, nowMs: number): BriefHandoff {
  const wait = state.dueAt - nowMs;
  if (wait <= 0) return { action: "run" };
  return { action: "defer", delaySeconds: Math.min(SQS_MAX_DELAY_SECONDS, Math.ceil(wait / 1_000)) };
}

// ---- settings ----

export interface BriefTriggerConfig {
  enabled: boolean;
  timing: BriefTriggerTiming;
  /** How often the recovery tick enqueues sweep hints for sessions with uncovered revisions. */
  sweepSeconds: number;
  /** Hosted only: the standard queue the producer and the recovery tick send hints to. */
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
 * The trigger's settings: SESSION_BRIEF_ENABLED ("true", default off), SESSION_BRIEF_MAX_DELAY_SECONDS (8),
 * SESSION_BRIEF_IDLE_SECONDS (600), SESSION_BRIEF_MAX_WAIT_SECONDS (3600), SESSION_BRIEF_TRIGGER_SWEEP_SECONDS
 * (300; the trigger path's own, so the existing SESSION_BRIEF_SWEEP_SECONDS default of 0 is unchanged for
 * deployments that already run briefs) and SESSION_BRIEF_QUEUE_URL (hosted; an SQS queue URL, refused otherwise).
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
    sweepSeconds: int(env, "SESSION_BRIEF_TRIGGER_SWEEP_SECONDS", 300, 0, 86_400),
    ...(queueUrl ? { queueUrl } : {}),
  };
}
