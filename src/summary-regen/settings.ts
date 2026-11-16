/**
 * Summary regeneration flags (docs: summaries-regenerate scope section 3). Each flag is on only
 * when its value is exactly "true"; anything else (unset, "TRUE", "1", " true") is off, so an
 * unset environment keeps today's behaviour byte for byte. Tuning values out of bounds fall back
 * to their defaults (the AWS stack validator refuses them before they reach a pod).
 */
export const REGEN_REASONS = [
  "turns_gained",
  "idle_uncovered",
  "ended_uncovered",
  "retry_backoff",
  "config_changed",
  "operator_requeue",
  "version_backfill",
] as const;
export type RegenReason = (typeof REGEN_REASONS)[number];

/** Bounds shared with the stack validator (stacks/aws/session-pipeline.ts summaryRegen). */
export const REGEN_BOUNDS = {
  threshold: { min: 1, max: 10_000, fallback: 10 },
  idleSeconds: { min: 60, max: 604_800, fallback: 600 },
  backoffSeconds: { min: 1, max: 604_800, maxEntries: 10, fallback: [300, 3_600, 21_600] as readonly number[] },
  retentionDays: { min: 1, max: 3_650, fallback: 90 },
} as const;

export interface RegenSettings {
  turns: { enabled: boolean; threshold: number; idleSeconds: number };
  retry: { enabled: boolean; backoffSeconds: readonly number[]; configReset: boolean };
  audit: { enabled: boolean; retentionDays: number };
}

const on = (env: NodeJS.ProcessEnv, name: string) => env[name] === "true";

function bounded(env: NodeJS.ProcessEnv, name: string, b: { min: number; max: number; fallback: number }): number {
  const raw = env[name];
  if (raw === undefined || !/^\d{1,9}$/.test(raw)) return b.fallback;
  const n = Number(raw);
  return n >= b.min && n <= b.max ? n : b.fallback;
}

function backoff(env: NodeJS.ProcessEnv): readonly number[] {
  const b = REGEN_BOUNDS.backoffSeconds;
  const raw = env.SUMMARY_RETRY_BACKOFF_SECONDS;
  if (raw === undefined) return b.fallback;
  const parts = raw.split(",");
  if (parts.length < 1 || parts.length > b.maxEntries || !parts.every((p) => /^\d{1,9}$/.test(p))) return b.fallback;
  const values = parts.map(Number);
  return values.every((v) => v >= b.min && v <= b.max) ? values : b.fallback;
}

export function regenSettings(env: NodeJS.ProcessEnv = process.env): RegenSettings {
  return {
    turns: {
      enabled: on(env, "SUMMARY_REGEN_TURNS_ENABLED"),
      threshold: bounded(env, "SUMMARY_REGEN_TURN_THRESHOLD", REGEN_BOUNDS.threshold),
      idleSeconds: bounded(env, "SUMMARY_REGEN_IDLE_SECONDS", REGEN_BOUNDS.idleSeconds),
    },
    retry: {
      enabled: on(env, "SUMMARY_RETRY_BACKOFF_ENABLED"),
      backoffSeconds: backoff(env),
      configReset: on(env, "SUMMARY_RETRY_CONFIG_RESET_ENABLED"),
    },
    audit: {
      enabled: on(env, "SUMMARY_REGEN_AUDIT_ENABLED"),
      retentionDays: bounded(env, "SUMMARY_REGEN_AUDIT_RETENTION_DAYS", REGEN_BOUNDS.retentionDays),
    },
  };
}

/** Any behaviour that reads or writes the new coverage columns ((1), (2), (2b)). Audit alone changes nothing. */
export function regenActive(s: RegenSettings | undefined): boolean {
  return !!s && (s.turns.enabled || s.retry.enabled || s.retry.configReset);
}

/** The settings with every flag off: what a worker uses when the schema is not there yet. */
export function regenOff(s: RegenSettings): RegenSettings {
  return {
    turns: { ...s.turns, enabled: false },
    retry: { ...s.retry, enabled: false, configReset: false },
    audit: { ...s.audit, enabled: false },
  };
}
