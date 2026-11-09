/**
 * The Neon-exit release profile (SESSIONS_NEON_EXIT, default off).
 *
 * There is no Neon SDK here: every Postgres client is plain `pg` given a URL,
 * so a consumer is a "Neon edge" only by what it is configured to reach. With
 * the profile on, each such edge in this repo refuses at startup instead of
 * running (docs/neon-exit-edge-map.md). It is not a global zero-Neon
 * guarantee: preserved migration tooling and edges outside this repo are out
 * of scope.
 *
 * Pure: takes an env record (a Worker's env object or a Lambda's process.env),
 * never reads process.env itself, and never echoes a setting's value.
 */

export const NEON_EXIT_ENV = "SESSIONS_NEON_EXIT";

export interface NeonExitProfile {
  readonly enabled: boolean;
}

const OFF: NeonExitProfile = Object.freeze({ enabled: false });
const ON: NeonExitProfile = Object.freeze({ enabled: true });

/** Strict: unset or "false" is off, "true" is on, anything else throws at startup. */
export function neonExitProfile(env: Record<string, unknown>): NeonExitProfile {
  const raw = env[NEON_EXIT_ENV];
  if (raw === undefined || raw === "false") return OFF;
  if (raw === "true") return ON;
  throw new Error(`96bulls: ${NEON_EXIT_ENV} must be unset, "true" or "false"`);
}

export class NeonExitRefusal extends Error {
  readonly code = "neon_exit_refused";
  constructor(readonly consumer: string, readonly setting: string) {
    super(`96bulls: ${NEON_EXIT_ENV}=true refuses ${consumer}: ${setting}`);
    this.name = "NeonExitRefusal";
  }
}

/**
 * Refuse the first offending setting when the profile is on. `facts` maps a
 * setting description (e.g. "INGEST_REPLICA_MODE is not off") to whether that
 * Neon edge is configured. Descriptions name settings only, never their raw
 * values. No-op when the profile is off.
 */
export function assertNoNeonEdge(profile: NeonExitProfile, consumer: string, facts: Record<string, boolean>): void {
  if (!profile.enabled) return;
  for (const [setting, present] of Object.entries(facts)) if (present) throw new NeonExitRefusal(consumer, setting);
}
