/**
 * Flags for SERVICE=coordination-advisor (docs/coordination-live-advisories.md, "Flags and gates").
 * Every behaviour is default off. The on/off flags follow the repo's exact-"true" convention
 * (config.ts exactTrueFlag, READ_API_COORDINATION_OVERLAPS_ENABLED): only the exact string "true"
 * enables; "1", "on", "TRUE" and every other value leave it off. Values are never echoed.
 */
import { exactTrueFlag, parseDirectoryWorkspaces, type FactsBackendKind } from "../session-read-api/config.ts";

export const COORDINATION_ADVISOR_ENABLED = "COORDINATION_ADVISOR_ENABLED";
export const COORDINATION_ADVISOR_INTERVAL_SECONDS = "COORDINATION_ADVISOR_INTERVAL_SECONDS";
export const COORDINATION_ADVISOR_PAIR_COOLDOWN_SECONDS = "COORDINATION_ADVISOR_PAIR_COOLDOWN_SECONDS";
export const COORDINATION_ADVISOR_SESSION_HOURLY_CAP = "COORDINATION_ADVISOR_SESSION_HOURLY_CAP";
export const COORDINATION_ADVISOR_RELAY_DM_ENABLED = "COORDINATION_ADVISOR_RELAY_DM_ENABLED";
export const COORDINATION_ADVISOR_RELAY_TOKEN = "COORDINATION_ADVISOR_RELAY_TOKEN";
export const COORDINATION_ADVISOR_RELAY_URL = "COORDINATION_ADVISOR_RELAY_URL";
export const COORDINATION_ADVISOR_FACTS_BACKEND = "COORDINATION_ADVISOR_FACTS_BACKEND";
/**
 * The advisory instance (20261105062220, docs/coordination-live-advisories.md "Advisory instances"). Exactly "true"
 * only, default off. On: the worker writes through coordination_advisories_upsert_v2 (each row gets an incarnation,
 * rotated on a LOW -> HIGH promotion). Off: upsert_v1 exactly as before (incarnation NULL, a legacy row). Instance mode
 * has no DM: on together with COORDINATION_ADVISOR_RELAY_DM_ENABLED the worker refuses to start, because
 * mark_delivered_v1 is fenced to legacy rows and a generation-aware mark_delivered is a later slice. Turning it on is a
 * coordinated step after ack-v2 Desktops ship (Khaliq's call).
 */
export const COORDINATION_ADVISOR_INCARNATION_ENABLED = "COORDINATION_ADVISOR_INCARNATION_ENABLED";
/**
 * M2-4: the conflict rows (constraint_violation, intent_conflict, overlap from the claim detectors). Exactly
 * "true" only, default off; the M3-1 `conflicts` control must also be on for the workspace. Off: the scan reads no
 * claims and runs no conflict code.
 */
export const SESSION_INTEL_CONFLICTS_ENABLED = "SESSION_INTEL_CONFLICTS_ENABLED";
/**
 * The read API's per-workspace canary (requireCanary "coordination"), read by the worker too: "*" or a
 * comma-separated list of workspace ids. Absent or empty: the worker scans, writes and DMs no workspace.
 */
export const READ_API_DIRECTORY_WORKSPACES = "READ_API_DIRECTORY_WORKSPACES";

export const DEFAULT_INTERVAL_SECONDS = 120;
export const DEFAULT_PAIR_COOLDOWN_SECONDS = 21_600;
export const DEFAULT_SESSION_HOURLY_CAP = 3;

export interface AdvisorConfig {
  intervalMs: number;
  pairCooldownMs: number;
  sessionHourlyCap: number;
  factsBackend: FactsBackendKind;
  /** The workspaces the worker may scan: the routes' canary allowlist. */
  workspaces: ReadonlySet<string> | "*";
  relayDm: { enabled: false } | { enabled: true; token: string; url: string };
  /** SESSION_INTEL_CONFLICTS_ENABLED is exactly "true". */
  conflicts: boolean;
  /** COORDINATION_ADVISOR_INCARNATION_ENABLED is exactly "true": write through upsert_v2. Never with relayDm on. */
  incarnation: boolean;
}

/** Only the exact string "true" turns the worker's scan and writes on. */
export const advisorEnabled = (env: Record<string, unknown>) => exactTrueFlag(env[COORDINATION_ADVISOR_ENABLED]);

function positiveInteger(env: Record<string, unknown>, name: string, fallback: number, min: number): number {
  const raw = env[name];
  if (raw === undefined || raw === "") return fallback;
  const n = typeof raw === "string" && /^[0-9]+$/.test(raw.trim()) ? Number(raw.trim()) : Number.NaN;
  if (!Number.isSafeInteger(n) || n < min) throw new Error(`coordination advisor: ${name} must be an integer >= ${min}`);
  return n;
}

const loopback = (host: string) => host === "localhost" || host === "127.0.0.1" || host === "[::1]";

/** The DM endpoint: https (http only on loopback), no credentials in it. Errors never echo the value. */
function relayUrl(raw: string): string {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new Error(`coordination advisor: ${COORDINATION_ADVISOR_RELAY_URL} is not a valid URL`);
  }
  if (url.protocol !== "https:" && !(url.protocol === "http:" && loopback(url.hostname)))
    throw new Error(`coordination advisor: ${COORDINATION_ADVISOR_RELAY_URL} must be https (http only on loopback)`);
  if (url.username || url.password) throw new Error(`coordination advisor: ${COORDINATION_ADVISOR_RELAY_URL} must not carry credentials`);
  // A token in the query (?token=) would sit in the deploy values and the request log; the token has its own variable.
  if (url.search || url.hash) throw new Error(`coordination advisor: ${COORDINATION_ADVISOR_RELAY_URL} must not carry a query or fragment`);
  return raw;
}

/**
 * Parsed only when the worker is enabled. A malformed value, an unknown facts backend
 * selection, the DM flag without its credential, or the DM flag with the instance flag refuses to start rather
 * than run degraded.
 */
export function advisorConfig(env: Record<string, unknown>): AdvisorConfig {
  const backendRaw = env[COORDINATION_ADVISOR_FACTS_BACKEND];
  let factsBackend: FactsBackendKind;
  if (backendRaw === undefined || backendRaw === "" || backendRaw === "clickhouse") factsBackend = "clickhouse";
  else if (backendRaw === "postgres") factsBackend = "postgres";
  else throw new Error(`coordination advisor: ${COORDINATION_ADVISOR_FACTS_BACKEND} must be unset, "", "clickhouse" or "postgres"`);
  const dmOn = exactTrueFlag(env[COORDINATION_ADVISOR_RELAY_DM_ENABLED]);
  const incarnation = exactTrueFlag(env[COORDINATION_ADVISOR_INCARNATION_ENABLED]);
  // DMs fail closed in instance mode: mark_delivered_v1 never marks a protected row, so a DM could repeat.
  if (incarnation && dmOn)
    throw new Error(`coordination advisor: ${COORDINATION_ADVISOR_INCARNATION_ENABLED} and ${COORDINATION_ADVISOR_RELAY_DM_ENABLED} must not both be on (instance mode has no DM)`);
  let relayDm: AdvisorConfig["relayDm"] = { enabled: false };
  if (dmOn) {
    const token = env[COORDINATION_ADVISOR_RELAY_TOKEN];
    const url = env[COORDINATION_ADVISOR_RELAY_URL];
    if (typeof token !== "string" || !token.trim())
      throw new Error(`coordination advisor: ${COORDINATION_ADVISOR_RELAY_DM_ENABLED} is on but ${COORDINATION_ADVISOR_RELAY_TOKEN} is missing`);
    if (typeof url !== "string" || !url.trim())
      throw new Error(`coordination advisor: ${COORDINATION_ADVISOR_RELAY_DM_ENABLED} is on but ${COORDINATION_ADVISOR_RELAY_URL} is missing`);
    relayDm = { enabled: true, token: token.trim(), url: relayUrl(url.trim()) };
  }
  return {
    intervalMs: positiveInteger(env, COORDINATION_ADVISOR_INTERVAL_SECONDS, DEFAULT_INTERVAL_SECONDS, 1) * 1_000,
    pairCooldownMs: positiveInteger(env, COORDINATION_ADVISOR_PAIR_COOLDOWN_SECONDS, DEFAULT_PAIR_COOLDOWN_SECONDS, 0) * 1_000,
    sessionHourlyCap: positiveInteger(env, COORDINATION_ADVISOR_SESSION_HOURLY_CAP, DEFAULT_SESSION_HOURLY_CAP, 0),
    factsBackend,
    workspaces: parseDirectoryWorkspaces(typeof env[READ_API_DIRECTORY_WORKSPACES] === "string" ? env[READ_API_DIRECTORY_WORKSPACES] as string : undefined),
    relayDm,
    conflicts: exactTrueFlag(env[SESSION_INTEL_CONFLICTS_ENABLED]),
    incarnation,
  };
}
