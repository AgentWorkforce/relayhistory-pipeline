/**
 * The legacy machine-claim window, as deployment env. Default off: a stage that
 * names no window sets nothing, so upgrading can never open one. The ingest
 * enforces the same limit at read time.
 */
export const CLAIMLESS_WINDOW_MAX_MS = 72 * 60 * 60 * 1000;

export interface ClaimlessWindow {
  /** RFC 3339 UTC: when it was activated. */
  from: string;
  /** RFC 3339 UTC: the fixed expiry, at most 72 hours after `from`. */
  until: string;
}

export function claimlessWindowEnv(
  window: ClaimlessWindow | undefined,
): { name: string; value: string }[] {
  if (!window) return [];
  const from = Date.parse(window.from);
  const until = Date.parse(window.until);
  if (
    !Number.isFinite(from) ||
    !Number.isFinite(until) ||
    until <= from ||
    until - from > CLAIMLESS_WINDOW_MAX_MS
  )
    throw new Error(
      "96bulls: a claimless window needs from and until at most 72 hours apart",
    );
  return [
    { name: "MANIFEST_MACHINE_CLAIM", value: "compat" },
    { name: "MANIFEST_MACHINE_CLAIM_COMPAT_FROM", value: window.from },
    { name: "MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL", value: window.until },
  ];
}
