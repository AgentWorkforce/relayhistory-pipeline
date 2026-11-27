/**
 * The backlog-ops sweeper freshness limit, in a module that imports nothing: shared by authority-execute
 * (backlog-authority.ts, which re-exports it) and the read-only sweeper-pass verifier (sweeper-pass-verify.ts), which
 * must not load the authority's write path.
 */

/** A sweeper pass older than this is not "live". */
export const BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS = 6 * 60 * 60 * 1000;
