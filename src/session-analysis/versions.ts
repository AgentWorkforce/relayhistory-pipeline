import { SESSION_ANALYSIS_SCHEMA_VERSION } from "./engine/analysis-contract.ts";
import { ANALYZER_VERSION } from "./engine/model.ts";

/**
 * relayhistory versions.ts: `${ANALYZER_VERSION}:relayhistory-session-v2` names the
 * Skip prompt plus relayhistory's Neon evidence adapter. The prompt, contract, model and
 * schema are unchanged here; only the evidence adapter is new (canonical S3), so only the
 * adapter suffix differs, exactly as the brief engine's `:96bulls-v1` does.
 */
export const SESSION_ANALYSIS_VERSION = `${ANALYZER_VERSION}:96bulls-s3-v1`;
/** relayhistory's own analysis version, accepted when a backfilled Neon result is served. */
export const RELAYHISTORY_SESSION_ANALYSIS_VERSIONS = [
  `${ANALYZER_VERSION}:relayhistory-session-v2`,
  `${ANALYZER_VERSION}:relayhistory-session-v1`,
] as const;
export const SESSION_ANALYSIS_RESULT_SCHEMA_VERSION = SESSION_ANALYSIS_SCHEMA_VERSION;
/** relayhistory versions.ts SESSION_ANALYSIS_MODEL_VERSION, unchanged. */
export const SESSION_ANALYSIS_MODEL_VERSION = "gpt-5.4";
/** The job envelope's own version (relayhistory processor.ts `schemaVersion: 1`). */
export const SESSION_ANALYSIS_ENVELOPE_VERSION = 1 as const;
