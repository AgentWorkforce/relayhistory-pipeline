/**
 * The never-in-Neon proof, shared by the session analysis (src/session-analysis) and the read
 * API's turns/metadata routes (src/session-read-api). A canonical scope without the legacy
 * export's coverage marker (canonical-legacy.ts `legacy_scope`) is still COMPLETE from S3 when
 * the session never existed in Neon, because then there are no Neon-only turns to miss. The
 * proof has two halves, and both must hold (any doubt is "not proven", which keeps the
 * Neon-era refusal):
 *
 *   database  sessions.session_analysis_neon_free (20261105046000): the frozen Neon copy is
 *             sealed, the session id is absent from it (any workspace of the org, physical or
 *             canonical id), it has a live pointer and every pointer was created after the
 *             seal. The read role calls it through sessions.session_neon_free_for, tenant
 *             pinned by session_directory_read_api_scope.
 *   objects   no manifest of the scope was written by the Neon export/sync: no signed export
 *             marker and no backfill epoch (neonDerivedManifest). This catches a turns-only
 *             Neon session that was copied to S3 after the seal.
 */
import { RELAYHISTORY_EXPORT_EXTENSION } from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import type { ManifestV1 } from "./vendor.ts";

/** The read role's database half (bulls96_read_api; tenant bound by the transaction). */
export const NEON_FREE_FOR_READ_SQL = "SELECT sessions.session_neon_free_for($1, $2) AS free";
/** The worker's database half (service/runtime role). */
export const NEON_FREE_SQL = "SELECT sessions.session_analysis_neon_free($1, $2, $3) AS free";

/** A manifest written by the Neon export/sync (signed export marker or a backfill epoch). */
export function neonDerivedManifest(manifest: Pick<ManifestV1, "extensions" | "raw">): boolean {
  return (
    manifest.extensions?.[RELAYHISTORY_EXPORT_EXTENSION] !== undefined ||
    manifest.raw.epochs.some((epoch) => epoch.reason === "backfill")
  );
}

/** Both halves: the database proved it AND no manifest is Neon-derived. */
export function neverInNeon(databaseProof: boolean, anyNeonDerivedManifest: boolean): boolean {
  return databaseProof === true && anyNeonDerivedManifest === false;
}
