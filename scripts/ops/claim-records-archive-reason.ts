import { ClaimRecordsArchiveError } from "../../src/session-ingest/claim-records-archive.ts";
// A closed, redacted reason only: a pg error message or stack can carry the DB host.
export const redactedReason = (e: unknown): string => {
  const code = (e as { code?: unknown } | null)?.code;
  return typeof code === "string" && /^[0-9A-Z]{5}$/.test(code) ? "sqlstate_" + code
    : (e as { name?: unknown })?.name === "OwnerUrlError" && typeof code === "string" ? code
    : e instanceof ClaimRecordsArchiveError && /^[a-z_]{1,64}$/.test(e.code) ? e.code
    : e instanceof Error && /^[a-z_]{1,64}$/.test(e.message) ? e.message : "internal_error";
};
