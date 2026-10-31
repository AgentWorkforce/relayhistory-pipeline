import type { ReportProvenance } from "../provenance.ts";

/** A complete, clean provenance for report fixtures (tests only). */
export const PROV: ReportProvenance = {
  harness: { commit: "a".repeat(40), dirty: false },
  inputs: { sha256: "b".repeat(64), source: "file" },
  configSha256: "c".repeat(64),
  stagePlan: { provenanceSha256: "d".repeat(64), generatedAt: "2026-10-31T01:32:53.897Z", commit: "e".repeat(40), planSha256: "f".repeat(64), allowedUpdates: [], updateApproval: null },
  charts: [{ name: "96bulls", version: "0.3.0", digest: "0".repeat(64) }],
};
