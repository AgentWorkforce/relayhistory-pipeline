// Test-only receipt envelope (fake values).
import type { JobEnvelope } from "../../frozen-source-copy-progress.ts";
export function testEnvelope(over: Partial<JobEnvelope> = {}): JobEnvelope {
  return {
    startedAt: 0,
    finishedAt: 1,
    budget: { maxWallMs: 10, deadline: 10, pageSizes: { records: 5000, receipts: 1000 }, concurrency: 2, maxPageMs: 4000, statementTimeoutMs: 60000, lockTimeoutMs: 5000 },
    progress: { bindingSha: null, totalOrgs: 2, completedOrgs: 0, dataset: null, pages: 0, rows: 0, resumeCount: 0, lastCheckpointAt: null, cursorSha256: null, inFlight: 0 },
    abort: { requested: false, observed: false, reason: null, requestedAt: null, completedAt: 1, sourceClosed: true, targetClosed: true, writesStopped: true },
    reconciliation: { result: "NOT_REQUIRED", readbackAt: null },
    ...over,
  };
}
