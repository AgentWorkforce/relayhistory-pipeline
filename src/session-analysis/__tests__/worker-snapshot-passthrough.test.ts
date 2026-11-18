/**
 * The worker's evidence deps (canonicalEvidenceDeps) pass every completeness signal of the producer
 * snapshot through to loadSessionSource's condition (d): neonDerived, legacyRowsMerged and
 * legacyDroppedReason; and the cutover proof's SQL binds the worker's own arguments.
 */
import { expect, test } from "bun:test";
import { canonicalEvidenceDeps, CUTOVER_FREE_SQL } from "../worker.ts";

let snap: Record<string, unknown> = {};

const ID = { orgId: "o", workspaceId: "w", sessionId: "s", source: "claude" };

test("legacyDroppedReason, legacyRowsMerged and neonDerived reach the analysis snapshot; absent stays absent", async () => {
  const queries: Array<{ text: string; params: unknown[] }> = [];
  const deps = canonicalEvidenceDeps(
    { query: async (text: string, params: unknown[]) => (queries.push({ text, params }), { rows: [{ free: true }] }) } as never,
    { store: {} as never, provider: "s3", bucket: "b", prefix: {}, decompress: (b: Uint8Array) => b },
    (async () => snap) as never,
  );
  snap = { exists: true, generation: "g", legacyCoverage: null, neonDerived: false, legacyRowsMerged: false, rows: [], legacyDroppedReason: "legacy_provenance_invalid" };
  expect(await deps.snapshot(ID)).toMatchObject({ neonDerived: false, legacyRowsMerged: false, legacyDroppedReason: "legacy_provenance_invalid" });
  snap = { exists: true, generation: "g", legacyCoverage: null, neonDerived: false, legacyRowsMerged: true, rows: [] };
  const plain = await deps.snapshot(ID);
  expect(plain.legacyRowsMerged).toBe(true);
  expect("legacyDroppedReason" in plain).toBe(false);
  expect(await deps.cutoverFree!({ ...ID, earliestEvidenceAt: "2026-11-17T10:00:00.000Z" })).toBe(true);
  expect(queries.at(-1)).toEqual({ text: CUTOVER_FREE_SQL, params: ["o", "w", "claude", "s", "2026-11-17T10:00:00.000Z"] });
});
