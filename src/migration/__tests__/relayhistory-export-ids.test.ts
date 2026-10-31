/**
 * One set of Cloud ids end to end: the grant lifecycle canonicalizes upload
 * inputs (canonicalizeUploads), the courier canonicalizes its export plan
 * (canonicalizeExportPlan, applied by runExportUpload). An aliased input must
 * give the same plan digest in both paths, and relayauth's own derivation.
 */
import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { deriveMigrationGrantPlan } from "../vendor/relayauth-migration-plan.ts";
import type { DeliveryRecord } from "../relayhistory-export.ts";
import {
  canonicalizeExportPlan,
  canonicalizeUploads,
  EMPTY_ID_MAPPING,
  type IdMappingLedger,
} from "../relayhistory-export-ids.ts";
import {
  derivePlanContract,
  planContract,
  plannedManifestFromRecords,
  uploadFromPlanned,
  type ExportPlan,
} from "../relayhistory-export-upload.ts";
import { exportPlanForShard, planShards } from "../relayhistory-export-shards.ts";

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const uuid = (label: string) => {
  const h = sha(label);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-8${h.slice(17, 20)}-${h.slice(20, 32)}`;
};
const ORG = uuid("org");
const WS = uuid("ws");
const USER = uuid("user");
const GRANT = `mgr_${"1".repeat(32)}`;
export const MAPPING: IdMappingLedger = {
  version: 1, reviewed_by: "reviewer", reviewed_at: "2026-10-30T12:00:00.000Z",
  entries: [
    { kind: "org", legacy_id: "org_legacy", cloud_id: ORG },
    { kind: "workspace", legacy_id: "ws_legacy", cloud_id: WS },
    { kind: "user", legacy_id: "user_legacy", cloud_id: USER },
  ],
};

const record = (i: number, session: string): DeliveryRecord => ({
  kind: "session_event", origin_id: "feed-1", record_id: `${session}-${i}`, revision: String(i + 1), operation: "upsert",
  received_at: "2026-10-01T00:00:00Z", payload: { ts_ms: Date.UTC(2026, 9, 1) + i * 1000, text: "x".repeat(300) },
});
/** An export plan built from Neon rows that carry the legacy ids. */
const aliasedPlan = (): ExportPlan => ({
  exportedAt: "2026-10-30T12:00:00.000Z",
  manifests: ["s1", "s2"].map((session) => plannedManifestFromRecords({
    orgId: "org_legacy", workspaceId: "ws_legacy", source: "claude", sessionId: session, machine: "m1",
    sourceUserId: "user_legacy", records: [0, 1, 2].map((i) => record(i, session)), maxChunkBytes: 700,
  })!),
});

describe("one set of Cloud ids for lifecycle, RelayAuth and courier", () => {
  test("an aliased input gives identical plan digests in the courier path and the lifecycle path", () => {
    const plan = aliasedPlan();
    // Courier path: what runExportUpload derives (canonicalizeExportPlan, then the contract).
    const courier = planContract(canonicalizeExportPlan(plan, MAPPING), GRANT);
    // Lifecycle path: upload inputs canonicalized, as the grant plan and create request carry them.
    const lifecycleUploads = canonicalizeUploads(plan.manifests.map(uploadFromPlanned), MAPPING);
    const lifecycle = derivePlanContract({ grantId: GRANT, exportedAt: plan.exportedAt, uploads: lifecycleUploads });
    // The issuer's own derivation of the grant plan from the same request body.
    const issuer = deriveMigrationGrantPlan({ grantId: GRANT, orgId: ORG, plan: { exported_at: plan.exportedAt, manifests: lifecycleUploads } });
    expect(courier.planDigest).toBe(lifecycle.planDigest);
    expect(issuer.planDigest).toBe(courier.planDigest);
    expect(courier.uploads).toEqual(lifecycle.uploads);
    // Only Cloud ids anywhere in what is signed.
    const text = JSON.stringify(courier);
    for (const alias of ["org_legacy", "ws_legacy", "user_legacy"]) expect(text).not.toContain(alias);
    expect(courier.entries.every((e) => e.key.startsWith(`v1/ws=${WS}/`) && e.org_id === ORG && e.source_user_id === USER)).toBe(true);
    // Without the mapping the digests differ (the aliases stay raw).
    expect(planContract(canonicalizeExportPlan(plan, EMPTY_ID_MAPPING), GRANT).planDigest).not.toBe(courier.planDigest);
  });

  test("a shard planned over the lifecycle's canonical uploads selects the courier's canonical manifests", () => {
    const plan = aliasedPlan();
    const shard = planShards({ exportedAt: plan.exportedAt, uploads: canonicalizeUploads(plan.manifests.map(uploadFromPlanned), MAPPING) }).shards[0]!;
    const part = exportPlanForShard(plan, shard, MAPPING);
    expect(part.manifests).toHaveLength(2);
    expect(part.manifests.every((m) => m.location.workspaceId === WS && m.orgId === ORG && m.sourceUserId === USER)).toBe(true);
    expect(() => exportPlanForShard(plan, shard)).toThrow("does not cover"); // the raw plan does not match canonical keys
  });
});
