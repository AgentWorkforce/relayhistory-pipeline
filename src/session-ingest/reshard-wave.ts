/**
 * reshard-verify for a wave: backlog-authority.ts reshardVerifyOrgs (the revision reshard's verify, unchanged), run
 * for each org on its own and up to `concurrency` orgs at once (org-wave.ts runWave). The summary is exactly
 * reshardVerifyOrgs' shape (RESHARD_SCHEMA), per_org in the wave's order, so every consumer (the reshard-dry receipt,
 * checkReshardReceipt) is unchanged. One org's failure is that org's verdict only (reshardVerifyOrgs already never
 * stops early; runWave keeps a throw, e.g. a bad org id, to that org as `reshard_failed`).
 */
import type { ObjectStore } from "../shared/vendor.ts";
import { evidenceHash } from "./authority-evidence.ts";
import { reshardVerifyOrgs, type ReshardVerifyOrg } from "./backlog-authority.ts";
import { runWave, waveOrgList } from "./org-wave.ts";

/** Orgs verified at once (each verify lists and reads its own prefix with `perOrgConcurrency` requests). */
export const RESHARD_WAVE_CONCURRENCY = 4;

export async function reshardVerifyWave(store: ObjectStore, keyRoot: string, orgs: readonly string[], concurrency = RESHARD_WAVE_CONCURRENCY, perOrgConcurrency = 8) {
  const list = waveOrgList([...orgs]);
  const settled = await runWave(list, concurrency, async (org) => (await reshardVerifyOrgs(store, keyRoot, [org], perOrgConcurrency)).per_org[0]!);
  const per: ReshardVerifyOrg[] = settled.map((s) => s.ok ? s.value : {
    listed: 0, legacy_shards: 0, entries: 0, merged: 0, would_merge: 0, shards: 0, invalid: 0, failed: 1, verified: 0, missing: 0,
    org_hash: evidenceHash("org", s.org), verify_passed: false, refused: "reshard_failed" });
  return { mode: "dry_run" as const, tool: "reshard_verify", org_count: list.length, verify_passed: per.every((p) => p.verify_passed), per_org: per };
}
