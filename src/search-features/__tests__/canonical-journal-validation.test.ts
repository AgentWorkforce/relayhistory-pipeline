import { describe, expect, test } from "bun:test";
import { CanonicalJournalStore } from "../canonical-journal.ts";
import { canonicalImageKey } from "../canonical-image-key.ts";

const A = "a".repeat(64), UID = "12345678-1234-1234-1234-123456789012";
const SCOPE = { orgId: "o", workspaceId: "w", harness: "h", sessionId: "s",
  generation: A };
const AUTH = { authorityId: UID, epoch: "1", fingerprint: A };
const PTR = { authorityId: UID, hash: "c".repeat(64), count: "1" };
const HS1 = canonicalImageKey(SCOPE, UID);
const base = {
  revision: "10", kind: "publish", org_id: "o", workspace_id: "w",
  scope: SCOPE, schema_generation: A, image_key: HS1,
  source_hash: A, prior_keyset_hash: null,
  source_authority: AUTH, prior_source_authority: null,
  pointer_set: PTR,
};
// A store over a fake that returns ONE row (the decoder validates it).
const store = (row: Record<string, unknown>) =>
  new CanonicalJournalStore({
    query: async <T>(_t: string, _p?: unknown[]) =>
      ({ rows: [row] as T[] }),
  });
const dec = (row: Record<string, unknown>) =>
  store(row).entries("0", 10);

describe("journal decoder — cross-field fail-closed negatives", () => {
  test("valid row decodes", async () => {
    const out = await dec(base);
    expect(out.entries[0]!.kind).toBe("publish");
  });
  const neg = (name: string, patch: Record<string, unknown>) =>
    test(name, () =>
      expect(dec({ ...base, ...patch })).rejects.toThrow("journal_row_invalid"));
  neg("hs1 digest mismatch", { image_key: HS1.replace("a", "b") });
  neg("hs1 generation mismatch", { schema_generation: "b".repeat(64) });
  neg("routing org mismatch", { org_id: "other" });
  neg("routing ws mismatch", { workspace_id: "other" });
  neg("scope generation != hs1", {
    scope: { ...SCOPE, generation: "b".repeat(64) } });
  neg("source_hash != fingerprint", { source_hash: "b".repeat(64) });
  neg("pointer authority != source", {
    pointer_set: { ...PTR, authorityId: "87654321-4321-4321-4321-210987654321" } });
  neg("epoch not positive", {
    source_authority: { ...AUTH, epoch: "0" } });
  neg("epoch leading zero", {
    source_authority: { ...AUTH, epoch: "01" } });
  neg("epoch too long", {
    source_authority: { ...AUTH, epoch: "1".repeat(20) } });
  neg("count too long", { pointer_set: { ...PTR, count: "1".repeat(7) } });
  neg("extra key in scope", { scope: { ...SCOPE, x: "y" } });
  neg("extra key in pointer set", { pointer_set: { ...PTR, x: "y" } });
  neg("publish: prior present without keyset", {
    prior_source_authority: AUTH });
  test("unknown kind", () =>
    expect(dec({ ...base, kind: "update" }))
      .rejects.toThrow("journal_kind_invalid"));
});
