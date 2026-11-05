import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { objectKey, readAll, type ManifestV1 } from "../../shared/vendor.ts";
import { objectStoreManifestReader } from "../../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { canonicalManifestIdentity } from "../../shared/manifest-identity.ts";
import { exportUnit } from "../../migration/neon-s3-sync/export-session.ts";
import { buildSessionStream } from "../../migration/relayhistory-export.ts";
import { RELAYHISTORY_LEGACY_MACHINE_ID } from "../../migration/neon-s3-sync/legacy-source.ts";
import { SeedProofIndex, type SeedPointer, type ProofDeps } from "../seed-proof.ts";
import { TENANT, scratchStore } from "./support.ts";

const sha = (s: string) => createHash("sha256").update(s).digest("hex");

test("gate 2: the seed proof acknowledges an exported legacy manifest's records without seeding or failing", async () => {
  const fs = scratchStore();
  const dir = mkdtempSync(join(tmpdir(), "seed-legacy-"));
  try {
    const unit = { orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, source: "claude", sessionId: "sess-1", firstMs: 1761955200000, lastMs: 1761955200000, machineIds: [], totals: {} };
    const line = (kind: string, origin: string, id: string, payload: Record<string, unknown>) => ({
      machine: RELAYHISTORY_LEGACY_MACHINE_ID,
      userId: TENANT.userId,
      bytes: buildSessionStream([{ kind, origin_id: origin, record_id: id, revision: "1", operation: "upsert", received_at: "2025-11-01T00:00:00Z", payload }]).bytes,
    });
    const scope = { org_id: unit.orgId, workspace_id: unit.workspaceId, source: "claude", session_id: "sess-1" };
    const result = await exportUnit({ store: fs.store, keyRoot: "96bulls", grantId: "g" }, unit, {
      fenced: async () => false,
      async *lines() {
        yield line("legacy_conversation_turn", "legacy:conversation_turns", "legacy:conversation_turns:t1", { ...scope, id: "t1", turn_index: 0, role: "user", session_owner: "u", content: "hi", actor_name: "U", actor_role: "owner", ts: "2025-11-01T00:00:00Z" });
        yield line("legacy_scope", "legacy:scope", "legacy:scope:coverage", { ...scope, version: 1, tables: ["conversation_turns", "session_links", "session_outcomes"] });
      },
    });
    const key = result.manifests[0]!.manifestKey;
    const manifest = JSON.parse(new TextDecoder().decode(await readAll((await fs.store.getObject(objectKey(key))).body))) as ManifestV1;
    const c = canonicalManifestIdentity(manifest);
    const pointer: SeedPointer = {
      org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, user_id: TENANT.userId, harness: "claude",
      session_id: c.sessionId, machine_id: c.machineId, bucket: "fixture", storage_provider: "s3", manifest_key: key,
      revision: String(manifest.revision), content_sha256: manifest.raw.content_sha256, raw_format: "ndjson",
      capture: { pointer_id: sha(key + manifest.raw.content_sha256), scope_sha256: sha("scope"), reset_epoch: "0", fence_versions: {} },
    };
    const deps: ProofDeps = {
      store: fs.store, bucket: "fixture", provider: "s3",
      reader: objectStoreManifestReader(fs.store, { provider: "s3", bucket: "fixture" }),
      prefixFor: (org) => "96bulls/" + org,
      decompress: (raw, max) => new Uint8Array(zstdDecompressSync(raw, { maxOutputLength: max })),
    };
    const index = new SeedProofIndex(join(dir, "proof.sqlite"), TENANT.orgId, "a".repeat(40));
    try {
      await index.addPointer(pointer, deps);
      expect(index.seal([pointer])).toMatchObject({ pointers: 1, records: 0, unseeded_records: 2 });
    } finally {
      index.close();
    }
  } finally {
    fs.done();
    rmSync(dir, { recursive: true, force: true });
  }
});
