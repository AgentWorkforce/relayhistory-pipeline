/** Native JSONL capacity proof; canonical envelopes stay separately budgeted.
 * Writes only compressed synthetic data to an owned scratch directory. */
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { createHash } from "node:crypto";
import { rawChunkKey, type ManifestV1 } from "../shared/vendor.ts";
import { createFsStore } from "../../packages/object-store/src/fs.ts";
import { rawDigest } from "../bob-costas/raw-digest.ts";
import { extractiveSummarizer } from "../bob-costas/summarizer.ts";

const root = await mkdtemp(join(tmpdir(), "bob-capacity-"));
try {
  const store = createFsStore({ root });
  const loc = { workspaceId: "w", machineId: "m", harness: "codex", sessionId: "large", startDate: "2026-11-04" };
  const raw = Buffer.from(JSON.stringify({ type: "response_item", payload: { type: "message", role: "user", content: [{ type: "input_text", text: "capacity-marker " + "x".repeat(8 * 1024 * 1024) }] } }) + "\n");
  const stored = zstdCompressSync(raw);
  const sha256 = createHash("sha256").update(raw).digest("hex");
  const chunks = [];
  for (let seq = 0; seq < 192; seq++) {
    await store.put(rawChunkKey(loc, seq, seq * raw.length, sha256.slice(0, 8), "jsonl"), stored);
    chunks.push({ seq, epoch: 0, start_offset: seq * raw.length, bytes: raw.length, stored_bytes: stored.length, sha256, sha256_8: sha256.slice(0, 8), compression: "zstd" });
  }
  const manifest = { session: { workspace_id: "w", machine_id: "m", harness: "codex", session_id: "large", started_at: "2026-11-04T00:00:00Z" }, raw: { format: "jsonl", raw_bytes: raw.length * chunks.length, chunks } } as unknown as ManifestV1;
  const digest = await rawDigest(manifest, "v1/ws=w/m=m/h=codex/d=2026/11/04/s=large/manifest.json", store);
  assert.equal(digest.records, 192);
  assert.equal(digest.userTurnCount, 192);
  assert.equal(digest.userTurns.length, 10);
  assert(digest.userTurns.every(s => s.length <= 2000));
  assert((await extractiveSummarizer.summarize("long", digest)).text.includes("capacity-marker"));
  console.log(JSON.stringify({ nativeRawBytes: manifest.raw.raw_bytes, chunks: 192, retainedPrompts: digest.userTurns.length, maxRssKiB: process.resourceUsage().maxRSS, canonicalLargeSupported: false }));
} finally { await rm(root, { force: true, recursive: true }); }
