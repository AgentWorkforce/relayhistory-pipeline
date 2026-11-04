/** Bounded raw reader. Native sessions retain one chunk plus a small digest.
 * Canonical streams need revision/delete folding; refuse their explicit budget
 * until an external-memory fold is available. Never publish a partial digest. */
import { zstdDecompressSync } from "node:zlib";
import { createHash } from "node:crypto";
import { PipelineError, parseSessionKey, rawChunkKey, type ManifestV1, type ObjectStore } from "../shared/vendor.ts";
import { readCapped } from "../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { normalizeRelayhistoryRecords } from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { projectRelayhistorySession } from "../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import { readManifestRecords } from "../session-read-api/canonical-projection.ts";
import { addDigestRecord, digestRecords, newDigest, type SessionDigest } from "./summarizer.ts";

export const MAX_SUMMARY_CHUNK_BYTES = 16 * 1024 * 1024;
export const MAX_CANONICAL_SUMMARY_BYTES = 64 * 1024 * 1024;
export const MAX_CANONICAL_SUMMARY_RECORDS = 100_000;

function fail(code: string): never {
  throw new PipelineError(code, "raw session cannot be summarized within the verified format and memory bounds", false);
}
const hash = (v: Uint8Array) => createHash("sha256").update(v).digest("hex");

/** Iterate without a session-sized string or split/spread argument list. */
function* lines(text: string) {
  let start = 0;
  for (;;) {
    const end = text.indexOf("\n", start);
    if (end < 0) { if (start < text.length) yield text.slice(start); return; }
    yield text.slice(start, end);
    start = end + 1;
  }
}

export async function rawDigest(m: ManifestV1, manifestKey: string, store: ObjectStore, maxRawBytes = Number.POSITIVE_INFINITY): Promise<SessionDigest> {
  if (m.raw.raw_bytes > maxRawBytes) fail("summary_host_budget");
  const canonical = m.raw.format === "ndjson" && (
    m.extensions?.["client.relayhistory_export"] !== undefined ||
    m.extensions?.["relayhistory_live"] !== undefined ||
    m.raw.owner === "relayhistory-api:delivery"
  );
  if (canonical && m.raw.raw_bytes > MAX_CANONICAL_SUMMARY_BYTES) fail("summary_canonical_budget");
  const d = newDigest(m.session.harness);
  const prefix = parseSessionKey(manifestKey)?.prefix;
  if (canonical) {
    // Reuse the authoritative decoder, integrity checks, revision/delete fold
    // and projection. Large streams must use the forthcoming published-page
    // adapter; never create a second permissive canonical decoder here.
    let bytes = 0;
    const records = await readManifestRecords(m, store, prefix ?? "", (stored, maxBytes) => {
      const raw = zstdDecompressSync(stored, { maxOutputLength: Math.min(maxBytes, MAX_SUMMARY_CHUNK_BYTES) });
      bytes += raw.byteLength;
      if (bytes > MAX_CANONICAL_SUMMARY_BYTES) fail("summary_canonical_budget");
      return raw;
    }, MAX_CANONICAL_SUMMARY_RECORDS);
    const projected = projectRelayhistorySession(normalizeRelayhistoryRecords(records), {
      workspaceId: m.session.workspace_id, source: m.session.harness,
      sessionId: m.session.session_id, machineId: m.session.machine_id, userId: "",
    });
    for (const { table, row } of projected.rows) {
      if (table !== "convergence_events") continue;
      d.records++;
      addDigestRecord(d, {
        role: row.actor_role, type: row.kind, content: row.content,
        timestamp: row.ts, model: row.model,
        ...(row.kind === "tool_call" ? { name: (row.tool_call as Record<string, unknown> | null)?.name } : {}),
      });
    }
    return d;
  }
  const loc = {
    ...(prefix ? { prefix } : {}), workspaceId: m.session.workspace_id,
    machineId: m.session.machine_id, harness: m.session.harness,
    sessionId: m.session.session_id, startDate: m.session.started_at.slice(0, 10),
  };
  let nativeBytes = 0;
  for (const chunk of m.raw.chunks) {
    if (chunk.stored_bytes > MAX_SUMMARY_CHUNK_BYTES || (chunk.stored_raw_bytes ?? chunk.bytes) > MAX_SUMMARY_CHUNK_BYTES)
      fail("summary_chunk_budget");
    const key = rawChunkKey(loc, chunk.seq, chunk.start_offset, chunk.sha256_8, m.raw.format);
    let stored: Uint8Array | null;
    try { stored = await readCapped(await store.get(key), MAX_SUMMARY_CHUNK_BYTES); }
    catch (error) {
      if ((error as { code?: string })?.code === "not_found")
        throw new PipelineError("chunk_missing", "a listed summary chunk is missing", true);
      throw new PipelineError("chunk_unavailable", "a summary chunk read failed", true);
    }
    if (!stored) fail("summary_chunk_budget");
    if ((chunk.stored_bytes !== undefined && stored.byteLength !== chunk.stored_bytes) ||
      (chunk.stored_sha256 && hash(stored) !== chunk.stored_sha256)) fail("chunk_digest_mismatch");
    let raw: Uint8Array;
    try { raw = zstdDecompressSync(stored, { maxOutputLength: MAX_SUMMARY_CHUNK_BYTES }); }
    catch { fail("summary_chunk_decode"); }
    if (chunk.raw_sha256 && hash(raw) !== chunk.raw_sha256) fail("chunk_digest_mismatch");
    nativeBytes += raw.byteLength;
    if (nativeBytes > maxRawBytes) fail("summary_host_budget");
    let text: string;
    try { text = new TextDecoder("utf-8", { fatal: true }).decode(raw); }
    catch { fail("chunk_not_utf8"); }
    digestRecords(m.session.harness, lines(text), d);
  }
  return d;
}
