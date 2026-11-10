/** Strict native transcript input for briefs. The public canonical record-stream
 * APIs stay ndjson-only. Nothing here relabels stored bytes or skips bad JSON. */
import { assertReaderScrubMarker, readerChunkBody } from "../../packages/pipeline/src/lib/scrub-engine/reader-policy.ts";
import { createHash } from "node:crypto";
import { chunkKeys, isManifestRest, parseSessionKey, type ManifestV1 } from "../../packages/object-store/src/index.ts";
import { objectStoreManifestReader, prefixAllowed, readCapped } from "../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { canonicalManifestIdentity } from "../shared/manifest-identity.ts";
import { manifestUserId } from "./canonical-projection.ts";
import { CANONICAL_SCOPE_BYTES, CanonicalReadError, canonicalSourceGeneration, checkedCanonicalPointers, type CanonicalDeps, type CanonicalIdentity } from "./canonical.ts";

const MAX_RECORDS = 100000;
const hash = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");
const digest = (s: string) => s.replace(/^\\x/u, "").toLowerCase();
export interface CanonicalTranscript {
  generation: string;
  records: Array<{ machineId: string; index: number; record: Record<string, unknown> }>;
}
function refuse(reason: string): never { throw new CanonicalReadError(reason, 501); }

/** Undefined selects the unchanged ndjson reader. A mixed-format scope refuses,
 * never silently discards a pointer. Strict current generation only; no pinning. */
export async function canonicalTranscriptSnapshot(deps: CanonicalDeps, identity: CanonicalIdentity, signal?: AbortSignal): Promise<CanonicalTranscript | undefined> {
  const initial = await deps.pointers(identity);
  if (!Array.isArray(initial)) throw new CanonicalReadError("pointer_bound");
  if (!initial.some(p => p.raw_format === "jsonl")) return undefined;
  const pointers = checkedCanonicalPointers(initial, identity, deps, 64, "jsonl");
  const generation = canonicalSourceGeneration(pointers, identity);
  const reader = objectStoreManifestReader(deps.store, { provider: deps.provider as "s3", bucket: deps.bucket, maxBytes: 1024 * 1024 });
  const parts: Array<{ manifest: ManifestV1; chunks: ReturnType<typeof chunkKeys>; machineId: string }> = [];
  let declaredBytes = 0, declaredRecords = 0;
  for (const p of pointers) {
    signal?.throwIfAborted();
    const read = await reader.read(p.manifest_key);
    if (!read) throw new CanonicalReadError("object_missing");
    const m = read.manifest, canonical = canonicalManifestIdentity(m), at = parseSessionKey(p.manifest_key);
    if (!at || !isManifestRest(at.rest) || !prefixAllowed(deps.prefix, at.prefix, identity.orgId)) refuse("prefix_mismatch");
    if (m.raw.format !== "jsonl" || m.raw.format !== p.raw_format || m.raw.content_sha256 !== digest(p.content_sha256) ||
        (p.manifest_etag != null && read.etag !== p.manifest_etag) || manifestUserId(m) !== p.user_id || String(m.revision) !== String(p.revision) ||
        canonical.sessionId !== p.session_id || canonical.machineId !== p.machine_id || m.session.workspace_id !== identity.workspaceId || m.session.harness !== identity.source ||
        at.workspaceId !== m.session.workspace_id || at.harness !== m.session.harness || at.sessionId !== m.session.session_id || at.machineId !== m.session.machine_id)
      refuse("manifest_mismatch");
    if (!m.raw.scrub) throw new CanonicalReadError("canonical_projection_incomplete", 503, ["manifest_unscrubbed"]);
    assertReaderScrubMarker(m.raw.scrub, deps.readerMaxRuleset);
    if (m.extensions?.["client.relayhistory_export"] !== undefined) refuse("transcript_export_marker_refused");
    // Full metadata/hash-chain validation above is unchanged. The live source
    // stream excludes superseded rewrite/truncation epochs, using the manifest
    // library's own selection (including any supported continuation chain).
    const chunks = chunkKeys(m, { prefix: at.prefix ?? "", currentEpochOnly: true });
    declaredBytes += chunks.reduce((n,{entry:c}) => n + Math.max(c.bytes, c.stored_raw_bytes ?? 0, c.stored_bytes), 0);
    declaredRecords += chunks.reduce((n,{entry:c}) => n + (c.records ?? 0), 0);
    if (declaredBytes > CANONICAL_SCOPE_BYTES || declaredRecords > MAX_RECORDS) refuse("transcript_scope_too_large");
    parts.push({ manifest: m, chunks, machineId: p.machine_id });
  }
  const records: CanonicalTranscript["records"] = [];
  let actualBytes = 0;
  for (const { manifest, chunks, machineId } of parts) {
    let pending = "", index = 0;
    const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
    const consume = (line: string) => {
      if (!line.trim()) return;
      if (records.length >= MAX_RECORDS) refuse("transcript_scope_too_large");
      let value: unknown;
      try { value = JSON.parse(line); } catch { refuse("transcript_json_invalid"); }
      if (!value || typeof value !== "object" || Array.isArray(value)) refuse("transcript_record_invalid");
      const record = value as Record<string, unknown>;
      // A recorded parent identity may use the layout or admitted logical id.
      // Sidechain records are deliberately omitted later by the existing normalizer.
      if (record.isSidechain !== true && typeof record.sessionId === "string" && record.sessionId !== manifest.session.session_id && record.sessionId !== identity.sessionId)
        refuse("transcript_session_mismatch");
      records.push({ machineId, index: ++index, record });
    };
    for (const { entry, key } of chunks) {
      signal?.throwIfAborted();
      if (entry.stored_bytes > CANONICAL_SCOPE_BYTES || (entry.stored_raw_bytes ?? entry.bytes) > CANONICAL_SCOPE_BYTES) refuse("transcript_chunk_too_large");
      const stored = await readCapped(await readerChunkBody(await deps.store.getObject(key), manifest.raw.scrub, deps.readerMaxRuleset), entry.stored_bytes);
      if (!stored || stored.byteLength !== entry.stored_bytes || hash(stored) !== entry.stored_sha256) refuse("transcript_chunk_digest_mismatch");
      let raw: Uint8Array;
      try { raw = deps.decompress(stored, entry.stored_raw_bytes ?? CANONICAL_SCOPE_BYTES); } catch { refuse("transcript_chunk_decompress_failed"); }
      if ((entry.stored_raw_bytes !== undefined && raw.byteLength !== entry.stored_raw_bytes) || (entry.raw_sha256 !== undefined && hash(raw) !== entry.raw_sha256)) refuse("transcript_chunk_digest_mismatch");
      actualBytes += raw.byteLength;
      if (actualBytes > CANONICAL_SCOPE_BYTES) refuse("transcript_scope_too_large");
      try { pending += decoder.decode(raw, { stream: true }); } catch { refuse("transcript_utf8_invalid"); }
      let newline: number;
      while ((newline = pending.indexOf("\n")) !== -1) { consume(pending.slice(0,newline)); pending = pending.slice(newline+1); }
    }
    try { pending += decoder.decode(); } catch { refuse("transcript_utf8_invalid"); }
    consume(pending);
    if (chunks.every(({entry:c}) => c.records !== undefined) && index !== chunks.reduce((n,{entry:c}) => n + c.records!, 0)) refuse("transcript_record_count_mismatch");
  }
  const fresh = checkedCanonicalPointers(await deps.pointers(identity), identity, deps, 64, "jsonl");
  if (canonicalSourceGeneration(fresh, identity) !== generation) throw new CanonicalReadError("source_generation_changed");
  return { generation, records };
}
