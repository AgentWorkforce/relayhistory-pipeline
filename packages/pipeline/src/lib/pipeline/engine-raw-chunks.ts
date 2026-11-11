/** Injected-engine raw sealer. Default off: only SCRUB_ENGINE=wasm-bindgen routes
 * here, through routedRawChunkSealer() in scrub-engine/index.ts, which binds this to
 * the verified pinned relayscrubber 0.5.0-alpha.5 instance (docs/scrub-engine.md,
 * "Cutover"). This module, the pin and the vendored bytes are digested in the
 * separate technical-foul-cutover.json entry. The historical TS sealer stays frozen.
 */
import { sha256Hex } from "../../../../object-store/src/crypto.js";
import { preflightScrubRecords, MAX_STREAM_BYTES } from "../scrub-engine/bindgen-admission.js";
import { poisonVerifiedBindgenEngine } from "../scrub-engine/bindgen-engine.js";
import type { ScrubEngine, ScrubMarker } from "../scrub-engine/engine.js";
import { ScrubberError } from "../scrub-engine/scrubber-error.js";
import { PipelineError } from "./errors.js";
import { isObjectStoreError, rawChunkKey, type ObjectStore } from "./object-layout.js";
import { rawZstdFrame, type SealRawChunk, type SealedChunk } from "./raw-chunks.js";

export interface ApprovedRetainedPolicy {
  /** An explicit data-model decision; never inferred from READER_MAX_RULESET. */
  readonly retained: "same-family-up-to-writer";
}
export interface EngineSealedChunk extends SealedChunk {
  /** Absent for historical objects without a profile. Informational to readers. */
  scrub: SealedChunk["scrub"] & { profile?: string };
}
export interface BoundRawChunkSealer {
  (store: ObjectStore, input: SealRawChunk): Promise<EngineSealedChunk>;
  readonly marker: ScrubMarker;
}

/** Capture the concrete verified instance before any await, including its health.
 * Production obtains this only via routedRawChunkSealer() (scrub-engine/index.ts);
 * the seam's sealRawChunk still refuses non-TS engines with seal_path_not_routed,
 * so an unwired call site refuses instead of falling back. Tests may inject a
 * distinguishable engine.
 */
export function bindEngineRawSealer(
  engine: ScrubEngine & { readonly failed: boolean },
  approvedPolicy: ApprovedRetainedPolicy,
): BoundRawChunkSealer {
  if (engine.kind !== "wasm-bindgen" || approvedPolicy.retained !== "same-family-up-to-writer")
    throw new PipelineError("chunk_sealer_policy_invalid", "raw sealing policy is not approved", false);
  const marker = Object.freeze({ ...engine.marker });
  let poisoned = false;
  const poison = (): never => {
    poisoned = true;
    poisonVerifiedBindgenEngine(engine);
    throw new ScrubberError("scrubber_instance_poisoned", "instance_poison");
  };
  const healthy = () => {
    if (poisoned || engine.failed !== false || marker.ruleset !== "relayhistory-scrub" ||
        !Number.isSafeInteger(marker.version) || marker.version < 1 || !marker.profile ||
        engine.rulesetVersion !== marker.version || engine.scrubProfile !== marker.profile ||
        engine.marker.ruleset !== marker.ruleset || engine.marker.version !== marker.version ||
        engine.marker.profile !== marker.profile) poison();
  };
  healthy();
  const seal = async (store: ObjectStore, input: SealRawChunk): Promise<EngineSealedChunk> => {
    healthy();
    // Snapshot all caller-owned inputs before hashing yields to another task.
    const source = preflightScrubRecords(input.source);
    if (source.byteLength === 0) throw new PipelineError("chunk_empty", "raw chunk is empty", false);
    // Rust legitimately returns zero records for a blank-only stream. Refuse
    // that request at admission instead of poisoning a healthy shared engine.
    if (!hasRecordContent(source)) throw new PipelineError("chunk_empty", "raw chunk has no records", false);
    const location = { ...input.location };
    const { seq, epoch, startOffset, format, traceId } = input;
    const sourceSha = await sha256Hex(source);
    const key = rawChunkKey(location, seq, startOffset, sourceSha, format);
    healthy();
    let result;
    try { result = engine.scrubRecords(source); }
    catch (cause) {
      healthy();
      if (cause instanceof ScrubberError) {
        if (cause.disposition === "instance_poison") return poison();
        throw cause;
      }
      return poison();
    }
    healthy();
    if (!result || !result.scrub || result.scrub.ruleset !== marker.ruleset || result.scrub.version !== marker.version ||
        result.scrub.profile !== marker.profile || !Number.isSafeInteger(result.records) || result.records <= 0)
      return poison();
    // Own the result before another await. Never compress input on scrub failure.
    let scrubbed: Uint8Array;
    try { scrubbed = snapshotOutput(result.scrubbed); }
    catch (cause) {
      if (cause instanceof PipelineError && cause.code === "scrub_output_too_large") throw cause;
      if (cause instanceof ScrubberError && cause.retryable) throw cause;
      return poison();
    }
    if (scrubbed.byteLength === 0) return poison();
    const stored = rawZstdFrame(scrubbed);
    const rawSha = await sha256Hex(scrubbed);
    const base = { seq, epoch, start_offset: startOffset, bytes: source.byteLength,
      sha256: sourceSha, sha256_8: sourceSha.slice(0, 8), compression: "zstd" as const, records: result.records };
    const retained = async (): Promise<EngineSealedChunk> => {
      // One GET supplies both body and metadata; no HEAD/GET provenance race.
      const object = await store.getObject(key);
      const meta = { ...object.meta.metadata };
      const bytes = await retainedBytes(object.body);
      const rawHash = meta["raw-sha256"], rawLength = meta["raw-bytes"], versionText = meta["scrub-version"];
      const rawBytes = Number(rawLength), version = Number(versionText);
      if (!rawHash || !/^[a-f0-9]{64}$/.test(rawHash) || rawLength === undefined ||
          !/^[1-9][0-9]*$/.test(rawLength) || !Number.isSafeInteger(rawBytes) || rawBytes < 1 || !versionText ||
          !/^[1-9][0-9]*$/.test(versionText) ||
          !Number.isSafeInteger(version) || version < 1 || !meta["scrub-ruleset"])
        throw new PipelineError("chunk_stored_unscrubbed", "stored chunk lacks valid scrub metadata", false);
      if (meta["scrub-ruleset"] !== marker.ruleset)
        throw new PipelineError("chunk_stored_foreign", "stored chunk has another scrub family", false);
      if (version > marker.version)
        throw new PipelineError("chunk_stored_newer", "stored chunk is newer than the bound writer", false);
      return { key, created: false,
        scrub: { ruleset: meta["scrub-ruleset"], version,
          ...(meta["scrub-profile"] === undefined ? {} : { profile: meta["scrub-profile"] }) },
        entry: { ...base, stored_bytes: bytes.byteLength, stored_sha256: await sha256Hex(bytes),
          stored_raw_bytes: rawBytes, raw_sha256: rawHash } };
    };
    // A concurrent poison before persistence also refuses; no raw/TS fallback.
    healthy();
    try {
      const ack = await store.put(key, stored, { ifNoneMatch: true, metadata: {
        "raw-sha256": rawSha, "raw-bytes": String(scrubbed.byteLength),
        "scrub-ruleset": marker.ruleset, "scrub-version": String(marker.version),
        "scrub-profile": marker.profile, ...(traceId ? { "trace-id": traceId } : {}),
      } });
      if (!ack.created) return retained();
      return { key, created: true, scrub: marker, entry: { ...base, stored_bytes: ack.size,
        stored_sha256: ack.contentSha256, stored_raw_bytes: scrubbed.byteLength, raw_sha256: rawSha } };
    } catch (cause) {
      if (!isObjectStoreError(cause, "already_exists")) throw cause;
      return retained();
    }
  };
  Object.defineProperty(seal, "marker", { value: marker, writable: false });
  return seal as BoundRawChunkSealer;
}

/** Anything outside Rust's `char::is_whitespace` set (Unicode White_Space),
 * which `str::trim()` in the crate's `scrub_records` uses to decide blank input.
 * Written out explicitly: JS `\s` differs (it includes U+FEFF and excludes U+0085).
 * U+FEFF, U+200B, U+180E and other format characters are content here.
 */
const RUST_NON_WHITESPACE =
  /[^\u0009-\u000D\u0020\u0085\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]/u;

/** True when `text` is empty or consists only of Rust/Unicode White_Space. */
export function isRustWhitespaceOnly(text: string): boolean {
  return !RUST_NON_WHITESPACE.test(text);
}

/** Whether a UTF-8 record stream contains anything the Rust engine would not
 * trim away as blank. Decodes in 64 KiB pieces with a streaming decoder, so a
 * code point split across a piece boundary is judged whole; `ignoreBOM: true`
 * keeps a leading U+FEFF as content, as Rust does. Shared with the sealer.
 */
export function hasRecordContent(source: Uint8Array): boolean {
  try {
    const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
    for (let i = 0; i < source.length; i += 65_536)
      if (!isRustWhitespaceOnly(decoder.decode(source.subarray(i, i + 65_536), { stream: true }))) return true;
    return !isRustWhitespaceOnly(decoder.decode());
  } catch {
    // UTF-8 was validated by preflight; inability to stage this check can retry.
    throw new ScrubberError("scrub_admission_unavailable", "call_refusal", undefined, true);
  }
}

/** Bound the collision read independently of untrusted stored metadata. */
async function retainedBytes(body: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = body.getReader(), chunks: Uint8Array[] = [];
  let length = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > MAX_STREAM_BYTES + 4096) {
        await reader.cancel();
        throw new PipelineError("chunk_stored_too_large", "stored chunk exceeds the sealing bound", false);
      }
      chunks.push(new Uint8Array(value));
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return bytes;
}

/** Output expansion is not subject to the input's per-record cap. The staging
 * memory bound remains 64 MiB; exceeding it refuses permanently without poisoning
 * a correctly functioning engine. Invalid output bytes are an engine defect.
 */
function snapshotOutput(output: Uint8Array): Uint8Array {
  if (!(output instanceof Uint8Array)) throw new TypeError("invalid scrub output");
  const proto = Object.getPrototypeOf(Uint8Array.prototype);
  const buffer = Object.getOwnPropertyDescriptor(proto, "buffer")!.get!.call(output);
  const length = Object.getOwnPropertyDescriptor(proto, "byteLength")!.get!.call(output);
  Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "byteLength")!.get!.call(buffer);
  if (length > MAX_STREAM_BYTES)
    throw new PipelineError("scrub_output_too_large", "scrubbed chunk exceeds the sealing capacity", false);
  let bytes: Uint8Array;
  try { bytes = new Uint8Array(length); bytes.set(output); }
  catch { throw new ScrubberError("scrub_admission_unavailable", "call_refusal", undefined, true); }
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
  for (let i = 0; i < bytes.length; i += 65_536)
    decoder.decode(bytes.subarray(i, i + 65_536), { stream: true });
  decoder.decode();
  return bytes;
}
