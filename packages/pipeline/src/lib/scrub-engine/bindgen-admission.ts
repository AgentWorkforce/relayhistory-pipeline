/** Byte admission only. Recognition and JSON parsing remain in the Rust engine. */
import { PipelineError } from "../pipeline/errors.js";

export const MAX_TEXT_BYTES = 16 * 1024 * 1024;
export const MAX_RECORD_BYTES = 16 * 1024 * 1024;
export const MAX_STREAM_BYTES = 64 * 1024 * 1024;

const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype);
const readBuffer = Object.getOwnPropertyDescriptor(typedArrayPrototype, "buffer")!.get!;
const readViewLength = Object.getOwnPropertyDescriptor(typedArrayPrototype, "byteLength")!.get!;
const readArrayBufferLength = Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "byteLength")!.get!;

export function refuseMalformedScrubInput(): never {
  throw new PipelineError("scrub_input_not_well_formed", "scrub input is not well-formed", false);
}

function refuseSize(): never {
  throw new PipelineError("scrub_input_too_large", "scrub input exceeds supported byte limits", false);
}

function refuseUnavailable(): never {
  throw new PipelineError("scrub_admission_unavailable", "scrub input admission is unavailable", true);
}

/** Count UTF-8 without first allocating an encoded copy. Never replace lone surrogates. */
export function preflightScrubText(value: unknown): asserts value is string {
  if (typeof value !== "string") refuseMalformedScrubInput();
  let bytes = 0;
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i);
    if (c <= 0x7f) bytes++;
    else if (c <= 0x7ff) bytes += 2;
    else if (c >= 0xd800 && c <= 0xdbff) {
      const next = value.charCodeAt(++i);
      if (!(next >= 0xdc00 && next <= 0xdfff)) refuseMalformedScrubInput();
      bytes += 4;
    } else if (c >= 0xdc00 && c <= 0xdfff) refuseMalformedScrubInput();
    else bytes += 3;
    if (bytes > MAX_TEXT_BYTES) refuseSize();
  }
}

/**
 * Snapshot before validation/invocation. Shared buffers are refused, so another
 * thread cannot change bytes during the copy. No view into caller memory escapes.
 * The qualified Rust package must independently recheck these same limits.
 */
export function preflightScrubRecords(source: Uint8Array): Uint8Array {
  if (!(source instanceof Uint8Array)) refuseMalformedScrubInput();
  let buffer: unknown;
  let length: number;
  try {
    buffer = readBuffer.call(source);
    length = readViewLength.call(source);
  } catch (error) {
    if (error instanceof TypeError) return refuseMalformedScrubInput();
    return refuseUnavailable();
  }
  // A positive ordinary-ArrayBuffer brand check also refuses SharedArrayBuffer
  // from another realm, where `instanceof SharedArrayBuffer` would miss it.
  try {
    readArrayBufferLength.call(buffer);
  } catch {
    throw new PipelineError("scrub_input_shared_buffer", "scrub input must not use shared memory", false);
  }
  if (length > MAX_STREAM_BYTES) refuseSize();
  let bytes: Uint8Array;
  try {
    bytes = new Uint8Array(length);
    bytes.set(source);
  } catch (error) {
    if (error instanceof TypeError) return refuseMalformedScrubInput();
    return refuseUnavailable();
  }
  let recordBytes = 0;
  for (const byte of bytes) {
    // Match Rust's physical LF-delimited record limit: LF is not part of the
    // record; CR and whitespace are, even for a blank record.
    if (byte === 0x0a) recordBytes = 0;
    else if (++recordBytes > MAX_RECORD_BYTES) refuseSize();
  }
  // Decode in bounded pieces only to validate UTF-8, retaining split codepoints
  // between pieces. Preserve the original bytes for Rust; never re-encode them.
  try {
    const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
    for (let i = 0; i < bytes.length; i += 65_536)
      decoder.decode(bytes.subarray(i, i + 65_536), { stream: true });
    decoder.decode();
  } catch (error) {
    if (error instanceof TypeError)
      throw new PipelineError("chunk_not_utf8", "raw chunk is not valid UTF-8", false);
    return refuseUnavailable();
  }
  return bytes;
}
