// Transport and identity checks only. No secret recognition in JavaScript.
import init, { initSync, manifestJson, scrubText, scrubTextChecked, scrubJson, scrubRecords } from './generated/relayscrubber.js';


/** Stable tagged failure. Only unexpected engine failures poison an instance. */
export class ScrubberError extends Error {
  constructor(code, disposition = 'call_refusal', record) {
    super(code);
    this.name = 'ScrubberError';
    this.code = code;
    this.disposition = disposition;
    if (record !== undefined) this.record = record;
  }
}
let poisoned = false;
const refusals = new Set(['text_too_large', 'record_too_large', 'stream_too_large', 'json_too_deep', 'invalid_json', 'invalid_utf8', 'serialization_error']);
function invoke(operation) {
  if (poisoned) throw new ScrubberError('scrubber_instance_poisoned', 'instance_poison');
  try { return operation(); } catch (error) {
    if (error instanceof ScrubberError) throw error;
    let tagged;
    try { tagged = JSON.parse(String(error)); } catch { /* unexpected runtime error */ }
    if (tagged?.disposition === 'call_refusal' && refusals.has(tagged.code)) {
      throw new ScrubberError(tagged.code, 'call_refusal', tagged.record);
    }
    poisoned = true;
    throw new ScrubberError('scrubber_engine_failure', 'instance_poison');
  }
}

export const MAX_RECORD_BYTES = 16 * 1024 * 1024;
export const MAX_STREAM_BYTES = 64 * 1024 * 1024;
// Count UTF-8 before crossing the WASM allocator. Lone surrogates use U+FFFD,
// matching TextEncoder and wasm-bindgen. Stop immediately over the bound.
function admitText(text, code) {
  if (typeof text !== 'string') throw new ScrubberError('scrubber_invalid_input');
  let bytes = 0;
  for (let i = 0; i < text.length; i++) {
    const c = text.charCodeAt(i);
    if (c < 0x80) bytes++;
    else if (c < 0x800) bytes += 2;
    else if (c >= 0xd800 && c <= 0xdbff && i + 1 < text.length && text.charCodeAt(i + 1) >= 0xdc00 && text.charCodeAt(i + 1) <= 0xdfff) { bytes += 4; i++; }
    else bytes += 3;
    if (bytes > MAX_RECORD_BYTES) throw new ScrubberError(code);
  }
}
function textChecked(text) { admitText(text, 'text_too_large'); return scrubTextChecked(text); }
function textCompatible(text) {
  try { admitText(text, 'text_too_large'); } catch (error) {
    if (error.code === 'text_too_large') return '[REDACTED]';
    throw error;
  }
  return scrubText(text);
}
function admitRecords(bytes) {
  if (!(bytes instanceof Uint8Array)) throw new ScrubberError('scrubber_invalid_input');
  if (bytes.byteLength > MAX_STREAM_BYTES) throw new ScrubberError('stream_too_large');
  let start = 0;
  for (let i = 0; i <= bytes.length; i++) {
    if (i === bytes.length || bytes[i] === 10) {
      if (i - start > MAX_RECORD_BYTES) throw new ScrubberError('record_too_large');
      start = i + 1;
    }
  }
}

function checked(expected) {
  const manifest = invoke(() => JSON.parse(manifestJson()));
  if (manifest.schema !== 3 || manifest.abiVersion !== 2 || typeof manifest.scrubProfile !== 'string' || !expected || ['schema', 'ruleset', 'rulesetVersion', 'rulesetSourceSha256', 'crateVersion', 'abiVersion', 'policy', 'redactionToken', 'scrubProfile'].some(key => expected[key] !== manifest[key])) {
    throw new ScrubberError('scrubber_manifest_mismatch');
  }
  return Object.freeze({
    manifest: Object.freeze(manifest), scrubText: text => invoke(() => textCompatible(text)),
    scrubLong: text => invoke(() => textCompatible(text)), scrubTextChecked: text => invoke(() => textChecked(text)),
    scrubJson: text => invoke(() => { admitText(text, 'record_too_large'); return JSON.parse(scrubJson(text)); }),
    scrubRecords: bytes => invoke(() => {
      admitRecords(bytes);
      const result = JSON.parse(scrubRecords(bytes));
      return { bytes: new TextEncoder().encode(result.text), recordCount: result.recordCount, redactionCount: result.redactionCount };
    }),
  });
}

let initialization;
let initializedDigest;
let compiledInitialized = false;

/** Initialize Node/Bun using trusted, pinned artifact bytes; never fetch implicitly. */
export async function loadScrubber({ bytes, expectedManifest, expectedWasmSha256 }) {
  if (poisoned) throw new ScrubberError('scrubber_instance_poisoned', 'instance_poison');
  if (compiledInitialized) throw new ScrubberError('scrubber_initialization_mismatch');
  if (!(bytes instanceof Uint8Array) || !/^[a-f0-9]{64}$/.test(expectedWasmSha256 ?? '')) throw new ScrubberError('scrubber_invalid_artifact');
  // Hash and initialize the same private snapshot across the asynchronous hash.
  const snapshot = Uint8Array.from(bytes);
  let hash;
  try { hash = await crypto.subtle.digest('SHA-256', snapshot); } catch {
    throw new ScrubberError('scrubber_artifact_verification_failure');
  }
  const digest = [...new Uint8Array(hash)].map(b => b.toString(16).padStart(2, '0')).join('');
  if (digest !== expectedWasmSha256) throw new ScrubberError('scrubber_artifact_mismatch');
  if (!initialization) {
    initializedDigest = digest;
    initialization = init({ module_or_path: snapshot });
  }
  try { await initialization; } catch {
    poisoned = true;
    throw new ScrubberError('scrubber_initialization_failure', 'instance_poison');
  }
  if (initializedDigest !== digest) throw new ScrubberError('scrubber_initialization_mismatch');
  return checked(expectedManifest);
}

/** Workers use statically imported Modules. Verify artifact bytes at build time;
 * opaque compiled Modules cannot be hashed at runtime. Source identity is checked
 * at startup. This API intentionally never pretends to verify a Module byte hash.
 */
export function loadCompiledScrubber({ module, expectedManifest }) {
  if (poisoned) throw new ScrubberError('scrubber_instance_poisoned', 'instance_poison');
  if (!(module instanceof WebAssembly.Module) || initialization || compiledInitialized) throw new ScrubberError('scrubber_invalid_module');
  compiledInitialized = true;
  try { initSync({ module }); } catch {
    poisoned = true;
    throw new ScrubberError('scrubber_initialization_failure', 'instance_poison');
  }
  return checked(expectedManifest);
}
