/**
 * The server's raw-chunk writer: scrubs a source range of PII and secrets
 * before it reaches object storage, then compresses and stores it at its v1
 * key. It mirrors the client's `scrub::seal::put_raw_chunk` (manifest spec,
 * "Scrubbing"), so a chunk the server seals is indistinguishable from one a
 * client sealed:
 *
 *   - `start_offset`, `bytes` and `sha256` describe the **source** range, so
 *     the key, resume, epochs and the hash chain do not depend on scrubbing;
 *   - `stored_raw_bytes` and `raw_sha256` describe the scrubbed records that
 *     were stored, and `stored_bytes`/`stored_sha256` the compressed object.
 *
 * The unscrubbed source is never written anywhere, and neither it nor the
 * scrubbed records are ever logged or put in an error message.
 */
import { sha256Hex as digestHex } from "../../../../object-store/src/crypto.js";
import {
  MAX_SCRUB_CHARS,
  exceedsScrubBound,
  isCardNumber,
  scrubText,
} from "../scrub.js";
import { PipelineError } from "./errors.js";
import {
  isObjectStoreError,
  rawChunkKey,
  readAll,
  type ChunkEntry,
  type ObjectKey,
  type ObjectStore,
  type RawFormat,
  type SessionLocation,
} from "./object-layout.js";

/** `raw.scrub.ruleset` for chunks scrubbed with `lib/scrub.ts`. */
export const SCRUB_RULESET = "relayhistory-scrub";
/**
 * `raw.scrub.version` for the current `lib/scrub.ts` rules. Bump it with any
 * rule change so readers can tell which rules a stored chunk went through;
 * existing objects are never rewritten.
 */
export const SCRUB_RULESET_VERSION = 1;

/** Codes for a chunk the scrubber could not vouch for: refused, never stored. */
export const SCRUB_REFUSAL_CODES: ReadonlySet<string> = new Set([
  "chunk_not_utf8",
  "chunk_unsegmentable",
  "chunk_string_too_long",
  "chunk_stored_unscrubbed",
]);

/** Object metadata the client's `put_raw_chunk` writes; read back on a lost create race. */
export const META_RAW_SHA256 = "raw-sha256";
export const META_RAW_BYTES = "raw-bytes";
export const META_SCRUB_RULESET = "scrub-ruleset";
export const META_SCRUB_VERSION = "scrub-version";

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

/**
 * Scrub one source range record by record. A line that is a JSON object,
 * array or string is a record: every string token in it (values and keys) is scrubbed
 * with `scrubText` in place, and an integer that is a card number becomes
 * `"[REDACTED]"`; everything else (number lexemes, whitespace, key order,
 * duplicate and `__proto__` keys) is kept byte for byte, so nothing but the
 * scrubbed strings changes. Consecutive lines that are not records are
 * scrubbed together as one text, so a multi-line secret (a PEM private key)
 * is matched whole; a long run is cut into segments under `MAX_SCRUB_CHARS`
 * at line boundaries (never inside an open private-key block or between two
 * digit groups), so `scrubText` never truncates it. Line endings are kept;
 * `records` counts complete lines.
 */
export function scrubRecords(source: Uint8Array): {
  scrubbed: Uint8Array;
  records: number;
} {
  let text: string;
  try {
    text = decoder.decode(source);
  } catch {
    // Not UTF-8: refuse rather than store bytes the scrubber cannot read.
    throw new PipelineError(
      "chunk_not_utf8",
      "raw chunk is not valid UTF-8",
      false,
    );
  }
  const parts = text.split(/(\r?\n)/);
  let records = 0;
  let out = "";
  // A segment of a run of non-record lines, scrubbed as one text.
  let run = "";
  let keyOpen = false;
  const flush = () => {
    if (run) out += scrubBounded(run);
    run = "";
  };
  for (let i = 0; i < parts.length; i += 2) {
    const line = parts[i]!;
    const ending = parts[i + 1] ?? "";
    if (ending) records += 1;
    if (isRecord(line)) {
      flush();
      keyOpen = false;
      out += scrubRecordLine(line) + ending;
      continue;
    }
    const next = line + ending;
    // A segment ends before a line that opens a private key (the key then
    // starts its own segment and is matched whole). Before a line that would
    // take it past the limit, it ends at the latest safe line boundary (see
    // `safeCut`), carrying the lines after that cut into the next segment.
    if (run && !keyOpen && privateKeyOpenAfter(line, false)) flush();
    else if (run && !keyOpen && run.length + next.length > MAX_SCRUB_CHARS) {
      const cut = safeCut(run, next);
      // Every boundary in 64 KiB splits a match: refuse the chunk rather than
      // let `scrubText` truncate the segment and drop lines.
      if (!cut)
        throw new PipelineError(
          "chunk_unsegmentable",
          "raw chunk text cannot be split for scrubbing without breaking a match",
          false,
        );
      const carry = run.slice(cut);
      run = run.slice(0, cut);
      flush();
      run = carry;
    }
    run += line + ending;
    keyOpen = privateKeyOpenAfter(line, keyOpen);
  }
  flush();
  return { scrubbed: encoder.encode(out), records };
}

/** How far back `safeCut` looks for a boundary, and how much text it scrubs. */
const CUT_SEARCH_CHARS = 65_536;
const CUT_CONTEXT_CHARS = 4096;

/**
 * Where a segment may end so that `next` starts the following one: the latest
 * line boundary in the last 64 KiB of `run` that no rule matches across, or 0
 * when there is none. Rules
 * that span a newline do so with a short prefix before the secret (`Bearer`,
 * `key =`, a digit group), so the text around a boundary is scrubbed together
 * and compared with scrubbing each side alone; any difference means a match
 * crosses it.
 */
function safeCut(run: string, next: string): number {
  const floor = Math.max(1, run.length - CUT_SEARCH_CHARS);
  for (let cut = run.length; cut >= floor; ) {
    const before = run.slice(Math.max(0, cut - CUT_CONTEXT_CHARS), cut);
    const after = run.slice(cut) + next;
    const head = after.slice(0, CUT_CONTEXT_CHARS);
    if (
      !(/[\d)]\s*$/.test(before) && /^[\d(]/.test(after)) &&
      scrubText(before + head) === scrubText(before) + scrubText(head)
    )
      return cut;
    // The previous line boundary: just after the newline ending the line
    // before this one (0, below the floor, when there is none).
    cut = run.lastIndexOf("\n", cut - 2) + 1;
  }
  return 0;
}

const PEM_BEGIN = /-----BEGIN [A-Z ]*PRIVATE KEY(?: BLOCK)?-----/g;
const PEM_END = /-----END [A-Z ]*PRIVATE KEY(?: BLOCK)?-----/g;

/** Whether a private-key block is still open after `line`. */
function privateKeyOpenAfter(line: string, open: boolean) {
  const marks = [
    ...[...line.matchAll(PEM_BEGIN)].map((m) => [m.index, true] as const),
    ...[...line.matchAll(PEM_END)].map((m) => [m.index, false] as const),
  ].sort((a, b) => a[0] - b[0]);
  return marks.length ? marks[marks.length - 1]![1] : open;
}

/**
 * A line that is a JSON object, array or string. A string line is a record
 * too: scrubbing it as plain text would miss a secret behind a `\\u` escape.
 */
function isRecord(line: string) {
  const first = line.trimStart()[0];
  if (first !== "{" && first !== "[" && first !== '"') return false;
  try {
    JSON.parse(line);
    return true;
  } catch {
    return false;
  }
}

/**
 * The scrubbed value of one JSON string token. A rule can match the raw
 * literal (escapes included) but not the decoded value; re-serializing that
 * value would still store what the rule matched, so the whole string is
 * redacted instead. Unchanged means neither form matched any rule.
 */
export function scrubStringToken(
  literal: string,
  value: string,
  scrub: (text: string) => string = scrubBounded,
): string {
  const scrubbed = scrub(value);
  if (scrubbed !== value) return scrubbed;
  return scrub(literal) === literal ? value : REDACTED;
}

const REDACTED = "[REDACTED]";

/**
 * `scrubText` for stored chunks: a string past its bound would come back cut
 * short (with a note), and the chunk would be stored without the rest while
 * reporting the full source range. Refuse it instead, so the client resends
 * smaller chunks rather than skipping text that was never stored.
 */
function scrubBounded(text: string): string {
  if (exceedsScrubBound(text))
    throw new PipelineError(
      "chunk_string_too_long",
      `raw chunk holds a line or string longer than the ${MAX_SCRUB_CHARS}-character scrub limit`,
      false,
    );
  return scrubText(text);
}

/** Key names seen in one JSON object while its record is rewritten. */
interface ObjectKeys {
  /** Names written so far (after scrubbing and any suffix). */
  emitted: Set<string>;
  /** Source key names, so a duplicate the source already had stays as it was. */
  source: Set<string>;
  /** Next suffix to try per scrubbed name, so many collisions stay linear. */
  nextSuffix: Map<string, number>;
  expectKey: boolean;
}

/**
 * Rewrite the string and number tokens of one valid JSON line. The line was
 * already parsed, so tokens can be found without a full grammar: a string runs
 * to its first unescaped quote, a number is a maximal run of number chars.
 *
 * Keys follow `scrubJson`: a key whose name collides with one already written
 * in the same object gets a ` (2)`, ` (3)`… suffix, so two keys that scrub to
 * the same name both stay readable. A key the source itself repeated is kept
 * as the source had it.
 */
function scrubRecordLine(line: string): string {
  // One entry per open container; null for an array.
  const stack: (ObjectKeys | null)[] = [];
  let out = "";
  let i = 0;
  while (i < line.length) {
    const ch = line[i]!;
    const top = stack[stack.length - 1];
    if (ch === '"') {
      let j = i + 1;
      while (line[j] !== '"') j += line[j] === "\\" ? 2 : 1;
      const literal = line.slice(i, j + 1);
      const value = JSON.parse(literal) as string;
      const scrubbed = scrubStringToken(literal, value);
      let name = scrubbed;
      if (top?.expectKey) {
        const repeated = top.source.has(value);
        top.source.add(value);
        if (!repeated && top.emitted.has(name)) {
          const base = name;
          let n = top.nextSuffix.get(base) ?? 2;
          while (top.emitted.has(`${base} (${n})`)) n += 1;
          name = `${base} (${n})`;
          top.nextSuffix.set(base, n + 1);
        }
        top.emitted.add(name);
        top.expectKey = false;
      }
      out += name === value ? literal : JSON.stringify(name);
      i = j + 1;
    } else if (ch === "-" || (ch >= "0" && ch <= "9")) {
      let j = i + 1;
      while (j < line.length && /[0-9eE+.-]/.test(line[j]!)) j += 1;
      const lexeme = line.slice(i, j);
      // Checked on the digits, so a card number past 2^53 is still caught.
      out +=
        /^\d+$/.test(lexeme) && isCardNumber(lexeme)
          ? JSON.stringify(REDACTED)
          : lexeme;
      i = j;
    } else {
      if (ch === "{")
        stack.push({
          emitted: new Set(),
          source: new Set(),
          nextSuffix: new Map(),
          expectKey: true,
        });
      else if (ch === "[") stack.push(null);
      else if (ch === "}" || ch === "]") stack.pop();
      else if (ch === "," && top) top.expectKey = true;
      out += ch;
      i += 1;
    }
  }
  return out;
}

type ZstdCompress = (input: Uint8Array) => Uint8Array;
let zstd: Promise<ZstdCompress | null> | undefined;
/**
 * `node:zlib`'s zstd encoder, when this runtime has one (Node 22.15+; a
 * Worker only with a zlib that carries zstd). Feature-detected, like the
 * request-body decoder, so a runtime without it fails one write, not the load.
 */
function zstdEncoder(): Promise<ZstdCompress | null> {
  zstd ??= import("node:zlib").then(
    (zlib) =>
      typeof zlib.zstdCompressSync === "function"
        ? (input) => new Uint8Array(zlib.zstdCompressSync(input))
        : null,
    () => null,
  );
  return zstd;
}

export async function rawChunkSealingAvailable() {
  return (await zstdEncoder()) !== null;
}

export interface SealRawChunk {
  location: SessionLocation;
  format: RawFormat;
  seq: number;
  epoch: number;
  startOffset: number;
  /** The unscrubbed source range. Scrubbed here; never stored or logged as is. */
  source: Uint8Array;
  traceId?: string;
}

/** The chunk-entry fields that describe the source range, known before the put. */
interface SourceFields {
  seq: number;
  epoch: number;
  start_offset: number;
  bytes: number;
  sha256: string;
  sha256_8: string;
  compression: "zstd";
  records: number;
}

export interface SealedChunk {
  key: string;
  entry: ChunkEntry;
  /** False when an identical chunk was already stored, or another writer won the create. */
  created: boolean;
  /** `raw.scrub` to record on the manifest for this chunk. */
  scrub: { ruleset: string; version: number };
}

/**
 * Scrub, compress and store one source range at its v1 raw key. Create-only:
 * if the key already holds different bytes (a re-delivery after a rule-set
 * bump, or a concurrent writer), the stored object is kept and its entry is
 * built from the stored object's metadata and bytes, never from this write.
 */
export async function sealRawChunk(
  store: ObjectStore,
  input: SealRawChunk,
): Promise<SealedChunk> {
  if (input.source.byteLength === 0)
    throw new PipelineError("chunk_empty", "raw chunk is empty", false);
  const compress = await zstdEncoder();
  if (!compress)
    throw new PipelineError(
      "zstd_unavailable",
      "this runtime cannot zstd-compress raw chunks",
      false,
    );
  const sourceSha = await digestHex(input.source);
  const key = rawChunkKey(
    input.location,
    input.seq,
    input.startOffset,
    sourceSha,
    input.format,
  );
  const { scrubbed, records } = scrubRecords(input.source);
  const stored = compress(scrubbed);
  const base: SourceFields = {
    seq: input.seq,
    epoch: input.epoch,
    start_offset: input.startOffset,
    bytes: input.source.byteLength,
    sha256: sourceSha,
    sha256_8: sourceSha.slice(0, 8),
    compression: "zstd" as const,
    records,
  };
  const scrub = { ruleset: SCRUB_RULESET, version: SCRUB_RULESET_VERSION };
  const rawSha = await digestHex(scrubbed);
  try {
    const ack = await store.put(key, stored, {
      ifNoneMatch: true,
      metadata: {
        [META_RAW_SHA256]: rawSha,
        [META_RAW_BYTES]: String(scrubbed.byteLength),
        [META_SCRUB_RULESET]: SCRUB_RULESET,
        [META_SCRUB_VERSION]: String(SCRUB_RULESET_VERSION),
        ...(input.traceId ? { "trace-id": input.traceId } : {}),
      },
    });
    // Identical bytes already stored: they may have been stored by another
    // writer without scrub metadata, so describe the object from its own
    // metadata (and refuse it without), exactly as for different bytes.
    if (!ack.created) return keepStored(store, key, base);
    return {
      key,
      created: true,
      scrub,
      entry: {
        ...base,
        stored_bytes: ack.size,
        stored_sha256: ack.contentSha256,
        stored_raw_bytes: scrubbed.byteLength,
        raw_sha256: rawSha,
      },
    };
  } catch (error) {
    if (!isObjectStoreError(error, "already_exists")) throw error;
    return keepStored(store, key, base);
  }
}

/** The entry for the object already at `key`, from its own metadata and bytes. */
async function keepStored(
  store: ObjectStore,
  key: ObjectKey,
  base: SourceFields,
): Promise<SealedChunk> {
  const object = await store.getObject(key);
  const bytes = await readAll(object.body);
  const meta = object.meta.metadata;
  const rawSha = meta[META_RAW_SHA256];
  const rawBytes = Number(meta[META_RAW_BYTES]);
  const version = Number(meta[META_SCRUB_VERSION]);
  // An object with no scrub metadata cannot vouch for itself: never record a
  // scrub marker for it.
  if (
    !rawSha ||
    !Number.isSafeInteger(rawBytes) ||
    rawBytes < 1 ||
    !meta[META_SCRUB_RULESET] ||
    !Number.isSafeInteger(version) ||
    version < 1
  )
    throw new PipelineError(
      "chunk_stored_unscrubbed",
      "the stored chunk at this key carries no scrub metadata",
      false,
    );
  return {
    key,
    created: false,
    scrub: { ruleset: meta[META_SCRUB_RULESET]!, version },
    entry: {
      ...base,
      stored_bytes: bytes.byteLength,
      stored_sha256: await digestHex(bytes),
      stored_raw_bytes: rawBytes,
      raw_sha256: rawSha,
    },
  };
}
