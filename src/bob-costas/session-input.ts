/**
 * Bob Costas's bounded read of a session's scrubbed chunks into a digest.
 *
 * The summary needs a digest (counts, the first/last/sampled user turns, tools, models,
 * span), never the whole transcript. So the chunks are streamed one at a time into a
 * `DigestBuilder` and each is let go once fed: the resident set is one chunk plus the
 * builder's bounded state, whatever the session's chunk count or size. A session past
 * the caps is read from a sample of its chunks (the first, the last, and an evenly
 * spaced selection between), and the digest says so (`digest.input`), which the summary
 * record carries as `input_truncated`.
 *
 *   BOB_COSTAS_MAX_CHUNKS       512 chunks read per summary.
 *   BOB_COSTAS_MAX_INPUT_BYTES  256 MiB of decompressed chunk bytes read per summary.
 *   BOB_COSTAS_MAX_CHUNK_BYTES  32 MiB per chunk, decompressed (the ingest writes at most
 *                               16 MiB); a bigger chunk is skipped, never inflated.
 *   BOB_COSTAS_MAX_LINE_BYTES   2 MiB per transcript line; a longer line is counted as a
 *                               record and not parsed.
 *   BOB_COSTAS_MAX_ENVELOPES    200000 delivery envelopes held for the settle.
 *
 * Sized for a 4 GiB pod: the worker handles one message at a time (`runWorker` starts each
 * claimed delivery in turn), so a summary's peak is about one chunk inflated (≤ 32 MiB, and
 * its string and line slices), the held envelopes (compact: ids, revision, a few payload
 * fields, user text within `ENVELOPE_TEXT_BUDGET_CHARS`), and RETAINED_USER_TURNS turns:
 * a few hundred MiB at most. Values outside a cap's range fall back to the default.
 */
import { zstdDecompressSync } from "node:zlib";
import {
  PipelineError,
  rawChunkKey,
  readAll,
  type ManifestV1,
  type ObjectStore,
} from "../shared/vendor.ts";
import { readerChunkBody } from "../../packages/pipeline/src/lib/scrub-engine/reader-policy.ts";
import { DigestBuilder, DEFAULT_MAX_ENVELOPES, type SessionDigest } from "./summarizer.ts";

export interface SummaryInputCaps {
  maxChunks: number;
  maxInputBytes: number;
  maxChunkBytes: number;
  maxLineBytes: number;
  maxEnvelopes: number;
}

const MiB = 1024 * 1024;

export const DEFAULT_SUMMARY_INPUT_CAPS: Readonly<SummaryInputCaps> = Object.freeze({
  maxChunks: 512,
  maxInputBytes: 256 * MiB,
  maxChunkBytes: 32 * MiB,
  maxLineBytes: 2 * MiB,
  maxEnvelopes: DEFAULT_MAX_ENVELOPES,
});

function int(env: NodeJS.ProcessEnv, name: string, fallback: number, min: number, max: number): number {
  const raw = env[name]?.trim();
  if (!raw || !/^\d{1,10}$/.test(raw)) return fallback;
  const value = Number(raw);
  return value >= min && value <= max ? value : fallback;
}

/** The caps from env; a missing or out-of-range value keeps its default. */
export function summaryInputCapsFromEnv(env: NodeJS.ProcessEnv = process.env): SummaryInputCaps {
  const d = DEFAULT_SUMMARY_INPUT_CAPS;
  return {
    maxChunks: int(env, "BOB_COSTAS_MAX_CHUNKS", d.maxChunks, 2, 100_000),
    maxInputBytes: int(env, "BOB_COSTAS_MAX_INPUT_BYTES", d.maxInputBytes, MiB, 2_048 * MiB),
    maxChunkBytes: int(env, "BOB_COSTAS_MAX_CHUNK_BYTES", d.maxChunkBytes, 64 * 1024, 256 * MiB),
    maxLineBytes: int(env, "BOB_COSTAS_MAX_LINE_BYTES", d.maxLineBytes, 4 * 1024, 64 * MiB),
    maxEnvelopes: int(env, "BOB_COSTAS_MAX_ENVELOPES", d.maxEnvelopes, 1_000, 10_000_000),
  };
}

type PlannedChunk = Pick<ManifestV1["raw"]["chunks"][number], "bytes"> & { stored_raw_bytes?: number };

/** A chunk's declared decompressed size; 0 when the manifest does not say. */
const declared = (c: PlannedChunk) => Math.max(Number(c.bytes) || 0, Number(c.stored_raw_bytes) || 0);

/** `n` indices evenly spaced over `[from, to)`, ascending, distinct. */
function spread(from: number, to: number, n: number): number[] {
  const len = to - from;
  if (n <= 0 || len <= 0) return [];
  if (n >= len) return Array.from({ length: len }, (_, i) => from + i);
  if (n === 1) return [from + Math.floor(len / 2)];
  return Array.from({ length: n }, (_, i) => from + Math.round((i * (len - 1)) / (n - 1)));
}

/**
 * Which chunks to read, ascending by position in `chunks`. Every chunk when the session
 * is within the caps (by its declared sizes). Otherwise the first quarter of the chunk
 * budget from the head, a quarter from the tail and the rest evenly spaced between, the
 * budget being `maxChunks` or what `maxInputBytes` holds at the session's mean chunk size,
 * whichever is smaller; then, head first, tail next, middle last, only what still fits
 * the byte budget. A chunk declared over `maxChunkBytes` is never read.
 */
export function planChunks(chunks: readonly PlannedChunk[], caps: SummaryInputCaps): { indices: number[]; sampled: boolean } {
  const eligible: number[] = [];
  let total = 0;
  for (let i = 0; i < chunks.length; i++) {
    const size = declared(chunks[i]!);
    if (size > caps.maxChunkBytes) continue;
    eligible.push(i);
    total += size;
  }
  const oversized = eligible.length < chunks.length;
  if (!oversized && eligible.length <= caps.maxChunks && total <= caps.maxInputBytes)
    return { indices: eligible, sampled: false };
  const mean = eligible.length ? total / eligible.length : 0;
  const byBytes = mean > 0 ? Math.floor(caps.maxInputBytes / mean) : Infinity;
  const k = Math.max(1, Math.min(caps.maxChunks, eligible.length, byBytes));
  const edge = Math.max(1, Math.floor(k / 4));
  const head = spread(0, Math.min(edge, eligible.length), edge);
  const tail = spread(Math.max(head.length, eligible.length - edge), eligible.length, edge);
  const middle = spread(head.length, eligible.length - tail.length, k - head.length - tail.length);
  const picked: number[] = [];
  let budget = caps.maxInputBytes;
  for (const at of [...head, ...tail, ...middle]) {
    const index = eligible[at]!;
    const size = declared(chunks[index]!);
    if (size > budget) continue;
    budget -= size;
    picked.push(index);
  }
  picked.sort((a, b) => a - b);
  return { indices: picked, sampled: oversized || picked.length < chunks.length };
}

/**
 * Streams the session's chunks into a digest within `caps`. Throws as the unbounded read
 * did for a missing chunk (`chunk_missing`, retryable) and for a chunk that is not zstd.
 */
export async function readSessionDigest(args: {
  manifest: ManifestV1;
  location: Parameters<typeof rawChunkKey>[0];
  store: ObjectStore;
  readerMaxRuleset?: number;
  caps?: SummaryInputCaps;
}): Promise<SessionDigest> {
  const { manifest: m, store } = args;
  const caps = args.caps ?? DEFAULT_SUMMARY_INPUT_CAPS;
  const plan = planChunks(m.raw.chunks, caps);
  const builder = new DigestBuilder(m.session.harness, { maxEnvelopes: caps.maxEnvelopes });
  let sampled = plan.sampled;
  let chunksRead = 0;
  let bytesRead = 0;
  let linesSkipped = 0;
  for (const index of plan.indices) {
    const chunk = m.raw.chunks[index]!;
    const key = rawChunkKey(args.location, chunk.seq, chunk.start_offset, chunk.sha256_8, m.raw.format);
    const text = await inflateChunk(store, key, m, args.readerMaxRuleset, caps.maxChunkBytes);
    // Over the per-chunk cap (an undeclared or misdeclared size), or past the byte budget.
    if (text === null || bytesRead + text.bytes > caps.maxInputBytes) {
      sampled = true;
      continue;
    }
    chunksRead += 1;
    bytesRead += text.bytes;
    linesSkipped += feedLines(builder, text.body, caps.maxLineBytes);
  }
  const digest = builder.finish();
  if (sampled || linesSkipped > 0 || builder.envelopesDropped > 0)
    digest.input = {
      chunksTotal: m.raw.chunks.length,
      chunksRead,
      bytesRead,
      linesSkipped,
      envelopesDropped: builder.envelopesDropped,
    };
  return digest;
}

/** One chunk, fetched and inflated; null when it inflates past `maxBytes` (not read). */
async function inflateChunk(
  store: ObjectStore,
  key: ReturnType<typeof rawChunkKey>,
  m: ManifestV1,
  readerMaxRuleset: number | undefined,
  maxBytes: number,
): Promise<{ body: string; bytes: number } | null> {
  let stored: Uint8Array;
  try {
    // Unset maximum: the unchanged body-only GET. Set: one GET binds the chunk's scrub metadata to its body.
    stored = await readAll(readerMaxRuleset === undefined
      ? await store.get(key)
      : await readerChunkBody(await store.getObject(key), m.raw.scrub!, readerMaxRuleset));
  } catch (err) {
    if ((err as { code?: string }).code === "not_found")
      throw new PipelineError("chunk_missing", "a listed chunk is not in object storage", true);
    throw err;
  }
  let raw: Buffer;
  try {
    raw = zstdDecompressSync(stored, { maxOutputLength: maxBytes });
  } catch (err) {
    if ((err as { code?: string }).code === "ERR_BUFFER_TOO_LARGE") return null;
    throw err;
  }
  return { body: raw.toString("utf8"), bytes: raw.length };
}

/** Feeds each line of `body` to the builder; returns how many were over `maxLineBytes`. */
function feedLines(builder: DigestBuilder, body: string, maxLineBytes: number): number {
  let skipped = 0;
  let start = 0;
  while (start <= body.length) {
    let end = body.indexOf("\n", start);
    if (end === -1) end = body.length;
    // Measured in UTF-16 code units, not UTF-8 bytes: within 3x of the byte length either way.
    if (end - start > maxLineBytes) {
      builder.countUnparsed();
      skipped += 1;
    } else if (end > start) {
      builder.addLine(body.slice(start, end));
    }
    start = end + 1;
  }
  return skipped;
}
