/**
 * Bob Costas's streamed read of a session's scrubbed chunks into a digest.
 *
 * The summary needs a digest (counts, user turns, tools, models, span), never the whole
 * transcript held at once. So the chunks are streamed one at a time into a `DigestBuilder`
 * and each is let go once fed; delivery envelopes are folded as they arrive (one seat per
 * record). This is always on, and with sampling off the digest is exactly the one the
 * whole-session read built.
 *
 *   BOB_COSTAS_INPUT_SAMPLING   off (default) | on. Off: every chunk is read and the
 *                               digest is unchanged. On: a transcript session past the
 *                               caps below is read from a sample of its chunks (the first
 *                               and the last, interleaved, then an evenly spaced selection
 *                               between), user turns are sampled and clipped, tool names
 *                               and models capped, and envelope text budgeted
 *                               (`summarizer.ts`). A session of delivery envelopes is always
 *                               read whole, so every revision and tombstone is folded.
 *   BOB_COSTAS_MAX_CHUNKS       512 chunks read per summary (sampling on).
 *   BOB_COSTAS_MAX_INPUT_BYTES  256 MiB of decompressed chunk bytes read per summary
 *                               (sampling on); never below BOB_COSTAS_MAX_CHUNK_BYTES.
 *   BOB_COSTAS_MAX_CHUNK_BYTES  32 MiB per chunk, decompressed, always: a bigger chunk is
 *                               never inflated past the cap. With sampling off it is never
 *                               below 24 MiB (the ingest writes at most 16 MiB a chunk).
 *
 * A cap that makes the counts lower bounds is recorded in `digest.input`, and the record carries
 * `input_truncated`. A capped read that yields no record, or a session of envelopes that
 * could not be read whole, clears the summary and then fails (non-retryable) instead of
 * publishing (handler.ts `failAfterPublish`).
 *
 * Sized for a 4 GiB pod: the worker handles one message at a time (`runWorker` starts each
 * claimed delivery in turn), so a summary's peak is one chunk inflated (≤ 32 MiB) and its
 * string, plus the digest's state. Values outside a cap's range fall back to the default.
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
import { DigestBuilder, type SessionDigest } from "./summarizer.ts";

export interface SummaryInputCaps {
  sampling: boolean;
  maxChunks: number;
  maxInputBytes: number;
  maxChunkBytes: number;
}

const MiB = 1024 * 1024;

export const DEFAULT_SUMMARY_INPUT_CAPS: Readonly<SummaryInputCaps> = Object.freeze({
  sampling: false,
  maxChunks: 512,
  maxInputBytes: 256 * MiB,
  maxChunkBytes: 32 * MiB,
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
  const maxChunkBytes = int(env, "BOB_COSTAS_MAX_CHUNK_BYTES", d.maxChunkBytes, 64 * 1024, 256 * MiB);
  return {
    sampling: ["on", "true", "1"].includes((env.BOB_COSTAS_INPUT_SAMPLING ?? "").trim().toLowerCase()),
    maxChunks: int(env, "BOB_COSTAS_MAX_CHUNKS", d.maxChunks, 2, 100_000),
    // A budget below one chunk would refuse every chunk the per-chunk cap admits.
    maxInputBytes: Math.max(maxChunkBytes, int(env, "BOB_COSTAS_MAX_INPUT_BYTES", d.maxInputBytes, MiB, 2_048 * MiB)),
    maxChunkBytes,
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

/** `a[0], b[0], a[1], b[1], ...`, then what is left of the longer. */
function interleave(a: number[], b: number[]): number[] {
  const out: number[] = [];
  for (let i = 0; i < Math.max(a.length, b.length); i++) {
    if (i < a.length) out.push(a[i]!);
    if (i < b.length) out.push(b[i]!);
  }
  return out;
}

/**
 * Which chunks to read (sampling on), ascending. Every chunk when the session is within the
 * caps by its declared sizes. Otherwise a chunk budget of `maxChunks`, or what
 * `maxInputBytes` holds at the session's mean chunk size if smaller: a quarter from the
 * head and a quarter from the tail, taken alternately from the first chunk forwards and
 * the last backwards, so the byte budget never starves the tail; then the rest evenly
 * spaced between, alternately from each end. A chunk is taken only while its declared size
 * fits the byte budget left; one declared over `maxChunkBytes` is never read.
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
  const tail = spread(Math.max(head.length, eligible.length - edge), eligible.length, edge).reverse();
  const middle = spread(head.length, eligible.length - tail.length, k - head.length - tail.length);
  const middleFromEnds = interleave(middle.slice(0, Math.ceil(middle.length / 2)), middle.slice(Math.ceil(middle.length / 2)).reverse());
  const picked: number[] = [];
  let budget = caps.maxInputBytes;
  for (const at of [...interleave(head, tail), ...middleFromEnds]) {
    const index = eligible[at]!;
    const size = declared(chunks[index]!);
    if (size > budget) continue;
    budget -= size;
    picked.push(index);
  }
  picked.sort((a, b) => a - b);
  return { indices: picked, sampled: oversized || picked.length < chunks.length };
}

interface ReadArgs {
  manifest: ManifestV1;
  location: Parameters<typeof rawChunkKey>[0];
  store: ObjectStore;
  readerMaxRuleset?: number;
  caps?: SummaryInputCaps;
}

interface Pass {
  builder: DigestBuilder;
  chunksRead: number;
  chunksSkipped: number;
  bytesRead: number;
}

/**
 * Streams the session's chunks into a digest within `caps`. Throws as the whole-session
 * read did for a missing chunk (`chunk_missing`, retryable) and for a chunk that is not
 * zstd; throws `summary_input_incomplete` (non-retryable) for a session of delivery
 * envelopes with a chunk it could not read, since a missed tombstone or later revision
 * could surface content the session no longer has.
 */
export async function readSessionDigest(args: ReadArgs): Promise<SessionDigest> {
  const caps = args.caps ?? DEFAULT_SUMMARY_INPUT_CAPS;
  const all = args.manifest.raw.chunks.map((_, i) => i);
  let pass = caps.sampling ? await readPass(args, caps, planChunks(args.manifest.raw.chunks, caps).indices, true) : null;
  // Envelopes found after the sample skipped a chunk: read the session whole instead.
  if (pass === null || (pass.builder.hasEnvelopes && pass.chunksSkipped > 0))
    pass = await readPass(args, caps, all, false);
  if (pass.builder.hasEnvelopes && pass.chunksSkipped > 0)
    throw new PipelineError("summary_input_incomplete", "a delivery-envelope session has a chunk over the per-chunk cap", false);
  const { builder } = pass;
  const digest = builder.finish();
  // Only caps that make the counts lower bounds; turn sampling is `digest.userTurnsSampled`.
  if (pass.chunksSkipped > 0 || builder.namesDropped > 0)
    digest.input = {
      chunksTotal: args.manifest.raw.chunks.length,
      chunksRead: pass.chunksRead,
      bytesRead: pass.bytesRead,
      namesDropped: builder.namesDropped,
    };
  return digest;
}

/**
 * The per-chunk inflate cap in force. Sampling on: BOB_COSTAS_MAX_CHUNK_BYTES. Sampling off:
 * never below UNSAMPLED_CHUNK_FLOOR, so it can only refuse a chunk the ingest could not have
 * written (at most 16 MiB) and the read matches the whole-session read.
 */
const UNSAMPLED_CHUNK_FLOOR = 24 * MiB;
export const perChunkCap = (caps: SummaryInputCaps) =>
  caps.sampling ? caps.maxChunkBytes : Math.max(caps.maxChunkBytes, UNSAMPLED_CHUNK_FLOOR);

/**
 * One pass over the chunks in `selected` (ascending), or, once the session shows delivery
 * envelopes before any chunk was skipped, over every chunk from there on. `budgeted`: the
 * byte budget applies until then; a chunk whose declared size no longer fits is not
 * fetched, and none is inflated past what is left.
 */
async function readPass(args: ReadArgs, caps: SummaryInputCaps, selected: number[], budgeted: boolean): Promise<Pass> {
  const m = args.manifest;
  const builder = new DigestBuilder(m.session.harness, { sampling: caps.sampling });
  const pass: Pass = { builder, chunksRead: 0, chunksSkipped: 0, bytesRead: 0 };
  // The byte budget is spent in chunk order, not in the plan's priority order: the plan
  // already fits its picks to the budget by their declared sizes (head and tail first), so
  // in-order spending only bites when a size is undeclared or understated, and then the
  // later chunks (the tail) are the ones skipped.
  const chunkCap = perChunkCap(caps);
  const wanted = new Set(selected);
  let whole = !budgeted;
  // Set once a chunk overran what was left of the budget: nothing further is fetched.
  let spent = false;
  for (let index = 0; index < m.raw.chunks.length; index++) {
    if (!whole && builder.hasEnvelopes && pass.chunksSkipped === 0) whole = true;
    const remaining = caps.maxInputBytes - pass.bytesRead;
    const chunk = m.raw.chunks[index]!;
    if (!whole && (spent || !wanted.has(index) || remaining <= 0 || declared(chunk) > remaining)) {
      pass.chunksSkipped += 1;
      continue;
    }
    const key = rawChunkKey(args.location, chunk.seq, chunk.start_offset, chunk.sha256_8, m.raw.format);
    const limit = whole ? chunkCap : Math.min(chunkCap, remaining);
    const body = await inflateChunk(args.store, key, m, args.readerMaxRuleset, limit);
    if (body === null) {
      if (!whole && limit < chunkCap) spent = true;
      pass.chunksSkipped += 1;
      continue;
    }
    pass.chunksRead += 1;
    pass.bytesRead += body.bytes;
    feedLines(builder, body.text);
  }
  return pass;
}

/** One chunk, fetched and inflated; null when it would inflate past `maxBytes` (not read). */
async function inflateChunk(
  store: ObjectStore,
  key: ReturnType<typeof rawChunkKey>,
  m: ManifestV1,
  readerMaxRuleset: number | undefined,
  maxBytes: number,
): Promise<{ text: string; bytes: number } | null> {
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
  return { text: raw.toString("utf8"), bytes: raw.length };
}

/** Feeds each line of `body` to the builder, as the whole-session read split it. */
function feedLines(builder: DigestBuilder, body: string): void {
  let start = 0;
  while (start <= body.length) {
    let end = body.indexOf("\n", start);
    if (end === -1) end = body.length;
    if (end > start) builder.addLine(body.slice(start, end));
    start = end + 1;
  }
}
