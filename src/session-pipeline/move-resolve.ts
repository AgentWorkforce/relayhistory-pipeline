/**
 * SESSION_POINTER_RESOLVE_MOVE_PENDING (default off): resolve a `pointer_move_pending` without an erasure.
 *
 * The pointer stage throws `pointer_move_pending` when the session's row names another key of the same identity
 * (another `d=` date) and that key's manifest is still live (pointer.ts). Today the only way out is the old prefix
 * being erased, which is an operator step. This module decides, under the session's pointer lock, whether one of
 * the two live copies is PROVABLY the authoritative one, so the stage can act without waiting:
 *
 *   resolved_complete   the landed key is authoritative: the row is re-keyed to it (the existing re-key path)
 *   resolved_skip       the row's key is authoritative: the landed (stale) key's event is acknowledged, row untouched
 *   still_pending:<r>   no proof: the stage fails with `pointer_move_pending` exactly as before, with reason r
 *
 * It never writes or deletes an object, and never deletes a pointer row beyond the existing re-key (which deletes
 * and rebuilds the derived row in the same lock). The stale copy stays in S3 for the erasure job.
 *
 * The proof, for the pair (landed L, stored S), and then for the authoritative copy A against every other live
 * date sibling X found by the probe:
 *   1. not_date_move     the two keys parse, and differ only in their `d=` date (same prefix, ws, m, h, s).
 *   2. identity_mismatch the `96bulls` extension's tenant and canonical ids agree where both copies carry them
 *                        (the reader already checks each manifest's session fields and start date against its key).
 *   3. writer_mismatch   same `raw.owner`, `writer.delivery_instance_id` and first epoch opener: one writer moved it.
 *   4. open_session      both copies are `raw.state: closed`: no writer is still appending to either.
 *   5. epoch_mismatch    the same epoch history (epoch, reason, opener) in both: no truncate/rewrite/backfill between.
 *   6. ambiguous_order   `updated_at` strictly orders the copies, and `raw.updated_at` does not contradict it.
 *                        A is the newer one.
 *   7. not_covered       A covers the older copy: either every one of its chunks (sha256 and length) is in A, or
 *                        both carry the exporter's `origin_revisions` and A's is >= for every origin of the older.
 *   8. multi_hop         every other live date sibling (probed +-MOVE_PROBE_DAYS around both keys) also passes 1-7
 *                        with A as the newer copy; at most MAX_EXTRA_SIBLINGS of them.
 *   9. too_many_chunks / dest_chunk_missing / dest_chunk_mismatch
 *                        every chunk A lists is present at A's prefix with the stored length, and with the
 *                        `raw-sha256` metadata equal to the manifest's `raw_sha256` (no body is read).
 *  10. no_etag / concurrent_write
 *                        a re-read of every copy returns the same ETag and version as the first read, and the row
 *                        still names S (not deleted, same revision): nothing was published meanwhile.
 * Any error while proving is `check_failed`. Synthetic, secret-free tests: __tests__/pointer-move-resolve.test.ts.
 */
import { exportWatermarks, manifestKey, parseSessionKey, rawChunkKey, type ManifestReader, type ManifestV1 } from "../shared/vendor.ts";
import type { HoopDb } from "../shared/db.ts";

/** Every reason a resolution can refuse with; the log keeps only these (shared/log.ts MOVE_RESOLUTION_VALUES). */
export const MOVE_PENDING_REASONS = [
  "not_date_move",
  "identity_mismatch",
  "writer_mismatch",
  "open_session",
  "epoch_mismatch",
  "ambiguous_order",
  "not_covered",
  "multi_hop",
  "too_many_chunks",
  "dest_chunk_missing",
  "dest_chunk_mismatch",
  "no_etag",
  "concurrent_write",
  "check_failed",
] as const;
export type MovePendingReason = (typeof MOVE_PENDING_REASONS)[number];
export type MoveResolution = "resolved_complete" | "resolved_skip" | `still_pending:${MovePendingReason}`;

/** The chunk metadata key `sealRawChunk` writes (packages/pipeline raw-chunks.ts META_RAW_SHA256). */
export const RAW_SHA256_METADATA = "raw-sha256";
/** Above this many chunks in the authoritative copy, the presence check is not attempted. */
export const MAX_VERIFIED_CHUNKS = 512;
/** More live siblings than this besides the pair: refused as multi_hop. */
export const MAX_EXTRA_SIBLINGS = 3;
/** How far either side of each key's date other siblings are probed (pointer.ts MOVE_PROBE_DAYS). */
export const RESOLVE_PROBE_DAYS = 45;
const HEAD_BATCH = 16;

export interface MoveResolveDeps {
  /** HEAD with the object's length and user metadata; undefined when there is no current object. */
  head(key: string): Promise<{ size: number; metadata: Record<string, string> } | undefined>;
}

interface Copy {
  key: string;
  read: { manifest: ManifestV1; etag?: string; version?: string };
}

export interface MoveResolveInput {
  landedKey: string;
  landed: Copy["read"];
  storedKey: string;
  stored: Copy["read"];
  /** The row's primary key (org, ws, harness, session, machine). */
  pk: unknown[];
  db: HoopDb;
  reader: ManifestReader;
  exists(key: string): Promise<boolean>;
  resolve: MoveResolveDeps;
}

export type MoveVerdict =
  | { outcome: "resolved_complete" | "resolved_skip" }
  | { outcome: "still_pending"; reason: MovePendingReason };

class Refusal extends Error {
  constructor(readonly reason: MovePendingReason) {
    super(reason);
  }
}
const refuse = (reason: MovePendingReason): never => {
  throw new Refusal(reason);
};

const ms = (v: unknown) => (typeof v === "string" ? Date.parse(v) : NaN);

function sessionExt(m: ManifestV1): Record<string, unknown> {
  const ext = m.extensions?.["96bulls"];
  return ext && typeof ext === "object" ? (ext as Record<string, unknown>) : {};
}

/** Checks 1-7 for one pair; returns [newer, older]. */
function order(a: Copy, b: Copy): [Copy, Copy] {
  const la = parseSessionKey(a.key);
  const lb = parseSessionKey(b.key);
  if (!la || !lb || la.rest !== "manifest.json" || lb.rest !== "manifest.json") return refuse("not_date_move");
  if ((la.prefix ?? "") !== (lb.prefix ?? "") || la.workspaceId !== lb.workspaceId || la.machineId !== lb.machineId || la.harness !== lb.harness || la.sessionId !== lb.sessionId || la.startDate === lb.startDate)
    refuse("not_date_move");
  const ma = a.read.manifest;
  const mb = b.read.manifest;
  const ea = sessionExt(ma);
  const eb = sessionExt(mb);
  for (const f of ["tenant_id", "canonical_session_id", "canonical_machine_id"])
    if (ea[f] !== undefined && eb[f] !== undefined && ea[f] !== eb[f]) refuse("identity_mismatch");
  if (ma.raw.owner !== mb.raw.owner || ma.writer.delivery_instance_id !== mb.writer.delivery_instance_id || ma.raw.epochs[0]?.opened_by !== mb.raw.epochs[0]?.opened_by)
    refuse("writer_mismatch");
  if (ma.raw.state !== "closed" || mb.raw.state !== "closed") refuse("open_session");
  const epochs = (m: ManifestV1) => JSON.stringify(m.raw.epochs.map((e) => [e.epoch, e.reason, e.opened_by, e.resumes_epoch ?? null]));
  if (ma.raw.epoch !== mb.raw.epoch || epochs(ma) !== epochs(mb)) refuse("epoch_mismatch");
  const ua = ms(ma.updated_at);
  const ub = ms(mb.updated_at);
  if (!Number.isFinite(ua) || !Number.isFinite(ub) || ua === ub) refuse("ambiguous_order");
  const [newer, older] = ua > ub ? [a, b] : [b, a];
  if (!(ms(newer.read.manifest.raw.updated_at) >= ms(older.read.manifest.raw.updated_at))) refuse("ambiguous_order");
  if (!covers(newer.read.manifest, older.read.manifest)) refuse("not_covered");
  return [newer, older];
}

/** Check 7: the newer copy holds the older one's content, or its source watermarks dominate. */
function covers(newer: ManifestV1, older: ManifestV1): boolean {
  if (newer.raw.format === older.raw.format) {
    const held = new Set(newer.raw.chunks.map((c) => `${c.sha256}:${c.bytes}`));
    if (older.raw.chunks.length > 0 && older.raw.chunks.every((c) => held.has(`${c.sha256}:${c.bytes}`))) return true;
  }
  const wo = exportWatermarks(older);
  const wn = exportWatermarks(newer);
  const origins = Object.keys(wo);
  if (origins.length === 0) return false;
  return origins.every((o) => typeof wn[o] === "number" && typeof wo[o] === "number" && wn[o]! >= wo[o]!);
}

/** Every other date's manifest key within RESOLVE_PROBE_DAYS of either key, nearest first, without the pair. */
function siblingCandidates(keys: string[]): string[] {
  const out: string[] = [];
  const seen = new Set(keys);
  for (const key of keys) {
    const loc = parseSessionKey(key)!;
    const base = Date.parse(`${loc.startDate}T00:00:00Z`);
    for (let d = 1; d <= RESOLVE_PROBE_DAYS; d++)
      for (const s of [-d, d]) {
        const k = manifestKey({ ...loc, startDate: new Date(base + s * 86_400_000).toISOString().slice(0, 10) });
        if (!seen.has(k)) {
          seen.add(k);
          out.push(k);
        }
      }
  }
  return out;
}

async function inBatches<T, R>(items: T[], size: number, f: (t: T) => Promise<R>): Promise<R[]> {
  const out: R[] = [];
  for (let i = 0; i < items.length; i += size) out.push(...(await Promise.all(items.slice(i, i + size).map(f))));
  return out;
}

/** Check 9: every chunk of `copy` present at its own prefix with the stored length and digest metadata. */
async function verifyChunks(copy: Copy, resolve: MoveResolveDeps): Promise<void> {
  const m = copy.read.manifest;
  if (m.raw.chunks.length > MAX_VERIFIED_CHUNKS) refuse("too_many_chunks");
  const loc = parseSessionKey(copy.key)!;
  const results = await inBatches(m.raw.chunks, HEAD_BATCH, async (c) => {
    const head = await resolve.head(String(rawChunkKey(loc, c.seq, c.start_offset, c.sha256, m.raw.format)));
    if (!head) return "dest_chunk_missing" as const;
    if (head.size !== c.stored_bytes) return "dest_chunk_mismatch" as const;
    if (c.raw_sha256 !== undefined && head.metadata[RAW_SHA256_METADATA] !== c.raw_sha256) return "dest_chunk_mismatch" as const;
    return undefined;
  });
  if (results.includes("dest_chunk_missing")) refuse("dest_chunk_missing");
  if (results.includes("dest_chunk_mismatch")) refuse("dest_chunk_mismatch");
}

async function storedRow(input: MoveResolveInput) {
  return (
    await input.db.query<{ manifest_key: string; deleted: boolean; revision: string }>(
      `SELECT manifest_key, deleted_at IS NOT NULL AS deleted, revision::text AS revision FROM sessions.session_index
        WHERE org_id = $1 AND workspace_id = $2 AND harness = $3 AND session_id = $4 AND machine_id = $5`,
      input.pk,
    )
  ).rows[0];
}

async function prove(input: MoveResolveInput): Promise<MoveVerdict> {
  const landed: Copy = { key: input.landedKey, read: input.landed };
  const stored: Copy = { key: input.storedKey, read: input.stored };
  const before = await storedRow(input);
  if (!before || before.deleted || before.manifest_key !== stored.key) refuse("concurrent_write");
  const [authority] = order(landed, stored);

  // Check 8: other live date siblings of the same identity (a multi-hop chain).
  const candidates = siblingCandidates([landed.key, stored.key]);
  const live = (await inBatches(candidates, 10, async (k) => ((await input.exists(k)) ? k : undefined))).filter((k): k is string => k !== undefined);
  if (live.length > MAX_EXTRA_SIBLINGS) refuse("multi_hop");
  const extras: Copy[] = [];
  for (const key of live) {
    const read = await input.reader.read(key);
    if (!read) continue; // gone since the HEAD: not live
    const other: Copy = { key, read };
    let newer: Copy;
    try {
      [newer] = order(authority, other);
    } catch (error) {
      if (error instanceof Refusal) refuse("multi_hop");
      throw error;
    }
    if (newer !== authority) refuse("multi_hop");
    extras.push(other);
  }

  // Check 9: the authoritative copy is complete where it lives.
  await verifyChunks(authority, input.resolve);

  // Check 10: nothing changed while proving.
  for (const copy of [landed, stored, ...extras]) {
    if (!copy.read.etag && !copy.read.version) refuse("no_etag");
    const again = await input.reader.read(copy.key);
    if (!again || again.etag !== copy.read.etag || again.version !== copy.read.version) refuse("concurrent_write");
  }
  const after = await storedRow(input);
  if (!after || after.deleted || after.manifest_key !== stored.key || after.revision !== before!.revision) refuse("concurrent_write");

  return { outcome: authority === landed ? "resolved_complete" : "resolved_skip" };
}

/** Never throws: a refusal or an error while proving is a still_pending verdict. */
export async function resolveMovePending(input: MoveResolveInput): Promise<MoveVerdict> {
  try {
    return await prove(input);
  } catch (error) {
    if (error instanceof Refusal) return { outcome: "still_pending", reason: error.reason };
    return { outcome: "still_pending", reason: "check_failed" };
  }
}

export function moveResolutionOf(verdict: MoveVerdict): MoveResolution {
  return verdict.outcome === "still_pending" ? `still_pending:${verdict.reason}` : verdict.outcome;
}
