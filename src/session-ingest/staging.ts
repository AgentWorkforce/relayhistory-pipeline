/**
 * Staging: how a batch too wide for one Worker invocation still lands in S3
 * before the response.
 *
 * A Worker invocation has a subrequest budget, and writing a session costs
 * about five (pointer, manifest read, chunk put, manifest put, retries). So a
 * batch with more than `inlineMaxSessions` sessions to write is not written
 * session by session in the request. Instead:
 *
 *   request: one create-only, scrubbed, zstd object holding the lines to write
 *            (live-batches/v1/ws=…/m=srv_…/b=<sha32(batch digest)>-<sha16(content)>.staged.ndjson.zst)
 *            -> fan-out messages on INGEST_FANOUT (one sendBatch) -> commit -> 200
 *   queue:   each message writes up to `FANOUT_GROUPS_PER_MESSAGE` sessions from it
 *            (writeGroups, idempotent by batch digest, retried by the queue)
 *
 * Durable in S3 at acknowledgement; visible to readers seconds later, when the
 * fan-out has written the session manifests. Never a fallback to Neon.
 *
 * The object: line 1 is the header (version, batch digest, received_at, origin,
 * user, and the groups' (source, session) pairs, which the session keys carry
 * anyway); every other line is `{"g":<group>,"l":<chunk line>}`, run through
 * the pipeline scrubber (`scrubRecords`) before the put, so
 * nothing unscrubbed is at rest.
 *
 * The key names the batch digest AND the exact groups and record identities
 * staged (`sha16(content)`). A retry that keeps the same records finds its
 * own object (create-only, identical); a retry whose kept set shrank (records
 * committed meanwhile by another batch are now stale) stages a new object, so
 * the fan-out ranges, the replica's placements and the commit always describe
 * the same object.
 */
import { createHash } from "node:crypto";
import { rawZstdFrame, scrubRecords } from "@rhc/lib/pipeline/raw-chunks.ts";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import { layoutComponent } from "../shared/layout-component.ts";
import { liveMachineId, writeGroups, type Group, type LineMeta, type WriteTarget } from "./writer.ts";
import type { Ledger, LedgerRecord } from "./ledger.ts";
import { fitLine } from "./oversize.ts";

export const STAGED_VERSION = 1;
/** Sessions one fan-out message writes: well inside a queue invocation's subrequest budget. */
export const FANOUT_GROUPS_PER_MESSAGE = 40;

export interface StagedHeader {
  v: 1;
  batch_digest: string;
  received_at: string;
  org_id: string;
  workspace_id: string;
  origin_id: string;
  user_id: string;
  groups: [source: string, sessionId: string][];
}

/** One fan-out message: write groups [from, to) of the staged batch. */
export interface FanoutMessage {
  v: 1;
  kind: "ingest-fanout";
  key: string;
  from: number;
  to: number;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const sha256Hex = (value: string) => createHash("sha256").update(value).digest("hex");

/** What the object holds, independent of `received_at`: groups and each line's record identity. */
export function stagedContentDigest(groups: Group[]): string {
  const identity = groups.map((g) => [
    g.source,
    g.sessionId,
    g.lines.map((l) => {
      const r = JSON.parse(l) as { record_id: string; revision: string; operation: string };
      return [r.record_id, r.revision, r.operation];
    }),
  ]);
  return sha256Hex(JSON.stringify(identity));
}

export function stagedKey(
  keyRoot: string,
  h: Pick<StagedHeader, "org_id" | "workspace_id" | "origin_id" | "user_id" | "batch_digest">,
  contentDigest: string,
): string {
  const machine = liveMachineId(h.workspace_id, h.origin_id, h.user_id);
  return `${keyRoot}/${h.org_id}/live-batches/v1/ws=${layoutComponent(h.workspace_id)}/m=${machine}/b=${sha256Hex(h.batch_digest).slice(0, 32)}-${contentDigest.slice(0, 16)}.staged.ndjson.zst`;
}

/** Stage the batch's groups (create-only; a retry keeps the object already there). Returns its key. */
export async function stageGroups(target: WriteTarget, header: Omit<StagedHeader, "v" | "groups">, groups: Group[]): Promise<string> {
  const full: StagedHeader = { v: STAGED_VERSION, ...header, groups: groups.map((g) => [g.source, g.sessionId]) };
  const body =
    groups.flatMap((g, i) => g.lines.map((l, j) => `{"g":${i},"m":${JSON.stringify(g.meta?.[j] ?? null)},"l":${l}}`)).join("\n") + "\n";
  // Lines were fitted for the scrubber when grouped (oversize.ts checks value and literal, as the scrubber does).
  const scrubbed = scrubRecords(encoder.encode(body)).scrubbed;
  const bytes = new Uint8Array([...encoder.encode(JSON.stringify(full) + "\n"), ...scrubbed]);
  const key = stagedKey(target.keyRoot, header, stagedContentDigest(groups));
  try {
    await target.store.put(objectKey(key), rawZstdFrame(bytes), { ifNoneMatch: true, contentType: "application/zstd" });
  } catch (error) {
    // An earlier attempt staged exactly these records (the key names them): that object is this one.
    if (!isObjectStoreError(error, "already_exists") && !isObjectStoreError(error, "precondition_failed")) throw error;
  }
  return key;
}

export function fanoutMessages(key: string, groups: number): FanoutMessage[] {
  const out: FanoutMessage[] = [];
  for (let from = 0; from < groups; from += FANOUT_GROUPS_PER_MESSAGE) out.push({ v: 1, kind: "ingest-fanout", key, from, to: Math.min(groups, from + FANOUT_GROUPS_PER_MESSAGE) });
  return out;
}

/** Decode a staged object (a single zstd frame). */
export async function readStaged(store: ObjectStore, key: string, decompress: (bytes: Uint8Array) => Uint8Array): Promise<{ header: StagedHeader; groups: Group[] }> {
  const bytes = decompress(await readAll((await store.getObject(objectKey(key))).body));
  const text = decoder.decode(bytes);
  const newline = text.indexOf("\n");
  const header = JSON.parse(text.slice(0, newline)) as StagedHeader;
  if (header.v !== STAGED_VERSION || !Array.isArray(header.groups)) throw new Error("staged object has an unknown version");
  const groups: Group[] = header.groups.map(([source, sessionId]) => ({ source, sessionId, lines: [], meta: [] }));
  for (const line of text.slice(newline + 1).split("\n")) {
    if (!line) continue;
    const { g, m, l } = JSON.parse(line) as { g: number; m: LineMeta | null; l: unknown };
    const group = typeof g === "number" && Number.isInteger(g) ? groups[g] : undefined;
    if (!group || !Array.isArray(m) || m.length !== 4) throw new Error("staged line is malformed");
    group.lines.push(JSON.stringify(l));
    group.meta!.push(m);
  }
  return { header, groups };
}

/**
 * The queue stage: write one message's groups. Idempotent (manifest replay
 * dedupe by batch digest). With a ledger (always in the Worker), the slice's
 * sessions fenced now are skipped, and only after the rest are written do
 * their records become stored revisions (`commitRevisions`, which re-arms
 * erasure for a session fenced in between). So a slice that dead-letters
 * leaves its records not stale, and a later re-export writes them.
 */
export async function fanout(
  target: WriteTarget,
  message: FanoutMessage,
  decompress: (bytes: Uint8Array) => Uint8Array,
  ledger?: Pick<Ledger, "fanoutFence" | "commitRevisions">,
) {
  const { header, groups } = await readStaged(target.store, message.key, decompress);
  const tenant = { orgId: header.org_id, workspaceId: header.workspace_id, userId: header.user_id };
  // Already scrubbed once when staged: fit again for the second scrub at sealing (oversize.ts fitLine).
  let slice = groups.slice(message.from, message.to).map((g) => ({ ...g, lines: g.lines.map(fitLine) }));
  let skipped = 0;
  if (ledger) {
    const sessionsOf = slice.flatMap((g) => {
      const sid = g.meta?.[0]?.[3];
      return sid ? [[g.meta![0]![2], sid] as [string, string]] : [];
    });
    const fenced = new Set((await ledger.fanoutFence(tenant, header.origin_id, sessionsOf)).map((p) => JSON.stringify(p)));
    const before = slice.length;
    slice = slice.filter((g) => {
      const m = g.meta?.[0];
      return !(m && m[3] && fenced.has(JSON.stringify([m[2], m[3]])));
    });
    skipped = before - slice.length;
  }
  const written = await writeGroups(
    target,
    { orgId: header.org_id, workspaceId: header.workspace_id, originId: header.origin_id, userId: header.user_id, batchDigest: header.batch_digest },
    slice,
    new Date(header.received_at),
  );
  let revisions = { committed: 0, rearmed: 0 };
  if (ledger) {
    const records: LedgerRecord[] = slice.flatMap((g) =>
      g.lines.map((line, j) => {
        const r = JSON.parse(line) as { record_id: string; revision: string };
        const [revisionId, digest, source, sessionId] = g.meta![j]!;
        return { source, sessionId, recordId: r.record_id, revisionId, revision: Number(r.revision), digest };
      }),
    );
    revisions = await ledger.commitRevisions(tenant, header.origin_id, written.machineId, records);
  }
  return { ...written, skipped, revisions };
}
