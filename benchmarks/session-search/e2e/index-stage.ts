/**
 * Proof-only `search-index` step, run after session-indexed. It follows #233's ingest
 * steps on real sealed chunks. Watermarks live in memory here; #233 puts them on the
 * Supabase row. The queue stage itself does not exist yet.
 */
import { createHash } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { rawChunkKey, readAll, type ManifestV1, type ObjectStore } from "../../../src/shared/vendor.ts";
import type { SearchRecord, SessionSearchStore } from "./store.ts";

export type IndexOutcome = "indexed" | "skipped_quarantined" | "skipped_opt_out" | "skipped_stale" | "skipped_format" | "skipped_no_new";

export interface IndexDeps {
  store: ObjectStore;
  search: SessionSearchStore;
  optedOutWorkspaces: Set<string>;
  /** session key to last indexed revision and chunk count. */
  watermarks: Map<string, { revision: number; chunks: number }>;
}

/** Text a person would search for, from one transcript record. Keys are not indexed. */
export function recordText(line: string): { text: string; kind: string; ts: string | null } {
  let value: unknown;
  try { value = JSON.parse(line); } catch { return { text: line, kind: "raw", ts: null }; }
  const out: string[] = [];
  const walk = (v: unknown, depth: number) => {
    if (depth > 12) return;
    if (typeof v === "string") out.push(v);
    else if (Array.isArray(v)) for (const x of v) walk(x, depth + 1);
    else if (v && typeof v === "object") {
      for (const [k, x] of Object.entries(v)) if (!["type", "role", "id", "uuid", "timestamp", "sessionId"].includes(k)) walk(x, depth + 1);
    }
  };
  walk(value, 0);
  const o = (value ?? {}) as { type?: unknown; timestamp?: unknown };
  return { text: out.join(" "), kind: typeof o.type === "string" ? o.type : "record", ts: typeof o.timestamp === "string" ? o.timestamp : null };
}

/** `keyPrefix` is the deployment or org prefix the object store holds this session under. */
export async function indexManifest(orgId: string, m: ManifestV1, deps: IndexDeps, keyPrefix = ""): Promise<IndexOutcome> {
  const s = m.session;
  const key = `${orgId}/${s.workspace_id}/${s.harness}/${s.session_id}/${s.machine_id}`;
  const mark = deps.watermarks.get(key);
  if (mark && m.revision <= mark.revision) return "skipped_stale";
  if (!m.raw.scrub) return "skipped_quarantined";
  if (deps.optedOutWorkspaces.has(s.workspace_id)) return "skipped_opt_out";
  if (m.raw.format !== "jsonl") return "skipped_format";
  const loc = { workspaceId: s.workspace_id, machineId: s.machine_id, harness: s.harness, sessionId: s.session_id, startDate: s.started_at.slice(0, 10) };
  const fresh = m.raw.chunks.slice(mark?.chunks ?? 0);
  if (!fresh.length) { deps.watermarks.set(key, { revision: m.revision, chunks: mark?.chunks ?? 0 }); return "skipped_no_new"; }
  for (const chunk of fresh) {
    const stored = await readAll(await deps.store.get(`${keyPrefix}${rawChunkKey(loc, chunk.seq, chunk.start_offset, chunk.sha256_8, m.raw.format)}` as never));
    if (createHash("sha256").update(stored).digest("hex") !== chunk.stored_sha256) throw new Error("chunk_hash_mismatch");
    const lines = zstdDecompressSync(Buffer.from(stored)).toString("utf8").split("\n").filter(Boolean);
    const records: SearchRecord[] = lines.map((line, i) => {
      const r = recordText(line);
      return {
        orgId, workspaceId: s.workspace_id, harness: s.harness, sessionId: s.session_id, machineId: s.machine_id,
        eventId: `${s.harness}:${s.session_id}:${chunk.epoch}:${chunk.seq}:${i}`, revision: m.revision, chunkSeq: chunk.seq, recordSeq: i,
        ts: r.ts ?? s.started_at, kind: r.kind, scrubVersion: m.raw.scrub!.version, text: r.text,
      };
    });
    await deps.search.insert(records, `${rawChunkKey(loc, chunk.seq, chunk.start_offset, chunk.sha256_8, m.raw.format)}:${chunk.stored_sha256}`);
  }
  deps.watermarks.set(key, { revision: m.revision, chunks: m.raw.chunks.length });
  return "indexed";
}
