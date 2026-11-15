/** Synthetic, secret-free streamed-reconcile fixtures (issue 610 phase 2): manifests in an fs object store. */
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as zlib from "node:zlib";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import type { ObjectStore } from "../../../packages/object-store/src/index.ts";
import { objectStoreManifestReader, type ManifestReader } from "../../../packages/pipeline/src/lib/pipeline/adapter.ts";
import type { RelayhistoryStreamRecord } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { exportUnit } from "../../migration/neon-s3-sync/export-session.ts";
import type { SessionUnit } from "../../migration/neon-s3-sync/neon-reader.ts";
import { canonicalManifestIdentity } from "../../shared/manifest-identity.ts";

export const ID = { orgId: "org_stream", workspaceId: "ws_stream", source: "claude", sessionId: "stream:parity" };
/** The physical scope: the layout component of the canonical session id. */
export const SCOPE = { orgId: ID.orgId, workspaceId: ID.workspaceId, harness: ID.source, sessionId: "stream_parity" };
export const T0 = Date.parse("2026-11-01T10:00:00.000Z");
export const USER = "user_stream";
export const prefixFor = (org: string) => `96bulls/${org}`;
export const decompress = (stored: Uint8Array, maxOutputLength: number) =>
  new Uint8Array(zlib.zstdDecompressSync(stored, { maxOutputLength }));

export function prng(seed: number) {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export type Line = { machine: string; record: RelayhistoryStreamRecord };
export const iso = (ms: number) => new Date(ms).toISOString();

/** A direct convergence line of `machine`'s manifest. */
export function direct(machine: string, id: string, ms: number, fields: Record<string, unknown> = {}, revision = "1"): Line {
  const at = iso(ms);
  return {
    machine,
    record: {
      kind: "convergence_event",
      origin_id: `machine:${machine}`,
      record_id: id,
      revision,
      operation: "upsert",
      received_at: at,
      payload: {
        source: ID.source, session_id: ID.sessionId, machine_id: machine, user_id: USER, event_id: id,
        kind: "session_event", type: "assistant", ts: at, ingested_at: at, actor_name: "StreamAgent",
        actor_role: "owner", content: `direct ${id} r${revision}`, model: "fixture-model", provider: "fixture",
        input_tokens: 3, output_tokens: 5, task_ref: {}, tags: ["stream"], files_touched: [], tool_calls: [],
        ...fields,
      },
    },
  };
}

/** A delivery (activity) record carried by `machine`'s manifest. */
export function activity(machine: string, origin: string, kind: string, id: string, ms: number, payload: Record<string, unknown>, revision = "1", seq?: number): Line {
  return {
    machine,
    record: {
      kind, origin_id: origin, record_id: id, revision, operation: "upsert", received_at: iso(ms),
      ...(seq === undefined ? {} : { accepted_seq: String(seq) }),
      payload: { ts_ms: ms, ...payload },
    },
  };
}

/** A random session over several manifests: revisions, deletes, copies in other manifests, time ties. */
export function generate(seed: number): Line[] {
  const random = prng(seed);
  const pick = <T,>(values: readonly T[]): T => values[Math.floor(random() * values.length)]!;
  const lanes = 1 + Math.floor(random() * 3);
  const machines = Array.from({ length: lanes }, (_, l) => `machine:${l}`);
  const out: Line[] = [];
  let seq = 1;
  const size = 10 + Math.floor(random() * 60);
  for (let i = 0; i < size; i++) {
    const machine = pick(machines);
    const ms = T0 + Math.floor(random() * 8) * 1000; // many equal timestamps
    const type = pick(["direct", "direct", "user", "assistant", "tool", "history", "edit", "catalog"]);
    const copies: Line[] = [];
    const revisions = random() < 0.3 ? ["1", "2"] : ["1"];
    for (const revision of revisions) {
      let line: Line;
      if (type === "direct")
        line = direct(machine, `direct-${seed}-${i}`, ms, i % 5 === 0 ? { type: "user", actor_role: "user", content: `prompt ${i} r${revision}` } : { content: `answer ${i} r${revision}` }, revision);
      else if (type === "catalog")
        line = activity(machine, `capacity:${machine}`, "session_marker", `marker-${i}`, ms, { marker_uid: `mk${i % 3}`, kind: "note", text: `n${revision}` }, revision, seq++);
      else {
        const kind = type === "tool" ? "tool_call" : type === "history" ? "history" : type === "edit" ? "file_edit" : "session_event";
        const payload =
          type === "user" ? { role: "user", text: `hello ${i} r${revision}`, message_id: `msg-${i}` }
          : type === "assistant" ? { role: "assistant", text: `reply ${i} r${revision}`, message_id: `msg-${i}`, model: "fixture-model" }
          : type === "tool" ? { name: "Read", tool_use_id: `tu-${i}`, input: { path: "src/a.ts" } }
          : type === "history" ? { text: `history ${i} r${revision}` }
          : { file_path: "src/a.ts", tool_use_id: `tu-${i}` };
        line = activity(machine, `capacity:${machine}`, kind, `event-${seed}-${i}`, ms, payload, revision, seq++);
      }
      copies.push(line);
      // Sometimes the same copy is also carried by another manifest.
      if (lanes > 1 && random() < 0.15) copies.push({ ...line, machine: pick(machines) });
    }
    if (random() < 0.08) {
      const last = copies.at(-1)!;
      copies.push({ machine: last.machine, record: { ...last.record, revision: "9", operation: "delete", payload: null } });
    }
    // Copies arrive in any order.
    for (let j = copies.length - 1; j > 0; j--) {
      const k = Math.floor(random() * (j + 1));
      [copies[j], copies[k]] = [copies[k]!, copies[j]!];
    }
    out.push(...copies);
  }
  return out;
}

export interface ExportedScope {
  store: ObjectStore;
  reader: ManifestReader;
  keys: string[];
  canonicalId: string;
  dispose(): Promise<void>;
}

/** Exports `lines` (one manifest per machine) into a temporary fs store. */
export async function exportStreamScope(lines: Line[], maxChunkBytes = 1500, id: typeof ID = ID): Promise<ExportedScope> {
  const root = await mkdtemp(join(tmpdir(), "stream-parity-"));
  const store = createFsStore({ root });
  const unit: SessionUnit = {
    orgId: id.orgId, workspaceId: id.workspaceId, source: id.source, sessionId: id.sessionId,
    firstMs: T0, lastMs: T0 + 60_000, machineIds: [], totals: {},
  };
  const encoder = new TextEncoder();
  const exported = await exportUnit({ store, keyRoot: "96bulls", grantId: "stream_parity", maxChunkBytes }, unit, {
    fenced: async () => false,
    async *lines() {
      for (const { machine, record } of lines)
        yield { machine, userId: USER, bytes: encoder.encode(JSON.stringify(record) + "\n") };
    },
  });
  const keys = exported.manifests.map((m) => m.manifestKey);
  const reader = objectStoreManifestReader(store, { provider: "s3", bucket: "stream-parity" });
  const first = await reader.read(keys[0]!);
  return {
    store, reader, keys,
    canonicalId: canonicalManifestIdentity(first!.manifest).sessionId,
    dispose: () => rm(root, { recursive: true, force: true }),
  };
}
