/**
 * Fakes for the owner-only buffered read tests (owner-buffered*.test.ts): a scratch fs object
 * store holding hand-built buffered stage objects (bypassing the writer's scrub on purpose, so
 * a test can store a pre-v4 body), a scripted control-head gate, and scripted RPC readers.
 * Nothing here reaches a network, a database or a bucket.
 */
import { createHash } from "node:crypto";
import { zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { objectKey, type ObjectStore } from "../../shared/vendor.ts";
import { scratchStore } from "../../session-ingest/__tests__/support.ts";
import { TURN_KIND, TURN_ORIGIN_ID, turnRecordId } from "../../session-ingest/turns.ts";
import type { AuthorityHead, BufferedScope, BufferedSessionRow, BufferedStageRow, ControlHeadGate, OwnerBufferedDeps, OwnerBufferedReads } from "../owner-buffered.ts";
import { BufferedFenced, BufferedTooLarge } from "../owner-buffered.ts";

export const SCOPE: BufferedScope = { orgId: "org-a", workspaceId: "ws-a", userId: "u-1" };
export const SESSION = "sess-b1";
/** The fixture-v3 control epoch, digest and a second epoch (synthetic hex). */
export const E1 = "1".repeat(64);
export const E2 = "2".repeat(64);
export const DIGEST = "3".repeat(64);

/** Synthetic privacy baseline (hex). */
export const BASELINE = "4".repeat(64);

export function authority(version: string | null, epoch = E1, cutover = E1): AuthorityHead {
  return { cutoverEpoch: cutover, privacyBaselineSha256: BASELINE, head: version === null ? null : { epoch, version, digest: DIGEST, kind: "selection", pending: false } };
}

/**
 * A frozen-wire (relayhistory-cloud #328 @ a06a58f9) 200 body for SCOPE with head version 7, in
 * the producer's key order; `over` replaces top-level keys. Synthetic identifiers only.
 */
export function headBody(over: Record<string, unknown> = {}) {
  return {
    protocol: "relay-session-read-control-head-v1", version: 1, issuer: "relayhistory-cloud",
    tenant: { orgId: SCOPE.orgId, workspaceId: SCOPE.workspaceId, userId: SCOPE.userId },
    controlEpoch: E1, cutover: { epoch: E1, status: "active", mode: "ingest" }, privacyBaselineSha256: BASELINE, sourceControlEpoch: E1,
    sourceControlVersion: "7", sourceControlDigest: DIGEST, sourceControlKind: "selection", pending: false, ...over,
  };
}
/** The frozen-wire "no head" body: version "0", null source epoch, digest and kind, pending false. */
export const noHeadBody = (over: Record<string, unknown> = {}) => headBody({ sourceControlEpoch: null, sourceControlVersion: "0", sourceControlDigest: null, sourceControlKind: null, ...over });

/** A scripted gate: one answer per observe() (the last repeats); an Error answer is thrown. */
export function scriptedGate(...answers: Array<AuthorityHead | Error>): ControlHeadGate & { calls: BufferedScope[] } {
  const calls: BufferedScope[] = [];
  return {
    calls,
    async observe(scope) {
      calls.push(scope);
      const a = answers[Math.min(calls.length - 1, answers.length - 1)]!;
      if (a instanceof Error) throw a;
      return a;
    },
  };
}
export const fencedGate = (reason = "gate_status") => scriptedGate(new BufferedFenced(reason));

/** One synthetic conversation_turn record (the shape canonicalLiveTurns accepts). */
export function turn(index: number, over: Record<string, unknown> = {}, revision = 1_793_000_000_000_000 + index, source = "claude", operation: "upsert" | "delete" = "upsert") {
  return {
    kind: TURN_KIND, origin_id: TURN_ORIGIN_ID, record_id: turnRecordId(SCOPE.workspaceId, source, SESSION, index), revision: String(revision), operation,
    received_at: "2026-11-05T12:00:00Z",
    payload: operation === "delete" ? null : {
      id: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`, org_id: SCOPE.orgId, workspace_id: SCOPE.workspaceId, source, session_id: SESSION,
      turn_index: index, session_owner: SCOPE.userId, role: "assistant", content: `buffered ${index}`, actor_name: "Ada", actor_role: "owner",
      metadata_json: JSON.stringify({ nativeCli: source, originNode: "node-b" }), ts: "2026-11-05T12:00:00.123Z", tagged: true, ...over,
    },
  };
}

export interface StoredStage { row: BufferedStageRow; key: string }

/**
 * Stores one buffered stage object exactly as given (header + lines, raw zstd), the bytes NOT
 * passed through any scrub: what an object written under an older ruleset holds. `header`
 * adds/overrides header keys (e.g. a claimed scrub ruleset version).
 */
export async function putStage(store: ObjectStore, n: number, records: unknown[], opts: { header?: Record<string, unknown>; source?: string; admission?: { epoch: string | null; version: string | null }; acceptedAt?: string } = {}): Promise<StoredStage> {
  const source = opts.source ?? "claude";
  const header = {
    v: 1, publication_state: "buffered", claim_token: `11111111-1111-4111-8111-${String(n).padStart(12, "0")}`, batch_digest: "a".repeat(64),
    received_at: "2026-11-05T12:00:00Z", org_id: SCOPE.orgId, workspace_id: SCOPE.workspaceId, origin_id: TURN_ORIGIN_ID, user_id: SCOPE.userId,
    groups: [[source, SESSION]], ...opts.header,
  };
  const lines = records.map((r, i) => JSON.stringify({ g: 0, m: [`r${n}-${i}`, "d".repeat(64), source, SESSION], l: r }));
  const raw = new TextEncoder().encode(`${JSON.stringify(header)}\n${lines.join("\n")}\n`);
  const body = new Uint8Array(zstdCompressSync(raw));
  const sha = createHash("sha256").update(body).digest("hex");
  const key = `96bulls/buffered-batches/v1/ws=${SCOPE.workspaceId}/m=srv_test/b=${n}-${sha.slice(0, 8)}.zst`;
  await store.put(objectKey(key), body, { contentType: "application/zstd" });
  const ordinals = records.map((_, i) => n * 10 + i);
  return {
    key,
    row: {
      batch_id: `b-${n}`, stage_key: key, stage_sha256: sha, stage_bytes: body.byteLength, group_indexes: [0], group_ordinals: [ordinals],
      kept_ordinals: ordinals, reset_epoch: "0", accepted_at: opts.acceptedAt ?? `2026-11-05T12:0${n}:00.000Z`,
      admission_epoch: opts.admission ? opts.admission.epoch : E1, admission_version: opts.admission ? opts.admission.version : "7",
    },
  };
}

/** zstd with the deps' bounded-output contract: maxOutputLength stops past the bound (BufferedTooLarge). */
export const unzip = (b: Uint8Array, maxBytes?: number) => {
  try {
    return new Uint8Array(zstdDecompressSync(b, maxBytes === undefined ? {} : { maxOutputLength: maxBytes }));
  } catch (error) {
    if (error instanceof RangeError) throw new BufferedTooLarge();
    throw error;
  }
};

/** Scripted readers: `stages` answers per call (the last repeats); every call is recorded. */
export function scriptedReads(init: { list?: BufferedSessionRow[] | Error; stages?: Array<BufferedStageRow[] | Error> } = {}) {
  const calls: Array<{ fn: "list" | "stages"; scope: BufferedScope; authority: AuthorityHead; args: unknown }> = [];
  let stageCalls = 0;
  const reads: OwnerBufferedReads = {
    async list(scope, authority, args) {
      calls.push({ fn: "list", scope, authority, args });
      if (init.list instanceof Error) throw init.list;
      return init.list ?? [];
    },
    async stages(scope, authority, source, sessionId) {
      calls.push({ fn: "stages", scope, authority, args: { source, sessionId } });
      const answers = init.stages ?? [[]];
      const a = answers[Math.min(stageCalls++, answers.length - 1)]!;
      if (a instanceof Error) throw a;
      return a;
    },
  };
  return { reads, calls };
}

export function bufferedRow(over: Partial<BufferedSessionRow> = {}): BufferedSessionRow {
  return {
    source: "claude", session_id: SESSION, first_ts: "2026-11-05T12:00:00.000Z", last_ts: "2026-11-05T12:05:00.000Z", stage_count: 1, bytes: 100,
    user_id: SCOPE.userId, cursor_ts: "2026-11-05T12:05:00.000000Z", admission_epoch: E1, admission_version: "7", ...over,
  };
}

export function scratch() {
  return scratchStore();
}

export function deps(over: Partial<OwnerBufferedDeps> & { store: ObjectStore }): OwnerBufferedDeps {
  return { enabled: true, workspaces: "*", reads: scriptedReads().reads, gate: scriptedGate(authority("7")), decompress: unzip, ...over };
}

/** A promise and its resolver (tests hold a GetObject open until they release it). */
export function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
}

/**
 * One shared, ordered trace of a buffered read's I/O: `get:<key>` when a stage GetObject starts,
 * `fail:<key>` when it throws, `decompressed` when its bytes reach the bounded decompress (the
 * stage fetch's end), `observe` per control-head observation and `stages` per stages() call.
 * `maxInFlight` is the most GetObjects started and not yet decompressed or failed. `hold(key, n)`
 * (n = the GetObject's start index) may delay a GetObject; `fail(key)` may throw instead of it.
 */
export function traced(inner: ObjectStore, opts: { hold?: (key: string, n: number) => Promise<void> | undefined; fail?: (key: string) => Error | undefined } = {}) {
  const events: string[] = [];
  let inFlight = 0;
  let started = 0;
  const t = { events, maxInFlight: 0, starts: () => events.filter((e) => e.startsWith("get:")).length };
  const store = new Proxy(inner, {
    get(target, prop) {
      if (prop === "getObject") {
        return async (key: Parameters<ObjectStore["getObject"]>[0], range?: Parameters<ObjectStore["getObject"]>[1]) => {
          const k = String(key);
          const n = started++;
          events.push(`get:${k}`);
          inFlight++;
          t.maxInFlight = Math.max(t.maxInFlight, inFlight);
          try {
            await opts.hold?.(k, n);
            const failure = opts.fail?.(k);
            if (failure) throw failure;
            return await target.getObject(key, range);
          } catch (error) {
            inFlight--;
            events.push(`fail:${k}`);
            throw error;
          }
        };
      }
      const v = Reflect.get(target, prop, target);
      return typeof v === "function" ? v.bind(target) : v;
    },
  });
  const decompress = (bytes: Uint8Array, maxBytes: number) => {
    inFlight--;
    events.push("decompressed");
    return unzip(bytes, maxBytes);
  };
  const gate = (...answers: Array<AuthorityHead | Error>): ControlHeadGate & { calls: BufferedScope[] } => {
    const g = scriptedGate(...answers);
    return { calls: g.calls, observe: (scope) => (events.push("observe"), g.observe(scope)) };
  };
  const reads = (r: OwnerBufferedReads): OwnerBufferedReads => ({ list: r.list, stages: (...a) => (events.push("stages"), r.stages(...a)) });
  return Object.assign(t, { store, decompress, gate, reads });
}
