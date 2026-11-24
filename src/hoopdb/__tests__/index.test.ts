import { describe, expect, test } from "bun:test";
import { hoopDbSessionIndex } from "../index.ts";
import type { HoopDb } from "../../shared/db.ts";
import type { ManifestReader, ManifestV1, PipelineQueue, RawPointer, TraceContext } from "../../shared/vendor.ts";

const traceId = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const sessionUuid = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19";
const manifestKey = "v1/ws=ws-test/m=machine-test/h=codex/d=2026/10/29/s=session-test/manifest.json";

function scrubbedManifest(): ManifestV1 {
  return {
    schema_v: 1,
    layout: "v1",
    trace_id: traceId,
    revision: 1,
    created_at: "2026-10-29T07:00:00.000Z",
    updated_at: "2026-10-29T07:00:01.000Z",
    session: {
      workspace_id: "ws-test",
      machine_id: "machine-test",
      harness: "codex",
      session_id: "session-test",
      started_at: "2026-10-29T07:00:00.000Z",
    },
    writer: { delivery_instance_id: "machine-test", plugin_version: "test" },
    raw: {
      owner: "machine-test",
      state: "open",
      updated_at: "2026-10-29T07:00:01.000Z",
      format: "jsonl",
      compression: "zstd",
      chunk_count: 1,
      raw_bytes: 12,
      stored_bytes: 10,
      content_sha256: "1".repeat(64),
      epoch: 0,
      epochs: [{ epoch: 0, opened_at: "2026-10-29T07:00:00.000Z", reason: "initial", first_seq: 0, opened_by: "machine-test" }],
      next_offset: 12,
      chunks: [{
        seq: 0,
        epoch: 0,
        start_offset: 0,
        bytes: 12,
        stored_bytes: 10,
        sha256: "2".repeat(64),
        sha256_8: "22222222",
        stored_sha256: "3".repeat(64),
        compression: "zstd",
      }],
      scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: {
      "96bulls": { session_uuid: sessionUuid, session_uuid_source: "minted" },
    },
  };
}

describe("HoopDb session index", () => {
  test("copies the manifest scrub verdict into the pointer upsert", async () => {
    const calls: Array<{ text: string; params?: unknown[] }> = [];
    const db = {
      async query<R>(text: string, params?: unknown[]) {
        calls.push({ text, params });
        if (text.includes("upsert_session_index")) return { rows: [{ applied: true, revision: "1" }] as R[] };
        return { rows: [] as R[] };
      },
    } as unknown as HoopDb;
    const manifest = scrubbedManifest();
    const reader: ManifestReader = {
      provider: "s3",
      bucket: "locker",
      async read() { return { manifest, etag: "etag-1" }; },
    };
    const queue: PipelineQueue = {
      backend: "memory",
      async send() {},
      async sendBatch() {},
    };
    const pointer: RawPointer = {
      traceId,
      workspaceId: "ws-test",
      machineId: "machine-test",
      harness: "codex",
      sessionId: "session-test",
      manifestKey,
      provider: "s3",
      bucket: "locker",
    };
    const ctx: TraceContext = {
      traceId,
      orgId: "org-test",
      workspaceId: "ws-test",
      machineId: "machine-test",
      harness: "codex",
      sessionId: "session-test",
    };

    await hoopDbSessionIndex({ db, reader, queue, current: () => undefined }).upsertRawPointer(pointer, ctx);

    const upsert = calls.find((call) => call.text.includes("upsert_session_index"));
    const rows = JSON.parse(String(upsert?.params?.[0])) as Array<{ scrubbed?: boolean }>;
    expect(rows[0]?.scrubbed).toBe(true);
  });

  test("repairs equal-revision identity before projecting the directory", async () => {
    const calls: Array<{ text: string; params?: unknown[] }> = [];
    const db = {
      async query<R>(text: string, params?: unknown[]) {
        calls.push({ text, params });
        if (text.includes("SELECT session_uuid::text"))
          return {
            rows: [{ session_uuid: sessionUuid, user_id: "unknown", fanout_revision: "1" }] as R[],
          };
        if (text.includes("upsert_session_index"))
          return { rows: [{ applied: false, revision: "1" }] as R[] };
        if (text.includes("WITH live AS"))
          return { rows: [{ projected: true }] as R[] };
        return { rows: [] as R[] };
      },
    } as unknown as HoopDb;
    const manifest = scrubbedManifest();
    manifest.session.session_id = "kimi_session";
    manifest.session.machine_id = "machine_encoded";
    manifest.extensions = {
      ...manifest.extensions,
      "96bulls": {
        session_uuid: sessionUuid,
        session_uuid_source: "minted",
        user_id: "user-real",
        canonical_session_id: "kimi:session",
        canonical_machine_id: "machine:encoded",
      },
    };
    const reader: ManifestReader = {
      provider: "s3",
      bucket: "locker",
      async read() {
        return { manifest, etag: "etag-1" };
      },
    };
    const queue: PipelineQueue = {
      backend: "memory",
      async send() {},
      async sendBatch() {},
    };
    await hoopDbSessionIndex({ db, reader, queue, current: () => undefined })
      .upsertRawPointer(
        {
          traceId,
          workspaceId: "ws-test",
          machineId: "machine_encoded",
          harness: "codex",
          sessionId: "kimi_session",
          manifestKey,
          provider: "s3",
          bucket: "locker",
        },
        {
          traceId,
          orgId: "org-test",
          workspaceId: "ws-test",
          machineId: "machine_encoded",
          harness: "codex",
          sessionId: "kimi_session",
        },
      );

    const extAt = calls.findIndex((call) =>
      call.text.includes("INSERT INTO sessions.session_index_ext"),
    );
    const repairAt = calls.findIndex((call) =>
      call.text.includes("canonical_session_id = $8"),
    );
    const projectionAt = calls.findIndex((call) =>
      call.text.includes("WITH live AS"),
    );
    expect(extAt).toBeGreaterThan(-1);
    expect(repairAt).toBeGreaterThan(extAt);
    expect(projectionAt).toBeGreaterThan(repairAt);
    expect(calls[extAt]!.params?.[8]).toBe("user-real");
    expect(calls[repairAt]!.params?.slice(6)).toEqual([
      "user-real",
      "kimi:session",
      "machine:encoded",
    ]);
  });
});

describe("HoopDb session index: the per-call liveness check", () => {
  type Event = { kind: "sql"; text: string; params?: unknown[]; arity: number } | { kind: "send"; stage: string };
  const label = (e: Event) => (e.kind === "sql" ? e.text.trim().split(/\s+/).slice(0, 2).join(" ") : `send ${e.stage}`);
  const pointer: RawPointer = {
    traceId,
    workspaceId: "ws-test",
    machineId: "machine-test",
    harness: "codex",
    sessionId: "session-test",
    manifestKey,
    provider: "s3",
    bucket: "locker",
  };
  const ctx: TraceContext = {
    traceId,
    orgId: "org-test",
    workspaceId: "ws-test",
    machineId: "machine-test",
    harness: "codex",
    sessionId: "session-test",
  };
  /**
   * One index over a recording executor and queue (statements and sends in one log). A new
   * session, so the live path runs every statement and fans out; with `brief` the wakeup is armed.
   * `after` runs after each statement and each send.
   */
  function rig(after: () => void = () => {}) {
    const events: Event[] = [];
    const db = {
      async query<R>(...args: [string, unknown[]?]) {
        const [text, params] = args;
        events.push({ kind: "sql", text, ...(params ? { params } : {}), arity: args.length });
        after();
        if (text.includes("upsert_session_index")) return { rows: [{ applied: true, revision: "1" }] as R[] };
        if (text.includes("RETURNING 1")) return { rows: [{ "?column?": 1 }] as R[] };
        return { rows: [] as R[] };
      },
    } as unknown as HoopDb;
    const manifest = scrubbedManifest();
    const reader: ManifestReader = { provider: "s3", bucket: "locker", async read() { return { manifest, etag: "etag-1" }; } };
    const queue: PipelineQueue = {
      backend: "memory",
      async send(message) { events.push({ kind: "send", stage: message.stage }); after(); },
      async sendBatch() { throw new Error("not used"); },
    };
    const index = hoopDbSessionIndex({ db, reader, queue, current: () => undefined, brief: { idleSeconds: 60, maxWaitSeconds: 600 } });
    return { events, index };
  }
  const tombstone: RawPointer = { ...pointer, deleted: true } as RawPointer;

  test("without a check (and with a check that never throws) the statements, their parameters, arity and order, and the sends are the same", async () => {
    for (const p of [pointer, tombstone]) {
      const plain = rig();
      await plain.index.upsertRawPointer(p, ctx);
      const checked = rig();
      await checked.index.upsertRawPointer(p, ctx, () => {});
      expect(checked.events).toEqual(plain.events);
    }
    // The statement list as it was before the check existed.
    const live = rig();
    await live.index.upsertRawPointer(pointer, ctx);
    expect(live.events.map(label)).toEqual([
      "SELECT session_uuid::text,",
      "SELECT applied,",
      "INSERT INTO",
      "UPDATE sessions.session_index",
      "WITH live",
      "WITH identity_stats",
      "send summary-short",
      "send summary-long",
      "send status-classify",
      "INSERT INTO",
      "UPDATE sessions.session_index_ext",
    ]);
    const gone = rig();
    await gone.index.upsertRawPointer(tombstone, ctx);
    expect(gone.events.map(label)).toEqual([
      "UPDATE sessions.session_index",
      "WITH identity_stats", // propagatePointerTombstone
      "WITH live", // projectSessionDirectory
      "WITH identity_stats",
    ]);
  });

  test("an error reported after any statement or send stops the next one, reads included, and the call fails with it", async () => {
    for (const p of [pointer, tombstone]) {
      const full = rig();
      await full.index.upsertRawPointer(p, ctx);
      for (let i = 0; i < full.events.length; i++) {
        const error = new Error(`lock connection gone after event ${i}`);
        let broken = false;
        const r = rig(() => {
          if (r.events.length === i + 1) broken = true;
        });
        const run = r.index.upsertRawPointer(p, ctx, () => {
          if (broken) throw error;
        });
        if (i === full.events.length - 1) await run;
        else await expect(run).rejects.toBe(error);
        expect(r.events.map(label)).toEqual(full.events.slice(0, i + 1).map(label));
      }
    }
  });

  test("the check is per call: a call without one on the same index after a checked call is not checked", async () => {
    const error = new Error("gone");
    const r = rig();
    await expect(r.index.upsertRawPointer(pointer, ctx, () => { throw error; })).rejects.toBe(error);
    expect(r.events).toEqual([]);
    await r.index.upsertRawPointer(pointer, ctx);
    expect(r.events.length).toBe(11);
  });
});
