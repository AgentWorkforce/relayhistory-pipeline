/**
 * The normalized-ready path (`docs/event-count-writeback.md`): the typed record,
 * the handlers, and retry and quarantine through the real `pollPostgresStage` and
 * `record_normalized_event_count` on PGlite, as the real `session_count_writer`.
 */
import { PGlite } from "@electric-sql/pglite";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import {
  fileNormalizeHandler,
  InMemoryPipelineEventSink,
  noopErrorReporter,
  normalizedReadyHandler,
  parseNormalizedReady,
  pollPostgresStage,
  postgresDeadLetterStore,
  postgresNormalizedCountStore,
  postgresPipelineQueue,
  type ConnectSql,
  type NormalizedCountOutcome,
  type NormalizedOutput,
  type NormalizedReady,
  type SqlExecutor,
  type StageMessage,
  type StageQueueDeps,
} from "../src/lib/pipeline/index.js";

const SUPABASE = new URL("../../../supabase/", import.meta.url).href;
const supabaseFile = (path: string) =>
  fileURLToPath(new URL(path, SUPABASE).href);
const TRACE = "0192f000-0000-7000-8000-000000000001";
const MANIFEST_KEY = "v1/ws=ws_a/m=m-1/h=claude/d=2026/10/30/s=s-1/manifest.json";
const WORK_KEY = `${MANIFEST_KEY}@etag-1`;
const CONTENT = "a".repeat(64);
const CTX = { traceId: TRACE, orgId: "org_a", workspaceId: "ws_a", machineId: "m-1", sessionId: "s-1", harness: "claude" };

const output = (over: Partial<NormalizedOutput> = {}): NormalizedOutput => ({
  outputKey: "v1/ws=ws_a/m=m-1/h=claude/d=2026/10/30/s=s-1/normalized/v2/relayhistory-x.ndjson",
  revision: 1,
  normalizedVersion: 2,
  contentSha256: CONTENT,
  outputSha256: "b".repeat(64),
  outputBytes: 100,
  convergenceEventCount: 42,
  unprojectedRecords: 0,
  outputReused: false,
  outputDivergent: false,
  ...over,
});
const normalizeMessage = (over: Partial<StageMessage> = {}): StageMessage => ({
  v: 1,
  stage: "file-normalize",
  ctx: CTX,
  workKey: WORK_KEY,
  revision: 1,
  payload: { manifestKey: MANIFEST_KEY },
  ...over,
});
const record = (over: Partial<NormalizedReady> = {}): NormalizedReady => ({
  v: 1,
  org_id: "org_a",
  workspace_id: "ws_a",
  harness: "claude",
  session_id: "s-1",
  machine_id: "m-1",
  manifest_key: MANIFEST_KEY,
  manifest_revision: 1,
  work_key: WORK_KEY,
  trace_id: TRACE,
  source_content_sha256: CONTENT,
  normalized_version: 2,
  output_key: output().outputKey,
  output_sha256: "b".repeat(64),
  output_bytes: 100,
  convergence_event_count: 42,
  unprojected_records: 0,
  output_reused: false,
  output_divergent: false,
  ...over,
});
const readyMessage = (payload: unknown, over: Partial<StageMessage> = {}): StageMessage => ({
  v: 1,
  stage: "normalized-ready",
  ctx: CTX,
  workKey: WORK_KEY,
  revision: 1,
  payload,
  ...over,
});
const recorder = () => ({ log: { info: vi.fn() } }) as never;
const JOB = { id: "1", leaseId: "0192f000-0000-7000-8000-00000000000a" };

describe("parseNormalizedReady", () => {
  it("accepts a whole record", () => {
    expect(parseNormalizedReady(record())).toEqual(record());
  });

  it.each([
    ["a missing field", (r: Record<string, unknown>) => delete r.work_key],
    ["an unknown field", (r: Record<string, unknown>) => (r.content = "x")],
    ["a wrong version", (r: Record<string, unknown>) => (r.v = 2)],
    ["a bad hash", (r: Record<string, unknown>) => (r.output_sha256 = "XYZ")],
    ["a negative count", (r: Record<string, unknown>) => (r.convergence_event_count = -1)],
    ["a fractional count", (r: Record<string, unknown>) => (r.convergence_event_count = 1.5)],
    ["a count past integer", (r: Record<string, unknown>) => (r.convergence_event_count = 2 ** 31)],
    ["revision 0", (r: Record<string, unknown>) => (r.manifest_revision = 0)],
    ["a string flag", (r: Record<string, unknown>) => (r.output_divergent = "false")],
  ])("refuses %s without a retry", (_, change) => {
    const bad = record() as unknown as Record<string, unknown>;
    change(bad);
    expect(() => parseNormalizedReady(bad)).toThrow(expect.objectContaining({ code: "invalid_normalized_ready", retryable: false }));
  });

  it("refuses something that isn't a record", () => {
    for (const bad of [null, [], "x"]) expect(() => parseNormalizedReady(bad)).toThrow();
  });

  it("names the field, never its value, in the error", () => {
    expect(() => parseNormalizedReady(record({ harness: "" }))).toThrow("harness");
    try {
      parseNormalizedReady({ ...record(), secret_field: "planted" });
    } catch (error) {
      expect(String(error)).not.toContain("planted");
    }
  });
});

describe("fileNormalizeHandler", () => {
  const run = async (result: unknown, send = vi.fn(async () => {}), message = normalizeMessage()) => {
    const handler = fileNormalizeHandler({ normalize: async () => result as never }, { send });
    return { send, out: await handler(message, recorder(), 1) };
  };

  it("sends the record with identity from ctx and the count from the output", async () => {
    const { send } = await run(output());
    expect(send).toHaveBeenCalledTimes(1);
    const [stage, sent] = send.mock.calls[0] as unknown as [string, StageMessage];
    expect(stage).toBe("normalized-ready");
    expect(sent).toMatchObject({ stage: "normalized-ready", workKey: WORK_KEY, revision: 1, ctx: CTX });
    expect(sent.payload).toEqual(record());
  });

  it("publishes first: a failed send fails the stage, so it retries", async () => {
    await expect(run(output(), vi.fn(async () => { throw new Error("queue down"); }))).rejects.toThrow("queue down");
  });

  it("sends a divergent output, so the consumer can quarantine it", async () => {
    const { send } = await run(output({ outputDivergent: true, outputReused: true }));
    expect((send.mock.calls[0] as unknown as [string, StageMessage])[1].payload).toMatchObject({ output_divergent: true });
  });

  it("sends nothing for a stale manifest or when nothing was normalized", async () => {
    const stale = await run({ skipped: "stale_manifest" });
    expect(stale.send).not.toHaveBeenCalled();
    expect(stale.out).toEqual({ status: "skipped", reason: "stale_manifest" });
    expect((await run({})).send).not.toHaveBeenCalled();
  });

  it("refuses a result for another revision than the message's", async () => {
    await expect(run(output({ revision: 2 }))).rejects.toMatchObject({ code: "invalid_normalized_ready" });
  });

  it("refuses a context with no machine, rather than guess at the row", async () => {
    const { machineId: _, ...ctx } = CTX;
    await expect(run(output(), undefined, normalizeMessage({ ctx }))).rejects.toMatchObject({ code: "invalid_normalized_ready" });
  });
});

describe("normalizedReadyHandler", () => {
  const store = (outcome: NormalizedCountOutcome = "applied") => ({ record: vi.fn(async () => outcome) });

  it("records it, and only applied is plain success", async () => {
    expect(await normalizedReadyHandler(store())(readyMessage(record()), recorder(), 1, JOB)).toBeUndefined();
    for (const outcome of ["replayed", "stale", "deleted"] as const)
      expect(await normalizedReadyHandler(store(outcome))(readyMessage(record()), recorder(), 1, JOB)).toEqual({ status: "skipped", reason: outcome });
  });

  it("passes only the job to the store, and needs one", async () => {
    const counts = store();
    await normalizedReadyHandler(counts)(readyMessage(record()), recorder(), 1, JOB);
    expect(counts.record).toHaveBeenCalledWith(JOB);
    await expect(normalizedReadyHandler(counts)(readyMessage(record()), recorder(), 1)).rejects.toMatchObject({
      code: "count_no_job_lease",
      retryable: false,
    });
  });

  it("quarantines divergent output and never writes it", async () => {
    const counts = store();
    await expect(
      normalizedReadyHandler(counts)(readyMessage(record({ output_divergent: true })), recorder(), 1, JOB),
    ).rejects.toMatchObject({ code: "count_conflict", retryable: false });
    expect(counts.record).not.toHaveBeenCalled();
  });

  it.each([
    ["another org", record({ org_id: "org_b" })],
    ["another workspace", record({ workspace_id: "ws_b" })],
    ["another machine", record({ machine_id: "m-2" })],
    ["another session", record({ session_id: "s-2" })],
    ["another harness", record({ harness: "codex" })],
    ["another work key", record({ work_key: "other@etag" })],
    ["another revision", record({ manifest_revision: 2 })],
    ["a key for another session", record({ manifest_key: MANIFEST_KEY.replace("s=s-1", "s=s-2") })],
  ])("refuses %s against the authenticated context", async (_, ready) => {
    const counts = store();
    await expect(normalizedReadyHandler(counts)(readyMessage(ready), recorder(), 1, JOB)).rejects.toMatchObject({
      code: "invalid_normalized_ready",
      retryable: false,
    });
    expect(counts.record).not.toHaveBeenCalled();
  });
});

describe("through the queue, on the real schema", () => {
  let db: PGlite;
  const sql = () => db as unknown as SqlExecutor;
  /** Connects as the runtime role, like the indexer's login. */
  const connect: ConnectSql = async () => {
    await db.exec("SET ROLE service_role");
    return { query: (text, params) => sql().query(text, params), release: () => void db.exec("RESET ROLE") };
  };
  const deps = (): StageQueueDeps => ({
    sink: new InMemoryPipelineEventSink(),
    deadLetters: postgresDeadLetterStore(sql()),
    reporter: noopErrorReporter,
    logger: { environment: "test" },
    handlers: { "normalized-ready": normalizedReadyHandler(postgresNormalizedCountStore(connect)) },
  });
  const poll = (d: StageQueueDeps) => pollPostgresStage({ sql: sql(), stage: "normalized-ready", deps: d });
  const send = (over: Partial<NormalizedReady> = {}, revision = 1) =>
    postgresPipelineQueue(sql()).send(readyMessage(record({ manifest_revision: revision, ...over }), { revision }));
  const index = async (revision: number) =>
    db.query("SELECT * FROM sessions.upsert_session_index($1::jsonb)", [
      JSON.stringify([{
        org_id: "org_a", workspace_id: "ws_a", harness: "claude", session_id: "s-1", machine_id: "m-1",
        user_id: "u", created_at: "2026-10-30T00:00:00Z", last_activity_at: "2026-10-30T00:00:00Z",
        event_count: 7, storage_provider: "s3", bucket: "b", manifest_key: MANIFEST_KEY,
        content_sha256: `\\x${CONTENT}`, raw_bytes: 1, chunk_count: 1, raw_format: "jsonl", format_version: 1,
        revision, trace_id: TRACE, scrubbed: true, deleted_at: null,
      }]),
    ]);
  const row = async () =>
    (await db.query<{ event_count: number; event_count_revision: number | null }>(
      "SELECT event_count, event_count_revision FROM sessions.session_index")).rows[0];
  const dueNow = () => db.exec("UPDATE sessions.pipeline_jobs SET visible_at = now()");
  const queued = async () =>
    (await db.query<{ dlq: boolean }>("SELECT dlq FROM sessions.pipeline_jobs WHERE stage = 'normalized-ready'")).rows;
  const letters = async () =>
    (await db.query<{ state: string }>("SELECT state FROM sessions.pipeline_dead_letters")).rows;

  beforeAll(async () => {
    db = new PGlite();
    await db.exec(readFileSync(supabaseFile("tests/supabase_shim.sql"), "utf8"));
    for (const file of readdirSync(supabaseFile("migrations/")).sort())
      await db.exec(readFileSync(supabaseFile(`migrations/${file}`), "utf8"));
  });
  afterAll(async () => {
    await db?.close();
  });
  beforeEach(async () => {
    vi.spyOn(console, "info").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    await db.exec(`TRUNCATE sessions.session_index CASCADE;
      TRUNCATE sessions.pipeline_jobs, sessions.pipeline_dead_letters`);
  });

  it("applies the count and acks the job", async () => {
    await index(1);
    await send();
    expect(await poll(deps())).toMatchObject({ acked: 1 });
    expect(await row()).toMatchObject({ event_count: 42, event_count_revision: 1 });
    expect(await queued()).toEqual([]);
  });

  it("the store's path takes session_count_writer: its login can't call the function directly", async () => {
    await index(1);
    await send();
    await db.exec("SET ROLE service_role");
    await expect(db.query("SELECT sessions.record_normalized_event_count(1, gen_random_uuid())")).rejects.toMatchObject({ code: "42501" });
    await db.exec("RESET ROLE");
    expect(await poll(deps())).toMatchObject({ acked: 1 });
    expect(await row()).toMatchObject({ event_count: 42 });
  });

  it("a redelivered record is a replay, and acks", async () => {
    await index(1);
    await send();
    await send();
    const d = deps();
    expect(await poll(d)).toMatchObject({ acked: 2 });
    expect(await row()).toMatchObject({ event_count: 42 });
  });

  it("an index that catches up lets the retry apply", async () => {
    await send();
    expect(await poll(deps())).toMatchObject({ retried: 1 });
    await index(1);
    await dueNow();
    expect(await poll(deps())).toMatchObject({ acked: 1 });
    expect(await row()).toMatchObject({ event_count: 42, event_count_revision: 1 });
  });

  it("a never-indexed row stops at the attempt budget and quarantines, claiming nothing", async () => {
    await send();
    const d = deps();
    for (let attempt = 1; attempt <= 6; attempt++) {
      await poll(d);
      await dueNow();
    }
    expect(await queued()).toEqual([{ dlq: true }]);
    expect(await poll(d)).toMatchObject({ acked: 1 });
    expect(await letters()).toEqual([{ state: "dead" }]);
    expect(await row()).toBeUndefined();
  });

  it("a conflict quarantines at once and writes nothing", async () => {
    await index(1);
    await send({ source_content_sha256: "c".repeat(64) });
    await poll(deps());
    await dueNow();
    await poll(deps());
    expect(await letters()).toEqual([{ state: "dead" }]);
    expect((await row())?.event_count_revision).toBeNull();
  });

  it("divergent output quarantines at once and writes nothing", async () => {
    await index(1);
    await send({ output_divergent: true, output_reused: true });
    await poll(deps());
    await dueNow();
    await poll(deps());
    expect(await letters()).toEqual([{ state: "dead" }]);
    expect((await row())?.event_count_revision).toBeNull();
  });

  it("a late revision acks as stale and leaves the newer count", async () => {
    await index(1);
    await index(2);
    await send({ convergence_event_count: 10 }, 2);
    await send({ convergence_event_count: 99 }, 1);
    expect(await poll(deps())).toMatchObject({ acked: 2 });
    expect(await row()).toMatchObject({ event_count: 10, event_count_revision: 2 });
  });
});
