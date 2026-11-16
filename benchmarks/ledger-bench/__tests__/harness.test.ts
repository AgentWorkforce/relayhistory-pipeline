import { describe, expect, test } from "bun:test";
import { createS3Store } from "../../../packages/object-store/src/s3.ts";
import type { ObjectStore } from "../../../packages/object-store/src/types.ts";
import { assumeRole, BudgetExhausted, Dynamo, expressStore, expressZone, meteredFetch, RequestMeter } from "../aws.ts";
import { LANES, parseConfig, SMOKE, STANDARD, type Profile } from "../config.ts";
import { key, type Cell, type Ctx } from "../context.ts";
import { costBound, FIXED_USD, laneCostBound } from "../cost.ts";
import { errorClass, runLoad } from "../load.ts";
import { checkLog, dynamoCommit, dynamoHeadSeq, entryCommit, logGroup, P13_ROUND_TRIPS, p13Batch, racingWriters, seedP13, type CommitStats, type StreamWriter } from "../protocols.ts";
import { Histogram, median, Tally } from "../stats.ts";
import { step3Log, step3P13, step3Race, step5 } from "../steps-protocols.ts";
import { ambiguousBody, ambiguousCommit, probeIfMatch, raceCas, raceCreate, raceDelete, step1, step2, step6 } from "../steps-s3.ts";
import { FakeDynamo, FakeS3, type FakeS3Options } from "./fake-aws.ts";

const CREDS = { accessKeyId: "AKIDEXAMPLE", secretAccessKey: "secret" };
const storeOn = (fake: FakeS3, fetcher: typeof fetch = fake.fetch): ObjectStore => createS3Store({ bucket: "ledger-bench-test", region: "us-west-2", endpoint: "http://localhost:9000", credentials: CREDS, fetch: fetcher });
const fakeStore = (options: FakeS3Options = {}) => {
  const fake = new FakeS3(options);
  return { fake, store: storeOn(fake) };
};
const TINY: Profile = { ...SMOKE, cellSeconds: 0.15, warmupSeconds: 0.02, protocolSeconds: 0.25, concurrencies: [1, 4], raceRounds: 6, poolRamp: [16], maxOpsPerCell: 300 };

function context(fake: FakeS3, dynamo?: FakeDynamo, profile: Profile = TINY): { ctx: Ctx; cells: Cell[]; meter: RequestMeter } {
  const meter = new RequestMeter(1_000_000);
  const cells: Cell[] = [];
  const store = storeOn(fake, meteredFetch(meter, "R2", fake.fetch));
  const ctx: Ctx = { profile, meter, root: "bench/test/r2", stores: { R1: store, R2: store, R3: store, R4: store, R5: store }, sseS3: { R2: store }, expressStats: {}, emit: (c) => cells.push(c), log: () => {} };
  if (dynamo) ctx.dynamo = new Dynamo({ table: "ledger_bench", region: "us-west-2" }, () => Promise.resolve(CREDS), meteredFetch(meter, "R6", dynamo.fetch));
  return { ctx, cells, meter };
}
const violations = (cells: Cell[]) => cells.reduce((a, c) => a + ((c.result as { violations?: number }).violations ?? 0), 0);

describe("stats", () => {
  test("percentiles are within the bucket precision", () => {
    const h = new Histogram();
    for (let i = 1; i <= 10_000; i++) h.record(i / 10);
    const s = h.summary();
    expect(s.n).toBe(10_000);
    expect(Math.abs(s.p50_ms! - 500) / 500).toBeLessThan(0.03);
    expect(Math.abs(s.p99_ms! - 990) / 990).toBeLessThan(0.03);
    expect(s.max_ms).toBe(1000);
    expect(s.min_ms).toBe(0.1);
    expect(h.buckets().reduce((a, [, c]) => a + c, 0)).toBe(10_000);
  });
  test("an empty histogram reports nulls, and a bad sample is refused", () => {
    expect(new Histogram().summary().p50_ms).toBeNull();
    expect(() => new Histogram().record(Number.NaN)).toThrow();
    expect(() => new Histogram().record(-1)).toThrow();
  });
  test("median of repeats ignores missing values", () => {
    expect(median([3, null, 1, 2])).toBe(2);
    expect(median([4, 2])).toBe(3);
    expect(median([null, undefined])).toBeNull();
  });
});

describe("load", () => {
  test("drops warm-up samples, counts outcomes and classes errors", async () => {
    let calls = 0;
    const result = await runLoad({
      concurrency: 4,
      warmupMs: 30,
      durationMs: 120,
      op: async (_w, i) => {
        calls++;
        await new Promise((r) => setTimeout(r, 2));
        if (i % 5 === 4) throw Object.assign(new Error("secret/key/name"), { name: "Boom" });
        return i % 2 === 0 ? "even" : undefined;
      },
    });
    expect(result.latency.n).toBeGreaterThan(10);
    expect(result.latency.n + (result.errors.thrown_Boom ?? 0)).toBeLessThan(calls);
    expect(result.outcomes.even).toBeGreaterThan(0);
    expect(result.outcomes.ok).toBeGreaterThan(0);
    expect(Object.keys(result.errors)).toEqual(["thrown_Boom"]);
    expect(result.latency.p50_ms!).toBeGreaterThanOrEqual(1);
  });
  test("the operation cap counts steady-state operations, so a fast cell still has samples after its warm-up", async () => {
    // Far more than 20 operations fit in the warm-up; the cap must not be spent there.
    const r = await runLoad({ concurrency: 4, warmupMs: 40, durationMs: 5_000, maxOps: 20, op: async () => {} });
    expect(r.capped_at_ops).toBe(20);
    expect(r.latency.n).toBe(20);
    expect(r.ops_per_sec).toBeGreaterThan(0);
  });
  test("stops at the operation cap and at the stop signal", async () => {
    const capped = await runLoad({ concurrency: 2, warmupMs: 0, durationMs: 5_000, maxOps: 20, op: async () => {} });
    expect(capped.capped_at_ops).toBe(20);
    expect(capped.latency.n).toBe(20);
    const stopped = await runLoad({ concurrency: 2, warmupMs: 0, durationMs: 5_000, stop: () => true, op: async () => {} });
    expect(stopped.truncated).toBe(true);
    expect(stopped.latency.n).toBe(0);
  });
  test("an error class never carries a message", () => {
    expect(errorClass(new Error("s3://bucket/secret"))).toBe("thrown_Error");
    expect(errorClass("text")).toBe("thrown_other");
  });
});

describe("config and cost", () => {
  const base = { runId: "run-1", lane: "r2", targets: { R2: { bucket: "acme-ledger-bench-r2", region: "us-west-2" }, R6: { table: "ledger_bench", region: "us-west-2" } } };
  test("parses a lane's config", () => {
    const c = parseConfig(JSON.stringify(base), {});
    expect(c.lane).toBe("r2");
    expect(c.profile).toBe(STANDARD);
    expect(c.targets.R6?.table).toBe("ledger_bench");
  });
  test("refuses a target that is not a ledger-bench resource", () => {
    expect(() => parseConfig(JSON.stringify({ ...base, targets: { R2: { bucket: "prod-sessions-ledger", region: "us-east-1" } } }), {})).toThrow(/not a ledger-bench resource/);
    expect(() => parseConfig(JSON.stringify({ ...base, targets: { R6: { table: "sessions", region: "us-west-2" } } }), {})).toThrow(/not a ledger-bench resource/);
  });
  test("the single-key probe's key must be a key no bucket uses", () => {
    const arn = (n: string) => `arn:aws:kms:us-west-2:111111111111:key/${n.repeat(8)}-${n.repeat(4)}-${n.repeat(4)}-${n.repeat(4)}-${n.repeat(12)}`;
    const withKey = { ...base, targets: { ...base.targets, R2: { ...base.targets.R2, kmsKeyArn: arn("a") } } };
    expect(parseConfig(JSON.stringify({ ...withKey, foreignKmsKeyArn: arn("b") }), {}).foreignKmsKeyArn).toBe(arn("b"));
    expect(() => parseConfig(JSON.stringify({ ...withKey, foreignKmsKeyArn: arn("a") }), {})).toThrow(/must not be a bucket's own key/);
    expect(() => parseConfig(JSON.stringify(base), { LEDGER_BENCH_PROFILE: "constructor" })).toThrow(/profile/);
  });
  test("refuses a remote endpoint override, a bad lane, run id, profile or step", () => {
    expect(() => parseConfig(JSON.stringify({ ...base, targets: { R2: { bucket: "x-ledger-bench", region: "us-west-2", endpoint: "https://evil.example" } } }), {})).toThrow(/loopback/);
    expect(() => parseConfig(JSON.stringify({ ...base, lane: "prod" }), {})).toThrow(/lane/);
    expect(() => parseConfig(JSON.stringify({ ...base, runId: "A/../b" }), {})).toThrow(/runId/);
    expect(() => parseConfig(JSON.stringify(base), { LEDGER_BENCH_PROFILE: "huge" })).toThrow(/profile/);
    expect(() => parseConfig(JSON.stringify(base), { LEDGER_BENCH_STEPS: "1,9" })).toThrow(/steps/);
    expect(() => parseConfig("{", {})).toThrow(/JSON/);
  });
  test("the standard profile, three repeats, stays under the approved cap", () => {
    const bound = costBound(STANDARD, 3);
    expect(bound.s3_lanes).toBe(LANES.length - 1);
    expect(bound.total_usd).toBeLessThan(300);
    // The bound moves with the budget: doubling it must show.
    expect(costBound({ ...STANDARD, requestBudgetPerLane: STANDARD.requestBudgetPerLane * 2 }, 3).total_usd).toBeGreaterThan(300);
    // The per-lane bounds the spend ledger adds up are the same money.
    const lanes = LANES.reduce((a, lane) => a + laneCostBound(STANDARD, lane), 0);
    expect(Math.abs(lanes * 3 + FIXED_USD - bound.total_usd)).toBeLessThan(0.25);
    expect(laneCostBound(STANDARD, "dynamo")).toBeLessThan(laneCostBound(STANDARD, "r2"));
  });
});

describe("request meter", () => {
  test("refuses the request after the budget and tallies statuses", async () => {
    const meter = new RequestMeter(2);
    const f = meteredFetch(meter, "R2", (async () => new Response(null, { status: 204 })) as unknown as typeof fetch);
    await f("http://localhost/a");
    await f("http://localhost/b", { method: "put" });
    await expect(f("http://localhost/c")).rejects.toBeInstanceOf(BudgetExhausted);
    expect(meter.used).toBe(2);
    expect(meter.exhausted).toBe(true);
    expect(meter.statuses.toJSON()).toEqual({ "R2 GET 204": 1, "R2 PUT 204": 1 });
    // A child's allowance is reserved whole, and refused whole when it does not fit.
    const parent = new RequestMeter(10);
    parent.take(6);
    expect(() => parent.take(6)).toThrow(BudgetExhausted);
    expect(parent.exhausted).toBe(true);
    expect(() => new RequestMeter(0)).toThrow();
  });
});

describe("credentials and S3 Express", () => {
  test("assumeRole signs for sts, parses credentials and reports only a class on failure", async () => {
    let seen: Headers | undefined;
    const ok = (async (_u: unknown, init?: RequestInit) => {
      seen = new Headers(init?.headers);
      return new Response("<AssumeRoleResponse><Credentials><AccessKeyId>AK2</AccessKeyId><SecretAccessKey>sk2</SecretAccessKey><SessionToken>tok</SessionToken><Expiration>2030-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleResponse>");
    }) as unknown as typeof fetch;
    const provider = assumeRole(() => Promise.resolve(CREDS), { roleArn: "arn:aws:iam::000000000000:role/ledger-bench", region: "us-west-2", sessionName: "ledger-bench-r2", fetch: ok });
    expect(await provider()).toMatchObject({ accessKeyId: "AK2", sessionToken: "tok" });
    expect(seen!.get("authorization")).toContain("/us-west-2/sts/aws4_request");
    const denied = assumeRole(() => Promise.resolve(CREDS), { roleArn: "arn:aws:iam::000000000000:role/ledger-bench", region: "us-west-2", sessionName: "ledger-bench-r2", fetch: (async () => new Response("<ErrorResponse><Error><Code>AccessDenied</Code><Message>User arn:aws:sts::000000000000:assumed-role/x is not authorized</Message></Error></ErrorResponse>", { status: 403 })) as unknown as typeof fetch });
    const error = await denied().catch((e: Error) => e);
    expect(errorClass(error)).toBe("sts_403_AccessDenied");
    expect((error as Error).message).not.toContain("arn:");
    expect(() => assumeRole(() => Promise.resolve(CREDS), { roleArn: "not-an-arn", region: "us-west-2", sessionName: "x1" })).toThrow();
  });

  test("re-signs for s3express with the session token, and refreshes the session before it expires", async () => {
    let clock = Date.parse("2026-11-16T00:00:00Z");
    const seen: { url: string; headers: Headers }[] = [];
    const fetcher = (async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
      const url = String(input);
      const headers = new Headers(init?.headers);
      seen.push({ url, headers });
      if (url.endsWith("/?session"))
        return new Response(`<CreateSessionResult><Credentials><SessionToken>st-${seen.length}</SessionToken><SecretAccessKey>ssk</SecretAccessKey><AccessKeyId>SAK</AccessKeyId><Expiration>${new Date(clock + 300_000).toISOString()}</Expiration></Credentials></CreateSessionResult>`);
      return new Response(null, { status: 200, headers: { etag: '"e"' } });
    }) as unknown as typeof fetch;
    const { store, stats } = expressStore({ bucket: "ledger-bench-a--usw2-az1--x-s3", region: "us-west-2" }, () => Promise.resolve({ ...CREDS, sessionToken: "iam-token" }), fetcher, { now: () => clock });
    await store.put(key("bench/run/a"), new Uint8Array(8));
    await store.put(key("bench/run/b"), new Uint8Array(8), { ifNoneMatch: true });
    expect(stats.createSessionCalls).toBe(1);
    const [session, put, create] = seen;
    expect(session!.url).toBe("https://ledger-bench-a--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com/?session");
    expect(session!.headers.get("authorization")).toContain("/us-west-2/s3express/aws4_request");
    expect(session!.headers.get("x-amz-security-token")).toBe("iam-token");
    expect(put!.url).toBe("https://ledger-bench-a--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com/bench/run/a");
    expect(put!.headers.get("x-amz-s3session-token")).toBe("st-1");
    expect(put!.headers.get("x-amz-security-token")).toBeNull();
    expect(put!.headers.get("authorization")).toContain("Credential=SAK/");
    expect(put!.headers.get("authorization")).toContain("/s3express/aws4_request");
    expect(put!.headers.get("authorization")).toContain("x-amz-s3session-token");
    expect(create!.headers.get("if-none-match")).toBe("*");
    // 4 minutes 30 seconds later the session is inside its refresh window.
    clock += 270_000;
    await store.put(key("bench/run/c"), new Uint8Array(8));
    expect(stats.createSessionCalls).toBe(2);
    expect(seen[seen.length - 1]!.headers.get("x-amz-s3session-token")).toBe(`st-${seen.length - 1}`);
  });

  test("names the zone of a directory bucket and refuses anything else", () => {
    expect(expressZone("ledger-bench-a--usw2-az3--x-s3")).toBe("usw2-az3");
    expect(() => expressZone("ledger-bench-a")).toThrow();
  });
});

describe("conditional-write checks", () => {
  test("a correct backend passes every race", async () => {
    const { store } = fakeStore();
    for (const writers of [2, 8]) {
      const r = await raceCreate(store, `t/create${writers}`, writers, 10);
      expect(r).toMatchObject({ rounds: 10, clean: 10, violations: 0, winners: { "1": 10 } });
      expect(r.refusals.http_412 ?? r.refusals.already_exists).toBe(10 * (writers - 1));
    }
    expect(await raceCas(store, "t/cas", 8, 10)).toMatchObject({ clean: 10, violations: 0, winners: { "1": 10 }, refusals: { http_412: 70 } });
    expect(await probeIfMatch(store, "t/probe")).toEqual({ support: "supported", fresh: "ok", stale: "http_412" });
    expect((await raceDelete(store, "t/delete", 10)).violations).toBe(0);
  });
  test("a round nobody wins is a liveness event, not a safety violation, and does not end the check", async () => {
    const fake = new FakeS3();
    const failing = (async (input: Parameters<typeof fetch>[0], init?: RequestInit) => (init?.method === "PUT" ? new Response("<Error><Code>SlowDown</Code></Error>", { status: 503 }) : fake.fetch(input, init))) as typeof fetch;
    const r = await raceCreate(storeOn(fake, failing), "t/none", 4, 3);
    expect(r).toMatchObject({ rounds: 3, clean: 0, violations: 0, no_winner: 3, winners: { "0": 3 }, refusals: { http_503: 12 } });
  });
  test("a backend that ignores If-None-Match fails the create race", async () => {
    const { store } = fakeStore({ ignoreIfNoneMatch: true });
    const r = await raceCreate(store, "t/create", 4, 5);
    expect(r.clean).toBe(0);
    expect(r.violations).toBe(5);
    expect(r.winners).toEqual({ "4": 5 });
  });
  test("a backend that ignores If-Match is reported as ignored, and fails the CAS race", async () => {
    const { store } = fakeStore({ ignoreIfMatch: true });
    expect((await probeIfMatch(store, "t/probe")).support).toBe("ignored");
    expect((await raceCas(store, "t/cas", 4, 5)).violations).toBe(5);
  });
  test("a backend that refuses If-Match is reported as rejected", async () => {
    const { store } = fakeStore({ rejectIfMatch: true });
    expect(await probeIfMatch(store, "t/probe")).toMatchObject({ support: "rejected", fresh: "http_501" });
  });
  test("ambiguous commit: whole, absent and torn writes are told apart, and a lost acknowledged write is counted", async () => {
    const { store } = fakeStore();
    const totals = await ambiguousCommit(store, "t/amb", 1, async (prefix, tag) => {
      await store.put(key(prefix, 0), ambiguousBody(tag, 0)); // acknowledged and present
      await store.put(key(prefix, 2), ambiguousBody(tag, 2)); // in flight, landed whole
      await store.put(key(prefix, 3), ambiguousBody(tag, 3).subarray(0, 1000)); // in flight, torn
      return { started: [0, 1, 2, 3, 4], done: [0, 1] }; // 1 acknowledged but missing; 4 in flight, absent
    });
    // A child that crashed before its first write proves nothing, and is counted as such.
    expect(await ambiguousCommit(store, "t/amb-empty", 2, async () => ({ started: [], done: [] }))).toMatchObject({ kills: 2, empty_rounds: 2, in_flight: 0 });
    expect(totals).toEqual({ kills: 1, empty_rounds: 0, in_flight: 3, present_whole: 1, absent: 1, torn: 1, acknowledged_lost: 1 });
  });
});

describe("protocols", () => {
  test("a P13 batch makes the round trips the design table lists", async () => {
    const { fake, store } = fakeStore();
    await seedP13(store, "t/p13", 0, 1);
    fake.requests.length = 0;
    const retries = new Tally();
    await p13Batch(store, "t/p13", 0, 0, "b1", retries);
    // 1 inbox + 2 doc/fence + 2 shards + owner + fence + claim + journal + pointer + manifest + chunk + manifest CAS
    // + 2 doc/fence + 2x(GET+PUT) shards + commit + outcome + tombstone.
    expect(fake.requests.length).toBe(22);
    expect(fake.requests.filter((r) => r.method === "PUT").length).toBe(8);
    expect(fake.requests.filter((r) => r.status >= 400).map((r) => r.status)).toEqual([404]);
    expect(P13_ROUND_TRIPS).toBe(17);
    expect(retries.total()).toBe(0);
    // The same batch again is refused at the claim: the batch doc exists with other content.
    await expect(p13Batch(store, "t/p13", 0, 0, "b1", retries)).rejects.toMatchObject({ code: "already_exists" });
  });

  test("a log group is one payload PUT and one entry PUT, pipelined, and reads back contiguous", async () => {
    const { fake, store } = fakeStore();
    const stats: CommitStats = { commit: new Histogram(), groups: 0 };
    const writer: StreamWriter = { stream: 0, tail: 0, committed: 0 };
    const commit = entryCommit(store, "t/plog", 0, "w0");
    for (let i = 0; i < 5; i++) expect(await logGroup(store, "t/plog", writer, 4, stats, commit)).toBe("ok");
    await writer.next?.done;
    const puts = fake.requests.filter((r) => r.method === "PUT");
    // 5 entries, 5 committed payloads and the one payload already pipelined for group 6.
    expect(puts.length).toBe(11);
    expect(puts.filter((r) => r.key.includes("/payloads/")).length).toBe(6);
    expect(fake.objects.get([...fake.objects.keys()].find((k) => k.includes("/payloads/"))!)!.body.length).toBe(4 * 64 * 1024);
    expect(stats.groups).toBe(5);
    expect(await checkLog(store, "t/plog", 0)).toEqual({ entries: 5, gaps: 0, duplicate_batches: 0, by_writer: { w0: 5 } });
  });

  test("a taken sequence number is a lost commit, and a gap is detected", async () => {
    const { store } = fakeStore();
    const a = entryCommit(store, "t/log", 0, "a");
    const b = entryCommit(store, "t/log", 0, "b");
    expect(await a(1, "p", ["x"])).toBe("committed");
    expect(await b(1, "p", ["y"])).toBe("lost");
    expect(await b(3, "p", ["y"])).toBe("committed");
    expect(await b(4, "p", ["y"])).toBe("committed");
    expect(await checkLog(store, "t/log", 0)).toMatchObject({ entries: 3, gaps: 2, duplicate_batches: 1 });
  });

  test("an ambiguous entry PUT is settled by reading the entry back", async () => {
    const fake = new FakeS3();
    let failNext: "after" | "before" | undefined;
    const flaky = (async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
      if (init?.method === "PUT" && failNext) {
        const mode = failNext;
        failNext = undefined;
        if (mode === "after") await fake.fetch(input, init);
        return new Response("<Error><Code>InternalError</Code></Error>", { status: 500 });
      }
      return fake.fetch(input, init);
    }) as typeof fetch;
    const commit = entryCommit(storeOn(fake, flaky), "t/amb", 0, "a");
    failNext = "after";
    expect(await commit(1, "p", ["x"])).toBe("committed");
    failNext = "before";
    await expect(commit(2, "p", ["y"])).rejects.toMatchObject({ code: "transient" });
    // Another writer's entry at the number we failed on is a loss, not a commit.
    await entryCommit(storeOn(fake), "t/amb", 0, "b")(2, "p", ["z"]);
    failNext = "before";
    expect(await commit(2, "p", ["y"])).toBe("lost");
  });

  test("racing writers: exactly one commit per sequence number on a correct backend", async () => {
    const { store } = fakeStore();
    const r = await racingWriters(store, "t/race", 0, 4, 10);
    expect(r.violations).toBe(0);
    expect(r.log).toMatchObject({ entries: 40, gaps: 0, duplicate_batches: 0 });
    expect(r.lost_sequences).toBeGreaterThan(0);
    expect(Object.values(r.believed)).toEqual([10, 10, 10, 10]);
  });

  test("racing writers on a backend that ignores If-None-Match is a violation", async () => {
    const { store } = fakeStore({ ignoreIfNoneMatch: true });
    const r = await racingWriters(store, "t/race", 0, 4, 10);
    expect(r.violations).toBeGreaterThan(0);
    expect(r.log.entries).toBeLessThan(40);
  });

  test("the DynamoDB commit advances the head once per sequence number", async () => {
    const fake = new FakeDynamo();
    const dynamo = new Dynamo({ table: "ledger_bench", region: "us-west-2" }, () => Promise.resolve(CREDS), fake.fetch);
    const cancelled = new Tally();
    const commit = dynamoCommit(dynamo, "stream", cancelled);
    expect(await commit(1, "p1", ["a", "b"])).toBe("committed");
    expect(await commit(1, "p1", ["c"])).toBe("lost");
    expect(await commit(2, "p2", ["c"])).toBe("committed");
    // A batch id that was already committed cancels the whole transaction: the head does not move.
    expect(await commit(3, "p3", ["a"])).toBe("lost");
    expect(await dynamoHeadSeq(dynamo, "stream")).toBe(2);
    expect((await dynamo.queryPartition("stream", "b#")).length).toBe(3);
    expect(cancelled.toJSON()).toEqual({ ddb_TransactionCanceledException_ConditionalCheckFailed: 2 });
  });

  test("a DynamoDB transaction that applied but whose answer was lost is a commit, not a lost sequence", async () => {
    const fake = new FakeDynamo();
    let mode: "applied_then_500" | "500" | "reset_after_apply" | undefined;
    const flaky = (async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
      const transact = new Headers(init?.headers).get("x-amz-target")?.endsWith("TransactWriteItems");
      if (transact && mode) {
        const m = mode;
        mode = undefined;
        if (m !== "500") await fake.fetch(input, init);
        if (m === "reset_after_apply") throw Object.assign(new Error("socket"), { code: "ECONNRESET" });
        return new Response(JSON.stringify({ __type: "com.amazonaws.dynamodb.v20120810#InternalServerError" }), { status: 500 });
      }
      return fake.fetch(input, init);
    }) as typeof fetch;
    const dynamo = new Dynamo({ table: "ledger_bench", region: "us-west-2" }, () => Promise.resolve(CREDS), flaky);
    const commit = dynamoCommit(dynamo, "s", new Tally());
    mode = "applied_then_500";
    expect(await commit(1, "p", ["a"])).toBe("committed");
    mode = "reset_after_apply";
    expect(await commit(2, "p", ["b"])).toBe("committed");
    // Nothing applied: the failure stands, and the head has not moved.
    mode = "500";
    await expect(commit(3, "p", ["c"])).rejects.toMatchObject({ exception: "InternalServerError" });
    expect(await dynamoHeadSeq(dynamo, "s")).toBe(2);
    // Another writer's entry at that number is a loss.
    await dynamoCommit(dynamo, "s", new Tally())(3, "other", ["d"]);
    mode = "500";
    expect(await commit(3, "p", ["c"])).toBe("lost");
  });

  test("commit latency is the payload's and the commit's own time, not the wait for the writer's turn", async () => {
    const { store } = fakeStore();
    const stats: CommitStats = { commit: new Histogram(), groups: 0 };
    const writer: StreamWriter = { stream: 0, tail: 0, committed: 0 };
    const commit = entryCommit(store, "t/idle", 0, "w0");
    await logGroup(store, "t/idle", writer, 1, stats, commit);
    // The pipelined payload finishes long before its writer is served again.
    await writer.next!.done;
    await new Promise((r) => setTimeout(r, 150));
    await logGroup(store, "t/idle", writer, 1, stats, commit);
    await writer.next?.done;
    expect(stats.commit.summary().max_ms!).toBeLessThan(100);
  });
});

describe("steps end to end on the fakes", () => {
  test("steps 1, 2, 3 and 6 emit their cells with no violation and no error", async () => {
    const fake = new FakeS3();
    const dynamo = new FakeDynamo();
    const { ctx, cells, meter } = context(fake, dynamo);
    await step1(ctx, "R2");
    await step2(ctx, "R2", async () => ({ started: [], done: [] }));
    await step3P13(ctx, "R2");
    await step3Log(ctx, "R2", "plog");
    await step3Race(ctx, "R2");
    await step3Log(ctx, "R2", "pddb");
    await step6(ctx, "R2");
    const names = new Set(cells.map((c) => `${c.step}:${c.cell}`));
    for (const expected of ["1:put", "1:get", "1:head", "1:list", "2:create_if_absent", "2:race_create", "2:if_match_support", "2:cas", "2:race_cas", "2:race_delete", "2:ambiguous_commit", "3:p13", "3:plog", "3:plog_readback", "3:racing_writers", "3:pddb", "3:pddb_readback", "6:pool_ramp"]) expect(names).toContain(expected);
    expect(violations(cells)).toBe(0);
    for (const c of cells) expect((c.result as { errors?: Record<string, number> }).errors ?? {}).toEqual({});
    // Concurrencies x (2 sizes x put/get + head + list) + the 256 KiB pair + the SSE-S3 comparison.
    expect(cells.filter((c) => c.step === 1).length).toBe(TINY.concurrencies.length * 6 + 2 + 4);
    expect(cells.filter((c) => c.cell === "plog").length).toBe(8);
    const list = cells.find((c) => c.cell === "list")!.result as { outcomes: Record<string, number> };
    expect(Object.keys(list.outcomes)).toEqual(["ok"]);
    expect(meter.used).toBe(fake.requests.length + dynamo.calls);
    // Everything written is under the lane's prefix.
    for (const k of fake.objects.keys()) expect(k.startsWith("bench/test/r2/")).toBe(true);
  }, 60_000);

  test("step 5 passes on a correct table", async () => {
    const dynamo = new FakeDynamo();
    const { ctx, cells } = context(new FakeS3(), dynamo, { ...TINY, cellSeconds: 0.1 });
    await step5(ctx);
    expect(cells.filter((c) => c.cell === "transact_write").length).toBe(6);
    expect(cells.filter((c) => c.cell === "hot_head").length).toBe(4);
    expect(violations(cells)).toBe(0);
    const idem = cells.find((c) => c.cell === "idempotency_replay")!.result as Record<string, unknown>;
    expect(idem).toMatchObject({ replay: "ok", same_token_other_payload: "ddb_IdempotentParameterMismatchException", applied: 1 });
  }, 60_000);

  test("a spent request budget ends the cells early instead of running on", async () => {
    const fake = new FakeS3();
    const meter = new RequestMeter(200);
    const cells: Cell[] = [];
    const store = storeOn(fake, meteredFetch(meter, "R2", fake.fetch));
    const ctx: Ctx = { profile: TINY, meter, root: "bench/test/r2", stores: { R2: store }, sseS3: {}, expressStats: {}, emit: (c) => cells.push(c), log: () => {} };
    await step6(ctx, "R2").catch(() => {});
    expect(meter.used).toBe(200);
    expect(fake.requests.length).toBe(200);
  });
});
