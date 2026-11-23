/**
 * SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED: with the flag on, the pointer stage ignores the sessionless
 * per-origin bucket (h=_none/s=_origin) before any lookup, for put and delete events alike; with it off or
 * unset the same key takes the existing path. Pure fakes, no database. Synthetic, secret-free data only.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { indexPointer, NO_SESSION_HARNESS, NO_SESSION_ID, type PointerDeps } from "../pointer.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { lambdaEnv, pointerFlagDeps, pointerSkipOriginBucketFromEnv } from "../lambda.ts";
import { logLine } from "../../shared/log.ts";

const keyOf = (harness: string, session: string, rest = "manifest.json") =>
  `96bulls/org-synthetic/v1/ws=ws-synthetic/m=m-synthetic/h=${harness}/d=2026/11/06/s=${session}/${rest}`;
const ORIGIN_KEY = keyOf(NO_SESSION_HARNESS, NO_SESSION_ID);
const IGNORED = { status: "ignored", reason: "no_session_origin" };

/** Deps where every member throws when touched, except the flag itself. */
function untouchable(flag: boolean): PointerDeps {
  return new Proxy({} as PointerDeps, {
    get: (_t, p) => {
      if (p === "skipOriginBucket") return flag ? true : undefined;
      throw new Error(`pointer dependency touched: ${String(p)}`);
    },
  });
}

/** Deps whose first downstream call (the org lookup) is recorded and answers "no org", so the existing path ends there. */
function firstCallDeps(flag: boolean | undefined) {
  const orgLookups: string[] = [];
  const deps = {
    reader: { bucket: undefined },
    orgs: { orgFor: async (loc: { harness: string; sessionId: string }) => { orgLookups.push(`${loc.harness}/${loc.sessionId}`); return null; } },
    ...(flag === undefined ? {} : { skipOriginBucket: flag }),
  };
  return { deps: new Proxy(deps as unknown as PointerDeps, {
    get: (t, p) => {
      if (p in t) return (t as unknown as Record<string | symbol, unknown>)[p];
      if (p === "skipOriginBucket") return undefined;
      throw new Error(`pointer dependency touched: ${String(p)}`);
    },
  }), orgLookups };
}

test("the constants are the ingest writer's spelling", () => {
  expect([NO_SESSION_HARNESS, NO_SESSION_ID]).toEqual(["_none", "_origin"]);
});

describe("indexPointer with the flag on", () => {
  test("an h=_none/s=_origin manifest put is ignored before any store, org, lock or database call", async () => {
    expect(await indexPointer({ key: ORIGIN_KEY }, untouchable(true))).toEqual(IGNORED);
  });
  test("a delete event for the same key is ignored too", async () => {
    expect(await indexPointer({ key: ORIGIN_KEY, deleted: true }, untouchable(true))).toEqual(IGNORED);
  });
  test("a non-manifest object under the bucket is ignored with the same reason (the skip runs before the manifest check)", async () => {
    expect(await indexPointer({ key: keyOf(NO_SESSION_HARNESS, NO_SESSION_ID, "chunks/000000.jsonl.gz") }, untouchable(true))).toEqual(IGNORED);
  });
  test("a normal session key is unaffected: it reaches the org lookup", async () => {
    const { deps, orgLookups } = firstCallDeps(true);
    expect(await indexPointer({ key: keyOf("claude", "sess-synthetic-1") }, deps)).toEqual({ status: "ignored", reason: "org_unresolved" });
    expect(orgLookups).toEqual(["claude/sess-synthetic-1"]);
  });
  for (const [label, harness, session] of [
    ["harness _none with another session id", "_none", "sess-synthetic-2"],
    ["session _origin under a real harness", "codex", "_origin"],
    ["session _origin2 (substring only)", "_none", "_origin2"],
    ["harness x_none (substring only)", "x_none", "_origin"],
    ["harness _none_ (substring only)", "_none_", "_origin"],
    ["session x_origin (substring only)", "_none", "x_origin"],
    ["harness _NONE (case variant)", "_NONE", "_origin"],
    ["session _Origin (case variant)", "_none", "_Origin"],
  ] as const) {
    test(`not skipped: ${label}`, async () => {
      const { deps, orgLookups } = firstCallDeps(true);
      expect(await indexPointer({ key: keyOf(harness, session) }, deps)).toEqual({ status: "ignored", reason: "org_unresolved" });
      expect(orgLookups).toEqual([`${harness}/${session}`]);
    });
  }
});

describe("indexPointer with the flag off or unset", () => {
  for (const flag of [undefined, false] as const) {
    test(`flag ${String(flag)}: the h=_none/s=_origin key takes the existing path (org lookup first)`, async () => {
      const { deps, orgLookups } = firstCallDeps(flag);
      expect(await indexPointer({ key: ORIGIN_KEY }, deps)).toEqual({ status: "ignored", reason: "org_unresolved" });
      expect(orgLookups).toEqual(["_none/_origin"]);
      const del = firstCallDeps(flag);
      expect(await indexPointer({ key: ORIGIN_KEY, deleted: true }, del.deps)).toEqual({ status: "ignored", reason: "org_unresolved" });
      expect(del.orgLookups).toEqual(["_none/_origin"]);
    });
  }
});

/** Run processBatch with every downstream dep a tripwire, capturing the JSON lines the real log path writes. */
async function batchThroughTripwires(body: string) {
  const touched: string[] = [];
  const tripwire = (name: string) =>
    new Proxy({}, { get: (_t, p) => { touched.push(`${name}.${String(p)}`); throw new Error(`${name} touched`); } });
  const deps = {
    pointer: untouchable(true),
    search: tripwire("search"),
    requeue: async () => { touched.push("requeue"); },
  } as unknown as ConsumerDeps;
  const lines: Array<Record<string, unknown>> = [];
  const out = process.stdout.write, err = process.stderr.write;
  const tap = (w: typeof process.stdout.write, stream: NodeJS.WriteStream) =>
    ((chunk: unknown, ...rest: unknown[]) => {
      for (const l of String(chunk).split("\n")) if (l.startsWith("{")) try { lines.push(JSON.parse(l)); } catch {}
      return (w as (...a: unknown[]) => boolean).call(stream, chunk, ...rest);
    }) as typeof process.stdout.write;
  process.stdout.write = tap(out, process.stdout);
  process.stderr.write = tap(err, process.stderr);
  let result;
  try {
    result = await processBatch([{ messageId: "msg-synthetic-1", body }], deps);
  } finally {
    process.stdout.write = out;
    process.stderr.write = err;
  }
  return { result, touched, lines };
}

const s3Body = (eventName: string) => JSON.stringify({
  Records: [{
    eventSource: "aws:s3", eventName, eventTime: "2026-11-06T00:00:00.000Z",
    s3: { bucket: { name: "bucket-synthetic" }, object: { key: encodeURIComponent(ORIGIN_KEY).replace(/%2F/g, "/"), size: 10, eTag: "etag-synthetic", sequencer: "0A" } },
  }],
});

describe("processBatch with the flag on", () => {
  for (const [label, body] of [
    ["an S3 put", s3Body("ObjectCreated:Put")],
    ["an S3 delete", s3Body("ObjectRemoved:Delete")],
    ["a reconcile body", JSON.stringify({ reconcile: { key: ORIGIN_KEY } })],
  ] as const) {
    test(`${label} of the origin key is handled: no failure, no requeue, no search, logged as ignored`, async () => {
      const { result, touched, lines } = await batchThroughTripwires(body);
      expect(result.batchItemFailures).toEqual([]);
      expect(touched).toEqual([]);
      expect(lines.filter((l) => l.stage === "object-landed")).toEqual([
        expect.objectContaining({ level: "info", message: "object event ignored", reason: "no_session_origin" }),
      ]);
      expect(lines.some((l) => l.stage === "session-indexed" || l.stage === "search-index")).toBe(false);
    });
  }
  test("the reason survives the real log filter (allowlisted key, no value rule)", () => {
    expect(JSON.parse(logLine("info", "object event ignored", { stage: "object-landed", reason: "no_session_origin" })).reason).toBe("no_session_origin");
  });
});

describe("SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED in the Lambda", () => {
  // No SEARCH_INDEX_ENABLED: the pointer flag does not need it.
  const base = {
    AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
    SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q",
  };
  test("unset/\"\"/false: env and pointer deps exactly as without it; true: deps.skipOriginBucket, without SEARCH_INDEX_ENABLED", () => {
    const plain = lambdaEnv(base);
    expect(plain.searchEnabled).toBe(false);
    for (const [value, expected] of [[undefined, false], ["", false], ["false", false], ["true", true]] as const) {
      const vars = value === undefined ? {} : { SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED: value };
      const env = lambdaEnv({ ...base, ...vars });
      expect(pointerSkipOriginBucketFromEnv(vars)).toBe(expected);
      expect("pointerSkipOriginBucketEnabled" in env).toBe(expected);
      expect(env).toEqual(expected ? { ...plain, pointerSkipOriginBucketEnabled: true } : plain);
      const flagDeps = pointerFlagDeps(env);
      expect("skipOriginBucket" in flagDeps).toBe(expected);
      expect(flagDeps).toEqual(expected ? { skipOriginBucket: true } : {});
    }
  });
  test("any other value throws naming the variable, from the parser and from lambdaEnv", () => {
    for (const bad of ["1", "TRUE", "yes", " true", "true ", "on", "0"]) {
      expect(() => pointerSkipOriginBucketFromEnv({ SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED: bad })).toThrow("SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED must be true or false");
      expect(() => lambdaEnv({ ...base, SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED: bad })).toThrow("SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED must be true or false");
    }
  });
  test("init spreads the flag deps into the pointer deps (source check; init needs AWS)", () => {
    const source = readFileSync(join(import.meta.dir, "../lambda.ts"), "utf8");
    const start = source.indexOf("pointer: {", source.indexOf("async function init("));
    const pointer = source.slice(start, source.indexOf("},", source.indexOf("fanout,", start)) + 2);
    expect(pointer).toContain("...pointerFlagDeps(env),");
  });
});
