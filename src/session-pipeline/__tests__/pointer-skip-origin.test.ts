/**
 * SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED: with the flag on, the pointer stage ignores the sessionless
 * per-origin bucket (h=_none/s=_origin) before any lookup, for put and delete events alike; with it off or
 * unset the same key takes the existing path. Pure fakes, no database. Synthetic, secret-free data only.
 */
import { describe, expect, test } from "bun:test";
import { indexPointer, type PointerDeps } from "../pointer.ts";
import { processBatch, type ConsumerDeps } from "../consumer.ts";
import { lambdaEnv, pointerSkipOriginBucketFromEnv, runtimeFromSecret } from "../lambda.ts";
import { NO_SESSION_HARNESS, NO_SESSION_ID } from "../../session-ingest/writer.ts";

const keyOf = (harness: string, session: string, rest = "manifest.json") =>
  `96bulls/org-synthetic/v1/ws=ws-synthetic/m=m-synthetic/h=${harness}/d=2026/11/06/s=${session}/${rest}`;
const ORIGIN_KEY = keyOf(NO_SESSION_HARNESS, NO_SESSION_ID);

/** Deps where every member throws when touched, except the flag itself (and `allow`, read by the test's caller). */
function untouchable(flag: boolean, allow: Record<string, unknown> = {}): PointerDeps {
  return new Proxy({} as PointerDeps, {
    get: (_t, p) => {
      if (p === "skipOriginBucket") return flag ? true : undefined;
      if (typeof p === "string" && p in allow) return allow[p];
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

test("the constants are the writer's spelling", () => {
  expect([NO_SESSION_HARNESS, NO_SESSION_ID]).toEqual(["_none", "_origin"]);
});

describe("indexPointer with the flag on", () => {
  test("an h=_none/s=_origin manifest put is ignored before any store or database call", async () => {
    expect(await indexPointer({ key: ORIGIN_KEY }, untouchable(true))).toEqual({ status: "ignored", reason: "no_session_origin" });
  });
  test("a delete event for the same key is ignored too", async () => {
    expect(await indexPointer({ key: ORIGIN_KEY, deleted: true }, untouchable(true))).toEqual({ status: "ignored", reason: "no_session_origin" });
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

describe("processBatch with the flag on", () => {
  test("the message is acked, logged as ignored, and no large route, search or replay runs", async () => {
    const touched: string[] = [];
    const tripwire = (name: string) => new Proxy({}, { get: (_t, p) => { touched.push(`${name}.${String(p)}`); throw new Error(`${name} touched`); } });
    const deps = {
      pointer: untouchable(true, { reader: { provider: "s3" } }),
      search: tripwire("search"),
      large: tripwire("large"),
      replay: tripwire("replay"),
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
      result = await processBatch([{ messageId: "msg-synthetic-1", body: JSON.stringify({ reconcile: { key: ORIGIN_KEY } }) }], deps);
    } finally {
      process.stdout.write = out;
      process.stderr.write = err;
    }
    expect(result.batchItemFailures).toEqual([]);
    expect(touched).toEqual([]);
    expect(lines.filter((l) => l.stage === "object-landed")).toEqual([
      expect.objectContaining({ level: "info", message: "object event ignored", reason: "no_session_origin" }),
    ]);
    expect(lines.some((l) => l.stage === "session-indexed" || l.message === "search index failed")).toBe(false);
  });
});

describe("SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED in the Lambda", () => {
  const base = {
    AWS_REGION: "us-east-1", PIPELINE_RAW_S3_BUCKET: "b", PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true",
    SESSION_PIPELINE_SECRET_ARN: "synthetic", SESSION_PIPELINE_QUEUE_URL: "https://sqs.invalid/q",
  };
  const secret = JSON.stringify({ hoopdb_url: "postgresql://synthetic.invalid/fixture", clickhouse: { url: "http://127.0.0.1:1", user: "u", password: "p", database: "d" } });
  test("unset/\"\"/false: no field, no pointer flag; true: deps.pointer.skipOriginBucket; strict grammar", async () => {
    for (const [value, expected] of [[undefined, false], ["", false], ["false", false], ["true", true]] as const) {
      const env = lambdaEnv({ ...base, ...(value === undefined ? {} : { SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED: value }) });
      expect("pointerSkipOriginBucketEnabled" in env).toBe(expected);
      expect(pointerSkipOriginBucketFromEnv(value === undefined ? {} : { SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED: value })).toBe(expected);
      const rt = runtimeFromSecret(env, secret, "synthetic-generation");
      try {
        expect("skipOriginBucket" in rt.deps.pointer).toBe(expected);
        if (expected) expect(rt.deps.pointer.skipOriginBucket).toBe(true);
      } finally {
        await (rt.deps.pointer.db as unknown as { end(): Promise<void> }).end();
      }
    }
    for (const bad of ["1", "TRUE", " true", "true ", "on", "yes"]) {
      expect(() => pointerSkipOriginBucketFromEnv({ SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED: bad })).toThrow("SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED must be true or false");
      expect(() => lambdaEnv({ ...base, SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED: bad })).toThrow("SESSION_POINTER_SKIP_ORIGIN_BUCKET_ENABLED must be true or false");
    }
  });
});
