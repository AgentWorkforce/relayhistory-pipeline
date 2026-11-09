/**
 * The Output and digest routes over an injected backend (output-backend.ts, digest-backend.ts):
 * every readiness code in its 501, the schema-missing 501, a backend error as a generic 503, the
 * 200 bodies, that an injected backend never touches ClickHouse, and that without one the
 * routes still read through ClickHouse exactly as before. Plus the backend flags (config.ts,
 * lambda.ts factsBackends). No database.
 */
import { describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { memoryLiveness } from "./support.ts";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import type { OutputBackend, OutputReadiness, SessionOutput } from "../output-backend.ts";
import type { DigestBackend } from "../digest-backend.ts";
import type { DailyDigest } from "../digest-query.ts";
import { DigestDirectoryUnavailable } from "../digest-query.ts";
import type { Directory } from "../directory.ts";
import { parseFactsBackend, readApiRouteEnabled, READ_API_DIGEST_BACKEND, READ_API_OUTPUT_BACKEND } from "../config.ts";
import { factsBackends } from "../lambda.ts";
import { FactsReadDeadline } from "../postgres/output-query.ts";

const ID: VerifiedIdentity = { userId: "u-1", orgId: "org-t", workspaceId: "ws-t", scopes: ["rth:read"] };
const WS = ID.workspaceId;
const selects: string[] = [];
const recordingCh = (rows: Record<string, unknown>[] = []): ClickHouseClient => ({
  database: "x",
  async command() { throw new Error("no command"); },
  async insert() { throw new Error("no insert"); },
  async select<T>(sql: string) { selects.push(sql); return rows as T[]; },
});
const directory: Directory = { list: async () => [], activity: async () => [] };
const OUTPUT: SessionOutput = { agentMessages: 3, commits: 2, linesAdded: 7, linesRemoved: 1, commitsWithoutLineCounts: 0 };
const DIGEST: DailyDigest = {
  stats: {
    sessionCount: 1, eventCount: 2, projectCount: 1, epicCount: 1, totalCostUsdMicros: 5, totalInputTokens: 6, totalOutputTokens: 7,
    models: ["m"], filesTouchedCount: 1, topFiles: [{ path: "a", sessionCount: 1 }], outcomes: { committed: 0, reverted: 0, unknown: 1 },
  },
  projects: [], epics: [], sessions: [],
};
const READINESS: OutputReadiness[] = ["backfill_incomplete", "before_watermark", "session_partial", "session_unindexed"];

type Calls = { readiness: Array<{ until: Date; untilExclusive?: boolean }>; reads: number };
function outputBackend(readiness: OutputReadiness | Error, read: SessionOutput | Error = OUTPUT, calls: Calls = { readiness: [], reads: 0 }): OutputBackend {
  return {
    kind: "postgres",
    async windowReadiness(_o, _w, _s, until, opts) {
      calls.readiness.push({ until, untilExclusive: opts?.untilExclusive });
      if (readiness instanceof Error) throw readiness;
      return readiness;
    },
    async readOutput() { calls.reads++; if (read instanceof Error) throw read; return read; },
    isSchemaMissing: (e) => (e as { code?: string })?.code === "42883",
  };
}
function digestBackend(readiness: OutputReadiness | Error, built: DailyDigest | Error = DIGEST, calls: Calls = { readiness: [], reads: 0 }): DigestBackend {
  const o = outputBackend(readiness, OUTPUT, calls);
  return {
    kind: "postgres",
    windowReadiness: o.windowReadiness,
    async buildDailyDigest() { calls.reads++; if (built instanceof Error) throw built; return built; },
    isSchemaMissing: o.isSchemaMissing,
  };
}

function api(over: { ch?: ClickHouseClient; output?: OutputBackend | "plain" | false; digest?: DigestBackend | "plain" | false }) {
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return ID;
    },
    // The self-host service binds the portable search adapter and no ClickHouse.
    ...(over.ch ? { ch: over.ch } : { adapter: {} as never }),
    liveness: memoryLiveness().liveness,
    completeSinceMs: 0,
    directory,
    directoryWorkspaces: new Set([WS]),
    ...(over.output === false || over.output === undefined ? {} : { output: over.output === "plain" ? { enabled: true as const } : { enabled: true as const, backend: over.output } }),
    ...(over.digest === false || over.digest === undefined ? {} : { digest: over.digest === "plain" ? {} : { backend: over.digest } }),
  });
  return (path: string) => app.request(path, { headers: { authorization: "Bearer good" } });
}
const body = async (r: Response) => (await r.json()) as Record<string, any>;
const OUT = `/v1/sessions/output?workspace=${WS}&since=2026-11-05T00:00:00Z&until=2026-11-05T23:59:59.999Z`;
const DAY = `/v1/digest/daily?workspace=${WS}&date=2026-11-05&tz=UTC`;
const schemaError = Object.assign(new Error("function does not exist"), { code: "42883" });

describe("GET /v1/sessions/output over a backend", () => {
  test("every readiness code is its own 501, in the route's naming; nothing is read", async () => {
    for (const code of READINESS) {
      const calls: Calls = { readiness: [], reads: 0 };
      const r = await api({ output: outputBackend(code, OUTPUT, calls) })(OUT);
      expect(r.status).toBe(501);
      expect((await body(r)).error.reason).toBe(`output_${code}`);
      expect(calls.reads).toBe(0);
      expect(calls.readiness[0]!.untilExclusive).toBeUndefined(); // Output's window is inclusive
    }
  });
  test("ready: 200 with the backend's output; never a ClickHouse read even when ch is present", async () => {
    selects.length = 0;
    const r = await api({ ch: recordingCh(), output: outputBackend("ready") })(OUT);
    expect(r.status).toBe(200);
    const b = await body(r);
    expect(b.output).toEqual(OUTPUT);
    expect(b.since).toBe("2026-11-05T00:00:00.000Z");
    expect(selects).toEqual([]);
  });
  test("schema missing (readiness or read) is 501 output_schema_missing; a deadline or other error is a 503", async () => {
    expect((await body(await api({ output: outputBackend(schemaError) })(OUT))).error.reason).toBe("output_schema_missing");
    expect((await body(await api({ output: outputBackend("ready", schemaError) })(OUT))).error.reason).toBe("output_schema_missing");
    for (const error of [new FactsReadDeadline(), Object.assign(new Error("permission denied"), { code: "42501" })]) {
      const r = await api({ output: outputBackend("ready", error) })(OUT);
      expect(r.status).toBe(503);
      expect((await body(r)).error.code).toBe("backend_unavailable");
    }
  });
  test("not configured: no deps.output, or neither a backend nor ch", async () => {
    expect((await body(await api({ output: false })(OUT))).error.reason).toBe("output_not_configured");
    expect((await body(await api({ output: "plain" })(OUT))).error.reason).toBe("output_not_configured");
    expect((await body(await api({ output: false, ch: recordingCh() })(OUT))).error.reason).toBe("output_not_configured");
  });
  test("the allowlist and the 400s come before the backend", async () => {
    const calls: Calls = { readiness: [], reads: 0 };
    const app = createReadApi({
      verify: async () => ID, adapter: {} as never, liveness: memoryLiveness().liveness, completeSinceMs: 0, directory, directoryWorkspaces: new Set(["other"]),
      output: { enabled: true, backend: outputBackend("ready", OUTPUT, calls) },
    });
    const r = await app.request(OUT, { headers: { authorization: "Bearer good" } });
    expect(r.status).toBe(501);
    expect((await body(r)).error.reason).toBe("output_workspace_not_enabled");
    expect((await api({ output: outputBackend("ready", OUTPUT, calls) })(`/v1/sessions/output?workspace=${WS}&since=x&until=y`)).status).toBe(400);
    expect(calls.readiness).toEqual([]);
  });
  test("without a backend the route still reads ClickHouse (the default, unchanged)", async () => {
    selects.length = 0;
    const r = await api({ ch: recordingCh([]), output: "plain" })(OUT);
    // No watermark row: the ClickHouse readiness answers backfill_incomplete from its first query.
    expect((await body(r)).error.reason).toBe("output_backfill_incomplete");
    expect(selects).toHaveLength(1);
    expect(selects[0]).toContain("complete_since");
  });
});

describe("GET /v1/digest/daily over a backend", () => {
  test("every readiness code is its own 501; the day window is half-open", async () => {
    for (const code of READINESS) {
      const calls: Calls = { readiness: [], reads: 0 };
      const r = await api({ digest: digestBackend(code, DIGEST, calls) })(DAY);
      expect(r.status).toBe(501);
      expect((await body(r)).error.reason).toBe(`digest_${code}`);
      expect(calls.reads).toBe(0);
      expect(calls.readiness[0]).toEqual({ until: new Date("2026-11-06T00:00:00Z"), untilExclusive: true });
    }
  });
  test("ready: 200 with the backend's digest, the omitted header and a narrative object", async () => {
    selects.length = 0;
    const r = await api({ ch: recordingCh(), digest: digestBackend("ready") })(DAY);
    expect(r.status).toBe(200);
    expect(r.headers.get("X-Sessions-Digest-Omitted")).toBe("outcomes.reverted");
    const b = await body(r);
    expect(b.stats).toEqual(DIGEST.stats);
    expect(b.window).toEqual({ since: "2026-11-05T00:00:00.000Z", until: "2026-11-06T00:00:00.000Z" });
    expect(typeof b.narrative).toBe("object");
    expect(selects).toEqual([]);
  });
  test("schema missing is 501 digest_schema_missing; the directory 501; other errors 503", async () => {
    expect((await body(await api({ digest: digestBackend(schemaError) })(DAY))).error.reason).toBe("digest_schema_missing");
    expect((await body(await api({ digest: digestBackend("ready", schemaError) })(DAY))).error.reason).toBe("digest_schema_missing");
    expect((await body(await api({ digest: digestBackend("ready", new DigestDirectoryUnavailable()) })(DAY))).error.reason).toBe("digest_directory_unavailable");
    const r = await api({ digest: digestBackend("ready", new FactsReadDeadline()) })(DAY);
    expect(r.status).toBe(503);
  });
  test("not configured: no deps.digest, or neither a backend nor ch; without a backend ClickHouse is read", async () => {
    expect((await body(await api({ digest: false })(DAY))).error.reason).toBe("digest_not_configured");
    expect((await body(await api({ digest: "plain" })(DAY))).error.reason).toBe("digest_not_configured");
    selects.length = 0;
    expect((await body(await api({ ch: recordingCh([]), digest: "plain" })(DAY))).error.reason).toBe("digest_backfill_incomplete");
    expect(selects).toHaveLength(1);
  });
});

describe("READ_API_OUTPUT_BACKEND / READ_API_DIGEST_BACKEND", () => {
  test("unset, empty or clickhouse = clickhouse; postgres = postgres", () => {
    for (const name of [READ_API_OUTPUT_BACKEND, READ_API_DIGEST_BACKEND] as const) {
      expect(parseFactsBackend({}, name)).toBe("clickhouse");
      expect(parseFactsBackend({ [name]: "" }, name)).toBe("clickhouse");
      expect(parseFactsBackend({ [name]: "clickhouse" }, name)).toBe("clickhouse");
      expect(parseFactsBackend({ [name]: "postgres" }, name)).toBe("postgres");
    }
  });
  test("any other value throws naming the variable, never echoing the value", () => {
    for (const value of ["Postgres", " postgres", "pg", "neon", "1", "synthetic-value-x"]) {
      for (const name of [READ_API_OUTPUT_BACKEND, READ_API_DIGEST_BACKEND] as const) {
        let error: Error | null = null;
        try { parseFactsBackend({ [name]: value }, name); } catch (e) { error = e as Error; }
        expect(error?.message).toStartWith(`session read api: ${name} must be`);
        if (value.length > 2) expect(error!.message).not.toContain(value);
      }
    }
  });
  test("factsBackends: postgres only on the service entry; the Lambda refuses it; bad values refuse both", () => {
    expect(factsBackends({}, "lambda")).toEqual({ output: "clickhouse", digest: "clickhouse" });
    expect(factsBackends({ READ_API_OUTPUT_BACKEND: "postgres", READ_API_DIGEST_BACKEND: "postgres" }, "service")).toEqual({ output: "postgres", digest: "postgres" });
    expect(() => factsBackends({ READ_API_OUTPUT_BACKEND: "postgres" }, "lambda")).toThrow(/served only by the self-host service/);
    expect(() => factsBackends({ READ_API_DIGEST_BACKEND: "postgres" }, "lambda")).toThrow(/served only by the self-host service/);
    expect(() => factsBackends({ READ_API_DIGEST_BACKEND: "nope" }, "service")).toThrow(/READ_API_DIGEST_BACKEND must be/);
  });
  test("the ENABLED flags keep their \"1\" semantics", () => {
    expect(readApiRouteEnabled("1")).toBe(true);
    expect(readApiRouteEnabled(" 1 ")).toBe(true);
    for (const v of [undefined, "", "0", "true", "yes", "11"]) expect(readApiRouteEnabled(v)).toBe(false);
  });
});
