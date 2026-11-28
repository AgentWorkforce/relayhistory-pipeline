/**
 * The outcome alarm's metric filters against the lines the read API really writes: the real app
 * (createReadApi and its "*" request middleware), wired to the real logger exactly as the Lambda
 * wires it (lambda.ts: `log: (fields) => log.info("session read", fields)`), with stdout captured.
 * Synthetic replay 200/501/503 (and their separate reason lines), healthy directory traffic and a
 * flag-disabled route go through it; the pinned filter patterns are then evaluated over the
 * captured lines.
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { log } from "../../../src/shared/log.ts";
import { AuthError, createReadApi, type VerifiedIdentity } from "../../../src/session-read-api/app.ts";
import type { Directory } from "../../../src/session-read-api/directory.ts";
import { memoryLiveness } from "../../../src/session-read-api/__tests__/support.ts";
import { ClickHouseError, type ClickHouseClient } from "../../../src/session-pipeline/vendor/search-index/clickhouse.ts";
import {
  OUTCOME_ALARM_DEFAULTS,
  READ_REPLAY_ROUTE,
  REPLAY_REQUESTS_FILTER_PATTERN,
  REPLAY_UNAVAILABLE_FILTER_PATTERN,
  sessionReadApiFunctionProps,
  sessionReadApiNames,
  sessionReadApiOutcomeAlarm,
} from "../session-read-api.ts";

const ID: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };

/**
 * The CloudWatch Logs JSON filter subset the two patterns use: `{ T && T ... && (T || T) }`, where T
 * is `$.field = "string"` (exact), `$.field = <number>` or `$.field >= <number>` (numeric fields only).
 * A missing field, or one of the other type, never matches. The parser refuses anything else, so a
 * pattern edit outside this subset fails here instead of being half-evaluated.
 */
function matcher(pattern: string): (line: Record<string, unknown>) => boolean {
  const body = /^\{ (.*) \}$/.exec(pattern)?.[1];
  if (!body) throw new Error(`not a JSON filter: ${pattern}`);
  const term = (t: string) => {
    const m = /^\$\.([a-z_]+) (=|>=) (?:"([^"*]*)"|(-?\d+))$/.exec(t.trim());
    if (!m) throw new Error(`unsupported term: ${t}`);
    const [, field, op, str, num] = m;
    return (line: Record<string, unknown>) => {
      const v = line[field!];
      if (str !== undefined) return op === "=" && typeof v === "string" && v === str;
      if (typeof v !== "number") return false;
      return op === "=" ? v === Number(num) : v >= Number(num);
    };
  };
  const clauses = body.split(" && ").map((c) => {
    const alt = /^\((.*)\)$/.exec(c);
    const terms = (alt ? alt[1]!.split(" || ") : [c]).map(term);
    return (line: Record<string, unknown>) => terms.some((t) => t(line));
  });
  return (line) => clauses.every((c) => c(line));
}

/** Answers what each synthetic request needs: partial sessions, a failing ClickHouse, or empty pages. */
function stubCh(mode: { partial?: string[]; fail?: boolean }): ClickHouseClient {
  return {
    database: "relay_search",
    async command() {},
    async insert() {},
    async select<T>(query: string, options?: { params?: Record<string, unknown> }) {
      if (mode.fail) throw new ClickHouseError(500, "210", null);
      if (query.includes("search_ingest_session_state")) {
        const session = options?.params?.session;
        return (mode.partial ?? []).filter((id) => session === undefined || id === session).map((session_id) => ({ session_id })) as T[];
      }
      return [] as T[];
    },
  } as ClickHouseClient;
}

const directory: Directory = {
  async list() { return []; },
  async activity() { return []; },
  async query() { return []; },
  async queryActivity() { return []; },
};

function app(mode: { partial?: string[]; fail?: boolean; retryable?: boolean } = {}) {
  const created = Date.parse("2026-10-01T00:00:00Z");
  const a = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return ID;
    },
    ch: stubCh(mode),
    liveness: memoryLiveness({ createdAt: { "codex\u0000s-ok": created, "codex\u0000s-partial": created, "codex\u0000s-down": created } }).liveness,
    completeSinceMs: 0,
    directory,
    directoryWorkspaces: new Set(["ws-a"]),
    ...(mode.retryable ? { sessionPartialRetryable: true } : {}),
    // Exactly lambda.ts's wiring.
    log: (fields) => log.info("session read", fields),
  });
  return (path: string) => a.request(path, { headers: { authorization: "Bearer good" } });
}

describe("session read api outcome alarm: the filters over the lines the app really writes", () => {
  let lines: string[] = [];
  const write = process.stdout.write;
  beforeEach(() => {
    lines = [];
    process.stdout.write = ((chunk: string | Uint8Array) => {
      lines.push(...String(chunk).split("\n").filter(Boolean));
      return true;
    }) as typeof process.stdout.write;
  });
  afterEach(() => {
    process.stdout.write = write;
  });

  const requests = matcher(REPLAY_REQUESTS_FILTER_PATTERN);
  const unavailable = matcher(REPLAY_UNAVAILABLE_FILTER_PATTERN);

  /** Synthetic traffic: replay 200 x3, 501 x2, 503 (ClickHouse) x1, 503 (retryable partial) x1; directory 200 x5; a disabled route 501 x4. */
  async function traffic() {
    const statuses: number[] = [];
    const ok = app();
    for (let i = 0; i < 3; i++) statuses.push((await ok("/v1/sessions/s-ok/events?workspace=ws-a")).status);
    const partial = app({ partial: ["s-partial"] });
    for (let i = 0; i < 2; i++) statuses.push((await partial("/v1/sessions/s-partial/events?workspace=ws-a")).status);
    statuses.push((await app({ fail: true })("/v1/sessions/s-down/events?workspace=ws-a")).status);
    statuses.push((await app({ partial: ["s-partial"], retryable: true })("/v1/sessions/s-partial/events?workspace=ws-a")).status);
    for (let i = 0; i < 5; i++) statuses.push((await ok("/v1/sessions?workspace=ws-a")).status);
    for (let i = 0; i < 4; i++) statuses.push((await ok("/v1/sessions/s-ok/summary?workspace=ws-a")).status);
    return statuses;
  }

  it("the synthetic traffic answers what it claims", async () => {
    expect(await traffic()).toEqual([200, 200, 200, 501, 501, 503, 503, 200, 200, 200, 200, 200, 501, 501, 501, 501]);
  });

  it("every line is one raw JSON object on stdout; the request line carries route, numeric status and duration_ms", async () => {
    await traffic();
    const parsed = lines.map((l) => JSON.parse(l) as Record<string, unknown>);
    expect(parsed.every((p) => p.message === "session read" && p.level === "info")).toBe(true);
    const requestLines = parsed.filter((p) => "duration_ms" in p);
    // One request line per request, in order, with the route Hono matched (not the middleware's "*").
    expect(requestLines.map((p) => [p.route, p.status])).toEqual([
      ...Array(3).fill([READ_REPLAY_ROUTE, 200]),
      ...Array(2).fill([READ_REPLAY_ROUTE, 501]),
      ...Array(2).fill([READ_REPLAY_ROUTE, 503]),
      ...Array(5).fill(["/v1/sessions", 200]),
      ...Array(4).fill(["/v1/sessions/:sessionId/summary", 501]),
    ]);
    for (const p of requestLines) {
      expect(typeof p.status).toBe("number");
      expect(typeof p.duration_ms).toBe("number");
    }
    // The handlers' reason lines exist (route, status, reason) and carry no duration_ms.
    const reasonLines = parsed.filter((p) => "reason" in p);
    expect(reasonLines.length).toBeGreaterThanOrEqual(2 + 1 + 1 + 4);
    expect(reasonLines.every((p) => !("duration_ms" in p))).toBe(true);
    expect(reasonLines.filter((p) => p.route === READ_REPLAY_ROUTE).map((p) => [p.status, p.reason])).toEqual(
      expect.arrayContaining([[501, "session_partial"], [503, "clickhouse_error"], [503, "session_partial"]]),
    );
  });

  it("the filters count each replay request once and only the replay route's 501/503s: 4 of 7, not diluted", async () => {
    await traffic();
    const parsed = lines.map((l) => JSON.parse(l) as Record<string, unknown>);
    expect(parsed.filter(requests)).toHaveLength(7);
    expect(parsed.filter(unavailable)).toHaveLength(4);
    // Every unavailable match is also a request match (the share never exceeds 100%).
    expect(parsed.filter(unavailable).every(requests)).toBe(true);
    // Neither the directory's 200s nor the disabled route's 501s, nor any reason line, is counted.
    expect(parsed.filter((p) => requests(p) && p.route !== READ_REPLAY_ROUTE)).toHaveLength(0);
    expect(parsed.filter((p) => requests(p) && !("duration_ms" in p))).toHaveLength(0);
  });

  it("the reviewer's case: 20 replays all 501 beside 100 healthy directory reads is 100%, not 16.7%", async () => {
    const partial = app({ partial: ["s-partial"] });
    const ok = app();
    for (let i = 0; i < 20; i++) expect((await partial("/v1/sessions/s-partial/events?workspace=ws-a")).status).toBe(501);
    for (let i = 0; i < 100; i++) expect((await ok("/v1/sessions?workspace=ws-a")).status).toBe(200);
    const parsed = lines.map((l) => JSON.parse(l) as Record<string, unknown>);
    const total = parsed.filter(requests).length;
    const bad = parsed.filter(unavailable).length;
    expect([bad, total]).toEqual([20, 20]);
    // The alarm's expression at its defaults (minRequests 20, threshold 50): breaching.
    expect(total >= 20 ? (100 * bad) / total : 0).toBeGreaterThan(50);
  });

  it("matcher sanity: the subset parser matches and refuses as CloudWatch would for these shapes", () => {
    const line = { message: "session read", route: READ_REPLAY_ROUTE, status: 501, duration_ms: 0 };
    expect(requests(line)).toBe(true);
    expect(unavailable(line)).toBe(true);
    expect(unavailable({ ...line, status: 200 })).toBe(false);
    expect(requests({ ...line, status: "501" as never, duration_ms: undefined as never })).toBe(false);
    expect(requests({ ...line, route: "/v1/sessions/:sessionId/events/x" })).toBe(false);
    expect(requests({ ...line, message: "session read api pool error" })).toBe(false);
    expect(() => matcher('{ $.route = "/v1/*" }')).toThrow(/unsupported term/);
  });
});

describe("session read api outcome alarm: Lambda log formatting", () => {
  it("the function sets no LoggingConfig, so Lambda keeps the Text format and stdout JSON lines reach CloudWatch as-is", () => {
    const props = sessionReadApiFunctionProps({ stage: "aws-sessions-prod", names: sessionReadApiNames("aws-sessions-prod"), p: { completeSince: "epoch" } }) as Record<string, unknown>;
    expect(props.logging).toEqual({ retention: "30 days" });
    for (const k of ["loggingConfig", "LoggingConfig", "logFormat", "LogFormat"]) expect(k in props).toBe(false);
    expect(Object.keys(props.logging as object)).toEqual(["retention"]);
  });

  it("the logger writes the line to process streams directly, never through console (which the runtime would prefix or wrap)", () => {
    const src = readFileSync(fileURLToPath(new URL("../../../src/shared/log.ts", import.meta.url)), "utf8");
    expect(src).toContain('else process.stdout.write(line + "\\n");');
    expect(src).not.toMatch(/console\.(log|info|warn|error)\(/);
    const lambda = readFileSync(fileURLToPath(new URL("../../../src/session-read-api/lambda.ts", import.meta.url)), "utf8");
    expect(lambda).toContain('log: (fields) => log.info("session read", fields),');
  });
});

/**
 * LOCAL evaluation, not CloudWatch: a small evaluator for the metric-math subset the synthesized
 * expression uses (IF, FILL, >=, *, /, identifiers, numbers), applied to one window's sums, then
 * the alarm's own ComparisonOperator, Threshold and TreatMissingData. The parser refuses anything
 * outside that subset, so an edit to the expression fails here rather than being half-evaluated.
 * Semantics assumed from the CloudWatch metric-math docs: a series with no datapoint is missing,
 * FILL(x, 0) supplies 0 for it when another series in the expression has a datapoint, and a
 * window with no datapoint in any series yields no value (then TreatMissingData decides).
 */
type Series = number | undefined;
function evaluate(expression: string, vars: Record<string, Series>): Series {
  const tokens = expression.match(/\s*(>=|[A-Z]+|[a-z_]+|\d+(?:\.\d+)?|[(),*/])/g)?.map((t) => t.trim()) ?? [];
  if (tokens.join("") !== expression.replace(/\s+/g, "")) throw new Error(`unsupported expression: ${expression}`);
  let i = 0;
  const peek = () => tokens[i];
  const eat = (t?: string) => {
    const got = tokens[i++];
    if (t !== undefined && got !== t) throw new Error(`expected ${t}, got ${got}`);
    return got!;
  };
  const anyPresent = Object.values(vars).some((v) => v !== undefined);
  const primary = (): Series => {
    const t = eat();
    if (/^\d/.test(t)) return Number(t);
    if (t === "IF") {
      eat("("); const c = comparison(); eat(","); const a = comparison(); eat(","); const b = comparison(); eat(")");
      return c === undefined ? undefined : c !== 0 ? a : b;
    }
    if (t === "FILL") {
      eat("("); const v = comparison(); eat(","); const fill = Number(eat()); eat(")");
      return v === undefined && anyPresent ? fill : v;
    }
    if (t === "(") { const v = comparison(); eat(")"); return v; }
    if (/^[a-z_]+$/.test(t)) {
      if (!(t in vars)) throw new Error(`unknown series ${t}`);
      return vars[t];
    }
    throw new Error(`unsupported token ${t}`);
  };
  const product = (): Series => {
    let v = primary();
    while (peek() === "*" || peek() === "/") {
      const op = eat(); const r = primary();
      v = v === undefined || r === undefined ? undefined : op === "*" ? v * r : v / r;
    }
    return v;
  };
  const comparison = (): Series => {
    const l = product();
    if (peek() !== ">=") return l;
    eat(); const r = product();
    return l === undefined || r === undefined ? undefined : l >= r ? 1 : 0;
  };
  const out = comparison();
  if (i !== tokens.length) throw new Error(`trailing tokens in ${expression}`);
  return out;
}

describe("session read api outcome alarm: LOCAL evaluation of the synthesized expression and comparison (not CloudWatch)", () => {
  const [, alarm] = sessionReadApiOutcomeAlarm({ stage: "aws-sessions-prod", names: sessionReadApiNames("aws-sessions-prod"), p: { completeSince: "epoch" } });
  const expression = alarm.Metrics.find((m) => m.ReturnData) as { Expression: string };
  /** One window's state for the alarm as configured: its expression, operator, threshold and missing-data rule. */
  const state = (window: { requests?: number; unavailable?: number }) => {
    expect(alarm.ComparisonOperator).toBe("GreaterThanThreshold");
    expect([alarm.EvaluationPeriods, alarm.DatapointsToAlarm]).toEqual([1, 1]);
    const value = evaluate(expression.Expression, { requests: window.requests, unavailable: window.unavailable });
    if (value === undefined) return alarm.TreatMissingData === "notBreaching" ? "quiet" : "other";
    return value > alarm.Threshold ? "breach" : "quiet";
  };

  it("evaluates the expression the alarm is built with, at the defaults", () => {
    expect(OUTCOME_ALARM_DEFAULTS).toEqual({ thresholdPercent: 50, windowMinutes: 30, minRequests: 20 });
    expect(expression.Expression).toBe("IF(FILL(requests, 0) >= 20, 100 * FILL(unavailable, 0) / requests, 0)");
    expect(alarm.Threshold).toBe(50);
  });

  it("missing data, 19/19, 10/20 and 11/20 unavailable: quiet, quiet, quiet, breach", () => {
    // No datapoint in the window at all (no log lines): no value, notBreaching.
    expect(state({})).toBe("quiet");
    // Log lines but no replay request (defaultValue 0 on both filters): 0 requests is below the minimum.
    expect(state({ requests: 0, unavailable: 0 })).toBe("quiet");
    // Below the minimum volume, however bad.
    expect(state({ requests: 19, unavailable: 19 })).toBe("quiet");
    // Exactly the threshold is not "greater than".
    expect(state({ requests: 20, unavailable: 10 })).toBe("quiet");
    expect(state({ requests: 20, unavailable: 11 })).toBe("breach");
  });

  it("edges: the unavailable series missing is 0%, the outage shape breaches, and a configured minimum moves the floor", () => {
    expect(state({ requests: 100 })).toBe("quiet");
    expect(state({ requests: 20, unavailable: 20 })).toBe("breach");
    expect(state({ requests: 1000, unavailable: 990 })).toBe("breach");
    const [, tuned] = sessionReadApiOutcomeAlarm({ stage: "aws-sessions-prod", names: sessionReadApiNames("aws-sessions-prod"), p: { completeSince: "epoch", outcomeAlarm: { minRequests: 5, thresholdPercent: 25 } } });
    const e = (tuned.Metrics.find((m) => m.ReturnData) as { Expression: string }).Expression;
    expect(evaluate(e, { requests: 5, unavailable: 2 })).toBe(40);
    expect(evaluate(e, { requests: 4, unavailable: 4 })).toBe(0);
  });

  it("the local evaluator refuses expressions outside its subset", () => {
    expect(() => evaluate("SUM(METRICS())", { requests: 1 })).toThrow();
    expect(() => evaluate("IF(requests > 1, 1, 0)", { requests: 1 })).toThrow();
    expect(() => evaluate("IF(FILL(other, 0) >= 1, 1, 0)", { requests: 1 })).toThrow(/unknown series other/);
  });
});
