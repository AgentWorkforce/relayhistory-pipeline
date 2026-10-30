import { afterEach, describe, expect, test } from "bun:test";
import {
  httpLatencyStats,
  responseFingerprint,
  runHttpBenchmark,
  type HttpBenchmarkConfig,
} from "../relayhistory-api-benchmark.ts";

const servers: Array<ReturnType<typeof Bun.serve>> = [];

afterEach(() => {
  while (servers.length) servers.pop()!.stop(true);
});

function apiServer(label: string, mismatch = false) {
  const server = Bun.serve({
    port: 0,
    fetch(request) {
      const url = new URL(request.url);
      expect(request.headers.get("authorization")).toBe("Bearer secret-benchmark-token");
      if (url.pathname === "/v1/sessions") {
        return Response.json({ sessions: [{ id: "session-a", count: 3 }], correlationId: `${label}-dynamic` });
      }
      if (url.pathname === "/v1/events") {
        return Response.json({
          events: [{ id: "event-a", kind: mismatch ? "different" : "message" }],
          correlationId: crypto.randomUUID(),
        });
      }
      if (url.pathname.includes("/events")) {
        return Response.json({ events: [{ id: "event-a" }], requestId: crypto.randomUUID() });
      }
      return Response.json({ error: "not found" }, { status: 404 });
    },
  });
  servers.push(server);
  return `http://127.0.0.1:${server.port}`;
}

function config(legacyUrl: string, compatibilityUrl: string): HttpBenchmarkConfig {
  return {
    legacyUrl,
    compatibilityUrl,
    legacyToken: "secret-benchmark-token",
    compatibilityToken: "secret-benchmark-token",
    query: "private search phrase",
    sessionId: "private-session-id",
    workspace: "private-workspace-id",
    runs: 4,
    warmups: 1,
    concurrency: 2,
    timeoutMs: 2_000,
  };
}

describe("relayhistory API benchmark", () => {
  test("calculates nearest-rank latency percentiles", () => {
    expect(httpLatencyStats([10, 2, 4, 8, 6])).toEqual({ p50Ms: 6, p95Ms: 10, p99Ms: 10 });
  });

  test("ignores volatile ids and canonicalizes JSON object keys", () => {
    expect(responseFingerprint('{"value":1,"correlationId":"one"}')).toBe(
      responseFingerprint('{"requestId":"two","value":1}'),
    );
  });

  test("reports parity and aggregate HTTP performance without sensitive content", async () => {
    const report = await runHttpBenchmark(config(apiServer("legacy"), apiServer("compatibility")));
    const output = JSON.stringify(report);

    expect(report.configuration.cases).toEqual(["recent_sessions", "event_text_search", "session_replay"]);
    for (const value of Object.values(report.results) as Array<any>) {
      expect(value.parity).toEqual({ status: true, response: true });
      expect(value.legacy.statuses).toEqual({ "200": 4 });
      expect(value.compatibility.statuses).toEqual({ "200": 4 });
      expect(value.legacy.latency.p99Ms).toBeGreaterThanOrEqual(0);
      expect(value.compatibility.throughputQps).toBeGreaterThan(0);
    }
    for (const secret of [
      "secret-benchmark-token",
      "private search phrase",
      "private-session-id",
      "private-workspace-id",
      "127.0.0.1",
    ]) {
      expect(output).not.toContain(secret);
    }
  });

  test("detects a response mismatch independently from status parity", async () => {
    const report = await runHttpBenchmark(config(apiServer("legacy"), apiServer("compatibility", true)));
    const result = report.results.event_text_search as any;
    expect(result.parity).toEqual({ status: true, response: false });
  });
});
