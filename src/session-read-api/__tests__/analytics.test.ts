/**
 * The read API's analytics event (src/shared/analytics.ts): one session_read.request_completed per
 * verified /v1 request, with the route PATTERN, never a concrete path, query or token; none for an
 * unauthenticated request; and a broken sink never changes the answer.
 */
import { describe, expect, test } from "bun:test";
import type { ClickHouseClient } from "../../session-pipeline/vendor/search-index/clickhouse.ts";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { projectEvent, type AnalyticsEvent, type AnalyticsPort } from "../../shared/analytics.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "u-PLANTED", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };

const ch: ClickHouseClient = {
  database: "relay_search",
  async command() {},
  async insert() {},
  async select<T>() {
    return [] as T[];
  },
};

function api(analytics?: AnalyticsPort) {
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return ID;
    },
    ch,
    liveness: memoryLiveness({}).liveness,
    completeSinceMs: 0,
    log: () => {},
    ...(analytics ? { analytics } : {}),
  });
  return (path: string, token: string | null = "good", headers: Record<string, string> = {}) =>
    app.request(path, { headers: { ...(token ? { authorization: `Bearer ${token}` } : {}), ...headers } });
}

function recorder() {
  const events: AnalyticsEvent[] = [];
  return { events, port: { capture: (e: AnalyticsEvent) => void events.push(e) } satisfies AnalyticsPort };
}

describe("session read analytics", () => {
  test("a verified read emits one event: route pattern, status, verified ids, correlation id", async () => {
    const { events, port } = recorder();
    const get = api(port);
    const r = await get("/v1/events?workspace=ws-a&q=PLANTEDQUERY&limit=20", "good", { "x-correlation-id": "corr-123" });
    expect(r.status).toBe(200);
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ kind: "session_read.request_completed", route: "/v1/events", status: 200, orgId: "org-a", workspaceId: "ws-a", traceId: "corr-123" });
    const envelope = projectEvent(events[0]!, { component: "session-read-api", deploymentId: "d" });
    expect(JSON.stringify(envelope)).not.toContain("PLANTED");
  });

  test("a session route exports its pattern, never the session id", async () => {
    const { events, port } = recorder();
    const get = api(port);
    await get("/v1/sessions/sess-PLANTED/events?workspace=ws-a");
    expect(events).toHaveLength(1);
    expect((events[0] as { route: string }).route).toBe("/v1/sessions/:sessionId/events");
    expect(JSON.stringify(projectEvent(events[0]!, { component: "session-read-api", deploymentId: "d" }))).not.toContain("PLANTED");
  });

  test("an unauthenticated request is not an event", async () => {
    const { events, port } = recorder();
    const get = api(port);
    expect((await get("/v1/events?workspace=ws-a&q=x", null)).status).toBe(401);
    expect((await get("/v1/events?workspace=ws-a&q=x", "bad")).status).toBe(401);
    expect((await get("/health", null)).status).toBe(200);
    expect(events).toHaveLength(0);
  });

  test("a throwing sink never changes the answer", async () => {
    const plain = api();
    const broken = api({ capture: () => { throw new Error("analytics down"); } });
    for (const path of ["/v1/events?workspace=ws-a&q=x&limit=20", "/v1/events?q=x", "/v1/sessions/s-1/events?workspace=ws-a"]) {
      // The same correlation id, so the bodies are byte-comparable.
      const a = await plain(path, "good", { "x-correlation-id": "corr-same" });
      const b = await broken(path, "good", { "x-correlation-id": "corr-same" });
      expect(b.status).toBe(a.status);
      expect(await b.text()).toBe(await a.text());
    }
  });
});
