/**
 * The ingest's analytics event (src/shared/analytics.ts): one session_ingest.batch_completed per batch,
 * counts and the outcome class only (never a record, batch or session id, never content); a refusal is
 * classified by its status; a broken sink never changes the answer. Plus the ingest Worker's
 * subrequest reservation and the self-host config's analytics refusals.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../app.ts";
import { DeliveryError } from "../vendor/delivery/delivery-contracts.ts";
import { reserveAnalyticsSubrequests, type IngestSettings } from "../worker.ts";
import { selfhostIngestConfig } from "../selfhost-config.ts";
import { projectEvent, type AnalyticsEvent, type AnalyticsPort } from "../../shared/analytics.ts";
import { WORKER_LIMITS } from "../../shared/analytics-export.ts";
import { MemoryLedger, TENANT, batch, record, scratchStore } from "./support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));

const good = encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] });

function app(ledger: MemoryLedger, analytics?: AnalyticsPort) {
  const s = scratchStore();
  cleanups.push(s.done);
  return createIngestApp({
    deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} } } as never,
    log: () => {},
    identify: (r) => bindingIdentity(r),
    ...(analytics ? { analytics } : {}),
  });
}

async function post(ledger: MemoryLedger, analytics?: AnalyticsPort, path = "/v1/delivery/batches") {
  const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "rec-PLANTED-1" }), record({ record_id: "rec-PLANTED-2" })]) });
  return app(ledger, analytics).fetch(new Request(`https://x${path}`, { method: "POST", body, headers: { [IDENTITY_HEADER]: good, "x-correlation-id": "corr-ingest" } }));
}

function recorder() {
  const events: AnalyticsEvent[] = [];
  return { events, port: { capture: (e: AnalyticsEvent) => void events.push(e) } satisfies AnalyticsPort };
}

describe("session ingest analytics", () => {
  test("an accepted batch emits one event with counts and the outcome class only", async () => {
    const { events, port } = recorder();
    const res = await post(new MemoryLedger(), port);
    expect(res.status).toBe(200);
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ kind: "session_ingest.batch_completed", route: "delivery", outcome: "accepted", records: 2, orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, traceId: "corr-ingest" });
    const envelope = projectEvent(events[0]!, { component: "session-ingest", deploymentId: "d" });
    if (typeof envelope === "string") throw new Error(envelope);
    expect(envelope.properties).toMatchObject({ route: "delivery", outcome: "accepted", record_count: 2 });
    expect(JSON.stringify(envelope)).not.toContain("PLANTED");
    expect(JSON.stringify(envelope)).not.toContain(TENANT.userId);
  });

  test("a refusal is classified by its status", async () => {
    const { events, port } = recorder();
    const conflict = new MemoryLedger();
    conflict.claim = async () => { throw new DeliveryError("delivery_conflict", 409, "conflict"); };
    expect((await post(conflict, port)).status).toBe(409);
    const down = new MemoryLedger();
    down.claim = async () => { throw new Error("db down"); };
    expect((await post(down, port)).status).toBe(503);
    expect(events.map((e) => (e as { outcome: string }).outcome)).toEqual(["conflict", "unavailable"]);
  });

  test("turns batches are their own route", async () => {
    const { events, port } = recorder();
    await post(new MemoryLedger(), port, "/v1/turns/batches");
    expect(events).toHaveLength(1);
    expect((events[0] as { route: string }).route).toBe("turns");
  });

  test("a throwing sink never changes the answer", async () => {
    const plain = await post(new MemoryLedger());
    const broken = await post(new MemoryLedger(), { capture: () => { throw new Error("analytics down"); } });
    expect(broken.status).toBe(plain.status);
    expect(await broken.json()).toEqual(await plain.json());
  });
});

describe("ingest Worker analytics budget", () => {
  const settings = { subrequestCap: 100 } as IngestSettings;
  test("off: the subrequest cap is unchanged; on: the flush's requests are reserved from it", () => {
    expect(reserveAnalyticsSubrequests(settings, { sink: "noop" })).toBe(settings);
    const on = reserveAnalyticsSubrequests(settings, { sink: "posthog", deploymentId: "d", host: "https://i.agentrelay.com", apiKey: "phc_testkey123" });
    expect(on.subrequestCap).toBe(100 - WORKER_LIMITS.maxRequestsPerDrain);
    expect(reserveAnalyticsSubrequests({ subrequestCap: 1 } as IngestSettings, { sink: "posthog", deploymentId: "d", host: "https://i.agentrelay.com", apiKey: "phc_testkey123" }).subrequestCap).toBe(0);
    const uncapped = {} as IngestSettings;
    expect(reserveAnalyticsSubrequests(uncapped, { sink: "posthog", deploymentId: "d", host: "https://i.agentrelay.com", apiKey: "phc_testkey123" })).toBe(uncapped);
  });
});

describe("self-host ingest analytics configuration", () => {
  const base = {
    RELAYHISTORY_SELFHOST_INGEST: "true",
    RELAYHISTORY_INGEST_ORIGIN: "https://ingest.example",
    RELAYCONTROL_INTROSPECT_URL: "https://control.example",
    RELAYCONTROL_INTROSPECT_SECRET: "introspect-secret",
    RELAYHISTORY_INGEST_DATABASE_URL: "postgres://bulls96_session_ingest:pw@db.example:5432/postgres",
    SESSION_POSTGRES_TLS_PROFILE: "supabase",
    LOCKER_BUCKET: "bucket",
  };
  test("default off; collector parsed; PostHog refused at startup", () => {
    const off = selfhostIngestConfig(base);
    expect("analytics" in off).toBe(false);
    const on = selfhostIngestConfig({ ...base, ANALYTICS_SINK: "collector", ANALYTICS_DEPLOYMENT_ID: "install-1", ANALYTICS_COLLECTOR_URL: "https://collector.example/v1/events", ANALYTICS_COLLECTOR_TOKEN: "producer-token-0123456789abcdef" });
    expect(on).toMatchObject({ analytics: { sink: "collector", deploymentId: "install-1" } });
    expect(() => selfhostIngestConfig({ ...base, ANALYTICS_SINK: "posthog", ANALYTICS_DEPLOYMENT_ID: "x" })).toThrow(/ANALYTICS_SINK/);
    expect(() => selfhostIngestConfig({ ...base, ANALYTICS_POSTHOG_KEY: "phc_testkey123" })).toThrow(/ANALYTICS_POSTHOG_KEY/);
    expect(() => selfhostIngestConfig({ ...base, ANALYTICS_SINK: "collecter" })).toThrow(/ANALYTICS_SINK/);
  });
});
