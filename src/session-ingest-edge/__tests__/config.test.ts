import { describe, expect, test } from "bun:test";
import { edgeConfig, edgeEnabled, edgeOrigin } from "../config.ts";

const full = {
  SESSION_INGEST_EDGE_ENABLED: "true",
  SESSION_INGEST_EDGE_ORIGIN: "https://edge.example.test",
  SESSION_INGEST_UPSTREAM_URL: "http://session-ingest.history.svc:8080",
  RELAYCONTROL_INTROSPECT_URL: "http://relaycontrol.control.svc.cluster.local:8080",
  RELAYCONTROL_INTROSPECT_SECRET: "synthetic-introspect-secret",
  INGEST_SIGNING_KEY: "synthetic-signing-key",
  ALLOW_IN_CLUSTER_HTTP: "true",
};

describe("SESSION_INGEST_EDGE_ENABLED", () => {
  test("unset, empty and false are off; true is on; anything else is a startup error", () => {
    expect(edgeEnabled({})).toBe(false);
    expect(edgeEnabled({ SESSION_INGEST_EDGE_ENABLED: "" })).toBe(false);
    expect(edgeEnabled({ SESSION_INGEST_EDGE_ENABLED: "false" })).toBe(false);
    expect(edgeEnabled({ SESSION_INGEST_EDGE_ENABLED: "true" })).toBe(true);
    for (const typo of ["1", "yes", "TRUE", "on", "0"]) expect(() => edgeEnabled({ SESSION_INGEST_EDGE_ENABLED: typo })).toThrow("must be true or false");
  });

  test("off reads nothing else: dormant with no other variable set", () => {
    expect(edgeConfig({})).toEqual({ enabled: false, port: 8080 });
    expect(edgeConfig({ SESSION_INGEST_EDGE_ORIGIN: "not a url" })).toEqual({ enabled: false, port: 8080 });
  });
});

describe("enabled configuration", () => {
  test("a complete in-cluster configuration", () => {
    expect(edgeConfig({ ...full, SESSION_INGEST_EDGE_PORT: "9090" })).toEqual({
      enabled: true,
      port: 9090,
      origin: "https://edge.example.test",
      upstreamUrl: "http://session-ingest.history.svc:8080",
      introspectUrl: "http://relaycontrol.control.svc.cluster.local:8080",
      introspectSecret: "synthetic-introspect-secret",
      signingKey: "synthetic-signing-key",
    });
  });

  test.each(["SESSION_INGEST_EDGE_ORIGIN", "SESSION_INGEST_UPSTREAM_URL", "RELAYCONTROL_INTROSPECT_URL", "RELAYCONTROL_INTROSPECT_SECRET", "INGEST_SIGNING_KEY"])("%s is required", (name) => {
    expect(() => edgeConfig({ ...full, [name]: "" })).toThrow(name);
  });

  test("the destination policy refuses non-cluster http, a path, credentials; errors never echo the value", () => {
    const run = (env: Record<string, string>) => { try { edgeConfig(env); return "ok"; } catch (e) { return (e as Error).message; } };
    const msg = run({ ...full, SESSION_INGEST_UPSTREAM_URL: "http://203.0.113.7:8080" });
    expect(msg).not.toBe("ok");
    expect(msg).not.toContain("203.0.113.7");
    expect(run({ ...full, ALLOW_IN_CLUSTER_HTTP: undefined as never })).not.toBe("ok");
    expect(run({ ...full, SESSION_INGEST_UPSTREAM_URL: "http://session-ingest.history.svc:8080/v1" })).not.toBe("ok");
    expect(run({ ...full, RELAYCONTROL_INTROSPECT_URL: "http://user:pw@relaycontrol.control.svc:8080" })).not.toBe("ok");
  });

  test("the origin must be exactly scheme://host[:port]", () => {
    expect(edgeOrigin("https://edge.example.test")).toBe("https://edge.example.test");
    expect(edgeOrigin("http://localhost:8080")).toBe("http://localhost:8080");
    for (const bad of ["https://edge.example.test/", "https://edge.example.test/path", "https://Edge.example.test", "https://edge.example.test:443", "ftp://edge.example.test", "https://u:p@edge.example.test", "edge.example.test", ""])
      expect(() => edgeOrigin(bad)).toThrow("SESSION_INGEST_EDGE_ORIGIN");
  });

  test("the port must be a TCP port", () => {
    for (const bad of ["0", "65536", "80a", "-1", "1.5"]) expect(() => edgeConfig({ SESSION_INGEST_EDGE_PORT: bad })).toThrow("SESSION_INGEST_EDGE_PORT");
  });
});
