/** RELAYHISTORY_SELFHOST_READ_API / RELAYHISTORY_READ_API_PORT parsing (service-config.ts). */
import { describe, expect, test } from "bun:test";
import { readApiServiceConfig, readApiServiceEnabled } from "../service-config.ts";

const SECRET = JSON.stringify({ supabase_url: "postgresql://reader@db.example.invalid:5432/postgres" });

describe("RELAYHISTORY_SELFHOST_READ_API", () => {
  test("unset, empty and \"false\" are dormant", () => {
    for (const value of [undefined, "", "false"]) {
      expect(readApiServiceEnabled({ RELAYHISTORY_SELFHOST_READ_API: value })).toBe(false);
      expect(readApiServiceConfig({ RELAYHISTORY_SELFHOST_READ_API: value })).toEqual({ enabled: false, port: 8080 });
    }
  });

  test("exactly \"true\" enables", () => {
    expect(readApiServiceEnabled({ RELAYHISTORY_SELFHOST_READ_API: "true" })).toBe(true);
  });

  test.each(["TRUE", "True", "1", "yes", "on", "0", "off", "False", "tru"])("%p fails startup and does not echo the value", (value) => {
    expect(() => readApiServiceConfig({ RELAYHISTORY_SELFHOST_READ_API: value })).toThrow(
      "96bulls: RELAYHISTORY_SELFHOST_READ_API must be true or false",
    );
  });

  test("dormant needs nothing else and ignores a bad ALLOW_IN_CLUSTER_HTTP", () => {
    expect(readApiServiceConfig({ ALLOW_IN_CLUSTER_HTTP: "yes" })).toEqual({ enabled: false, port: 8080 });
  });

  test("enabled needs the secret, named without its value", () => {
    expect(() => readApiServiceConfig({ RELAYHISTORY_SELFHOST_READ_API: "true" })).toThrow(
      "96bulls: RELAYHISTORY_SELFHOST_READ_API=true needs RELAYHISTORY_READ_API_SECRET",
    );
    expect(() => readApiServiceConfig({ RELAYHISTORY_SELFHOST_READ_API: "true", RELAYHISTORY_READ_API_SECRET: "  " })).toThrow(
      "needs RELAYHISTORY_READ_API_SECRET",
    );
    expect(readApiServiceConfig({ RELAYHISTORY_SELFHOST_READ_API: "true", RELAYHISTORY_READ_API_SECRET: SECRET })).toEqual({
      enabled: true,
      port: 8080,
      secret: SECRET,
    });
  });

  test("enabled checks ALLOW_IN_CLUSTER_HTTP", () => {
    const env = { RELAYHISTORY_SELFHOST_READ_API: "true", RELAYHISTORY_READ_API_SECRET: SECRET };
    expect(() => readApiServiceConfig({ ...env, ALLOW_IN_CLUSTER_HTTP: "yes" })).toThrow("ALLOW_IN_CLUSTER_HTTP must be true or false");
    expect(readApiServiceConfig({ ...env, ALLOW_IN_CLUSTER_HTTP: "true" }).enabled).toBe(true);
  });
});

describe("RELAYHISTORY_READ_API_PORT", () => {
  test("default 8080; bounds 1 and 65535 are accepted", () => {
    expect(readApiServiceConfig({}).port).toBe(8080);
    expect(readApiServiceConfig({ RELAYHISTORY_READ_API_PORT: "1" }).port).toBe(1);
    expect(readApiServiceConfig({ RELAYHISTORY_READ_API_PORT: "65535" }).port).toBe(65_535);
    expect(readApiServiceConfig({ RELAYHISTORY_READ_API_PORT: "9090" }).port).toBe(9090);
  });

  test.each(["0", "65536", "-1", "80.5", "abc", "8080x", "1e9"])("%p fails startup, even when dormant", (value) => {
    expect(() => readApiServiceConfig({ RELAYHISTORY_READ_API_PORT: value })).toThrow(
      "96bulls: RELAYHISTORY_READ_API_PORT must be a TCP port",
    );
  });

  test("parsed when enabled too", () => {
    expect(() =>
      readApiServiceConfig({ RELAYHISTORY_SELFHOST_READ_API: "true", RELAYHISTORY_READ_API_SECRET: SECRET, RELAYHISTORY_READ_API_PORT: "0" }),
    ).toThrow("must be a TCP port");
  });
});
