import { expect, test } from "bun:test";
import { acquisitionConfig } from "../config.ts";
import { NATIVE_SEED_HEX, pinnedKeysJson } from "./helpers.ts";

const live = {
  ACQUISITION_ENABLED: "true", ACQUISITION_ORIGIN: "https://history.example.test", RELAYCONTROL_INTROSPECT_URL: "https://relaycontrol.example.test/v1/introspect",
  RELAYCONTROL_INTROSPECT_SECRET: "x", ACQUISITION_PINNED_KEYS: pinnedKeysJson, ACQUISITION_DATABASE_URL: "postgres://u@db.example.test/h",
};

test("default off: nothing else is read", () => {
  expect(acquisitionConfig({})).toEqual({ enabled: false, port: 8080, opsPort: 8082, opsHost: "127.0.0.1" });
  expect(acquisitionConfig({ ACQUISITION_ENABLED: "false", ACQUISITION_PINNED_KEYS: "garbage" }).enabled).toBe(false);
  expect(() => acquisitionConfig({ ACQUISITION_ENABLED: "yes" })).toThrow();
  expect(() => acquisitionConfig({ ACQUISITION_PORT: "9000", ACQUISITION_OPS_PORT: "9000" })).toThrow();
});

test("ops listener: loopback bind and port 8082 by default (8081 is the VPC-open worker health port)", () => {
  const c = acquisitionConfig(live);
  expect([c.port, c.opsPort, c.opsHost]).toEqual([8080, 8082, "127.0.0.1"]);
  expect(acquisitionConfig({ ACQUISITION_OPS_HOST: "10.0.0.5" }).opsHost).toBe("10.0.0.5");
  expect(acquisitionConfig({ ACQUISITION_OPS_HOST: "::1" }).opsHost).toBe("::1");
  // Read and validated even while disabled; a hostname is refused (no DNS-dependent bind).
  expect(() => acquisitionConfig({ ACQUISITION_OPS_HOST: "ops.internal" })).toThrow(/ACQUISITION_OPS_HOST/);
  expect(() => acquisitionConfig({ ACQUISITION_OPS_PORT: "8080" })).toThrow();
});

test("enabled: required variables, native needs signer + intent key, sub-flags default off", () => {
  const c = acquisitionConfig(live);
  expect(c.enabled && [c.nativeEnabled, c.opsEnabled, c.signer, c.intentKey]).toEqual([false, false, null, null]);
  for (const k of Object.keys(live).filter((k) => k !== "ACQUISITION_ENABLED"))
    expect(() => acquisitionConfig({ ...live, [k]: undefined })).toThrow();
  expect(() => acquisitionConfig({ ...live, ACQUISITION_ORIGIN: "https://history.example.test/path" })).toThrow();
  expect(() => acquisitionConfig({ ...live, ACQUISITION_NATIVE_ENABLED: "true" })).toThrow();
  const n = acquisitionConfig({ ...live, ACQUISITION_NATIVE_ENABLED: "true", ACQUISITION_ISSUER_KEY_ID: "native-key-1",
    ACQUISITION_ISSUER_PRIVATE_KEY: Buffer.from(NATIVE_SEED_HEX, "hex").toString("base64"), ACQUISITION_INTENT_KEY: Buffer.alloc(32, 1).toString("base64") });
  expect(n.enabled && n.signer?.issuerKeyId).toBe("native-key-1");
  // The parsed config never serializes the private key.
  expect(JSON.stringify(n).includes(Buffer.from(NATIVE_SEED_HEX, "hex").toString("base64"))).toBe(false);
});

test("Q3 cloud publication: default off, read only under the master flag, refuses (never throws) on missing config", () => {
  const key = "test-ingest-signing-key-not-real";
  const cloudEnv = { ACQUISITION_CLOUD_PUBLICATION_ENABLED: "true", ACQUISITION_CLOUD_INGEST_ORIGIN: "https://ingest.example.test", ACQUISITION_CLOUD_INGEST_SIGNING_KEY: key };
  const state = (env: Record<string, string | undefined>) => {
    const c = acquisitionConfig(env);
    return c.enabled ? c.cloudPublication : null;
  };
  // Default off; the master flag off means the cloud flag is not even read.
  expect(state(live)).toEqual({ state: "disabled" });
  expect(state({ ...live, ACQUISITION_CLOUD_PUBLICATION_ENABLED: "false" })).toEqual({ state: "disabled" });
  expect(acquisitionConfig({ ACQUISITION_CLOUD_PUBLICATION_ENABLED: "garbage" }).enabled).toBe(false);
  expect(() => acquisitionConfig({ ...live, ACQUISITION_CLOUD_PUBLICATION_ENABLED: "1" })).toThrow(/ACQUISITION_CLOUD_PUBLICATION_ENABLED/);
  // Ready: bare origin, signing key, cloud-class registry subset only.
  const ready = state({ ...live, ...cloudEnv });
  expect(ready?.state).toBe("ready");
  if (ready?.state !== "ready") throw new Error("unreachable");
  expect(ready.ingestOrigin).toBe("https://ingest.example.test");
  expect([...ready.registry.values()].every((k) => k.ownerKind === "cloud")).toBe(true);
  expect(ready.signingKey.reveal()).toBe(key);
  expect(JSON.stringify(acquisitionConfig({ ...live, ...cloudEnv })).includes(key)).toBe(false);
  // Missing / invalid values: unconfigured with the variable names only (never the values).
  const missing = (over: Record<string, string | undefined>) => {
    const s = state({ ...live, ...cloudEnv, ...over });
    return s?.state === "unconfigured" ? s.missing : s?.state;
  };
  expect(missing({ ACQUISITION_CLOUD_INGEST_ORIGIN: undefined })).toEqual(["ACQUISITION_CLOUD_INGEST_ORIGIN"]);
  expect(missing({ ACQUISITION_CLOUD_INGEST_SIGNING_KEY: " " })).toEqual(["ACQUISITION_CLOUD_INGEST_SIGNING_KEY"]);
  for (const bad of ["https://ingest.example.test/v1/delivery/batches", "https://ingest.example.test/", "http://ingest.example.test",
    "https://u:p@ingest.example.test", "https://INGEST.example.test", "not a url"])
    expect(missing({ ACQUISITION_CLOUD_INGEST_ORIGIN: bad })).toEqual(["ACQUISITION_CLOUD_INGEST_ORIGIN"]);
  expect(missing({ ACQUISITION_CLOUD_INGEST_ORIGIN: "http://127.0.0.1:8787" })).toBe("ready");
  const nativeOnly = JSON.stringify(JSON.parse(pinnedKeysJson).filter((k: { issuerClass: string }) => k.issuerClass === "native"));
  expect(missing({ ACQUISITION_PINNED_KEYS: nativeOnly })).toEqual(["ACQUISITION_PINNED_KEYS (no cloud-class key)"]);
});
