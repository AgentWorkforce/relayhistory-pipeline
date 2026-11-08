import { expect, test } from "bun:test";
import { REFUSED_ISSUER_ENV, acquisitionConfig } from "../config.ts";
import { parsePinnedKeys } from "../key-registry.ts";
import { NATIVE_SEED_HEX, cloudPinnedEntry, nativePinnedEntry, pinnedKeysJson } from "./helpers.ts";

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

test("enabled: required variables, ops sub-flag default off, no signer or intent key in the config", () => {
  const c = acquisitionConfig(live);
  expect(c.enabled && c.opsEnabled).toBe(false);
  expect(Object.keys(c).sort()).toEqual(["databaseUrl", "enabled", "introspectSecret", "introspectUrl", "opsEnabled", "opsHost", "opsPort", "origin", "port", "registry"]);
  const o = acquisitionConfig({ ...live, ACQUISITION_OPS_ENABLED: "true" });
  expect(o.enabled && o.opsEnabled).toBe(true);
  for (const k of Object.keys(live).filter((k) => k !== "ACQUISITION_ENABLED"))
    expect(() => acquisitionConfig({ ...live, [k]: undefined })).toThrow();
  expect(() => acquisitionConfig({ ...live, ACQUISITION_ORIGIN: "https://history.example.test/path" })).toThrow();
});

test("verify-only: any issuer-era variable refuses startup, enabled or not, without echoing its value", () => {
  const seed = Buffer.from(NATIVE_SEED_HEX, "hex").toString("base64");
  expect(REFUSED_ISSUER_ENV).toEqual(["ACQUISITION_ISSUER_PRIVATE_KEY", "ACQUISITION_ISSUER_KEY_ID", "ACQUISITION_INTENT_KEY"]);
  for (const name of REFUSED_ISSUER_ENV)
    for (const base of [{}, { ACQUISITION_ENABLED: "false" }, live])
      for (const value of [seed, ""]) {
        let message = "";
        try { acquisitionConfig({ ...base, [name]: value }); } catch (e) { message = String(e); }
        expect([name, message]).toEqual([name, `Error: 96bulls: ${name} is set but the acquisition service is verify-only (relaycontrol is the sole issuer); remove it`]);
      }
  // The pre-rework native config (switch + key + key id + intent key) is refused, not silently ignored.
  const legacy = { ...live, ACQUISITION_NATIVE_ENABLED: "true", ACQUISITION_ISSUER_KEY_ID: "native-key-1", ACQUISITION_ISSUER_PRIVATE_KEY: seed, ACQUISITION_INTENT_KEY: Buffer.alloc(32, 1).toString("base64") };
  expect(() => acquisitionConfig(legacy)).toThrow(/verify-only/);
  try { acquisitionConfig(legacy); } catch (e) { expect(String(e)).not.toContain(seed); }
  for (const v of ["true", "yes", "1"])
    for (const base of [{}, live]) expect(() => acquisitionConfig({ ...base, ACQUISITION_NATIVE_ENABLED: v })).toThrow(/ACQUISITION_NATIVE_ENABLED is not supported.*verify-only/);
  // Unset, empty or "false" is the off state and still starts.
  for (const v of [undefined, "", "false"]) expect(acquisitionConfig({ ...live, ACQUISITION_NATIVE_ENABLED: v }).enabled).toBe(true);
});

const NATIVE_REFUSED = "Error: 96bulls: ACQUISITION_PINNED_KEYS issuerClass must be cloud; native-class keys are refused (no native issuer; relaycontrol is the sole issuer)";

test("pinned keys: parsed, malformed refused, one public key under two issuerKeyIds refused", () => {
  expect([...parsePinnedKeys(pinnedKeysJson).entries()].map(([id, k]) => [id, k.ownerKind])).toEqual([["test-key-1", "cloud"]]);
  expect(() => parsePinnedKeys("[]")).toThrow();
  expect(() => parsePinnedKeys("not json")).toThrow();
  expect(() => parsePinnedKeys(JSON.stringify([{ ...cloudPinnedEntry, issuerClass: "other" }]))).toThrow(/issuerClass must be cloud$/);
  const cloud = cloudPinnedEntry;
  const otherKey = nativePinnedEntry.publicKeyBase64;
  expect(() => parsePinnedKeys(JSON.stringify([cloud, { ...cloud, publicKeyBase64: otherKey }]))).toThrow(/duplicate issuerKeyId/);
  const twoIds = [cloud, { issuerKeyId: "cloud-key-2", issuerClass: "cloud", publicKeyBase64: cloud.publicKeyBase64 }];
  expect(() => parsePinnedKeys(JSON.stringify(twoIds))).toThrow(/same public key under two issuerKeyIds/);
  try { parsePinnedKeys(JSON.stringify(twoIds)); } catch (e) { expect(String(e)).not.toContain(cloud.publicKeyBase64); }
});

test("pinned keys: native-class entries are refused at startup (alone, mixed, any order), never echoing values", () => {
  const cases = {
    alone: [nativePinnedEntry],
    cloudFirst: [cloudPinnedEntry, nativePinnedEntry],
    nativeFirst: [nativePinnedEntry, cloudPinnedEntry],
    // A native-class entry pinning the cloud key: refused on class, before the duplicate-key check.
    nativeWithCloudKey: [cloudPinnedEntry, { ...nativePinnedEntry, publicKeyBase64: cloudPinnedEntry.publicKeyBase64 }],
  };
  for (const [label, entries] of Object.entries(cases)) {
    const raw = JSON.stringify(entries);
    let parseMessage = "";
    try { parsePinnedKeys(raw); } catch (e) { parseMessage = String(e); }
    expect([label, parseMessage]).toEqual([label, NATIVE_REFUSED]);
    // Startup path: acquisitionConfig with the service enabled refuses the same way.
    let startupMessage = "";
    try { acquisitionConfig({ ...live, ACQUISITION_PINNED_KEYS: raw }); } catch (e) { startupMessage = String(e); }
    expect([label, startupMessage]).toEqual([label, NATIVE_REFUSED]);
    for (const m of [parseMessage, startupMessage])
      for (const secret of [nativePinnedEntry.publicKeyBase64, cloudPinnedEntry.publicKeyBase64, nativePinnedEntry.issuerKeyId, cloudPinnedEntry.issuerKeyId, NATIVE_SEED_HEX])
        expect([label, m.includes(secret)]).toEqual([label, false]);
  }
  // Disabled: ACQUISITION_PINNED_KEYS is not read at all (default off is unchanged).
  expect(acquisitionConfig({ ACQUISITION_PINNED_KEYS: JSON.stringify([nativePinnedEntry]) }).enabled).toBe(false);
});

test("verify-only: no service module can sign, and the removed variables are read only by the refusal check", async () => {
  const { readdirSync, readFileSync } = await import("node:fs");
  const dir = new URL("..", import.meta.url).pathname;
  const files = readdirSync(dir).filter((f) => f.endsWith(".ts")).sort();
  expect(files).toEqual(["app.ts", "config.ts", "installer.ts", "key-registry.ts", "main.ts", "validation.ts", "verifier.ts"]);
  const removed = [...REFUSED_ISSUER_ENV, "ACQUISITION_NATIVE_ENABLED"];
  for (const f of files) {
    const src = readFileSync(dir + f, "utf8");
    // No private-key parsing, signing, HMAC or symmetric encryption primitive anywhere in the service.
    for (const banned of [/createPrivateKey/, /generateKeyPair/, /createSign\b/, /createHmac/, /createCipheriv/, /\bsign\s*\(/, /subtle\./, /\bimport\s*\{[^}]*\bsign\b[^}]*\}\s*from\s*"node:crypto"/])
      expect([f, banned.source, banned.test(src)]).toEqual([f, banned.source, false]);
    // No issuer-era module or symbol survives.
    for (const gone of ["native-issuer", "intent-store", "NativeIssuer", "createIssuerSigner", "parseIssuerPrivateKey", "IssuerSigner", "intentKey", "assertKeysRetained"])
      expect([f, gone, src.includes(gone)]).toEqual([f, gone, false]);
    // The removed variables appear only in config.ts's refusal list/check (and its doc comment).
    if (f !== "config.ts") for (const name of removed) expect([f, name, src.includes(name)]).toEqual([f, name, false]);
  }
  const config = readFileSync(dir + "config.ts", "utf8");
  const body = config.slice(config.indexOf("export function acquisitionConfig"));
  for (const name of removed) expect([name, body.includes(name)]).toEqual([name, false]);
  // The key registry exposes public-key helpers only.
  expect(Object.keys(await import("../key-registry.ts")).sort()).toEqual(["ed25519PublicKey", "parsePinnedKeys", "rawPublicKey", "registryOfClass"]);
});

test("verify-only: the refusal check reads only the removed names, and a clean environment never touches a value", () => {
  const reads: string[] = [];
  const env = new Proxy({ ...live, ACQUISITION_OPS_ENABLED: "true" } as NodeJS.ProcessEnv, { get: (t, k) => { reads.push(String(k)); return t[String(k)]; } });
  const c = acquisitionConfig(env);
  expect(c.enabled).toBe(true);
  // Each removed name is looked up exactly once (the presence check) and nothing derived from it reaches the config.
  for (const name of [...REFUSED_ISSUER_ENV, "ACQUISITION_NATIVE_ENABLED"]) expect([name, reads.filter((r) => r === name).length]).toEqual([name, 1]);
  expect(JSON.stringify(c, (_k, v) => (v instanceof Map ? [...v.keys()] : v))).not.toMatch(/ISSUER_PRIVATE|INTENT_KEY|signer/i);
});
