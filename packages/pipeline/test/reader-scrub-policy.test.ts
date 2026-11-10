import { describe, expect, it } from "vitest";
import { assertReaderScrubMarker, parseReaderMaxRuleset, readerChunkBody, readerMaxRuleset } from "../src/lib/scrub-engine/reader-policy.js";
import { bindgenProfile, parseScrubBindgenPin } from "../src/lib/scrub-engine/bindgen-engine.js";
import { SCRUB_RULESET_VERSION } from "../src/lib/pipeline/raw-chunks.js";

describe("reader ruleset capability independent of writer stamps", () => {
  it("leaves legacy reads unchanged when unset, explicitly admits 10, leaves the TS writer at 8", () => {
    expect(parseReaderMaxRuleset(undefined)).toBeUndefined();
    expect(parseReaderMaxRuleset("10")).toBe(10);
    expect(SCRUB_RULESET_VERSION).toBe(8);
    for (const version of [1, 7, 8, 9, 10])
      expect(() => assertReaderScrubMarker({ ruleset: "relayhistory-scrub", version }, 10)).not.toThrow();
  });
  for (const value of ["1", "7", "", " 10", "10 ", "010", "+10", "1e1", "1.0", "0", "11", "9999999999999999999999999", 10, null]) {
    it(`refuses malformed/out-of-capability config ${JSON.stringify(value)} without echoing it`, () => {
      expect(() => parseReaderMaxRuleset(value)).toThrow("READER_MAX_RULESET must be an integer from 8 through 10");
    });
  }
  for (const marker of [null, {}, { ruleset: "SYNTH_PRIVATE", version: 1 }, { ruleset: "relayhistory-scrub", version: 0 }, { ruleset: "relayhistory-scrub", version: 11 }, { ruleset: "relayhistory-scrub", version: 1.5 }, { ruleset: "relayhistory-scrub", version: "10" }]) {
    it(`refuses unsupported markers atomically ${JSON.stringify(marker)}`, () => {
      let error: unknown;
      try { assertReaderScrubMarker(marker, 10); } catch (e) { error = e; }
      expect(error).toMatchObject({ code: "scrub_ruleset_unsupported", retryable: false });
      expect(String(error)).not.toContain("SYNTH_PRIVATE");
    });
  }

  function pin(version: number) {
    const manifest = { schema: 3, ruleset: "relayhistory-scrub", rulesetVersion: version, rulesetSourceSha256: "1".repeat(64), crateVersion: "0.0.0-inert-test", abiVersion: 2, policy: "secrets", redactionToken: "[REDACTED]", scrubProfile: `relayhistory-secrets-v${version}` };
    return { schema: 1, package: "@agentworkforce/relayscrubber", package_version: manifest.crateVersion, wasm_sha256: "2".repeat(64), glue_sha256: "3".repeat(64), loader_sha256: "4".repeat(64), paths: { wasm: "generated/test.wasm", glue: "generated/test.js", loader: "index.js" }, manifest, scrub_profile: bindgenProfile(manifest) };
  }
  it("rejects writer pins above the reader ceiling before any artifact/glue loads", () => {
    expect(() => parseScrubBindgenPin(pin(10), 8)).toThrow(expect.objectContaining({ reason: "bindgen_ruleset_above_reader_ceiling" }));
    expect(parseScrubBindgenPin(pin(10), 10).manifest.rulesetVersion).toBe(10);
    expect(() => parseScrubBindgenPin(pin(7), 10)).toThrow(expect.objectContaining({ reason: "bindgen_ruleset_below_floor" }));
    expect(() => parseScrubBindgenPin(pin(11), 10)).toThrow(expect.objectContaining({ reason: "bindgen_ruleset_above_reader_ceiling" }));
  });
});


it("mixed-version chunks use same-response metadata and cancel unsupported bodies", async () => {
  for (const metadata of [{ "scrub-ruleset": "relayhistory-scrub", "scrub-version": "10" }, { "scrub-ruleset": "foreign", "scrub-version": "8" }, { "scrub-ruleset": "relayhistory-scrub", "scrub-version": "7" }]) {
    let cancelled = false;
    const object = { meta: { metadata }, body: new ReadableStream<Uint8Array>({ cancel() { cancelled = true; } }) };
    await expect(readerChunkBody(object as never, { ruleset: "relayhistory-scrub", version: 8 }, 8)).rejects.toMatchObject({ code: "scrub_ruleset_unsupported", retryable: false });
    expect(cancelled).toBe(true);
  }
});
it("future manifest versions require chunk metadata; legacy metadata-free reads remain readable", async () => {
  const object = () => ({ meta: { metadata: {} }, body: new ReadableStream<Uint8Array>() });
  await expect(readerChunkBody(object() as never, { ruleset: "relayhistory-scrub", version: 10 }, 10)).rejects.toMatchObject({ code: "scrub_ruleset_unsupported" });
  const legacy = object();
  expect(await readerChunkBody(legacy as never, { ruleset: "relayhistory-scrub", version: 1 }, 8)).toBe(legacy.body);
  await legacy.body.cancel();
});


it("unset policy preserves legacy markers and never consults chunk metadata or ambient process env", async () => {
  const saved = process.env.READER_MAX_RULESET;
  process.env.READER_MAX_RULESET = "8";
  try {
    expect(readerMaxRuleset({})).toBeUndefined();
    for (const marker of [{ruleset:"relayhistory-scrub",version:9},{ruleset:"foreign-family",version:8}]) {
      expect(() => assertReaderScrubMarker(marker)).not.toThrow();
      let touched = false;
      const body = new ReadableStream<Uint8Array>();
      const object = {body, get meta() { touched=true; throw new Error("legacy GET has no metadata contract"); }};
      expect(await readerChunkBody(object as never, marker)).toBe(body);
      expect(touched).toBe(false);
      await body.cancel();
      expect(() => assertReaderScrubMarker(marker,8)).toThrow(expect.objectContaining({code:"scrub_ruleset_unsupported"}));
    }
  } finally {
    if (saved===undefined) delete process.env.READER_MAX_RULESET;
    else process.env.READER_MAX_RULESET=saved;
  }
});


it("metadata-free legacy objects remain readable under explicit policy; future objects refuse by code",async()=>{
 for(const version of [1,10]){
  let cancelled=false;const body=new ReadableStream<Uint8Array>({cancel(){cancelled=true;}});
  const object={meta:{},body};const read=readerChunkBody(object as never,{ruleset:"relayhistory-scrub",version},10);
  if(version===1){expect(await read).toBe(body);expect(cancelled).toBe(false);await body.cancel();}
  else{await expect(read).rejects.toMatchObject({code:"scrub_ruleset_unsupported",retryable:false});expect(cancelled).toBe(true);}
 }
});
