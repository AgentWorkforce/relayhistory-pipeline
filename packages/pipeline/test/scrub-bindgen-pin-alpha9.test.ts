/**
 * The pinned relayscrubber 0.5.0-alpha.9 cutover package (docs/scrub-engine.md,
 * "Cutover: pinned relayscrubber 0.5.0-alpha.9").
 *
 * - Every vendored file under packages/pipeline/scrub-bindgen/ hashes to the pin AND
 *   to the release's own artifact.json (byte-for-byte vendoring).
 * - A mutated byte or a swapped module fails closed before the glue is evaluated.
 * - Reader-first: with READER_MAX_RULESET unset (8) or 9 the pin is refused with
 *   bindgen_ruleset_above_reader_ceiling; with 10 the real wasm loads.
 * - The crate-owned profile is compared verbatim, against the pin and the instance.
 */
import { readdirSync, statSync } from "node:fs";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { parseScrubBindgenPin } from "../src/lib/scrub-engine/bindgen-engine.js";
import {
  ScrubEngineUnavailable,
  activeScrubEngine,
  initScrubEngine,
  resetScrubEngineForTests,
  scrubEngineReady,
} from "../src/lib/scrub-engine/index.js";
import { PIN, TRIPLE, VENDOR_DIR, faultGlue, readVendored, sha256 } from "./support/real-bindgen.js";

const KEYS = ["SCRUB_ENGINE", "SCRUB_ENGINE_REQUIRED", "READER_MAX_RULESET", "SCRUB_BINDGEN_DIR"] as const;
let saved: Record<string, string | undefined>;
beforeEach(() => {
  saved = Object.fromEntries(KEYS.map((k) => [k, process.env[k]]));
  for (const k of KEYS) delete process.env[k];
  resetScrubEngineForTests();
});
afterEach(() => {
  for (const k of KEYS) if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
  resetScrubEngineForTests();
});

const EXPECTED = {
  package_version: "0.5.0-alpha.9",
  wasm_sha256: "4228d2cd50334a6b714dcbd8cfc89220fdf36b1c903d170f873ad9b738fee090",
  glue_sha256: "55dd6fc6a44ed35f846655ab08bf3161ffe105123cbe11d3e529923d6bdeae5b",
  loader_sha256: "ff139ab391a958475b455ef483e7119d66215d172647223965dc35348f912490",
  artifact_json_sha256: "3134f3b65f93fba7b050d47e243af2639f3c9d4b7ebfd863a7ad1f9d1e3fdec5",
  release: {
    source: "github-release",
    repository: "AgentWorkforce/relayscrubber",
    tag: "v0.5.0-alpha.9",
    tarball: "agentworkforce-relayscrubber-0.5.0-alpha.9.tgz",
    tarball_sha256: "c3103a59edd24075c35d974c955aee1faf3fd6fb6eeabcd92aea338ac2448832",
    sha256sums: { "artifact.json": "3134f3b65f93fba7b050d47e243af2639f3c9d4b7ebfd863a7ad1f9d1e3fdec5" },
  },
  manifest: {
    schema: 3,
    ruleset: "relayhistory-scrub",
    rulesetVersion: 10,
    rulesetSourceSha256: "7837062214ff56145c72999880179598244cff5f80554a4b697548920561c143",
    crateVersion: "0.5.0-alpha.9",
    abiVersion: 2,
    policy: "secrets",
    redactionToken: "[REDACTED]",
    scrubProfile: "relayhistory-secrets-v10",
  },
};

const ARTIFACT_JSON = readVendored("generated/artifact.json");
const artifact = JSON.parse(new TextDecoder().decode(ARTIFACT_JSON)) as {
  schema: number; toolchain: string; wasmBindgen: string; manifest: Record<string, unknown>;
  files: Record<string, string>; adapterFiles: Record<string, string>;
};

async function initError(options: Parameters<typeof initScrubEngine>[0]): Promise<unknown> {
  try { await initScrubEngine(options); } catch (error) { return error; }
  throw new Error("expected init to fail");
}

describe("alpha.9 pin and vendored bytes", () => {
  it("the checked-in pin holds exactly the reviewed alpha.9 values, with no placeholder marker", () => {
    expect(PIN.package_version).toBe(EXPECTED.package_version);
    expect(PIN.schema).toBe(2);
    expect(PIN.release).toStrictEqual(EXPECTED.release);
    // The release's SHA256SUMS line for artifact.json is the vendored artifact.json.
    expect(PIN.release.sha256sums["artifact.json"]).toBe(EXPECTED.artifact_json_sha256);
    expect(PIN.wasm_sha256).toBe(EXPECTED.wasm_sha256);
    expect(PIN.glue_sha256).toBe(EXPECTED.glue_sha256);
    expect(PIN.loader_sha256).toBe(EXPECTED.loader_sha256);
    expect(PIN.scrub_profile).toBe("relayhistory-secrets-v10");
    expect(PIN.manifest).toStrictEqual(EXPECTED.manifest);
    expect(Object.keys(PIN).sort()).toEqual(
      ["glue_sha256", "loader_sha256", "manifest", "package", "package_version", "paths", "release", "schema", "scrub_profile", "wasm_sha256"],
    );
    expect(JSON.stringify(PIN)).not.toMatch(/UNPINNED|PLACEHOLDER|0{64}/i);
  });

  it("recomputed sha256 of each pinned file equals the pin and artifact.json", () => {
    const pinned = { wasm: PIN.wasm_sha256, glue: PIN.glue_sha256, loader: PIN.loader_sha256 } as const;
    for (const name of ["wasm", "glue", "loader"] as const) {
      const digest = sha256(readVendored(PIN.paths[name]));
      expect(digest, name).toBe(pinned[name]);
      const base = PIN.paths[name].split("/").pop()!;
      expect(digest, `${name} vs artifact.json`).toBe(name === "loader" ? artifact.adapterFiles[base] : artifact.files[base]);
    }
  });

  it("every file artifact.json lists is vendored byte-for-byte, and nothing else is", () => {
    const listed = [
      ...Object.entries(artifact.files).map(([f, d]) => [`generated/${f}`, d] as const),
      ...Object.entries(artifact.adapterFiles).map(([f, d]) => [f, d] as const),
    ];
    for (const [rel, digest] of listed) expect(sha256(readVendored(rel)), rel).toBe(digest);
    expect(sha256(ARTIFACT_JSON)).toBe(EXPECTED.artifact_json_sha256);
    const walk = (dir: string, prefix = ""): string[] =>
      readdirSync(dir).flatMap((f) => (statSync(`${dir}${f}`).isDirectory() ? walk(`${dir}${f}/`, `${prefix}${f}/`) : [`${prefix}${f}`]));
    expect(walk(VENDOR_DIR).sort()).toEqual(
      [...listed.map(([rel]) => rel), "generated/artifact.json", ".gitattributes"].sort(),
    );
  });

  it("artifact.json's manifest is the pin's manifest verbatim; toolchain and package version agree", () => {
    expect(artifact.manifest).toStrictEqual(PIN.manifest);
    expect(artifact.schema).toBe(1);
    expect(artifact.toolchain).toBe("1.88.0");
    expect(artifact.wasmBindgen).toBe("0.2.104");
    expect(PIN.manifest.crateVersion).toBe(PIN.package_version);
  });
});

describe("reader-first ceiling", () => {
  it("READER_MAX_RULESET=8 refuses the pin (and the pin parser's default stays 8): bindgen_ruleset_above_reader_ceiling", async () => {
    expect(() => parseScrubBindgenPin(PIN)).toThrow(expect.objectContaining({ reason: "bindgen_ruleset_above_reader_ceiling" }));
    const error = await initError({ env: { SCRUB_ENGINE: "wasm-bindgen", SCRUB_ENGINE_REQUIRED: "wasm-bindgen", READER_MAX_RULESET: "8" } });
    expect((error as ScrubEngineUnavailable).reason).toBe("bindgen_ruleset_above_reader_ceiling");
    expect(scrubEngineReady()).toBe(false);
  });

  it("READER_MAX_RULESET=9 refuses the pin", async () => {
    expect(() => parseScrubBindgenPin(PIN, 9)).toThrow(expect.objectContaining({ reason: "bindgen_ruleset_above_reader_ceiling" }));
    const error = await initError({ env: { ...TRIPLE, READER_MAX_RULESET: "9" } });
    expect((error as ScrubEngineUnavailable).reason).toBe("bindgen_ruleset_above_reader_ceiling");
  });

  it("READER_MAX_RULESET=10 loads the real vendored wasm; the instance reports the pinned manifest", async () => {
    const engine = await initScrubEngine({ env: TRIPLE });
    expect(engine.kind).toBe("wasm-bindgen");
    expect(engine.marker).toStrictEqual({ ruleset: "relayhistory-scrub", version: 10, profile: "relayhistory-secrets-v10" });
    expect((engine as unknown as { manifest: unknown }).manifest).toStrictEqual(PIN.manifest);
    expect(activeScrubEngine()).toBe(engine);
    expect(scrubEngineReady()).toBe(true);
    expect(engine.scrubText("token=ghp_abcdefghijklmnopqrstuvwxyz0123456789")).toBe("token=[REDACTED]");
  });
});

describe("mutated and swapped bytes fail closed before evaluation", () => {
  const real = { wasm: readVendored(PIN.paths.wasm), glue: readVendored(PIN.paths.glue), loader: readVendored(PIN.paths.loader) };
  async function refusal(bytes: Partial<Record<keyof typeof real, Uint8Array>>) {
    const f = faultGlue();
    let imported = 0;
    const error = await initError({
      env: TRIPLE,
      bindgen: {
        readArtifact: async (name) => bytes[name] ?? real[name],
        importGlue: async (b) => { imported++; return f.importGlue(b); },
      },
    });
    return { error, imported };
  }

  for (const name of ["wasm", "glue", "loader"] as const)
    for (const at of ["first", "middle", "last"] as const)
      it(`one flipped bit in the ${at} byte of ${name}: bindgen_sha256_mismatch_${name}, glue never evaluated, not ready`, async () => {
        const mutated = Uint8Array.from(real[name]);
        const i = at === "first" ? 0 : at === "last" ? mutated.length - 1 : mutated.length >> 1;
        mutated[i] ^= 1;
        const { error, imported } = await refusal({ [name]: mutated });
        expect(error).toBeInstanceOf(ScrubEngineUnavailable);
        expect((error as ScrubEngineUnavailable).reason).toBe(`bindgen_sha256_mismatch_${name}`);
        expect(imported).toBe(0);
        expect(scrubEngineReady()).toBe(false);
      });

  const swaps: [string, Partial<Record<keyof typeof real, Uint8Array>>, string][] = [
    ["wasm <-> glue", { wasm: real.glue, glue: real.wasm }, "bindgen_sha256_mismatch_wasm"],
    ["glue <-> loader", { glue: real.loader, loader: real.glue }, "bindgen_sha256_mismatch_glue"],
    ["loader served as glue", { glue: real.loader }, "bindgen_sha256_mismatch_glue"],
    ["artifact.json served as wasm", { wasm: ARTIFACT_JSON }, "bindgen_sha256_mismatch_wasm"],
    ["truncated wasm", { wasm: real.wasm.subarray(0, real.wasm.length - 1) }, "bindgen_sha256_mismatch_wasm"],
    ["wasm with one appended byte", { wasm: Uint8Array.from([...real.wasm, 0]) }, "bindgen_sha256_mismatch_wasm"],
  ];
  for (const [name, bytes, reason] of swaps)
    it(`swapped module (${name}): ${reason}`, async () => {
      const { error, imported } = await refusal(bytes);
      expect((error as ScrubEngineUnavailable).reason).toBe(reason);
      expect(imported).toBe(0);
    });
});

describe("the crate-owned profile is compared verbatim", () => {
  const variants = ["Relayhistory-secrets-v10", "relayhistory-secrets-v10 ", "relayhistory-secrets-v9", "relayhistory-secrets"];
  for (const v of variants)
    it(`pin scrub_profile ${JSON.stringify(v)} alone: bindgen_pin_profile_inconsistent`, () => {
      expect(() => parseScrubBindgenPin({ ...PIN, scrub_profile: v }, 10)).toThrow(
        expect.objectContaining({ reason: "bindgen_pin_profile_inconsistent" }),
      );
    });

  for (const v of variants)
    it(`pin scrub_profile and manifest.scrubProfile both ${JSON.stringify(v)}: the real instance disagrees (bindgen_manifest_mismatch_scrubProfile)`, async () => {
      const pin = { ...PIN, scrub_profile: v, manifest: { ...PIN.manifest, scrubProfile: v } };
      const error = await initError({ env: { ...TRIPLE, SCRUB_ENGINE_REQUIRED: undefined }, bindgen: { pin } });
      expect((error as ScrubEngineUnavailable).reason).toBe("bindgen_manifest_mismatch_scrubProfile");
      expect(scrubEngineReady()).toBe(false);
    });
});
