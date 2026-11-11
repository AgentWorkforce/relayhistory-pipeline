/**
 * SCRUB_ENGINE=wasm-bindgen (bindgen-engine.ts, docs/scrub-engine.md "wasm-bindgen path").
 *
 * No relayscrubber artifact is qualified or pinned here; these tests use doubles: fake wasm / glue /
 * loader bytes whose digests are pinned at run time, and a glue double whose scrub
 * functions wrap the TypeScript scrubber (a stand-in, not an oracle claim). One group
 * evaluates a fake glue SOURCE through the real `data:` import and the real file reader.
 *
 * Covered: happy path; every placeholder pin field refused by name; each artifact's
 * digest mismatch / absence; every manifest field mismatch; glue errors and traps
 * discard the input; double and concurrent init; zero-change inputs still call the
 * engine; raw-ABI and bindgen selection are exclusive; the Workers compiled path.
 */
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { SCRUB_RULESET_VERSION, scrubLong as tsScrubLong, scrubRecords as tsScrubRecords } from "../src/lib/pipeline/raw-chunks.js";
import { isSafeIdentifier } from "../src/lib/pipeline/trace.js";
import { scrubJson as tsScrubJson, scrubText as tsScrubText } from "../src/lib/scrub.js";
import * as seam from "../src/lib/scrub-engine/index.js";
import {
  BINDGEN_MANIFEST_FIELDS,
  SCRUB_UNAVAILABLE_LOG_TEXT,
  ScrubEngineConfigError,
  ScrubEngineUnavailable,
  ScrubberError,
  SCRUBBER_CALL_REFUSAL_CODES,
  bindgenProfile,
  createCompiledBindgenEngine,
  initScrubEngine,
  parseScrubBindgenPin,
  resetScrubEngineForTests,
  scrubEngineReady,
  selectScrubEngine,
  tsScrubEngine,
  type ScrubEngineFailure,
} from "../src/lib/scrub-engine/index.js";
import { importGlueFromVerifiedBytes, initBindgenScrubEngine } from "../src/lib/scrub-engine/bindgen-engine.js";
import pinFile from "../scrub-bindgen-pin.json";

/** The placeholder pin checked in before the alpha.4 cutover (kept here as a refusal fixture). */
const PLACEHOLDER_PIN = Object.freeze({
  schema: 2,
  package: "@agentworkforce/relayscrubber",
  package_version: "UNPINNED",
  wasm_sha256: "0".repeat(64),
  glue_sha256: "0".repeat(64),
  loader_sha256: "0".repeat(64),
  paths: { wasm: "generated/relayscrubber_bg.wasm", glue: "generated/relayscrubber.js", loader: "index.js" },
  scrub_profile: "UNPINNED",
  manifest: {
    schema: 3,
    ruleset: "UNPINNED",
    rulesetVersion: 0,
    rulesetSourceSha256: "0".repeat(64),
    crateVersion: "UNPINNED",
    abiVersion: 0,
    policy: "UNPINNED",
    redactionToken: "UNPINNED",
    scrubProfile: "UNPINNED",
  },
  placeholder: "PLACEHOLDER",
});

const enc = new TextEncoder();
const dec = new TextDecoder();
const sha = (b: Uint8Array | string) => createHash("sha256").update(b).digest("hex");

// ---------------------------------------------------------------- doubles

const WASM = enc.encode("\0asm test double: not a real relayscrubber module");
const GLUE = enc.encode("// test double: not the generated relayscrubber.js\n");
const LOADER = enc.encode("// test double: not the relayscrubber index.js\n");
const BYTES = { wasm: WASM, glue: GLUE, loader: LOADER } as const;

const MANIFEST = {
  schema: 3,
  ruleset: "relayhistory-scrub",
  rulesetVersion: SCRUB_RULESET_VERSION,
  rulesetSourceSha256: sha("test-double ruleset source"),
  crateVersion: "0.0.0-test-double",
  abiVersion: 2,
  policy: "secrets",
  redactionToken: "[REDACTED]",
  scrubProfile: "relayhistory-secrets-v8",
};
type Manifest = typeof MANIFEST;

function makePin(over: Record<string, unknown> = {}, manifestOver: Record<string, unknown> = {}) {
  const manifest = { ...MANIFEST, ...manifestOver };
  return {
    schema: 2,
    package: "@agentworkforce/relayscrubber",
    package_version: "0.0.0-test-double",
    release: { source: "github-release", repository: "AgentWorkforce/relayscrubber", tag: "v0.0.0-test", tarball: "test.tgz", tarball_sha256: "e".repeat(64), sha256sums: { "artifact.json": "f".repeat(64) } },
    wasm_sha256: sha(WASM),
    glue_sha256: sha(GLUE),
    loader_sha256: sha(LOADER),
    paths: { wasm: "generated/relayscrubber_bg.wasm", glue: "generated/relayscrubber.js", loader: "index.js" },
    // From the base manifest: per-field overrides must hit their own refusal, not the profile's.
    scrub_profile: bindgenProfile(MANIFEST),
    manifest,
    ...over,
  };
}

type Op = "scrubText" | "scrubLong" | "scrubJson" | "scrubRecords";
interface GlueOptions {
  manifest?: Record<string, unknown> | string;
  /** "trap": WebAssembly.RuntimeError; "error": a deliberate Error code; "string": a thrown code string. */
  throwOn?: Partial<Record<Op, "trap" | "error" | "string">>;
  fault?: Partial<Record<Op, unknown>>;
  initThrows?: boolean;
  omit?: string;
  /** Override scrubJson's envelope. */
  jsonEnvelope?: (input: string) => string;
}

function glueDouble(o: GlueOptions = {}) {
  const calls: { op: string; arg: unknown }[] = [];
  const inits: unknown[] = [];
  const fail = (op: Op) => {
    if (Object.hasOwn(o.fault ?? {}, op)) throw o.fault![op];
    const kind = o.throwOn?.[op];
    if (kind === "trap") throw new WebAssembly.RuntimeError("unreachable");
    if (kind === "error") throw new Error("scrubber_invalid_input");
    if (kind === "string") throw "scrubber_invalid_input";
  };
  const glue: Record<string, unknown> = {
    default: async (input: unknown) => {
      inits.push(input);
      if (o.initThrows) throw new Error("CompileError");
      return {};
    },
    initSync: (input: unknown) => {
      inits.push(input);
      if (o.initThrows) throw new Error("CompileError");
      return {};
    },
    manifestJson: () => (typeof o.manifest === "string" ? o.manifest : JSON.stringify(o.manifest ?? MANIFEST)),
    scrubTextChecked: (s: string) => {
      calls.push({ op: "scrubTextChecked", arg: s });
      fail("scrubText");
      fail("scrubLong");
      return tsScrubLong(s);
    },
    scrubText: (s: string) => {
      calls.push({ op: "scrubText", arg: s });
      fail("scrubText");
      return tsScrubText(s);
    },
    scrubLong: (s: string) => {
      calls.push({ op: "scrubLong", arg: s });
      fail("scrubLong");
      return tsScrubLong(s);
    },
    scrubJson: (text: string) => {
      calls.push({ op: "scrubJson", arg: text });
      fail("scrubJson");
      if (o.jsonEnvelope) return o.jsonEnvelope(text);
      const value = tsScrubJson(JSON.parse(text) as unknown);
      return JSON.stringify({ value, jsonText: JSON.stringify(value), redactionCount: 0 });
    },
    scrubRecords: (bytes: Uint8Array) => {
      calls.push({ op: "scrubRecords", arg: bytes });
      fail("scrubRecords");
      const r = tsScrubRecords(bytes);
      return JSON.stringify({ text: dec.decode(r.scrubbed), recordCount: r.records, redactionCount: 0 });
    },
  };
  if (o.omit) delete glue[o.omit];
  return { glue, calls, inits };
}

interface Setup {
  glue?: GlueOptions;
  pin?: unknown;
  bytes?: Partial<Record<"wasm" | "glue" | "loader", Uint8Array | Error>>;
}
function setup(s: Setup = {}) {
  const double = glueDouble(s.glue);
  const imported: Uint8Array[] = [];
  const reads: string[] = [];
  const options = {
    env: { SCRUB_ENGINE: "wasm-bindgen" },
    bindgen: {
      pin: "pin" in s ? s.pin : makePin(),
      readArtifact: async (name: "wasm" | "glue" | "loader", path: string) => {
        reads.push(`${name}:${path}`);
        const b = s.bytes?.[name] ?? BYTES[name];
        if (b instanceof Error) throw b;
        return b;
      },
      importGlue: async (verified: Uint8Array) => {
        imported.push(verified);
        return double.glue;
      },
    },
  };
  return { ...double, imported, reads, options };
}

async function initError(options: Parameters<typeof initScrubEngine>[0]) {
  return initScrubEngine(options).then(
    () => null,
    (error: unknown) => error,
  );
}

beforeEach(() => resetScrubEngineForTests());
afterEach(() => resetScrubEngineForTests());

/** Every scrub entry point of the seam refuses with `reason`; logging and predicates never see the input. */
async function expectRefused(reason: ScrubEngineFailure) {
  const calls: [string, () => unknown][] = [
    ["scrubText", () => seam.scrubText("token=abc")],
    ["scrubLong", () => seam.scrubLong("token=abc")],
    ["scrubJson", () => seam.scrubJson({ password: "x" })],
    ["scrubNumber", () => seam.scrubNumber(4111111111111111)],
    ["scrubRecords", () => seam.scrubRecords(enc.encode('{"a":"b"}\n'))],
    ["activeScrubMarker", () => seam.activeScrubMarker()],
  ];
  for (const [name, call] of calls) {
    let thrown: unknown;
    try {
      call();
    } catch (error) {
      thrown = error;
    }
    expect(thrown, name).toBeInstanceOf(reason === "engine_failed" ? ScrubberError : ScrubEngineUnavailable);
    if (reason === "engine_failed") expect(thrown).toMatchObject({ code: "scrubber_instance_poisoned", disposition: "instance_poison" });
    expect((thrown as ScrubEngineUnavailable).reason, name).toBe(reason);
  }
  await expect(seam.sealRawChunk({} as never, { source: enc.encode("x\n") } as never)).rejects.toBeInstanceOf(reason === "engine_failed" ? ScrubberError : ScrubEngineUnavailable);
  expect(scrubEngineReady()).toBe(false);
  expect(seam.scrubTextForLog("token=abc")).toBe(SCRUB_UNAVAILABLE_LOG_TEXT);
  expect(isSafeIdentifier("0190a8f2-7c1e-7d3a-9b2f-1c2d3e4f5a6b")).toBe(false);
}

// ---------------------------------------------------------------- happy path

describe("wasm-bindgen: happy path with doubles", () => {
  it("verifies, imports the verified glue, initialises with the verified wasm snapshot, and scrubs through the glue", async () => {
    const s = setup();
    const engine = await initScrubEngine(s.options);
    expect(engine.kind).toBe("wasm-bindgen");
    expect(seam.activeScrubEngine()).toBe(engine);
    expect(scrubEngineReady()).toBe(true);
    expect(s.reads).toEqual(["wasm:generated/relayscrubber_bg.wasm", "glue:generated/relayscrubber.js", "loader:index.js"]);
    // The glue evaluated is a private copy of the verified bytes; the wasm is passed as bytes, never a path or URL.
    expect(s.imported).toHaveLength(1);
    expect(sha(s.imported[0]!)).toBe(sha(GLUE));
    expect(s.imported[0]).not.toBe(GLUE);
    expect(s.inits).toHaveLength(1);
    const arg = s.inits[0] as { module_or_path: unknown };
    expect(Object.keys(arg)).toEqual(["module_or_path"]);
    expect(arg.module_or_path).toBeInstanceOf(Uint8Array);
    expect(arg.module_or_path).not.toBe(WASM);
    expect(sha(arg.module_or_path as Uint8Array)).toBe(sha(WASM));

    const secret = `password=${"q".repeat(24)}`;
    expect(seam.scrubText(secret)).toBe(tsScrubText(secret));
    expect(seam.scrubLong(secret)).toBe(tsScrubLong(secret));
    expect(seam.scrubJson({ password: "hunter2-hunter2" })).toEqual(tsScrubJson({ password: "hunter2-hunter2" }));
    const line = enc.encode(`{"msg":"${secret}"}\n`);
    const r = seam.scrubRecords(line);
    expect(dec.decode(r.scrubbed)).toBe(dec.decode(tsScrubRecords(line).scrubbed));
    expect(r.records).toBe(1);
    // The marker is the instance's manifest identity.
    const profile = bindgenProfile(MANIFEST);
    expect(r.scrub).toEqual({ ruleset: MANIFEST.ruleset, version: MANIFEST.rulesetVersion, profile });
    expect(seam.activeScrubMarker()).toEqual(r.scrub);
    expect(engine.scrubProfile).toBe(profile);
    // The glue module itself is not reachable from the engine object.
    expect(Object.values(engine)).not.toContain(s.glue);
    expect(Object.isFrozen(engine)).toBe(true);
  });

  it("scrubJson returns the glue's jsonText, never its `.value` view or a reserialisation", async () => {
    const s = setup({
      glue: { jsonEnvelope: () => JSON.stringify({ value: { a: "FROM-VALUE" }, jsonText: '{"a":"FROM-JSONTEXT"}', redactionCount: 1 }) },
    });
    await initScrubEngine(s.options);
    expect(seam.scrubJson({ a: "x" })).toEqual({ a: "FROM-JSONTEXT" });
  });

  it("a zero-change input still goes through the engine on every entry point", async () => {
    const s = setup();
    await initScrubEngine(s.options);
    expect(seam.scrubText("plain")).toBe("plain");
    expect(seam.scrubLong("plain")).toBe("plain");
    expect(seam.scrubJson({ a: "plain" })).toEqual({ a: "plain" });
    expect(seam.scrubJson(null)).toBe(null);
    expect(seam.scrubJson(true)).toBe(true);
    expect(seam.scrubNumber(42)).toBe(42);
    expect(dec.decode(seam.scrubRecords(enc.encode('{"a":"plain"}\n')).scrubbed)).toBe('{"a":"plain"}\n');
    expect(seam.scrubTextForLog("plain")).toBe("plain");
    expect(isSafeIdentifier("0190a8f2-7c1e-7d3a-9b2f-1c2d3e4f5a6b")).toBe(true);
    expect(s.calls.map((c) => c.op)).toEqual([
      "scrubTextChecked",
      "scrubTextChecked",
      "scrubJson",
      "scrubJson",
      "scrubJson",
      "scrubJson",
      "scrubRecords",
      "scrubTextChecked",
      "scrubTextChecked",
    ]);
    expect(s.calls.filter((c) => c.op === "scrubJson").map((c) => c.arg)).toEqual(['{"a":"plain"}', "null", "true", "42"]);
  });

  it("a later write to the reader's buffers cannot reach what was hashed, evaluated or instantiated", async () => {
    const wasm = Uint8Array.from(WASM);
    const glue = Uint8Array.from(GLUE);
    const s = setup({ bytes: { wasm, glue } });
    const pending = initScrubEngine(s.options);
    // Mutate the caller-owned buffers while hashing is in flight.
    await Promise.resolve();
    setTimeout(() => {
      wasm.fill(0x41);
      glue.fill(0x42);
    }, 0);
    await pending;
    expect(sha(s.imported[0]!)).toBe(sha(GLUE));
    expect(sha((s.inits[0] as { module_or_path: Uint8Array }).module_or_path)).toBe(sha(WASM));
  });

  it("the sealRawChunk path is refused under wasm-bindgen, never run on TypeScript", async () => {
    await initScrubEngine(setup().options);
    await expect(seam.sealRawChunk({} as never, {} as never)).rejects.toMatchObject({ reason: "seal_path_not_routed" });
  });
});

// ---------------------------------------------------------------- pins

describe("wasm-bindgen: placeholder and malformed pins are refused by name", () => {
  it("the historical placeholder pin is refused; the checked-in alpha.5 pin is refused at the default reader ceiling", () => {
    expect(() => parseScrubBindgenPin(PLACEHOLDER_PIN)).toThrow(expect.objectContaining({ reason: "bindgen_unpinned_package_version" }));
    expect(() => parseScrubBindgenPin(pinFile)).toThrow(expect.objectContaining({ reason: "bindgen_ruleset_above_reader_ceiling" }));
    expect(parseScrubBindgenPin(pinFile, 10).package_version).toBe("0.5.0-alpha.5");
  });

  it("SCRUB_ENGINE=wasm-bindgen with the checked-in pin and default reader is unavailable, and every call site refuses", async () => {
    const error = await initError({ env: { SCRUB_ENGINE: "wasm-bindgen" } });
    expect(error).toBeInstanceOf(ScrubEngineUnavailable);
    expect((error as ScrubEngineUnavailable).reason).toBe("bindgen_ruleset_above_reader_ceiling");
    expect((error as Error).message).toBe("the scrub engine is unavailable (bindgen_ruleset_above_reader_ceiling)");
    await expectRefused("bindgen_ruleset_above_reader_ceiling");
  });

  it("selected but never initialised: not_initialised everywhere", async () => {
    selectScrubEngine({ SCRUB_ENGINE: "wasm-bindgen" });
    await expectRefused("not_initialised");
  });

  const ZERO = "0".repeat(64);
  const placeholderPin = PLACEHOLDER_PIN as unknown as { manifest: Record<string, unknown> } & Record<string, unknown>;
  const cases: { name: string; pin: () => unknown; reason: ScrubEngineFailure }[] = [
    { name: "not an object", pin: () => null, reason: "bindgen_pin_invalid" },
    { name: "pin schema 1 (no release provenance)", pin: () => makePin({ schema: 1 }), reason: "bindgen_pin_invalid" },
    { name: "another pin schema", pin: () => makePin({ schema: 3 }), reason: "bindgen_pin_invalid" },
    { name: "another package", pin: () => makePin({ package: "other" }), reason: "bindgen_pin_invalid" },
    { name: "package_version placeholder", pin: () => makePin({ package_version: placeholderPin.package_version }), reason: "bindgen_unpinned_package_version" },
    { name: "package_version empty", pin: () => makePin({ package_version: "" }), reason: "bindgen_unpinned_package_version" },
    { name: "manifest missing", pin: () => makePin({ manifest: undefined }), reason: "bindgen_pin_invalid" },
    { name: "manifest with a non-primitive extra field", pin: () => makePin({}, { extra: { a: 1 } }), reason: "bindgen_pin_invalid" },
    { name: "paths missing", pin: () => makePin({ paths: undefined }), reason: "bindgen_pin_paths_invalid" },
    { name: "absolute path", pin: () => makePin({ paths: { ...makePin().paths, wasm: "/etc/x.wasm" } }), reason: "bindgen_pin_paths_invalid" },
    { name: "escaping path", pin: () => makePin({ paths: { ...makePin().paths, glue: "../x.js" } }), reason: "bindgen_pin_paths_invalid" },
    { name: "scrub_profile placeholder", pin: () => makePin({ scrub_profile: "UNPINNED" }), reason: "bindgen_unpinned_scrub_profile" },
    { name: "scrub_profile not the manifest's", pin: () => makePin({ scrub_profile: "relayscrubber/other" }), reason: "bindgen_pin_profile_inconsistent" },
    { name: "ruleset below the TypeScript floor", pin: () => makePin({}, { rulesetVersion: SCRUB_RULESET_VERSION - 1 }), reason: "bindgen_ruleset_below_floor" },
  ];
  for (const name of ["wasm", "glue", "loader"] as const)
    cases.push(
      { name: `${name}_sha256 placeholder (all zero)`, pin: () => makePin({ [`${name}_sha256`]: ZERO }), reason: `bindgen_unpinned_${name}_sha256` },
      { name: `${name}_sha256 empty`, pin: () => makePin({ [`${name}_sha256`]: "" }), reason: `bindgen_unpinned_${name}_sha256` },
      { name: `${name}_sha256 not 64 hex`, pin: () => makePin({ [`${name}_sha256`]: "ab".repeat(31) }), reason: `bindgen_unpinned_${name}_sha256` },
      { name: `${name}_sha256 uppercase`, pin: () => makePin({ [`${name}_sha256`]: sha(BYTES[name]).toUpperCase() }), reason: `bindgen_unpinned_${name}_sha256` },
    );
  for (const field of BINDGEN_MANIFEST_FIELDS) {
    cases.push({
      name: `manifest.${field} placeholder`,
      pin: () => makePin({}, { [field]: field === "schema" ? 2 : placeholderPin.manifest[field] }),
      reason: `bindgen_unpinned_manifest_${field}`,
    });
    cases.push({ name: `manifest.${field} missing`, pin: () => makePin({}, { [field]: undefined }), reason: `bindgen_unpinned_manifest_${field}` });
  }
  cases.push({ name: "manifest.policy unknown", pin: () => makePin({}, { policy: "everything" }), reason: "bindgen_unpinned_manifest_policy" });

  const release = () => makePin().release as Record<string, unknown>;
  const releaseCases: [string, unknown][] = [
    ["release missing", undefined],
    ["release not an object", "github-release"],
    ["release an array", []],
    ["release unknown source", { ...release(), source: "npm" }],
    ["release extra key", { ...release(), integrity: "sha512-x" }],
    ["release key missing (tag)", { ...release(), tag: undefined }],
    ["release repository empty", { ...release(), repository: "" }],
    ["release repository without owner", { ...release(), repository: "relayscrubber" }],
    ["release tag placeholder", { ...release(), tag: "UNPINNED" }],
    ["release tarball with a directory", { ...release(), tarball: "dist/x.tgz" }],
    ["release tarball_sha256 not 64 hex", { ...release(), tarball_sha256: "ab".repeat(31) }],
    ["release tarball_sha256 uppercase", { ...release(), tarball_sha256: "E".repeat(64) }],
    ["release tarball_sha256 all zero", { ...release(), tarball_sha256: ZERO }],
    ["release sha256sums empty", { ...release(), sha256sums: {} }],
    ["release sha256sums digest malformed", { ...release(), sha256sums: { "artifact.json": "x" } }],
    ["release sha256sums name with a directory", { ...release(), sha256sums: { "generated/artifact.json": "f".repeat(64) } }],
  ];
  for (const [name, value] of releaseCases)
    cases.push({ name, pin: () => makePin(value === undefined ? { release: undefined } : { release: value }), reason: "bindgen_unpinned_release" });
  cases.push({ name: "unsupported ABI", pin: () => makePin({}, { abiVersion: 3 }), reason: "bindgen_unpinned_manifest_abiVersion" });
  for (const c of cases)
    it(`${c.name}: ${c.reason}, nothing read or evaluated`, async () => {
      const s = setup({ pin: c.pin() });
      const error = await initError(s.options);
      expect(error).toBeInstanceOf(ScrubEngineUnavailable);
      expect((error as ScrubEngineUnavailable).reason).toBe(c.reason);
      expect(s.reads).toEqual([]);
      expect(s.imported).toEqual([]);
      expect(s.inits).toEqual([]);
    });

  it("the placeholder pin is refused for every one of its fields when fixed one at a time", () => {
    // Walk the historical placeholder towards a real pin: each step exposes the next named refusal.
    const real = makePin();
    const steps: [string, ScrubEngineFailure][] = [
      ["package_version", "bindgen_unpinned_release"],
      ["release", "bindgen_unpinned_wasm_sha256"],
      ["wasm_sha256", "bindgen_unpinned_glue_sha256"],
      ["glue_sha256", "bindgen_unpinned_loader_sha256"],
      ["loader_sha256", "bindgen_unpinned_scrub_profile"],
      ["scrub_profile", "bindgen_unpinned_manifest_ruleset"],
    ];
    const pin = structuredClone(PLACEHOLDER_PIN) as Record<string, unknown> & { manifest: Record<string, unknown> };
    for (const [field, next] of steps) {
      pin[field] = (real as Record<string, unknown>)[field];
      expect(() => parseScrubBindgenPin(pin), field).toThrow(expect.objectContaining({ reason: next }));
    }
    const manifestSteps: [string, ScrubEngineFailure | null][] = [
      ["ruleset", "bindgen_unpinned_manifest_rulesetVersion"],
      ["rulesetVersion", "bindgen_unpinned_manifest_rulesetSourceSha256"],
      ["rulesetSourceSha256", "bindgen_unpinned_manifest_crateVersion"],
      ["crateVersion", "bindgen_unpinned_manifest_abiVersion"],
      ["abiVersion", "bindgen_unpinned_manifest_policy"],
      ["policy", "bindgen_unpinned_manifest_redactionToken"],
      ["redactionToken", "bindgen_unpinned_manifest_scrubProfile"],
      ["scrubProfile", null],
    ];
    for (const [field, next] of manifestSteps) {
      pin.manifest[field] = (MANIFEST as Record<string, unknown>)[field];
      if (next) expect(() => parseScrubBindgenPin(pin), field).toThrow(expect.objectContaining({ reason: next }));
    }
    delete pin.placeholder;
    expect(parseScrubBindgenPin(pin).manifest).toEqual(MANIFEST);
  });
});

// ---------------------------------------------------------------- artifacts and manifest

describe("wasm-bindgen: artifact verification before anything is evaluated", () => {
  for (const name of ["wasm", "glue", "loader"] as const) {
    it(`${name} digest mismatch: bindgen_sha256_mismatch_${name}, glue never imported, wasm never initialised`, async () => {
      const tampered = Uint8Array.from(BYTES[name]);
      tampered[tampered.length - 1] ^= 1;
      const s = setup({ bytes: { [name]: tampered } });
      const error = await initError(s.options);
      expect((error as ScrubEngineUnavailable).reason).toBe(`bindgen_sha256_mismatch_${name}`);
      expect(s.imported).toEqual([]);
      expect(s.inits).toEqual([]);
      await expectRefused(`bindgen_sha256_mismatch_${name}`);
    });

    it(`${name} unreadable or empty: bindgen_artifact_missing_${name}`, async () => {
      for (const bytes of [new Error("ENOENT"), new Uint8Array(0)]) {
        resetScrubEngineForTests();
        const s = setup({ bytes: { [name]: bytes } });
        expect(((await initError(s.options)) as ScrubEngineUnavailable).reason).toBe(`bindgen_artifact_missing_${name}`);
        expect(s.imported).toEqual([]);
      }
    });
  }

  it("loader corruption (one flipped byte) is refused even though the loader is never evaluated", async () => {
    const s = setup({ bytes: { loader: enc.encode("// test double: not the relayscrubber index.js\r\n") } });
    expect(((await initError(s.options)) as ScrubEngineUnavailable).reason).toBe("bindgen_sha256_mismatch_loader");
  });

  it("glue import failure: bindgen_glue_import_failed", async () => {
    const s = setup();
    s.options.bindgen.importGlue = async () => {
      throw new SyntaxError("Unexpected token");
    };
    expect(((await initError(s.options)) as ScrubEngineUnavailable).reason).toBe("bindgen_glue_import_failed");
  });

  for (const name of ["default", "manifestJson", "scrubTextChecked", "scrubJson", "scrubRecords"]) {
    it(`glue without ${name}: bindgen_export_missing`, async () => {
      const s = setup({ glue: { omit: name } });
      expect(((await initError(s.options)) as ScrubEngineUnavailable).reason).toBe("bindgen_export_missing");
    });
  }

  it("init throws: bindgen_init_failed", async () => {
    const s = setup({ glue: { initThrows: true } });
    expect(((await initError(s.options)) as ScrubEngineUnavailable).reason).toBe("bindgen_init_failed");
    await expectRefused("bindgen_init_failed");
  });

  it("SCRUB_BINDGEN_DIR that is not absolute: bindgen_dir_invalid", async () => {
    const error = await initError({ env: { SCRUB_ENGINE: "wasm-bindgen", SCRUB_BINDGEN_DIR: "relative/dir" }, bindgen: { pin: makePin() } });
    expect((error as ScrubEngineUnavailable).reason).toBe("bindgen_dir_invalid");
  });
});

describe("wasm-bindgen: the instance's manifest must equal the pin in every field", () => {
  const other: Record<string, unknown> = {
    schema: 4,
    ruleset: "relayhistory-scrub-other",
    rulesetVersion: SCRUB_RULESET_VERSION + 2,
    rulesetSourceSha256: sha("another source"),
    crateVersion: "0.0.1-test-double",
    abiVersion: 1,
    policy: "pii",
    redactionToken: "[SCRUBBED]",
    scrubProfile: "different-crate-profile",
  };
  for (const field of BINDGEN_MANIFEST_FIELDS) {
    it(`${field} differs: bindgen_manifest_mismatch_${field}`, async () => {
      const s = setup({ glue: { manifest: { ...MANIFEST, [field]: other[field] } } });
      expect(((await initError(s.options)) as ScrubEngineUnavailable).reason).toBe(`bindgen_manifest_mismatch_${field}`);
      await expectRefused(`bindgen_manifest_mismatch_${field}`);
    });
    it(`${field} absent: bindgen_manifest_mismatch_${field}`, async () => {
      const m: Record<string, unknown> = { ...MANIFEST };
      delete m[field];
      const s = setup({ glue: { manifest: m } });
      expect(((await initError(s.options)) as ScrubEngineUnavailable).reason).toBe(`bindgen_manifest_mismatch_${field}`);
    });
    it(`${field} of another type: bindgen_manifest_mismatch_${field}`, async () => {
      const value = (MANIFEST as Record<string, unknown>)[field];
      const s = setup({ glue: { manifest: { ...MANIFEST, [field]: typeof value === "number" ? String(value) : [value] } } });
      expect(((await initError(s.options)) as ScrubEngineUnavailable).reason).toBe(`bindgen_manifest_mismatch_${field}`);
    });
  }

  it("a field the pin does not list: bindgen_manifest_mismatch_extra_field; a pinned extra field must match", async () => {
    let s = setup({ glue: { manifest: { ...MANIFEST, segmenter: "v2" } } });
    expect(((await initError(s.options)) as ScrubEngineUnavailable).reason).toBe("bindgen_manifest_mismatch_extra_field");
    resetScrubEngineForTests();
    s = setup({ pin: makePin({}, { segmenter: "v2" }), glue: { manifest: { ...MANIFEST, segmenter: "v3" } } });
    expect(((await initError(s.options)) as ScrubEngineUnavailable).reason).toBe("bindgen_manifest_mismatch_extra_field");
    resetScrubEngineForTests();
    s = setup({ pin: makePin({}, { segmenter: "v2" }), glue: { manifest: { ...MANIFEST, segmenter: "v2" } } });
    expect((await initScrubEngine(s.options)).kind).toBe("wasm-bindgen");
  });

  for (const manifest of ["not json", "[1,2]", "null", '"schema"'])
    it(`manifestJson() returning ${manifest}: bindgen_manifest_invalid`, async () => {
      const s = setup({ glue: { manifest } });
      expect(((await initError(s.options)) as ScrubEngineUnavailable).reason).toBe("bindgen_manifest_invalid");
    });
});

// ---------------------------------------------------------------- scrub failures

describe("wasm-bindgen: a glue error discards the input", () => {
  const SECRET_INPUT = "token=do-not-return-me";
  const sites: [Op, () => unknown][] = [
    ["scrubText", () => seam.scrubText(SECRET_INPUT)],
    ["scrubLong", () => seam.scrubLong(SECRET_INPUT)],
    ["scrubJson", () => seam.scrubJson({ note: SECRET_INPUT })],
    ["scrubRecords", () => seam.scrubRecords(enc.encode(`{"note":"${SECRET_INPUT}"}\n`))],
  ];

  for (const [op, call] of sites) {
    for (const kind of ["error", "string"] as const)
      it(`${op}: an untagged ${kind} poisons the instance and returns nothing`, async () => {
        const s = setup({ glue: { throwOn: { [op]: kind } } });
        await initScrubEngine(s.options);
        expect(call).toThrow(expect.objectContaining({ code: "scrubber_engine_failure", disposition: "instance_poison" }));
        const before = s.calls.length;
        await expectRefused("engine_failed");
        expect(s.calls.length).toBe(before);
        expect(seam.scrubTextForLog(SECRET_INPUT)).toBe(SCRUB_UNAVAILABLE_LOG_TEXT);
      });

    it(`${op}: a trap (RuntimeError, e.g. panic or OOM) poisons the instance (engine_failed)`, async () => {
      const s = setup({ glue: { throwOn: { [op]: "trap" } } });
      await initScrubEngine(s.options);
      expect(call).toThrow(expect.objectContaining({ reason: "engine_failed" }));
      const before = s.calls.length;
      await expectRefused("engine_failed");
      // Never called again after the trap.
      expect(s.calls.length).toBe(before);
      // A repeat init does not resolve with the poisoned instance.
      expect(((await initError(s.options)) as ScrubEngineUnavailable).reason).toBe("engine_failed");
      expect(s.inits).toHaveLength(1);
      await expectRefused("engine_failed");
    });
  }

  it("scrubRecords: invalid UTF-8 is refused before the engine with chunk_not_utf8", async () => {
    const s = setup({ glue: { throwOn: { scrubRecords: "error" } } });
    await initScrubEngine(s.options);
    expect(() => seam.scrubRecords(new Uint8Array([0x7b, 0xff, 0x7d, 0x0a]))).toThrow(expect.objectContaining({ code: "chunk_not_utf8", retryable: false }));
    expect(s.calls).toEqual([]);
  });

  for (const op of ["scrubText", "scrubJson", "scrubRecords"] as const)
    it(`${op}: malformed glue output poisons the instance`, async () => {
      const s = setup();
      const engine = await initScrubEngine(s.options);
      s.glue[op === "scrubText" ? "scrubTextChecked" : op] = () => op === "scrubText" ? undefined : op === "scrubJson" ? '{"value":{"a":1}}' : '{"text":"x","recordCount":-1}';
      const call = op === "scrubText" ? () => engine.scrubText("x") : op === "scrubJson" ? () => engine.scrubJson({a:1}) : () => engine.scrubRecords(enc.encode("{}\n"));
      expect(call).toThrow(expect.objectContaining({ code: "scrubber_engine_failure", disposition: "instance_poison" }));
      expect(() => engine.scrubJson(undefined)).toThrow(expect.objectContaining({ code: "scrubber_instance_poisoned" }));
      expect(() => engine.scrubText("\uD800")).toThrow(expect.objectContaining({ code: "scrubber_instance_poisoned" }));
      expect(scrubEngineReady()).toBe(false);
    });

  it("refuses text that cannot cross wasm-bindgen's UTF-8 strings unchanged (lone surrogates)", async () => {
    const s = setup();
    await initScrubEngine(s.options);
    for (const v of ["a\uD800b", "\uDC00"]) expect(() => seam.scrubText(v)).toThrow(expect.objectContaining({ code: "scrub_input_not_well_formed" }));
    expect(() => seam.scrubJson(10n as unknown)).toThrow(expect.objectContaining({ code: "scrub_input_not_well_formed" }));
    expect(s.calls).toEqual([]);
  });
});

// ---------------------------------------------------------------- init lifecycle

describe("wasm-bindgen: double and concurrent init", () => {
  it("concurrent init with the same pin coalesces: one import, one init, the same engine", async () => {
    const s = setup();
    const [a, b, c] = await Promise.all([initScrubEngine(s.options), initScrubEngine(s.options), initScrubEngine(s.options)]);
    expect(a).toBe(b);
    expect(b).toBe(c);
    expect(s.imported).toHaveLength(1);
    expect(s.inits).toHaveLength(1);
  });

  it("concurrent init that fails: every caller sees the same failure, after one attempt", async () => {
    const s = setup({ glue: { manifest: { ...MANIFEST, crateVersion: "9.9.9" } } });
    const results = await Promise.allSettled([initScrubEngine(s.options), initScrubEngine(s.options)]);
    expect(results.map((r) => r.status)).toEqual(["rejected", "rejected"]);
    const [x, y] = results.map((r) => (r as PromiseRejectedResult).reason as ScrubEngineUnavailable);
    expect(x).toBe(y);
    expect(x!.reason).toBe("bindgen_manifest_mismatch_crateVersion");
    expect(s.inits).toHaveLength(1);
    // Sticky: a repeat with the same pin is the same failure, not a second initialisation.
    expect(await initError(s.options)).toBe(x);
    expect(s.inits).toHaveLength(1);
  });

  it("a second init with a different pin throws bindgen_init_conflict and leaves the verified engine in place", async () => {
    const s = setup();
    const engine = await initScrubEngine(s.options);
    const other = setup({ pin: makePin({ package_version: "0.0.1-test-double" }) });
    expect(((await initError(other.options)) as ScrubEngineUnavailable).reason).toBe("bindgen_init_conflict");
    expect(other.reads).toEqual([]);
    expect(seam.activeScrubEngine()).toBe(engine);
    expect(seam.scrubText("plain")).toBe("plain");
  });

  it("a second init with the same pin but different bytes throws bindgen_init_conflict", async () => {
    const engine = await initScrubEngine(setup().options);
    const tampered = Uint8Array.from(WASM);
    tampered[0] ^= 1;
    const other = setup({ bytes: { wasm: tampered } });
    expect(((await initError(other.options)) as ScrubEngineUnavailable).reason).toBe("bindgen_init_conflict");
    expect(other.imported).toEqual([]);
    // Same pin and same bytes: the same engine, no second initialisation.
    const same = setup();
    expect(await initScrubEngine(same.options)).toBe(engine);
    expect(same.imported).toEqual([]);
    expect(same.inits).toEqual([]);
  });
});

// ---------------------------------------------------------------- selection

describe("wasm-bindgen: selection", () => {
  it("SCRUB_ENGINE unset is still the TypeScript engine, byte-identical", async () => {
    expect(await initScrubEngine({ env: {} })).toBe(tsScrubEngine);
    const input = `Authorization: Bearer ${"z".repeat(40)} someone@example.com`;
    expect(seam.scrubText(input)).toBe(tsScrubText(input));
    expect(seam.scrubLong(input)).toBe(tsScrubLong(input));
    const line = enc.encode(`{"m":"${input}"}\n`);
    expect(Buffer.from(seam.scrubRecords(line).scrubbed).equals(Buffer.from(tsScrubRecords(line).scrubbed))).toBe(true);
  });

  it("the raw-ABI path and the bindgen path cannot both be selected, in either order", async () => {
    expect(selectScrubEngine({ SCRUB_ENGINE: "wasm" })).toBe("wasm");
    expect(() => selectScrubEngine({ SCRUB_ENGINE: "wasm-bindgen" })).toThrow(ScrubEngineConfigError);
    await expect(initScrubEngine(setup().options)).rejects.toBeInstanceOf(ScrubEngineConfigError);
    resetScrubEngineForTests();
    expect(selectScrubEngine({ SCRUB_ENGINE: "wasm-bindgen" })).toBe("wasm-bindgen");
    expect(() => selectScrubEngine({ SCRUB_ENGINE: "wasm" })).toThrow(ScrubEngineConfigError);
    expect(() => selectScrubEngine({})).toThrow(ScrubEngineConfigError);
  });

  it("wasm-bindgen never consults the raw-ABI loader or instantiate, even when given them", async () => {
    let rawCalls = 0;
    const s = setup();
    const engine = await initScrubEngine({
      ...s.options,
      pin: { schema: 1 },
      loadArtifact: async () => {
        rawCalls++;
        return { bytes: WASM };
      },
      instantiate: async () => {
        rawCalls++;
        return {};
      },
    });
    expect(engine.kind).toBe("wasm-bindgen");
    expect(rawCalls).toBe(0);
  });

  it("a bindgen failure never falls back to the raw-ABI or TypeScript engine", async () => {
    let rawCalls = 0;
    const s = setup({ glue: { initThrows: true } });
    await expect(
      initScrubEngine({ ...s.options, loadArtifact: async () => (rawCalls++, { bytes: WASM }), instantiate: async () => (rawCalls++, {}) }),
    ).rejects.toBeInstanceOf(ScrubEngineUnavailable);
    expect(rawCalls).toBe(0);
    expect(() => seam.activeScrubEngine()).toThrow(ScrubEngineUnavailable);
  });

  for (const value of ["WASM-BINDGEN", "wasm_bindgen", "bindgen", "wasm-bindgen "])
    it(`near-miss SCRUB_ENGINE ${JSON.stringify(value)} throws`, () => {
      expect(() => selectScrubEngine({ SCRUB_ENGINE: value })).toThrow(ScrubEngineConfigError);
    });
});

// ---------------------------------------------------------------- real data: import + file reader

describe("wasm-bindgen: default glue import from verified bytes and default file reader", () => {
  /** A fake glue SOURCE (plain JS, not wasm-bindgen output) shaped like the generated module. */
  const glueSource = (manifest: unknown) =>
    enc.encode(`let ready = false; let seen;
export default async function init(input) { seen = input && input.module_or_path; ready = seen instanceof Uint8Array; return {}; }
export function initSync() { throw new Error("not in this test"); }
export function manifestJson() { if (!ready) throw new Error("not initialised"); return ${JSON.stringify(JSON.stringify(manifest))}; }
export function scrubText(s) { return s.split("hunter2").join("[REDACTED]"); }
export function scrubLong(s) { return scrubText(s); }
export function scrubTextChecked(s) { return scrubText(s); }
export function scrubJson(t) { const jsonText = scrubText(t); return JSON.stringify({ value: JSON.parse(jsonText), jsonText, redactionCount: 0 }); }
export function scrubRecords(b) { const text = scrubText(new TextDecoder().decode(b)); return JSON.stringify({ text, recordCount: text.split("\\n").filter(Boolean).length, redactionCount: 0 }); }
export function seenLength() { return seen ? seen.length : -1; }
`);

  it("importGlueFromVerifiedBytes evaluates exactly the given bytes and resolves nothing from disk", async () => {
    const mod = (await importGlueFromVerifiedBytes(enc.encode("export const marker = 'from-bytes-" + sha("x").slice(0, 8) + "';"))) as { marker: string };
    expect(mod.marker).toBe(`from-bytes-${sha("x").slice(0, 8)}`);
    await expect(importGlueFromVerifiedBytes(enc.encode("import x from './relayscrubber_bg.js'; export default x;"))).rejects.toBeDefined();
  });

  it("end to end: SCRUB_BINDGEN_DIR + default reader + data: import + manifest check", async () => {
    const dir = mkdtempSync(join(tmpdir(), "scrub-bindgen-test-"));
    try {
      const glue = glueSource(MANIFEST);
      mkdirSync(join(dir, "generated"));
      writeFileSync(join(dir, "generated/relayscrubber_bg.wasm"), WASM);
      writeFileSync(join(dir, "generated/relayscrubber.js"), glue);
      writeFileSync(join(dir, "index.js"), LOADER);
      const pin = makePin({ glue_sha256: sha(glue) });
      const engine = await initScrubEngine({ env: { SCRUB_ENGINE: "wasm-bindgen", SCRUB_BINDGEN_DIR: dir }, bindgen: { pin } });
      expect(engine.kind).toBe("wasm-bindgen");
      expect(seam.scrubText("pw hunter2")).toBe("pw [REDACTED]");
      expect(seam.scrubJson({ pw: "hunter2" })).toEqual({ pw: "[REDACTED]" });
      expect(seam.scrubRecords(enc.encode('{"a":"hunter2"}\n')).records).toBe(1);
      // Rewriting the glue file afterwards changes nothing: it was evaluated from the verified bytes.
      writeFileSync(join(dir, "generated/relayscrubber.js"), "export function scrubText(s) { return s; }");
      expect(seam.scrubText("pw hunter2")).toBe("pw [REDACTED]");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("end to end: a glue file that differs from the pin is never evaluated", async () => {
    const dir = mkdtempSync(join(tmpdir(), "scrub-bindgen-test-"));
    try {
      mkdirSync(join(dir, "generated"));
      writeFileSync(join(dir, "generated/relayscrubber_bg.wasm"), WASM);
      writeFileSync(join(dir, "generated/relayscrubber.js"), "globalThis.__bindgenTestEvaluated = true; export {};");
      writeFileSync(join(dir, "index.js"), LOADER);
      const error = await initError({ env: { SCRUB_ENGINE: "wasm-bindgen", SCRUB_BINDGEN_DIR: dir }, bindgen: { pin: makePin() } });
      expect((error as ScrubEngineUnavailable).reason).toBe("bindgen_sha256_mismatch_glue");
      expect((globalThis as { __bindgenTestEvaluated?: boolean }).__bindgenTestEvaluated).toBeUndefined();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------- Workers compiled path

describe("wasm-bindgen: Workers compiled-module path (createCompiledBindgenEngine)", () => {
  /** A real, empty WebAssembly.Module: the double's initSync ignores it. */
  const ModuleCtor = WebAssembly.Module as unknown as new (bytes: Uint8Array) => WebAssembly.Module;
  const emptyModule = () => new ModuleCtor(new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]));

  it("initialises synchronously with the Module, verifies the manifest, and scrubs through the glue", () => {
    const d = glueDouble();
    const module = emptyModule();
    const engine = createCompiledBindgenEngine({ glue: d.glue, module, pin: makePin() });
    expect(engine.kind).toBe("wasm-bindgen");
    expect(d.inits).toEqual([{ module }]);
    expect(engine.scrubText("plain")).toBe("plain");
    expect(d.calls.map((c) => c.op)).toEqual(["scrubTextChecked"]);
    expect(engine.marker.profile).toBe(bindgenProfile(MANIFEST));
    // Same pin again: the same engine, no second initSync.
    expect(createCompiledBindgenEngine({ glue: d.glue, module, pin: makePin() })).toBe(engine);
    expect(d.inits).toHaveLength(1);
  });

  it("refuses a non-Module (bytes cannot be compiled or hashed here): bindgen_module_invalid", () => {
    expect(() => createCompiledBindgenEngine({ glue: glueDouble().glue, module: WASM, pin: makePin() })).toThrow(
      expect.objectContaining({ reason: "bindgen_module_invalid" }),
    );
  });

  it("refuses the placeholder pin, and the checked-in alpha.5 pin at the default reader ceiling", () => {
    expect(() => createCompiledBindgenEngine({ glue: glueDouble().glue, module: emptyModule(), pin: PLACEHOLDER_PIN })).toThrow(
      expect.objectContaining({ reason: "bindgen_unpinned_package_version" }),
    );
    expect(() => createCompiledBindgenEngine({ glue: glueDouble().glue, module: emptyModule(), pin: pinFile })).toThrow(
      expect.objectContaining({ reason: "bindgen_ruleset_above_reader_ceiling" }),
    );
  });

  it("refuses a manifest mismatch, and stays refused (sticky)", () => {
    const d = glueDouble({ manifest: { ...MANIFEST, policy: "pii" } });
    expect(() => createCompiledBindgenEngine({ glue: d.glue, module: emptyModule(), pin: makePin() })).toThrow(
      expect.objectContaining({ reason: "bindgen_manifest_mismatch_policy" }),
    );
    expect(() => createCompiledBindgenEngine({ glue: glueDouble().glue, module: emptyModule(), pin: makePin() })).toThrow(
      expect.objectContaining({ reason: "bindgen_init_conflict" }),
    );
  });

  it("refuses a second compiled init with a different pin", () => {
    createCompiledBindgenEngine({ glue: glueDouble().glue, module: emptyModule(), pin: makePin() });
    expect(() => createCompiledBindgenEngine({ glue: glueDouble().glue, module: emptyModule(), pin: makePin({ package_version: "x-test" }) })).toThrow(
      expect.objectContaining({ reason: "bindgen_init_conflict" }),
    );
  });

  it("glue without initSync: bindgen_export_missing", () => {
    const d = glueDouble({ omit: "initSync" });
    expect(() => createCompiledBindgenEngine({ glue: d.glue, module: emptyModule(), pin: makePin() })).toThrow(
      expect.objectContaining({ reason: "bindgen_export_missing" }),
    );
  });

  it("refused once the Node loader path has started, and the Node path is refused once the compiled path has", async () => {
    const pending = initBindgenScrubEngine({ pin: makePin(), readArtifact: async (name) => BYTES[name], importGlue: async () => glueDouble().glue });
    expect(() => createCompiledBindgenEngine({ glue: glueDouble().glue, module: emptyModule(), pin: makePin() })).toThrow(
      expect.objectContaining({ reason: "bindgen_path_conflict" }),
    );
    await pending;
    resetScrubEngineForTests();
    createCompiledBindgenEngine({ glue: glueDouble().glue, module: emptyModule(), pin: makePin() });
    const s = setup();
    expect(((await initError(s.options)) as ScrubEngineUnavailable).reason).toBe("bindgen_path_conflict");
    expect(s.reads).toEqual([]);
  });
});


describe("reader-first writer initialization gate", () => {
  const future = { ...MANIFEST, rulesetVersion: 10, scrubProfile: "relayhistory-secrets-v10" };
  const futurePin = () => makePin({ scrub_profile: bindgenProfile(future) }, { rulesetVersion: 10, scrubProfile: future.scrubProfile });
  it("Node refuses above-ceiling pins before loading and accepts only after explicit reader configuration", async () => {
    const s = setup({ pin: futurePin(), glue: { manifest: future } });
    await expect(initScrubEngine(s.options)).rejects.toMatchObject({ reason: "bindgen_ruleset_above_reader_ceiling" });
    expect(s.reads).toEqual([]);
    const engine = await initScrubEngine({ ...s.options, env: { SCRUB_ENGINE: "wasm-bindgen", READER_MAX_RULESET: "10" } });
    expect(engine.rulesetVersion).toBe(10);
    expect(s.reads).toHaveLength(3);
  });
  it("compiled-module initialization applies the same ceiling before glue initialization", () => {
    const glue = glueDouble({ manifest: future });
    const ModuleCtor = WebAssembly.Module as unknown as new (bytes: Uint8Array) => WebAssembly.Module;
    const options = { pin: futurePin(), glue: glue.glue, module: new ModuleCtor(new Uint8Array([0,97,115,109,1,0,0,0])) };
    expect(() => createCompiledBindgenEngine(options)).toThrow(expect.objectContaining({ reason: "bindgen_ruleset_above_reader_ceiling" }));
    expect(glue.inits).toEqual([]);
    expect(createCompiledBindgenEngine({ ...options, readerMaxRuleset: 10 }).rulesetVersion).toBe(10);
    expect(glue.inits).toHaveLength(1);
  });
});


it("reader capability alone never selects or relabels the TS writer", async () => {
  const engine = await initScrubEngine({ env: { SCRUB_ENGINE: "ts", READER_MAX_RULESET: "10" } });
  expect(engine.kind).toBe("ts"); expect(engine.rulesetVersion).toBe(8);
});


for (const path of ["node", "compiled"] as const) {
  describe(`${path}: bounded Rust text export is mandatory`, () => {
    const module = () => new (WebAssembly.Module as unknown as new (b: Uint8Array) => WebAssembly.Module)(
      new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]),
    );

    it("text and long calls use checked Rust entrypoint, never compatibility exports", async () => {
      const s = setup();
      let uncheckedCalls = 0;
      const unchecked = () => {
        uncheckedCalls++;
        throw new Error("unchecked compatibility route");
      };
      s.glue.scrubText = unchecked;
      s.glue.scrubLong = unchecked;
      const engine = path === "node"
        ? await initScrubEngine(s.options)
        : createCompiledBindgenEngine({ pin: makePin(), module: module(), glue: s.glue });
      expect(engine.scrubText("plain")).toBe("plain");
      expect(engine.scrubLong("plain")).toBe("plain");
      expect(s.calls.map((c) => c.op)).toEqual(["scrubTextChecked", "scrubTextChecked"]);
      expect(uncheckedCalls).toBe(0);
    });

    it("missing checked export refuses startup even when both compatibility exports exist", async () => {
      const s = setup({ glue: { omit: "scrubTextChecked" } });
      const start = () => path === "node"
        ? initScrubEngine(s.options)
        : createCompiledBindgenEngine({ pin: makePin(), module: module(), glue: s.glue });
      await expect(Promise.resolve().then(start)).rejects.toMatchObject({ reason: "bindgen_export_missing" });
      expect(s.calls).toEqual([]);
      expect(s.inits).toEqual([]);
    });
  });
}
