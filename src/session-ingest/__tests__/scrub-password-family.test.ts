/**
 * Password-family disclosure regressions through the scrub seam, under the DEFAULT selection
 * (docs/scrub-engine.md, "Default flip" and "Cutover: pinned relayscrubber 0.5.0-alpha.9").
 *
 * Test-first: the security review FAILED relayscrubber 0.5.0-alpha.9 because natural-language
 * password disclosures ("my password is x") come back byte-identical while the assignment form
 * (`password = API_TOKEN;`) is redacted. The disclosure cases below are EXPECTED TO FAIL on the
 * alpha.9 pin; do not weaken them to make them pass. A repin that fixes the engine turns them green.
 *
 * One clean child process (fixtures/scrub-password-family-child.ts, spawned like
 * scrub-default-flip.test.ts: env -i style, temporary cwd without bunfig.toml, so no preload and
 * no TS grant; SCRUB_ENGINE, SCRUB_ENGINE_REQUIRED and READER_MAX_RULESET unset) initialises the
 * real vendored wasm-bindgen artifact once and reports, for every invented input, the output of
 * scrubText, scrubLong, scrubJson (nested and top level), scrubRecords (NDJSON) and the routed
 * raw-chunk sealer (sealed into a temporary fs store, read back, zstd-decompressed), each also
 * re-scrubbed for idempotence. Each (seam, case) pair is its own test below.
 *
 * Every input is invented. No credential-shaped string is stored here.
 */
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, test } from "bun:test";

const REPO = join(import.meta.dir, "..", "..", "..");
const CHILD = join(import.meta.dir, "fixtures", "scrub-password-family-child.ts");
const PIN_PATH = join(REPO, "packages", "pipeline", "scrub-bindgen-pin.json");
const BINDGEN_DIR = join(REPO, "packages", "pipeline", "scrub-bindgen");

// Pin binding. These constants identify the engine the password-family expectations were written
// against (relayscrubber 0.5.0-alpha.9, which fails the disclosure cases by design). A repin MUST
// update these constants together with the password-family expectations below, never one alone.
const PINNED = {
  package: "@agentworkforce/relayscrubber",
  package_version: "0.5.0-alpha.9",
  tag: "v0.5.0-alpha.9",
  tarball_sha256: "c3103a59edd24075c35d974c955aee1faf3fd6fb6eeabcd92aea338ac2448832",
  wasm_path: "generated/relayscrubber_bg.wasm",
  wasm_sha256: "4228d2cd50334a6b714dcbd8cfc89220fdf36b1c903d170f873ad9b738fee090",
  crateVersion: "0.5.0-alpha.9",
  rulesetSourceSha256: "7837062214ff56145c72999880179598244cff5f80554a4b697548920561c143",
  rulesetVersion: 10,
  scrub_profile: "relayhistory-secrets-v10",
  redactionToken: "[REDACTED]",
} as const;

const V10 = { ruleset: "relayhistory-scrub", version: 10, profile: "relayhistory-secrets-v10" };
const PLACEHOLDER = PINNED.redactionToken;

/** input = prefix + value + suffix; `value` must not survive any seam. */
interface Disclosure { name: string; prefix: string; value: string; suffix: string }
const disclosure = (name: string, prefix: string, value: string, suffix = ""): Disclosure => ({ name, prefix, value, suffix });
const inputOf = (c: Disclosure) => `${c.prefix}${c.value}${c.suffix}`;

const DISCLOSURES: readonly Disclosure[] = [
  disclosure("one-character letter value", "my password is ", "x"),
  disclosure("one-character symbol value", "the password is ", "#"),
  disclosure("symbol-heavy unquoted value", "password: ", "p@$$w0rd!&*"),
  disclosure("single-quoted value", "my password is '", "hunter2", "'"),
  disclosure("double-quoted value", 'my password is "', "hunter2", '"'),
  disclosure("multi-word natural-language value", "my password is ", "correct horse battery staple"),
  disclosure("passphrase keyword", "my passphrase is ", "x"),
  disclosure("pwd keyword", "my pwd is ", "x"),
  disclosure("pass: keyword", "pass: ", "x"),
  disclosure("trailing sentence after the value", "my password is ", "x", ", please keep it"),
];

/** Controls that already pass on alpha.9: the assignment form is redacted exactly. */
const inventedIdentifier = ["inv", "ented", "Tok", "en9"].join(""); // built at run time
const ASSIGNMENTS: readonly { name: string; input: string; expected: string; value: string }[] = [
  { name: "password = API_TOKEN; assignment", input: "password = API_TOKEN;", expected: "password = [REDACTED];", value: "API_TOKEN" },
  { name: "password = <invented identifier>; assignment", input: `password = ${inventedIdentifier};`, expected: "password = [REDACTED];", value: inventedIdentifier },
];

/** Documentary / reference text: must come back byte-for-byte unchanged from every seam. */
const BENIGN: readonly string[] = [
  "reset your password from the settings page",
  "the password field is required",
  "password rotation policy: every 90 days",
  "// validate password length",
  "/docs/password-reset",
];

/**
 * Problems with one scrubbed string for a disclosure case (empty = pass):
 * - a redaction placeholder must be present;
 * - one-character value: the value token in its position (prefix + value) must be gone;
 * - longer value: the value must be absent; no suffix of it (one character included) may follow a
 *   placeholder and no prefix of it may precede one; and no substring of it of length >= 2 may survive
 *   (protected suffix / prefix). Substrings are checked on the residual (output minus the
 *   untouched prefix, suffix and placeholders) and, for substrings that do not also occur in the
 *   surrounding context, on the whole output.
 */
function leaks(out: unknown, c: Disclosure): string[] {
  if (typeof out !== "string") return [`not a string: ${JSON.stringify(out)}`];
  const problems: string[] = [];
  const show = JSON.stringify(out);
  if (!out.includes(PLACEHOLDER)) problems.push(`no ${PLACEHOLDER} in ${show}`);
  if (c.value.length === 1) {
    if (out.includes(c.prefix + c.value)) problems.push(`value ${JSON.stringify(c.value)} still follows ${JSON.stringify(c.prefix)} in ${show}`);
    return problems;
  }
  if (out.includes(c.value)) problems.push(`value ${JSON.stringify(c.value)} survives in ${show}`);
  // No protected suffix / prefix, no minimum length: any leftover end of the value touching a
  // placeholder (one character included) counts as retained.
  for (let i = 1; i < c.value.length; i++) {
    const suffix = c.value.slice(i);
    if (out.includes(PLACEHOLDER + suffix)) problems.push(`suffix ${JSON.stringify(suffix)} retained after the placeholder in ${show}`);
  }
  for (let i = 1; i < c.value.length; i++) {
    const prefix = c.value.slice(0, i);
    if (out.includes(prefix + PLACEHOLDER)) problems.push(`prefix ${JSON.stringify(prefix)} retained before the placeholder in ${show}`);
  }
  let residual = out;
  if (residual.startsWith(c.prefix)) residual = residual.slice(c.prefix.length);
  if (c.suffix && residual.endsWith(c.suffix)) residual = residual.slice(0, -c.suffix.length);
  residual = residual.split(PLACEHOLDER).join("\u0000");
  const context = `${c.prefix}\u0000${c.suffix}\u0000${PLACEHOLDER}`;
  const kept = new Set<string>();
  for (let len = 2; len <= c.value.length; len++) {
    for (let i = 0; i + len <= c.value.length; i++) {
      const sub = c.value.slice(i, i + len);
      if (residual.includes(sub) || (!context.includes(sub) && out.includes(sub))) kept.add(sub);
    }
  }
  if (kept.size > 0) {
    const longest = [...kept].sort((a, b) => b.length - a.length)[0];
    problems.push(`${kept.size} substring(s) of the value survive (longest ${JSON.stringify(longest)}) in ${show}`);
  }
  return problems;
}

// biome-ignore lint/suspicious/noExplicitAny: child JSON
type Report = any;
// biome-ignore lint/suspicious/noExplicitAny: child JSON
type Result = any;

const cwd = mkdtempSync(join(tmpdir(), "scrub-password-family-"));
let report: Report;
const byInput = new Map<string, Result>();
const resultFor = (input: string): Result => {
  const r = byInput.get(input);
  if (!r) throw new Error(`no child result for ${JSON.stringify(input)}`);
  return r;
};

beforeAll(() => {
  const inputs = [...DISCLOSURES.map(inputOf), ...ASSIGNMENTS.map((a) => a.input), ...BENIGN];
  const proc = Bun.spawnSync([process.execPath, CHILD, JSON.stringify({ inputs, benignLines: BENIGN })], {
    cwd,
    env: { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "" },
    stdout: "pipe",
    stderr: "pipe",
  });
  const stdout = proc.stdout.toString().trim();
  const last = stdout.split("\n").at(-1) ?? "";
  if (proc.exitCode !== 0 || !last.startsWith("{")) {
    throw new Error(`password-family child exited ${proc.exitCode}\n${stdout}\n${proc.stderr.toString()}`);
  }
  report = JSON.parse(last);
  for (const r of report.results) byInput.set(r.input, r);
});
afterAll(() => rmSync(cwd, { recursive: true, force: true }));

/**
 * The scrubbed NDJSON lines. The bound engine re-serialises each record (keys sorted), so a
 * record is compared as parsed JSON (every string in it byte-identical), not as raw line bytes.
 * Returns whether every benign record is intact and the last (disclosure) record's content.
 */
function ndjson(source: string, out: string): { benignIntact: boolean; allIntact: boolean; content: unknown; lines: number } {
  const parse = (line: string | undefined): unknown => { try { return JSON.parse(line ?? ""); } catch { return `<unparseable: ${line}>`; } };
  const src = source.trimEnd().split("\n").map(parse);
  const got = out.trimEnd().split("\n").map(parse);
  const same = (i: number) => Bun.deepEquals(got[i], src[i], true);
  const benignIntact = got.length === src.length && src.slice(0, -1).every((_, i) => same(i));
  const last = got.at(-1) as { message?: { content?: unknown } } | string | undefined;
  const content = typeof last === "object" && last !== null ? last.message?.content : last;
  return { benignIntact, allIntact: benignIntact && same(src.length - 1), content, lines: got.length };
}

describe("default selection and pin binding", () => {
  test("the child ran the default selection: wasm-bindgen, no grant, no selection env, v10 marker, alpha.9 crate", () => {
    expect(report.grantsAtStart).toStrictEqual([]);
    expect(report.grantsAtEnd).toStrictEqual([]);
    expect(report.selectionEnv).toStrictEqual({ SCRUB_ENGINE: null, SCRUB_ENGINE_REQUIRED: null, READER_MAX_RULESET: null, SCRUB_BINDGEN_DIR: null });
    expect(report.kind).toBe("wasm-bindgen");
    expect(report.sameInstance).toBe(true);
    expect(report.marker).toStrictEqual(V10);
    expect(report.crateVersion).toBe(PINNED.crateVersion);
    expect(report.routed).toBe(true);
  });

  test("packages/pipeline/scrub-bindgen-pin.json still names the engine these expectations were written for", () => {
    const pin = JSON.parse(readFileSync(PIN_PATH, "utf8"));
    expect(pin.package).toBe(PINNED.package);
    expect(pin.package_version).toBe(PINNED.package_version);
    expect(pin.release.tag).toBe(PINNED.tag);
    expect(pin.release.tarball_sha256).toBe(PINNED.tarball_sha256);
    expect(pin.paths.wasm).toBe(PINNED.wasm_path);
    expect(pin.wasm_sha256).toBe(PINNED.wasm_sha256);
    expect(pin.scrub_profile).toBe(PINNED.scrub_profile);
    expect(pin.manifest.crateVersion).toBe(PINNED.crateVersion);
    expect(pin.manifest.rulesetSourceSha256).toBe(PINNED.rulesetSourceSha256);
    expect(pin.manifest.rulesetVersion).toBe(PINNED.rulesetVersion);
    expect(pin.manifest.redactionToken).toBe(PINNED.redactionToken);
  });

  test("the vendored wasm hashes to the pin's wasm_sha256", () => {
    const pin = JSON.parse(readFileSync(PIN_PATH, "utf8"));
    const digest = createHash("sha256").update(readFileSync(join(BINDGEN_DIR, pin.paths.wasm))).digest("hex");
    expect(digest).toBe(pin.wasm_sha256);
    expect(digest).toBe(PINNED.wasm_sha256);
  });
});

describe("scrubText", () => {
  for (const c of DISCLOSURES) {
    test(`redacts ${c.name}: ${JSON.stringify(inputOf(c))}`, () => {
      const r = resultFor(inputOf(c));
      expect(leaks(r.text, c)).toStrictEqual([]);
      expect(r.textTwice).toBe(r.text);
    });
  }
});

describe("scrubLong", () => {
  for (const c of DISCLOSURES) {
    test(`redacts ${c.name}: ${JSON.stringify(inputOf(c))}`, () => {
      const r = resultFor(inputOf(c));
      expect(leaks(r.long, c)).toStrictEqual([]);
      expect(r.longTwice).toBe(r.long);
    });
  }
});

describe("scrubJson (nested in object and array, and as a top-level string)", () => {
  for (const c of DISCLOSURES) {
    test(`redacts ${c.name}: ${JSON.stringify(inputOf(c))}`, () => {
      const r = resultFor(inputOf(c));
      const j = r.jsonNested;
      expect(Object.keys(j).sort()).toStrictEqual(["message", "notes", "type"]);
      expect(j.type).toBe("user");
      expect(j.message.role).toBe("user");
      expect(j.message.content[0].type).toBe("text");
      expect({
        nestedObject: leaks(j.message.content[0].text, c),
        nestedArray: leaks(j.notes[0], c),
        topLevel: leaks(r.jsonTop, c),
      }).toStrictEqual({ nestedObject: [], nestedArray: [], topLevel: [] });
      expect(r.jsonNestedTwice).toStrictEqual(j);
      expect(r.jsonTopTwice).toBe(r.jsonTop);
    });
  }
});

describe("scrubRecords (NDJSON, disclosure inside message content)", () => {
  for (const c of DISCLOSURES) {
    test(`redacts ${c.name}: ${JSON.stringify(inputOf(c))}`, () => {
      const r = resultFor(inputOf(c));
      const lines = r.source.trimEnd().split("\n").length;
      expect(r.records.records).toBe(lines);
      expect(r.records.scrub).toStrictEqual(V10);
      const n = ndjson(r.source, r.records.out);
      expect(n.lines).toBe(lines);
      expect(n.benignIntact).toBe(true);
      expect(leaks(n.content, c)).toStrictEqual([]);
      expect(r.records.twice).toBe(r.records.out);
    });
  }
});

describe("routedRawChunkSealer (sealed to a temp fs store, read back, decompressed)", () => {
  for (const c of DISCLOSURES) {
    test(`never persists ${c.name}: ${JSON.stringify(inputOf(c))}`, () => {
      const r = resultFor(inputOf(c));
      const lines = r.source.trimEnd().split("\n").length;
      expect(r.seal.routed).toBe(true);
      expect(r.seal.records).toBe(lines);
      expect(r.seal.scrub).toStrictEqual(V10);
      expect(r.seal.metadata["scrub-version"]).toBe("10");
      expect(r.seal.metadata["scrub-profile"]).toBe("relayhistory-secrets-v10");
      const n = ndjson(r.source, r.seal.stored);
      expect(n.benignIntact).toBe(true);
      expect(leaks(n.content, c)).toStrictEqual([]);
    });
  }
});

describe("control: the assignment form is redacted (passes on alpha.9)", () => {
  for (const a of ASSIGNMENTS) {
    test(`${a.name}: every seam`, () => {
      const r = resultFor(a.input);
      expect(r.text).toBe(a.expected);
      expect(r.textTwice).toBe(a.expected);
      expect(r.long).toBe(a.expected);
      expect(r.longTwice).toBe(a.expected);
      expect(r.jsonTop).toBe(a.expected);
      expect(r.jsonNested.message.content[0].text).toBe(a.expected);
      expect(r.jsonNested.notes[0]).toBe(a.expected);
      for (const stored of [r.records.out, r.seal.stored]) {
        const n = ndjson(r.source, stored);
        expect(n.benignIntact).toBe(true);
        expect(n.content).toBe(a.expected);
        expect(stored).not.toContain(a.value);
      }
      expect(r.records.twice).toBe(r.records.out);
      expect(r.seal.records).toBe(r.records.records);
    });
  }
});

describe("control: documentary / reference text is byte-identical (passes on alpha.9)", () => {
  for (const b of BENIGN) {
    test(`unchanged through every seam: ${JSON.stringify(b)}`, () => {
      const r = resultFor(b);
      expect(r.text).toBe(b);
      expect(r.long).toBe(b);
      expect(r.jsonTop).toBe(b);
      expect(r.jsonNested.message.content[0].text).toBe(b);
      expect(r.jsonNested.notes[0]).toBe(b);
      for (const stored of [r.records.out, r.seal.stored]) {
        const n = ndjson(r.source, stored);
        expect(n.allIntact).toBe(true);
        expect(n.content).toBe(b);
      }
      expect(r.records.twice).toBe(r.records.out);
      expect(r.records.records).toBe(r.source.trimEnd().split("\n").length);
      expect(r.seal.records).toBe(r.records.records);
    });
  }
});
