/**
 * Credential-keyword sentence seam regression for the pinned relayscrubber package
 * (docs/scrub-engine.md, "Cutover: pinned relayscrubber 0.5.0-alpha.10").
 *
 * A value given in a plain sentence after one of the credential keywords (password,
 * passcode, passphrase, passwd, pwd, pw) must not survive any of the five seams production
 * uses: scrubText, scrubLong, scrubJson, scrubRecords and the routed raw-chunk sealer. The
 * real vendored wasm is loaded and verified by initScrubEngine under the self-host env
 * triple. Each seam is also checked for idempotence (scrubbing the output again changes
 * nothing).
 *
 * Controls: sentences that use a keyword without giving a value stay byte-identical, an
 * exact `project_key` holding a `host.tld/owner/repo` identity is kept, and path, IP-host
 * and domainless-host `project_key` values stay redacted. All values are synthetic.
 */
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createFsStore } from "../../object-store/src/fs.js";
import { readAll, type ObjectStore } from "../src/lib/pipeline/object-layout.js";
import type { SealRawChunk } from "../src/lib/pipeline/raw-chunks.js";
import {
  initScrubEngine,
  resetScrubEngineForTests,
  routedRawChunkSealer,
  scrubJson,
  scrubLong,
  scrubRecords,
  scrubText,
} from "../src/lib/scrub-engine/index.js";
import { PIN, TRIPLE } from "./support/real-bindgen.js";

const KEYWORDS = ["password", "passcode", "passphrase", "passwd", "pwd", "pw"] as const;
// Synthetic, assembled so no scanner reads a credential in the source.
const VALUE = ["Zq7v", "R2mK9x", "Lp4sTw"].join("");

/** [input, exact expected output] per keyword; the value must be gone from every output. */
function sentences(k: string): Array<readonly [string, string]> {
  return [
    [`my ${k} is ${VALUE}`, `my ${k} is [REDACTED]`],
    [`My ${k.toUpperCase()} was ${VALUE}`, `My ${k.toUpperCase()} was [REDACTED]`],
    [`The ${k} is ${VALUE}. Then restart the service.`, `The ${k} is [REDACTED]. Then restart the service.`],
    [`the ${k} is: ${VALUE}`, `the ${k} is: [REDACTED]`],
    [`${k}: ${VALUE}`, `${k}: [REDACTED]`],
  ];
}
const CASES = KEYWORDS.flatMap((k) => sentences(k).map(([input, expected]) => ({ k, input, expected })));

/** Keyword used with no value: must be byte-identical. */
const BENIGN = [
  ...KEYWORDS.flatMap((k) => [
    `Please reset your ${k} before Friday.`,
    `The ${k} field is required.`,
    `The ${k} is incorrect.`,
    `Forgot ${k}? Click the link.`,
  ]),
  "cd ~ && pwd",
];

const KEPT_PROJECT_KEYS = ["github.com/example/overlap-sandbox", "gitlab.example.org/team/app"] as const;
const REDACTED_PROJECT_KEYS = [
  "/home/someone/src/app", // path fallback
  "10.0.0.5/owner/repo", // IP host
  "localhost/owner/repo", // domainless host
  "buildhost/owner/repo", // domainless host
] as const;

const KEYS = ["SCRUB_ENGINE", "SCRUB_ENGINE_REQUIRED", "READER_MAX_RULESET", "SCRUB_BINDGEN_DIR"] as const;
let saved: Record<string, string | undefined>;
const dirs: string[] = [];
beforeEach(() => {
  saved = Object.fromEntries(KEYS.map((k) => [k, process.env[k]]));
  for (const k of KEYS) delete process.env[k];
  resetScrubEngineForTests();
});
afterEach(async () => {
  for (const k of KEYS) if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
  resetScrubEngineForTests();
  await Promise.all(dirs.splice(0).map((d) => rm(d, { recursive: true, force: true })));
});

async function realEngine() {
  const engine = await initScrubEngine({ env: TRIPLE });
  // Guard against a silent fallback: this must be the verified pinned instance.
  expect(engine.kind).toBe("wasm-bindgen");
  expect(PIN.package_version).toBe("0.5.0-alpha.10");
  expect((engine as unknown as { manifest: { crateVersion: string } }).manifest.crateVersion).toBe("0.5.0-alpha.10");
  return engine;
}

async function fsStore(): Promise<ObjectStore> {
  const root = await mkdtemp(join(tmpdir(), "keyword-sentence-seal-"));
  dirs.push(root);
  const s = createFsStore({ root });
  return {
    backend: s.backend, head: s.head.bind(s), get: s.get.bind(s), getObject: s.getObject.bind(s),
    list: s.list.bind(s), presignGet: s.presignGet.bind(s), put: s.put.bind(s), delete: s.delete.bind(s),
  } satisfies ObjectStore;
}

function chunk(text: string, seq: number): SealRawChunk {
  return {
    location: { prefix: "test/o", workspaceId: "w", machineId: "m", harness: "h", sessionId: `s${seq}`, startDate: "2026-11-20" },
    format: "ndjson", seq, epoch: 0, startOffset: 0, source: new TextEncoder().encode(text),
  };
}

const enc = new TextEncoder();
const dec = new TextDecoder();
const ndjson = (records: unknown[]) => records.map((r) => `${JSON.stringify(r)}\n`).join("");
const recordsOut = (text: string) => dec.decode(scrubRecords(enc.encode(text)).scrubbed);

describe("credential-keyword sentence seam regression (real vendored wasm)", () => {
  it("scrubText: the value is redacted, output is idempotent", async () => {
    await realEngine();
    for (const { input, expected } of CASES) {
      const out = scrubText(input);
      expect(out, input).toBe(expected);
      expect(out, input).not.toContain(VALUE);
      expect(scrubText(out), input).toBe(out);
    }
  });

  it("scrubLong: the value is redacted, output is idempotent (also inside a longer multi-line text)", async () => {
    await realEngine();
    for (const { input, expected } of CASES) {
      const out = scrubLong(input);
      expect(out, input).toBe(expected);
      expect(scrubLong(out), input).toBe(out);
    }
    const long = CASES.map((c) => c.input).join("\nnext line stays\n");
    const out = scrubLong(long);
    expect(out).toBe(CASES.map((c) => c.expected).join("\nnext line stays\n"));
    expect(out).not.toContain(VALUE);
    expect(scrubLong(out)).toBe(out);
  });

  it("scrubJson: the value is redacted in string fields and arrays, output is idempotent", async () => {
    await realEngine();
    for (const { input, expected } of CASES) {
      const out = scrubJson({ type: "user", text: input, parts: [input] });
      expect(out, input).toStrictEqual({ type: "user", text: expected, parts: [expected] });
      expect(JSON.stringify(out), input).not.toContain(VALUE);
      expect(scrubJson(out), input).toStrictEqual(out);
    }
  });

  it("scrubRecords: the value is redacted in every record, output is idempotent", async () => {
    await realEngine();
    const source = ndjson(CASES.map(({ input }) => ({ type: "user", text: input })));
    const result = scrubRecords(enc.encode(source));
    expect(result.records).toBe(CASES.length);
    const out = dec.decode(result.scrubbed);
    expect(out).not.toContain(VALUE);
    const lines = out.split("\n").filter((l) => l.length > 0);
    expect(lines.map((l) => (JSON.parse(l) as { text: string }).text)).toStrictEqual(CASES.map((c) => c.expected));
    expect(recordsOut(out)).toBe(out);
  });

  it("routedRawChunkSealer: the stored object never holds the value, resealing is idempotent", async () => {
    await realEngine();
    const sealer = routedRawChunkSealer();
    expect(sealer, "SCRUB_ENGINE=wasm-bindgen must route raw sealing to the bound engine").toBeDefined();
    const store = await fsStore();
    const seal = async (text: string, seq: number) => {
      const out = await sealer!(store, chunk(text, seq));
      const object = await store.getObject(out.key as Parameters<ObjectStore["getObject"]>[0]);
      expect(object.meta.metadata["scrub-version"]).toBe("10");
      expect(object.meta.metadata["scrub-profile"]).toBe("relayhistory-secrets-v10");
      return { records: out.entry.records, stored: dec.decode(zstdDecompressSync(await readAll(object.body))) };
    };
    const first = await seal(ndjson(CASES.map(({ input }) => ({ type: "user", text: input }))), 0);
    expect(first.records).toBe(CASES.length);
    expect(first.stored).not.toContain(VALUE);
    const texts = first.stored.split("\n").filter((l) => l.length > 0).map((l) => (JSON.parse(l) as { text: string }).text);
    expect(texts).toStrictEqual(CASES.map((c) => c.expected));
    const second = await seal(first.stored, 1);
    expect(second.stored).toBe(first.stored);
  });
});

describe("credential-keyword sentence seam regression: controls", () => {
  it("a keyword with no value is byte-identical through every seam", async () => {
    await realEngine();
    for (const input of BENIGN) {
      expect(scrubText(input), input).toBe(input);
      expect(scrubLong(input), input).toBe(input);
      expect(scrubJson({ text: input }), input).toStrictEqual({ text: input });
    }
    const records = BENIGN.map((text) => ({ text }));
    const out = recordsOut(ndjson(records));
    expect(out.split("\n").filter((l) => l.length > 0).map((l) => JSON.parse(l))).toStrictEqual(records);

    const sealer = routedRawChunkSealer()!;
    const store = await fsStore();
    const sealed = await sealer(store, chunk(ndjson(records), 0));
    const object = await store.getObject(sealed.key as Parameters<ObjectStore["getObject"]>[0]);
    const stored = dec.decode(zstdDecompressSync(await readAll(object.body)));
    expect(stored.split("\n").filter((l) => l.length > 0).map((l) => JSON.parse(l))).toStrictEqual(records);
  });

  it("project_key: a host.tld/owner/repo identity is kept; path, IP-host and domainless-host values stay redacted", async () => {
    await realEngine();
    for (const value of KEPT_PROJECT_KEYS) {
      expect(scrubText(`project_key=${value}`), value).toBe(`project_key=${value}`);
      expect(scrubLong(`project_key=${value}`), value).toBe(`project_key=${value}`);
      expect(scrubJson({ project_key: value }), value).toStrictEqual({ project_key: value });
    }
    for (const value of REDACTED_PROJECT_KEYS) {
      expect(scrubText(`project_key=${value}`), value).toBe("project_key=[REDACTED]");
      expect(scrubLong(`project_key=${value}`), value).toBe("project_key=[REDACTED]");
      expect(scrubJson({ project_key: value }), value).toStrictEqual({ project_key: "[REDACTED]" });
    }
    const records = [...KEPT_PROJECT_KEYS, ...REDACTED_PROJECT_KEYS].map((project_key) => ({ type: "session", project_key }));
    const expected = [
      ...KEPT_PROJECT_KEYS.map((project_key) => ({ type: "session", project_key })),
      ...REDACTED_PROJECT_KEYS.map(() => ({ type: "session", project_key: "[REDACTED]" })),
    ];
    const out = recordsOut(ndjson(records));
    expect(out.split("\n").filter((l) => l.length > 0).map((l) => JSON.parse(l))).toStrictEqual(expected);
    expect(recordsOut(out)).toBe(out);

    const sealer = routedRawChunkSealer()!;
    const store = await fsStore();
    const sealed = await sealer(store, chunk(ndjson(records), 0));
    const object = await store.getObject(sealed.key as Parameters<ObjectStore["getObject"]>[0]);
    const stored = dec.decode(zstdDecompressSync(await readAll(object.body)));
    expect(stored.split("\n").filter((l) => l.length > 0).map((l) => JSON.parse(l))).toStrictEqual(expected);
    for (const value of REDACTED_PROJECT_KEYS) expect(stored).not.toContain(value);
  });
});
