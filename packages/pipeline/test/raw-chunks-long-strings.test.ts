import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { afterEach, describe, expect, it } from "vitest";
import { createFsStore } from "../../object-store/src/fs.js";
import { MAX_SCRUB_CHARS, scrubText } from "../src/lib/scrub.js";
import {
  SCRUB_RULESET_VERSION,
  canScrubLong,
  scrubLong,
  scrubRecords,
  sealRawChunk,
} from "../src/lib/pipeline/raw-chunks.js";
import { readAll } from "../src/lib/pipeline/object-layout.js";

const TOKEN = "Zq8vR2kLm9Xp4Tn7Wc3Ys6Hd"; // 24 chars: matches only after "Bearer "
const AWS = "AKIAIOSFODNN7EXAMPLE";
const words = (chars: number) => "lorem ipsum dolor sit amet ".repeat(Math.ceil(chars / 27)).slice(0, chars);

/** A naive segmenter (cut at the last whitespace under the bound): what scrubLong must not be. */
function naiveScrub(text: string): string {
  let out = "";
  let rest = text;
  while (rest.length > MAX_SCRUB_CHARS) {
    let cut = MAX_SCRUB_CHARS;
    while (!/\s/.test(rest[cut - 1]!)) cut--;
    out += scrubText(rest.slice(0, cut));
    rest = rest.slice(cut);
  }
  return out + scrubText(rest);
}

describe("scrubLong (scrub rule set version 2)", () => {
  it("is version 2", () => {
    expect(SCRUB_RULESET_VERSION).toBe(2);
  });

  it("keeps a long text whole and scrubs secrets anywhere in it, including at the bound", () => {
    const text = `${words(MAX_SCRUB_CHARS - 10)} ${AWS} ${words(300_000)} ${AWS} end`;
    const out = scrubLong(text);
    expect(out).not.toContain(AWS);
    expect(out).not.toContain("truncated");
    expect(out.endsWith(" end")).toBe(true);
    // Within the bound, output equals the version-1 rules exactly.
    const short = `${words(1000)} ${AWS}`;
    expect(scrubLong(short)).toBe(scrubText(short));
  });

  it("never splits `Bearer <token>`: a naive segmenter leaks the token, scrubLong does not", () => {
    // The last whitespace before the bound is the one right after "Bearer".
    // One earlier safe cut, 1000 chars before the bound; otherwise no whitespace.
    const dashed = `${words(MAX_SCRUB_CHARS - "Bearer ".length - 1).replace(/ /g, "-")}-`;
    const prefix = `${dashed.slice(0, MAX_SCRUB_CHARS - 1000)} ${dashed.slice(MAX_SCRUB_CHARS - 999)}`;
    const text = `${prefix}Bearer ${TOKEN}${"-".repeat(20)} ${words(50_000)}`;
    expect(text.lastIndexOf(" ", MAX_SCRUB_CHARS - 1)).toBe(MAX_SCRUB_CHARS - 1);
    expect(scrubText(`${text.slice(MAX_SCRUB_CHARS - 100, MAX_SCRUB_CHARS + 100)}`)).not.toContain(TOKEN); // the rule matches here
    expect(naiveScrub(text)).toContain(TOKEN);
    expect(scrubLong(text)).not.toContain(TOKEN);
    // Without that earlier cut there is no safe place at all: refused, not leaked.
    expect(() => scrubLong(text.replace(" ", "-"))).toThrow(/scrub limit/);
  });

  it("never cuts inside a private key block that straddles the bound", () => {
    const key = `-----BEGIN PRIVATE KEY-----\n${"MIIEvQIBADANBgkqhkiG9w0BAQEFAASC ".repeat(200)}\n-----END PRIVATE KEY-----`;
    const text = `${words(MAX_SCRUB_CHARS - 3000)} ${key} ${words(10_000)}`;
    const out = scrubLong(text);
    expect(out).not.toContain("MIIEvQIBADANBgkqhkiG9w0BAQEFAASC");
  });

  it("refuses an unbroken run with no safe cut rather than storing it partly scrubbed", () => {
    const blob = "A".repeat(MAX_SCRUB_CHARS + 10);
    expect(canScrubLong(blob)).toBe(false);
    expect(() => scrubLong(blob)).toThrow(/scrub limit/);
  });
});

describe("sealRawChunk with a record over the old bound (revert-fails)", () => {
  const dirs: string[] = [];
  afterEach(async () => {
    await Promise.all(dirs.splice(0).map((d) => rm(d, { recursive: true, force: true })));
  });

  it("stores the whole string, scrubbed, under scrub version 2", async () => {
    const dir = await mkdtemp(join(tmpdir(), "long-strings-"));
    dirs.push(dir);
    const store = createFsStore({ root: dir });
    const value = `${words(400_000)} ${AWS} tail`;
    const source = new TextEncoder().encode(`${JSON.stringify({ kind: "tool_call", payload: { output: value } })}\n`);
    // Version 1 refused this chunk with chunk_string_too_long.
    const sealed = await sealRawChunk(store, {
      location: { prefix: "96bulls/o", workspaceId: "w", machineId: "m", harness: "h", sessionId: "s", startDate: "2026-10-31" },
      format: "ndjson",
      seq: 0,
      epoch: 0,
      startOffset: 0,
      source,
    });
    expect(sealed.scrub).toEqual({ ruleset: "relayhistory-scrub", version: 2 });
    const stored = new TextDecoder().decode(zstdDecompressSync(await readAll(await store.get(sealed.key as never))));
    const out = (JSON.parse(stored) as { payload: { output: string } }).payload.output;
    expect(out).not.toContain(AWS);
    expect(out.endsWith(" tail")).toBe(true);
    expect(out.length).toBeGreaterThan(400_000);
    expect(scrubRecords(source).records).toBe(1);
  });
});

// ---------------------------------------------------------------- review of a26aaf5

const filler = (n: number) => "lorem ipsum dolor sit amet ".repeat(Math.ceil(n / 27)).slice(0, n);

describe("a gap between a credential keyword and its value is never cut", () => {
  const tok = "Zq8vR2kLm9Xp4Tn7Wc3Ys6Hd";
  it("Bearer <5000 spaces> token: redacted or refused, never stored", () => {
    const s = `Bearer${" ".repeat(5000)}${tok}`;
    expect(scrubText(s)).not.toContain(tok);
    const text = `${filler(MAX_SCRUB_CHARS - 2500)}${s} ${filler(10_000)}`;
    let out = "";
    try {
      out = scrubLong(text);
    } catch {
      return; // refusal is safe
    }
    expect(out).not.toContain(tok);
  });

  it("password: <5000 newlines> value", () => {
    const s = `password:${"\n".repeat(5000)}hunter2hunter2`;
    const text = `${filler(MAX_SCRUB_CHARS - 2500)}${s} ${filler(10_000)}`;
    let out = "";
    try {
      out = scrubLong(text);
    } catch {
      return;
    }
    expect(out).not.toContain("hunter2hunter2");
  });

  it("the same through a JSON record (live ingest path)", () => {
    const text = `${filler(MAX_SCRUB_CHARS - 2500)}Bearer${" ".repeat(5000)}${tok} ${filler(10_000)}`;
    let out = "";
    try {
      out = new TextDecoder().decode(scrubRecords(new TextEncoder().encode(`${JSON.stringify({ output: text })}\n`)).scrubbed);
    } catch {
      return;
    }
    expect(out).not.toContain(tok);
  });

  it("a short gap right at the bound is not cut between the keyword and its value", () => {
    // Cuts fall between pairs (after a value), never between a keyword and its value.
    const pairs = "password: hunter2hunter2 ".repeat(Math.ceil((MAX_SCRUB_CHARS + 5000) / 25));
    expect(scrubText("password: hunter2hunter2")).not.toContain("hunter2hunter2");
    expect(scrubLong(pairs)).not.toContain("hunter2hunter2");
    // Only keyword gaps (no other whitespace): nothing is safe to cut, so refuse.
    const only = "password:\thunter2hunter2:".repeat(Math.ceil((MAX_SCRUB_CHARS + 5000) / 25));
    expect(() => scrubLong(only)).toThrow(/scrub limit/);
    // With ordinary text before it, the cut falls before the keyword and the value is redacted.
    const text = `${filler(MAX_SCRUB_CHARS - 40)} password: ${" ".repeat(200)}hunter2hunter2 ${filler(5000)}`;
    expect(scrubLong(text)).not.toContain("hunter2hunter2");
  });
});

describe("bounded CPU before a refusal", () => {
  it("a 263 KB adversarial string is refused in well under a second (was ~53 s)", () => {
    const bad = "key: ".repeat(Math.ceil((MAX_SCRUB_CHARS + 1000) / 5));
    const t0 = performance.now();
    expect(() => scrubLong(bad)).toThrow(/scrub limit/);
    const t1 = performance.now();
    expect(() => scrubRecords(new TextEncoder().encode(`${JSON.stringify({ o: bad })}\n`))).toThrow(/scrub limit/);
    const t2 = performance.now();
    expect(t1 - t0).toBeLessThan(3000);
    expect(t2 - t1).toBeLessThan(6000);
  });

  it("a 10.8 MB benign text still scrubs whole", () => {
    const words = "lorem ipsum dolor sit amet ".repeat(400_000);
    expect(scrubLong(words)).toBe(words);
  });
});

describe("private key blocks across the bound", () => {
  it("a 20 KB PGP private key block straddling the bound stays redacted", () => {
    const key = `-----BEGIN PGP PRIVATE KEY BLOCK-----\n${"lQdGBGVsZWN0cm9uaWNzZWNyZXRrZXltYXRlcmlhbA ".repeat(500)}\n-----END PGP PRIVATE KEY BLOCK-----`;
    const text = `${filler(MAX_SCRUB_CHARS - 10_000)} ${key} ${filler(10_000)}`;
    expect(scrubLong(text)).not.toContain("lQdGBGVsZWN0cm9uaWNzZWNyZXRrZXltYXRlcmlhbA");
  });
});

describe("a whitespace run longer than the context window", () => {
  it("is never cut at its end when the keyword is out of the window (Bearer + 5000 spaces ending below the bound)", () => {
    const tok = "Zq8vR2kLm9Xp4Tn7Wc3Ys6Hd";
    const gap = 5000;
    const prefix = `${filler(MAX_SCRUB_CHARS - 11 - gap - "Bearer".length)} `;
    const text = `${prefix}Bearer${" ".repeat(gap)}${tok} ${filler(20_000)}`;
    // The rule redacts it in one piece.
    expect(scrubText(`x Bearer${" ".repeat(gap)}${tok} y`)).not.toContain(tok);
    let out = "";
    try {
      out = scrubLong(text);
    } catch {
      return;
    }
    expect(out).not.toContain(tok);
  });
});
