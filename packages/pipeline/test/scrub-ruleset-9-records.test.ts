/**
 * Rule set 9, records structured path: `scrubRecords` (the raw-chunk record-line tokenizer
 * in the segmenter block of raw-chunks.ts) scrubs a string value under the public replay
 * selector (`isPublicSelectorKey`: `idempotency_key`, any letter case, trimmed), directly
 * or anywhere below it, as a container, as `scrubJson` does: a nested credential assignment
 * runs to the string's end. Before this, the tokenizer scrubbed such a value as plain text
 * and stored rule set 8's single token:
 *
 *   {"idempotency_key":"password=SECRET_ALPHA SECRET_BETA"}
 *   before: {"idempotency_key":"password=[REDACTED] SECRET_BETA"}   (leak)
 *   now:    {"idempotency_key":"password=[REDACTED]"}
 *
 * Precedence: a secret key context (rule set 5, inherited) wins, so a string under both
 * is redacted whole. A public key's context covers its own value only, never a sibling's;
 * keys are scrubbed as text, never as containers.
 *
 * The frozen rule set 8 (`fixtures/scrub-ruleset-8.ts`) is the negative proof: its
 * `scrubJson` stores the tail for every shape here. The rule set 8 `scrubRecords` harness
 * (raw-chunks.ts over the frozen source) needs `vi.doMock`, so its leak proof and the
 * differential fuzz live in `scrub-v8-v9-fuzz.test.ts`. Synthetic values only.
 */
import { describe, expect, it } from "vitest";
import { MAX_SCRUB_CHARS, scrubJson, scrubRecord, scrubText } from "../src/lib/scrub.js";
import { scrubLong, scrubRecords } from "../src/lib/pipeline/raw-chunks.js";
import { scrubJson as v8Json } from "./fixtures/scrub-ruleset-8.js";

const enc = new TextEncoder(), dec = new TextDecoder();
/** scrubRecords over one NDJSON line, without its line ending. */
const rec = (line: string) => dec.decode(scrubRecords(enc.encode(`${line}\n`)).scrubbed).replace(/\n$/, "");
const R = "[REDACTED]";
const B = "password=SECRET_ALPHA SECRET_BETA";
const AB = "password=A B";

/** Output exact, a second pass byte-identical, and (when given) rule set 8 leaking `tail`. */
function check(input: unknown, expected: unknown, leakedByV8?: string) {
  const line = typeof input === "string" ? input : JSON.stringify(input);
  const want = typeof expected === "string" ? expected : JSON.stringify(expected);
  const once = rec(line);
  expect(once, line).toBe(want);
  expect(rec(once), `${line} second pass`).toBe(once);
  if (leakedByV8 !== undefined) {
    expect(JSON.stringify(v8Json(JSON.parse(line))), `${line} v8`).toContain(leakedByV8);
  }
  return once;
}

describe("ruleset 9 records path: public selector values are containers", () => {
  it("exact B: the tail is gone, the selector and peer fields stay", () => {
    check({ idempotency_key: B }, { idempotency_key: "password=[REDACTED]" }, "SECRET_BETA");
    check(
      { before: "keep-1", n: 7, idempotency_key: B, after: "keep-2", z: [1, "keep-3"] },
      { before: "keep-1", n: 7, idempotency_key: "password=[REDACTED]", after: "keep-2", z: [1, "keep-3"] },
      "SECRET_BETA",
    );
    // Same as scrubJson on the parsed record.
    expect(JSON.parse(rec(JSON.stringify({ idempotency_key: B })))).toEqual(scrubJson({ idempotency_key: B }));
  });

  it("covers every string below the selector: objects, arrays, any depth", () => {
    const cases: Array<[unknown, unknown]> = [
      [{ idempotency_key: { a: AB } }, { idempotency_key: { a: "password=[REDACTED]" } }],
      [{ idempotency_key: [AB, "x"] }, { idempotency_key: ["password=[REDACTED]", "x"] }],
      [{ task: { idempotency_key: AB } }, { task: { idempotency_key: "password=[REDACTED]" } }],
      [{ a: { b: { c: { idempotency_key: AB } } } }, { a: { b: { c: { idempotency_key: "password=[REDACTED]" } } } }],
      [{ idempotency_key: { a: [{ b: [AB] }] } }, { idempotency_key: { a: [{ b: ["password=[REDACTED]"] }] } }],
      [{ idempotency_key: [[[AB]]], peer: AB }, { idempotency_key: [[["password=[REDACTED]"]]], peer: "password=[REDACTED] B" }],
    ];
    for (const [input, expected] of cases) {
      check(input, expected, "B");
      expect(JSON.parse(rec(JSON.stringify(input))), JSON.stringify(input)).toEqual(scrubJson(input));
    }
  });

  it("treats every duplicate of the key as a container and keeps the written-name reuse", () => {
    check('{"idempotency_key":"ok","idempotency_key":"password=A B"}', '{"idempotency_key":"ok","idempotency_key":"password=[REDACTED]"}');
    check('{"idempotency_key":"password=A B","idempotency_key":"ok"}', '{"idempotency_key":"password=[REDACTED]","idempotency_key":"ok"}');
    check(
      '{"idempotency_key":"password=A B","x":1,"idempotency_key":"password=C D"}',
      '{"idempotency_key":"password=[REDACTED]","x":1,"idempotency_key":"password=[REDACTED]"}',
    );
    // A repeated source key is still written under its first name, never suffixed.
    const out = rec('{"idempotency_key":"a","IDEMPOTENCY_KEY":"password=A B","idempotency_key":"password=C D"}');
    expect(out).toBe('{"idempotency_key":"a","IDEMPOTENCY_KEY":"password=[REDACTED]","idempotency_key":"password=[REDACTED]"}');
    expect(out.match(/"idempotency_key(?: \(\d+\))?"/g)).toEqual(['"idempotency_key"', '"idempotency_key"']);
  });

  it("does not leak the context into siblings; a secret key context wins", () => {
    // A sibling after the selector keeps rule set 8's single-token behaviour (unchanged).
    check({ idempotency_key: "public-replay-42", note: AB }, { idempotency_key: "public-replay-42", note: "password=[REDACTED] B" });
    check({ note: AB, idempotency_key: "public-replay-42" }, { note: "password=[REDACTED] B", idempotency_key: "public-replay-42" });
    check({ idempotency_key: { a: AB }, note: AB }, { idempotency_key: { a: "password=[REDACTED]" }, note: "password=[REDACTED] B" });
    expect(JSON.parse(rec(JSON.stringify({ idempotency_key: "x", note: AB })))).toEqual(scrubJson({ idempotency_key: "x", note: AB }));
    // Secret context (rule set 5) first: the value is redacted whole, above or below the selector.
    check({ password: { idempotency_key: AB } }, { password: { idempotency_key: R } });
    check({ idempotency_key: { token: "public A B" } }, { idempotency_key: { token: R } });
    check({ api_key: [{ idempotency_key: "public-replay-42" }] }, { api_key: [{ idempotency_key: R }] });
    // Keys are never containers (as in scrubJson).
    check({ idempotency_key: { [AB]: 1 } }, { idempotency_key: { "password=[REDACTED] B": 1 } });
  });

  it("removes a tail after an existing marker and keeps a lone marker", () => {
    check({ idempotency_key: "password=[REDACTED] SECRET_BETA" }, { idempotency_key: "password=[REDACTED]" }, "SECRET_BETA");
    check({ idempotency_key: R }, { idempotency_key: R });
    check({ idempotency_key: "password=[REDACTED]" }, { idempotency_key: "password=[REDACTED]" });
  });

  it("keeps public values unchanged (B1/B2 controls)", () => {
    for (const key of ["idempotency_key", "IDEMPOTENCY_KEY", "Idempotency_Key", " idempotency_key "]) {
      check({ [key]: "public-replay-42" }, { [key]: "public-replay-42" });
      check({ [key]: B }, { [key]: "password=[REDACTED]" }, "SECRET_BETA");
    }
    check({ idempotency_key: "public-replay-20261109-1", n: 1 }, { idempotency_key: "public-replay-20261109-1", n: 1 });
    // Not the selector: idempotency_key_2, x_idempotency_key keep rule set 8 behaviour.
    check({ idempotency_key_2: AB }, { idempotency_key_2: "password=[REDACTED] B" });
  });

  it("bounds an inner quoted credential by its close and keeps the rest of the value", () => {
    check({ idempotency_key: 'x="password=A B" keep' }, { idempotency_key: 'x="password=[REDACTED]" keep' });
    check({ idempotency_key: "password%3DSECRET_ALPHA SECRET_BETA" }, { idempotency_key: "password%3D[REDACTED]" }, "SECRET_BETA");
    check({ idempotency_key: "line\npassword=A\tB C" }, { idempotency_key: "line\npassword=[REDACTED]" });
  });

  it("leaves text, long, JSON and record paths unchanged on exact B", () => {
    expect(scrubText(B)).toBe("password=[REDACTED] SECRET_BETA");
    expect(scrubLong(B)).toBe("password=[REDACTED] SECRET_BETA");
    expect(scrubText(`idempotency_key="${B}"`)).toBe('idempotency_key="password=[REDACTED]"');
    expect(scrubJson({ idempotency_key: B })).toEqual({ idempotency_key: "password=[REDACTED]" });
    expect(scrubRecord({ id: "i", task: { idempotency_key: B } })).toEqual({ id: "i", task: { idempotency_key: "password=[REDACTED]" } });
  });
});

describe("ruleset 9 records path: public selector values past the scrub bound", () => {
  const words = (n: number) => {
    let s = "";
    for (let i = 0; s.length < n; i++) s += `w${i % 997} `;
    return s;
  };

  it("keeps the rest of the value after a closed inner credential", () => {
    const value = `x="password=SECRET_ALPHA SECRET_BETA" ${words(MAX_SCRUB_CHARS + 20_000)}KEEPTAIL`;
    const out = JSON.parse(rec(JSON.stringify({ idempotency_key: value, peer: "keep" })));
    expect(out.peer).toBe("keep");
    expect(out.idempotency_key.startsWith('x="password=[REDACTED]" w0 ')).toBe(true);
    expect(out.idempotency_key.endsWith("KEEPTAIL")).toBe(true);
    expect(out.idempotency_key).not.toMatch(/SECRET_/);
    expect(out.idempotency_key.length).toBe(value.length - "SECRET_ALPHA SECRET_BETA".length + R.length);
  });

  it("drops the later segments when a nested credential runs to the string's end", () => {
    const value = `prefix password=SECRET_ALPHA ${words(MAX_SCRUB_CHARS + 20_000)}SECRET_BETA`;
    const line = JSON.stringify({ before: "keep-1", idempotency_key: value, after: "keep-2" });
    const once = rec(line);
    expect(once).toBe('{"before":"keep-1","idempotency_key":"prefix password=[REDACTED]","after":"keep-2"}');
    expect(rec(once)).toBe(once);
    // The same value under a plain key: segmented as text, rule set 8's single token.
    const plain = JSON.parse(rec(JSON.stringify({ note: value })));
    expect(plain.note.startsWith("prefix password=[REDACTED] w0 ")).toBe(true);
    expect(plain.note.endsWith("SECRET_BETA")).toBe(true);
  });

  it("redacts a credential found in a later segment to the string's end", () => {
    const head = words(MAX_SCRUB_CHARS + 20_000);
    const out = JSON.parse(rec(JSON.stringify({ idempotency_key: `${head}password=SECRET_ALPHA SECRET_BETA` })));
    expect(out.idempotency_key).toBe(`${head}password=${R}`);
  });

  it("still refuses a value with no safe cut", () => {
    const value = "a".repeat(MAX_SCRUB_CHARS + 10);
    for (const record of [{ idempotency_key: value }, { note: value }]) {
      let error: unknown;
      try {
        rec(JSON.stringify(record));
      } catch (e) {
        error = e;
      }
      expect(error).toMatchObject({ code: "chunk_string_too_long", retryable: false });
    }
  });
});
