/**
 * Rule set 9 promise: on every input, rule set 9 never emits a marker rule set 8 redacted
 * (it only redacts more), in `scrubText`, `scrubJson` and `scrubRecords`; on input with no
 * credential-labelled assignment it is byte-identical to rule set 8.
 *
 * Rule set 9 changes the `<name>=<value>` secret-assignment rule only:
 * - a quoted value is redacted to its matching closing quote (through whitespace and line
 *   breaks) within `MAX_QUOTED_VALUE` units; rule set 8 stopped at the first whitespace;
 * - the separator may be written `=`, `:`, `\x3d`, `\x3a`, `%3D` or `%3A` (any hex case);
 * - an unquoted value leaves out a backslash run escaping the quote that ends it.
 * The explicit proof that rule set 8 leaks is in `scrub-ruleset-9.test.ts`.
 *
 * One allowance, on the `scrubRecords` record path only: a JSON string whose decoded value
 * no rule changes is checked again in its escaped form, and redacted WHOLE if anything
 * matches there (`scrubStringToken`). Where rule set 8 saw nothing in the decoded value but
 * rule set 9 now redacts an encoded-separator or quoted value in it, rule set 9 stores the
 * value redacted in place instead of `[REDACTED]` for the whole string, so markers rule set
 * 8 dropped with the rest of that string come back. That is allowed only when the string
 * is such a rule-set-8 whole-string fallback (so rule set 8's own decoded scrub removed no
 * marker from it) AND rule set 9's stored value is stable: a second rule set 9 pass over it
 * removes no further marker. The escaped-form match behind the fallback is often not a
 * credential at all: a JSON `\n` glued to the next word, so `<LF>idempotency_key : v`
 * reads `nidempotency_key : v` escaped (an exempt public selector no longer at a word
 * start). The count of stored values whose escaped form rule set 9 would still flag is
 * logged, not asserted.
 *
 * 2,000 inputs each by default (fixed seed 90909; about 35 s under vitest on the agent host, kept
 * under 2 minutes for CI); SCRUB_FUZZ_INPUTS raises it, SCRUB_FUZZ_SEED changes the seed.
 *
 * Rule set 8 is frozen in `fixtures/scrub-ruleset-8.ts`: byte for byte
 * `git show 5a16e910:packages/pipeline/src/lib/scrub.ts`, pinned by its sha256 below. Rule
 * set 8's `scrubRecords` is the current raw-chunks.ts (its segmenter is unchanged in rule
 * set 9) loaded with `lib/scrub.ts` mocked to that fixture.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { scrubJson as v8Json, scrubText as v8 } from "./fixtures/scrub-ruleset-8.js";
import { MAX_QUOTED_VALUE, scrubJson as v9Json, scrubText as v9 } from "../src/lib/scrub.js";

const V8_FIXTURE_SHA256 = "fce75866a2dbf312f1865210fe343e1c372d216c3318dfacc27de6a3f8a82cc7";
const INPUTS = Number(process.env.SCRUB_FUZZ_INPUTS ?? 2000);
const SEED = Number(process.env.SCRUB_FUZZ_SEED ?? 90909);

const enc = new TextEncoder();
const dec = new TextDecoder();
type Records = (input: Uint8Array) => { scrubbed: Uint8Array };
let v8RecordsRaw: Records;
let v9RecordsRaw: Records;
const v8Records = (text: string) => dec.decode(v8RecordsRaw(enc.encode(text)).scrubbed);
const v9Records = (text: string) => dec.decode(v9RecordsRaw(enc.encode(text)).scrubbed);

beforeAll(async () => {
  vi.resetModules();
  vi.doMock("../src/lib/scrub.js", () => import("./fixtures/scrub-ruleset-8.js"));
  v8RecordsRaw = (await import("../src/lib/pipeline/raw-chunks.js")).scrubRecords as Records;
  vi.doUnmock("../src/lib/scrub.js");
  vi.resetModules();
  v9RecordsRaw = (await import("../src/lib/pipeline/raw-chunks.js")).scrubRecords as Records;
  vi.resetModules();
});

function rng(start: number) {
  let seed = start;
  const rnd = (n: number) => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return Math.floor((seed / 0x80000000) * n);
  };
  const pick = <T,>(a: readonly T[]) => a[rnd(a.length)] as T;
  return { rnd, pick };
}

/** A plain lowercase word no rule matches on its own; unique per call. */
function markers() {
  let n = 0;
  const all: string[] = [];
  const next = () => {
    let k = n++;
    let s = "";
    do {
      s += String.fromCharCode(97 + (k % 26));
      k = Math.floor(k / 26);
    } while (k > 0);
    const m = `zqmark${s}zq`;
    all.push(m);
    return m;
  };
  return { next, all };
}

const B = "\\";
const NAMES = ["password", "api_key", "auth_token", "client_secret", "DB_PASSWORD", "token", "apikey", "pwd"];
const PLAIN = ["name", "note", "path", "author", "idempotency_key"];
const SEPS = ["=", ":", " = ", ": ", "\t=", `${B}u003d`, `${B}u003D`, `${B}u003a`, `${B}u003A`, `${B}x3d`, `${B}x3D`, `${B}x3a`, `${B}${B}u003d`, `${B}${B}x3D`, "%3D", "%3d", "%3A", "%3a"];
const QUOTES = ['"', "'", "`", `${B}"`, `${B}${B}${B}"`, "", "", ""];
const ATOMS = [" ", " ", "\n", "\r\n", "\t", ",", ";", B, `${B}${B}`, `${B}"`, '"', "'", "`", "a b", "x=1", "{", "}", ":", "AbC123xyz"];
/** Rule set 8 redacted `s` whole on the record path: nothing matched decoded, something escaped. */
const v8Fallback = (s: string) => v8(s) === s && v8(JSON.stringify(s)) !== JSON.stringify(s);
/** Markers in rule set 9's stored value for `s` that a second rule set 9 pass would remove. */
const v9Residual = (s: string, all: string[]) => {
  const out = v9(s);
  return all.filter((m) => out.includes(m) && !v9(out).includes(m));
};
/** Whether rule set 9 would still flag the escaped form of its stored value for `s`. */
const v9EscapedFlag = (s: string) => {
  const lit = JSON.stringify(v9(s));
  return v9(lit) !== lit;
};
/** Whether `text` holds a credential name followed by any separator form rule set 9 knows. */
const ASSIGNMENT_SHAPE = /(?:api[_-]?key|apikey|key|secret|token|password|passwd|passphrase|pwd|credentials?)\s*(?:[:=]|\\+(?:u003[ad]|x3[ad])|%3[ad])/i;

function generate(rnd: (n: number) => number, pick: <T>(a: readonly T[]) => T, mk: ReturnType<typeof markers>, plainOnly: boolean) {
  const filler = (n: number) => {
    let s = "";
    while (s.length < n) s += rnd(3) === 0 ? ` ${mk.next()} ` : rnd(4) === 0 ? "v".repeat(1 + rnd(40)) : pick(ATOMS);
    return s.slice(0, n);
  };
  let text = rnd(3) ? mk.next() : "";
  for (let p = 1 + rnd(3); p > 0; p -= 1) {
    const q = pick(QUOTES);
    const name = plainOnly ? pick(PLAIN) : rnd(5) === 0 ? pick(PLAIN) : pick(NAMES);
    const lead = rnd(3) === 0 ? `idempotency_key${pick(["=", ":", " : "])}${pick(["", '"'])}` : "";
    const r = rnd(10);
    const len = r < 6 ? rnd(30) : r < 9 ? rnd(400) : MAX_QUOTED_VALUE - 8 + rnd(16);
    text += pick(["", " ", "\n", "{", ", ", '"']) + lead + name + (plainOnly ? pick(["=", ": "]) : pick(SEPS)) + q;
    text += (rnd(2) ? mk.next() : "") + filler(len) + (q && rnd(4) ? q : "") + (rnd(2) ? ` ${mk.next()}` : "");
  }
  return text;
}

describe("rule set 8 vs 9 (differential fuzz)", () => {
  it("runs against the frozen rule set 8 source", () => {
    const fixture = readFileSync(fileURLToPath(new URL("./fixtures/scrub-ruleset-8.ts", import.meta.url).href), "utf8").replace(/\r\n/g, "\n");
    expect(createHash("sha256").update(fixture).digest("hex")).toBe(V8_FIXTURE_SHA256);
    expect(v8('password="zqmarkazq zqmarkbzq"')).toContain("zqmarkbzq");
    expect(v9('password="zqmarkazq zqmarkbzq"')).toBe('password="[REDACTED]"');
  });

  it(`never re-emits a marker rule set 8 redacted (${INPUTS} texts)`, () => {
    const { rnd, pick } = rng(SEED);
    let leaked = 0, unexpected = 0, differ = 0, morRedacted = 0, recordsChecked = 0, plain = 0, plainDiffer = 0, fallbackAllowed = 0, escapedFlagged = 0;
    let first = "";
    for (let k = 0; k < INPUTS; k += 1) {
      const mk = markers();
      // Every fifth input uses only plain names, independent of the generator's state.
      const plainOnly = k % 5 === 0;
      const text = generate(rnd, pick, mk, plainOnly);
      const shaped = ASSIGNMENT_SHAPE.test(text);
      if (!shaped) plain += 1;
      const outs: Array<[string, string, string]> = [
        ["scrubText", v8(text), v9(text)],
        ["scrubJson", (v8Json({ content: text }) as { content: string }).content, (v9Json({ content: text }) as { content: string }).content],
      ];
      if (k % 4 === 0) {
        recordsChecked += 1;
        outs.push(["scrubRecords raw", v8Records(`${text}\n`), v9Records(`${text}\n`)]);
        const line = `${JSON.stringify({ type: "user", message: { content: [{ type: "tool_result", content: text }] } })}\n`;
        outs.push(["scrubRecords record", v8Records(line), v9Records(line)]);
      }
      let differs = false;
      for (const [path, a, b] of outs) {
        if (a !== b) differs = true;
        let reEmitted = mk.all.filter((m) => !a.includes(m) && b.includes(m));
        if (path === "scrubRecords record" && reEmitted.length && v8Fallback(text) && v9Residual(text, mk.all).length === 0) {
          fallbackAllowed += 1;
          if (v9EscapedFlag(text)) escapedFlagged += 1;
          reEmitted = [];
        }
        if (reEmitted.length) leaked += 1;
        if (mk.all.some((m) => a.includes(m) && !b.includes(m))) morRedacted += 1;
        if (a !== b && !shaped) unexpected += 1;
        if (!first && (reEmitted.length || (a !== b && !shaped))) {
          first = `input ${k} (seed ${SEED}) ${path}: reEmitted ${reEmitted.join(",")} shaped ${shaped} ${JSON.stringify(text.slice(0, 300))}`;
        }
      }
      if (differs) differ += 1;
      if (differs && !shaped) plainDiffer += 1;
    }
    console.log(`scrub v8/v9 text fuzz: ${INPUTS} inputs (${recordsChecked} also through scrubRecords), ${plain} without an assignment shape (byte-identical required, ${plainDiffer} differ), ${differ} differ from rule set 8, ${morRedacted} outputs redacting a marker rule set 8 kept, ${fallbackAllowed} record outputs under the whole-string fallback allowance (${escapedFlagged} escaped form still flagged), ${unexpected} unexpected, ${leaked} re-emitting a redacted marker`);
    expect(leaked, first).toBe(0);
    expect(unexpected, first).toBe(0);
    expect(plain).toBeGreaterThan(INPUTS / 20);
    expect(differ).toBeGreaterThan(INPUTS / 10);
    expect(morRedacted).toBeGreaterThan(0);
  }, 300_000);

  it(`never re-emits a marker rule set 8 redacted (${INPUTS} records)`, () => {
    const { rnd, pick } = rng(SEED ^ 0x5a5a);
    let leaked = 0, differ = 0, fallbackAllowed = 0;
    let first = "";
    for (let k = 0; k < INPUTS; k += 1) {
      const mk = markers();
      const placed: string[] = [];
      const strings = () => {
        const s = generate(rnd, pick, mk, rnd(5) === 0);
        placed.push(s);
        return s;
      };
      const node = (depth: number): unknown => {
        const r = rnd(10);
        if (depth > 3 || r < 4) return r === 0 ? rnd(1000) : r === 1 ? null : strings();
        if (r < 6) return Array.from({ length: rnd(4) }, () => node(depth + 1));
        const o: Record<string, unknown> = {};
        for (let i = rnd(4); i >= 0; i -= 1) o[pick([...PLAIN, "content", "text"])] = node(depth + 1);
        return o;
      };
      const input = node(0);
      const line = `${JSON.stringify(input)}\n`;
      const pairs: Array<[string, string]> = [
        [JSON.stringify(v8Json(input)), JSON.stringify(v9Json(input))],
        [v8Records(line), v9Records(line)],
      ];
      // Markers in strings rule set 8 redacted whole on the record path, whose rule set 9
      // stored value keeps nothing rule set 9 would redact (see the header).
      const allowed = new Set(placed.filter((s) => v8Fallback(s) && v9Residual(s, mk.all).length === 0).flatMap((s) => mk.all.filter((m) => s.includes(m))));
      for (const [i, [a, b]] of pairs.entries()) {
        if (a !== b) differ += 1;
        const reEmitted = mk.all.filter((m) => !a.includes(m) && b.includes(m) && !(i === 1 && allowed.has(m)));
        if (i === 1 && mk.all.some((m) => !a.includes(m) && b.includes(m) && allowed.has(m))) fallbackAllowed += 1;
        if (reEmitted.length) {
          leaked += 1;
          if (!first) first = `record ${k} (seed ${SEED}): ${reEmitted.join(",")} ${line.slice(0, 300)}`;
        }
        expect(() => JSON.parse(b), `record ${k}`).not.toThrow();
      }
    }
    console.log(`scrub v8/v9 record fuzz: ${INPUTS} records, ${differ} outputs differ from rule set 8, ${fallbackAllowed} record outputs under the whole-string fallback allowance, ${leaked} re-emitting a redacted marker`);
    expect(leaked, first).toBe(0);
    expect(differ).toBeGreaterThan(0);
  }, 300_000);
});
