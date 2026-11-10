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
 * set 8's `scrubRecords` is the current raw-chunks.ts loaded with `lib/scrub.ts` mocked to
 * that fixture (`support/frozen-scrub-module.ts`). Rule set 9 changed the segmenter in one
 * place only: a string under the public replay selector is scrubbed as a container. The
 * mock's `isPublicSelectorKey` is always false, so against the fixture the segmenter takes
 * rule set 8's path everywhere.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { scrubJson as v8Json, scrubText as v8 } from "./fixtures/scrub-ruleset-8.js";
import { MAX_QUOTED_VALUE, scrubJson as v9Json, scrubText as v9 } from "../src/lib/scrub.js";
import { frozenScrubModule } from "./support/frozen-scrub-module.js";

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
  vi.doMock("../src/lib/scrub.js", async () => frozenScrubModule(await import("./fixtures/scrub-ruleset-8.js")));
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

/*
 * Records structured path (rule set 9 changed the segmenter here): a string under the
 * public replay selector, directly or below it, is a container on `scrubRecords` too. The
 * rule set 8 harness above (raw-chunks.ts over the frozen source) stores the tail; rule set
 * 9 never re-emits a token rule set 8 redacted, and drops every token after a nested
 * credential inside such a container. NDJSON lines are scrubbed in batches of 500.
 * SCRUB_RECORDS_FUZZ_INPUTS changes the count (default 200,000 records).
 */
const RECORD_INPUTS = Number(process.env.SCRUB_RECORDS_FUZZ_INPUTS ?? 200_000);

describe("rule set 8 vs 9 on the records path under a public selector", () => {
  it("rule set 8's scrubRecords stores the tail; rule set 9's does not", () => {
    const B = "password=SECRET_ALPHA SECRET_BETA";
    for (const value of [
      { idempotency_key: B },
      { before: "k", idempotency_key: B, after: "k" },
      { idempotency_key: { a: B } },
      { idempotency_key: [B, "x"] },
      { task: { idempotency_key: B } },
      { a: { b: { c: { idempotency_key: B } } } },
      { IDEMPOTENCY_KEY: B },
      { idempotency_key: "password=[REDACTED] SECRET_BETA" },
    ]) {
      const line = JSON.stringify(value);
      expect(v8Records(`${line}\n`), line).toContain("SECRET_BETA");
      const out = v9Records(`${line}\n`);
      expect(out, line).not.toContain("SECRET_BETA");
      expect(JSON.parse(out), line).toEqual(v9Json(value));
    }
    for (const dup of ['{"idempotency_key":"ok","idempotency_key":"password=A SECRET_BETA"}', '{"idempotency_key":"password=A SECRET_BETA","idempotency_key":"ok"}']) {
      expect(v8Records(`${dup}\n`), dup).toContain("SECRET_BETA");
      expect(v9Records(`${dup}\n`), dup).not.toContain("SECRET_BETA");
    }
    // Siblings and controls: byte-identical to rule set 8.
    for (const line of ['{"idempotency_key":"public-replay-42","note":"password=A B"}', '{"IDEMPOTENCY_KEY":"public-replay-42"}', '{"idempotency_key":"[REDACTED]"}']) {
      expect(v9Records(`${line}\n`), line).toBe(v8Records(`${line}\n`));
    }
  });

  it(`never re-emits a token rule set 8 redacted, and drops tails in public containers (${RECORD_INPUTS} NDJSON records)`, () => {
    // mulberry32: the shared LCG above loses precision in doubles and cycles after about
    // 10k draws from this seed, which would repeat the same record shapes every ~100 lines.
    let state = (SEED ^ 0x9e9e) >>> 0;
    const rnd = (n: number) => {
      state = (state + 0x6d2b79f5) >>> 0;
      let t = state;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return Math.floor((((t ^ (t >>> 14)) >>> 0) / 0x100000000) * n);
    };
    const pick = <T,>(a: readonly T[]) => a[rnd(a.length)] as T;
    const PUBLIC_KEYS = ["idempotency_key", "IDEMPOTENCY_KEY", "Idempotency_Key", " idempotency_key "];
    const OTHER_KEYS = ["note", "content", "text", "idempotency_key_2", "name"];
    const SECRET_KEYS = ["password", "token", "api_key"];
    const CRED = ["password", "api_key", "auth_token", "client_secret", "DB_PASSWORD", "token", "pwd"];
    const CSEPS = ["=", ":", " = ", ": ", "\t=", `${B}u003d`, `${B}x3A`, `${B}${B}u003D`, "%3D", "%3a", "%253D", "%25253a"];
    // No quote forms: a tail after a credential in a value built only from these is inside
    // no quoted string of its own, so it runs to the container's end.
    const SAFE = [" ", "  ", "\n", "\r\n", "\t", ",", ";", "x=1", "%20", `${B}n`, "a b", "{", "}", "[", "]", "word", "Ab9"];
    const WILD = [...SAFE, '"', "'", "`", `${B}"`, `${B}${B}${B}"`, "%22", "%2522", `${B}u0022`, B, "=", ":", "[REDACTED]"];
    let n = 0;
    const tok = (kind: string) => `SECRET_${kind}${n++}Z`;
    const fill = (atoms: readonly string[], max: number) => {
      let s = "";
      for (let i = rnd(max); i > 0; i -= 1) s += rnd(4) === 0 ? ` ${tok("M")} ` : pick(atoms);
      return s;
    };
    let tails: string[] = [];
    /** A string value; under a public selector (not secret) its tail tokens must go. */
    const str = (isPublic: boolean): string => {
      const r = rnd(10);
      if (r < 5) {
        const q = pick(["", "", "", '"', "'"]);
        const beta = tok("BETA");
        const s = fill(SAFE, 4) + pick([" ", "\n", "\t", ",", ";", "{", "[", `${B}n`]) + pick(CRED) + pick(CSEPS) + q + tok("ALPHA") + fill(SAFE, 5) + beta + (q && rnd(2) ? q : "") + (q ? "" : fill(SAFE, 3));
        if (isPublic) tails.push(beta);
        return s;
      }
      if (r < 9) return fill(WILD, 6) + (rnd(2) ? pick(CRED) + pick(CSEPS) + pick(["", '"', "%22", `${B}"`]) + tok("ALPHA") : "") + fill(WILD, 6);
      return pick(["public-replay-42", "[REDACTED]", "password=[REDACTED]", `password=[REDACTED] ${tok("BETA")}`]);
    };
    // JSON text is written directly (not built as objects), so duplicate keys survive.
    const node = (depth: number, isPublic: boolean, isSecret: boolean): string => {
      const r = rnd(10);
      if (depth > 3 || r < 5) return r === 0 ? String(rnd(1000)) : r === 1 ? "null" : JSON.stringify(str(isPublic && !isSecret));
      if (r < 7) return `[${Array.from({ length: 1 + rnd(3) }, () => node(depth + 1, isPublic, isSecret)).join(",")}]`;
      return obj(depth + 1, isPublic, isSecret);
    };
    const obj = (depth: number, isPublic: boolean, isSecret: boolean): string => {
      const parts: string[] = [];
      for (let i = 1 + rnd(3); i > 0; i -= 1) {
        const k = rnd(10);
        const key = k < 5 ? pick(PUBLIC_KEYS) : k < 8 ? pick(OTHER_KEYS) : k < 9 ? "task" : pick(SECRET_KEYS);
        const pub = isPublic || /^idempotency_key$/i.test(key.trim());
        const sec = isSecret || SECRET_KEYS.includes(key);
        parts.push(`${JSON.stringify(key)}:${node(depth, pub, sec)}`);
      }
      return `{${parts.join(",")}}`;
    };
    /** String values inside a public selector container and outside a secret one, in order (independent oracle). */
    const publicValues = (line: string): string[] => {
      const out: string[] = [];
      const stack: Array<{ obj: boolean; inh: boolean; sec: boolean; pub: boolean; secKey: boolean; expectKey: boolean }> = [];
      for (const [tok] of line.matchAll(/"(?:[^"\\]|\\.)*"|[{}[\]:,]|[^{}[\]:,"\s]+|\s+/g)) {
        const top = stack[stack.length - 1];
        if (tok === "{" || tok === "[") {
          const inh = top ? top.pub : false, sec = top ? top.secKey : false;
          stack.push({ obj: tok === "{", inh, sec, pub: inh, secKey: sec, expectKey: tok === "{" });
        } else if (tok === "}" || tok === "]") stack.pop();
        else if (tok === ":") top!.expectKey = false;
        else if (tok === ",") top!.expectKey = top!.obj;
        else if (tok.startsWith('"')) {
          const value = JSON.parse(tok) as string;
          if (top?.obj && top.expectKey) {
            top.pub = top.inh || /^idempotency_key$/i.test(value.trim());
            top.secKey = top.sec || SECRET_KEYS.includes(value);
          } else if (top?.pub && !top.secKey) out.push(value);
        }
      }
      return out;
    };
    expect(publicValues('{"a":{" Idempotency_Key ":["x",{"b":"y","password":"s"},3],"c":"z"},"idempotency_key":"w","n":"v","idempotency_key_2":"u"}')).toEqual(["x", "y", "w"]);
    let leaked = 0, tailLeaks = 0, differ = 0, unstable = 0, unstableElsewhere = 0, tailsChecked = 0, publicChecked = 0;
    let first = "";
    const started = Date.now();
    for (let done = 0; done < RECORD_INPUTS; ) {
      const lines: string[] = [];
      const lineTails: string[][] = [];
      for (let b = 0; b < 500 && done < RECORD_INPUTS; b += 1, done += 1) {
        tails = [];
        lines.push(obj(0, false, false));
        lineTails.push(tails);
      }
      const batch = lines;
      const a = v8Records(`${batch.join("\n")}\n`).split("\n");
      const b9 = v9Records(`${batch.join("\n")}\n`).split("\n");
      for (let i = 0; i < batch.length; i += 1) {
        const input = batch[i]!, x = a[i]!, y = b9[i]!;
        if (x !== y) differ += 1;
        const tokens = input.match(/SECRET_[A-Z]+\d+Z/g) ?? [];
        const re = tokens.filter((t) => !x.includes(t) && y.includes(t));
        if (re.length) {
          leaked += 1;
          if (!first) first = `re-emitted ${re.join(",")} in ${input.slice(0, 400)} -> ${y.slice(0, 400)}`;
        }
        for (const t of lineTails[i]!) {
          tailsChecked += 1;
          if (y.includes(t)) {
            tailLeaks += 1;
            if (!first) first = `tail ${t} kept in ${input.slice(0, 400)} -> ${y.slice(0, 400)}`;
          }
        }
        expect(() => JSON.parse(y)).not.toThrow();
      }
      if (done % 10_000 === 0) {
        // Second pass: every value inside a public container is byte-identical. Lines whose
        // only change is in a non-public string (pre-existing rule 8 behaviour on encoded
        // separators before a marker, identical at a63ccfac) are counted, not failed.
        const again = v9Records(`${b9.join("\n")}`).split("\n");
        for (let i = 0; i < batch.length; i += 1) {
          const before = publicValues(b9[i]!), after = publicValues(again[i]!);
          publicChecked += before.length;
          if (again[i] === b9[i]) continue;
          if (JSON.stringify(before) === JSON.stringify(after)) {
            unstableElsewhere += 1;
            continue;
          }
          unstable += 1;
          if (!first) first = `unstable ${b9[i]!.slice(0, 400)} -> ${again[i]!.slice(0, 400)}`;
        }
      }
    }
    console.log(`scrub v8/v9 records public-selector fuzz: ${RECORD_INPUTS} records in ${Date.now() - started} ms, ${differ} differ from rule set 8, ${tailsChecked} tails checked (${tailLeaks} kept), ${leaked} re-emitting a token rule set 8 redacted, ${unstable} of ${publicChecked} public values unstable on a second pass (${unstableElsewhere} lines changed only outside public containers)`);
    expect(leaked, first).toBe(0);
    expect(tailLeaks, first).toBe(0);
    expect(unstable, first).toBe(0);
    expect(publicChecked).toBeGreaterThan(RECORD_INPUTS / 20);
    expect(tailsChecked).toBeGreaterThan(RECORD_INPUTS / 2);
    expect(differ).toBeGreaterThan(RECORD_INPUTS / 20);
  }, 600_000);
});
