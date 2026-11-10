import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { MAX_QUOTED_VALUE, MAX_SCRUB_CHARS, scrubJson, scrubText, unsplittableQuotedValueSpans } from "../src/lib/scrub.js";
import { scrubLong, scrubRecords, SCRUB_RULESET_VERSION } from "../src/lib/pipeline/raw-chunks.js";
import { scrubText as v8 } from "./fixtures/scrub-ruleset-8.js";

// Synthetic values only. The packet writes the `\u003d`-style vectors with String.raw;
// Bun's transpiler cooks `\u` escapes inside String.raw, so the literal backslash is
// written as "\\" here. Vectors are [name, fullInput, fullSafe, nestedInput, nestedSafe].
type Vector = [string, string, string, string, string];

const encoder = new TextEncoder(), decoder = new TextDecoder();
const recordsRaw = (value: unknown) => decoder.decode(scrubRecords(encoder.encode(JSON.stringify(value))).scrubbed);
const parsedRecord = (value: unknown) => JSON.parse(recordsRaw(value));
const read = (rel: string) =>
  readFileSync(fileURLToPath(new URL(rel, import.meta.url).href), "utf8").replace(/\r\n/g, "\n");

const vectors: Vector[] = [
  ["q-password", 'idempotency_key=password="SECRET_ALPHA SECRET_BETA"', 'idempotency_key=password="[REDACTED]"', 'password="SECRET_ALPHA SECRET_BETA"', 'password="[REDACTED]"'],
  ["q-api", 'idempotency_key=api_key="SECRET_ALPHA SECRET_BETA"', 'idempotency_key=api_key="[REDACTED]"', 'api_key="SECRET_ALPHA SECRET_BETA"', 'api_key="[REDACTED]"'],
  ["q-auth", 'idempotency_key=auth_token="SECRET_ALPHA SECRET_BETA"', 'idempotency_key=auth_token="[REDACTED]"', 'auth_token="SECRET_ALPHA SECRET_BETA"', 'auth_token="[REDACTED]"'],
  ["newline-client", 'idempotency_key=client_secret="SECRET_ALPHA\nSECRET_BETA"', 'idempotency_key=client_secret="[REDACTED]"', 'client_secret="SECRET_ALPHA\nSECRET_BETA"', 'client_secret="[REDACTED]"'],
  ["u-eq", "idempotency_key=password\\u003dSECRET_ALPHA", "idempotency_key=password\\u003d[REDACTED]", "password\\u003dSECRET_ALPHA", "password\\u003d[REDACTED]"],
  ["x-eq", "idempotency_key=api_key\\x3dSECRET_ALPHA", "idempotency_key=api_key\\x3d[REDACTED]", "api_key\\x3dSECRET_ALPHA", "api_key\\x3d[REDACTED]"],
  ["pct-eq", "idempotency_key=auth_token%3DSECRET_ALPHA", "idempotency_key=auth_token%3D[REDACTED]", "auth_token%3DSECRET_ALPHA", "auth_token%3D[REDACTED]"],
  ["u-colon", "idempotency_key=client_secret\\u003aSECRET_ALPHA", "idempotency_key=client_secret\\u003a[REDACTED]", "client_secret\\u003aSECRET_ALPHA", "client_secret\\u003a[REDACTED]"],
];

// Uppercase hex and percent variants join the same matrix.
const encodedVariants: Vector[] = [
  "\\u003D", "\\u003A", "\\x3D", "%3d", "%3D", "%3a", "%3A",
].map((sep) => [
  `enc-${sep}`,
  `idempotency_key=password${sep}SECRET_ALPHA`, `idempotency_key=password${sep}[REDACTED]`,
  `password${sep}SECRET_ALPHA`, `password${sep}[REDACTED]`,
]);

const positives: Vector[] = [
  ["P1", "idempotency_key=password=fixture-only", "idempotency_key=password=[REDACTED]", "password=fixture-only", "password=[REDACTED]"],
];

// R8 observed outputs from the packet (undefined = R8 left the input unchanged).
const r8Observed: Record<string, string | undefined> = {
  "q-password": 'idempotency_key=password=[REDACTED] SECRET_BETA"',
  "q-api": 'idempotency_key=api_key=[REDACTED] SECRET_BETA"',
  "q-auth": 'idempotency_key=auth_token=[REDACTED] SECRET_BETA"',
  "newline-client": 'idempotency_key=client_secret=[REDACTED]\nSECRET_BETA"',
};

const benign = ["idempotency_key=public-replay-42", 'IDEMPOTENCY_KEY : "public-replay-42"'];
const noSecrets = (out: string) => {
  expect(out).not.toContain("SECRET_ALPHA");
  expect(out).not.toContain("SECRET_BETA");
};

describe("ruleset 9 closes nested credential tails and encoded separators", () => {
  it("freezes v8 and proves each vector leaked there", () => {
    const fixture = read("./fixtures/scrub-ruleset-8.ts");
    expect(createHash("sha256").update(fixture).digest("hex")).toBe("fce75866a2dbf312f1865210fe343e1c372d216c3318dfacc27de6a3f8a82cc7");
    const segmenter = /\/\/ <scrub-segmenter>[\s\S]*?\/\/ <\/scrub-segmenter>/.exec(read("../src/lib/pipeline/raw-chunks.ts"))![0];
    expect(createHash("sha256").update(`${fixture}\n${segmenter}`).digest("hex")).toBe("50c2e0c67395f488a308cbcfc7c945606374b3d67d37138e284849c6190c8214");
    expect(SCRUB_RULESET_VERSION).toBe(9);
    for (const [name, full] of vectors) {
      const out = v8(full);
      expect(out, name).toBe(r8Observed[name] ?? full);
      expect(out.includes("SECRET_ALPHA") || out.includes("SECRET_BETA"), name).toBe(true);
    }
    for (const [name, full] of encodedVariants) expect(v8(full), name).toBe(full);
  });

  for (const [name, full, fullSafe, nested, nestedSafe] of [...vectors, ...encodedVariants, ...positives]) {
    it(`${name}: exact safe output on text, long, JSON and NDJSON paths`, () => {
      const outputs: string[] = [];
      const text = scrubText(full);
      expect(text).toBe(fullSafe);
      expect(scrubLong(full)).toBe(fullSafe);
      expect(scrubJson({ text: full })).toEqual({ text: fullSafe });
      expect(scrubJson({ idempotency_key: nested })).toEqual({ idempotency_key: nestedSafe });
      const fullRaw = recordsRaw({ text: full }), nestedRaw = recordsRaw({ idempotency_key: nested });
      expect(JSON.parse(fullRaw)).toEqual({ text: fullSafe });
      expect(JSON.parse(nestedRaw)).toEqual({ idempotency_key: nestedSafe });
      outputs.push(text, fullRaw, nestedRaw, scrubText(nested));
      for (const out of outputs) noSecrets(out);
      // Second pass is byte-identical.
      expect(scrubText(fullSafe)).toBe(fullSafe);
      expect(scrubText(nestedSafe)).toBe(nestedSafe);
      expect(scrubLong(fullSafe)).toBe(fullSafe);
      expect(recordsRaw(JSON.parse(fullRaw))).toBe(fullRaw);
      expect(recordsRaw(JSON.parse(nestedRaw))).toBe(nestedRaw);
    });

    it(`${name}: escape depths 0-4`, () => {
      let input = full, safe = fullSafe, nIn = nested, nSafe = nestedSafe;
      for (let depth = 0; depth < 5; depth++) {
        expect(scrubText(input), `depth ${depth}`).toBe(safe);
        expect(parsedRecord({ text: input }).text, `depth ${depth}`).toBe(safe);
        expect(scrubJson({ idempotency_key: nIn }), `depth ${depth}`).toEqual({ idempotency_key: nSafe });
        expect(parsedRecord({ idempotency_key: nIn }), `depth ${depth}`).toEqual({ idempotency_key: nSafe });
        input = JSON.stringify(input); safe = JSON.stringify(safe);
        nIn = JSON.stringify(nIn); nSafe = JSON.stringify(nSafe);
      }
    });

    it(`${name}: long-bound tail`, () => {
      const prefix = "ordinary ".repeat(Math.floor(MAX_SCRUB_CHARS / 9));
      expect(scrubLong(prefix + full) === prefix + fullSafe).toBe(true);
      expect(scrubLong(prefix + fullSafe) === prefix + fullSafe).toBe(true);
    });
  }

  it("leaves benign public replay selectors unchanged on every path and depth", () => {
    for (const control of benign) {
      expect(scrubLong(control)).toBe(control);
      expect(scrubJson({ text: control })).toEqual({ text: control });
      expect(scrubJson({ idempotency_key: control })).toEqual({ idempotency_key: control });
      expect(recordsRaw({ text: control })).toBe(JSON.stringify({ text: control }));
      expect(recordsRaw({ idempotency_key: control })).toBe(JSON.stringify({ idempotency_key: control }));
      let input = control;
      for (let depth = 0; depth < 5; depth++) {
        expect(scrubText(input), `${control} depth ${depth}`).toBe(input);
        expect(parsedRecord({ text: input }).text).toBe(input);
        expect(parsedRecord({ idempotency_key: input })).toEqual({ idempotency_key: input });
        input = JSON.stringify(input);
      }
      expect(v8(control)).toBe(control);
    }
  });

  it("keeps the name and separator, and skips past an exempt public selector", () => {
    expect(scrubText('idempotency_key="public-1" password="a b"')).toBe('idempotency_key="public-1" password="[REDACTED]"');
    expect(scrubText("password = 'two words' next=1")).toBe("password ='[REDACTED]' next=1");
    expect(scrubText("token=`x y` ok")).toBe("token=`[REDACTED]` ok");
    // An unclosed quote fails closed: the rest of the input is redacted, never a partial token.
    expect(scrubText('password="unclosed tail')).toBe('password="[REDACTED]');
    expect(v8('password="unclosed tail')).toBe("password=[REDACTED] tail");
    // So does a close past MAX_QUOTED_VALUE (it is not searched for).
    expect(scrubText(`password="${"z".repeat(5000)}" tail`)).toBe('password="[REDACTED]');
    expect(v8(`password="${"z".repeat(5000)}" tail`)).toBe('password=[REDACTED]" tail');
    // The backslash escaping an enclosing quote stays outside the redacted value.
    expect(scrubText('{"a":"\\"password=v\\""}')).toBe('{"a":"\\"password=[REDACTED]\\""}');
    expect(v8('{"a":"\\"password=v\\""}')).toBe('{"a":"\\"password=[REDACTED]""}');
  });

  it("redacts every value of overlapping and chained assignments (union of v8's)", () => {
    // A quoted value whose closing quote opens a later assignment's value (fuzz seed 4).
    const quotedOverlap = "api_key\\u003a'SECRET_ALPHA\\' x\nauth_token = SECRET_BETA apikey: 'SECRET_GAMMA`{ tail";
    // The later opener has no close: it fails closed to the end of the input.
    expect(scrubText(quotedOverlap)).toBe("api_key\\u003a'[REDACTED]'[REDACTED]");
    expect(v8(quotedOverlap)).toContain("SECRET_ALPHA");
    // An unquoted value that takes the next assignment's name: rule set 8 stored `y`.
    expect(scrubText("password=x{password\t=y{password\t=z end")).toBe("password=[REDACTED]\t=[REDACTED]\t=[REDACTED] end");
    expect(v8("password=x{password\t=y{password\t=z end")).toBe("password=[REDACTED]\t=y{password\t=[REDACTED] end");
    // The exemption still holds inside a chain; a prefixed name is not exempt.
    expect(scrubText("password=x{idempotency_key =public-1 ok")).toBe("password=[REDACTED] =public-1 ok");
    expect(scrubText("password=x{x_idempotency_key =zz ok")).toBe("password=[REDACTED] =[REDACTED] ok");
    for (const input of [quotedOverlap, "password=x{password\t=y{password\t=z end"]) {
      expect(scrubText(scrubText(input))).toBe(scrubText(input));
      expect(parsedRecord({ text: input }).text).toBe(scrubText(input));
    }
  });

  it("does not cut a long text inside a multi-word quoted value, nor refuse it", () => {
    // Over 48 whitespace runs: before rule set 9's spans the cut search exhausted its budget
    // inside the value and refused the chunk (fail closed); no cut may land inside it.
    for (const sep of [" ", "\n"]) {
      const value = Array.from({ length: 300 }, (_, i) => `SECRETW${i}`).join(sep);
      for (const off of [-3000, -1500, -40]) {
        const head = ` idempotency_key=password\\u003d"`;
        const pad = "ordinary ".repeat(MAX_SCRUB_CHARS).slice(0, MAX_SCRUB_CHARS + off - head.length);
        const input = `${pad}${head}${value}" after ${"tail ".repeat(2000)}`;
        const safe = `${pad}${head}[REDACTED]" after ${"tail ".repeat(2000)}`;
        expect(scrubLong(input)).toBe(safe);
        expect(parsedRecord({ text: input }).text).toBe(safe);
      }
    }
  });

  it("stays linear on a 64 KiB adversarial input", () => {
    const inputs = [
      'password="'.repeat(6554),
      "password\\u003d".repeat(4682),
      "password%3D\"\\'`".repeat(4096),
      `password=${'\\"'.repeat(32768)}`,
      "a_".repeat(32768),
      `password="${"\\".repeat(65530)}`,
      // Rule set 9 (second round): openers of alternating kinds and depths, none closing
      // another within the bound; long backslash runs before encoded separators and
      // quotes; encoded openers with no close.
      Array.from({ length: 4096 }, (_, i) => `pwd=${"\\".repeat(i % 44)}${["\"", "'", "`"][i % 3]}x `).join(""),
      Array.from({ length: 4096 }, (_, i) => `pwd=${"\\".repeat(1 + (i % 40))}${["u0022", "x27", "U0060"][i % 3]}x `).join(""),
      `password${"\\".repeat(65520)}u003d`,
      `password${"\\".repeat(65520)}`,
      "password\\u003d".repeat(10) + "\\".repeat(65000) + "u0022",
      "token=%22".repeat(7281),
      "token\\u003d\\u0022".repeat(4681),
      "token=\\x22 ".repeat(6553),
      `${"idempotency_key".repeat(2)}${"\\".repeat(65000)}u003d`,
    ];
    for (const input of inputs) {
      const started = performance.now();
      const out = scrubText(input.slice(0, 65536));
      expect(typeof out).toBe("string");
      expect(performance.now() - started, input.slice(0, 16)).toBeLessThan(2000);
    }
  });
});

// Second review round: five fail-open classes in bdce720d, each a quoted or encoded value
// shape the first round fell back to rule set 8's partial token for. Vectors are
// [name, fullInput, fullSafe, nestedInput, nestedSafe, failsClosed]: a value that fails
// closed is redacted from its opener's end to the end of the input.
type ClassVector = [string, string, string, string, string, boolean];
const BS = "\\";
const A4096 = "a".repeat(MAX_QUOTED_VALUE);
const classVectors: ClassVector[] = [
  ["c1-unclosed", 'idempotency_key=password="SECRET_ALPHA SECRET_BETA', 'idempotency_key=password="[REDACTED]', 'password="SECRET_ALPHA SECRET_BETA', 'password="[REDACTED]', true],
  ["c1-unclosed-single", "idempotency_key=api_key='SECRET_ALPHA\nSECRET_BETA", "idempotency_key=api_key='[REDACTED]", "api_key='SECRET_ALPHA\nSECRET_BETA", "api_key='[REDACTED]", true],
  ["c2-over-bound", `idempotency_key=password="${A4096} SECRET_BETA"`, 'idempotency_key=password="[REDACTED]', `password="${A4096} SECRET_BETA"`, 'password="[REDACTED]', true],
  ["c2-over-bound-tail", `idempotency_key=password="${A4096}a" SECRET_BETA`, 'idempotency_key=password="[REDACTED]', `password="${A4096}a" SECRET_BETA`, 'password="[REDACTED]', true],
  ["c3-17-backslashes", `idempotency_key=password${BS.repeat(17)}u003dSECRET_ALPHA`, `idempotency_key=password${BS.repeat(17)}u003d[REDACTED]`, `password${BS.repeat(17)}u003dSECRET_ALPHA`, `password${BS.repeat(17)}u003d[REDACTED]`, false],
  ["c3-100-backslashes-x", `idempotency_key=token${BS.repeat(100)}x3ASECRET_ALPHA`, `idempotency_key=token${BS.repeat(100)}x3A[REDACTED]`, `token${BS.repeat(100)}x3ASECRET_ALPHA`, `token${BS.repeat(100)}x3A[REDACTED]`, false],
  ["c3-17-backslash-quote", `idempotency_key=password=${BS.repeat(17)}"SECRET_ALPHA SECRET_BETA${BS.repeat(17)}"`, `idempotency_key=password=${BS.repeat(17)}"[REDACTED]${BS.repeat(17)}"`, `password=${BS.repeat(17)}"SECRET_ALPHA SECRET_BETA${BS.repeat(17)}"`, `password=${BS.repeat(17)}"[REDACTED]${BS.repeat(17)}"`, false],
  ["c4-percent", "idempotency_key=password%3D%22SECRET_ALPHA SECRET_BETA%22", "idempotency_key=password%3D%22[REDACTED]%22", "password%3D%22SECRET_ALPHA SECRET_BETA%22", "password%3D%22[REDACTED]%22", false],
  ["c4-percent-single", "idempotency_key=password%3a%27SECRET_ALPHA SECRET_BETA%27 x", "idempotency_key=password%3a%27[REDACTED]%27 x", "password%3a%27SECRET_ALPHA SECRET_BETA%27", "password%3a%27[REDACTED]%27", false],
  ["c4-percent-backtick", "idempotency_key=password=%60SECRET_ALPHA SECRET_BETA%60", "idempotency_key=password=%60[REDACTED]%60", "password=%60SECRET_ALPHA SECRET_BETA%60", "password=%60[REDACTED]%60", false],
  ["c4-percent-unclosed", "idempotency_key=password%3D%22SECRET_ALPHA SECRET_BETA", "idempotency_key=password%3D%22[REDACTED]", "password%3D%22SECRET_ALPHA SECRET_BETA", "password%3D%22[REDACTED]", true],
  // Peer-lane vector: a percent-encoded separator with an unquoted value, in either hex case.
  ["c4-percent-sep-unquoted", "idempotency_key=password%3DSECRET_ALPHA", "idempotency_key=password%3D[REDACTED]", "password%3DSECRET_ALPHA", "password%3D[REDACTED]", false],
  ["c4-percent-sep-unquoted-lower", "idempotency_key=password%3dSECRET_ALPHA", "idempotency_key=password%3d[REDACTED]", "password%3dSECRET_ALPHA", "password%3d[REDACTED]", false],
  ["c4-percent-lower", "idempotency_key=password%3d%22SECRET_ALPHA SECRET_BETA%22", "idempotency_key=password%3d%22[REDACTED]%22", "password%3d%22SECRET_ALPHA SECRET_BETA%22", "password%3d%22[REDACTED]%22", false],
  // Coordinator vectors: every percent-separator case, double percent-encoding, and escape
  // letters in upper case (not valid escapes; matched anyway, which only redacts more).
  ["sep-pct3a", `idempotency_key=password%3aSECRET_ALPHA`, `idempotency_key=password%3a[REDACTED]`, `password%3aSECRET_ALPHA`, `password%3a[REDACTED]`, false],
  ["sep-pct3A", `idempotency_key=password%3ASECRET_ALPHA`, `idempotency_key=password%3A[REDACTED]`, `password%3ASECRET_ALPHA`, `password%3A[REDACTED]`, false],
  ["sep-pct253D", `idempotency_key=password%253DSECRET_ALPHA`, `idempotency_key=password%253D[REDACTED]`, `password%253DSECRET_ALPHA`, `password%253D[REDACTED]`, false],
  ["sep-pct253d", `idempotency_key=password%253dSECRET_ALPHA`, `idempotency_key=password%253d[REDACTED]`, `password%253dSECRET_ALPHA`, `password%253d[REDACTED]`, false],
  ["sep-pct253A", `idempotency_key=password%253ASECRET_ALPHA`, `idempotency_key=password%253A[REDACTED]`, `password%253ASECRET_ALPHA`, `password%253A[REDACTED]`, false],
  ["sep-pct253a", `idempotency_key=password%253aSECRET_ALPHA`, `idempotency_key=password%253a[REDACTED]`, `password%253aSECRET_ALPHA`, `password%253a[REDACTED]`, false],
  ["sep-pct253D-quoted", `idempotency_key=password%253D%2522SECRET_ALPHA SECRET_BETA%2522`, `idempotency_key=password%253D%2522[REDACTED]%2522`, `password%253D%2522SECRET_ALPHA SECRET_BETA%2522`, `password%253D%2522[REDACTED]%2522`, false],
  ["sep-pct253D-unclosed", `idempotency_key=password%253D%2522SECRET_ALPHA SECRET_BETA`, `idempotency_key=password%253D%2522[REDACTED]`, `password%253D%2522SECRET_ALPHA SECRET_BETA`, `password%253D%2522[REDACTED]`, true],
  ["sep-pct3D-dq-quote", `idempotency_key=password%3D%2527SECRET_ALPHA SECRET_BETA%2527 x`, `idempotency_key=password%3D%2527[REDACTED]%2527 x`, `password%3D%2527SECRET_ALPHA SECRET_BETA%2527 x`, `password%3D%2527[REDACTED]%2527 x`, false],
  ["sep-U003D", `idempotency_key=password${BS}U003DSECRET_ALPHA`, `idempotency_key=password${BS}U003D[REDACTED]`, `password${BS}U003DSECRET_ALPHA`, `password${BS}U003D[REDACTED]`, false],
  ["sep-X3D", `idempotency_key=password${BS}X3DSECRET_ALPHA`, `idempotency_key=password${BS}X3D[REDACTED]`, `password${BS}X3DSECRET_ALPHA`, `password${BS}X3D[REDACTED]`, false],
  ["sep-U003A-U0022", `idempotency_key=password${BS}U003A${BS}U0022SECRET_ALPHA SECRET_BETA${BS}u0022`, `idempotency_key=password${BS}U003A${BS}U0022[REDACTED]${BS}u0022`, `password${BS}U003A${BS}U0022SECRET_ALPHA SECRET_BETA${BS}u0022`, `password${BS}U003A${BS}U0022[REDACTED]${BS}u0022`, false],
  ["c5-unicode", `idempotency_key=password${BS}u003d${BS}u0022SECRET_ALPHA SECRET_BETA${BS}u0022`, `idempotency_key=password${BS}u003d${BS}u0022[REDACTED]${BS}u0022`, `password${BS}u003d${BS}u0022SECRET_ALPHA SECRET_BETA${BS}u0022`, `password${BS}u003d${BS}u0022[REDACTED]${BS}u0022`, false],
  ["c5-hex", `idempotency_key=password=${BS}x22SECRET_ALPHA SECRET_BETA${BS}x22 x`, `idempotency_key=password=${BS}x22[REDACTED]${BS}x22 x`, `password=${BS}x22SECRET_ALPHA SECRET_BETA${BS}x22`, `password=${BS}x22[REDACTED]${BS}x22`, false],
  ["c5-unicode-single", `idempotency_key=secret: ${BS}u0027SECRET_ALPHA SECRET_BETA${BS}u0027`, `idempotency_key=secret:${BS}u0027[REDACTED]${BS}u0027`, `secret: ${BS}u0027SECRET_ALPHA SECRET_BETA${BS}u0027`, `secret:${BS}u0027[REDACTED]${BS}u0027`, false],
  ["c5-hex-backtick", `idempotency_key=pwd${BS}x3d${BS}x60SECRET_ALPHA SECRET_BETA${BS}x60`, `idempotency_key=pwd${BS}x3d${BS}x60[REDACTED]${BS}x60`, `pwd${BS}x3d${BS}x60SECRET_ALPHA SECRET_BETA${BS}x60`, `pwd${BS}x3d${BS}x60[REDACTED]${BS}x60`, false],
  // A close of a different depth (here one backslash more) is not the matching close.
  ["c5-unicode-unclosed", `idempotency_key=password${BS}u003d${BS}u0022SECRET_ALPHA SECRET_BETA${BS}${BS}u0022 SECRET_BETA`, `idempotency_key=password${BS}u003d${BS}u0022[REDACTED]`, `password${BS}u003d${BS}u0022SECRET_ALPHA SECRET_BETA`, `password${BS}u003d${BS}u0022[REDACTED]`, true],
];

/** `safe` written at escaping depth `depth`: JSON-stringified, or for a value that fails closed, its opener so escaped and then `[REDACTED]` to the end. */
const safeAtDepth = (safe: string, depth: number, failsClosed: boolean) => {
  if (!failsClosed) {
    let s = safe;
    for (let d = 0; d < depth; d++) s = JSON.stringify(s);
    return s;
  }
  let s = `${safe.slice(0, -"[REDACTED]".length)}§`;
  for (let d = 0; d < depth; d++) s = JSON.stringify(s);
  return `${s.slice(0, s.lastIndexOf("§"))}[REDACTED]`;
};

describe("ruleset 9 fails closed on quoted, over-bound, deep-escaped and encoded-quote values", () => {
  it("the R8 fixture leaks every class (frozen negative proof)", () => {
    expect(createHash("sha256").update(read("./fixtures/scrub-ruleset-8.ts")).digest("hex")).toBe("fce75866a2dbf312f1865210fe343e1c372d216c3318dfacc27de6a3f8a82cc7");
    for (const [name, full, , nested] of classVectors) {
      const out = v8(full);
      expect(out.includes("SECRET_ALPHA") || out.includes("SECRET_BETA"), name).toBe(true);
      const nestedOut = v8(nested);
      expect(nestedOut.includes("SECRET_ALPHA") || nestedOut.includes("SECRET_BETA"), name).toBe(true);
    }
    // The packet's exact five inputs and the R8 outputs that leak them.
    expect(v8('idempotency_key=password="SECRET_ALPHA SECRET_BETA')).toBe("idempotency_key=password=[REDACTED] SECRET_BETA");
    expect(v8(`idempotency_key=password="${A4096} SECRET_BETA"`)).toBe('idempotency_key=password=[REDACTED] SECRET_BETA"');
    expect(v8(`idempotency_key=password${BS.repeat(17)}u003dSECRET_ALPHA`)).toBe(`idempotency_key=password${BS.repeat(17)}u003dSECRET_ALPHA`);
    expect(v8("idempotency_key=password%3D%22SECRET_ALPHA SECRET_BETA%22")).toBe("idempotency_key=password%3D%22SECRET_ALPHA SECRET_BETA%22");
    expect(v8(`idempotency_key=password${BS}u003d${BS}u0022SECRET_ALPHA SECRET_BETA${BS}u0022`)).toBe(`idempotency_key=password${BS}u003d${BS}u0022SECRET_ALPHA SECRET_BETA${BS}u0022`);
  });

  for (const [name, full, fullSafe, nested, nestedSafe, failsClosed] of classVectors) {
    it(`${name}: exact safe output on text, long, JSON and NDJSON paths, direct and wrapped`, () => {
      expect(scrubText(full)).toBe(fullSafe);
      expect(scrubLong(full)).toBe(fullSafe);
      expect(scrubJson({ text: full })).toEqual({ text: fullSafe });
      expect(scrubJson({ idempotency_key: nested })).toEqual({ idempotency_key: nestedSafe });
      expect(scrubJson({ text: nested })).toEqual({ text: nestedSafe });
      const fullRaw = recordsRaw({ text: full }), nestedRaw = recordsRaw({ idempotency_key: nested });
      expect(JSON.parse(fullRaw)).toEqual({ text: fullSafe });
      expect(JSON.parse(nestedRaw)).toEqual({ idempotency_key: nestedSafe });
      // The raw (non-record) NDJSON path: one text line, scrubbed as text (a value that
      // fails closed takes the line ending with it: it runs to the end of the run).
      const line = decoder.decode(scrubRecords(encoder.encode(`${full}\n`)).scrubbed);
      expect(line).toBe(failsClosed ? fullSafe : `${fullSafe}\n`);
      for (const out of [scrubText(full), scrubText(nested), fullRaw, nestedRaw, line]) noSecrets(out);
      // Second pass is byte-identical.
      for (const safe of [fullSafe, nestedSafe]) {
        expect(scrubText(safe)).toBe(safe);
        expect(scrubLong(safe)).toBe(safe);
      }
      expect(recordsRaw(JSON.parse(fullRaw))).toBe(fullRaw);
      expect(recordsRaw(JSON.parse(nestedRaw))).toBe(nestedRaw);
      // The segmenter may not cut between the name and the end of the redaction.
      const spans = unsplittableQuotedValueSpans(full);
      const nameStart = full.indexOf("=") + 1;
      if (failsClosed) {
        expect(spans).toContainEqual([nameStart, full.length]);
        expect(unsplittableQuotedValueSpans(full, { prefix: true })).toContainEqual([nameStart, full.length]);
      }
    });

    it(`${name}: escape depths 0-4`, () => {
      let input = full, nIn = nested;
      for (let depth = 0; depth < 5; depth++) {
        const safe = safeAtDepth(fullSafe, depth, failsClosed), nSafe = safeAtDepth(nestedSafe, depth, failsClosed);
        expect(scrubText(input), `depth ${depth}`).toBe(safe);
        expect(parsedRecord({ text: input }).text, `depth ${depth}`).toBe(safe);
        expect(scrubJson({ idempotency_key: nIn }), `depth ${depth}`).toEqual({ idempotency_key: nSafe });
        expect(parsedRecord({ idempotency_key: nIn }), `depth ${depth}`).toEqual({ idempotency_key: nSafe });
        noSecrets(scrubText(input));
        expect(scrubText(safe)).toBe(safe);
        input = JSON.stringify(input);
        nIn = JSON.stringify(nIn);
      }
    });

    it(`${name}: long-bound tail`, () => {
      const prefix = "ordinary ".repeat(Math.floor(MAX_SCRUB_CHARS / 9));
      expect(scrubLong(prefix + full) === prefix + fullSafe).toBe(true);
      expect(scrubLong(prefix + fullSafe) === prefix + fullSafe).toBe(true);
      const records = decoder.decode(scrubRecords(encoder.encode(`${prefix}\n${full}\n`)).scrubbed);
      expect(records === `${prefix}\n${fullSafe}${failsClosed ? "" : "\n"}`).toBe(true);
    });
  }

  it("fails closed whatever the backslash run before a quote, and keeps the opener as written", () => {
    for (const n of [0, 1, 3, 7, 15, 16, 17, 31, 32, 33, 64, 200]) {
      const run = BS.repeat(n);
      for (const q of ['"', "'", "`", "u0022", "x27", "%22"]) {
        if (n === 0 && q.length === 5) continue;
        if (n === 0 && q === "x27") continue;
        const input = `password=${run}${q}SECRET_ALPHA SECRET_BETA`;
        const out = scrubText(input);
        expect(out, `${n} ${q}`).toBe(`password=${run}${q}[REDACTED]`);
        expect(scrubText(out)).toBe(out);
      }
    }
    // An unescaped `u0022` (no backslash) is no quote: the value is the token.
    expect(scrubText("password=u0022SECRET_ALPHA SECRET_BETA")).toBe("password=[REDACTED] SECRET_BETA");
    // Every separator encoding accepts any run length; the public selector stays exempt.
    for (const n of [1, 16, 17, 40]) {
      expect(scrubText(`token${BS.repeat(n)}u003dSECRET_ALPHA`)).toBe(`token${BS.repeat(n)}u003d[REDACTED]`);
      expect(scrubText(`idempotency_key${BS.repeat(n)}u003dpublic-replay-42`)).toBe(`idempotency_key${BS.repeat(n)}u003dpublic-replay-42`);
    }
  });

  it("does not let a segment cut leave the tail of a value that fails closed keyless", () => {
    // The opener sits just before the long bound and its value runs past it with no close.
    for (const off of [-3000, -1500, -40]) {
      const head = ` idempotency_key=password%3D%22`;
      const pad = "ordinary ".repeat(MAX_SCRUB_CHARS).slice(0, MAX_SCRUB_CHARS + off - head.length);
      const value = Array.from({ length: 300 }, (_, i) => `SECRET_W${i}`).join(" ");
      const input = `${pad}${head}${value} after ${"tail ".repeat(200)}`;
      const safe = `${pad}${head}[REDACTED]`;
      expect(scrubLong(input) === safe, `${off}`).toBe(true);
      expect(parsedRecord({ text: input }).text === safe, `${off}`).toBe(true);
    }
  });
});
