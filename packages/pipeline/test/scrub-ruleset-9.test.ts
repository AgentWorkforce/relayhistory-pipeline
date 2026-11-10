import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { MAX_SCRUB_CHARS, scrubJson, scrubText } from "../src/lib/scrub.js";
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
    // An unclosed quote within the bound falls back to the unquoted value rule.
    expect(scrubText('password="unclosed tail')).toBe("password=[REDACTED] tail");
    // A close past MAX_QUOTED_VALUE is not searched for: rule set 8's value applies.
    expect(scrubText(`password="${"z".repeat(5000)}"`)).toBe('password=[REDACTED]"');
    expect(scrubText(`password="${"z".repeat(5000)}"`)).toBe(v8(`password="${"z".repeat(5000)}"`));
    // The backslash escaping an enclosing quote stays outside the redacted value.
    expect(scrubText('{"a":"\\"password=v\\""}')).toBe('{"a":"\\"password=[REDACTED]\\""}');
    expect(v8('{"a":"\\"password=v\\""}')).toBe('{"a":"\\"password=[REDACTED]""}');
  });

  it("redacts every value of overlapping and chained assignments (union of v8's)", () => {
    // A quoted value whose closing quote opens a later assignment's value (fuzz seed 4).
    const quotedOverlap = "api_key\\u003a'SECRET_ALPHA\\' x\nauth_token = SECRET_BETA apikey: 'SECRET_GAMMA`{ tail";
    expect(scrubText(quotedOverlap)).toBe("api_key\\u003a'[REDACTED]`{ tail");
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
    ];
    for (const input of inputs) {
      const started = performance.now();
      const out = scrubText(input.slice(0, 65536));
      expect(typeof out).toBe("string");
      expect(performance.now() - started, input.slice(0, 16)).toBeLessThan(2000);
    }
  });
});
