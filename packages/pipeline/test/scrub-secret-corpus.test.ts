/**
 * The shared secret-scrubbing corpus (`fixtures/scrub-secret-corpus.json`).
 *
 * The same `lib/scrub.ts` runs in AgentWorkforce/sessions-pipeline (Technical Foul,
 * raw chunks) and AgentWorkforce/relayhistory-cloud (`/v1/delivery` ingest). This file
 * and the corpus are byte-identical in both repos, and `CORPUS_SHA256` pins the corpus,
 * so a case added on one side fails here until it is copied to the other.
 *
 * No secret-shaped value is stored anywhere: every `{{name}}` placeholder is filled with
 * a fresh random value from the corpus generators on each run.
 */
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { MAX_SCRUB_CHARS, scrubJson, scrubText } from "../src/lib/scrub.js";

/** sha256 of the corpus. Update in both repos together when the corpus changes. */
const CORPUS_SHA256 =
  "691955d24883d7f2f67d2e26c235c52e44e9e3262844c88a86f776e6f60a6c43";

const CORPUS_PATH = fileURLToPath(
  new URL("./fixtures/scrub-secret-corpus.json", import.meta.url).href,
);
const corpusText = readFileSync(CORPUS_PATH, "utf8").replace(/\r\n/g, "\n");

type Part = [string, number | string];
interface Generator {
  prefix?: string;
  suffix?: string;
  parts: Part[];
}
interface Case {
  id: string;
  input: string;
  expected: string;
}
interface Corpus {
  generators: Record<string, Generator>;
  secrets: string[];
  cases: Case[];
}
const corpus = JSON.parse(corpusText) as Corpus;

const CHARSETS: Record<string, string> = {
  alnum: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
  upper_alnum: "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
  letters: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
  digits: "0123456789",
  hex: "0123456789abcdef",
  urlsafe: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-",
  b64: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
};

function randomFrom(charset: string, n: number): string {
  // Rejection sampling keeps the distribution uniform; the values are random,
  // so a generated token never repeats between runs.
  let out = "";
  while (out.length < n) {
    for (const b of randomBytes(n * 2)) {
      if (b < 256 - (256 % charset.length)) out += charset[b % charset.length];
      if (out.length === n) break;
    }
  }
  return out;
}

function generate(g: Generator): string {
  let body = "";
  for (const [kind, n] of g.parts) {
    if (kind === "lit") body += String(n);
    else if (kind === "uuid") body += randomUUID();
    else {
      const charset = CHARSETS[kind];
      if (!charset) throw new Error(`unknown charset ${kind}`);
      body += randomFrom(charset, Number(n));
    }
  }
  return `${g.prefix ?? ""}${body}${g.suffix ?? ""}`;
}

/** Fill every placeholder in `input` and `expected` with one fresh value per name. */
function instantiate(c: Case) {
  const values = new Map<string, string>();
  const fill = (s: string) =>
    s.replace(/\{\{([a-z0-9_]+)\}\}/g, (_m, name: string) => {
      const g = corpus.generators[name];
      if (!g) throw new Error(`${c.id}: unknown placeholder ${name}`);
      if (!values.has(name)) values.set(name, generate(g));
      return values.get(name) as string;
    });
  const input = fill(c.input);
  const expected = fill(c.expected);
  const secrets = [...values]
    .filter(([name]) => corpus.secrets.includes(name))
    .map(([, v]) => v);
  return { input, expected, secrets };
}

/** Every 12-character window of a secret; none may survive scrubbing. */
function fragments(secret: string): string[] {
  const out: string[] = [];
  for (const line of secret.split("\n")) {
    if (line.length < 12) continue;
    for (let i = 0; i + 12 <= line.length; i += 4)
      out.push(line.slice(i, i + 12));
  }
  return out;
}

describe("shared secret-scrubbing corpus", () => {
  it("is the pinned corpus (keep it identical in sessions-pipeline and relayhistory-cloud)", () => {
    expect(createHash("sha256").update(corpusText).digest("hex")).toBe(
      CORPUS_SHA256,
    );
  });

  it("stores no secret-shaped value", () => {
    // Inputs hold placeholders, never a provider-shaped token.
    for (const c of corpus.cases) {
      expect(c.input, c.id).not.toMatch(/cf(?:k|ut|at)_[A-Za-z0-9]{40}/);
    }
  });

  // Several rounds, so each case meets several random values.
  for (const c of corpus.cases) {
    it(c.id, () => {
      for (let round = 0; round < 5; round++) {
        const { input, expected, secrets } = instantiate(c);
        const once = scrubText(input);
        expect(once, c.id).toBe(expected);
        for (const secret of secrets)
          for (const f of fragments(secret))
            expect(once.includes(f), `${c.id} leaked ${f.length} chars`).toBe(
              false,
            );
        expect(scrubText(once), `${c.id} is not idempotent`).toBe(once);
      }
    });
  }

  it("scrubs the escaped-JSON tool result through scrubJson, the record path", () => {
    const token = generate(corpus.generators.cf_user_token as Generator);
    const record = {
      type: "user",
      message: {
        content: [
          {
            type: "tool_result",
            content: JSON.stringify({ username: "ops", password: token }),
          },
        ],
      },
    };
    const line = JSON.stringify(record);
    expect(line).toContain('\\"password\\":\\"');
    const scrubbed = JSON.stringify(scrubJson(record));
    expect(scrubbed).not.toContain(token);
    expect(scrubbed).toContain('\\"password\\":\\"[REDACTED]\\"');
    expect(scrubText(line)).not.toContain(token);
  });

  it("stays fast on a large chunk and on adversarial runs", () => {
    const sha = randomFrom(CHARSETS.hex as string, 40);
    const unit =
      `{"type":"assistant","message":{"content":[{"type":"tool_use","input":{"command":"git log ${sha}"}},` +
      `{"type":"text","text":"ok \\"key\\": \\"value\\", \\"name\\": \\"x\\" token usage 123"}]}}\n`;
    const inputs = [
      unit.repeat(Math.floor(MAX_SCRUB_CHARS / unit.length)),
      "\\".repeat(MAX_SCRUB_CHARS),
      '"'.repeat(MAX_SCRUB_CHARS),
      `"token": "${"a".repeat(MAX_SCRUB_CHARS - 20)}`,
      '"password":'.repeat(Math.floor(MAX_SCRUB_CHARS / 11)),
      '\\"token\\": \\"x'.repeat(Math.floor(MAX_SCRUB_CHARS / 14)),
      "token-".repeat(Math.floor(MAX_SCRUB_CHARS / 6)),
      `${"a".repeat(63)}token": "x" `.repeat(Math.floor(MAX_SCRUB_CHARS / 76)),
      `'token': "${`'token': 'x' `.repeat(Math.floor(MAX_SCRUB_CHARS / 14))}`,
      `'token': "${`'token':'x'`.repeat(Math.floor(MAX_SCRUB_CHARS / 11))}`,
    ];
    for (const input of inputs) {
      const start = performance.now();
      scrubText(input);
      const ms = performance.now() - start;
      expect(
        ms,
        `${input.slice(0, 24)}… took ${ms.toFixed(0)} ms`,
      ).toBeLessThan(2_000);
    }
    // The large transcript-shaped chunk keeps its sha and has nothing to redact.
    expect(scrubText(inputs[0] as string)).toBe(inputs[0]);
  }, 60_000);
});
