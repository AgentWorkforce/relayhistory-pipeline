/**
 * Rule set 6, defect 4 (found in review of #356): a private-key header inside one QUOTED
 * secret value and its footer inside a later UNQUOTED credential (`token=…`,
 * `Authorization: Bearer …`, `?access_token=…`).
 *
 * The first (over-cap) pass redacts such a value through the later footer, so the label
 * of the later credential is taken with it. Before this fix it then put the first value's
 * closing quote back whenever no quote lay between the value and the footer, so the rest
 * of the later credential (`SECRETTAIL`, glued to the footer) was kept with no label for
 * the assignment, Bearer or OAuth rule to find:
 *
 *   `"password": "abc-----BEGIN PRIVATE KEY-----" KEEPMID token=abc-----END PRIVATE KEY-----SECRETTAIL KEEPTRAIL`
 *   rule set 5:          `"password": "[REDACTED]`             (to the end of the input)
 *   pre-fix rule set 6:  `"password": "[REDACTED]"SECRETTAIL KEEPTRAIL`   (leak)
 *   rule set 6:          `"password": "[REDACTED]" KEEPTRAIL`
 *
 * The redaction now also takes the run of non-whitespace glued to the footer (to the end
 * of the input when that run holds a quote). The pre-fix source is frozen in
 * `fixtures/scrub-ruleset-6-prefix-f6a97053.ts`, byte for byte
 * `git show f6a97053:packages/pipeline/src/lib/scrub.ts` (it has no imports), pinned by
 * its sha256 below, only to prove the leak; it is no shipped rule set and is pinned
 * nowhere else. Every value here is synthetic.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { scrubJson as prefixJson, scrubText as prefixText } from "./fixtures/scrub-ruleset-6-prefix-f6a97053.js";
import { scrubJson as v5Json, scrubText as v5Text } from "./fixtures/scrub-ruleset-5.js";
import { MAX_SCRUB_CHARS, scrubJson, scrubRecord, scrubText } from "../src/lib/scrub.js";
import { scrubLong, scrubRecords } from "../src/lib/pipeline/raw-chunks.js";

/** sha256 of fixtures/scrub-ruleset-6-prefix-f6a97053.ts (LF line endings). */
const PREFIX_FIXTURE_SHA256 = "737d6d4678ebf94f74a99f1b85d9e1536a5c0ffacb752d2eff3ccd8bdf1392ff";

const R = "[REDACTED]";
const enc = new TextEncoder();
const dec = new TextDecoder();
type Records = (input: Uint8Array) => { scrubbed: Uint8Array };
const run = (fn: Records, text: string) => dec.decode(fn(enc.encode(text)).scrubbed);
let prefixRecords: Records;

beforeAll(async () => {
  vi.resetModules();
  vi.doMock("../src/lib/scrub.js", () => import("./fixtures/scrub-ruleset-6-prefix-f6a97053.js"));
  prefixRecords = (await import("../src/lib/pipeline/raw-chunks.js")).scrubRecords as Records;
  vi.doUnmock("../src/lib/scrub.js");
  vi.resetModules();
});

const h = "-----BEGIN PRIVATE KEY-----";
const f = "-----END PRIVATE KEY-----";
const CONTEXTS = {
  assignment: "token=",
  Bearer: "Authorization: Bearer ",
  query: "https://example.test/cb?access_token=",
} as const;
const repro = (label: string) => `"password": "abc${h}" KEEPMID ${label}abc${f}SECRETTAIL KEEPTRAIL`;
const recordLine = (content: string) =>
  `${JSON.stringify({ type: "user", message: { content: [{ type: "tool_result", content }] } })}\n`;

/** scrubText, scrubJson (under a plain key), scrubRecord and scrubRecords (raw line and record line). */
function paths(
  text: string,
  t: (s: string) => string,
  j: (v: unknown) => unknown,
  records: (s: string) => string,
  record?: (v: Record<string, unknown>) => unknown,
): Record<string, string> {
  const out: Record<string, string> = {
    scrubText: t(text),
    scrubJson: (j({ content: text }) as { content: string }).content,
    "scrubRecords raw": records(`${text}\n`),
    "scrubRecords record": records(recordLine(text)),
  };
  if (record) out.scrubRecord = JSON.stringify(record({ trace: { content: text } }));
  return out;
}

describe("rule set 6: a private-key footer inside a later unquoted credential (defect 4)", () => {
  it("runs the pre-fix proof against the frozen f6a97053 source", () => {
    const fixture = readFileSync(
      fileURLToPath(new URL("./fixtures/scrub-ruleset-6-prefix-f6a97053.ts", import.meta.url).href),
      "utf8",
    ).replace(/\r\n/g, "\n");
    expect(createHash("sha256").update(fixture).digest("hex")).toBe(PREFIX_FIXTURE_SHA256);
  });

  for (const [name, label] of Object.entries(CONTEXTS)) {
    it(`${name}: the pre-fix source leaked SECRETTAIL, rule set 5 did not, rule set 6 does not`, () => {
      const text = repro(label);
      const old = paths(text, prefixText, prefixJson, (s) => run(prefixRecords, s));
      for (const [path, out] of Object.entries(old)) {
        expect(out.includes("SECRETTAIL"), `pre-fix ${path} leaks SECRETTAIL`).toBe(true);
      }
      expect(prefixText(text)).toBe(`"password": "${R}"SECRETTAIL KEEPTRAIL`);
      // Rule set 5 redacted to the end of the input.
      expect(v5Text(text)).toBe(`"password": "${R}`);
      expect((v5Json({ content: text }) as { content: string }).content.includes("SECRETTAIL")).toBe(false);

      const now = paths(text, scrubText, scrubJson, (s) => run(scrubRecords, s), scrubRecord);
      for (const [path, out] of Object.entries(now)) {
        expect(out.includes("SECRETTAIL"), `${path} keeps no SECRETTAIL`).toBe(false);
        expect(/-----(?:BEGIN|END) [A-Z ]*PRIVATE KEY/.test(out), `${path} keeps no boundary`).toBe(false);
        expect(out.includes("KEEPTRAIL"), `${path} keeps KEEPTRAIL`).toBe(true);
        // KEEPMID lies between the header and its footer: inside the private key, redacted.
        expect(out.includes("KEEPMID"), `${path}`).toBe(false);
      }
      expect(now.scrubText).toBe(`"password": "${R}" KEEPTRAIL`);
      expect(now.scrubJson).toBe(`"password": "${R}" KEEPTRAIL`);
      expect(now["scrubRecords raw"]).toBe(`"password": "${R}" KEEPTRAIL\n`);
      expect(now["scrubRecords record"]).toBe(recordLine(`"password": "${R}" KEEPTRAIL`));
      expect(scrubText(now.scrubText)).toBe(now.scrubText);
      // The segmented path: past the scrub bound, the block and the glued rest stay together.
      const PAD = "lorem ipsum dolor sit amet\n".repeat(Math.ceil(MAX_SCRUB_CHARS / 27) + 10);
      expect(scrubLong(text)).toBe(`"password": "${R}" KEEPTRAIL`);
      expect(scrubLong(PAD + text) === `${PAD}"password": "${R}" KEEPTRAIL`).toBe(true);
    });

    it(`${name}: a long generated tail, a second line and an escaped (JSON-in-JSON) form`, () => {
      const tail = "Q7xK2mP9vR4tW8nB3cY6".repeat(4);
      const text = `before KEEPMID\n"api_key": 'x' "password": "abc${h}\nMIIBODY" ${label}abc${f}SECRETTAIL${tail} KEEPTRAIL\nnext KEEPTRAIL2`;
      for (const [path, out] of Object.entries(paths(text, scrubText, scrubJson, (s) => run(scrubRecords, s), scrubRecord))) {
        expect(out.includes("SECRETTAIL") || out.includes(tail.slice(0, 12)), path).toBe(false);
        expect(out.includes("KEEPTRAIL") && out.includes("KEEPTRAIL2"), path).toBe(true);
      }
      const escaped = JSON.stringify(text).slice(1, -1);
      const out = scrubText(escaped);
      expect(out.includes("SECRETTAIL") || out.includes(tail.slice(0, 12))).toBe(false);
    });

    it(`${name}: a quote glued after the footer: redacted to the end of the input (as rule set 5)`, () => {
      const text = `"password": "abc${h}" KEEPMID ${label}abc${f}SEC"RETTAIL KEEPTRAIL`;
      expect(scrubText(text)).toBe(`"password": "${R}"`);
      // Rule set 5 kept the rest after the inner quote.
      expect(v5Text(text).includes("RETTAIL")).toBe(true);
    });
  }

  it("a later quoted (JSON string) footer and a footer followed by whitespace are unchanged", () => {
    const quoted = `"password": "abc${h}" KEEPMID "token": "abc${f}SECRETTAIL" KEEPTRAIL`;
    expect(scrubText(quoted)).toBe(`"password": "${R}" KEEPTRAIL`);
    expect(prefixText(quoted)).toBe(`"password": "${R}" KEEPTRAIL`);
    const spaced = `"password": "abc${h}" KEEPMID ${f}\nKEEPTRAIL`;
    expect(scrubText(spaced)).toBe(`"password": "${R}"\nKEEPTRAIL`);
    expect(prefixText(spaced)).toBe(scrubText(spaced));
  });

  it("a bare token glued to the footer is redacted with it", () => {
    const text = `"password": "abc${h}" KEEPMID abc${f}SECRETTAIL KEEPTRAIL`;
    expect(prefixText(text)).toBe(`"password": "${R}"SECRETTAIL KEEPTRAIL`);
    expect(scrubText(text)).toBe(`"password": "${R}" KEEPTRAIL`);
  });

  it("a private-key header glued to the footer is left to the private-key rule", () => {
    for (const next of [`${h}MIIBODY${f}`, `-----BEGIN RSA PRIVATE KEY-----\nMIIBODY\n-----END RSA PRIVATE KEY-----`]) {
      const text = `"password": "abc${h}" KEEPMID ${f}${next} KEEPTRAIL`;
      const out = scrubText(text);
      expect(out).toBe(`"password": "${R}"${R} KEEPTRAIL`);
    }
  });

  it("with a header before the value too, the later credential's rest is redacted", () => {
    const H = "-----BEGIN RSA PRIVATE KEY-----";
    const F = "-----END RSA PRIVATE KEY-----";
    for (const label of Object.values(CONTEXTS)) {
      const text = `${H} x "password": "y${F} z${h}" mid ${label}abc${f}SECRETTAIL KEEPTRAIL`;
      expect(v5Text(text).includes("SECRETTAIL")).toBe(true);
      expect(scrubText(text)).toBe(`${R} KEEPTRAIL`);
    }
  });
});
