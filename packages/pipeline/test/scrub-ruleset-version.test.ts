import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { SCRUB_RULESET_VERSION } from "../src/lib/pipeline/raw-chunks.js";

/**
 * Every `lib/scrub.ts` that has sealed raw chunks, by `raw.scrub.version`.
 * Stored chunks record the version they were scrubbed with and are never
 * rewritten, so a rule change must come with a new version. When this test
 * fails after editing `lib/scrub.ts`: bump `SCRUB_RULESET_VERSION` in
 * `lib/pipeline/raw-chunks.ts` and add the new version and digest here. Never
 * edit an existing entry.
 */
const SCRUB_RULESETS: Record<number, string> = {
  1: "db9d65c7ab230d67488900203bb78ca8bd3ad16949a0a51b5d3d117d0cff49f8",
  // From version 2 the digest also covers the segmenter in raw-chunks.ts
  // (between the `<scrub-segmenter>` markers): texts past MAX_SCRUB_CHARS are
  // scrubbed whole, in segments, where version 1 refused them.
  // Updated before release (K-1, K-2: key-block segmentation fixes). Version 2 had
  // sealed no chunk: it was only ever on the unmerged #108 branch, and images build from main.
  2: "417d545e6b79b1100e206e7a6add66f24819868b2606e9ca2c45c09be9644ee2",
  // Quoted secret keys at any JSON escaping depth, Cloudflare scannable tokens and
  // more provider prefixes, non-Bearer Authorization, and the entropy fallback next
  // to a secret name (2026-11-02 Cloudflare-token-in-transcript finding).
  // Updated before release (#162 adversarial review: the segmenter's CUT_CONTEXT_CHARS
  // widened from 4096 to 8192, past the quoted rule's reach). Version 3 has sealed no
  // chunk: it is only on the unmerged #162 branch.
  // Then the entropy fallback stopped firing after a `/`, `.` or `-` (paths, members,
  // branch names), and an unquoted value after a quoted key is taken only after a
  // one-word key and only when it looks generated (code and quoted prose are kept).
  // Then the literal-prefix rules stopped starting after a `-` inside a longer token
  // (they cut SendGrid and other full-shape keys and leaked their start; a glued
  // `word-sk-…` is taken later, whole), and the SendGrid rule moved ahead of them.
  // Then the glued-prefix pass tries the relay_ rule first (its body may hold a `.`), so
  // `build-relay_…-sk-….tail` is no longer split at the `.` by the `sk-` rule.
  3: "e7607073a3afca3db6b015945f0b5fab256202f355b740f515987ec162bf3076",
  // A quoted secret value past the quoted rule's 4096-unit scan bound is redacted whole
  // (to its matching closing quote or the end of input; no line break ends it), not cut at
  // the bound with the rest stored; a generated unquoted value after a quoted key likewise.
  // The segmenter never cuts between such a value's key and its end (longCut, safeCut, and
  // a run open in one keeps its next record or key line). Values within the bound scrub
  // byte-identically to version 3.
  // Updated before release (N1 delta review: longCut finds those values past the segment
  // bound too, so a key just below the bound is protected; the unquoted over-bound value
  // gets a span. N2: the line-confined narrowing is withdrawn, over-bound values run across
  // line breaks. N3: the bound caps the value's total length to its matching close across
  // lines, not its key line's, and a run or cut window ending inside an undecided secret
  // value fails closed). Version 4 has sealed no chunk: it is only on this unmerged branch.
  4: "429200e870dea6b5602f6ffa6b7a577cf2016b27aa9d99e6a3e61b55e5ec612c",
  // An over-cap quoted secret value is classified and redacted on the original text, by a
  // pre-pass ahead of every other rule (version 4 measured it after the token rules had
  // shrunk it under the bound and stored its later lines). The record path keeps key
  // context: a non-empty string under a secret key is redacted at any depth, through
  // arrays, in scrubJson/scrubRecord and the record-line tokenizer. From version 5 the
  // segmenter markers also enclose scrubRecordLine, so the digest covers the record path.
  // Output with no secret-keyed record value, no over-cap quoted value and no quoted
  // secret value left unclosed or open across a line break is byte-identical to version 4.
  // Updated before release (repeated source keys: scrubRecordLine writes a key the source
  // repeats under its first written name, never another key's). Version 5 has sealed no chunk.
  // Updated before release (PASS gate blockers 1 and 2: multi-line or unclosed quoted
  // secret values within the bound are redacted whole and never cut inside; secret context
  // runs through composite values, objects as well as arrays).
  // Updated before release (exact-head review of ff5fc268: a value starting `op://` is kept
  // only when the whole value is a strict one-line reference, OP_REFERENCE; one holding an
  // escaped line separator, a backslash, a quote, a tab or a double space is redacted whole).
  // Updated before release (exact-head review of 0f9b2416: an op:// reference has exactly three
  // or four segments; two, or five or more, are redacted whole).
  5: "2e03990c86a790f89e0cb11dc50fe2ea016855b20d50838aef0e29d7799b2424",
};

const read = (rel: string) =>
  // Line endings depend on the checkout, not the rules.
  readFileSync(fileURLToPath(new URL(rel, import.meta.url).href), "utf8").replace(/\r\n/g, "\n");

/** The segmenter's source: everything between the markers in raw-chunks.ts. */
function segmenterSource(): string {
  const text = read("../src/lib/pipeline/raw-chunks.ts");
  const m = /\/\/ <scrub-segmenter>[\s\S]*?\/\/ <\/scrub-segmenter>/.exec(text);
  if (!m) throw new Error("raw-chunks.ts lost its <scrub-segmenter> markers");
  return m[0];
}

function scrubSourceDigest(version: number) {
  const scrub = read("../src/lib/scrub.ts");
  // Version 1 predates the segmenter: its digest is lib/scrub.ts alone.
  const source = version >= 2 ? `${scrub}\n${segmenterSource()}` : scrub;
  return createHash("sha256").update(source).digest("hex");
}

describe("scrub ruleset version", () => {
  it("is bumped with every change to lib/scrub.ts", () => {
    const latest = Math.max(...Object.keys(SCRUB_RULESETS).map(Number));
    expect(
      SCRUB_RULESET_VERSION,
      "SCRUB_RULESET_VERSION must be the newest entry in SCRUB_RULESETS",
    ).toBe(latest);
    expect(
      scrubSourceDigest(SCRUB_RULESET_VERSION),
      "lib/scrub.ts or the raw-chunks segmenter changed: bump SCRUB_RULESET_VERSION and add its digest to SCRUB_RULESETS",
    ).toBe(SCRUB_RULESETS[SCRUB_RULESET_VERSION]);
  });
});
