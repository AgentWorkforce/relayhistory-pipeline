# Public idempotency selectors in tool input

Ruleset 7 preserves the exact case-insensitive `idempotency_key` identifier in unquoted assignments. This field selects a replay receipt and is not an authentication credential. The broader assignment rule had redacted its benign value; a second scrub of the escaped JSON literal could then replace the entire tool-input string with a placeholder.

Only this exact identifier is exempt. Prefixed secret assignments, quoted secret-key context, provider token shapes, Bearer credentials, JWTs, private keys, and the escaped-literal fallback retain their protections. A recognizable credential stored under this field still gets scrubbed.

Version 6 and its digest remain immutable, with a frozen source fixture. Newly sealed objects use version 7 and its new manifest digest; existing stored objects are not rewritten. Tool-use display classification is a separate projection concern.

## Ruleset 8

Ruleset 8 leaves the public identifier unmatched instead of consuming its value, so scanning continues into that value and a labelled credential nested in it (`idempotency_key=password=VALUE`) is redacted. Version 7 is frozen in `test/fixtures/scrub-ruleset-7.ts`.

## Ruleset 9

Ruleset 9 closes nested credential tails that ruleset 8 still stored. All values below are synthetic.

- **Quoted values are redacted whole.** `idempotency_key=password="A B"` and `client_secret="A<LF>B"` store `password="[REDACTED]"`. The value runs to its matching closing quote, through whitespace and line breaks, using the same escape-depth test as the quoted-key rule (up to 16 backslashes before the opener). The closing quote must lie within `MAX_QUOTED_VALUE` (4096 units). Ruleset 8 stopped at the first space or line break and stored the rest. Both quotes are kept. An empty or already-redacted quoted value is left as is. When no close is found within the bound, the ruleset 8 value rule applies.
- **Encoded separators.** These forms now separate a name from its value, at any backslash depth up to 16 and in either hex case:
  - `\u003d` and `\u003a` (literal backslash sequences)
  - `\x3d` and `\x3a`
  - `%3D` and `%3A`

  For example, `password\u003dVALUE` stores `password\u003d[REDACTED]`. The public-selector exemption accepts the same separators.
- **Overlapping and chained assignments.** Assignments are matched at every position, not only after the previous value. The output redacts the union of their values, so every value ruleset 8 redacted is still redacted. This covers assignments whose name was swallowed by an earlier value (`password=x{password<TAB>=y`), and values whose opening quote closed an earlier quoted value.
- **Escaped closing quotes.** An unquoted value no longer takes the backslash run that escapes the enclosing string's closing quote. Under ruleset 8, `\"password=v\"` became `\"password=[REDACTED]"`.
- **Long texts.** A quoted assignment value that holds whitespace is reported as unsplittable (`unsplittableQuotedValueSpans`). As a result, `scrubLong` and the record path cut before its name, never inside it. Without that, a value with many words near the 256 KiB bound used up the cut-search budget and the chunk was refused.

The record path keeps its whole-string fallback. A string is scrubbed decoded, and its escaped literal is scrubbed only when the decoded form is unchanged. When ruleset 9 changes a decoded string that ruleset 8 left alone, that string is no longer replaced whole. Its remaining text is checked to be stable under a second pass. `test/scrub-v8-v9-fuzz.test.ts` documents this as the only permitted kind of difference.

Version 8 and its digest stay immutable, and the frozen source is in `test/fixtures/scrub-ruleset-8.ts`. Newly sealed objects use version 9 and the new manifest digest. Existing stored objects are not rewritten.

### Rollback

Rolling back to a version 8 image is a code revert. No data migration is involved, but objects already sealed under version 9 stay version 9, and a version 8 image refuses them:

- The manifest POST returns `409 chunks_unverified`. `chunksAreServerSealed` accepts only `scrub-version` <= the image's version.
- The Neon-to-S3 export throws `chunk_scrub_mismatch` for a version 9 chunk. It also ignores a stored manifest above its own version.

Before rolling back, stop new sealing or accept those refusals until a version 9 image is redeployed. No re-scrub is needed: a version 8 image reads no version 9 object, and version 9 objects stay valid for the next version 9 or later image.
