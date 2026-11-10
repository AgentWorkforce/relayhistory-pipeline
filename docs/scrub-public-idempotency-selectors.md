# Public idempotency selectors in tool input

Ruleset 7 preserves the exact case-insensitive `idempotency_key` identifier in unquoted assignments. This field selects a replay receipt and is not an authentication credential. The broader assignment rule had redacted its benign value; a second scrub of the escaped JSON literal could then replace the entire tool-input string with a placeholder.

Only this exact identifier is exempt. Prefixed secret assignments, quoted secret-key context, provider token shapes, Bearer credentials, JWTs, private keys, and the escaped-literal fallback retain their protections. A recognizable credential stored under this field still gets scrubbed.

Version 6 and its digest remain immutable, with a frozen source fixture. Newly sealed objects use version 7 and its new manifest digest; existing stored objects are not rewritten. Tool-use display classification is a separate projection concern.

## Ruleset 8

Ruleset 8 leaves the public identifier unmatched instead of consuming its value, so scanning continues into that value and a labelled credential nested in it (`idempotency_key=password=VALUE`) is redacted. Version 7 is frozen in `test/fixtures/scrub-ruleset-7.ts`.

## Ruleset 9

Ruleset 9 closes nested credential tails that ruleset 8 still stored. All values below are synthetic.

- **Quoted values are redacted whole.** `idempotency_key=password="A B"` and `client_secret="A<LF>B"` store `password="[REDACTED]"`. The value runs to its matching closing quote, through whitespace and line breaks. A quote closes the value when its backslash run matches the opener's escape depth. The closing quote must lie within `MAX_QUOTED_VALUE` (4096 units). Ruleset 8 stopped at the first space or line break and stored the rest. Both quotes are kept. An empty or already-redacted quoted value is left as is.
- **Quoted values fail closed.** If a quoted value has no matching close within `MAX_QUOTED_VALUE`, it is redacted from the value start to the end of the input. This covers an unclosed opener and a close beyond the bound. For example, `password="A B` stores `password="[REDACTED]`. Scanning stops there. The value never falls back to the single-token rule of ruleset 8, which stored the rest of the value. This deliberately over-redacts: everything after such an opener is lost, including text that was not secret.
- **Encoded separators.** These forms now separate a name from its value, at any backslash depth and in either hex case:
  - `\u003d` and `\u003a` (literal backslash sequences)
  - `\x3d` and `\x3a`
  - `%3D` and `%3A`, behind any number of `%25` layers (`%253D`, `%25253A`, ...). The layers are counted, never decoded.
  - the legacy percent-u forms `%u003d` and `%u003a`, also behind `%25` layers

  The escape letters also match in upper case (`\U003D`, `\X3A`, `%U003D`). These are not valid escapes, but matching them only redacts more. For example, `password\u003dVALUE` stores `password\u003d[REDACTED]`. The public-selector exemption accepts the same separators. Ruleset 9 as first proposed capped the backslash run at 16; longer runs left the value unscrubbed. The scan is still linear.
- **Encoded quote openers.** `%22`, `%27` and `%60`, and the legacy percent-u forms `%u0022`, `%u0027` and `%u0060` (either case), behind any number of `%25` layers (`%2522`, `%252522`, `%25u0022`), and `\u0022` / `\x22` (also `27` and `60`) behind one or more backslashes, open a quoted value just as a literal quote does. The matching close is the same encoded form (letter case aside) behind the same number of backslashes or `%25` layers; a close with a different layer count is not the close. Stored bytes are never decoded or rewritten. For example, `password%3D%22A B%22` stores `password%3D%22[REDACTED]%22`.
- **Quoted key/value rule, any escape depth.** The quoted key/value rule (`"password": "value"`, `\"password\": \"value\"`, ...) no longer caps the backslash run before the key's quotes or before the value's opening quote. Ruleset 8 read at most 16 backslashes, so a key or value five JSON encodings deep (31 backslashes, `JSON.stringify` applied six times) was not matched and the value was stored. A value with no matching close still runs to the end of the input, and is now also reported as unsplittable from its key to the end of the text. A value closed past `MAX_QUOTED_VALUE` is still redacted whole to its close, as since ruleset 4. The text after the close is kept.
- **Overlapping and chained assignments.** Assignments are matched at every position, not only after the previous value. The output redacts the union of their values, so every value ruleset 8 redacted is still redacted. This covers assignments whose name was swallowed by an earlier value (`password=x{password<TAB>=y`), and values whose opening quote closed an earlier quoted value.
- **Escaped closing quotes.** An unquoted value no longer takes the backslash run that escapes the enclosing string's closing quote. Under ruleset 8, `\"password=v\"` became `\"password=[REDACTED]"`.
- **Long texts.** A quoted assignment value that holds whitespace, or that fails closed, is reported as unsplittable (`unsplittableQuotedValueSpans`). The span runs from the name to the end of the redaction, or to the end of the input when the value fails closed. As a result, `scrubLong` and the record path cut before its name, never inside it. Without that, a value with many words near the 256 KiB bound used up the cut-search budget and the chunk was refused. When text after a fail-closed opener does not fit in one chunk, the chunk is refused (`chunk_string_too_long` / `chunk_unsegmentable`) rather than split without its name.
- **Nested assignments run to their container's end.** A credential assignment inside a quoted string that was itself opened as a value (`idempotency_key="password=A B"`, `{"idempotency_key":"password=A B","peer":"x"}`, also behind the encoded separators and quotes above), or inside a JSON string value under a public selector key (`scrubJson`/`scrubRecord` on `{"idempotency_key":"password=A B"}`), is redacted to the end of that string: `idempotency_key="password=[REDACTED]"`. Ruleset 8 stopped at the first space and stored `B`. When the container's close is known, the redaction stops there and the text after it (peer fields) is kept. An unclosed container fails closed to the end of the input. Outside a container, ruleset 8's single-token rule for an unquoted value is unchanged (`password=A B` stores `password=[REDACTED] B`). Object keys are never containers.
- **Names behind an encoded quote or escape.** A credential name directly after an encoded quote (`%22`, `%2522`, `%u0022`, `\u0022`, `\x27`, any of the forms ending in `22`, `27` or `60`) or an escape sequence written out (`\n`, `\t`, ...) is now matched, as in `%22password=` or `line\npassword=`. Ruleset 8 required a word boundary there and stored the value. The start is fixed length and bounded, so the scan stays linear; a look-alike such as `x22token=` is also redacted.

The structured record path (`scrubRecords`) treats a string value under a public selector key the same way: `{"idempotency_key":"password=A B"}` stores `{"idempotency_key":"password=[REDACTED]"}`, and so does every string anywhere below that key (objects, arrays, any depth, every duplicate of the key). A secret key wins where both apply: the value is redacted whole, as since ruleset 5. Sibling keys are not affected. A value longer than the scrub bound is scrubbed in segments; once a nested credential runs to the end of a segment, the later segments are dropped, because the redaction runs on to the end of the string. This changed the segmenter block, so the frozen ruleset-7 and ruleset-8 tests now pin their unchanged fixtures together with the live segmenter.

Not covered: opaque encodings (base64 and the like), HTML entities (`&quot;`, `&#61;`) and other encodings than those listed above are not recognised as separators or quotes.

The record path keeps its whole-string fallback. A string is scrubbed decoded, and its escaped literal is scrubbed only when the decoded form is unchanged. When ruleset 9 changes a decoded string that ruleset 8 left alone, that string is no longer replaced whole. Its remaining text is checked to be stable under a second pass. `test/scrub-v8-v9-fuzz.test.ts` documents this as the only permitted kind of difference.

Version 8 and its digest stay immutable, and the frozen source is in `test/fixtures/scrub-ruleset-8.ts`. Newly sealed objects use version 9 and the new manifest digest. Existing stored objects are not rewritten.

### Rollback

Rolling back to a version 8 image is a code revert. No data migration is involved, but objects already sealed under version 9 stay version 9, and a version 8 image refuses them:

- The manifest POST returns `409 chunks_unverified`. `chunksAreServerSealed` accepts only `scrub-version` <= the image's version.
- The Neon-to-S3 export throws `chunk_scrub_mismatch` for a version 9 chunk. It also ignores a stored manifest above its own version.

Before rolling back, stop new sealing or accept those refusals until a version 9 image is redeployed. No re-scrub is needed: a version 8 image reads no version 9 object, and version 9 objects stay valid for the next version 9 or later image.
