# Technical Foul shared artifact manifest

`packages/pipeline/technical-foul.json` pins the Technical Foul (secret scrub)
ruleset and its test corpus so the Rust port in desktop-core can detect drift.

It records the ruleset version (`SCRUB_RULESET_VERSION`), the digest of
`lib/scrub.ts` plus the `<scrub-segmenter>` block of `lib/pipeline/raw-chunks.ts`
(the same digest `test/scrub-ruleset-version.test.ts` pins), the segmenter digest
alone, the digest of `test/fixtures/scrub-secret-corpus.json` (the same digest
`test/scrub-secret-corpus.test.ts` pins), the redaction token and
`MAX_SCRUB_CHARS`. All digests are sha256 over LF-normalised text.

## Changing the ruleset

A change to `scrub.ts`, the segmenter or the corpus must, in the same PR, bump
`SCRUB_RULESET_VERSION` (for rule or segmenter changes), update the pins in the
two tests, and regenerate the manifest:

    cd packages/pipeline
    bun run technical-foul:manifest   # write
    bun run technical-foul:check      # verify; exits 1 with a short diff when stale

`test/technical-foul-manifest.test.ts` fails in CI (vitest) when the checked-in
manifest differs from the one computed from source.

## desktop-core

desktop-core vendors the corpus and this manifest, recomputes the same digests
over its own ruleset and the vendored corpus, and fails its lockstep test when
they drift from the manifest.
