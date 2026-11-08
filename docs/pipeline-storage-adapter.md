# Session pipeline: storage-notification adapter

The adapter turns "a session manifest landed in object storage" into one
object-landed `StageMessage` on the pipeline queue
(`packages/relayhistory/src/lib/pipeline/adapter.ts`). It is inert: nothing
here is bound or configured in any deployment, and nothing runs until the env
below is set.

## Inputs

| Input                                                                | When                                                                                          | Org from                                                                                                                             |
| -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `POST /v1/pipeline/manifests` (`rth:sync`)                           | The delivery coordinator reports a manifest it wrote. Needs no bucket event source.           | Auth. The key's `ws=` and prefix must be the caller's, and the object's owner (same rule as notifications) must be the caller's org. |
| R2 event notifications on `relayhistory-object-events`               | Hosted, once the bucket's notifications are bound. Puts and deletes.                          | `PIPELINE_ORG_ID` or `PIPELINE_ORG_FROM_PREFIX=true`. Otherwise the event retries into the queue's DLQ.                              |
| Reconcile job (`reconcile.ts`), from the minute cron every 5 minutes | Self-host MinIO and any store with no event source, plus a safety net for lost notifications. | Same as notifications.                                                                                                               |

All three build the message with `buildObjectLandedMessage`. Trace id and
revision come from the manifest, tenancy from the key, and
`workKey = manifestWorkKey(payload)` with the ETag normalized, so the same
manifest reported twice is one unit of work. A manifest that contradicts its
key, or has no valid trace id, is rejected rather than retried. Chunk objects
are ignored. A manifest delete becomes an object-landed message with
`deleted: true` and version `deleted:<event time>`; the index re-reads the key.

The payload carries `provider` (`r2`, `s3`, `gcs`, `azure`, `fs`) and `bucket`,
which the index stores on the pointer row, and `scrubbed`.

### Manifest writer and migration fencing

The authenticated manifest route checks more than the object-store
precondition. A live write must keep `raw.owner` equal to its delivery writer,
preserve every acknowledged epoch/chunk and carry the exact previous object
version. An assertion whose verified subject/class is
`agent_relayhistory_migration`/`migration_assertion` is additionally bound to
its `grantId` and `planDigest` claims. It may create a cold export owned by
`migration:<grantId>`, or append one `backfill` epoch to an existing live
manifest. A straddling append preserves the live owner and writer, all prior
raw data, the server section and other extension owners. The epoch's
`opened_by` must equal `migration:<grantId>`.

The migration exception does not weaken compare-and-swap: if live delivery
commits after the exporter read, the export receives 409 and must reread and
rebuild its epoch. A normal `rth:sync` assertion cannot create a `backfill`
epoch. Identical request replay remains idempotent.

## Scrubbing before object storage

Raw chunks are scrubbed of PII and secrets before they reach object storage
(manifest spec, "Scrubbing"; decided by Khaliq on 2026-10-25). The Rust client
scrubs when it seals a chunk. For a client that cannot, the server's
`POST /v1/pipeline/chunks` (`rth:sync`) does the same thing
(`lib/pipeline/raw-chunks.ts`):

- a line that is a JSON object, array or string is a record: every string
  token in it (values and keys) is decoded, scrubbed with `scrubText` and
  written back, and an integer that is a card number becomes `"[REDACTED]"`.
  A source literal is kept only when neither its decoded value nor its raw
  text matches a rule, so a secret behind a `\u` escape is caught. Keys that
  scrub to the same name get `scrubJson`'s ` (2)`, ` (3)` suffixes. Nothing
  else changes, so 64-bit ids, number formatting and `__proto__` keys are
  stored exactly;
- consecutive lines that are not records are scrubbed together, so a
  multi-line secret such as a PEM private key is matched whole. A long run is
  cut into segments under the 256 KiB scrub limit at line boundaries: a
  private key starts its own segment, and no cut falls inside one or between
  two digit groups or inside any match spanning a newline (a `Bearer` or
  `key =` prefix and its secret), so no line is truncated. A chunk with no
  such boundary in the last 64 KiB of a run is refused with 400 rather than
  truncated. From scrub rule set 2, a single line or string longer than the
  256 KiB scrub limit is stored whole, scrubbed in segments cut at safe
  whitespace and never inside a private-key block; one with no safe cut is
  refused. Bytes that are not UTF-8 are refused;
- the caller must own the location by the same rule as the manifest POST
  (`PIPELINE_ORG_ID` or the org prefix), checked before the body is read, and
  the body is capped at 16 MiB while it is read;
- the scrubbed records are zstd-compressed and stored create-only at the v1 raw
  key, with `raw-sha256`, `raw-bytes`, `scrub-ruleset` and `scrub-version`
  metadata;
- the response is the chunk entry and the `raw.scrub` marker
  (`relayhistory-scrub`, the current version: 2 from scrub rule set 2, 3 from rule set 3, 4 from rule set 4, 5 from rule set 5, 6 from rule set 6) for the client's manifest.
  Rule set 4: a quoted secret value whose matching closing quote is not within
  the quoted rule's 4096-unit scan bound (`MAX_QUOTED_VALUE` in `lib/scrub.ts`,
  UTF-16 code units counted from the value's start across line breaks) is
  replaced whole by `[REDACTED]`, up to that closing quote or, with none, to the
  end of the input; rule set 3 stored the part past the bound in clear. A value
  whose total length to its matching close is within the bound (multi-line
  ones included), or whose input ends within it, scrubbed byte-identically to
  rule set 3 under rule set 4; rule set 5 redacts such a value whole when it
  holds a line break or has no close (below).
  - **Across line breaks.** The bound caps the value's total length, not its
    first line's: CR, LF, CRLF, U+2028, U+2029 and U+0085 are ordinary value
    units, so a value with a short first line that crosses the bound only after
    one or more breaks is over the bound too. A template literal, an escaped
    line continuation or a malformed transcript value that spans lines is
    redacted to its closing quote (same quote, same escaping test; any supported
    quote, backtick included) or the end of the input, and the text after the
    closing quote is kept. Stopping at a line break would leave a keyless tail
    that no later rule could identify. Consequence: an unclosed quote after a
    secret key with no matching quote within 4096 units now redacts through the
    next matching quote of the same kind, possibly on a much later line, or to
    the end of the input. The scan is one forward walk per value and the
    redaction consumes it, so the pass stays linear.
  - **New refusals.** No segment boundary falls strictly between the start of
    such a value's key and the value's end; a cut exactly at the key's start is
    allowed. The same holds for a generated unquoted value of 4096 or more units
    after a quoted key. `longCut` (long strings, cut after any whitespace) finds
    these values (`overCapQuotedValueSpans`; `unsplittableQuotedValueSpans` since
    rule set 5) in the text up to 4096 + 256 units
    past the segment bound, which covers the longest key plus the scan's reach
    (4096 + 150 units from the key's start), so a key just below the bound or
    straddling it is protected too; one still open there, decided or not, is
    taken to run to the end. `safeCut` (runs of text lines, cut at line
    boundaries) skips line boundaries inside them, scanning the run and its
    look-ahead as a prefix (a value open at its end is open to the end), and a
    run that ends inside a secret value which is, or may yet become, over the
    bound keeps a following record or private-key line instead of ending
    before it (fail closed, at most 256 rescans per chunk). Under rule set 4,
    values within the bound got no such span and relied, as in rule set 3, on
    the context check (`CUT_CONTEXT_CHARS`); rule set 5 gives a multi-line or
    unclosed one a span too. When no cut outside them is left the
    string is rejected with `chunk_string_too_long` (a run: `chunk_unsegmentable`),
    which is non-retryable and holds no content. The session ingest (`src/session-ingest/oversize.ts`) replaces such
    a string with its redaction marker before sealing, and the neon reader
    (`src/migration/neon-s3-sync/neon-reader.ts`) truncates it to a prefix that
    the sealer then scrubs.
  - **Quoted keys only.** The whole-value rule covers values after a quoted key.
    A later follow-up (not rule set 5) covers the rest: unquoted keys (YAML,
    TOML, env, kwargs), more than 16 spaces before the colon, `\u` escapes in
    the key, keys longer than 64 characters, and an NBSP separator.

  Rule set 5 closes five rule set 4 leaks; output for input with no secret-keyed
  record value, no over-cap quoted value, no quoted secret value that holds a
  line break or has no matching close, and no quoted secret value that starts
  with `op://` but is not a strict reference (below) is byte-identical to rule
  set 4.
  - **Over-cap measured on the original text.** Rule set 4 measured a quoted
    secret value after the token rules had replaced its provider-shaped tokens
    with `[REDACTED]`, so a value over the bound (say 130 `capy_` tokens and then
    a line break and more text) could shrink under it and keep its later lines.
    Rule set 5 runs an over-cap pass first, on the original text: it finds the
    same values as the full quoted rule and redacts each over-bound one whole
    (through a private-key footer when a key block opens inside it).
  - **Key context on the record path.** `scrubJson`, `scrubRecord` and the
    raw-chunk record-line tokenizer (`scrubRecordLine`) redact a non-empty
    string under a secret key name (`isSecretKeyName`), at any depth and through
    arrays, unless it is already `[REDACTED]` or a strict `op://` reference. Rule set
    4 scrubbed each value alone and stored `{"password":"plain words"}` as it
    was. Numbers, booleans and null are kept. The `<scrub-segmenter>` markers
    now also enclose `scrubRecordLine`, so the pinned digest covers the record
    path.
  - **Secret context through composite values.** The secret context of a
    secret-keyed value runs through every descendant: in
    `{"password":{"value":"..."}}`, `{"token":{"nested":{"deeper":"..."}}}` or
    `{"credentials":[{"value":"..."}]}` every string below the secret key is
    redacted, whatever its own key is named, in `scrubJson`/`scrubRecord` and
    `scrubRecordLine` alike. The structure is kept (keys are still scrubbed and
    de-duplicated), as are numbers, booleans, null, empty strings, `[REDACTED]`
    and strict `op://` references; a sibling of the secret key is scrubbed by its own
    keys only. The context resets at the end of the secret-keyed value.
    `scrubText` over a raw JSON line does not parse it: there a composite under
    a secret key is still left to the token rules (the quoted rule needs a
    quoted value).
  - **Multi-line quoted values within the bound.** A quoted secret value
    within the 4096-unit bound that holds a line break (CR, LF, CRLF, U+2028,
    U+2029, U+0085) before its matching close, or has no matching close, is
    redacted whole: through its close, or to the end of the input. Rule set 4
    redacted it only up to the first whitespace (rule set 3's fallback) and
    kept the rest, later lines included; that fallback is gone. The empty
    value, `[REDACTED]` and strict `op://` exceptions remain (see below), as do unquoted values and
    the over-cap first pass. The segmenter protects such a value as it does an
    over-bound one (`unsplittableQuotedValueSpans`): no `longCut` or `safeCut`
    falls strictly between its key's start and its value's end, and a window
    scanned as a prefix (`prefix: true`) takes a value still open at its end
    as open to the end (fail closed). A value on one line that closes within
    the bound scrubs as in rule set 4.
  - **Only a strict `op://` reference is kept.** The quoted rule
    (`scrubText`, at every escaping depth) and the record paths (`scrubJson`,
    `scrubRecord`, `scrubRecordLine`, through `redactsSecretKeyedValue`) keep
    a value that starts with `op://` only when the whole value is a complete
    one-line 1Password reference (`OP_REFERENCE` in `lib/scrub.ts`):
    `^op://SEG(/SEG){2,3}$`: exactly `vault/item/field` or
    `vault/item/section/field`; anything else, including two segments or five
    or more, is redacted. Each segment is one or more runs of ASCII
    letters, digits, `.`, `_` or `-` joined by single spaces
    (`op://vault/item/field`, `op://My Vault/item name/field`). Any other
    value starting `op://` is redacted whole: one holding a backslash (so any
    escape sequence, such as an escaped line separator `\n`, `\r`, `\u2028`,
    `\u2029`, `\u0085`, or an escaped quote), a quote, a control character or
    line separator, a tab, a double space, a leading or trailing space in a
    segment, a query string, or too few or too many segments. Before this, a value was kept unless it held a
    literal line separator, so in nested or escaped JSON
    (`{"api_key":"op://vault/item\nTAIL"}`, the text holding a backslash then
    `n`; a tool result) the separator was stored as an escape and the tail was
    kept. A quoted value with no matching
    close still runs to the end of the input, and is kept only when all of it
    up to there matches the grammar (nothing follows it).
  - **Repeated source keys.** `scrubRecordLine` writes a key the source repeats
    in one object under the name its first occurrence got, so a repeated key
    that scrubs to `[REDACTED] (2)` no longer lands on the first key's
    `[REDACTED]` after parsing.
  - **Re-scrubbing rule set 4 output.** Rule set 5 over stored rule set 4
    records catches the first leak (the key is still there) and the composite
    one (the secret key is still above the value). The second leak's stored
    value is already under the bound, but it still holds the line break after
    which rule set 4 stopped, so the multi-line rule redacts it whole too.

  Rule set 6 corrects two rule set 5 defects; output for input with no unquoted
  secret value of 4096 units or more and no private-key boundary inside a
  quoted secret value is byte-identical to rule set 5
  (`test/scrub-v5-v6-fuzz.test.ts`).
  - **Unquoted values classified whole.** An unquoted value after a quoted
    secret key (`"password": 0000…`) is read to its real end before it is
    classified. Rule set 5 classified its first 4096 units only, so 4096 digits
    then an alphanumeric tail passed as a number and the tail was stored. One
    of 4096 units or more is now redacted whole, whatever it holds (a pure
    number that long included); a shorter one is kept when it is a JSON
    literal, a number or not generated-looking, as before. Number lexemes in a
    parsed JSON record are still kept.
  - **Private-key boundaries kept paired.** The over-cap first pass runs before
    the private-key rule, which pairs each header with the next footer. Rule
    set 5 took a footer inside an over-cap value with the value, so a header
    before that value was paired with nothing and everything from it to the end
    of the input was redacted. Now a quoted secret value (of any length) that
    holds the footer of a header before it is redacted from that header through
    the value's closing quote, and the text after it is kept; one that holds a
    header whose footer is after it is redacted through that footer with its
    closing quote kept (rule set 5 dropped the quote, and the quoted rule then
    redacted to the end). Within the bound, rule set 5 kept the part of such a
    value after the footer; it is redacted too. The segmenter is unchanged:
    no cut falls between a key block's header and its footer, nor inside the
    value.

The unscrubbed bytes are never written to storage. If the key already holds
an object (identical bytes or not), it is kept and described from its own
metadata; an object with no scrub metadata is refused rather than vouched for.
`SCRUB_RULESET_VERSION` must be bumped with any change to `lib/scrub.ts`:
`test/scrub-ruleset-version.test.ts` pins each version to the file's digest
and fails until the bump is made. Regenerate `packages/pipeline/technical-foul.json` in the same PR
(`bun run technical-foul:manifest` in `packages/pipeline`; see `docs/technical-foul-manifest.md`).

A manifest with no `raw.scrub` is still indexed, with `scrubbed: false`, so its
pointer row is quarantined and the transform never reads its chunks.

## Encryption at rest and logging

- R2 encrypts every object at rest. For MinIO, configure KMS auto-encryption,
  or set `PIPELINE_RAW_S3_SSE=AES256` (or `aws:kms`) to request SSE on every
  put; the request then fails rather than storing plaintext if the server
  cannot encrypt.
- Credentials are runtime secrets and are never logged.
- Log lines carry codes, counts and ids only. Manifest bodies, chunk bytes and
  object-store error messages (which can quote manifest values) are never
  logged or echoed in a response.

## Env

| Name                                                                                           | Meaning                                                                                                                                  |
| ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `PIPELINE_RAW_S3_BUCKET`, `PIPELINE_RAW_S3_ACCESS_KEY_ID`, `PIPELINE_RAW_S3_SECRET_ACCESS_KEY` | The raw store over the S3 API. Unset: the manifest POST validates the posted copy, the chunk route answers 503, and notifications retry. |
| `PIPELINE_RAW_S3_ENDPOINT`, `PIPELINE_RAW_S3_REGION`                                           | MinIO (`http://minio:9000`) or R2's S3 endpoint.                                                                                         |
| `PIPELINE_RAW_PROVIDER`                                                                        | `s3` (default), `r2`, or `gcs`. `gcs` uses the GCS adapter and a metadata token, and refuses HMAC keys. Any other name leaves the store unconfigured. An S3 or R2 endpoint on the GCS API is refused. |
| `PIPELINE_RAW_S3_SSE`                                                                          | `AES256` or `aws:kms`; any other value leaves the raw store unconfigured (fail closed).                                                  |
| `PIPELINE_ORG_ID` / `PIPELINE_ORG_FROM_PREFIX`                                                 | Object ownership. With neither, the manifest POST fails closed with 503.                                                                 |
| `PIPELINE_KEY_PREFIX`                                                                          | Deployment prefix raw keys sit under. Required for reconcile when the org is taken from the prefix.                                      |
| `PIPELINE_RECONCILE_ENABLED`, `PIPELINE_RECONCILE_LOOKBACK_MINUTES`                            | `true` runs reconcile every 5 minutes over the last 15 minutes (default).                                                                |

## Decisions and follow-ups

- The R2 event consumer stays inactive (Khaliq, 2026-10-28): no binding is
  activated without his separate OK. The `CLAUDE.md` contract says "No D1/R2
  endpoints"; the adapter adds no binding and reaches R2 only through its S3
  API with runtime secrets.
- The static (or key-prefix) workspace → org mapping is accepted for now.
- Follow-up: reconcile lists the whole manifest prefix each run (chunks
  included) and filters by last-modified. That is fine for self-host sizes;
  hosted scale needs a persisted reconcile cursor.
