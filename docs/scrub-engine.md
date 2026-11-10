# Scrub engine seam (TypeScript today, relayscrubber wasm later)

Status: seam only. The default engine is the TypeScript scrubber
(`packages/pipeline/src/lib/scrub.ts`, ruleset `relayhistory-scrub` version 8, plus
`scrubRecords`/`scrubLong`/`sealRawChunk` in `packages/pipeline/src/lib/pipeline/raw-chunks.ts`).
No `.wasm` artifact is checked in, and the checked-in pin is a placeholder that refuses to load.

Code: `packages/pipeline/src/lib/scrub-engine/`

| File | Role |
|---|---|
| `engine.ts` | `ScrubEngine` interface, `ScrubEngineUnavailable` / `ScrubEngineConfigError`, the frozen `tsScrubEngine` |
| `wasm-abi.ts` | The only file that knows the wasm export surface (ABI version 1) |
| `wasm-engine.ts` | Pin parsing, artifact verification, `WasmScrubEngine` |
| `index.ts` | `selectScrubEngine`, `initScrubEngine`, `activeScrubEngine`, the sync call-site accessors |
| `predicate-hook.ts` | Late-bound `scrubText` for `trace.ts` `isSafeIdentifier` (import-cycle break, see below) |
| `packages/pipeline/scrub-wasm-pin.json` | The pin |
| `bindgen-engine.ts` | `SCRUB_ENGINE=wasm-bindgen`: pin parsing, verified glue import, manifest check, `createCompiledBindgenEngine` (see "wasm-bindgen path") |
| `packages/pipeline/scrub-bindgen-pin.json` | The wasm-bindgen pin (placeholder) |

Tests: `packages/pipeline/test/scrub-engine-parity.test.ts`, `packages/pipeline/test/scrub-engine-bindgen.test.ts`.

## Selection

`SCRUB_ENGINE` selects the engine for the whole process (or Worker isolate):

| Value | Engine |
|---|---|
| unset, `""` or `ts` | TypeScript, byte-identical to main. No I/O, nothing to initialise. |
| `wasm` | The pinned relayscrubber artifact only (raw ABI v1). |
| `wasm-bindgen` | The pinned `@agentworkforce/relayscrubber` wasm-bindgen package only. See "wasm-bindgen path". |
| anything else | `ScrubEngineConfigError` (`scrub_engine_invalid`): startup fails. |

The selection is made once. A second, conflicting selection in the same process throws.
`""` is treated as unset because Compose and Helm emit it for an unset variable.

Every self-host `SERVICE=` entry point calls `await initScrubEngine()` before it serves or
runs its job (`src/shared/worker.ts` `runWorker` covers the indexer, referee, bob-costas and
session-brief). With `wasm` this verifies the pin and the module. On failure the process exits
non-zero rather than start. The ingest server's `/readyz` also requires `scrubEngineReady()`.

## Fail-closed rules (SCRUB_ENGINE=wasm)

- `initScrubEngine` throws `ScrubEngineUnavailable(reason)` on any failure. The reasons are:
  - `unpinned`
  - `pin_invalid`
  - `artifact_missing`
  - `sha256_mismatch`
  - `instantiate_failed`
  - `export_missing`
  - `abi_mismatch`
  - `ruleset_mismatch`
  - `profile_mismatch`
- After a failure, or before init, every scrub call throws `ScrubEngineUnavailable`. That covers
  `scrubText`, `scrubLong`, `scrubJson`, `scrubNumber`, `scrubRecords` and `sealRawChunk`. The
  reason is the init failure's reason, or `not_initialised`.
- **There is no fallback to TypeScript and no raw passthrough.** `activeScrubEngine()` returns
  `tsScrubEngine` only when `ts` was selected.
- A trap or throw inside the module poisons the instance (`engine_failed`). Every later call refuses.
- An unexpected status, or output that is not UTF-8, refuses that call (`engine_refused`). The
  input is never returned.
- Text containing a lone UTF-16 surrogate cannot cross a UTF-8 ABI unchanged. It is refused with
  `scrub_input_not_well_formed` instead of being silently rewritten to U+FFFD.
- `ScrubEngineUnavailable` is a `PipelineError` (`scrub_engine_unavailable`, retryable). Queue
  consumers retry, and the existing `instanceof PipelineError` paths classify it with a stable code.
- Log lines (`src/shared/log.ts`) scrub through `scrubTextForLog`. Under `ts` this is exactly
  `scrubText`. Under an unavailable or failing wasm engine the field becomes `[scrub-unavailable]`,
  so logging never throws and never emits unscrubbed text.

## The pin

`packages/pipeline/scrub-wasm-pin.json`:

```json
{ "schema": 1, "crate": "relayscrubber", "crate_version": "x.y.z",
  "ruleset_version": 8, "scrub_profile": "<profile id>",
  "wasm_sha256": "<64 hex>", "abi_version": 1 }
```

`parseScrubWasmPin` enforces these rules:

- The checked-in placeholder is rejected as `unpinned`: a sha of all zeros, or `UNPINNED` as the
  crate version or profile.
- The crate must be `relayscrubber`.
- The sha must be 64 hex characters.
- The versions must be integers.
- `abi_version` must equal `WASM_ABI_VERSION`.
- `ruleset_version` must be at least the TypeScript `SCRUB_RULESET_VERSION`.

At init the artifact's sha256 must equal the pin. The module's self-reported ABI version, ruleset
version and profile must also equal the pin. A mismatch in any of them is a refusal.

## ABI contract (version 1, `wasm-abi.ts`)

- Instantiated with **no imports** (no WASI, no host clock or randomness, no I/O).
- All values are i32. Pointers and lengths are read as unsigned.
- Required exports. A missing or non-callable export fails init with `export_missing`:

| Export | Contract |
|---|---|
| `memory` | Linear memory |
| `scrub_abi_version() -> i32` | `1` |
| `scrub_ruleset_version() -> i32` | The ruleset this build implements |
| `scrub_profile() -> status` | On OK the result buffer holds the profile id (UTF-8) |
| `scrub_alloc(len) -> ptr` / `scrub_free(ptr, len)` | The input buffer, owned by the module |
| `scrub_text(ptr, len) -> status` | Same contract as TS `scrubText` (UTF-8 in, UTF-8 out) |
| `scrub_long(ptr, len) -> status` | Same contract as TS `scrubLong` (segmentation inside the crate) |
| `scrub_json(ptr, len) -> status` | Input is one JSON text; output is the scrubbed JSON text, with key context as TS `scrubJson` |
| `scrub_records(ptr, len) -> status` | Input is the **raw chunk bytes**; output is the scrubbed bytes, as TS `scrubRecords` |
| `scrub_result_ptr()` / `scrub_result_len()` | The last OK result |
| `scrub_result_records()` | `scrub_records` only: the number of complete lines |
| `scrub_result_free()` | Releases the result buffer |

- The status values are `0` OK, `1` INVALID_UTF8 (`scrub_records` maps it to the existing
  `chunk_not_utf8` refusal), and `2` INVALID_JSON. Anything else is `engine_refused`.
- Host sequence per call:
  1. `alloc`
  2. copy in
  3. operation
  4. on OK, copy the result out (`.slice()`, because memory may grow)
  5. `result_free`
  6. `free` the input, in a `finally`
- The host never parses records, segments text or applies secret-context rules. The crate does
  all of that. The adapter only moves bytes and checks UTF-8. `scrubJson` stringifies the value,
  sends the text, and parses the result. `undefined`, `null` and booleans are returned as-is,
  exactly as TS does. Values JSON cannot carry differ by construction: `BigInt` throws (fail-closed), `NaN` becomes `null`, and `toJSON` is applied. Lone surrogates inside JSON strings cross as `\u` escapes.

### Crate obligations

The crate must produce byte-identical output to the TS oracle for its declared ruleset. The parity
test runs against a TS-backed mirror of this ABI. When the crate lands, the same corpus,
frozen-fixture and edge cases must run against the real module:
- the 199-case secret corpus, plus the multi-record chunks
- the frozen fixtures `scrub-ruleset-3..7.ts` and `6-prefix-f6a97053.ts`

## Persisted marker rule

The marker persisted with scrubbed bytes is `{ruleset, version, profile}`. It covers object
metadata `scrub-ruleset` / `scrub-version`, manifests and summary `scrub_version` columns.

- **The marker comes only from the engine instance that produced the bytes.**
  - `scrubRecords` returns `{scrubbed, records, scrub}`, where `scrub` is that engine's `marker`.
  - The wasm marker is built from the instance's own `scrub_ruleset_version()` and
    `scrub_profile()`, after they were checked against the pin. It is never taken from the pin
    or from configuration alone.
  - `writeShortSummary` (`src/hoopdb/session-directory-summary.ts`) takes the engine once, and
    stamps that engine's `rulesetVersion` with the text the same engine scrubbed.
- The TypeScript engine stamps exactly what main stamps: `relayhistory-scrub` / `8`. Its profile
  id is `relayhistory-scrub/typescript`, which is not persisted by any TS path.
- Readers never re-scrub. Unset reader policy retains the existing presence/quarantine
  and courier admission checks. An explicit `READER_MAX_RULESET` enables the independent
  family/version and per-chunk policy described below; it never changes TS writer stamps.
  A wasm cutover above 8 still requires a coordinated reader-first rollout. The pin refuses
  a writer ruleset below 8 or above its configured ceiling (default writer gate: 8).

## Cutover rule

1. While `SCRUB_ENGINE` is unset or `ts`, all output is byte-identical to main. `technical-foul.json`
   still pins the bytes of `scrub.ts` and the raw-chunks segmenter, and this change did not alter them.
2. **The first deployable cutover makes the pinned wasm mandatory.** The services image and Worker
   ship with `SCRUB_ENGINE=wasm` and a real pin. From then on, `scrub.ts` is the **frozen oracle**:
   it is used only by the parity test, and is not called by production paths. There is no
   per-request or per-service mixing and no automatic fallback.
3. Before that cutover, `sealRawChunk` must be routed. Today the seam's `sealRawChunk` delegates to
   raw-chunks under `ts`, and refuses under `wasm` (`seal_path_not_routed`), because raw-chunks
   scrubs internally and its bytes are pinned. The follow-up re-pins `raw-chunks.ts` so that
   `sealRawChunk` takes the engine as a parameter. It needs a technical-foul re-pin with review.

## Worker bundling (findings, nothing shipped)

- The hosted ingest Worker (`stacks/cloudflare/session-ingest.ts`, `nodejs_compat`) is built by
  Alchemy's bundled (rolldown) mode. In the pinned Alchemy beta.84, that mode has **no `.wasm`
  module rule**: its text rules cover only `.sql`, `.txt` and `.html`.
- The prebuilt `bundle: false` path does have default rules: `**/*.wasm` becomes `CompiledWasm`,
  and `**/*.bin` becomes `Data`.
- Workers cannot compile wasm from bytes at runtime (`WebAssembly.compile` / `instantiate(bytes)`
  are disallowed). The module must arrive as a `CompiledWasm` binding / import. That is why
  `ScrubWasmArtifact` is `{bytes, module?}`:
  - The Worker loader will supply the pinned bytes, for the sha256 check, together with the
    precompiled `module`.
  - `defaultInstantiate` instantiates `module` when present.
  - The bytes and the module must come from the same pinned file. The Worker build must assert
    this.
- Options:
  - (a) Add a `.wasm` rule / plugin to the bundled build that emits the file as a `CompiledWasm`
    module, plus a `Data` copy of the same bytes for hashing.
  - (b) Move the ingest Worker to a prebuilt `bundle: false` upload with `**/*.wasm` rules.
- Until then, `src/session-ingest/worker.ts` `loadWorkerScrubWasm` throws `artifact_missing`.
  With `SCRUB_ENGINE=wasm`, every request is answered `503 not_configured` and every queue message
  is retried after 300 s. Nothing is scrubbed by another engine.
- **Seam changes must not change hosted Worker behaviour with `SCRUB_ENGINE` unset.**
  - With it unset, `initScrubEngine` returns `tsScrubEngine` with no I/O. The Worker's readiness
    check is always true.
  - Module evaluation does not touch `import.meta.url`. The wasm file path is resolved only
    inside the Node/Bun loader.
  - Every call site gets the same TS functions as before.
  - `worker-bundle.test.ts` and the existing Worker tests pass unchanged.
- The Lambda handlers (read API, pipeline, bob-costas) do not call `initScrubEngine`. Under `ts`
  they resolve lazily, as before. Under `wasm` they would refuse with `not_initialised`, which is
  fail-closed. They are hosted-only and out of scope for the self-host cutover.

## wasm-bindgen path (`SCRUB_ENGINE=wasm-bindgen`, default off, nothing bound)

A third engine for the relayscrubber package as published by wasm-bindgen: generated
`relayscrubber_bg.wasm`, the generated JS glue `relayscrubber.js`, and the package
loader `index.js`. It is independent of the raw ABI v1 path. `wasm` and `wasm-bindgen`
are mutually exclusive in one process (a conflicting selection throws), and a bindgen
init never consults the raw-ABI loader. Raw ABI v1 stays in the tree, but the first
cutover is not planned to use it.

**Nothing is bound.** `packages/pipeline/scrub-bindgen-pin.json` is a placeholder: no
package version, no digests, no manifest values. Selecting `wasm-bindgen` today makes
`initScrubEngine` throw `ScrubEngineUnavailable(bindgen_unpinned_package_version)`, and
every call site then refuses as described in "Fail-closed rules".

### The pin

| Field | Meaning |
|---|---|
| `schema` / `package` | `1` / `@agentworkforce/relayscrubber`, else `bindgen_pin_invalid` |
| `package_version` | The exact published version |
| `wasm_sha256`, `glue_sha256`, `loader_sha256` | Lowercase hex sha256 of each file |
| `paths` | Package-relative paths of the three files (no absolute paths, no `..`) |
| `scrub_profile` | Must equal the crate-owned `manifest.scrubProfile` string verbatim |
| `manifest` | The full schema-3 `manifestJson()` the instance must report: `schema`, `ruleset`, `rulesetVersion`, `rulesetSourceSha256`, `crateVersion`, `abiVersion`, `policy` (`secrets`/`pii`), `redactionToken`, `scrubProfile` |

Every placeholder value (`UNPINNED`, empty, the all-zero digest, `0` versions) is refused
with its own reason: `bindgen_unpinned_package_version`, `bindgen_unpinned_<wasm|glue|loader>_sha256`,
`bindgen_unpinned_scrub_profile`, `bindgen_unpinned_manifest_<field>`. A manifest
`rulesetVersion` below the TypeScript ruleset (8) is `bindgen_ruleset_below_floor`. Only
`manifest.schema == 3` and `manifest.abiVersion == 2` are fixed in code. The
expected fields, including `scrubProfile`, must all match the pin verbatim. The
writer version must also fit the supported floor and independent reader ceiling.

### Load sequence (Node / Bun, `initScrubEngine`)

1. Parse the pin. Nothing is read if it is refused.
2. Read the wasm, glue and loader bytes (base: absolute `SCRUB_BINDGEN_DIR`, else
   `packages/pipeline/scrub-bindgen/`), copy each into a private snapshot, and hash the
   snapshot. Any mismatch is `bindgen_sha256_mismatch_<name>`; a missing or empty file is
   `bindgen_artifact_missing_<name>`.
3. Import the glue **from the verified snapshot** as a `data:text/javascript;base64` URL.
   The glue file is never imported by path, so it cannot change between hash and
   evaluation. The glue resolves nothing relative to itself (a relative import fails closed).
4. The loader is hashed but not evaluated. Its relative import of the glue would either
   fail from a `data:` URL or re-read the glue from disk after hashing.
5. `await init({ module_or_path: <verified wasm snapshot> })`. It always gets bytes, never a
   path or URL: there is no network fetch and no fallback of any kind.
6. Compare `manifestJson()` with the pinned manifest, one field at a time
   (`bindgen_manifest_mismatch_<field>`). A field missing from either side is
   `bindgen_manifest_mismatch_extra_field`.

Concurrent `initScrubEngine` calls share one initialisation (the promise is stored
synchronously, before the first await). A failure is sticky. A later init with a different
pin, or with the same pin but different bytes, throws `bindgen_init_conflict`. The glue
module stays private to the engine closure.

### Scrub behaviour

- `scrubJson` sends `JSON.stringify(value)` and returns `JSON.parse(jsonText)` from the
  glue's envelope. It never uses the envelope's `value` view. `undefined` is returned
  without an engine call, because it has no JSON text; an already poisoned instance
  still refuses it.
- A thrown glue error discards the input. Only a recognized tagged call refusal
  preserves the instance. Every unknown throw, trap, runtime/OOM error or malformed
  result poisons it; see the candidate contract below. JS error classes alone never
  establish that a refusal was deliberate.
- `scrubRecords` refuses invalid UTF-8 before any glue call (`chunk_not_utf8`).
  Lone surrogates are refused before the engine call
  (`scrub_input_not_well_formed`).
- The marker is `{ruleset, version, profile}` from the verified manifest (see "Persisted
  marker rule"). `sealRawChunk` is still refused (`seal_path_not_routed`).

### Workers (`createCompiledBindgenEngine`, not wired)

Workers cannot compile bytes, so `createCompiledBindgenEngine({glue, module, pin})` takes the
statically imported glue and a `CompiledWasm` `WebAssembly.Module`, calls
`initSync({ module })`, and checks the manifest in the same way. It cannot hash the module, so
the Worker build must guarantee that the bundled files are the pinned ones. It refuses with
`bindgen_path_conflict` once the Node loader path has started (and vice versa). No Worker
entrypoint calls it yet. With `SCRUB_ENGINE=wasm-bindgen` the hosted Worker's
`initScrubEngine` refuses, so the Worker answers `503 not_configured` as it does for `wasm`.

### Blockers before a real pin (relayscrubber-lead 245870894909468672)

Merging this PR is a default-off, unpinned scaffold only. It is NOT a security PASS, artifact pin, cutover, image, marker or adoption clearance. The successor that binds a real package must satisfy all of these first:

1. **Reader-first.** A v10 writer may not start until every relevant reader accepts ruleset <= 10. Add a separate explicit `READER_MAX_RULESET=10`; pin parsing must refuse a manifest rulesetVersion above it and below the supported floor. Roll the reader ceiling out first with wasm-bindgen still off, then the v10 writer/pin. Do not overload the TS `SCRUB_RULESET_VERSION` as the reader ceiling. (Reader policy source implemented in #489; fleet rollout and relayscrubber-lead acceptance remain pending.)
2. **Profile is crate-owned.** The schema-2 manifest must report a canonical profile field/string; the pipeline pins and compares it verbatim. The format derived in this PR (`relayscrubber/<ruleset>/<ver>/<policy>/<sourceSha>`) is a placeholder and must be replaced by the crate's value before any pin. (Candidate schema-3 crate-owned profile comparison implemented in #489; exact-package acceptance remains pending.)
3. **Stable tagged glue errors.** relayscrubber must expose stable error tags. Recognised domain/admission errors refuse one call; `WebAssembly.RuntimeError`, `RangeError`/OOM, or ANY unrecognised throw poisons the instance. The current Error-vs-trap heuristic is removed before pin. (Explicit refusal-tag recognition and sticky unknown-error poisoning implemented in #489; package/transport acceptance remains pending.)
4. **Loader policy in the adapter.** Hashing `index.js` without executing it is package provenance only, never runtime security. The adapter may import the verified generated glue directly but must implement the reviewed loader policy itself, including the 16 MiB UTF-8 preflight before the bindgen copy. The registry package/archive checksum goes into the build receipt; component digests remain the runtime pins. (Adapter-side preflight implemented in #489, pending relayscrubber-lead acceptance; qualified package/provenance requirements remain open.)
5. **Workers.** CI/build must hash the exact wasm and glue source files against the pin before bundling, prove through the bundler metafile/module manifest that the CompiledWasm input is that exact path, and retain a durable bundle receipt mapping wasmSha/glueSha/sourceSha to the deployment bundle digest. Startup still verifies the instance manifest/profile. Mutated-wasm and swapped-module build tests must fail.

Additional cutover requirements: adapter-side `MAX_TEXT_BYTES` = 16 MiB UTF-8 for text and JSON before any WASM call; records 16 MiB each and 64 MiB aggregate; JS preflight plus Rust recheck; limit-1 / limit / limit+1 multibyte tests; copy or reject shared mutable record buffers before invocation; route and re-pin every `sealRawChunk` call site; no TS or raw fallback. (Adapter-side admission implemented in #489, pending relayscrubber-lead acceptance; Rust recheck/package boundary proofs and seal-path routing remain open.)

Adapter tests with inert glue doubles do not qualify a Rust/WASM package.

### Adapter input admission

`bindgen-admission.ts` runs before every text, long-text, serialized-JSON or record call into the glue, on both the Node loader and compiled-module paths:

- Text and serialized JSON are limited to 16 MiB of UTF-8. Text is counted without first allocating an encoded copy. Lone UTF-16 surrogates are refused rather than replaced.
- NDJSON is limited to 16 MiB per physical LF-delimited record and 64 MiB per stream. LF is excluded from the record size but included in the stream size; CR and whitespace count toward the record size, matching Rust's check before blank-line handling.
- Shared record buffers are refused, including foreign-realm SharedArrayBuffers. Ordinary record buffers are copied into a private Uint8Array, so Buffer views cannot retain a mutable alias. UTF-8 is validated in bounded chunks; the original bytes are passed to Rust without decoding/re-encoding or JSON parsing.

Invalid-input errors are content-free, nonretryable `ScrubberError`s (a `PipelineError` subclass) with `disposition: "call_refusal"`: `scrub_input_too_large`, `scrub_input_not_well_formed`, `scrub_input_shared_buffer`, or the existing `chunk_not_utf8`. A local copy/decoder allocation failure instead returns retryable `scrub_admission_unavailable`, so resource pressure does not classify valid data as permanently malformed. These errors return no partial output and do not call or poison the engine. The checked-in pin remains UNPINNED; this admission layer changes neither recognition rules nor any reader ceiling or scrub marker.

### Candidate crate contract (not artifact clearance)

The candidate manifest reports schema `3`, ABI `2`, and a crate-owned
`scrubProfile` such as `relayhistory-secrets-v10`. The adapter compares all nine
required fields verbatim with the pin and takes its marker profile from the
verified instance. It does not derive a substitute profile from the source hash.
The historical ruling above is preserved; this candidate schema supersedes its
schema-2 placeholder shape, pending security-owner acceptance.

A recognized Rust/WASM refusal is a primitive JSON string with exactly `code` and
`disposition: "call_refusal"`, plus an optional positive, one-based safe-integer
`record`. Supported codes are `text_too_large`, `record_too_large`,
`stream_too_large`, `json_too_deep`, `invalid_json`, `invalid_utf8`, and
`serialization_error`. It becomes a content-free, nonretryable `ScrubberError`
with those stable fields. It returns no output, and the next independent call may
proceed. Extra fields, unknown codes/dispositions, invalid record numbers, wrapped
Error messages and unrecognized thrown shapes are not trusted refusal tags.

An unknown exception or malformed result produces `ScrubberError` with
`code: "scrubber_engine_failure"` and `disposition: "instance_poison"`. Every
later scrub call, including JSON `undefined`, and repeat initialization returns
`scrubber_instance_poisoned` without entering the glue again. Foreign exception
text never reaches the error message. Local preflight refusals remain separate
from glue failures and do not poison an untouched instance; local admission
allocation failures retain their retryable classification.

These guards use inert glue tests on both loader paths. No source/WASM/package
pin is qualified, no runtime is enabled, and the sealed-write routing and Worker
bundle qualification requirements remain open.

## Import cycle (why `predicate-hook.ts`)

`pipeline/errors.ts` imports `pipeline/trace.ts`. The seam's error classes extend `PipelineError`.
A static import of the seam from `trace.ts` would form the cycle
`errors.ts -> trace.ts -> seam -> errors.ts`, which leaves `PipelineError` uninitialised.

`trace.ts` therefore calls `predicateScrubText` from `predicate-hook.ts`, a module with no
dependency on the seam. The seam installs the hook at load:

- Under `ts`, the hook is exactly `scrubText`.
- Under `wasm`, the hook is the engine's `scrubText`. An unavailable or failed engine returns
  `undefined`, so `isSafeIdentifier` is false and the id is dropped.
- Before the seam loads, the hook reads `SCRUB_ENGINE` the same way: unset, `""` or `ts` uses
  `scrubText`, and anything else returns `undefined`.

## Known non-seam uses of scrub.ts / raw-chunks.ts (allowed, documented)

These neither produce persisted scrubbed bytes nor return input text. They are size and
identifier predicates, constants, or additive post-passes:

- `oversize.ts`, `delivery/prepare.ts`: `exceedsScrubBound`, `MAX_SCRUB_CHARS`. These are length
  predicates.
- `neon-reader.ts`, `technical-foul-manifest.ts`: `MAX_SCRUB_CHARS`.
- `vendor.ts` re-exports `canScrubLong` (raw-chunks). It is a feasibility predicate used by the
  migration reader, and it runs TS segmentation checks but returns only a boolean.
- `owner-buffered.ts` `redactQuotedTail` uses `isSecretKeyName` from scrub.ts as an additive
  post-pass over engine output: the v4 quoted-tail rule. It can only add redactions. Drop it once
  the engine implements v4.
- `claim-records-archive.ts`: `rawZstdFrame` (compression, no scrubbing).
- `alleyoop/manifests.ts`: the reader-side `SCRUB_RULESET_VERSION` check (unchanged).
- `delivery/prepare.ts` `scrubStructured` walks structured evidence and calls the seam's
  `scrubText` / `scrubNumber` per key and per string. That is pre-existing structure, with no
  secret-context logic of its own.

## Reachability matrix

Derived from the Bun.build metafile import graph of each entry point named in
`docker/entrypoint.sh` (the services image), plus the hosted entry points, for the seam as built
in this change. Every self-host `SERVICE=` component imports the seam (`scrub-engine/index.ts`)
and calls `initScrubEngine()` before serving.

| Entry point (file:function) | Self-host services image (SERVICE=) | Hosted-only |
|---|---|---|
| `src/shared/log.ts:log.*` (field scrubbing via `scrubTextForLog`) | yes: all 16 except `session-erase-bytes`, which logs without scrub | also Worker / Lambdas |
| `packages/pipeline/src/lib/pipeline/trace.ts:isSafeIdentifier` (via `predicate-hook.ts`) | yes: all 16 | also Worker / Lambdas |
| `packages/pipeline/src/routes/pipeline.ts` (`sealRawChunk` on raw ingest) | yes: every component except `hoopdb-migrate` and `acquisition` | also Worker / pipeline Lambda |
| `src/session-ingest/staging.ts:scrubRecords` | yes: `session-ingest`, `session-read-api`, `session-pipeline-consumer`, `-consumer-bootstrap`, `-output-facts-bootstrap`, `session-publication` | also Worker / Lambdas |
| `src/session-ingest/async-admission.ts:scrubRecords` | yes: `session-ingest` | also Worker |
| `src/session-ingest/babysitter-retention.ts:scrubRecords` | yes: `session-ingest` | also Worker |
| `src/session-ingest/replica.ts:scrubRecords` | yes: `session-ingest` | also Worker |
| `src/session-ingest/vendor/delivery/prepare.ts:scrubText/scrubNumber` | yes: `session-ingest` | also Worker |
| `src/session-ingest/writer.ts:sealRawChunk` | yes: `session-ingest`, `session-read-api`, `session-pipeline-consumer`, `-consumer-bootstrap`, `-output-facts-bootstrap`, `session-publication` | also Worker / Lambdas |
| `src/session-ingest/publication-image.ts:sealRawChunk` | yes: `session-publication` | no |
| `src/session-read-api/owner-buffered.ts:scrubText/scrubJson` | yes: `session-ingest`, `session-read-api` | also Worker / read-API Lambda |
| `src/session-read-api/digest-narrative-selfhost.ts:scrubText` (via vendor) | yes: `session-read-api` | also read-API Lambda |
| `src/hoopdb/session-directory-summary.ts:writeShortSummary` | yes: `hoopdb-indexer`, `bob-costas`, `session-brief`, `session-pipeline-consumer`, `-consumer-bootstrap`, `-output-facts-bootstrap` | also pipeline Lambda / bob-costas handler |
| `src/bob-costas/summarizer.ts`, `model-summarizer.ts:scrubText` (via vendor) | yes: `bob-costas` | also bob-costas handler (`summarizer.ts`) |
| `src/migration/neon-s3-sync/export-session.ts:sealRawChunk` (via vendor) | no | yes: migration tool |
| `src/proof/aws/*`, `scripts/opensearch-proof/e2e.ts` | no | yes: proof tooling |

### Per self-host row, with SCRUB_ENGINE=wasm

- **log.ts**: `scrubTextForLog` returns the engine's output, or `[scrub-unavailable]`, and never
  the input or a TS result (`index.ts` `scrubTextForLog`, `activeScrubEngine`).
- **trace.ts isSafeIdentifier**: the hook installed by `index.ts` calls the wasm engine only. If
  the engine is unavailable or fails, the id is dropped. `predicate-hook.ts` never uses TS unless
  `ts` is selected.
- **routes/pipeline.ts sealRawChunk**: it imports the seam's `sealRawChunk`, which throws
  `ScrubEngineUnavailable` / `seal_path_not_routed` under wasm before raw-chunks is reached
  (`index.ts` `sealRawChunk`). So no TS scrub and no raw bytes are stored.
- **staging.ts scrubRecords**: the seam's `scrubRecords` sends raw chunk bytes to the wasm engine
  or throws (`index.ts` `scrubRecords` -> `activeScrubEngine`). Raw bytes are never returned on
  refusal.
- **async-admission.ts scrubRecords**: same seam call. A failure throws, and admission is retried,
  never stored raw.
- **babysitter-retention.ts scrubRecords**: same seam call. It throws before anything is written.
- **replica.ts scrubRecords**: same seam call. It throws before the replica write.
- **delivery/prepare.ts**: `scrubText` and `scrubNumber` are seam accessors (wasm or throw). The
  `exceedsScrubBound` predicate only sets a flag.
- **writer.ts sealRawChunk**: the seam's `sealRawChunk` refuses under wasm (`index.ts`). No TS
  path and no raw write.
- **publication-image.ts sealRawChunk**: the same seam refusal.
- **owner-buffered.ts**: `scrubText` and `scrubJson` are seam accessors (wasm or throw). The
  `isSecretKeyName` post-pass only adds redactions to engine output.
- **digest-narrative-selfhost.ts**: `scrubText` is re-exported by `vendor.ts` from the seam (wasm
  or throw).
- **session-directory-summary.ts writeShortSummary**: it takes `activeScrubEngine()` once (wasm or
  throw). `ScrubEngineUnavailable` propagates instead of becoming `scrub_failed`, so an older
  summary is not cleared. The stamped version is that engine's `rulesetVersion`.
- **bob-costas summarizers**: `scrubText` comes from `vendor.ts`, and so from the seam (wasm or
  throw).

Under wasm, `sealRawChunk` is refused on every self-host path that seals raw chunks. So
`SCRUB_ENGINE=wasm` is not deployable for `session-ingest`, `session-read-api`, the consumers or
`session-publication` until the raw-chunks re-pin follow-up lands. This is deliberate: refusing
is the fail-closed outcome.


### Explicit reader ruleset capability

`READER_MAX_RULESET` selects a read capability independently of the TypeScript writer stamp. Unset preserves main's existing reads: existing marker-presence/quarantine checks remain, with no new family, ceiling, or per-chunk metadata checks. Explicitly setting exactly `"8"`, `"9"` or `"10"` enables the stricter policy. Empty, padded, signed, fractional and higher values refuse configuration. Raising the ceiling does not enable a Rust engine, qualify any package or change TypeScript scrub markers.

Canonical record/transcript readers, export readers, the indexing consumer, brief input and manifest/chunk admission use this policy only when explicitly configured. Unknown families and versions outside 1 through the configured maximum refuse before record parsing. A manifest records the **lowest** chunk ruleset, so per-chunk metadata is also checked. Readers bind that metadata to the same GET response as the body and cancel refused bodies before consuming them. Historical metadata-free chunks at versions through 8 keep their existing read contract; newer manifest versions require per-chunk scrub metadata. A declared chunk version cannot fall below the manifest minimum.

Node services validate the setting at startup and thread it into canonical reads. Alleyoop also forwards it through its real app bindings. Canonical and timing Worker routes validate their own bindings rather than consulting a Node process environment. Offline seed readers receive an explicit optional policy in their dependencies. An unset policy does not add a marker check to any of these paths. The wasm-bindgen pin parser retains a separate default writer ceiling of 8 and refuses a writer above its explicitly configured ceiling before reading artifacts or initializing glue. Compiled-module callers must pass their verified reader ceiling explicitly when it exceeds the default. The raw ABI path remains unpinned and outside this first cutover.

Operationally, deploy and verify every relevant reader with ceiling 10 **while the Rust engine remains off**, then qualify and enable the matching writer. Configuration in one process does not prove that a remote fleet has been upgraded. Keep the reader ceiling at 10 on rollback while version-10 objects remain stored; reverting a writer does not downgrade existing objects. Existing artifact-page formats do not change. This source change does not satisfy the remaining package safety, provenance, error-tagging, Worker build-binding or persistence-routing gates.


### Configuration failure runbook

Leave `READER_MAX_RULESET` absent to preserve legacy reads. An explicit value
opts into strict marker and chunk checks, including canonical decimal metadata;
older metadata such as `"08"` or `"8.0"` is accepted by the legacy courier path
but refused by the explicit policy. Explicit policy also checks zero-chunk
manifest markers. Verify existing retained objects before opting in.

Empty, malformed, padded, unsupported or number-typed Worker bindings are
configuration errors. In ingest, engine initialization fails closed and
`scrubEngineReady` stays false: sealing does not continue through TS or another
engine. Correct or remove the malformed binding and restart through the normal
operator process. Raising a reader ceiling never qualifies a package or a writer.
