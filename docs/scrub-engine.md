# Scrub engine seam (relayscrubber wasm-bindgen by default, TypeScript on hosted entries)

Status: the DEFAULT engine is the pinned relayscrubber `0.5.0-alpha.9` wasm-bindgen package,
vendored byte-for-byte under `packages/pipeline/scrub-bindgen/` (ruleset `relayhistory-scrub`
version 10, profile `relayhistory-secrets-v10`). With `SCRUB_ENGINE` unset or `""` a process
selects it and fails closed if it cannot verify it (see "Default flip"). The TypeScript scrubber
(`packages/pipeline/src/lib/scrub.ts`, ruleset version 8, plus `scrubRecords`/`scrubLong`/`sealRawChunk`
in `packages/pipeline/src/lib/pipeline/raw-chunks.ts`) runs only in the enumerated hosted Worker /
Lambda handlers and the test preloads, which hold a code-level grant. The raw-ABI pin
(`scrub-wasm-pin.json`) is still a placeholder that refuses to load.

Code: `packages/pipeline/src/lib/scrub-engine/`

| File | Role |
|---|---|
| `engine.ts` | `ScrubEngine` interface, `ScrubEngineUnavailable` / `ScrubEngineConfigError`, the frozen `tsScrubEngine` |
| `wasm-abi.ts` | The only file that knows the wasm export surface (ABI version 1) |
| `wasm-engine.ts` | Pin parsing, artifact verification, `WasmScrubEngine` |
| `index.ts` | `selectScrubEngine`, `initScrubEngine`, `activeScrubEngine`, the sync call-site accessors |
| `predicate-hook.ts` | Late-bound `scrubText` for `trace.ts` `isSafeIdentifier` (import-cycle break, see below) |
| `ts-capability.ts` | The non-env TS grant (`declareScrubTsCapability`); see "Default flip" |
| `packages/pipeline/scrub-wasm-pin.json` | The pin |
| `bindgen-engine.ts` | `SCRUB_ENGINE=wasm-bindgen`: pin parsing, verified glue import, manifest check, `createCompiledBindgenEngine` (see "wasm-bindgen path") |
| `packages/pipeline/scrub-bindgen-pin.json` | The wasm-bindgen pin (relayscrubber `0.5.0-alpha.9`) |
| `packages/pipeline/scrub-bindgen/` | The vendored package bytes the pin names (data; `.gitattributes` `-text`) |
| `packages/pipeline/technical-foul-cutover.json` | The cutover provenance entry (separate from `technical-foul.json`) |

Tests: `packages/pipeline/test/scrub-engine-parity.test.ts`, `packages/pipeline/test/scrub-engine-bindgen.test.ts`.

## Selection

`SCRUB_ENGINE` selects the engine for the whole process (or Worker isolate):

| Value | Engine |
|---|---|
| unset or `""`, no TS grant (every Node / Bun self-host process and tool) | The pinned `@agentworkforce/relayscrubber` wasm-bindgen package, exactly as `wasm-bindgen`. Needs `initScrubEngine()`; fails closed. |
| unset or `""`, TS grant (hosted Worker / Lambda handler, test preload) | TypeScript, byte-identical to main. No I/O, nothing to initialise. |
| `ts`, TS grant | TypeScript. |
| `ts`, no TS grant | `ScrubEngineConfigError` (`scrub_engine_invalid`, reason `scrub_engine_ts_not_allowed`), latched for the process. |
| `wasm` | The pinned relayscrubber artifact only (raw ABI v1). |
| `wasm-bindgen` | The pinned `@agentworkforce/relayscrubber` wasm-bindgen package only. See "wasm-bindgen path". |
| anything else | `ScrubEngineConfigError` (`scrub_engine_invalid`): startup fails. |

The grant and the entry points that hold it are in "Default flip".

The selection is made once. A second, conflicting selection in the same process throws.
`""` is treated as unset because Compose and Helm emit it for an unset variable.

### Required engine (`SCRUB_ENGINE_REQUIRED`, default off)

`SCRUB_ENGINE_REQUIRED` is an operator assertion about `SCRUB_ENGINE`. It selects nothing on its
own, with one exception: `SCRUB_ENGINE_REQUIRED=wasm-bindgen` overrides a TS grant, so unset
`SCRUB_ENGINE` then resolves to wasm-bindgen even in a granted process. With `SCRUB_ENGINE` unset
and no grant it passes (the default already resolves to wasm-bindgen).

| `SCRUB_ENGINE_REQUIRED` | Effect |
|---|---|
| unset or `""` | No check. |
| `ts`, `wasm` or `wasm-bindgen` | The resolved `SCRUB_ENGINE` kind (and any selection already made in the process) must equal it. |
| anything else | `ScrubEngineConfigError` at selection; this does not latch. |

A mismatch throws `ScrubEngineConfigError` with `reason: "engine_selection_required"`, sets
`lastFailure` to `engine_selection_required` and **latches**: every later
`selectScrubEngine` / `initScrubEngine` / `activeScrubEngine` call in that process refuses the
same way, so `scrubText`, `scrubRecords`, `activeScrubMarker` and sealing refuse,
`scrubEngineReady()` is false (ingest `/readyz` stays not-ready), `scrubTextForLog` returns
`[scrub-unavailable]` and `isSafeIdentifier` is false. Nothing falls back to TypeScript. A
matching requirement adds nothing: with `wasm-bindgen` the existing pin and artifact refusals
(for example `bindgen_ruleset_above_reader_ceiling` under an explicit `READER_MAX_RULESET` of 8 or 9, or
`bindgen_sha256_mismatch_wasm`) still apply unchanged, before any glue import. In addition, with
`SCRUB_ENGINE_REQUIRED=wasm-bindgen` the active instance must have been verified against the
checked-in pin exactly (`bindgen_pin_not_exact` otherwise; a reordered or edited pin object is not exact). Only
`resetScrubEngineForTests()` clears the latch. Deploy bindings for this variable are not added.

Every self-host `SERVICE=` entry point, and every Node tool, proof, migration or ops script that
scrubs or seals, calls `await initScrubEngine()` before it serves or runs its job
(`src/shared/worker.ts` `runWorker` covers the indexer, referee, bob-costas and session-brief).
`src/cli/main.ts` initialises per subcommand, in the subcommands that scrub or seal (`seed` and
`smoke`), not once at startup.
`scripts/scrub-default-scan.ts` enforces this (see "Default flip"). Under the default this
verifies the wasm-bindgen pin and vendored bytes; with `wasm` the raw-ABI pin and module. On
failure the process exits non-zero rather than start. The ingest server's `/readyz` also requires
`scrubEngineReady()`.

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
  a writer ruleset below 8 or above its effective ceiling (`effectiveReaderMaxRuleset` in
  `reader-policy.ts`: an explicit `READER_MAX_RULESET` first; unset is 10 on self-host and 8 for
  the hosted ingest Worker).

## Cutover rule

1. In a process holding a TS grant with `SCRUB_ENGINE` unset or `ts`, all output is byte-identical to main
   (the hosted entries; Node self-host defaults to wasm-bindgen since "Default flip"). `technical-foul.json`
   still pins the bytes of `scrub.ts` and the raw-chunks segmenter, and this change did not alter them.
2. **The first deployable cutover makes the pinned wasm mandatory.** The services image and Worker
   ship with `SCRUB_ENGINE=wasm` and a real pin. From then on, `scrub.ts` is the **frozen oracle**:
   it is used only by the parity test, and is not called by production paths. There is no
   per-request or per-service mixing and no automatic fallback.
3. Before that cutover, `sealRawChunk` must be routed. Today the seam's `sealRawChunk` delegates to
   raw-chunks under `ts`, and refuses under `wasm` (`seal_path_not_routed`), because raw-chunks
   scrubs internally and its bytes are pinned. The follow-up re-pins `raw-chunks.ts` so that
   `sealRawChunk` takes the engine as a parameter. It needs a technical-foul re-pin with review.
   (Done differently for the alpha.4 cutover: `raw-chunks.ts` stays sealed and unchanged, and the
   bound sealer in `engine-raw-chunks.ts` is injected per call site; see "Cutover: pinned relayscrubber 0.5.0-alpha.9".)

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
  - The Worker declares the `hosted-worker` TS grant at handler entry ("Default flip"). With
    `SCRUB_ENGINE` unset, `initScrubEngine` then returns `tsScrubEngine` with no I/O. The
    Worker's readiness check is always true.
  - Module evaluation does not touch `import.meta.url`. The wasm file path is resolved only
    inside the Node/Bun loader.
  - Every call site gets the same TS functions as before.
  - `worker-bundle.test.ts` and the existing Worker tests pass unchanged.
- The Lambda handlers (read API, pipeline incl. the bob-costas stage, upload admission, buffered
  GC, rearm sweeper) declare the `hosted-lambda` TS grant at handler entry and do not call
  `initScrubEngine`. Unset or `ts` then resolves lazily to TypeScript, as before. Under `wasm` they
  would refuse with `not_initialised`, which is fail-closed. They are hosted-only and out of scope
  for the self-host cutover.

## wasm-bindgen path (`SCRUB_ENGINE=wasm-bindgen`, the default)

A third engine for the relayscrubber package as published by wasm-bindgen: generated
`relayscrubber_bg.wasm`, the generated JS glue `relayscrubber.js`, and the package
loader `index.js`. It is independent of the raw ABI v1 path. `wasm` and `wasm-bindgen`
are mutually exclusive in one process (a conflicting selection throws), and a bindgen
init never consults the raw-ABI loader. Raw ABI v1 stays in the tree, but the first
cutover is not planned to use it.

`packages/pipeline/scrub-bindgen-pin.json` pins relayscrubber `0.5.0-alpha.9` (see "Cutover:
pinned relayscrubber 0.5.0-alpha.9" for the values, the env triple and what refuses when).
Selecting `wasm-bindgen` under an explicit `READER_MAX_RULESET` of 8 or 9 (or on the hosted ingest
Worker, whose unset ceiling stays 8) makes `initScrubEngine` throw
`ScrubEngineUnavailable(bindgen_ruleset_above_reader_ceiling)`, and every call site then
refuses as described in "Fail-closed rules".

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
  marker rule"). The seam's `sealRawChunk` is still refused (`seal_path_not_routed`); raw sealing
  goes only through the bound sealer from `routedRawChunkSealer()` (see "Cutover").

### Workers (`createCompiledBindgenEngine`, not wired, excluded from the cutover)

Workers cannot compile bytes, so `createCompiledBindgenEngine({glue, module, pin})` takes the
statically imported glue and a `CompiledWasm` `WebAssembly.Module`, calls
`initSync({ module })`, and checks the manifest in the same way. It cannot hash the module, so
the Worker build must guarantee that the bundled files are the pinned ones. It refuses with
`bindgen_path_conflict` once the Node loader path has started (and vice versa). No Worker
entrypoint calls it. With `SCRUB_ENGINE=wasm-bindgen` the hosted Worker's
`initScrubEngine` refuses (explicitly, via `readWorkerBindgenArtifact` in
`src/session-ingest/worker.ts`), so the Worker answers `503 not_configured` as it does for `wasm`.

### Blockers before a real pin (relayscrubber-lead 245870894909468672)

(Historical record from the scaffold PR; the status of each item for the alpha.9 cutover is in "Cutover: pinned relayscrubber 0.5.0-alpha.9".) Merging this PR is a default-off, unpinned scaffold only. It is NOT a security PASS, artifact pin, cutover, image, marker or adoption clearance. The successor that binds a real package must satisfy all of these first:

1. **Reader-first.** A v10 writer may not start until every relevant reader accepts ruleset <= 10. Add a separate explicit `READER_MAX_RULESET=10`; pin parsing must refuse a manifest rulesetVersion above it and below the supported floor. Roll the reader ceiling out first with wasm-bindgen still off, then the v10 writer/pin. Do not overload the TS `SCRUB_RULESET_VERSION` as the reader ceiling. (Reader policy implemented in #489. The alpha.9 pin is version 10 and refuses without `READER_MAX_RULESET=10`; see "Cutover" and "Reader-first rollout". Fleet rollout of the ceiling is an operational step, not a source change.)
2. **Profile is crate-owned.** The schema-2 manifest must report a canonical profile field/string; the pipeline pins and compares it verbatim. The format derived in this PR (`relayscrubber/<ruleset>/<ver>/<policy>/<sourceSha>`) is a placeholder and must be replaced by the crate's value before any pin. (Schema-3 crate-owned profile comparison implemented in #489; the alpha.9 pin compares `relayhistory-secrets-v10` verbatim, see "Cutover".)
3. **Stable tagged glue errors.** relayscrubber must expose stable error tags. Recognised domain/admission errors refuse one call; `WebAssembly.RuntimeError`, `RangeError`/OOM, or ANY unrecognised throw poisons the instance. The current Error-vs-trap heuristic is removed before pin. (Refusal-tag recognition and sticky unknown-error poisoning implemented in #489; the fault matrix runs against the real alpha.9 module in `packages/pipeline/test/scrub-bindgen-faults.test.ts`.)
4. **Loader policy in the adapter.** Hashing `index.js` without executing it is package provenance only, never runtime security. The adapter may import the verified generated glue directly but must implement the reviewed loader policy itself, including the 16 MiB UTF-8 preflight before the bindgen copy. The registry package/archive checksum goes into the build receipt; component digests remain the runtime pins. (Adapter-side preflight implemented in #489. Release provenance is implemented for alpha.9: the pin's `release` block records the GitHub release tarball sha256, every receipt emits it, and the `--tarball` check proves the vendored bytes equal the tarball's; see "Cutover" / "Provenance checks".)
5. **Workers.** CI/build must hash the exact wasm and glue source files against the pin before bundling, prove through the bundler metafile/module manifest that the CompiledWasm input is that exact path, and retain a durable bundle receipt mapping wasmSha/glueSha/sourceSha to the deployment bundle digest. Startup still verifies the instance manifest/profile. Mutated-wasm and swapped-module build tests must fail.

Additional cutover requirements: adapter-side `MAX_TEXT_BYTES` = 16 MiB UTF-8 for text and JSON before any WASM call; records 16 MiB each and 64 MiB aggregate; JS preflight plus Rust recheck; limit-1 / limit / limit+1 multibyte tests; copy or reject shared mutable record buffers before invocation; route and re-pin every `sealRawChunk` call site; no TS or raw fallback. (Adapter-side admission implemented in #489. The Rust recheck and limit-1 / limit / limit+1 boundary tests run against the real alpha.9 module in `packages/pipeline/test/scrub-bindgen-limits.test.ts`. Seal-path routing is implemented, see "Cutover" / "Routing"; unwired call sites refuse with `seal_path_not_routed`. Workers remain excluded.)

Adapter tests with inert glue doubles do not qualify a Rust/WASM package.

### Adapter input admission

`bindgen-admission.ts` runs before every text, long-text, serialized-JSON or record call into the glue, on both the Node loader and compiled-module paths:

- Text and serialized JSON are limited to 16 MiB of UTF-8. Text is counted without first allocating an encoded copy. Lone UTF-16 surrogates are refused rather than replaced.
- NDJSON is limited to 16 MiB per physical LF-delimited record and 64 MiB per stream. LF is excluded from the record size but included in the stream size; CR and whitespace count toward the record size. This is the intended Rust physical-byte contract before blank-line handling; adapter tests alone do not prove package conformance.
- Shared record buffers are refused, including foreign-realm SharedArrayBuffers. Ordinary record buffers are copied into a private Uint8Array, so Buffer views cannot retain a mutable alias. UTF-8 is validated in bounded chunks; the original bytes are passed to Rust without decoding/re-encoding or JSON parsing.

Invalid-input errors are content-free, nonretryable `ScrubberError`s (a `PipelineError` subclass) with `disposition: "call_refusal"`: `scrub_input_too_large`, `scrub_input_not_well_formed`, `scrub_input_shared_buffer`, or the existing `chunk_not_utf8`. A local copy/decoder allocation failure instead returns retryable `scrub_admission_unavailable`, so resource pressure does not classify valid data as permanently malformed. These errors return no partial output and do not call or poison the engine. This admission layer changes neither recognition rules nor any reader ceiling or scrub marker. The limits are exercised against the real alpha.9 module at limit-1 / limit / limit+1 with multibyte boundaries in `packages/pipeline/test/scrub-bindgen-limits.test.ts`.

The adapter requires the ABI-2 `scrubTextChecked` export for both text and long
text after JS admission. That Rust entrypoint rechecks the byte limit before
allocating scanner state. Missing it refuses startup even if the unchecked
`scrubText`/`scrubLong` compatibility exports exist; there is no fallback to them.

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

These guards use inert glue tests on both loader paths, and the fault matrix is
repeated against the real alpha.9 module through an injecting wrapper (never by
editing vendored bytes) in `packages/pipeline/test/scrub-bindgen-faults.test.ts`.
Worker bundle qualification remains open (Workers are excluded from the cutover).

## Cutover: pinned relayscrubber 0.5.0-alpha.9

The default since "Default flip" (below): with `SCRUB_ENGINE` unset or `""` every Node / Bun
process runs this engine. Only a process holding a TS grant (hosted Worker / Lambda handlers, test
preloads) keeps the TS engine, the TS `sealRawChunk` and ruleset version 8; there
`routedRawChunkSealerDeps()` returns `{}` so every constructor's object is unchanged.

### Pin (`packages/pipeline/scrub-bindgen-pin.json`)

| Field | Value |
|---|---|
| `package` / `package_version` | `@agentworkforce/relayscrubber` / `0.5.0-alpha.9` |
| `wasm_sha256` (`generated/relayscrubber_bg.wasm`) | `4228d2cd50334a6b714dcbd8cfc89220fdf36b1c903d170f873ad9b738fee090` |
| `glue_sha256` (`generated/relayscrubber.js`) | `55dd6fc6a44ed35f846655ab08bf3161ffe105123cbe11d3e529923d6bdeae5b` |
| `loader_sha256` (`index.js`, hashed, never evaluated) | `ff139ab391a958475b455ef483e7119d66215d172647223965dc35348f912490` |
| `scrub_profile` | `relayhistory-secrets-v10` |
| `manifest` | schema 3, `relayhistory-scrub`, rulesetVersion 10, rulesetSourceSha256 `7837062214ff56145c72999880179598244cff5f80554a4b697548920561c143`, crateVersion `0.5.0-alpha.9`, abiVersion 2, policy `secrets`, redactionToken `[REDACTED]`, scrubProfile `relayhistory-secrets-v10` |

The release's `generated/artifact.json` (sha256 `3134f3b65f93fba7b050d47e243af2639f3c9d4b7ebfd863a7ad1f9d1e3fdec5`,
toolchain Rust 1.88.0, wasm-bindgen 0.2.104) and the `.d.ts` files are vendored for provenance.
`packages/pipeline/scrub-bindgen/.gitattributes` (`* -text`) keeps git from normalising the bytes.
The vendored directory reaches the services image through `COPY packages packages`.

Pin history: the cutover first shipped with `0.5.0-alpha.4` (wasm
`bd181f94e2d03d7a5381a31488aa5ffbc991e4d41132c7f8f08095031608b5da`, rulesetSourceSha256
`959b6e2894600ec66381ae56cb9bc144d2814d0f27662ca1453e89491c41ceeb`, release tarball
`c0665dc5668bdefd5636656c3a7358273be09a4b8d1e086b99641943e3569641`), then moved to `0.5.0-alpha.5`
(wasm `91cf23177f1d9fa42484f4ce76b54bdd468177852b9e8b259af0b72ad5823f4d`, rulesetSourceSha256
`bd7d6620626ef40b4c319fa6260bb42ddb20035301ad974332a13453835563f5`, `artifact.json`
`4ba0f5210995e8db20ba361235ed43ceb777309ac946567885a8805f286f0010`, release tarball
`bc4101df467ba520e16b8b8d766c63a9017631a6551a369ee100b34e47de441b`), then to `0.5.0-alpha.6`
(tag `v0.5.0-alpha.6` -> relayscrubber `1c667369`, wasm
`f1d393b22ce47d4cb4ca762107a7f66e8b2747f6de60c193e8b07646db28e5fd`, rulesetSourceSha256
`aea569aaeca8a53190194eb10cb751195d3d21c27cd0fee2aeef78f81db3d814`, `artifact.json`
`43a896799549358ed0abb5fd6fab9395ca9455bd591e2f166a4e46b34c759b93`, release tarball
`374b72f5d21ecaae558273bb350103c2d111cdbbb1eaf803615c221449066cc6`, crates.io checksum
`26bbc39f73bf1bcfa7044c1ed1edfeec0848e09b940da65071816d2abce91307`). alpha.6 (and alpha.7, never
pinned here) preserved bare credential reassignments such as `password = API_TOKEN;`; `0.5.0-alpha.8`
(tag `v0.5.0-alpha.8` -> relayscrubber `693f3809`, wasm
`546cc6e7f9bf0d644608d84686dc59e1638045661cc43f2fb9255d29cc28c825`, rulesetSourceSha256
`653b5822768c7ef2ae51f74f52f3514389a7fcd019cf5d92eb5fb6c173d01714`, `artifact.json`
`588d8ec32ced45f40da1ae88a95c7e3b7674841d33f29e29734eeedc80e56bb0`, release tarball
`7f08abcda6ee1e463a2414878131433631de869125c8f9153ed8c9ded555f099`, crates.io checksum
`7f79b62f5fc4b52a738cb86e428c1a69b7f55ccb7258ffa16ebced7eb7beebc6`) fixes that recall regression.
`0.5.0-alpha.9` (tag `v0.5.0-alpha.9` -> relayscrubber `934e7b67`, crates.io checksum
`6a97cd235bb6ded9f15f9f100e37627ee3aeef2f73e3c32c13f4a0e913d8f1e6`) keeps an exact `project_key`
whose value is a normalized repository identity (`github.com/owner/repo`), which alpha.8 redacted
as a generic `*_key`; capture's project tag then reaches the session directory. It was released on
Khaliq's authorization without a relayscrubber-lead exact-head PASS. Each step changes only the wasm,
the ruleset source digest, the crate version and therefore `artifact.json`; rulesetVersion 10, the
`relayhistory-secrets-v10` profile, abiVersion 2, the glue, the loader and the `.d.ts` files are
byte-identical.

The pin (schema 2) also carries a `release` block: `source` `github-release`, `repository`
`AgentWorkforce/relayscrubber`, `tag` `v0.5.0-alpha.9`, `tarball`
`agentworkforce-relayscrubber-0.5.0-alpha.9.tgz`, `tarball_sha256`
`c3103a59edd24075c35d974c955aee1faf3fd6fb6eeabcd92aea338ac2448832`, and `sha256sums`
(`artifact.json` = the vendored digest above). `parseScrubBindgenPin` refuses a pin without a
well-formed block (`bindgen_unpinned_release`). The pin is read from disk on first use by
`loadScrubBindgenPin()` (failure `bindgen_pin_unreadable`), never imported as JSON, so its
digests and version-10 profile stay out of any bundle that imports the seam.

Provenance checks:
- `packages/pipeline/test/scrub-bindgen-pin-alpha9.test.ts` recomputes every vendored digest against
  the pin and `artifact.json`; mutated bytes and swapped modules fail closed.
- `packages/pipeline/test/scrub-bindgen-reassignment-alpha8.test.ts` drives the real vendored wasm
  through the pipeline's own binding (seam `scrubText` / `scrubLong` / `scrubJson` and the routed
  raw-chunk sealer) and asserts that bare sensitive reassignments (`password = API_TOKEN;`,
  `clientSecret = getSecret(input);`, `export API_KEY = cfg.apiKey;`) are redacted, while declarations
  and non-sensitive assignments stay byte-identical. alpha.6 returned those three inputs unchanged.
- `scripts/scrub-bindgen-receipt.ts` writes a receipt (pin digests, package version, crate manifest,
  toolchain, the pin's `release` block, timestamp, git HEAD) and exits non-zero on any mismatch. CI
  runs it in the `unit-tests-aux` job (part of the `unit-tests` aggregate) WITHOUT `--tarball` (no network in CI) and uploads
  `scrub-bindgen-receipt.json` as an artifact; that receipt's `release` has `verified: false`,
  `reason: "tarball not supplied"` by design. It still checks that `release.sha256sums["artifact.json"]`
  equals the vendored `artifact.json`.
- `--tarball <path>` (on the receipt, or standalone `bun scripts/scrub-bindgen-provenance.ts --tarball <path>`)
  is the reproducible provenance step, run at pin time with the release tarball downloaded separately
  and recorded in the PR. It checks the tarball sha256 against `release.tarball_sha256`, the
  `package/package.json` name and version against the pin, that every vendored file (except the
  repo-local `.gitattributes`) is byte-equal to its `package/` entry, and that the archive holds no
  other file. The archive is parsed in memory as data (links, devices, `..`/absolute paths, duplicates
  and bad checksums are refused); nothing is extracted or executed. Any mismatch gives `verified: false`
  with `mismatches`, and exit 1 (2 for a usage error such as a missing file). Tests:
  `packages/pipeline/test/scrub-bindgen-provenance.test.ts` (synthetic tarballs).
- `packages/pipeline/technical-foul-cutover.json` is a NEW technical-foul entry (pin digests, vendored
  file digests, the bindgen sealer sources, and a reference to the unchanged TS entry including the
  `raw-chunks.ts` digest). `technical-foul.json` (ruleset 8) is immutable history and is not
  relabelled. `bun run technical-foul:check` verifies both files.

### Env triple (AA self-host services image)

No binding is needed any more: unset `SCRUB_ENGINE`, unset `SCRUB_ENGINE_REQUIRED` and unset
`READER_MAX_RULESET` (self-host effective ceiling 10, `effectiveReaderMaxRuleset`) select this
engine. The former opt-in triple is still accepted and selects exactly the same engine:

```
SCRUB_ENGINE=wasm-bindgen
SCRUB_ENGINE_REQUIRED=wasm-bindgen   # optional pin: refuses any other kind, overrides a TS grant
READER_MAX_RULESET=10
```

`technical-foul-cutover.json` records this as `envTriple` with every value `null` (unset). Readers
accept version 10 by default (reader-first, see "Self-host default reader ceiling") before any
writer stamps it.

### Routing

When the resolved kind is wasm-bindgen (the default, or the explicit triple), `routedRawChunkSealer()`
(`scrub-engine/index.ts`) uses the resolved kind, never a raw env comparison, and returns a sealer bound with
`bindEngineRawSealer` to the exact verified instance `initScrubEngine` installed, cached per
instance. `routedRawChunkSealerDeps()` spreads it into each production constructor after
`initScrubEngine` has resolved:

| Call site | Production constructor |
|---|---|
| `src/session-ingest/writer.ts` (`target.rawChunkSealer`) | `src/session-ingest/selfhost-server.ts` `selfhostIngestRuntime` (`DeliverDeps.target`) |
| `src/session-ingest/publication-image.ts` | `publication-runtime.ts` / `publication-portable.ts` `canonicalPublicationTick` deps, forwarded by `publication-job.ts` |
| `src/migration/neon-s3-sync/export-session.ts` | `src/migration/neon-s3-sync/main.ts` `Syncer` target (initialises the engine first under `wasm-bindgen`) |
| `packages/pipeline/src/routes/pipeline.ts` | `src/alleyoop/ingest.ts` `createPipelineRoutes` |

Callers that reach the seam with no bound sealer are **unwired, refuses under the default**
(`seal_path_not_routed`, zero TS sealer calls, nothing stored):

| Unwired call site | Why |
|---|---|
| `src/session-ingest/publication-canary.ts` `publishBufferedStage` (no `rawChunkSealer`) | canary tooling, TS only |
| `packages/pipeline/src/routes/pipeline.ts` default `pipelineRoutes` / `pipelineOpsRoutes` / `defaultDeps`, mounted by `packages/pipeline/src/app.ts` | standalone pipeline app, not the self-host ingest |
| `src/session-ingest/worker.ts` `deliverDepsFor` and its write targets | Workers excluded (below) |

The seam's `sealRawChunk` keeps refusing non-TS engines (`seal_path_not_routed`), so any call site
that is not wired refuses instead of falling back. There is no TS or raw fallback on the selected path.
`src/session-ingest/__tests__/scrub-cutover-reachability.test.ts` drives all four wired paths end to end
and proves zero TS scrub/sealer calls with `SCRUB_ENGINE`, `SCRUB_ENGINE_REQUIRED` and
`READER_MAX_RULESET` unset and no TS grant (the default), with a positive control in a process
holding the hosted TS grant with the knob unset, and `SCRUB_ENGINE=ts` rows without a grant that
refuse with `scrub_engine_ts_not_allowed`. It also drives the three unwired callers under the
default and asserts each refuses with
`seal_path_not_routed` and stores nothing, and a source scan fails if any `?? sealRawChunk` fallback or
`DeliverDeps` / `PipelineRouteDeps` / publication-deps / write-target constructor is not classified as
wired or unwired. `scripts/scrub-cutover-reachability.ts` runs it and writes a JSON report.

### What refuses when

| Condition | Result |
|---|---|
| `SCRUB_ENGINE=wasm-bindgen`, `READER_MAX_RULESET` explicitly `8` or `9` (or unset on the hosted ingest Worker) | `bindgen_ruleset_above_reader_ceiling`, before any artifact read |
| A vendored file missing / empty | `bindgen_artifact_missing_<wasm|glue|loader>` |
| A vendored file's bytes differ from the pin | `bindgen_sha256_mismatch_<name>` |
| Relative `SCRUB_BINDGEN_DIR` | `bindgen_dir_invalid` |
| Pin file unreadable / not JSON | `bindgen_pin_unreadable` |
| Pin without a well-formed `release` block | `bindgen_unpinned_release` |
| Instance manifest differs from the pin | `bindgen_manifest_mismatch_<field>` / `bindgen_profile_mismatch` |
| `SCRUB_ENGINE_REQUIRED=wasm-bindgen` and the instance was verified against another pin | `bindgen_pin_not_exact` |
| `SCRUB_ENGINE_REQUIRED=wasm-bindgen` and `SCRUB_ENGINE` is anything else | `engine_selection_required` (latched) |
| `SCRUB_ENGINE=ts` in a process without a TS grant (any Node / Bun self-host process or tool) | `ScrubEngineConfigError` `scrub_engine_ts_not_allowed` (latched) |
| Scrub / log / seal call before `initScrubEngine` resolved (default) | scrub and seal throw `not_initialised`; `scrubTextForLog` returns `[scrub-unavailable]`; nothing latched |
| Recognised tagged call refusal from the crate | that one call refuses (nonretryable `ScrubberError`); the instance stays healthy |
| `WebAssembly.RuntimeError`, `RangeError`, or any unrecognised throw / malformed result | instance poisoned (sticky `scrubber_instance_poisoned`) |
| Blank-only input (Rust `White_Space`, incl. U+0085) | refused at admission, engine not called, not poisoned |
| Unwired call site | `seal_path_not_routed` |

For the startup and configuration rows (reader ceiling, artifact, digest, directory, pin,
manifest/profile, exact-pin and selection rows), `initScrubEngine` refuses, `scrubEngineReady()` is
false, the Node services exit non-zero at startup, and nothing is stored. The per-call rows do not
stop the process: a tagged refusal or blank input refuses that one call (the request fails, nothing
is stored for it, the instance stays ready); poisoning after startup makes every later call refuse
and turns readiness false (`503 scrub_unavailable` / `/readyz` not ready); an unwired call site
refuses that call or request with `seal_path_not_routed`. No row falls back to TS or raw.

### Workers excluded

Cloudflare Workers are not in this cutover. The hosted ingest Worker (`src/session-ingest/worker.ts`),
`packages/session-read-worker` and `packages/amp-transport-worker` keep the TS path and reader
ceiling 8. `createCompiledBindgenEngine` stays unwired. The ingest Worker passes
`readWorkerBindgenArtifact`, which always throws `artifact_missing`, so a mis-set `SCRUB_ENGINE=wasm-bindgen`
answers `503 not_configured` rather than reading any bytes. No Worker config sets the triple.
`src/session-ingest/__tests__/worker-scrub-cutover-exclusion.test.ts` guards that the Worker never
claims scrub version 10 (behaviour, bundle contents, sources and configs).

### Default flip

Since this change the pinned wasm-bindgen engine is the default scrub path; the opt-in triple is
no longer needed (it still works, unchanged).

**Resolution** (`resolveScrubEngineKind`, `packages/pipeline/src/lib/scrub-engine/index.ts:189`):

| `SCRUB_ENGINE` | TS grant | `SCRUB_ENGINE_REQUIRED` | Kind |
|---|---|---|---|
| unset / `""` | none | unset or `wasm-bindgen` | wasm-bindgen |
| unset / `""` | present | unset | ts |
| unset / `""` | present | `wasm-bindgen` | wasm-bindgen (REQUIRED overrides the grant) |
| `ts` | present | unset or `ts` | ts |
| `ts` | none | any | refuses: `ScrubEngineConfigError`, `scrub_engine_invalid`, reason `scrub_engine_ts_not_allowed` (latched; a grant made later does not lift it) |
| any | any | a different kind | refuses: reason `engine_selection_required` (latched) |

Unset fails closed: an init failure (missing / mismatched vendored bytes, pin, manifest, profile,
reader ceiling) throws `ScrubEngineUnavailable` with the existing reason, `scrubEngineReady()` is
false, the failure is sticky for the process, and there is never a TS or raw fallback. A scrub or
log call before init refuses (`scrubTextForLog` returns `[scrub-unavailable]`) but latches nothing,
so the later same-kind `initScrubEngine()` still succeeds. Two initialisers in one process share one
instance (the second call is a no-op returning it).

**The TS grant** (`packages/pipeline/src/lib/scrub-engine/ts-capability.ts`): `declareScrubTsCapability(kind)`
with kind `hosted-worker`, `hosted-lambda` or `test`. It is not environment-driven, is not
re-exported from the seam or any package index, and `resetScrubEngineForTests()` clears it.
`predicate-hook.ts` agrees with the seam before it loads (see "Import cycle").
The only TS-capable entry points are these artifact-incapable hosted entries (they ship no
relayscrubber bytes). Each declares the grant **at module load**: its first import is the bare
side-effect module `packages/pipeline/src/lib/scrub-engine/hosted-lambda-grant.ts` or
`hosted-worker-grant.ts`, so the grant exists before any other module of its graph evaluates and a
logger or helper that reaches the seam early (for example `scrubTextForLog`) resolves the unset
default to `ts` instead of latching wasm-bindgen. The handler-entry declarations stay as an
idempotent second call. The pipeline and read API `lambda.ts` modules cannot carry the module-load
grant, because self-host Node services import them for helpers (`consumer-service.ts`,
`large-read-main.ts`, `semantic-copy-main.ts`, `server.ts`, ...); those Lambdas therefore deploy thin
`lambda-entry.ts` wrappers (`stacks/aws` `main`) that import the grant and re-export `handler`.

| Entry point (deployed module) | Module-load grant | Handler-entry grant (file:line) |
|---|---|---|
| Hosted ingest Worker `fetch` / `queue` (`stacks/cloudflare/session-ingest.ts` `main`) | `src/session-ingest/worker.ts:76` | `src/session-ingest/worker.ts:573` via `scrubEngineReady` (`hosted-worker`) |
| Hosted session-read Worker `fetch` (`packages/session-read-worker/wrangler.jsonc`) | `packages/session-read-worker/src/index.ts:74` | `packages/session-read-worker/src/index.ts:640` (`hosted-worker`) |
| Digest narrative Worker `fetch` (`wrangler.digest-narrative.jsonc`; not deployed by this repo) | `packages/session-read-worker/src/digest-narrative.ts:18` | `packages/session-read-worker/src/digest-narrative.ts:85` (`hosted-worker`) |
| Read API Lambda handler (`src/session-read-api/lambda-entry.ts`) | `src/session-read-api/lambda-entry.ts:9` | `src/session-read-api/lambda.ts:743` (`hosted-lambda`) |
| Session pipeline Lambda handler, incl. bob-costas / session-brief stages (`src/session-pipeline/lambda-entry.ts`) | `src/session-pipeline/lambda-entry.ts:9` | `src/session-pipeline/lambda.ts:882` (`hosted-lambda`) |
| Portable Lambda zips (`src/lambda-artifact/pipeline-entry.ts`, `read-api-entry.ts`, built by `scripts/build-lambda-artifact.ts`) | line 1 of each | the wrapped `lambda.ts` handler, as above |
| Upload admission Lambda handler | `src/session-ingest/upload-admission-lambda.ts:23` | `src/session-ingest/upload-admission-lambda.ts:55` (`hosted-lambda`) |
| Buffered GC Lambda handler (`scripts/build-buffered-gc.ts`) | `src/session-ingest/buffered-gc-lambda.ts:3` | `src/session-ingest/buffered-gc-lambda.ts:10` (`hosted-lambda`) |
| Rearm sweeper Lambda handler | `src/session-ingest/rearm-sweeper-lambda.ts:11` | `src/session-ingest/rearm-sweeper-lambda.ts:19` (`hosted-lambda`) |
| Test preloads only | none | `scripts/test-preload/scrub-ts-capability.ts` (root `bunfig.toml` `[test] preload`), `packages/pipeline/test/setup/scrub-ts-capability.ts` (vitest `setupFiles`), `packages/session-read-worker/test/setup-env.ts:12` (`test`) |

No other process reaches a grant module. The schema release rehearsal
(`scripts/pg-proof/schema-rehearsal.ts`, stage R6) needs only `acceptedGenerations`, so it loads it from
the grant-free `src/session-ingest/accept-generations.ts` (which `worker.ts` re-exports) through its
computed candidate-tree path, never `worker.ts`; release trees from before that split fall back to
`worker.ts`, which there carries no module-load grant. The rehearsal process therefore holds no TS
grant: its ingest stages (R7-pre, R7, through `prepareDelivery`) resolve the wasm-bindgen default, and
since the rehearsal calls no `initScrubEngine`, a scrub there fails closed (`not_initialised`); it never
scrubs with TS.

Node self-host never reaches TS, even with `SCRUB_ENGINE=ts`. The large canonical read worker
(`src/session-pipeline/large-read-main.ts`) initialises the default and fails closed
(`scrub_engine_unavailable`) where the vendored bytes are absent; its bundle never contains the wasm
bytes or the pin digest (`src/session-pipeline/__tests__/large-read-bundle.test.ts`). The compiled
canonical publication zip (`scripts/build-canonical-publication.ts`, `node index.mjs`) is the same: it
bundles only `index.mjs` and `package.json`, and the engine resolves the pin
(`packages/pipeline/scrub-bindgen-pin.json`) and the default artifact directory relative to its own
module location (`SCRUB_BINDGEN_DIR` overrides only the artifact directory), so an enabled publish run
of the zip fails closed at startup with `scrub_engine_unavailable` (`bindgen_pin_unreadable`) unless the
pinned wasm-bindgen package is reachable from the bundle's location; a disabled run and `--gc` are
unaffected. The compiled-mode proof (`scripts/proof-canonical-publication.ts`,
`PUBLICATION_COMPILED_BUNDLE`) therefore cannot pass without the pin and bytes present, and cannot
false-pass.

**Entry inventory** (`scripts/scrub-default-scan.ts`, enforced in CI by
`src/session-ingest/__tests__/scrub-default-scan.test.ts`): every entry the scan's heuristic detects
among tracked `.ts`, `.mts`, `.mjs` and `.js` files (the vendored wasm-bindgen glue under
`packages/pipeline/scrub-bindgen/` excepted) under `src`, `packages`, `scripts`, `e2e`, `benchmarks`
and `stacks` (`import.meta.main`, top-level await, a non-declaration top-level statement, a Worker
`export default { fetch | queue | scheduled }`, a Lambda `export const handler` /
`export [async] function handler`) whose static import graph reaches the seam must call
`initScrubEngine(` (or `runWorker(`) in its code (a comment or string does not count), be a hosted
entry or a wrapped Lambda handler, or be listed in `ENTRY_EXEMPT` with a reason (GitHub-API gates,
bundlers, the technical-foul manifest writer, the RDS activation and claim-records archive tools, and
the Alchemy IaC program `stacks/aws/alchemy.aws.run.ts`, which import the seam graph but never scrub).
A repo path named in an exemption reason (with or without an extension) must be a tracked source, an
intermediate module of the entry's seam route (not the entry or a seam module), and one that every route
from the entry to the seam passes through, so a reason cannot cite a path the graph does not use. The host publication
CLI `src/session-ingest/publication-main.ts` initialises the engine under `import.meta.main` before
its first publish cycle (importing the module initialises nothing; a disabled run, `--gc` and
invalid arguments never need the engine), and `scripts/proof-canonical-publication.ts` initialises
both the source seam and, for `PUBLICATION_COMPILED_BUNDLE`, the bundle's own copy (re-exported by
publication-main) before it seals. It also fails on a grant declared outside the
enumerated declarers, a grant re-exported from the seam, a hosted entry whose first import is not
its module-load grant, any other non-test file that reaches a module-load grant, a stacks or
wrangler `main` that is not a hosted entry, a production module importing a test preload, and a
stale exemption, on a file the transpiler cannot parse, and on a workspace-package import that resolves
to no tracked source (an unknown `exports` shape or an untracked target; fail closed). Each rule has a
planted-violation test; the scan is a heuristic, not a proof.
Known limits, listed in the scan header: an initialiser anywhere in the entry counts even if a seam
use runs first or it sits under a condition (the runtime still fails closed there); computed and
variable loads stay invisible (`import(x)`, `require(x)`, `import.meta.require(x)`, a template with
`${...}`, an import wrapper such as the rehearsal's `mod(path)`, a stored `createRequire` result
called later, `new Function(...)` / `eval(...)`, `require.resolve`, `new Worker(url)`, `Bun.spawn` of another file); entries shaped
differently from the patterns above (for example `export default app`) are not detected. The
rehearsal's R6 module choice is pinned separately by `src/hoopdb/__tests__/schema-rehearsal-r6.test.ts`.
Edges are read by `Bun.Transpiler.scanImports`, the transpiler's own parse rather than a line
regex: every static import and re-export in any spacing or position (`import{}from"x"`,
`export*from"x"`, mid-line, after a comment, with unicode escapes; `import {} from` still evaluates
the module, only `import type` and all-`type` braces are erased), `import("x")` with comments,
options or a trailing comma, `` import(`x`) `` and `require("x")`, plus `import.meta.require("x")`
and `createRequire(...)("x")` from the TypeScript AST. A `?query` or `#hash` suffix is stripped
before resolution through relative, `@rhc/*`, `@rhc-object-store` and workspace-package
(`@96bulls/pipeline/...` and the other `packages/*` names, through their `exports` when present)
specifiers; an extensionless path tries Bun's order (`.mts`, `.ts`, `.mjs`, `.js`, then the same for
`/index`).
`hosted-grant-first` reads the TypeScript AST: after comments, a shebang and a directive prologue,
the hosted entry's first statement must be the static bare import of its grant with no import
attributes (`with { type: "text" }` or `assert` loads it as data and never runs the grant), and it
must be the first load the transpiler reports; a named or namespace import, `void import(...)`,
`await import(...)`, or import text inside a string, template or comment does not count.

**Tests** (each in a clean child process, no preload, asserting no grant at start):
`src/session-ingest/__tests__/scrub-default-flip.test.ts` (fixture child
`src/session-ingest/__tests__/fixtures/scrub-default-child.ts`, and
`src/session-ingest/__tests__/fixtures/scrub-entry-grant-child.ts` for the per-module cases) covers unset default with a routed
seal and readback of a fixture session body, explicit `wasm-bindgen`, explicit `wasm`, disallowed
Node TS (test "SCRUB_ENGINE=ts without a capability grant refuses with scrub_engine_ts_not_allowed,
sticky"), REQUIRED pass / refuse, init failure, pre-init logging, a Node service entry, the hosted
grant, and, per hosted entry module, a log scrub before the handler runs that resolves `ts`
(module-load grant), plus self-host mains that start with no grant and resolve wasm-bindgen.
`src/session-ingest/__tests__/predicate-hook-fallback.test.ts` covers the pre-seam predicate fallback.
`src/session-ingest/__tests__/scrub-default-scan.test.ts` plants a violation for each scan rule and
each covered entry shape.

**Unchanged:** the reader-first policy and its 8..10 bounds, marker and profile comparison, tagged
refusals, the 16 MiB preflight, blank parity (U+0085, U+FEFF), empty-input refusal, non-retryable
oversize, shared poison, the seam's `sealRawChunk` refusal (`seal_path_not_routed`), the pin and the
vendored bytes, the ruleset / profile / marker (v8 and v10 reads stay compatible), and
`technical-foul.json`.

### Rollback

Deploy the previous image, or revert this change, and restart. There is no environment switch back
to TS on Node self-host: `SCRUB_ENGINE=ts` refuses there by design. Reverting the writer does not
downgrade stored objects, so readers must keep accepting version 10 while any version-10 object
remains stored. What that requires depends on the image rolled back to:

- **An image that contains "self-host readers accept ruleset 10 by default"** (the commit before this
  change): nothing to set. Unset `READER_MAX_RULESET` is the effective ceiling 10 there.
- **An image built from that commit's parent or earlier**: REQUIRED, set `READER_MAX_RULESET=10`
  explicitly on `SERVICE=alleyoop-ingest` (`src/alleyoop/ingest.ts`) **before** the rollback image
  starts. On those images `src/alleyoop/manifests.ts` (`chunksAreServerSealed`) checks each listed
  chunk's `scrub-version` against `maximum ?? DEFAULT_READER_MAX_RULESET` (8), so without it every
  manifest that lists a version-10 chunk is answered `409 chunks_unverified`. The other Node readers
  on those images treat unset as legacy reads (no ceiling, version 10 accepted) and need nothing
  for reading. On those images the TS engine is the Node default again; a service that keeps the
  opt-in wasm-bindgen triple there also needs `READER_MAX_RULESET=10`, or its `initScrubEngine`
  refuses the version-10 pin (the pin parser's unset writer ceiling is 8) and the service fails closed.
  Note that the explicit value also turns on the strict marker and per-chunk metadata checks for
  alleyoop-ingest ("Explicit reader ruleset capability" below: unknown families refuse, manifests
  above version 8 need per-chunk scrub metadata); confirm the objects it serves meet that.
- The hosted Workers are not changed by this change (TS path, ceiling 8), so rolling it back changes
  nothing there.

## Import cycle (why `predicate-hook.ts`)

`pipeline/errors.ts` imports `pipeline/trace.ts`. The seam's error classes extend `PipelineError`.
A static import of the seam from `trace.ts` would form the cycle
`errors.ts -> trace.ts -> seam -> errors.ts`, which leaves `PipelineError` uninitialised.

`trace.ts` therefore calls `predicateScrubText` from `predicate-hook.ts`, a module with no
dependency on the seam. The seam installs the hook at load:

- Under `ts`, the hook is exactly `scrubText`.
- Under `wasm` / `wasm-bindgen` (the default), the hook is the engine's `scrubText`. An
  unavailable, uninitialised or failed engine returns `undefined`, so `isSafeIdentifier` is false
  and the id is dropped.
- Before the seam loads, the fallback answers with `scrubText` exactly where the seam's first
  selection would pick TS without refusing: a TS grant AND `SCRUB_ENGINE` unset, `""` or `ts` AND
  `SCRUB_ENGINE_REQUIRED` unset, `""` or `ts`. Anything else (the ungranted default; `SCRUB_ENGINE=ts`
  without a grant; any other engine or requirement) returns `undefined`. The refusal latches live in
  the seam only and cannot exist before it loads; the fallback never latches, and once the seam
  loads it installs its own hook, so every later answer is the seam's.
  `src/session-ingest/__tests__/predicate-hook-fallback.test.ts` covers this in a clean child process.

**Failure mode (pre-existing, now the self-host default):** `undefined` makes the identifier
predicate fail, so the identifier is dropped: a request carrying it answers 400, or the field is
omitted from the output. That is how an engine fault surfaces on these paths (wasm-bindgen
uninitialised, failed, or refused): as a 400 or a missing field, never as unscrubbed output. Before
the default flip only opted-in wasm processes saw this; now every self-host process that reaches an
identifier predicate before `initScrubEngine()` succeeds, or after it fails, does.

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
  path and no raw write. (Under `wasm-bindgen` the wired constructors pass the bound sealer
  instead; see "Cutover".)
- **publication-image.ts sealRawChunk**: the same seam refusal (same `wasm-bindgen` note).
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

Node services validate the setting at startup and thread it into canonical reads. Alleyoop also forwards it through its real app bindings. Canonical and timing Worker routes validate their own bindings rather than consulting a Node process environment. Offline seed readers receive an explicit optional policy in their dependencies. An unset policy does not add a marker check to any of these paths. The wasm-bindgen pin parser retains a separate default writer ceiling of 8 and refuses a writer above its ceiling before reading artifacts or initializing glue; `initScrubEngine` passes it the effective ceiling (`effectiveReaderMaxRuleset`: explicit `READER_MAX_RULESET` first, unset = `DEFAULT_SELF_HOST_READER_MAX_RULESET` (10) on Node / Bun, 8 for the hosted ingest Worker via `hostedReaderCeiling`). The alleyoop courier's per-chunk version check uses the same function. Compiled-module callers must pass their verified reader ceiling explicitly when it exceeds the default. The raw ABI path remains unpinned and outside this first cutover.

Operationally, deploy and verify every relevant reader with ceiling 10 **while the Rust engine remains off**, then qualify and enable the matching writer. Since "Default flip" the writer is on by default in every Node / Bun self-host process, so an image carrying it may only be deployed where every reader of its objects already accepts 10 (the self-host default below, or an explicit `READER_MAX_RULESET=10`). Self-host readers accept ruleset 10 by default (see "Self-host default reader ceiling" below): an unset `READER_MAX_RULESET` no longer refuses the v10 pin on Node / Bun. An operator who explicitly sets `READER_MAX_RULESET=8` (or 9) on a process keeps that process refusing the v10 pin. Configuration in one process does not prove that a remote fleet has been upgraded. Keep the reader ceiling at 10 on rollback while version-10 objects remain stored; reverting a writer does not downgrade existing objects. Existing artifact-page formats do not change. This source change does not satisfy the remaining package safety, provenance, error-tagging, Worker build-binding or persistence-routing gates.


### Reader-first rollout: entrypoints honouring READER_MAX_RULESET

Audit of every Node/Bun reader of a committed manifest's `raw.scrub` marker or of committed raw
chunk bodies (2026-11-11). Each passes `readerMaxRuleset(env)` / `readerMaxRuleset(process.env)`
(or a value derived from it); unset yields `undefined`, which keeps legacy reads on every row.
"Byte-only" paths copy, delete, rehash or HEAD objects without interpreting scrubbed content and
need no ceiling. Writer reuse is governed by the sealer policy; `READER_MAX_RULESET` never grants
writer permission. Cloudflare Worker routes validate their own bindings and are out of scope here.

| Entrypoint / reader (file:line) | Reader path | Before | After |
|---|---|---|---|
| `src/session-read-api/service-canonical.ts:160,190` | canonical transcript / records (`canonical-transcript.ts:43,75`, `canonical.ts:255`) | wired | unchanged |
| `src/session-read-api/server.ts:197` | self-host read server startup validation | wired | unchanged |
| `src/session-brief/config.ts:88` (`worker.ts:139`, Lambda `briefConfig()`) | brief input via `readRelayhistoryExport` | wired | unchanged |
| `src/session-pipeline/consumer-service.ts:1111,1201` | consumer `search` → `indexSearchScope` | wired | unchanged |
| `src/alleyoop/manifests.ts:653,679,1014`, `ingest.ts:166` | `chunksAreServerSealed` | wired (`c.env`) | unchanged |
| `scripts/session-ingest-seed.ts:67,124` → `seed-proof.ts:144` | seed index `streamManifestRecords` | wired | unchanged |
| `src/session-pipeline/lambda.ts` `lambdaEnv` → `deps.search` | Lambda search stage → `readCanonicalScope` | **omitted** | `env.readerMaxRuleset` |
| `src/session-pipeline/lambda.ts` `deps.replay` | Lambda replay stage → `produceCanonicalReplay` | **omitted** (no field) | `env.readerMaxRuleset` |
| `src/session-pipeline/lambda.ts` `analysisTick({canonical})` | session analysis → `canonicalProducerSnapshot` | **omitted** | `env.readerMaxRuleset` |
| `src/session-pipeline/replay-publication.ts` `ReplayProducerDeps` | `canonicalProducerSnapshot` (also `consumer.ts` replay) | **no field** | field added and forwarded |
| `src/session-pipeline/large-read-main.ts` `largeReadRuntime` | large-read Job `search` + `replay` | **omitted** | `readerMaxRuleset(env)` |
| `src/bob-costas/main.ts` → `handler.ts` `buildOutcome` | summary reader (marker + chunk GET) | **no check** (presence only) | marker + `readerChunkBody` when set |
| `scripts/capture-canonical-frontier.ts` | operator frontier capture → `SeedProofIndex` | **omitted** | `readerMaxRuleset(process.env)` |
| `scripts/proof-canonical-publication.ts`, `proof-ingest-s3-seed.ts`, `proof-canonical-index-capacity.ts`, `proof-directory-display.ts`, `proof-canonical-replay-pages.ts`, `proof-publication-canary.ts`, `opensearch-proof/e2e.ts` | proof readers (`readCanonicalScope`, `streamManifestRecords`, `readManifestRecords`, `indexSearchScope`, `produceCanonicalReplay`) | **omitted** | `readerMaxRuleset(process.env)` |
| `src/session-ingest/writer.ts`, `publication-image.ts`, `migration/neon-s3-sync/export-session.ts`, `raw-chunks.ts`/`engine-raw-chunks.ts` collision re-reads | writer reuse / byte copy with hash check | n/a (writer / byte-only) | unchanged |
| `parked.ts`, `neon-s3-erasure`, `session-erase`, `claim-records-archive.ts`, `buffered-gc.ts`, `redaction-barrier.ts`, `locker/init.ts` | delete / archive / HEAD | n/a (byte-only) | unchanged |
| `adapter.ts` manifest reader, `hoopdb/*` catalog jobs, `alleyoop/courier.ts` | marker presence or carry-forward, no content read | n/a | unchanged |
| `src/smoke/e2e.ts` | smoke harness leak check over its own fixture | n/a (test harness) | unchanged |

`RelayhistoryExportTransform` has no production constructor; any future one must pass the
ceiling. An explicit `READER_MAX_RULESET` on any process above is still honoured exactly.

### Self-host default reader ceiling

`DEFAULT_SELF_HOST_READER_MAX_RULESET` (10, `reader-policy.ts`) is the ceiling where a self-host
process needs a number and `READER_MAX_RULESET` is unset; `effectiveReaderMaxRuleset` is the one
function that resolves it (explicit value first). It applies to exactly two places: the alleyoop
courier's per-chunk `scrub-version` check (`src/alleyoop/manifests.ts`, `chunksAreServerSealed`)
and the wasm-bindgen writer gate in `initScrubEngine`. Every other reader in the table above keeps
legacy reads when unset (no ceiling, so ruleset 10 is accepted) or, when set explicitly, the exact
8..10 policy. `DEFAULT_READER_MAX_RULESET` stays 8: it is the pin parser's default, the
metadata-free legacy cutoff and the hosted ingest Worker's unset writer gate. No deployed config,
stack or Dockerfile sets `READER_MAX_RULESET`, so no deployed reader meets version-10 objects at an
effective ceiling of 8 unless an operator sets 8 explicitly. Tests: `reader-scrub-policy.test.ts`
(limit-1 / limit / limit+1 at unset, explicit 8 and explicit 10) and
`src/alleyoop/__tests__/manifests.test.ts` ("self-host reader ceiling").


### Configuration failure runbook

Leave `READER_MAX_RULESET` absent to preserve legacy reads. An explicit value
opts into strict marker and chunk checks, including canonical decimal metadata;
older metadata such as `"08"` or `"8.0"` is accepted by the legacy courier path
but refused by the explicit policy. The courier always requires family
`relayhistory-scrub` and a positive manifest version, including zero-chunk
manifests, as main does. Explicit policy additionally checks the ceiling and
canonical chunk metadata. Verify existing retained objects before opting in.

Empty, malformed, padded, unsupported or number-typed Worker bindings are
configuration errors. In ingest, engine initialization fails closed and
`scrubEngineReady` stays false: sealing does not continue through TS or another
engine. Correct or remove the malformed binding and restart through the normal
operator process. Raising a reader ceiling never qualifies a package or a writer.
