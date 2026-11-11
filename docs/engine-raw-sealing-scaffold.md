# Injected-engine raw sealing scaffold

Status (cutover, pinned to relayscrubber `0.5.0-alpha.6`; first routed with `0.5.0-alpha.4`): the sealer is now routed in production, opt-in only.
Under `SCRUB_ENGINE=wasm-bindgen` + `SCRUB_ENGINE_REQUIRED=wasm-bindgen` +
`READER_MAX_RULESET=10`, `routedRawChunkSealer()` in `scrub-engine/index.ts` binds
this sealer to the verified relayscrubber `0.5.0-alpha.6` instance and the four
production constructors inject it (see docs/scrub-engine.md, "Cutover: pinned
relayscrubber 0.5.0-alpha.6"). The seam's own `sealRawChunk` still refuses non-TS
engines with `seal_path_not_routed`, so an unwired call site refuses. With the
default (unset/`ts`) nothing here is reached. The cutover's provenance is the new
`packages/pipeline/technical-foul-cutover.json` entry; the historical TypeScript
manifest (`technical-foul.json`) and `raw-chunks.ts` are unchanged. It does not
qualify an image; image qualification and live acceptance remain separate.

The original scaffold description follows.

`bindEngineRawSealer` captures a concrete healthy wasm-bindgen engine and an
explicit `same-family-up-to-writer` retained-object policy. Callers install the
returned dependency directly; no environment variable enables it. The engine's
marker is copied and immutable. Source bytes and location are privately captured
before the first await. The source digest and scrub invocation use the same
snapshot. Each seal invokes that engine even for unchanged output and existing
objects. Failed health checks, mismatched results, or unknown traps poison the
binding and mark a verified bound instance failed for every caller, including
`activeScrubEngine()`. Empty output or zero records for non-empty input is an
instance defect for admitted nonblank input and is never persisted. Empty or
blank-only streams are refused at admission before invoking or poisoning the
engine (the real Rust artifact legitimately emits zero records for those).
"Blank" is exactly Rust `str::trim` / Unicode `White_Space` (`isRustWhitespaceOnly`
in `engine-raw-chunks.ts`): U+0085 counts as blank, U+FEFF does not, unlike JS `\s`.
The stream is decoded in 64 KiB pieces with a streaming fatal decoder, so a character
split across pieces is judged whole; invalid UTF-8 is never judged blank.
Typed call refusals remain typed; retryable allocation refusal remains
retryable. Output may expand past the input per-record limit; the scaffold
stages at most 64 MiB of output. Larger output refuses with non-retryable
`scrub_output_too_large` (HTTP 413) without poisoning a healthy engine. This
capacity bound needs cutover review. Nothing falls back to TypeScript or
unscrubbed input.

The new module frames only successful scrub output with the existing exported
raw-Zstd-frame transport encoder. These are valid Zstd frames containing scrubbed
bytes in Raw blocks, without compression; this is not a plaintext fallback.
New objects are create-only and carry the bound
family, version and `scrub-profile`. Profile metadata is informational until a
separate manifest schema/pin review; default readers do not require it and there
is no new manifest-wide profile for mixed histories.

When create-only loses, one GET supplies the winning object's body and metadata.
Its body is bounded, hashed and sized independently. It is never compared with
the new engine's output or overwritten. Retained metadata must contain a valid
raw digest/positive size and safe integer version, family `relayhistory-scrub`,
and a version from 1 through the bound writer's version. Older version 8 objects
without a profile remain version 8 with profile absent. Missing/malformed
metadata, another family and a newer version refuse with distinct content-free
errors. `READER_MAX_RULESET` never grants writer or retention permission.

The four real ports accept an explicit bound-sealer dependency:

- Pipeline HTTP chunk route: retryable scrub failure returns 503, not a data 400.
- Inline writer: scrub failure propagates with its retryable classification;
  no raw chunk or manifest is published. Its pre-existing session locator is
  metadata-only and may already have been created before sealing.
- Publication image: captures exact sealed bytes and options; durable
  publication uses those bytes/options after its prepare/register protocol.
- Migration export: its default TS stamps and prior-chunk reuse stay unchanged.
  Explicit injection initializes the writer version from the bound marker,
  bypasses the prior-manifest shortcut so retention goes through the sealer,
  and checks returned versions against that bound writer. This audits the
  prior `export-session.ts` lines 191–225 without increasing a reader ceiling.

Validation exercises these actual ports, unchanged and changed output, poisoned
instances, input mutation across awaits, selection conflicts, refusal without
persistence, retained provenance, competing creates, acknowledgement loss, input
limits, and byte-exact historical/default wrapper comparisons. It is source
validation with distinguishable test engines. Workers compiled-WASM provenance,
real artifact recall/precision/performance, image qualification, retained database
rehearsal, and live end-to-end acceptance remain separate mandatory cutover work.

The HTTP route also returns 503 `scrub_unavailable` when a non-TS engine is
selected but is uninitialised or no explicit sealer is routed; this is not an
activation path. Deterministic output overflow is 413, while allocation refusal
is retryable. Publication capture reserves roughly 400 bytes below its 16 MiB
limit for framing; the injected sealer can therefore be refused at that capture
bound before the raw sealer's 64 MiB capacity.

Cutover decisions: output records may expand beyond 16 MiB (still the 64 MiB
staging bound above; open). Production binding to the exact verified instance is
now proven by `src/session-ingest/__tests__/scrub-cutover-reachability.test.ts`
(real wasm, all four ports, zero TS calls). `publication-job.ts` now forwards
`deps.rawChunkSealer`. The image context excludes packages/pipeline/test,
including synthetic engine fixtures; the vendored package under
`packages/pipeline/scrub-bindgen/` is included.
