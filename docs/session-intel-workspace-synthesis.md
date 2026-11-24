# Session Intelligence: workspace synthesis (`workspace-synthesis/1`)

A deterministic aggregate over the per-session analysis results (F1, session-analysis schema v2) of one workspace
and one window: what kind of work happened, how it ended, how fresh and how complete the analyses are, how much of the
usage is observed, and which failures recur across independent roots. It gives the operator view and its designer one
stable shape to build on.

Code: [`src/session-intel/synthesis/synthesize.ts`](../src/session-intel/synthesis/synthesize.ts) (the aggregator),
[`schema.ts`](../src/session-intel/synthesis/schema.ts) (the JSON Schema const `WORKSPACE_SYNTHESIS_SCHEMA` and
`validateWorkspaceSynthesis`). Fixture: `src/session-intel/__tests__/fixtures/synthesis/` (`cohort.json`, built by
`make-cohort.ts`; `expected-synthesis.json`, the golden).

## Stage 1 and Stage 2

Stage 1 (this contract) is deterministic: every number is a count or a ratio of two counts over validated F1 results,
and the same cohort in any order gives byte-identical JSON. Nothing is inferred and no model is called.

Stage 2 (model-inferred initiatives, themes and improvement hypotheses) is out of scope. `workspace-synthesis/1` has
**no** placeholder field for it. Stage 2 adds its fields either as optional fields that a `/1` reader may ignore, or as
a new contract version (`workspace-synthesis/2`); it never changes the meaning of a `/1` field.

## Input: the cohort

Only completed, scrubbed F1 results, never raw transcripts. The cohort binds one workspace and one window:

```json
{
  "format": "workspace-synthesis-cohort/1",
  "workspace": "fx-ws-synthesis",
  "window": { "since": "2026-11-01T00:00:00.000Z", "until": "2026-11-08T00:00:00.000Z" },
  "eligible": 60,
  "sessions": [
    { "status": "analyzed", "workspace": "fx-ws-synthesis", "source": "fx-claude", "sessionId": "fx-s01",
      "repo": "example.test/fx/alpha", "projectId": null, "result": { "schemaVersion": 1, "analysis": { "...": "..." } } },
    { "status": "skipped", "workspace": "fx-ws-synthesis", "source": "fx-codex", "sessionId": "fx-skip-failed",
      "reason": "failed" }
  ]
}
```

- `workspace`: a plain id, `^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$` (the tenant id rule of the session-intel CLIs).
  Every entry repeats it; an entry of another workspace (or without one) refuses the whole cohort (`cross_workspace`).
  The F1 snapshot itself carries no workspace, so the entry's own field is what binds it.
- `window`: `since` inclusive, `until` exclusive (`[since, until)`), ISO-8601. The output writes both in UTC with
  milliseconds (`toISOString`). `until - since` must be positive and at most 30 days (`window_too_long` otherwise).
- `eligible`: how many sessions in the window the producer could consider at all, or `null` when it does not know.
  It is never derived. It must be at least the number of entries.
- `source`: `^[A-Za-z0-9_.-]{1,64}$`; `sessionId`: `^[A-Za-z0-9._:-]{1,255}$` (the read API's id rule).
  A repeated `(source, sessionId)` is refused (`duplicate_session`).
- `repo`, `projectId`: optional, `^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$` or null. They only feed the independence root.
- `result`: the stored F1 envelope (`SessionAnalysisResult`, `schemaVersion: 1` around an analysis of
  `schemaVersion: 2`), or `null`.
- Any undeclared field, at the top or on an entry, refuses the cohort. A refusal is a typed `SynthesisInputError`
  with a `code`.

### Validation of each analyzed entry, and the skip reasons

Each analyzed entry goes through `decodeSessionAnalysisResult` (envelope, snapshot, identity: the result's
`source`/`sessionId` must equal the entry's), which runs `validateEvidenceBundle` on its evidence and
`validateSemanticAnalysis` on its understanding (every cited evidence id must exist in that session's bundle). A result
that fails is **counted as skipped with reason `invalid`, never trusted and never aggregated**; it does not refuse the
cohort. The closed set of skip reasons:

| Reason | Meaning |
|---|---|
| `not_analyzed` | no completed job (queued or running), or a `null` result |
| `failed` | the analysis job failed (declared by the producer) |
| `unsupported` | an envelope or snapshot schema version this reader does not know |
| `invalid` | the envelope, snapshot, identity, evidence bundle or a cited evidence id fails validation |
| `too_large` | the result is over `ANALYSIS_RESULT_MAX_BYTES` (8 MiB, `results.ts`) |
| `stale_version` | a valid result whose `analysisVersion` is not accepted (`versions.ts`: this pipeline's and relayhistory's) |
| `out_of_window` | `analyzedAt`, an evidence reference's `at`, or an interaction period's start or end is outside `[since, until)` |

A producer may also declare any of these on a `skipped` entry.

## Output

Top-level keys, in this order: `contract`, `workspace`, `window`, `generatedAt`, `inputs`, `freshness`,
`usageCoverage`, `categories`, `issues`. Nothing else.

| Field | Semantics |
|---|---|
| `contract` | `"workspace-synthesis/1"` |
| `workspace`, `window` | echoed from the cohort (the window normalized to UTC with ms) |
| `generatedAt` | the injected `now`; no clock is read |

### `inputs`

| Field | Semantics |
|---|---|
| `eligible` | the cohort's `eligible`, or `null`. Not derived |
| `considered` | entries considered: `min(entries, 500)` |
| `analyzed` | considered entries whose result validated and is in window |
| `skipped` | `{<reason>: count}` for every reason above, zeros included |
| `truncated` | `true` when any cap below truncated |
| `caps` | `{limit, truncated}` per cap: `sessions` (with `offered`, the entry count), `drilldown`, `issues`, `evidencePerIssue`, `textChars`, `artifactBytes` |
| `notes` | fixed sentences with counts: what is excluded, an unknown `eligible`, a truncation, a missing watermark |

Invariant: `considered = analyzed + sum(skipped)`. `eligible`, when not null, is at least `caps.sessions.offered`.
All other aggregates are over the analyzed sessions only.

### `freshness`

| Field | Semantics |
|---|---|
| `staleAfterMs` | the stale threshold (default 24 h) |
| `analyzedAtMin`, `analyzedAtMax` | the oldest and newest `analysis.analyzedAt`; `null` when nothing is analyzed |
| `indexedThroughMs` | the MIN over analyzed sessions of `analysis.evidence.indexedThroughMs`; `null` when ANY analyzed session lacks one, and `null` when nothing is analyzed |
| `indexedThroughMissing` | analyzed sessions without a watermark (0 when nothing is analyzed) |
| `staleCount` | analyzed sessions with `analyzedAt < generatedAt - staleAfterMs` |
| `analyzedAfterNow` | analyzed sessions with `analyzedAt > generatedAt` (a wrong `now`, or clock skew) |
| `bySource` | `{<source>: {analyzed, analyzedAtMin, analyzedAtMax, staleCount}}`, sources with an analyzed session |

`indexedThroughMs` is a completeness watermark (the cohort is complete through the earliest analyzed session's
index) **only when `indexedThroughMissing = 0`**.

### `usageCoverage`

`{<source>: {considered, analyzed, observed, unobserved, knownCost, nullCost, observedRate, nullCostRate}}`, for every
source with a considered entry. Observed usage and known cost are separate axes: a session can have observed tokens and
a null cost.

- `observed`: analyzed sessions with `metrics.usage.observed = true`; `unobserved = analyzed - observed`.
- `knownCost`: analyzed sessions with a non-null `metrics.usage.costUSD`; `nullCost = analyzed - knownCost`.
- `observedRate = observed / analyzed`, `nullCostRate = nullCost / analyzed`, rounded to 4 decimals; both `null`
  (never 0) when `analyzed = 0`.
- Invariants: `observed + unobserved = analyzed`, `knownCost + nullCost = analyzed`; the per-source `considered` and
  `analyzed` sum to `inputs.considered` and `inputs.analyzed`.

No token or cost sums are published, and nothing is imputed or extrapolated.

### `categories`

A partition of the analyzed sessions: `[{sessionType, outcomeStatus, count, truncated, sessions: [{source, sessionId}]}]`.
`sessionType` is `understanding.sessionType.value`. `outcomeStatus` is the session's one distinct outcome status,
`mixed` when its outcomes have several, `none` when it has none. Only non-empty categories are listed, in enum order
(session types, then the eight outcome statuses, `mixed`, `none`). The `count`s sum to `inputs.analyzed`. `sessions` is
sorted by `(source, sessionId)` and capped (`truncated` says so; `count` is always the full count).

### `issues`

Recurrence **candidates**, never findings: `[{id, label: "candidate", kind, key, keyTruncated, text, textTruncated,
roots, sessionCount, sessionsTruncated, sessions: [{source, sessionId}], evidenceTruncated, evidence: [{source,
sessionId, eventId, ts?, kind?}], basis: "deterministic"}]`.

Where they come from. Only failure claims count: a session's `blockers`, the `verification` of an outcome whose
verification is `failed`, and the `assessment` of an outcome whose status is `blocked`. A claim contributes only
through the evidence it cites, so an uncited claim contributes nothing. Each cited reference gives at most these keys:

- `artifact`: the path of a cited `file-edit` reference, or of a cited `tool-call` whose whole target is one path
  (from the evidence summary `<tool>: <target>`).
- `failed-command`: for a failed verification only, the summary `<tool>: <command>` of a cited tool call that itself
  failed (its `isError`, or a cited `tool-result` linked to it with `isError`). A failed verification citing a command
  that did not fail has no failure evidence and gives no key.

**Keys are exact.** Two contributions share an issue only when their `(kind, key)` strings are equal. There is no
normalization, no fuzzy matching and no similarity merging: `bun test src/fx` and `cargo test --workspace` stay two
issues even when the failure text is the same.

**Independence rule.** A session's root is `repo:<repo>` when it has a repo, else `project:<projectId>`, else
`source:<source>`. A key is a candidate only when it has at least `minRecurrence` sessions (default 3) **and** at least
2 distinct roots. A failure that recurs only inside one repository (one flaky step there) is not a candidate however
often it recurs. Each session counts once per key.

What `roots` means. An issue's `roots` counts **scope groups**: the distinct values of that grouping key (`repo:`,
`project:` or `source:`) among its sessions. It does not prove independent people, teams or agent fleets: two
repositories may share one author, and one repository may hold many. Recurrence inside one scope group (for example
one repository) is excluded by the current rule, however many sessions it spans. The field keeps its name.

Fields. `id` is `<kind>:<first 16 hex of sha256(kind NUL key)>`, stable across runs. `key` is the exact key and `text`
the first contributing session's (in `(source, sessionId)` order) smallest claim text, both verbatim prefixes of at most
280 characters (`keyTruncated`, `textTruncated`). `roots` and `sessionCount` are full counts; `sessions` is capped like
a category. `evidence` lists the cited references session by session, at most 5: `eventId` is the F1 evidence
reference id, always with its `source` and `sessionId` (the cohort session whose bundle holds it), plus `ts` (its `at`)
and `kind` when known. Issues are ordered by `sessionCount` desc, `roots` desc, `kind`, `key`.

## Bounds

| Bound | Value | Beyond it |
|---|---|---|
| window | 30 days | refused (`window_too_long`) |
| cohort entries | 5,000 | refused (`cohort_too_large`), before any result is decoded |
| sessions considered | 500 | the first 500 in `(source, sessionId)` order are considered; `caps.sessions.truncated` |
| sessions per category or issue | 50 | cut, with the list's `truncated` flag and `caps.drilldown.truncated` |
| issues | 10 | the first 10 in issue order; `caps.issues.truncated` |
| evidence per issue | 5 | cut; `evidenceTruncated` and `caps.evidencePerIssue.truncated` |
| text per field | 280 characters | a verbatim prefix; `keyTruncated`/`textTruncated` and `caps.textChars.truncated` |
| artifact | 256 KiB (262,144 bytes of the serialized JSON) | drilldowns shorten to 25, 10, then 0 sessions, with `caps.artifactBytes.truncated`; still too large is refused (`artifact_too_large`) |
| F1 result | 8 MiB | that entry is skipped `too_large` |

`inputs.truncated` is true when any of these truncated. Truncating to the first sessions in id order is deterministic,
not a sample: the note says how many were left out, and a consumer that needs them all should narrow the window.

## What is not inferred

No themes, initiatives or hypotheses. No similarity between claims. No imputed usage, cost or outcome. No session-level
status beyond the categories rule above. No per-session ranking. The only text in the output is F1 text (claim text and
evidence summaries, already scrubbed when the result was stored) cut to a prefix, plus ids, enums, timestamps, counts
and fixed notes; a test walks the output to prove it. No scrub logic lives here.

## Relation to `session-intel-workspace/1`

[`session-intel-workspace/1`](session-intel-workspace-api.md) is the live coordination view: alerts, status and
handoffs, now. `workspace-synthesis/1` is the retrospective aggregate over analysis results for a window. They share
the workspace scoping and the session reference shape `{source, sessionId}`, and nothing else.

## Storage: the offline job

[`src/session-analysis/workspace-synthesis.ts`](../src/session-analysis/workspace-synthesis.ts) `runWorkspaceSynthesisJob`
synthesizes a cohort and writes ONE artifact for its workspace and window, in the analysis-result object family next
to the per-session results ([`workspace-synthesis-layout.ts`](../src/session-analysis/workspace-synthesis-layout.ts)):

```
{keyRoot}/{org}/v1/ws={workspace}/normalized/session-analysis-v2/workspace-synthesis-v1/w={sinceMs}-{untilMs}/g={generatedAtMs, 13 digits}-{sha256}.json
```

`{keyRoot}` is the deployment's tenant base (`PIPELINE_KEY_PREFIX`). `{workspace}` is an injective escape of the id
(`synthesisComponent`: `_` becomes `__`, `:` becomes `_c`, every other admitted character is kept), not the raw
layout's `layoutComponent`, which maps `:` to `_` and would give `a:b` and `a_b` one directory. An `_` in the
component is always followed by `_` or `c`, so the escape decodes uniquely and every admitted id gets its own
directory.
Per-session results sit deeper, under each session's `m=/h=/d=/s=` directory; a workspace aggregate has no session to
anchor to, so it sits under the workspace directory. Why this place:

- **Object notifications.** The bucket notifies only keys ending in `/manifest.json`; an artifact key never does.
- **Pointer stage.** An artifact key does not parse as a session key, so the pointer stage
  (`src/session-pipeline/pointer.ts` `indexPointer`) answers `ignored` / `not_a_session_key`, never failed (tested).
- **Bucket policy.** The key is inside the analysis-result object glob (`ANALYSIS_RESULT_OBJECT_GLOB`,
  `stacks/aws/session-pipeline.ts`) the pipeline role writes and the read API role may `GetObject` and decrypt, so
  no policy statement changes for writing or reading an object (tested against the glob). See the enable condition
  below for listing.
- **Erasure.** No existing erase enumerated a workspace-wide prefix (the per-user erase walks the user's own
  `m=srv_...` machine prefixes; there is no whole-workspace erase lane). The per-user erase
  (`src/session-erase/user-erase.ts` `userErasePrefixes`) now also deletes the workspace's whole synthesis directory
  `{keyRoot}/{org}/v1/ws={ws}/normalized/session-analysis-v2/workspace-synthesis-v1/`, every window and version, and
  verifies it empty with the rest. The artifacts are derived and recomputable, so a user's erase removes them for the
  whole workspace; the job rebuilds them from a cohort without that user's sessions. An org-wide request (empty
  workspace), or a workspace id that no synthesis can have (`isSynthesisWorkspace` false: the erase accepts any
  1-512 character id, the synthesis only the cohort's rule), names no such directory and erases the base prefixes
  as before. A per-session erase does not delete artifacts; the read-time check below refuses
  any artifact that names the erased session.

The artifact is an envelope around the unchanged document:

```
{format: "workspace-synthesis-artifact/1", orgId, workspace, window, generatedAt, considered, related, synthesis}
```

- `considered`: every session the synthesis considered (analyzed or skipped), in `(source, sessionId)` order.
- `related`: every other session the analyzed results' evidence belongs to: each evidence reference's own session
  (a delegated child, for example) and the snapshot's `relatedSessionIds`. At most 10,000; more refuses the job,
  so the erase check is never partial.
- `synthesis`: the `workspace-synthesis/1` document, byte for byte what `synthesize` returns. The envelope is never
  served.

The envelope is at most 4 MiB. Writes are create-only and read back byte for byte; the same cohort with the same `now`
gives the same key, so a rerun is a no-op. The job takes a store interface (`SynthesisArtifactStore`):
`objectStoreArtifacts` adapts an `ObjectStore` and `memoryArtifactStore` serves tests. It reads no clock and no
environment.

## Read route

```
GET /v1/session-intel/synthesis?workspace=<id>&since=<ISO>&until=<ISO>
```

- **Gating.** Unless `READ_API_WORKSPACE_SYNTHESIS_ENABLED` is exactly `true`, the route answers the standard
  `501 unsupported_by_backend` envelope, reason `workspace_synthesis_disabled`, and reads nothing. Enabled without an
  artifact store and key root, it is `501 workspace_synthesis_not_configured`. Wired on the read app, the hosted
  Lambda and self-host service (`READ_API_ANALYSIS_BUCKET` and `PIPELINE_KEY_PREFIX`) and the read Worker (the raw
  store, `PIPELINE_RAW_S3_*`, and `PIPELINE_KEY_PREFIX`).
- **Auth and scope, as `GET /v1/session-intel/workspace`.** The bearer's `401` first; `workspace` exactly once and
  equal to the token's (`403` otherwise; absent is `501 org_scope`); the `coordination` canary; `since` and `until`
  exactly once each (`400` when missing, repeated, not ISO-8601 with a zone, empty or over 30 days); any other
  parameter is `501 param_<name>`.
- **Authority.** The newest complete artifact for the exact workspace and window under the token's org: the newest by
  its key's generation whose bytes match the key's digest, whose org, workspace, window and generation match, whose
  synthesis passes the schema and invariants, and whose `considered` covers every session a drilldown or issue names.
  Up to 5 of the newest are tried; none is `404 not_found`. Another org's artifact found under the prefix is a `500`
  refusal.
- **Erasure.** After the artifact is read, every session in `considered` and `related` is checked against the pointer
  tombstones (`Liveness.erased`, in batches of 500: a session whose every pointer row is tombstoned). If any is erased
  the artifact is not served: `404 not_found`, reason `synthesis_invalidated`, until the job recomputes it. So an
  erased session, a child only the evidence belongs to included, never stays in a drilldown or a count.
- **Freshness.** The job is rerun when the cohort changes; the new artifact's later generation makes it the
  authoritative one. `generatedAt` and `freshness` in the body say how current it is.
- **Body.** The `workspace-synthesis/1` document only (no org id, no provenance lists). A store failure is `503`.

### Enable condition

`READ_API_WORKSPACE_SYNTHESIS_ENABLED` stays off until all of these hold. Who confirms them is not decided here.

- **One bucket, one key root.** The job's writer and both readers (the Lambda on `READ_API_ANALYSIS_BUCKET`, the
  Worker on the raw store) are confirmed to use the same bucket and the same key root (`PIPELINE_KEY_PREFIX`).
- **The hosted stack sets the key root, and can list.** `stacks/aws/session-read-api.ts` does not set
  `PIPELINE_KEY_PREFIX` for the read API today, so with the flag on the hosted Lambda answers
  `501 workspace_synthesis_not_configured`. Setting it is a stack change (not part of this slice). Once it is set,
  the lookup lists the window's prefix, and the read API role's bucket grant is `GetObject` on the analysis-result
  glob only, with no `ListBucket`: until a listing grant for that prefix exists (an IAM change, separately gated)
  the route answers `503`.
- **Retention.** A lookup lists at most 1,000 keys of one window; more is an error, so a window with over 1,000
  generations answers `503` for good. Nothing prunes old generations yet: a prune (keep the newest few complete
  artifacts per window) is a later slice and must exist before the job runs on a schedule.

### Wiring the job

The synthesis writer is outside the ingest fence. A job write that lands in a workspace whose user is being erased,
during the erase's settle window, puts a new object under the synthesis directory, and the erase's verification pass
then finds it and stays unverified (it erases it and retries, up to its byte rounds). Before the job is wired to run
on its own, it must either take the user-erase fence (refuse a workspace with an active erase) or be scheduled so it
cannot write during one. Enabling, real data and any deploy are separate steps, gated by Khaliq.

## Running the CLI

```
bun src/session-intel/synthesis/cli.ts --in <cohort.json> --out <dir> --now <ISO> [--allow-repo-out]
```

Writes `<dir>/synthesis.json` (the exact bytes the golden pins) and a short `<dir>/synthesis.md`. `--now` is
required; the CLI reads no clock, environment or network. Outputs name sessions, so `--out` inside this repository is
refused unless `--allow-repo-out` is given (synthetic runs only), as the replay CLI does. For the fixture:

```
bun src/session-intel/synthesis/cli.ts --in src/session-intel/__tests__/fixtures/synthesis/cohort.json \
  --out /tmp/synthesis --now 2026-11-08T00:00:00.000Z
```

Regenerate the fixture with `bun src/session-intel/__tests__/fixtures/synthesis/make-cohort.ts`, then the golden with
the CLI (`--out` a temporary directory, then copy `synthesis.json` to `expected-synthesis.json`).
