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

## Read design

- **Route.** A read route, off by default: it answers the read API's standard `501 unsupported_by_backend` envelope
  unless its flag is exactly `true`, with the same workspace attestation and canary as `/v1/session-intel/workspace`.
- **Storage and authority.** An offline job writes one artifact per workspace and window under its own result prefix.
  For a lookup, the authoritative artifact is the newest complete one for the exact workspace and window.
- **Freshness.** The artifact is refreshed when the cohort changes; its `generatedAt` and `freshness` say how current
  it is.
- **Erasure.** An artifact that cites an erased session must not be served: the read path checks every session it
  names against the erase record and refuses (or the job recomputes) when any is erased, so an erased session never
  stays in a drilldown or a count.

Real data and any production enablement are later steps, gated by Khaliq.
