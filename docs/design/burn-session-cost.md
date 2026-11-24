# Scoping: burn as the session token and cost engine

Status: scoping + proof of concept, code only. Branch `review/burn-cost-scope-poc` off `48f61828` (frozen main).
Companion burn change: AgentWorkforce/burn#593 (`price_usage` / `priceUsage`, open, `merge-train:hold`).
Author: burn-cost-scope, for sessionspipelinegrok. Lane owners consulted: layered-sessions-pipeline (runner and lander) and
sessions-intel-lead (F1 contract consumers, workspace synthesis, spend attribution).

## Summary

- The runner has no price table. Today F1 `metrics.usage.costUSD` is non-null only when the harness itself wrote a cost into
  its transcript (`relayhistory-projection.ts` `normalizedUsage`; `evidence.ts` drops a cost of 0 because History defaults
  a missing cost to 0). For Claude Code and Codex, that is almost never.
- The runner never sees transcript files. It reads canonical projected rows (`convergence_events`, `session_index`). Burn's
  transcript verbs (`measureSession`, `analyzeSession`) need a path on disk, so they do not fit the runner.
- The rows already carry what burn needs to price a request: `model`, `provider` and the full usage split (input, output,
  reasoning, cacheRead, cacheCreate, cacheCreate5m, cacheCreate1h).
- So the integration is **burn as the pricing engine over rows the pipeline already has**. Burn needed one new public verb
  for that: `price_usage`, in #593. Everything else (tariffs, provider precedence, context tiers, 1-hour cache writes,
  Codex reasoning rules, aliases) stays in burn. The pipeline holds no tariffs.
- The POC adds `src/session-analysis/burn-cost/session-cost.ts`. It maps rows to burn requests and folds the answers into
  F1's `SessionUsageSummary` without changing that contract: null means unknown, never 0, and nothing is imputed. It is
  not wired into the runner.

## 1. Crate API to call

| Need | burn API | Fits the runner? |
|---|---|---|
| USD for counted usage | **`price_usage(PriceUsageOptions { requests, pricing_path })`** in Rust, `priceUsage()` in Node (new in #593) | **Yes.** No session read, no ledger, no network. Returns `burn.usage-prices.v1`. |
| Tokens + cost from one transcript file | `measure_session` / `measureSession` | No. Needs a file on disk and a throwaway ai-hist SQLite store. Only Claude, Codex and OpenCode. |
| Findings, subagent tree, overhead, reasoning | `analyze_session` / `analyzeSession` (path or id); `analyze_evidence(&ai_hist::SessionEvidence, settings)` (Rust only) | Not now. Needs relayhistory `SessionEvidence`, which the TS runner does not hold. This is the path for later "evidence-backed suggestions" (see §9). |
| Pricing table | `load_pricing` / `PricingTable` | `pub(crate)` on purpose; `price_usage` is the public seam. |

`burn.usage-prices.v1` (one entry per request, in order):

```jsonc
{
  "schema": "burn.usage-prices.v1",
  "pricing": { "builtinSnapshotSha256": "<64 hex>", "overrideApplied": false },
  "prices": [
    { "model": "claude-sonnet-4-5", "priced": true, "costUsdMicros": 60000,
      "breakdown": { "model": "...", "total": 0.06, "input": 0.03, "output": 0.03,
                     "reasoning": 0, "cacheRead": 0, "cacheCreate": 0 } },
    { "model": "fx-unknown", "priced": false, "costUsdMicros": null, "breakdown": null }
  ]
}
```

Request: `{ model, source?: burn SourceKind, usage: { input, output, reasoning, cacheRead, cacheCreate5m, cacheCreate1h },
aggregate?: bool (base rates, no context tier), measured?: bool (all-zero usage is then $0 even for an unpriced model) }`.

How the crate gets in (layered-sessions-pipeline's condition: this is its own step, not part of the POC):

- **Recommended: the `@relayburn/sdk` napi package**, pinned to an exact version. It ships prebuilt
  `linux-x64-gnu` and `linux-arm64-gnu` binaries. Both pipeline images use Debian bases (`oven/bun:1.4.2-slim`,
  `node:24.21.0-bookworm-slim`), so glibc matches. Verified locally: Bun 1.4.2 loads the napi binding and the POC's
  opt-in end-to-end test passes against it.
- This still changes `package.json`, `bun.lock`, the image (a ~10 MB native .node) and CI. It is a separate slice with
  two reviews of the exact diff.
- Alternatives:
  - A wasm build alongside `scrub-bindgen`. Burn has no wasm target today. It would mean new burn CI work and a
    `rusqlite`/`ai-hist` feature split.
  - A `burn` binary subprocess. That needs a new CLI verb (`burn price --json` from stdin JSONL), and it adds an
    exec per job.
- The pricing snapshot is compiled into burn (`data/models.dev.json`, `include_str!`). That means **no models.dev
  network egress from the runner**. The table is refreshed by burn's weekly `update-pricing` workflow, and reaches the
  pipeline only through a deliberate dependency bump. `builtinSnapshotSha256` records which table priced each session.
- Unknown model → `costUsdMicros: null` → F1 `costUSD: null`.

## 2. Data mapping (pipeline row → burn request)

Source: the `apiEvent` row (`evidence.ts` ~282-320), before `normalizedEvent` drops fields.

| Pipeline field | burn field | Note |
|---|---|---|
| `model` | `model` | Burn resolves aliases, `provider/` prefixes and reattribution. A row with usage but no model makes the session cost null (`modelessRows`). |
| `source` (`claude`, `codex`, `opencode`, `copilot`) | `source` (`claude-code`, `codex`, `opencode`, `copilot-cli`) | Selects Codex's "reasoning is inside output" rule and Copilot's billing provider. grok, muse, cursor, devin and capy are left unset, so they are priced by model id with the model's own reasoning rule. |
| `usage.input` | `usage.input` | The projection already subtracts cached input for Codex, Muse and Grok (`relayhistory-projection.ts` ~340-400), which is burn's convention. |
| `usage.output` | `usage.output` | |
| `usage.reasoning` | `usage.reasoning` | **Dropped today** by `normalizedEvent`. |
| `usage.cacheRead` | `usage.cacheRead` | F1 `cachedInputTokens`. |
| `usage.cacheCreate5m` / `cacheCreate1h` | same | **Dropped today.** When only `cacheCreate` is present, the POC bills it at the 5-minute TTL. |
| `costUsdMicros` (> 0 only) | not sent | Kept as `reportedCostUsdMicros` for cross-checking. Never summed with burn's figure. |
| `provider` | not sent | Burn picks the tariff from the model id. See gap G5. |
| `eventId`, `machineId`, `source`, `sessionId`, `kind` | dedupe key | The same identity `session-metrics.ts` uses, so a replayed row is priced once. |

Session fold:

- `costUsdMicros` is the sum when **every** usage row is priced (the same rule as burn's `measureSession`).
- Otherwise it is null.
- A session with no observed usage gets null, never 0.
- Token fields keep F1's definitions, including `totalTokens = input + output`. Burn's own `totalTokens` adds cache and
  reasoning, so the two are **not** interchangeable.

## 3. Harness coverage gaps

| Harness | Pipeline usage today | burn source | Cost after integration | Gap |
|---|---|---|---|---|
| Claude Code | yes (5m/1h split) | `claude-code` | priced | none |
| Codex | yes | `codex` | priced | none |
| OpenCode | **no**: projection skips usage (`["claude","codex","grok","muse"]`, projection.ts ~313) | `opencode` | null | pipeline projection (relayhistory side), not burn |
| Grok | yes (estimated in some versions) | none (burn #489 open) | priced by model id if models.dev lists it (xai is in burn's first-party list) | burn #489 / #560 G3: `SourceKind::Grok`, fidelity policy; Grok CLI logs may lack per-turn tokens |
| Muse | yes | none | priced only if Muse's model ids resolve in models.dev | no burn issue found; needs a burn `SourceKind` and its model ids mapped |
| Cursor | **no** usage in projection | none (burn #560 G3 open) | null | Cursor usage needs relayhistory parity (#166) and then burn #560 |
| Devin, Capy | no | none | null | out of scope |
| Copilot CLI | not a pipeline source | `copilot-cli` (ledger only) | n/a | none |

Coverage is reported, never hidden: `provenance.unpricedModels` and `modelessRows` feed Stage 1 `usageCoverage`.

## 4. What burn 5.0 needs to ship (unreleased; npm is at 4.1.0)

1. **#593 `price_usage` / `priceUsage`**: required. Open, `merge-train:hold`.
2. **A 5.0 `@relayburn/sdk` release** with the linux gnu binaries. Khaliq's release decision. Not done here.
3. Recommended (G5): return the chosen tariff's provider and model id in `UsagePrice` (for example
   `tariff: { provider, model }`). Spend attribution asks for "engine, snapshot and provider" per session, and today
   burn only returns the snapshot hash.
4. Later: `SourceKind` entries for Grok, Cursor and Muse (#489, #560). Exposing `analyze_evidence` to Node, or adding
   a row-based analyze entry point, for findings.

Nothing else in 5.0 blocks this path. The transcript-parsing changes in 5.0 do not affect `price_usage`.

## 5. Output schema for Stage 1

**Phase A: no contract change.** Burn fills the existing `metrics.usage.costUSD`, and the reasoning and cache-write
fields are restored only inside the cost computation. Stage 1's `usageCoverage.knownCost` rises and nothing else moves.
sessions-intel-lead confirmed this binds cleanly.

**Phase B: additive, needs sessions-intel-lead's agreement and a version decision.** This adds an optional block. It is
not added in the POC:

```ts
metrics.cost?: {
  engine: "burn";
  schema: "burn.usage-prices.v1";
  pricingSnapshotSha256: string;
  overrideApplied: boolean;
  costUsdMicros: number | null;            // integer, mirrors costUSD
  reasoningTokens: number;
  cacheCreateTokens: number;
  byModel: { model: string; rows: number; costUsdMicros: number | null }[];
  pricedRows: number; unpricedRows: number; unpricedModels: string[]; modelessRows: number;
  reportedCostUsdMicros: number | null;    // harness's own figure, cross-check only
  // + tariff provider once burn returns it (gap G5)
}
```

The POC's `SessionCost` return value is exactly this shape, so Phase B is a persistence decision, not new math.

## 6. Overlap with existing cost fields

| Field | Today | After |
|---|---|---|
| F1 `metrics.usage.costUSD` | sum of the harness-reported cost, else null | burn's figure; null if any row is unpriced |
| `metrics.usage.*Tokens` | from normalized events | unchanged (same rows, same dedupe, same definitions) |
| projection `cost_usd_micros` | harness-reported only | unchanged; becomes `reportedCostUsdMicros` |
| `session_analysis_jobs_v2.input/output_tokens` | **the analysis model's own spend** | unchanged; not session cost, do not mix |
| session-intel `costUsdMicros` (normalize, replay views), coordination-detect, search index `cost_usd_micros` | read History's column | unchanged in Phase A. Whether they switch to burn's figure is sessions-intel-lead's call. |

The rule is one number per session per engine. Burn's figure and a harness-reported figure are never added together.
Sums, the parent/child rule and cross-harness rules belong to the spend-attribution slice
(`docs/proposals/session-intel-spend-attribution.md`, sessions-intel-lead).

Note for that slice: burn 5.0's transcript verbs bill Claude subagent turns into the parent session. `price_usage` over
rows does not do that. Each session's rows are priced as they are. So if the pipeline stores subagent sessions
separately, summing parents and children is safe here. That would not hold if a later phase used `analyzeSession` totals.

## 7. Tests

- **POC** (`src/session-analysis/__tests__/burn-session-cost.test.ts`): 10 unit tests with a fixed-tariff fake pricer.
  They cover:
  - priced sums and F1 token parity;
  - unpriced → null;
  - modeless → null;
  - no usage → null and `observed: false`;
  - replay dedupe;
  - pass-through of reasoning, cache TTLs and source;
  - reported cost kept for cross-checking only;
  - bigint micros from napi;
  - an answer of the wrong length fails loudly;
  - the source mapping.
- There is also an **opt-in end-to-end test** against a locally built `@relayburn/sdk`
  (`BURN_SDK_NODE_ENTRY=<burn>/packages/sdk-node/src/index.js bun test ...`). It passes: a real Claude model is priced
  above 0 with a 64-hex snapshot hash, and an unknown model gives null. All fixtures use fx- names.
- **burn #593**:
  - 7 Rust unit tests: unknown → null, the override tariff per bucket, measured zero, builtin parity, Codex reasoning
    not billed twice, the override fallback is reported, and request defaults;
  - 2 napi option tests;
  - 1 Node conformance test.
  - The full suites pass: 1054 SDK tests, 23 node-crate tests and 24 Node tests.
- **When wiring in** (a later slice):
  - a processor test that `metrics.usage` is unchanged except `costUSD`;
  - a golden test of `costUSD` for a Claude, Codex and Grok fx session;
  - a snapshot-hash pin test so a burn bump that moves prices shows up in review;
  - a `SOURCE.json` check that no vendored engine file changed.

## 8. Risks

1. **Vendored engine.** `engine/session-metrics.ts` and its siblings are vendored from relayhistory-cloud and pinned by
   `SOURCE.json`. Wiring burn into `aggregateUsage` means an upstream change first. Overriding `metrics.usage.costUSD`
   in the non-vendored `processor.ts` after `sessionMetrics()` avoids that, but `processor.ts` is in
   layered-sessions-pipeline's lane.
2. **The rows are the real limit, not burn.** OpenCode and Cursor have no usage in the projection, and Grok tokens may be
   estimates. Burn cannot price what the pipeline never captured.
3. **Price drift.** Burn's weekly snapshot refresh moves historical costs when the dependency is bumped. Mitigations:
   the `pricingSnapshotSha256` in Phase B, the pin test, and recomputing only on a new analysis version.
4. **Native dependency.** It only works on glibc. A future Alpine/musl image or a Windows dev box would break. Bun napi
   support is good but less exercised than Node.
5. **Tariff choice is opaque** until G5 ships. Different providers serving the same model id get burn's resolved
   tariff, not the one actually billed (Bedrock vs. first-party Anthropic, for example).
6. **Contract creep.** Phase B is a stored-field change: it needs sessions-intel-lead's agreement and a version decision.
   No migration is needed, since it lives in the result JSON in S3.
7. **Frozen main.** Nothing lands until the schema rebind and the 062220 release. Landing needs two reviews of the exact
   diff and a full chain from layered-sessions-pipeline.

## 9. Toward Florian's dashboard

- **Spend drill-down** goes org → workspace → person → project/repo (spend-attribution slice) → session → model
  (`byModel`) → row. Teams wait on Khaliq's decision about a teams table vs. IdP groups. Project tags already exist as
  `projectId`, `git_repo` and `git_branch`. The projection writes `tags: []` today.
- **Evidence-backed suggestions, near term.** These are computable from priced rows alone, and each cites the event ids
  it came from:
  - cache-hit ratio (`cacheRead` / input);
  - spend on 1-hour cache writes;
  - reasoning share of output;
  - an expensive model on short sessions (model mix vs. tokens per session);
  - unpriced-model share (a coverage warning).
- **Later.** Burn's `analyze_evidence` findings (`WasteFinding`, with `impact { tokens, costUsd }`, turn ids and
  files). This needs either relayhistory `SessionEvidence` at projection time (Rust side, relayhistory-cloud) or a new
  row-based analyze entry point in burn. Scope it separately.

## 10. Effort and sequencing

| Step | Owner | Effort | Gate |
|---|---|---|---|
| S0 burn #593 `price_usage` | burn | done (review) | burn review; merge-train hold |
| S0b G5 tariff provider in `UsagePrice` | burn | 0.5 day | — |
| S1 burn 5.0 release with linux gnu binaries | burn | 0.5 day + CI | Khaliq |
| S2 add the `@relayburn/sdk` dependency (exact pin), image + CI + bun.lock | layered-sessions-pipeline lane | 1 day | lander; after the schema rebind |
| S3 Phase A wiring: rows → `priceSessionUsage` → `metrics.usage.costUSD` in `processor.ts` (non-vendored) | layered-sessions-pipeline lane | 1-1.5 days | two reviews; no flag flip without Khaliq |
| S4 Phase B `metrics.cost` block + Stage 1 coverage/provenance | sessions-intel-lead + this lane | 1-2 days | contract version decision |
| S5 harness gaps: OpenCode/Cursor usage in projection; burn Grok/Cursor/Muse `SourceKind` | relayhistory + burn | 2-4 days | #489, #560 |
| S6 suggestions and dashboard | dashboard owner | separate | — |

The core path, S0 through S3, takes about 3 to 4 engineer days after the freeze lifts.

## POC paths (all new; no existing file changed)

- `src/session-analysis/burn-cost/session-cost.ts`
- `src/session-analysis/__tests__/burn-session-cost.test.ts`
- `docs/design/burn-session-cost.md`
