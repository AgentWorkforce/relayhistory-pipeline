# Offline coordination alerts and evals

This is an offline collector inside sessions-pipeline. It produces `alerts.json`
and a Markdown table, not notifications. It adds no runtime service, schema,
queue consumer, production reader or deployment. Nothing depends on legacy Cloud
execution or the closed coordination-report PR #211.

## Components

```text
Owner-authorized session tool results
                ↓ normalize explicitly to Dataset
source.ts       → scrubbed, bounded evidence with provenance and expiry
                ↓
evidence.ts     → time window / visibility / as-of evidence
candidates.ts   → ranked session pairs (files, task references, topic terms)
                ↓
judgment.ts     → deterministic shared-write baseline
model.ts        → optional customer-selected model judge
                ↓ checked citations from both sessions
collector.ts    → candidate alert records, stable IDs, coverage
                ├→ report.ts → Markdown table + JSON (CLI)
                └→ evaluation.ts → labelled retrieval and judgment metrics

A future sender consumes candidate records under a separate contract.
```

The public TypeScript contracts live in `src/overlap-alerts/contracts.ts`.
`collectAlerts(dataset, judge, options)` is the reusable core. CLI and filesystem
artifacts are adapters. Both baseline and model implement `Judge`; changing the
model transport does not change matching, evaluation, or delivery.

## Run the checked-in synthetic baseline

```sh
mkdir -p .overlap-eval
bun run alerts:collect \
  --input src/overlap-alerts/fixtures/synthetic.json \
  --out .overlap-eval/baseline-table \
  --as-of 2026-11-07T12:00:00Z --replay
bun run alerts:eval \
  --input src/overlap-alerts/fixtures/synthetic.json \
  --cases src/overlap-alerts/fixtures/cases.json \
  --out .overlap-eval/baseline-eval
```

Output directories must be new. Inspect `alerts.md`, `alerts.json`, and
`eval.json`. Every row has scope, both session and user identities, an opportunity
kind, reason, exact supporting quotes, stable ID, evidence fingerprint, expiry,
and `status: candidate`. IDs deduplicate the same pair/kind across reruns;
fingerprints distinguish changed evidence. These are candidate assessments, not a
notification outbox or a durable SQL table.

The 12 synthetic cases cover duplicate work, potentially conflicting edits,
cross-repository dependencies, reusable findings, shared outcomes, and missing
ownership. Controls cover deliberate collaboration, shared reads, unrelated
repositories with equal paths, generic topics, ambiguity, and late evidence.
The file-only baseline detects one of six positive cases and misses five. That
is a baseline limitation, not an evaluation of model quality. Mocked transport
tests prove the contract, not that a model recognizes these opportunities.

## Production-derived cases: source and labeling protocol

No production session content is checked in. Obtain samples through supported
access. If the signed-in MCP endpoint returns Cloudflare 1010
`browser_signature_banned`, stop; do not use a different user agent, raw pipeline
database, S3, or a direct read API as a workaround.

The supported source, confirmed with pipeline-lead, is the normal signed-in
`search_shared_sessions`, `get_shared_session`, and `list_team_activity` tools.
Use each owner's authorized login, or the applicable explicit sharing grant;
a fixture field is not an authorization check. This command deliberately does
not perform those reads. Normalize only successfully authorized tool results to
`Dataset`, then pass the normalized file to:

```sh
bun run alerts:import --input authorized-evidence.json --out .overlap-eval/sample
```

Normalize and scrub in memory before writing any source file. `importEvidence`
is exported for this purpose; the CLI also scrubs before collection/evaluation.
The existing pipeline scrubber removes known credential patterns but cannot
prove all sensitive content has been removed. Do not commit transcripts, titles,
quotes, labels, or derived alert tables from production. Keep the minimal reviewed
sample in a private ignored run; scrub both task identifiers and text if needed.

Record the actual reader, scope, selected owner IDs, capture time, selection
method, and completeness. Use full remote repository identities, relative paths,
and only observed successful edits as `write`; a file mention/touch is `unknown`.
Do not invent shared visibility, completion, relationships, or arrival times.
Unknown visibility is excluded from the shared corpus. Current titles are display
metadata, never retrospective retrieval evidence. A summary must carry the time
it became available, not the first event time in the session it summarizes.

Start with a bounded common time window of Will and Khaliq's authorized sessions.
Include all selected sessions in that window as distractors, not just known
collisions. Label pairs independently of detector output using these questions:

1. What concrete benefit would connecting these agents have had at this time?
2. Is it duplicate effort, conflicting changes, a dependency, a reusable finding,
   a shared outcome, or an ownership gap? Which evidence on each side supports it?
3. Was it already coordinated, intentionally independent, complete, or only about
   the same broad topic? Label those `suppress`; use `uncertain` for ambiguity.
4. Was the evidence actually available by the cutoff? Later discoveries cannot
   support a positive replay label.

Use the fixture `cases.json` as the label schema. Record rationale, evidence IDs,
label source (`human` or `agent_reviewed`), workstream and split. Put a session and
all related workstreams into one split; keep held-out labels out of prompts and
threshold tuning. Have the owners review agent-proposed labels before calling
scores human-validated. Report per-kind precision/recall, retrieval misses,
failures, uncertain suggestions, and unlabelled candidate pairs. A curated
balanced set does not estimate production alert precision: also sample ordinary
windows and measure false alerts per active user-hour before choosing thresholds.

`--replay` uses event time AND `availableAt`. Unknown arrival time is excluded and
counted. Without arrival timestamps, use the default `retrospective` mode and
label the result accordingly: it cannot establish live alert timing. Repository
identity and shared scope remain fixed snapshot metadata; this version does not
reconstruct historical authorization or repository moves.

## Optional enterprise model

Pass `--semantic` to collect/eval. Set `OVERLAP_MODEL_URL` to the full
Responses-compatible endpoint, `OVERLAP_MODEL` to the model identifier, and
`OVERLAP_MODEL_KEY` in memory through your approved secret mechanism. There is no
default hosted endpoint. Existing destination policy supports HTTPS, loopback,
and explicitly opted-in cluster HTTP. No environment files or saved tokens.
Configure a customer-controlled endpoint before using private evidence; the
command sends pair evidence to exactly that endpoint with `store: false`.

The judge receives at most the latest 12 events per session and 2,000 characters
per event; omitted/truncated context is disclosed to it and counted in output
coverage. Responses are bounded and must cite exact evidence from both sessions.
Failures and ungrounded responses become uncertain assessments, never alerts.
Transcript instructions are untrusted. Exact citation validation establishes
provenance, not correctness of the model's conclusion.

Retrieval currently matches exact task references, canonical repository/file
identities, or at least three shared topical terms. It can find cross-repository
opportunities, but misses paraphrases with little vocabulary overlap. The eval
counts those misses. Use real labels to decide whether embedding retrieval or
structured intent extraction improves recall before adding that component.

At least one session must have activity in the last six hours; evidence can span
seven days to reuse an older finding. Same-owner/private/explicitly related pairs
are excluded. The default collection budget is 20 pairs (maximum 200), with
omitted pairs reported. No model-derived result automatically connects agents.

## Retention and a future sender

Production artifacts expire at most seven days after capture. Derived runs keep
the source expiry; importing does not renew it. Directories use mode 0700, files
0600. Run `bun run alerts:purge --root .overlap-eval` to delete expired runs;
there is no installed scheduler. Consumers refuse expired production inputs.
Use `bun run alerts:erase --run .overlap-eval/RUN` when any included source session
is erased or access is revoked. Each run manifest lists all source session keys;
erase every dependent run, including copies outside this root. Labels are also
derived sensitive artifacts and belong in the same private lifecycle.

Before a live worker or sender ships, review a separate shared-data contract.
That component must recheck current authorization/visibility and expiry, honor
notification settings, deduplicate persistent alert IDs, implement cooldowns,
and handle revocations/erasure. Current CLI inputs are trusted authorized
snapshots, not tenant credentials. Do not expose the pure collector as a public
API or schedule it against production merely because these offline evals pass.
