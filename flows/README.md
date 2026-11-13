# flows

The relayflows that review and verify a finished change before
`layered-sessions-pipeline` squashes it onto `main`
([docs/maintainer-workflow.md](../docs/maintainer-workflow.md)).

| Flow | What it does |
| --- | --- |
| `ci.flow.ts` | classifies the change (`lib/classify.mjs`), runs the lanes it needs side by side (`lib/lane.sh`, `lib/fleet.mjs`), the full-pipeline e2e, and with `{"github":true}` the GitHub gate (`lib/remote-ci.mjs`) |
| `pipeline-e2e.flow.ts` | the full-pipeline e2e lane on its own (`e2e/pipeline/run.ts`), locally or in a Blaxel sandbox |
| `adversarial-review.flow.ts` | a reviewer fleet that sees only the patch, never its intent |
| `context-review.flow.ts` | a reviewer fleet that judges the change against what was asked |
| `reviewer.flow.ts` | one reviewer of either fleet, as its own run |
| `cloud-review.flow.ts` | one review round as an Agent Relay Cloud run (the lenses and briefs of the two fleets) |

```sh
npm ci                     # here, once
npm run -s check           # typecheck, flows check of every flow, budgets, helper tests
```

## Provenance

Copied from AgentWorkforce/relay-app `flows/` at `5316a98` (2026-11-12), and
kept close to it so fixes can move both ways:

- **Unchanged apart from names** (`relay-app.*` flow ids are `sessions-pipeline.*`):
  `lib/fleet.ts`, `lib/fleet.mjs`, `lib/review.ts`, `lib/review-state.mjs`,
  `lib/state-dir.mjs`, `lib/shell.ts`, `lib/check-findings.mjs`,
  `lib/check-budgets.mjs`, `reviewer.flow.ts`, and their tests; pinned to the
  same `relayflows` / `@relayflows/surface` 2.0.59.
- **Adapted**: the reviewer lenses in `adversarial-review.flow.ts`,
  `context-review.flow.ts` and `cloud-review.flow.ts` (this repo's deployments,
  Postgres and object stores in place of macOS/GTK/Windows; its conventions in
  place of the changelog and the Swift contract), and the "do not run" note in
  `lib/review.ts`; `lib/changes.ts` (this repo's machine-local dirs; the lane
  selector is `lib/classify.mjs`); `lib/remote-ci.mjs` (dispatches `ci.yml` and
  `pg-proof.yml`, and refuses secret-like files and production identifiers
  before a push); `ci.flow.ts` (this repo's lanes and the classifier).
- **New here**: `lib/classify.mjs` (path rules, optional Jev escalation),
  `lib/lane.sh`, `pipeline-e2e.flow.ts`, and their tests.
- **Not copied**: relay-app's review sizing (`sized-review.flow.ts`,
  `plan-review.flow.ts`, `lib/depth.ts`, `lib/plan.ts`,
  `lib/change-summary.mjs`) and the tests that need its `agent/review.sh`.
  `cloud-review.flow.ts` takes the round's depth in its input; the submitter
  that prepares that input (relay-app's `agent/review.sh`) belongs to the
  layered agent, not to this directory.
