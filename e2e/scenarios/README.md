# Real-agent overlap scenarios

`manifest.json` is the ground truth for independent agent sessions in two disposable Git repositories. The repositories use fake `example/*` remotes for project identity and must never be pushed. A scenario becomes `complete` only after its participants have finished and its session IDs, observed start/end times, branches, and commit outcomes have been recorded.

Run the scorer with a workspace-scoped read API bearer token supplied **only through the environment** by the authorized operator. Do not type the token into a shell command that may be saved in history:

```sh
node e2e/scenarios/score.mjs --workspace "$SCENARIO_WORKSPACE_ID"
```

For iteration before redeploying the read API, run `node e2e/scenarios/score.mjs --in-process --workspace <workspace-id>`. This reads the current kind facts through PostgreSQL, builds detector lanes, and runs the detector from this checkout. To score another worktree's detector code, pass `--detector-module /absolute/path/to/packages/coordination-detect/src/index.ts`. It needs no bearer token. The result is labeled `local_fact_replay` and does not claim read API parity.

The scorer reaches kind through `docker exec agent-relay-local-control-plane kubectl --kubeconfig /etc/kubernetes/admin.conf`. It executes a read API request from the read API pod and a read-only per-session facts query from the PostgreSQL pod. It neither copies kubeconfig nor writes the bearer token to disk. For an offline check, provide `--response path` and `--evidence path` containing previously captured JSON; never save a token with those files.

When a ledger response is available, pass `--ledger-response path` to check scenario `j`'s ordered categories. The ledger route is not called yet because its wire contract is still being defined.
Scenario `j` also reports whether its observed duration meets the manifest's 15-minute minimum. The first four-pivot run is shorter and remains a cadence-coverage gap until the session is extended or rerun.

The score states distinguish:

- `pending`: a participant has not been recorded or a scenario has not finished.
- `no_evidence`: at least one recorded session has no event or output facts.
- `missing_signal`: session facts exist, but a required write, commit, or revert fact is absent.
- `evidence_no_finding`: the required facts exist but the endpoint omitted the expected relation.
- `hit`, `wrong_relation`, `correct_absence`, and `unverified_no_finding`: direct comparisons with the endpoint.

Facts dated before 2000 fail the result's `quality_gate` as `evidence_defect`, even when a pair also has another score state. This catches zero-time file edits before interpreting detector misses.

`planned_parallel`, `continuation`, and `none` expect no overlap alert. Their positive relationship labels, plus the multi-episode expectation in scenario `j`, need the ledger endpoint once its contract exists. The current overlaps endpoint supports `duplicate_work`, `concurrent_edit`, and `superseded_work`; other positive labels remain measured misses until implemented.
