# Real-agent overlap scenarios

`manifest.json` is the ground truth for independent agent sessions in two disposable Git repositories. The repositories use fake `example/*` remotes for project identity and must never be pushed. A scenario becomes `complete` only after its participants have finished and its session IDs, observed start/end times, branches, and commit outcomes have been recorded.

Run the scorer with a workspace-scoped read API bearer token supplied **only through the environment** by the authorized operator. Do not type the token into a shell command that may be saved in history:

```sh
node e2e/scenarios/score.mjs --workspace "$SCENARIO_WORKSPACE_ID"
```

For iteration before redeploying the read API, run `node e2e/scenarios/score.mjs --in-process --workspace <workspace-id>`. This reads the current kind facts through PostgreSQL, builds detector lanes, and runs the detector from this checkout. To score another worktree's detector code, pass `--detector-module /absolute/path/to/packages/coordination-detect/src/index.ts`. It needs no bearer token. The result is labeled `local_fact_replay` and does not claim read API parity.

For a detector that falls back from a write's missing branch to the session directory branch, add `--branch-fallback` so the scorer evaluates the same evidence requirement. To test the upstream project-key loss independently, add `--capture-db /absolute/path/to/history.db` to an in-process run. This reads the local capture SQLite database in read-only mode and substitutes its remote-derived project key only where the kind directory lacks a repo. The result reports `replay_overrides` and marks mitigated evidence gaps. This is a counterfactual replay; it does not alter kind facts or prove that the deployed endpoint has those keys.

The scorer reaches kind through `docker exec agent-relay-local-control-plane kubectl --kubeconfig /etc/kubernetes/admin.conf`. It executes a read API request from the read API pod and a read-only per-session facts query from the PostgreSQL pod. It neither copies kubeconfig nor writes the bearer token to disk. For an offline check, provide `--response path` and `--evidence path` containing previously captured JSON; never save a token with those files.

When a ledger response is available, pass `--ledger-response path` to check scenario `j`'s ordered categories and episode start times. Start times must be within two minutes of the recorded instruction boundaries; absent start times produce `boundary_unverified`. A new episode at the recorded same-review follow-up produces `false_split`. The ledger route is not called yet because its wire contract is still being defined.
Scenario `j` reports whether its observed duration meets the manifest's 15-minute minimum. The second run spans over 16 minutes, with four unrelated tasks and one same-review follow-up. The first, shorter run remains available in local capture as a diagnostic, but is no longer the manifest's cadence ground truth.
Its local Git reflog records three branch transitions, while kind's session directory retains only the initial branch and its event facts have null branch values. The instruction boundaries still appear in the canonical transcript; the ledger must confirm the three task pivots and reject the review follow-up.
The pivot session's flag fix and the earlier flag feature both rewrote `src/flags.ts` on unmerged branches about 62 minutes apart. The approved detector's T3 rule uses a 48-hour window for rare same-file writes, so that pair expects a low-confidence `concurrent_edit` despite different task intent. Its Claude lane also needs the capture project-key override until the scrubber gap is fixed; the raw kind replay omits this finding.

The score states distinguish:

- `pending`: a participant has not been recorded or a scenario has not finished.
- `no_evidence`: at least one recorded session has no event or output facts.
- `missing_signal`: session facts exist, but a required write, commit, or revert fact is absent.
- `evidence_no_finding`: the required facts exist but the endpoint omitted the expected relation.
- `hit`, `wrong_relation`, `correct_absence`, and `unverified_no_finding`: direct comparisons with the endpoint.

Facts dated before 2000 fail the result's `quality_gate` as `evidence_defect`, even when a pair also has another score state. This catches zero-time file edits before interpreting detector misses.

`known_evidence_gaps` records independently diagnosed blockers without changing the measured state. `mitigated_evidence_gaps` shows which of those gaps the selected replay overrides cover. Capture computes a stable Git remote project key for Claude worktrees, but the deployed wasm scrubber redacts its value and Claude provides no `repo_url` fallback. The manifest calls this `scrubber_redacted_project_key`. Write facts also lack branch values (`write_branch_absent`), which can be mitigated by a detector fallback to the directory branch. The revert scenario now has both the source commit and revert outcome facts and scores a `superseded_work` hit in the local replay. Scrubbing changes belong to the relayscrubber project.

The planned and unsplit fleet sessions are complete. A first kind replay briefly had zero-time facts and missing writes because a Helm upgrade restored an older consumer image. Reprojection on the corrected consumer removed the pre-2000 facts and recovered the Claude and Codex writes. In the capture-key replay, the planned split produces no overlap alert, while the unsplit pair gets a low-confidence `concurrent_edit` rather than its expected `duplicate_work`. The planned agents edited disjoint files, so absence of an alert alone does not prove that the plan caused suppression; the ledger must identify their shared assignment. The unsplit agents exchanged messages only after both committed, which is a negative test for suppressing duplicates merely because of later chat. The ledger's episode intent is needed to distinguish duplicate outcomes from shared-file edits.

`planned_parallel`, `continuation`, and `none` expect no overlap alert. Their positive relationship labels, plus the multi-episode expectation in scenario `j`, need the ledger endpoint once its contract exists. The current overlaps endpoint supports `duplicate_work`, `concurrent_edit`, and `superseded_work`; other positive labels remain measured misses until implemented.
