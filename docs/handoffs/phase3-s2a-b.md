# Handoff: phase3-s2a-b (relayhistory-pipeline, Phase 3 search work)

Agent: phase3-s2a-b, on linuyc-desktop. Lead and lander: layered-sessions-pipeline (Agent Relay).
Repo: AgentWorkforce/relayhistory-pipeline (public; pull requests disabled; the lead squashes or fast-forwards review branches onto main).
Rules that still apply: code, tests and docs only; no deploy, no migration applied anywhere real, no flag on any install, no secrets, no production identifiers in the repo; never push to main; no force-push after a head is reported.

## State at checkpoint (2026-11-22)

Nothing running. No uncommitted work of mine. Safe to interrupt.

### Landed on main (done)
- Staged reconcile: migration 062110 (search_reconcile_* tables and functions), the module, the consumer wiring behind SEARCH_INDEX_STAGED_RECONCILE_ENABLED (default off), sizing docs, slice 3 (summary line, attempts cap 24 with a slow retry every 72nd claim, runbook "A stuck or failed staged run" in docs/local-stack.md).

### Open branches
1. review/phase3-s2a-b-idx-skip @ 09694cf95d22e7d0cdd4ac96fb3924c951ba8d94
   - SEARCH_INDEX_SKIP_CURRENT_ENABLED (default off, hosted Lambda): a search pass returns before any S3 read or ClickHouse query when the canonical session_directory row proves a complete pass covered the live pointer set (event_projection_ready, event_projection_sources equal to the live manifest_key -> revision map, version 1; facts state complete when ClickHouse output facts are on).
   - Status: the lead's verdict was PASS WITH NOTES; notes N1-N3 are fixed in 09694cf9. Waiting for the lead to land it (it rebases mechanically).
   - Tests: search-index-skip-current.pglite (11/0), search-index-skip-off-golden (flag off, captured at the base).
2. review/phase3-s2a-b-lock-hold @ a27adfb1 (head at checkpoint)
   - Mine: 06e58b9a (pointer-stage flag-off golden at the base), 315b39b8 (SESSION_POINTER_PREREAD_ENABLED: object-store reads before the pointer scope lock; stamp of the scope's session_index rows re-read under the lock; retry once, then retryable pointer_scope_changed).
   - Not mine: be9282c9 and 3770d954 (author phase3-s2a-b-codex) and a27adfb1 (khaliqgant), pushed after my last report. They supersede my uncommitted "unchanged eTag" change, which I dropped. I have not reported this head to the lead.
3. review/phase3-s2a-b-codex-search-preread @ 789cadd7: khaliqgant's, checked out in my clone ~/work/idx-skip. I have not touched it.

### Waiting on a decision
- Khaliq or the lead: who owns review/phase3-s2a-b-lock-hold now, and whether its head a27adfb1 is the one to review. I do not push there again until told.
- Khaliq's own word for any enable or deploy of the three flags: all default off, set nowhere.

## To resume
1. Read the latest Agent Relay messages from layered-sessions-pipeline (landing of idx-skip; lock-hold ownership).
2. Clones: ~/work/phase3-s2a-b (staged work), ~/work/idx-skip (idx-skip and lock-hold; currently on khaliqgant's search-preread branch, so do `git checkout` your own branch before working and never commit onto someone else's).
3. Verify before reporting (repo root, one at a time, check every rc): bunx tsc --noEmit -p tsconfig.json; bun scripts/check-engine-identifiers.ts; bun scripts/ci/unit-test-shards.ts --check --of 4; git diff --check <base>; bun test --timeout 300000 ./<each changed test file>; bun test --timeout 30000 ./stacks; cd packages/session-read-worker && bun run typecheck.
4. Report to the lead leading with the outcome: full head sha, file list, rc with counts, open questions.
