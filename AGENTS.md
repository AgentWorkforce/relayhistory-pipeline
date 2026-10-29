# Agent operating rules for 96BullSessions

## Standing order for pull requests (Khaliq Gant, 2026-10-28)

Until the development environment is more stable, every agent that opens a PR
follows this order, every time:

1. **Open the PR** against `main`, with the validation evidence in the body.
2. **Subscribe to it** with Agent Relay right away (`subscribing-relay-webhooks-and-writebacks` skill: one glob, `/github/repos/AgentWorkforce/96BullSessions/pulls/<N>/**`), so reviews and comments reach the agent without anyone naming the PR.
3. **Address the feedback** as it comes in: fix, push to the PR branch, and answer each review comment with what changed.
4. **Merge it** once it is green and approved. Never merge with unresolved findings or failing checks.

This standing order covers the PR merge only. Deploys, cloud resources,
credentials, IAM changes, and applying migrations to any real database still
need Khaliq's separate, explicit OK in his own words.

## Other rules

- Never push to `main` directly; never force-push a shared branch.
- Nothing credential-like on disk on any machine: no `.env`, no saved tokens. 1Password is the system of record.
- `.96bulls/` (local secrets) and `.alchemy/` (plaintext state) stay gitignored.
