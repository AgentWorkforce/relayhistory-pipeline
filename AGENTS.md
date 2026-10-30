# Agent operating rules for 96BullSessions

## Standing order for changes to `main` (Khaliq Gant, 2026-10-29)

Agents have standing approval to commit and push code and documentation changes
directly to `main`. A pull request, external reviewer, or review-bot response is
not required. Agents also have standing approval to merge an existing pull
request once its required checks pass and the review process below is complete;
no separate merge approval is needed.

Before pushing or merging, the owning agent must review the exact final diff in
two distinct passes:

1. **Adversarial review:** try to break the change. Look for security and trust
   boundary failures, destructive behavior, credential exposure, data loss,
   race conditions, error-path failures, compatibility regressions, missing
   negative tests, and assumptions that do not hold in production.
2. **Contextual review:** verify that the change fulfills the person's actual
   request and fits the surrounding architecture, conventions, documentation,
   operational model, and current repository state. Check adjacent consumers
   and keep related behavior consistent.

Address every material finding, rerun validation proportional to the risk, and
repeat both reviews if the fixes materially change the diff. Record the review
and validation evidence in the commit message, pull-request body, or final
handoff. If the agent cannot complete either review confidently, or required
checks fail, do not push or merge; use a pull request or ask for help instead.
For a direct push, run all relevant checks available locally first. If a
push-triggered check later fails, own the failure through a prompt follow-up fix
or a normal revert commit; never rewrite `main` to hide it.

This standing order covers code integration only. Deploys, cloud resources,
credentials, IAM changes, and applying migrations to any real database still
need Khaliq's separate, explicit OK in his own words.

## Other rules

- Never force-push `main` or another shared branch.
- Nothing credential-like on disk on any machine: no `.env`, no saved tokens. 1Password is the system of record.
- `.96bulls/` (local secrets) and `.alchemy/` (plaintext state) stay gitignored.
