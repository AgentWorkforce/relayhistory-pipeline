# Azure Blob: the real-account proof (owed)

The Azure Blob paths are proven against an in-memory fake server (every path) and Azurite (versioning on and off).
The paths are the session-analysis runner (S2), the read API's analysis source (S2b) and the operator erase jobs and
worker (unit D). Neither stand-in can prove the account-level behaviour these paths depend on: blob soft delete,
permanent delete, the order of snapshots and versions, and the real error statuses. This page is the run that closes
that gap.

**It has not run, and nothing here may run without an explicit go.** It needs Khaliq's OK in his own words. It also
needs a disposable development storage account and short-lived credentials, issued by layered-infra-ops for this
run only, never a customer or production account. Until it has run, the runner keeps refusing
`SUMMARY_DECISION_RECORDS_ENABLED=true` on Azure (`azure_decision_records_unsupported`).

## Accounts

Four disposable accounts, or one account reconfigured between runs, each with blob versioning on unless noted:

| Account | Blob soft delete | Permanent delete | Expected |
| --- | --- | --- | --- |
| A | off | n/a | every path passes |
| B | on | allowed | every path passes; an erase run may end `not verified` (exit 3) first, then the rerun verifies |
| C | on | not allowed | the runner and the erase jobs refuse `blob_soft_delete_retained` / `azure_blob_soft_delete_retained` at the preflight, before any delete |
| D | versioning **off** | n/a | the runner refuses `blob_not_versioned`; the erase jobs refuse `azure_blob_not_versioned` |

Each account gets one container. Every object written uses `fx-` style synthetic ids. The objects an erase targets are
erased by the run. Some objects are not: the neighbouring sessions kept on purpose, the objects a refused run on C or D
leaves untouched, and any probe copy younger than 15 minutes. Those are removed at teardown (below), not by the run.
The key is mounted as a file and never printed; the run's logs carry codes and counts only.

## Steps (per account)

1. **Runner (S2).** Start `SERVICE=session-analysis` with `LOCKER_PROVIDER=azure` and the account's settings. Expect
   the startup result from the table. On A and B, seed one synthetic result key with two versions and a snapshot, and
   run one sweep pass over a ticket for it. The owned version must survive, and everything else must be erased and
   verified.
2. **Read API (S2b).** On A, with `READ_API_ANALYSIS_BUCKET` naming the container, GET a synthetic completed result
   (200), then overwrite its key with other bytes (503, the bytes never served). POST admits through a test
   admission login.
3. **Per-session erase.** Seed one synthetic session: two manifest versions, a snapshot, raw chunks and the live
   pointer, plus a neighbouring session. Run a dry run (list only, `preflight:"skipped"`), then execute. Expect the
   table's outcome. On B, expect exit 3, then a verified rerun.
4. **Per-user erase**, with `SUMMARY_DECISION_RECORDS_ENABLED=true` against a disposable database copy holding
   synthetic rows for one synthetic user. The relayfacts step runs before `begin`; the bytes are then erased and
   verified. Then make a refusal happen after `begin`, using a fault the account supports. A Shared Key has no
   per-operation permissions, so the fault is key rotation: start a run, and once `begin` has run, regenerate the
   account key that the mounted file holds. The run's next listing is answered 403, and it must refuse
   `azure_forbidden` (exit 2). Then mount the new key and rerun the same request: it must resume and verify. The
   per-operation cases (a listing 403 versus a delete 403, which stays exit 4 as `erase_azure_delete_forbidden`) are
   covered by the synthetic harness (`selfhost-eraser-azure.test.ts`, `user-erase-azure.test.ts`), not by this run.
5. **In-service worker.** With decision records on, claim one synthetic receipt. The decision-records erase runs
   before purge, the bytes erase through the gate, and the receipt is verified. On C, the claim stays
   `failed_retryable` with zero deletes, and ingest keeps serving.
6. Record, per account and step: the exit code, the printed code, the counts and the run's start and end times. No
   key, account name or URL goes into the record.

## Teardown and verification (the account owner)

After the run, the owner of the disposable accounts (layered-infra-ops):

1. On B and C (soft delete on), permanently deletes or waits out the soft-deleted copies. Then, on every account,
   lists each container the run created with `include=versions,deleted,snapshots` and removes what the run left by
   design: the neighbours, the objects a refused run on C or D did not touch, and any young probe copy. The final
   listing must show nothing recoverable.
2. Deletes every container the run created.
3. Deletes the disposable accounts, or returns them to the pool empty, and revokes the run's short-lived credentials
   (both account keys are regenerated, including the one rotated in step 4).
4. Records that teardown is done next to the run's results: codes and counts only, no account name.

## After the run

When every row of the table holds, the follow-up is one small change: remove `azure_decision_records_unsupported`,
its test row and its doc lines (`src/session-analysis/runner.ts`, `docs/session-analysis-runner.md`), and add a test
that claims and decision records run on Azure. The erase side already covers relayfacts through the shared SQL, so
nothing else changes. If any row fails, the refusal stays and the failure becomes its own fix.
