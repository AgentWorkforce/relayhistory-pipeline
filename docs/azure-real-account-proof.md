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

Each account gets one container. Every object written uses `fx-` style synthetic ids and is erased by the run itself.
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
   verified. Then repeat with the key's list permission removed after `begin` (a refusal after begin), restore the
   permission, and rerun the same request: it must verify.
5. **In-service worker.** With decision records on, claim one synthetic receipt. The decision-records erase runs
   before purge, the bytes erase through the gate, and the receipt is verified. On C, the claim stays
   `failed_retryable` with zero deletes, and ingest keeps serving.
6. Record, per account and step: the exit code, the printed code, the counts and the run's start and end times. No
   key, account name or URL goes into the record.

## After the run

When every row of the table holds, the follow-up is one small change: remove `azure_decision_records_unsupported`,
its test row and its doc lines (`src/session-analysis/runner.ts`, `docs/session-analysis-runner.md`), and add a test
that claims and decision records run on Azure. The erase side already covers relayfacts through the shared SQL, so
nothing else changes. If any row fails, the refusal stays and the failure becomes its own fix.
