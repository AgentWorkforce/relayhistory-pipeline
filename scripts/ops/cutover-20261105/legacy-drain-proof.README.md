# legacy-drain-proof.mjs

Read-only evidence that relayhistory's legacy out-of-transaction writers have stopped. It covers relayhistory-cloud `docs/buffered-delivery-cutover.md`: "Prerequisite that this source CANNOT establish" and "Historical S3 batch marker bridge".

Run it from the Mini. The wrapper builds credentials in RAM (neonctl owner URL, wrangler OAuth token, the sessions-prod SSO export) and pipes them on stdin:

```
python3 -I drain-proof-creds.py                                                 # pre-hold baseline (every gate FAILs by design)
python3 -I drain-proof-creds.py --hold-start 2026-11-05T03:00:00Z --expect-version <held version id>
```

The wrapper is in the Mini session scratchpad `drain-lane/`, with a copy beside this file. Exit status is always 0. Read `overall` and `results[]` instead: each result has `PASS`, `FAIL` or `INFO`, the evidence, and the `evidenceSha256`. `reportSha256` covers the whole report.

## Gates (all must PASS before seeding/admission)

| id | writer | PASS threshold |
|---|---|---|
| W1 | Old Worker version (s3_first producer, its object phases and its waitUntil work) | Exactly one deployed version at 100% (equal to `--expect-version` when given), and at least 180 s since hold start. 180 s is the 60 s s3_first budget plus the 90 s Neon fallback plus waitUntil slack. |
| W2 | In-flight Neon legacy writers (accept_delivery_batch, apply_neon_replica_v1, set_session_uploads, reset_uploaded_sessions) | 0 transactions in pg_stat_activity |
| W3 | NEON_REPLICA queue `relayhistory-neon-replica` | backlog 0, OR delivery paused with 0 in-flight replica transactions |
| W5 | S3 batch-marker PUTs (`96bulls/<org>/live-batches/v1/...`) | 0 markers with LastModified at or after hold start, across every org that has a Neon receipt in the last 72 h |

INFO: W0 (cutover phase), W4 (replica DLQ depth), W6 (bridge gap: replica backlog + DLQ).

## Limits

- **W5** covers only orgs with a Neon receipt in the last 72 h. An org whose every batch in 72 h went s3_first and is still unreplicated would be missed. The replica lag was about 24 h at baseline, so none are expected.
- **W5 timing:** LastModified is S3 server time. Allow for clock skew by starting `--hold-start` a few seconds before the hold config went live.
- **What W3 PASS means when paused:** a paused nonzero backlog is batches the client got an ACK for that are NOT yet in Neon. Their receipts are covered only by the S3 marker bridge, their Neon read copies are missing, and messages expire after 4 days.
- **Scope:** the script does not GET marker bodies, so pending-vs-committed state is not counted here. The 299 bridge returns 503 for a same-digest pending marker, and that is never an ACK.

## Baseline

`legacy-drain-proof-baseline-20261105T0207Z.json` (reportSha256 `0ccf87d1…`).
