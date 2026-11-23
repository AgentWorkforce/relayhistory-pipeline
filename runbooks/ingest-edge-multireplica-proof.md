# Ingest edge on two replicas: the S3-ledger proof

`scripts/proof-ingest-edge-multireplica.ts` proves that the self-host session ingest edge
(`SERVICE=session-ingest`, the chart's `ingestEdge`) admits every upload exactly once when two
replicas share one S3 upload ledger (`INGEST_LEDGER=s3`, [docs/selfhost-composition.md](../docs/selfhost-composition.md)
section 5). It is for a **dev or stage** deployment only. It never deploys, scales or dispatches
anything. It refuses production-looking targets before it sends a single request.

## What it does

1. **Preflight.** The run stops (exit 2, `{"refused":[codes]}`) unless every one of these holds:
   - `--i-know-this-is-dev` is set.
   - `--target-env` is `dev`, `stage` or `sandbox`.
   - **Every target name passes two checks.** The targets are the deployment, the ingest host, the control
     host, the `--endpoint` host, the canonical and ledger buckets, and the kube namespace, deployment and context.
     - **Deny-list:** no target name may contain a `prod`, `production`, `prd` or `live` token.
     - **Positive marker:** each must carry a non-production marker (`dev`, `development`, `stage`, `staging`,
       `sandbox` or `test`).
     - **Loopback exemptions:** a loopback ingest or control host, and the buckets behind a loopback
       `--endpoint` (the local mode), are exempt from the marker but never from the deny-list.
     - **`--endpoint` scheme:** it must be https, or plain http to `localhost` / `127.0.0.1` / `[::1]` only.
     - **The direct-upload PUT target** (the presigned URL relay-control answers) passes the same checks before
       any PUT: https or loopback, no production token in the host or the path-style bucket, and a marker in one
       of them. A refused URL is never sent to; the run fails with its `upload_url_*` code.
     - **`AWS_ENDPOINT_URL` / `AWS_ENDPOINT_URL_S3`.** The script's S3 client reads no environment, so these are
       ignored: without `--endpoint` it always talks to AWS S3. The run refuses if either is set without
       `--endpoint`; use `--endpoint`, which is checked.

     > **Warning:** loopback means a stack running on this machine (the local mode). Never reach a remote
     > deployment through a `kubectl port-forward`, an ssh tunnel or any other forward to a loopback port: that
     > skips the marker check on a host and bucket the script cannot see, and plain http then carries the tokens.
     > Point the script at the deployment's own https names instead.

     The fence-authority marker is not the only guard.
   - `--reset-user-is-synthetic` is set. The mid-run reset is **user-wide**: `resetUploadedSessions` tombstones
     every session user 0 owns, not only the run's. User 0 must be a dedicated synthetic user.
   - User 0 owns no live session yet (a read-only list of its ownership stubs against its fence). A previous
     complete run leaves nothing live: its mid-run reset tombstoned its reset sessions, and its end-of-run reset
     (after the read-back, step 4) its after sessions. So a rerun with the same synthetic user 0 is allowed. A run that stopped before
     its end-of-run reset (killed, or the reset refused) leaves live stubs, and the next run refuses
     `reset_user_owns_sessions_outside_the_run`: use a fresh synthetic user 0 then.
   - At least two distinct synthetic users are named, and each token variable holds an `sh_at_` token.
   - The org is covered by a valid fence-authority marker, and no `.stop.json` kill switch is present.
   - Two or more replicas are confirmed, one of two ways:
     - with `--kube-namespace`, `--kube-deployment` and `--kube-context` (required: the script never uses the
       current kubectl context), the script itself runs read-only `kubectl get` and `kubectl logs`. It requires
       `readyReplicas >= 2`, `INGEST_LEDGER=s3`, and `INGEST_LEDGER_LOG_COMMIT` unset or `off`;
     - or, if the operator has checked by hand (below), with `--replicas-confirmed`.

   It prints the config it sees (target, shape and, with kubectl, the deployment's ledger, admission and
   cap variables) on stderr. It never prints a token, user id or key.
2. **Burst** (default with 3 users: 178 unique batches, about 436 upload requests, 6 origins).
   - Each user uploads from two origins at once. Origin `a` uses `POST /v1/delivery/batches`. Origin `b`
     uses the presigned upload inbox (relay-upload-inbox-v1: credentials from relay-control, a create-only
     PUT, then `GET /v1/delivery/admission?uploadKey=`) when `--control-url` is set; otherwise it also uses HTTP.
   - Each session is a chain of record revisions 1..N. Each revision is sent only after the previous one
     is terminal (in `async` mode, it polls `GET /v1/delivery/admission`).
   - Every other revision is sent as `--retry-copies` concurrent copies of **the same batch**. That is
     the retry storm the two replicas race on.
   - The batches are the pipeline e2e's Desktop-shaped batches (`e2e/pipeline/fixtures.ts`), under ids
     that are unique to the run.
3. **Reset mid-burst.** Once user 0's sessions are admitted, while the other users are still uploading:
   - The script fires one more revision for each of user 0's sessions (the race batches: they may or may
     not land).
   - First, a read-only check: the live sessions user 0 owns must be exactly the run's reset sessions. If there
     is any other, the reset is **not** applied and the run fails `reset_refused_foreign_sessions`.
   - It resets user 0 through `ledger-s3.ts` `resetUploadedSessions`, then replays the reset with the same
     request id. The e2e's `controls.s3` does the same: self-host under `INGEST_LEDGER=s3` serves no reset route.
   - It then re-uploads user 0's reset sessions at revision 1000. These must never land.
   - It also uploads one new session per origin. These must land.
4. **Quiescence.** The script polls every 5 s until all of these are clean:
   - no admission-inbox attempt with content is left under the run's machines;
   - no upload-inbox object is left under its origins;
   - every batch record is out of `pending`.

   `--quiesce-timeout-s` (default 600) bounds the whole run, and the read-back gets 120 s more.
   After the read-back, the **end-of-run reset** tombstones the run's after sessions with a second user-wide
   reset of user 0 (request id `mrp-cleanup-<run_id>`). It runs only if the live sessions user 0 owns are a
   subset of the run's after sessions (the same read-only check as the mid-run reset), so it never reaches
   further than the mid-run reset could. It fails `cleanup_reset_refused_*`, `cleanup_reset_not_run` or
   `cleanup_count_not_equal_live_after_sessions` otherwise.
5. **Assertions** (`evaluate()`). Each failure is a fixed code in the receipt.
   - Every batch is terminal. The only refusals allowed are `delivery_privacy_superseded`, and only on a race or blocked batch.
   - Every sent batch has exactly one S3 ledger batch record, `committed`, whose `receiptId` is the one the
     upload answered. Concurrent copies all got the same receipt.
   - Each record's revision-index entry is the highest admitted revision. It is never lower (no regression)
     and never higher.
   - The session's canonical `manifest.json` revision equals the number of admitted batches. Each admitted
     batch carries a new, higher revision, so a duplicate admission of a retried batch writes the manifest
     once more and fails as `duplicate_admission_manifest_written_twice`. There is exactly one manifest per session.
   - **Reset honoured.** No index entry at or above revision 1000 exists for a reset session, and the manifest
     moved at most once past its pre-reset value (only the race batch, if it was claimed first).
   - The reset tombstoned **exactly** the run's sessions for user 0 (`reset_count_not_equal_run_sessions`
     otherwise: fewer is a missed tombstone, more means it reached a session outside the run). The replay
     answered the same count, and the fence records the reset.
   - Nothing is in the dead-letter prefix. No client retry code falls outside the contention set
     (`EXPECTED_RETRY_CODES`, for example `ledger_*_contended`, `ledger_attempt_superseded`, `tenant_limit`,
     `delivery_busy`; extend it with `--allow-retry-code`).
   - With kubectl, the pod logs since the start must also show:
     - both pods did work (an upload or an admission);
     - no `dead_lettered` line;
     - no retry or refused `error_code` outside the same set.

   The logs carry no tenant, so other dev traffic in the window counts too.
6. **Receipt.** One JSON receipt on stdout, also written to `--receipt-out` if given. It holds counts only,
   plus `verdict` and `failures: {code: count}`. The target names (deployment, hosts) are not in the receipt
   or on stderr. The verdict is one of:
   - `PASS`: every assertion held, and the per-replica logs (kubectl, or the local mode) show the work spread.
   - `PASS_UNATTRIBUTED`: every storage assertion held, but there is no per-replica evidence
     (`--replicas-confirmed` without kubectl), so nothing shows a second replica did any of it. **It is not a
     two-replica proof.**
   - `FAIL`.

   Exit codes: 0 on `PASS` or `PASS_UNATTRIBUTED`, 1 on `FAIL`, 2 refused.

## Run it

Prerequisites, all by the operator: a dedicated test org and workspace on the dev deployment; two or three
synthetic users in it with `rth:sync`, and a relay-control access token (`sh_at_`) for each; read
access to the canonical bucket (and to the ledger bucket, if split); and write access to the users'
`upload-fences.json` and `ledger/owners/` (needed by the reset). Use the same grants as the operator role in section 5.
User 0 (the first `--user`) must be a dedicated synthetic user that is used for nothing else.

> **Needs Khaliq's own OK first** (AGENTS.md "What still needs Khaliq's explicit OK"): deploys, cloud
> resources and IAM changes need Khaliq's separate, explicit OK in his own words. A request relayed by
> another agent is not that OK, except as "Relayed approvals" in AGENTS.md describes. This covers changing dev values, scaling with kubectl, pausing the
> reconciler and granting write access.


### 1. Scale dev to two replicas with the halved caps

The chart pins `replicas: 1` (`charts/sessions-pipeline/templates/ingest-edge.yaml`, `strategy: Recreate`) and
renders no `INGEST_LEDGER_LOG_COMMIT` (so it is off). For the proof:

   > **Needs Khaliq's own OK first** (AGENTS.md "What still needs Khaliq's explicit OK"): deploys, cloud
   > resources and IAM changes need Khaliq's separate, explicit OK in his own words. A request relayed by
   > another agent is not that OK, except as "Relayed approvals" in AGENTS.md describes. This covers changing dev values, scaling with kubectl, pausing the
   > reconciler and granting write access.

1. Halve the per-replica caps. The S3 tenant limits are per replica, so two replicas at half caps bound a
   tenant like one replica at full caps (docs/selfhost-composition.md section 5, "The caps are per replica").
   Set these in the dev values and roll out through the normal dev release:

   | value | env | single replica (example) | each of two replicas |
   | --- | --- | --- | --- |
   | `ingestEdge.ledger` | `INGEST_LEDGER` | `s3` | `s3` |
   | `ingestEdge.s3TenantLimit` | `INGEST_S3_TENANT_LIMIT` | `true` (`on`) | `true` (`on`) |
   | `ingestEdge.maxPerUser` | `INGEST_MAX_PER_USER` | 4 | 2 |
   | `ingestEdge.maxPerOrg` | `INGEST_MAX_PER_ORG` | 32 | 16 |
   | `ingestEdge.admission.concurrency` | `SELFHOST_ADMISSION_CONCURRENCY` | 8 | 4 |
   | `ingestEdge.admission.asyncMaxPerOrg` | `INGEST_ASYNC_MAX_PER_ORG` | 4 | 2 |
   | (not rendered) | `INGEST_LEDGER_LOG_COMMIT` | unset | unset (off) |

   Use the dev deployment's current values as the "single replica" column. Halve each of them, rounding
   down, with a minimum of 1.
2. Scale. Until the chart has a replicas value, use kubectl, and pause the dev environment's reconciler
   for this release so that it does not scale the deployment back during the run:

   ```sh
   kubectl -n <namespace> scale deployment/<release>-ingest-edge --replicas=2
   kubectl -n <namespace> rollout status deployment/<release>-ingest-edge
   ```
3. Check it by hand (this is what `--replicas-confirmed` attests):

   ```sh
   kubectl -n <namespace> get deployment <release>-ingest-edge -o jsonpath='{.status.readyReplicas}{"\n"}'   # 2
   kubectl -n <namespace> get pods -l app.kubernetes.io/component=ingest-edge -o wide                         # 2 Running, ideally on 2 nodes
   kubectl -n <namespace> get deployment <release>-ingest-edge -o json \
     | jq -r '.spec.template.spec.containers[].env[] | select(.name|test("INGEST_LEDGER|MAX_PER|ADMISSION|TENANT_LIMIT")) | "\(.name)=\(.value)"'
   ```

### 2. Dry run (no network)

```sh
bun scripts/proof-ingest-edge-multireplica.ts --plan-only --i-know-this-is-dev --target-env dev --reset-user-is-synthetic \
  --deployment <dev deployment name> --ingest-url https://<dev ingest origin> --org <test org> --workspace <test workspace> \
  --user <user id 1>:PROOF_TOKEN_1 --user <user id 2>:PROOF_TOKEN_2 --user <user id 3>:PROOF_TOKEN_3 \
  --bucket <canonical bucket> --region <region>
```

### 3. Run

Supply the tokens and the AWS credentials from the secret store through the environment only (for
example `op run --`). Never put them on the command line or in a file.

For example, with a `proof.env` that maps each variable to a secret reference (`PROOF_TOKEN_1=op://…`,
`AWS_ACCESS_KEY_ID=op://…`, and so on; references only, never values):

```sh
op run --env-file proof.env -- \
bun scripts/proof-ingest-edge-multireplica.ts --i-know-this-is-dev --target-env dev --reset-user-is-synthetic \
  --deployment <dev deployment name> --ingest-url https://<dev ingest origin> --org <test org> --workspace <test workspace> \
  --user <user id 1>:PROOF_TOKEN_1 --user <user id 2>:PROOF_TOKEN_2 --user <user id 3>:PROOF_TOKEN_3 \
  --bucket <canonical bucket> --region <region> [--ledger-bucket <ledger bucket> --ledger-region <region>] \
  [--sse-kms-key-id <key arn, if the bucket policy requires it on PUT>] \
  --kube-namespace <namespace> --kube-deployment <release>-ingest-edge --kube-context <dev context> \
  [--control-url https://<dev relay-control origin>] \
  --receipt-out ~/.agent-tmp/<you>/ingest-edge-multireplica-receipt.json
```

- The user ids must be the ids relay-control's introspection answers for those tokens. The S3 ledger is
  keyed by them; a wrong id shows up as `admitted_without_batch_record`.
- Inside the cluster, `--credentials podIdentity` uses the pod's EKS Pod Identity instead of the `AWS_*` keys.
- Knobs: `--sessions-per-origin` (4), `--revisions` (4; the non-reset users send twice as many),
  `--retry-copies` (4), `--concurrency` (12 requests in flight), `--quiesce-timeout-s` (600).

Afterwards, scale back to one replica, restore the caps, and resume the reconciler. Like the scale-up, these
are dev infrastructure changes and need Khaliq's own OK (above).

## Local mode (no cloud, no credentials)

`scripts/proof-ingest-edge-multireplica-local.ts` runs the same burst, reset and assertions (`runProof()` in the
proof script) against two ingest-edge processes on this machine. Run it before the dev run, and after any change
to the S3 ledger, the async consumer or upload promotion.

```sh
docker build -f docker/services.Dockerfile -t 96bulls/services:mrp-local .     # or omit --image: the script builds and removes its own
bun scripts/proof-ingest-edge-multireplica-local.ts --image 96bulls/services:mrp-local \
  [--sessions-per-origin 12]  [--revisions 4] [--retry-copies 4] [--concurrency 12] [--receipt-out ~/.agent-tmp/<you>/local-receipt.json] \
  [--runs 2]
```

`--runs N` (1 to 3) runs the whole proof N times back to back on the same stack, users and user 0, each run
after the dev mode's pre-run check. Run 2 passing proves a rerun with the same synthetic user 0 works. Each
run's log tally covers only its own window. With `--receipt-out`, run 2's receipt goes to `<file>.run2`.

- **Stack.** A fresh compose project, `e2e/selfhost/compose.yaml` with `e2e/multireplica/compose.yaml` layered
  over it: Postgres (the ingest's startup role check needs it; no upload accounting touches it), MinIO, the
  migrations and role bootstrap, the Control introspection stub, and **two** ingest-edge processes `ingest-r1`
  and `ingest-r2` with identical settings on one bucket and key root:
  - `INGEST_LEDGER=s3` and `INGEST_LEDGER_LOG_COMMIT=off`;
  - `SELFHOST_INGEST_ADMISSION=async`, `SELFHOST_ADMISSION_CONCURRENCY=8`, `SELFHOST_ADMISSION_SCAN_MS=2000`
    (so each process's scan also picks up the other's inbox objects);
  - `INGEST_S3_TENANT_LIMIT=on`, `INGEST_ASYNC_MAX_PER_ORG=2`, `INGEST_MAX_PER_USER=2`;
  - `HISTORY_UPLOAD_DIRECT=on` with a per-run witness key.

  Override with `E2E_MRP_CONCURRENCY`, `E2E_MRP_SCAN_MS`, `E2E_MRP_ASYNC_MAX_PER_ORG`, `E2E_MRP_MAX_PER_USER`, `E2E_MRP_MAX_PER_ORG`.
- **Tenants.** Three synthetic users (`--users`) in one test org and workspace, with per-run `sh_at_` tokens in
  the stub's table. The script writes the org's fence-authority marker into the local bucket before the burst.
- **Load.** Every HTTP attempt (POST and admission poll) goes to the next process, round-robin
  (`http_requests_by_target`). Origin `b` uses the upload inbox. The script stands in for relay-control: it reads
  the user's privacy-journal seq, signs the witness and does the create-only PUT itself, with no presigned URL.
  Both consumers promote from the same inbox.
- **Extra assertions** (`requireAdmissionsPerReplica`; the logs are this run's only traffic):
  - each process must have committed batches of its own (`firstWrites`, admissions answered `accepted*`);
  - across both processes, `accepted` lines must not outnumber the admitted batches;
  - every admitted batch must be covered by an `accepted` or a `replay_fast` line. A `replay_fast` is how one
    process takes over a batch the other wrote but had not committed yet; the other's commit then answers
    `ledger_attempt_superseded`, and its retry answers `replay`;
  - neither process restarted.
- **Cleanup.** The compose project, its volumes and network, the per-run certificate directory and (without
  `--image`) the image are removed on exit. `--keep` leaves the project up and prints how to remove it.
- **Exit codes.** 0 every run PASS, 1 any FAIL, 2 the stack did not come up or a run's pre-run check refused.
- On a host with `umask 077`, build the image from a tree that is world-readable (`chmod -R a+rX` the checkout,
  excluding `node_modules`). Otherwise the image's `/entrypoint.sh` and sources are unreadable to its `bun` user.

## If it fails

| failure code | look at |
| --- | --- |
| `duplicate_admission_manifest_written_twice`, `retry_copies_got_different_receipts`, `batch_record_receipt_mismatch` | Exactly-once is broken: two replicas both admitted one batch. Check the claim CAS on `ledger/batches/` (`ledger-s3.ts` claim/commit) and the pod logs around `ledger_batch_contended` / `ledger_attempt_superseded`. **Stop the multi-replica rollout.** |
| `revision_regressed_or_lost`, `revision_ahead_of_any_admitted` | The revision-index merge (`mergeIndex`, the 4096 shards) lost or reordered a write under contention. Look for `ledger_revision_contended` retries that exhausted their attempts. |
| `reset_superseded_revision_admitted`, `reset_superseded_write_landed` | A claim admitted after the reset without seeing the fence. Check `ledger_fence_contended` and `ledger_control_pending` in the logs, and the user's `upload-fences.json` (`resets`, `resetsCompleted`). **Stop the rollout.** |
| `manifest_missing`, `admitted_batch_not_written`, `more_than_one_manifest_for_session`, `revision_missing_from_index` | An admission answered admitted but the canonical write or index merge is missing. Check `admission_objects_ms` / `admission_commit_ms` on the pods and the session's prefix under `<root>/<org>/v1/ws=<ws>/m=srv_*/`. |
| `reset_not_run`, `reset_replay_answered_differently`, `reset_not_recorded_in_fence` | The operator credential's write access to `upload-fences.json` and the `ledger/owners/` listing; the SSE-KMS key on PUT. |
| `reset_count_not_equal_run_sessions` | The mid-run reset tombstoned a different number of sessions than the run's reset sessions: fewer is a missed tombstone (a run session's owner stub missing or unreadable), more means it reached a session outside the run. **Treat more as a safety failure**: check what user 0 owned. |
| `reset_refused_foreign_sessions` | The mid-run reset was not applied: user 0 owned a live session outside the run when it was due. Something else used user 0 during the run. It must be a dedicated synthetic user; use a fresh one. |
| `reset_refused_owners_unreadable` | The mid-run reset was not applied: the read-only listing of user 0's `ledger/owners/` or its fence failed. The operator credential's list and read access there. |
| `cleanup_reset_refused_foreign_sessions`, `cleanup_reset_refused_owners_unreadable`, `cleanup_reset_not_run`, `cleanup_count_not_equal_live_after_sessions` | The end-of-run reset of the after sessions did not run or did not cover them. The same causes as the mid-run reset's. The run's after sessions stay live, so the next run with this user 0 refuses: use a fresh synthetic user 0. |
| `upload_url_looks_like_production`, `upload_url_not_marked_non_production`, `upload_url_plain_http_not_loopback`, `upload_url_invalid` | relay-control answered a presigned PUT URL that fails the target checks; nothing was PUT. Check which upload bucket the `--control-url` deployment presigns for. **Stop: that control plane may not be the dev one.** |
| `inbox_left_unprocessed`, `upload_inbox_left_unpromoted`, `not_quiesced_within_timeout`, `batch_never_terminal` | The consumers on both pods: `/readyz` `consumer.backlog`, `ingest.admission` lines with `outcome: deferred` (`tenant_limit`: are the caps too low?) or `retry`. Rerun with a longer `--quiesce-timeout-s` before calling it a bug. |
| `dead_lettered`, `dead_lettered_in_logs` | `<root>/.admission-dlq/<org>/…` for the run's machines; the `error_code` on the `dead_lettered` log line. |
| `unexpected_retry_code`, `unexpected_log_error_code` | `counts.http.retryCodes` and `counts.log_codes`. `ledger_fence_authority_*` means the test org's marker is missing or invalid. `ledger_store_error` means S3 throttling or permissions. |
| `fewer_than_two_replicas_did_work`, `fewer_than_two_ready_replicas` | The Service is not spreading the traffic (session affinity?), or one pod is not Ready. The proof says nothing until both pods work. |
| `replica_admitted_nothing`, `replica_restarted` (local) | One process never committed a batch (it only replayed), or crashed. The local proof says nothing about two writers then. Check `--- ingest-r*` on stderr. |
| `more_first_writes_than_admitted_batches` (local) | Two processes both committed one batch. **Exactly-once is broken**, as `duplicate_admission_manifest_written_twice` is. |
| `admitted_batches_without_a_logged_commit` (local) | An admitted batch shows no `accepted` or `replay_fast` line on either process: a logging or attribution gap. Compare the ledger's batch records with `claimToken: null` (fast commits) against the logs. |
| **Refused before any upload (exit 2)** | |
| `missing_i_know_this_is_dev`, `target_env_not_dev_or_stage`, `reset_user_not_declared_synthetic` | Flags the operator must set: `--i-know-this-is-dev`, `--target-env dev\|stage\|sandbox`, and `--reset-user-is-synthetic` (only after making sure user 0 is a dedicated synthetic user). |
| `<target>_looks_like_production` (`deployment`, `ingest_url`, `control_url`, `endpoint`, `bucket`, `ledger_bucket`, `kube_namespace`, `kube_deployment`, `kube_context`) | A target name carries `prod`, `production`, `prd` or `live`. **Do not work around it.** Check you are pointing at the dev or stage deployment. |
| `<target>_not_marked_non_production` (same targets) | A target name carries no `dev`, `development`, `stage`, `staging`, `sandbox` or `test` token. Use the deployment's marked names, not an alias. |
| `ingest_url_invalid`, `control_url_invalid`, `endpoint_invalid`, `endpoint_plain_http_not_loopback` | A URL that is not https (plain http is for a loopback host only: a stack on this machine, never a tunnel), or that carries credentials, a query or a fragment. |
| `kube_context_required_with_kubectl`, `kube_namespace_and_deployment_both_required`, `replicas_unconfirmed` | kubectl evidence needs `--kube-namespace`, `--kube-deployment` and `--kube-context` together; otherwise `--replicas-confirmed`. |
| `aws_endpoint_env_without_endpoint_flag` | `AWS_ENDPOINT_URL` or `AWS_ENDPOINT_URL_S3` is set but `--endpoint` is not. Unset them or pass the store with `--endpoint`. |
| `reset_user_owns_sessions_outside_the_run`, `reset_user_owners_unreadable` | User 0 already owns a live session (a run that stopped before its end-of-run reset, or the user is not dedicated), or its owners listing is unreadable. Use a fresh synthetic user 0; never reset by hand to clear it. |
| `fence_authority_stop_present`, `org_not_covered_by_fence_authority_marker`, `store_unreadable` | The `.stop.json` kill switch is set, the test org has no valid fence-authority marker, or the operator credential cannot read the ledger. |
| `kubectl_deployment_unreadable`, `fewer_than_two_ready_replicas`, `ingest_ledger_not_s3`, `ledger_log_commit_not_off` | The kubectl preflight: the context, namespace and deployment, and the deployment's replicas and ledger env. |
| `unexpected_http_answer`, `batch_not_sent` | `counts.http.statuses`. A 401 or 403 is a token or scope problem; a 422 `delivery_account_mismatch` means `--org` / `--workspace` is not the tokens' tenant; a 404 `history_upload_direct_off` from control means direct upload is off for the test org (drop `--control-url` or allowlist the org). |

## Cleanup

Everything the run writes is under the test org and workspace you passed in, keyed by its `run_id`:
- origins `mrp-<run_id>-u<n>-<a|b>`;
- sessions and project paths with `mrproof<run_id>`;
- under `<root>/.admission-inbox/<org>/v1/ws=<ws>/m=srv_*/`: the per-origin inbox attempts and outcome
  objects, and, under each user's admission directory, that user's `ledger/` prefix: its batch records
  (`ledger/batches/`), revision shards (`ledger/revisions/`) and owner stubs (`ledger/owners/`), as
  docs/selfhost-composition.md section 5 lays out (in the ledger bucket instead when it is split);
- user 0's fence (`upload-fences.json`), which now records two more resets (the mid-run reset and the
  end-of-run reset of the after sessions);
- the canonical objects under `<root>/<org>/v1/ws=<ws>/m=srv_*/`;
- the search and directory rows the consumers index from them.

**What the run destroys.** The script deletes no object, but it is not harmless:
- The mid-run reset is a privacy control on user 0. It **tombstones every live session user 0 owns** in
  user 0's S3 upload fence. Those sessions' later uploads are never admitted, and this script cannot undo that. The reset is
  user-wide, not run-scoped. The end-of-run reset is user-wide too, and runs only if what user 0 owns live is
  a subset of the run's after sessions. That is why the run refuses unless `--reset-user-is-synthetic` is set, refuses
  before any upload if user 0 already owns a live session, and refuses the reset itself if user 0 owns any
  session outside the run.
- **Never** name a real person's account as the first `--user`.
- The S3 ledger is never deleted from, by design. The reset's tombstones and the fence's reset count stay.

> **Warning:** pointing the first `--user` at a real user would destroy that user's uploaded history.

- Prefer a throwaway test workspace that is retired as a whole after the proof.
- To remove a run's sessions, use the normal per-session erase path for the test workspace, or the
  canonical erase tooling scoped to that workspace prefix. Never use a prefix wider than the test org and
  workspace, and never touch another tenant's data.
- The users' S3 ledger objects can stay: they are small. A rerun uses a new `run_id`, so its origins, sessions
  and batch ids never collide with them. A rerun with the same user 0 is allowed only because the previous run's
  end-of-run reset left user 0 owning nothing live; if that reset did not run (the receipt shows a `cleanup_*`
  failure, or the run was killed), the rerun refuses before any upload and needs a fresh synthetic user 0.

## What it does not cover

- **The reset is applied directly to the S3 ledger, not through an HTTP route.** Self-host under
  `INGEST_LEDGER=s3` serves none. The script writes only the S3 half; the server's
  `withSqlControlMirror` also reconciles SQL `upload_deletions`, which it skips. So the read API's and the
  eraser's view of the reset (they read SQL) is **not** proven; admission and the canonical objects are.
- **Selection (per-session disable) is not exercised.** It is refused at startup under `INGEST_LEDGER=s3`.
- **Visibility is proven at the ledger and canonical-object level.** It is not proven through search or
  the read API.
- **The direct-upload path runs only with `--control-url`** (or in local mode, where the script mints the witness
  itself). The witness seq is learned from relay-control's first answer; this has not been run against a live
  relay-control. Local mode does not cover the presigned URL, the bucket policy or SSE-KMS on PUT.
- **Replica attribution comes from pod logs, which carry no tenant**, so dev background traffic counts too.
  Responses carry no instance id.
- **`INGEST_LEDGER_LOG_COMMIT=on` (log mode) is out of scope**, as is the synchronous path's tenant limiter
  at its caps (`sync` mode).
- **Local mode** runs on one host and one MinIO: no network partitions, no real S3 consistency or throttling, no
  KMS, and no load balancer, so it does not test session affinity. It does not cover the kubectl preflight.
  Postgres is there only for the ingest's startup role check.
- **Coverage so far.** It has been unit-tested (the preflight, the plan, the assertions, the kubectl parsing, and
  a rerun with the same user 0 over a scratch store) and run end to end in local mode, including two consecutive
  runs on one stack (`--runs 2`). It has not yet been run against a live dev stack.
