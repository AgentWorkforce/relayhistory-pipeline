# Acquisition cleanup debt

Operator runbook for native acquisition installs that could not reserve a revoke (PR #331, `src/acquisition/`). It is a source-level checklist: the tables are only a proposal (`supabase/acquisition-intents.proposed.sql`, never applied directly), and this runbook grants no permission to apply DDL, deploy, enable the service or touch credentials. Enablement stays behind `ACQUISITION_ENABLED` and `ACQUISITION_NATIVE_ENABLED` (`src/acquisition/config.ts`, `docs/acquisition-exchange.md`).

## 1. What cleanup debt is, and why it is never dropped

When an install is applied, the intent store stores a pre-signed reserved revoke of the grant in the same transaction (`reserveRevoke`, `src/acquisition/native-issuer.ts`). If the SQL result carries no positive `ownerEpoch` (or the revision is at the bigint limit) the reservation is `no_epoch`; if signing throws it is `sign_failed`. The install is then not finalized: `markInstallApplied` (`src/acquisition/intent-store.ts`) keeps it `status = 'pending'`, records `reservation`, counts an attempt, backs off and clears the lease. Bytes (`assertion`, `signature`), the encrypted G (`grant_ciphertext`, `grant_nonce`) and the issuer key id stay stored. The claim answers `503 acquisition_unavailable` (retryable).

It is never dropped because the install SQL may already have committed. Deleting the row would lose G (no recovery by replay), lose key retention (`assertKeysRetained` refuses startup if a pending intent's issuer key is unpinned) and leave a live grant with no revoke row. Invariant: no install is ever applied without a revoke row.

## 2. Detect

- Reconciler counters (`NativeIssuer.reconcile()`): debt counts in `retry`, and in `stalled` after `MAX_SEND_ATTEMPTS` (8) failed attempts. `src/acquisition/main.ts` logs `acquisition reconcile` (`reason: retry=N`) at info, and at warn `acquisition reconcile stalled` (`reason: max_send_attempts`) and `acquisition reconcile stale` (`reason: lease_superseded`). Alert on `stalled`.
- `stale` is not debt. It counts a completion whose `lease_token` compare-and-set lost to another sender (also logged as `acquisition stale completion`). It changes nothing; investigate only if it is sustained.
- Pending-row condition, read-only (proposed tables). This SQL is source-derived from the proposal and the code; it has not been executed against any database. Check every table and column name against the DDL actually applied in the target environment before running it:

```sql
SELECT i.operation_uuid, i.installation_id, i.reservation, i.cancel_reason, i.attempts,
       i.next_attempt_at, i.created_at, n.retired_at
  FROM sessions.ingest_acquisition_native_intents i
  JOIN sessions.ingest_acquisition_native_installations n USING (installation_id)
 WHERE i.kind = 'install' AND i.status = 'pending'
   AND i.reservation IN ('no_epoch', 'sign_failed')
   AND NOT EXISTS (SELECT 1 FROM sessions.ingest_acquisition_native_intents r
                    WHERE r.kind = 'revoke' AND r.target_grant_id = i.operation_uuid)
 ORDER BY i.created_at;
```

Stalled means `attempts >= 8`. Never select `grant_ciphertext`, `grant_nonce`, `assertion` or `signature`.

## 3. Triage

| Reason | Meaning | Resolves automatically |
| --- | --- | --- |
| `no_epoch` | Install result had no usable `ownerEpoch`/revision. | A later resend replays the same bytes, the result reports an epoch, and the revoke is reserved; or a revoke row for the grant appears. |
| `sign_failed` | The issuer signer threw while building the revoke. | Once the signer works again (key and `ACQUISITION_ISSUER_KEY_ID` config fixed), the next resend reserves the revoke. Check startup config, not the row. |

Backoff is `min(30 s * 2^(attempts-1), 1 h)` (`RETRY_BASE_MS`, `RETRY_CAP_MS`); stalled rows are still resent hourly. A recognised retired installation: `installations.retired_at IS NOT NULL` (set by `NativeIssuer.retireInstallation`, which also sets `cancel_reason = 'installation_retired'` on pending installs). If it retired before the install ever received an epoch, the debt stays stalled and alerting forever by design; the install is not refused because its grant may have landed. The claim route answers `403 installation_not_proven` for it, so no client can resolve it.

## 4. Last-resort repair design (not an approved operational command)

This section is a design for a last resort, not a generally sanctioned operational procedure, and there is no approved CLI for it today. It may only be used as part of a scoped repair plan owned by the issuer owner, with the concrete SQL for that specific environment approved by the owner before it runs. Operators must not run it on their own authority.

The design records an explicit revoke row for the grant so the next resend applies the install against it (the debt completion checks `NOT EXISTS` of a `kind = 'revoke'` row for the grant). The intended path is the issuer's own `unreservedRevoke` / `createRevokeIntent` (`POST .../revocations`, reason `explicit_exclude`), which signs against the authority status row only when its `grantId` is this install, else `409 grant_not_installed`. A debt install not yet stalled answers `409 session_busy`. A retired installation cannot use the route (403), so a row is needed. Statement shape (source-derived from the `INSERT` in `createRevokeIntent`, not executed anywhere; a template for the owner's approved SQL, not a command):

```sql
INSERT INTO sessions.ingest_acquisition_native_intents
  (operation_uuid, kind, installation_id, org_id, workspace_id, source, session_id, target_grant_id, origin_id,
   assertion, signature, evidence_sha256, request_sha256, grant_ciphertext, grant_nonce,
   status, client_operation_uuid, activation_reason)
VALUES (:fresh_uuid, 'revoke', :installation_id, :org, :workspace, :source, :session, :install_operation_uuid, NULL,
        :a, :s, :evidence_sha256, :request_sha256, NULL, NULL, 'pending', :client_uuid, 'explicit_exclude')
ON CONFLICT (target_grant_id) WHERE kind = 'revoke' DO NOTHING;
```

`:a`/`:s` must be a revoke assertion signed by the pinned issuer key (`acquisitionPayload`/`acquisitionAssertionBytes`, `src/session-ingest/acquisition-assertion.ts`) against the authority status epoch and revision, never invented. Operators hold no private key, so the owner of the issuer runs this. Requirements: one revoke per grant (unique index `ingest_acquisition_native_intents_one_revoke`); `target_grant_id` is the install's `operation_uuid`; the install row's bytes, G and `reservation` are untouched; the reconciler then applies the install and sends the revoke. Verify the debt query returns no row for it.

## 5. Do not

- DELETE or TRUNCATE pending rows (the role has no such grant; a superuser must not either).
- UPDATE `assertion`, `signature`, `grant_ciphertext`, `grant_nonce`, evidence or request digests.
- Null `lease_token` or `lease_until` to unstick a row: that defeats the compare-and-set and lets a late completion overwrite the new sender. Leases expire in `SEND_LEASE_MS` (5 min).
- Set `status = 'applied'` or `'void'` by hand, or unpin an issuer key still held by a pending row.
- Apply `supabase/acquisition-intents.proposed.sql` to any real database without the owner's approval.

## 6. `lease_token` for environments with an earlier DDL copy

An earlier copy of the proposed DDL may lack `lease_token uuid` on `ingest_acquisition_native_intents`, and the column-scoped `UPDATE (... lease_until, lease_token, ...)` grant. The code needs both, because every completion is a CAS on `lease_token`. Do not alter the table by hand: the SQL lane folds the final DDL into its migration. Treat such an environment as non-conforming before enablement:

- Recreating it from the current proposal is allowed only for an explicitly disposable environment whose acquisition tables are EMPTY, with the owner's approval.
- Any environment holding real or durable rows needs an owner-approved SQL-lane migration that preserves every intent, its G (`grant_ciphertext`, `grant_nonce`), revoke rows and leases. Obligations are never dropped to make a column change.
- No hand `ALTER` in either case.
