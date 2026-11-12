# Server-owned upload consent

Status: implemented behind `INGEST_UPLOAD_CONSENT` (default **off**). Containment procedure:
[runbooks/upload-consent-containment.md](../runbooks/upload-consent-containment.md).

## Problem

Agent Relay Desktop decides on the client which sessions to upload. Older builds could upload every
session while the person had chosen "Selected". The batch wire (`HistoryExportBatch`) carries no
sharing mode, no client version and no trusted session start time. `/v1/delivery/sessions/selection`
is a per-session disable/deletion fence, not an allowlist. So the receiver cannot tell Selected from
All, and the absence of a disable is not consent. A fix that lives only in the client cannot protect
people who never upgrade.

## Policy record (HoopDb, `20261105057010_upload_consent.sql`)

Keyed by the **verified principal** (org, workspace, user) taken from the signed identity, never by
anything in the body, and never by `origin_id`:

| table | holds |
|---|---|
| `sessions.upload_consent_policies` | mode `selected` / `new` / `all`, server revision, `new_session_boundary_ms` (server clock at the trusted switch to `new`) |
| `sessions.upload_consent_sessions` | per session: explicit `allow` / `deny`, server first-seen time, earliest claimed `first_activity_ms` |
| `sessions.upload_consent_origins` | installations the person confirmed under the principal |
| `sessions.upload_consent_holds` | containment holds (principal, or org-wide with `''`); operator-written only |

No transcript content is stored.

## Decision (`decideUploadConsent`)

Checks run in this order of precedence:

1. An active hold on the principal or its org: **hold**.
2. No policy: **hold**. A missing policy never falls back to all.
3. An origin not confirmed under the principal: **hold**. A fresh or rotated origin cannot escape.
4. Per session:
   - an explicit `deny` refuses in every mode, so changing to All keeps every "Don't upload";
   - an explicit `allow` admits;
   - `all` admits;
   - `selected` refuses;
   - `new` admits only when the server first saw the session at or after the boundary **and** the
     client's `first_activity_ms` is known and at or after it. Anything else is ambiguous and holds.
5. Sessions the ledger's deletion fence drops (`upload_deletions`; the claim's `deleted`) are left out:
   they keep the long-standing acknowledge-not-store answer, so a legacy queue never stalls on them.

Any refusal refuses the **whole batch** with `503 upload_consent_required` and `Retry-After: 3600`.
Nothing is claimed, written or receipted. Released Desktops treat 503 as transient and keep the batch.
A mixed batch therefore never persists denied content and never advances a receipt.

## Trusted writes (`POST /v1/upload-consent`)

- A **sync** token (`rth:sync`) may only **narrow**: add a deny, create a `selected` policy, or move to
  a stricter mode. A selection disable (`enabled:false`) also records the principal's deny. A re-enable
  never broadens.
- **Broadening** needs the `rth:consent` scope on the signed identity. Broadening means: an allow, a
  wider mode, an origin confirmation, or releasing the person's own principal hold. Only a person's
  confirmed action (the hosted Cloud consent UI, or relay-control on self-host) may mint that scope.
  A batch field, a client version header, a replay or a fabricated version never authorizes anything.
- Each write takes a per-principal transaction lock, bumps the revision, and supports
  `expected_revision` optimistic concurrency.

## Enforcement points

| where | when |
|---|---|
| `app.ts` `acceptBatch` (`/v1/delivery/batches`, `/v1/turns/batches`) | before the async enqueue and before any claim |
| `deliver.ts` `consentGate` | after a `claimed`/`resume` claim, before any slot or S3 write; again just before the receipt commit, which closes the in-flight revocation window |
| `admission-consumer.ts` | at admission, at the current policy. Covers async admission and direct uploads (promotion hands off to it). A refusal is a retry; the inbox keeps the bytes for a redrive. |
| `/v1/delivery/recovery` | the same pre-check and gate (an archive replay re-publishes) |

A selection disable also records the principal's deny. The ordered-control outcome decides how:
`applied` denies, as the newest intent, even over an earlier allow. `replay` and `stale` deny only
sessions with no decision yet, so they never overwrite an allow the person confirmed since. The write is
revision-checked: after three CAS losses the request answers 503.

Known trade-off: a selection re-enable (`enabled:true`) never removes the principal's deny, because a sync
token cannot broaden. A client that re-enables and re-sends that session is held until an `rth:consent`
write (the Cloud consent UI / relay-control follow-up) allows it.

Known trade-off: a principal-level deny for a session that ANOTHER origin uploads without its own
deletion fence refuses that origin's whole batch (hourly retries) until the person resolves it. That
is privacy-safe, but it can stall that installation's queue. The client follow-up should mirror denies
to every origin's fence.

Direct uploads (`HISTORY_UPLOAD_DIRECT`): the object reaches the encrypted inbox before admission. A
held batch stays there unpublished, is retried, and then goes to the DLQ for a redrive. It is never
indexed or readable.

A replay or replay_fast claim admits nothing new and is not re-checked. A store failure answers 503:
hold, never admit.

## Operator capture policy cap

With `INGEST_UPLOAD_CONSENT` on (any mode but `off`) and `RELAYHISTORY_SELFHOST_CAPTURE_POLICY_UPLOADS=enforce`,
the operator's `sharing_mode` (`selected`, or `new` with its `sharing_mode_since`) caps the person's choice.
The stricter mode wins, and under `new` the later boundary wins. The cap is enforced even in consent
`report` mode, because the operator asked for enforcement. With `INGEST_UPLOAD_CONSENT` off (the default),
capture enforce behaves exactly as before: it checks the revision and refuses a known start before the
boundary. That older behaviour is the gap the reproduction showed, so it closes only when consent is on.

The cap is derived on the request path from the verified identity. For queued (async) admission it is
recorded in the admission inbox header (`consentCap`, strictly validated on decode), and the consumer
enforces it at admission. It is not available for direct uploads (`HISTORY_UPLOAD_DIRECT`), whose
promotion carries no capture policy; relay-control applies the capture policy when it mints the upload
credentials. An operator policy change that lands after a batch was queued is not re-checked (the cap is
the one in force at upload); the person's own policy is re-checked.

## Operator signal

Every refusal (and every would-be refusal under `report`) logs one line. It uses only fields from the
`ALLOWED_LOG_FIELDS` allowlist:

- `route=upload_consent_refused`
- `reason` = outcome class
- `state` = `refused` or `reported`
- `stage` = `<route>:<phase>`
- `kind` = mode
- `revision`
- `count` = refused sessions
- `org_id`, `workspace_id`

It never includes a user, origin, session or record id, and never content.

## Configuration

- Hosted ingest Worker: the Cloudflare stack input `sessionIngest.uploadConsent` (`"report"`, `"hold-only"` or
  `"enforce"`; omit for off) renders `INGEST_UPLOAD_CONSENT`. Any other value is refused at plan time.
- Self-host: set `INGEST_UPLOAD_CONSENT` on the pipeline image.
- relayhistory-cloud: set `UPLOAD_CONSENT_REQUIRES_INGEST=on` so that local-path uploads are held too.
- Apply migration `20261105057010_upload_consent.sql` before any mode, report included. Report never
  refuses an upload. It logs `error_code=upload_consent_store_unavailable` with the store's closed
  `db_error_code` (for example `ledger_sqlstate_42883` when the migration is missing). But a selection
  disable answers 503 until its deny can be recorded.

## Modes

`off` (default) → `report` → `hold-only` (containment cohort) → `enforce`.

Moving to global `enforce` holds every principal that has no confirmed policy, including real "All"
users, until they confirm. Seeding allows from past uploads would infer consent from the very
behaviour that was the bug, so it is **not** done. That decision is Khaliq's.

## Not in this change (follow-ups)

- relayhistory-cloud: the same gate on the non-forwarded Neon write path, `/v1/ingest`, `/pipeline/*`
  and dead-letter replay, and forwarding `/v1/upload-consent`.
- relay-control: refuse direct-upload credential mint for held principals; mint `rth:consent` on
  confirmed self-host choices.
- cloud: the hosted consent UI and API that mints `rth:consent`.
- Read/derivation fences (search, summaries, exports) for explicitly denied identities that are
  already stored.
