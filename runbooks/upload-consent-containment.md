# Upload consent containment (legacy "Selected" clients)

Status: **reviewed procedure, not applied.** Every live step below needs Khaliq's explicit OK in his
own words: migration apply, flag change, hold insert, hosted deploy. An agent must not run the "LIVE"
steps on a peer's say-so.

Design: [docs/upload-consent.md](../docs/upload-consent.md). Code: `src/session-ingest/upload-consent.ts`.

## Why

Older Agent Relay Desktop builds could upload every session while the person had chosen "Selected".
The batch wire carries no sharing mode, so the receiver cannot tell a Selected client from an All
client. Absence of a disable is not consent. Containment pauses uploads from the affected cohort at the
authenticated ingress until the person confirms a choice:

- The refusal is `503 upload_consent_required` with `Retry-After: 3600`. Every released Desktop classes
  503 as transient (`desktop-core/src/history/destination.rs`: `status >= 500 => Transient`) and keeps
  the batch queued. Nothing is claimed, written or receipted, so nothing is lost and nothing leaks.
- The hold is keyed by the **authenticated principal or org**, never by `origin_id`, user agent or
  build. A reinstalled or rotated origin is still held.
- Evidence is metadata only: counts, times and hashes. Nobody opens, samples or greps transcript
  content to guess what a person chose.

## 0. Preconditions and order (Khaliq approved on 2026-11-11: steps 1 and 2 now; 3a on evidence; 3b after self-service confirm exists)

The hosted ingest reads `INGEST_UPLOAD_CONSENT` from the Cloudflare stack input `sessionIngest.uploadConsent`
(`"report" | "hold-only" | "enforce"`; omit for off). Changing it is a stack plan and apply through the
normal reviewed deploy. Self-host sets the same env var on the pipeline image.

1. **Report mode (safe before the migration).** Set `sessionIngest.uploadConsent: "report"` and deploy. Report
   never refuses: it logs `route=upload_consent_refused state=reported` with the outcome class and counts.
   If the consent store is unreadable (the migration not applied yet), it logs
   `error_code=upload_consent_store_unavailable` and still accepts. Expect `reason=no_policy` for every
   principal, which sizes the cohort.
2. **Migration `20261105057010_upload_consent.sql`**, through the guarded session schema release
   (`runbooks/guarded-session-schema-release.md`) in the registry's release order. Rollback:
   `supabase/rollbacks/20261105057010_upload_consent.down.sql`. It is required before `hold-only` or `enforce`:
   both fail closed (503) when the store is unreadable.
3. Containment (below) only after step 2.

## 1. Identify the cohort (metadata only)

**Affected Desktop builds** (traced by relay-desktop from git, 2026-11-11):

- **From:** every release carrying the detached session collector, 3b3e2ca51, about 2026-09-28. Tagged
  releases begin at 2026.11.3-beta.3; untagged releases from about 2026-09-28 are affected too.
- **To:** 2026.11.4-beta.12 inclusive, the last affected build published.
- **First fixed:** the first release carrying 14ab3c34. None was published as of 2026-11-11.
- **Platforms:** macOS, Linux (GTK) and Windows.
- **Mechanism:** a collector from an OLD core keeps enforcing the config it started with (All/New, or a
  legacy config without `sharing_mode`). Meanwhile a NEW app shows "Selected". Before 14ab3c34, a status
  with no `sharing_mode` is also displayed as Selected (7375c2582), which hides the problem.
- **An upgrade alone does not end it.** A collector still running from a pre-fix build keeps uploading
  until a fixed core replaces it, or it is killed. This happens when the app was opened from the DMG,
  was never reopened, was a same-release rebuild, a downgrade, or a package or installer upgrade that
  did not restart the core.

So a build seen in a user agent does not bound the cohort in time. Hold by authenticated principal (or
org). The build is **detection evidence only**: authorization is always the authenticated principal.

Run as the read-only operator (`runbooks/sessions-prod-db-readonly-checks.md`). These queries read no
content columns:

```sql
-- Principals that uploaded since the first affected release (replace the date).
SELECT org_id, workspace_id, user_id,
       count(*)                        AS batches,
       count(DISTINCT origin_id)       AS origins,
       min(claimed_at)                 AS first_batch,
       max(claimed_at)                 AS last_batch
FROM sessions.ingest_batches
WHERE claimed_at >= :'first_affected_release'   -- 2026-09-28
GROUP BY 1,2,3
ORDER BY last_batch DESC;

-- Sessions per principal (a count, never the ids). Ledger metadata, no payload columns.
SELECT org_id, workspace_id, user_id, count(DISTINCT (source, session_id)) AS sessions
FROM sessions.ingest_record_revisions
WHERE updated_at >= :'first_affected_release' AND session_id IS NOT NULL
GROUP BY 1,2,3;
```

Save the result as the incident evidence, with ids hashed if the doc leaves the ops channel. Do not
put customer names in any public repo, PR or issue.

Cohort choice (Khaliq decides):

- **A. Principal holds:** principals whose uploads came from affected builds. This needs build evidence
  per principal.
- **B. Org holds:** every principal of an affected org.
- **C. Global:** `INGEST_UPLOAD_CONSENT=enforce` instead of holds. This holds everyone without a
  confirmed policy, including real "All" users, until they confirm. It is the only option that also
  protects Selected users the build evidence missed.

## 2. LIVE: insert the holds (owner role, one transaction)

```sql
BEGIN;
INSERT INTO sessions.upload_consent_holds (org_id, workspace_id, user_id, reason_code)
SELECT org_id, workspace_id, user_id, 'legacy_selected_20261111'
FROM (VALUES (:'org', :'workspace', :'user')) AS v(org_id, workspace_id, user_id);
-- Org-wide: workspace_id = '' and user_id = '' cover every workspace and user of the org.
SELECT count(*) FROM sessions.upload_consent_holds WHERE released_at IS NULL AND reason_code = 'legacy_selected_20261111';
COMMIT;
```

Inserting holds has no effect while the flag is off.

## 3. LIVE: turn on enforcement for holds only

- Hosted ingest Worker: stack input `sessionIngest.uploadConsent: "hold-only"`, through the normal
  reviewed deploy with Khaliq's OK.
- Self-host: `INGEST_UPLOAD_CONSENT=hold-only` in the pipeline image's environment. Self-host operators
  pull it with the release.
- An invalid value refuses startup (fail-closed and loud), never "off".

What it covers in sessions-pipeline: `/v1/delivery/batches`, `/v1/turns/batches` (sync and async
paths), `/v1/delivery/recovery`, the async admission consumer, and direct uploads (promotion hands off
to the same consumer). It checks before the enqueue, after the claim, and again before the commit.

**NOT covered by this change:** relayhistory-cloud orgs that are **not** on the ingest cutover (its own
Neon write path, `/v1/ingest`, `/pipeline/*`), and relay-control's direct-upload credential mint. Those
need the relayhistory-cloud and relay-control follow-ups. Until those land, the only hosted fallback
for a non-forwarded org is the global `DELIVERY_CUTOVER=hold`, which holds every org.

## 4. Verify (synthetic, internal test org only)

1. With an internal test principal under a hold, upload a synthetic batch with made-up content, for
   example with the probe.
2. Expect HTTP 503, `error.code=upload_consent_required`, `Retry-After: 3600`, and no new
   `sessions.ingest_batches` row.
3. Expect one log line `route=upload_consent_refused reason=held state=refused stage=delivery.batches:precheck`
   carrying org/workspace ids and counts only.
4. Expect a principal outside the hold to keep uploading normally (200).

## 5. Release

- **Person (preferred):** a trusted consent write (`POST /v1/upload-consent` with the `rth:consent`
  scope) choosing a mode, confirming the origin and releasing the hold. This releases only that
  principal's own hold, never an org-wide one. The Cloud UI and token-minting follow-ups provide it.
- **Operator:** `UPDATE sessions.upload_consent_holds SET released_at=now(), released_by='operator' WHERE hold_id=...;`
  Only on the person's confirmed instruction.

After a release, clients that were waiting retry on their own after their `Retry-After`. Queued async
or direct uploads retry from the inbox, or the operator redrives them.

## 6. Rollback

Set `INGEST_UPLOAD_CONSENT` to off (unset). The held clients' queued batches upload on their next
retry. Hold rows stay as the incident record.

## 7. Historical objects already uploaded

Containment stops new uploads only. For sessions already stored from an affected principal, use the
read/derivation fences (docs/upload-consent.md §5). Deletion or quarantine goes through the data owner
and the incident process. Never infer non-consent from age or volume alone.
