# Self-host privacy controls, pipeline-native (design)

## 1. Status and gates

- **Design only.** Nothing here is implemented, applied, deployed or enabled. Any migration apply, flag flip, deploy,
  credential or IAM change needs Khaliq's separate, explicit OK in his own words (AGENTS.md). Merging a future
  implementation PR under the standing code-integration order does not turn anything on.
- **Default-off.** Every new behaviour below sits behind its own flag. Unset means today's behaviour, byte for byte.
  No existing flag implies any of them.
- **Scope.** A self-host serves session history entirely from the `sessions-*` services: the pipeline (ingest,
  indexer, consumers) and the read API (`SERVICE=session-read-api`). **relayhistory-cloud (RHC) and RelayAuth are not
  used.** Identity is relaycontrol's `sh_at_` token, checked through introspection
  (`packages/pipeline/src/auth/relaycontrol.ts`). Hosted Option C is unchanged by this doc.
- **Base:** `main` at `c048238c`. File and line references are to that tree unless they name a PR head.

## 2. Hosted vs self-host, in one table

| Concern | Hosted (Option C) | Self-host (this design) |
|---|---|---|
| Who issues selection/reset | Desktop → RHC `POST /v1/delivery/sessions/{selection,reset}` → ingest `/v1/custody/authority/begin`, ordered fence, `/v1/custody/authority/complete` (runbooks/c3-supabase-privacy-authority.md, "What changes") | Desktop → self-host ingest edge (#316) → **new** in-pipeline orchestrator running the same three steps (section 5) |
| Who stores the head | `sessions.ingest_source_control_heads` under the Supabase authority (041000) | the same table, same functions |
| Who serves the control head | RHC, named entrypoint `SessionReadPrivacyControls`, `GET /v1/session-read/control-head`, over the Worker's `HISTORY_CONTROL_HEAD` binding | **new** in-process producer: a SECURITY DEFINER SQL function read by the read API's own pool (section 4) |
| Consumer rule | v5.3 rules 1-5 (`docs/handoffs/neon-census-role-20261107/r2-control-head-contract-v5.3-CURRENT.md`) | the same rules, the same parser and the same readers |
| Operator erase | #349 (held): ingest route, HMAC operator key | the same route; section 6 lists what differs |

The point of the design is that self-host does **not** fork the privacy model. It reuses the Supabase privacy
authority, the ordered fence, the 043700 readers and the read API's gate code. The new parts replace only what RHC
does in hosted: issuing controls and serving the head.

## 3. What exists today vs what is new

### 3.1 Exists on main (reused unchanged unless noted)

| Piece | Where | Role here |
|---|---|---|
| Head table | `sessions.ingest_source_control_heads` (`supabase/migrations/20261105010000_frozen_source_custody.sql` l.59-66; `deleted` column added by 041000 l.49) | one row per (org, ws, user): `epoch, version, digest, kind, payload, pending, before_privacy, privacy_version, deleted` |
| Supabase privacy authority | `20261105041000_ingest_privacy_authority.sql`: `ingest_privacy_authority` singleton (l.65-76), `_fence` (l.178), `_activate` (l.203), `_begin` (l.445), `_complete` (l.510), `_apply` (l.369), statement guard (l.95-124) | the self-host authority of record; `begin`/`complete` EXECUTE to `bulls96_session_ingest` only (l.551-552) |
| Frozen privacy tables | `ingest_frozen_source_deletions`, `ingest_frozen_source_resets` (010000 l.32-40), `ingest_frozen_source_owned_sessions` (041000 l.56-63) | written by `_apply`; empty at self-host genesis (no Neon era) |
| Target fences | `sessions.upload_deletions`, `sessions.upload_resets` (`20261104022000_session_ingest_ledger.sql` l.158-178), `ingest_fence_versions` | written by `ingest_apply_selection` / `ingest_apply_reset` (`20261104030000_ingest_fence_ownership.sql` l.395, l.435) |
| Ordered fence | `sessions.ingest_apply_ordered_control` + `ingest_control_receipts` (`20261104052000_ordered_ingest_controls.sql` l.8-112) | applies a control exactly once per (org, ws, user, epoch, version); contiguous versions; digest-checked |
| Pointer and directory retire | trigger `ingest_publication_retire_fence` on `ingest_fence_versions` and `upload_resets` (`20261104060000_ingest_canonical_publication.sql` l.450-482) | tombstones `session_index` rows and the `session_directory` row of fenced, published sessions |
| Ingest routes | `src/session-ingest/app.ts` l.503-560 (selection, reset; ordered when the signed identity carries `control`), l.626-660 (authority begin/complete, only with `privacyAuthority === "supabase"`) | called by the orchestrator, not by clients |
| Self-host upload edge | **PR #316 (open)** `src/session-ingest-edge/`: introspects `sh_at_`, re-signs with `INGEST_SIGNING_KEY`, forwards an allowlist including selection and reset; refuses `/v1/custody/*` | the front door this design extends |
| Owner-buffered readers | `20261105043700_owner_buffered_read.sql`: `owner_buffered_read_scope` (l.308), `buffered_sessions_for` (l.401), `buffered_session_stages_for` (l.627), parity table `owner_buffered_parity` (l.190) with **no writer**, `owner_buffered_parity_expected` (l.238) | unchanged; self-host must supply parity rows (section 5.5) |
| Read-side gate | `src/session-read-api/owner-buffered.ts`: `ControlHeadGate` (l.158), `CONTROL_HEAD_WIRE` (l.175), `parseControlHeadResponse` (l.215), `serviceBindingControlHead` (l.264), `admissionVisible` (l.342), `finalGate` (l.873) | the parser and the gate interface are reused; only the transport is new |
| Transcript eraser | `src/migration/neon-s3-erasure/` with `S3X_PRIVACY_AUTHORITY=supabase`; read surface `ingest_privacy_eraser_*` (041000 l.650-724), role `bulls96_privacy_eraser` | erases S3 copies of fenced sessions; needs a self-host runner (section 7) |
| Buffered GC | `src/session-ingest/buffered-gc*.ts`, `docs/buffered-upload-gc.md` | redacts staged copies; `_complete` moves the user's stages to the queue head (041000 l.544-546) |
| Operator erase design | **PR #349 (held)** `docs/operator-session-erase-design.md` @ `030b47a4` | section 6 |

### 3.2 New (all default-off)

1. `sessions.selfhost_control_head_for(p_workspace_id text)`: the in-process control-head producer (section 4.2).
   New migration, slot from the registry keeper.
2. `selfhostControlHead(run)`: a `ControlHeadGate` over that function, in `owner-buffered.ts` (section 4.3).
3. Self-host wiring of `ownerBuffered` in the service entry (`src/session-read-api/lambda.ts`
   `readAppFromSecret`, `entry: "service"`), which today never wires it (lambda.ts l.24-26) (section 8).
4. The self-host privacy orchestrator for selection/reset (section 5.2), hosted in the #316 edge.
5. A genesis bootstrap: an owner-run, one-time script that brings a fresh self-host database to "authority active"
   with an empty Neon-era snapshot (section 5.3).
6. A parity-row writer for rule 4 (section 5.5), owner-only SQL plus a caller. This is the most consequential new
   piece and has an open question (Q2).
7. A fence for the AlleyOop upload path, if self-host runs it (section 7.3, Q4).

## 4. (a) The in-process control head

### 4.1 Exact response shape

The self-host producer returns **the v5.3 wire, key for key**, so `parseControlHeadResponse` (owner-buffered.ts
l.215) parses it with no change to its rules. The shape is `CONTROL_HEAD_WIRE` (l.175-190):

```json
{
  "protocol": "relay-session-read-control-head-v1",
  "version": 1,
  "issuer": "<see Q1>",
  "tenant": { "orgId": "<verified>", "workspaceId": "<verified>", "userId": "<verified>" },
  "controlEpoch": "<hex64>",
  "cutover": { "epoch": "<hex64, == controlEpoch>", "status": "active", "mode": "ingest" },
  "privacyBaselineSha256": "<hex64>",
  "sourceControlEpoch": "<hex64>" | null,
  "sourceControlVersion": "<decimal string 0..2^63-1>",
  "sourceControlDigest": "<hex64>" | null,
  "sourceControlKind": "selection" | "reset" | null,
  "pending": false | true
}
```

Field sources, all read in **one** SQL snapshot:

| Field | Source |
|---|---|
| `tenant.*` | the transaction settings `bulls96_read.org_id`, `bulls96_read.workspace_id`, `bulls96_read.user_id`, which the read API binds from the verified identity (`liveness.ts` `TENANT_SETTINGS_SQL`, `owner-buffered.ts` `BUFFERED_USER_SQL`). Never an argument. |
| `controlEpoch`, `cutover.epoch` | `sessions.ingest_lazy_policy.control_epoch` (`20261104055000_lazy_current_custody.sql` l.20-31), required equal to `ingest_privacy_authority.control_epoch` |
| `cutover.status` / `mode` | the constants `"active"` / `"ingest"`, emitted only when `ingest_privacy_authority.state = 'active'` for that epoch |
| `privacyBaselineSha256` | `ingest_lazy_policy.privacy_baseline_sha256` |
| head fields | the caller's `ingest_source_control_heads` row. No row: `sourceControlVersion "0"`, and `sourceControlEpoch`, `sourceControlDigest`, `sourceControlKind` null, `pending` false |

Rules carried over from v5 for the producer:

- A head row whose `epoch` differs from `controlEpoch` is a **refusal**, never "no head".
- The policy row must exist and `valid_until > clock_timestamp()`. The read-side scope (043700 l.308 onward) checks
  the same, so the two cannot disagree.
- A pending head is reported as `pending: true`. The parser fences it (`gate_pending`). The producer does not hide it,
  so the reason stays visible in logs.

### 4.2 SQL producer (new)

```sql
sessions.selfhost_control_head_for(p_workspace_id text)
  RETURNS jsonb   -- exactly the body above
  LANGUAGE plpgsql STABLE SECURITY DEFINER
  SET search_path = pg_catalog, sessions, pg_temp
```

- **Caller pin.** It uses the same guard as `owner_buffered_read_scope`: refuse 42501 unless the `role` setting (or
  `session_user`) is `bulls96_read_api`, all three tenant settings are non-empty, and `p_workspace_id` equals the
  bound workspace. The user is **only** the bound setting.
- **Grants.** EXECUTE goes to `bulls96_read_api` alone, and the function is added to
  `src/hoopdb/sessions-function-acl.json`. It grants no table privilege. The read role still cannot `SELECT` the head,
  policy or authority tables directly.
- **Refusals.** Each is SQLSTATE `55006` with a static message, so it maps to `BufferedFenced`. The cases: authority
  absent or `fenced`, policy absent or expired, epoch mismatch between policy and authority, head epoch foreign, or
  an identifier the wire rejects.
- **No locks, no writes.** Plain snapshot reads only, like 043700. It runs inside the read API's `BEGIN READ ONLY`
  with the per-transaction `statement_timeout` (`BUFFERED_STATEMENT_TIMEOUT_MS`, 2 s).
- **Identifier bound.** The producer applies the same rule as `isControlHeadIdentifier`: 1..512 UTF-8 bytes, trimmed,
  no control characters. The gate also checks locally before the call, as `serviceBindingControlHead` does.

### 4.3 TypeScript gate (new)

```ts
export function selfhostControlHead(run: TenantRunner, opts?: { deadlineMs?: number }): ControlHeadGate
```

- `observe(scope)` runs one READ ONLY tenant transaction. It binds `bulls96_read.user_id` (`BUFFERED_USER_SQL`), sets
  `statement_timeout`, and runs `SELECT sessions.selfhost_control_head_for($1)` with the verified workspace.
- The returned jsonb goes through **`parseControlHeadResponse(body, scope)` unchanged**. There is one parser and one
  wire table (`CONTROL_HEAD_WIRE`) for both transports. That way a producer drift fences instead of parsing loosely.
- **Deadline.** `CONTROL_HEAD_DEADLINE_MS` (2 s) wraps the transaction, as the binding gate's race does. A timeout is
  `gate_timeout`.
- **Errors map to `BufferedFenced`:** 55006 is `gate_status`, 57014 is `gate_timeout`, 42501 is `gate_identity`, and
  anything else is `gate_error`. Never a 500, never cached, and one call per `observe()`.
- **Two observations.** `readOwnerBufferedTurns` and `readOwnerBufferedList` already observe before the RPCs and
  again after every S3 read (`finalGate`, l.873), and fence on any difference (`sameAuthority`). With an in-process
  producer, the second observation is a separate transaction. It still proves the head did not move across the
  request. Self-host keeps both observations. No code path changes.

### 4.4 Fail-closed summary (self-host)

| Condition | Outcome |
|---|---|
| Flag on, the gate cannot be built (no read pool, or the function is missing at startup) | startup refuses: `session read api: selfhost_control_head_unavailable` |
| Authority row absent or `fenced` | 55006, so `BufferedFenced("gate_status")`, so 503 `buffered_fenced` once a buffered candidate exists, else the route's existing answer |
| Policy expired or epoch mismatch | the same |
| Head pending | `gate_pending`, so fenced |
| Head epoch foreign | the producer refuses (55006). If a malformed body ever reached the parser, it would answer `gate_epoch` |
| Any extra or missing key, a constant mismatch or a tenant echo mismatch | `gate_shape` / `gate_echo` |
| Over 2 s | `gate_timeout` |
| First and final observation differ | fenced (`sameAuthority`) |

## 5. (b) User selection/reset: write path and storage

### 5.1 Storage (reuse; nothing new for the control itself)

A self-host control lands in exactly the hosted Supabase-authority tables:

- **Head:** `ingest_source_control_heads` (version +1 per control, `pending` true until complete).
- **Target fences:** `upload_deletions` (selection `enabled:false`, and every owned session on reset),
  `upload_resets` (`request_id`, `deleted` count), `ingest_fence_versions`.
- **Exactly-once receipts:** `ingest_control_receipts` (contiguous versions 1..V per (org, ws, user, epoch)).
- **Frozen tables:** `ingest_frozen_source_*`. They are empty in a greenfield self-host. `_apply` still writes them
  (for example a reset's `ingest_frozen_source_resets` row), so replay detection by `request_id` works as hosted.

This is the "0078-style head" in pipeline form. The original heads are Neon's `delivery_cutover_control_heads` from
RHC's 0077/0078 cutover (010000 l.57-58 calls the Supabase table their mirror). 041000 made
`ingest_source_control_heads` authoritative. Self-host uses that table as the only copy.

### 5.2 Write path: the self-host privacy orchestrator (new)

**Host: the #316 ingest edge** (`src/session-ingest-edge/`). It already introspects `sh_at_`, holds
`INGEST_SIGNING_KEY` and forwards `POST /v1/delivery/sessions/{selection,reset}`. Today it forwards them **without** a
`control` tuple. The Worker then runs the legacy unversioned `setSessionUploads` / `resetUploadedSessions`. For an
org enrolled in buffered admission that unversioned path is refused ("Buffered selection/reset cannot fall back",
052000 l.114-115), and either way no head or receipt is written. That makes the edge's current forwarding the gap
this section closes.

New flag `SESSION_INGEST_EDGE_PRIVACY_AUTHORITY`. Only exactly `true` enables it, and any other non-empty value fails
startup. With it on, the edge runs RHC's three steps itself, for the introspected (org, ws, user) only:

1. **Begin.** It calls the Worker's `POST /v1/custody/authority/begin` with `{epoch, kind, payload}`, edge-signed
   with the identity from introspection (no `control`). It does this only after it has validated the client body
   (below):
   - `epoch` is the live `controlEpoch`. The edge reads it from a **new** tiny ingest route,
     `GET /v1/custody/authority/epoch`, which returns `{controlEpoch}` from `ingest_lazy_policy` and is reachable only
     with the edge signature. The edge never takes the epoch from a client.
   - `payload` is `{origin_id, sessions:[{source,session_id}...], enabled}` for selection and `{request_id}` for
     reset. These are the exact keys `_begin` requires (041000 l.457-468).
   - The answer is `{epoch, version, digest, kind, pending:true, deleted, apply}`.
2. **Fence.** It forwards the client's selection or reset body to the Worker. This time the signed identity carries
   `control: {epoch, version, digest, kind}` from step 1. The Worker runs `applyOrderedControl` (app.ts l.520-524,
   l.546-549). For an unchanged ordered fence, the frozen witness is built in `frozen-supabase` custody mode
   (app.ts l.518, l.541).
3. **Complete.** It calls `POST /v1/custody/authority/complete` with
   `{epoch, version, digest, kind, outcome}`, where `outcome` is the fence ack's.

**Client contract (unchanged from relayhistory):** the same bodies and answers as `parseSelection` / `parseReset`
(app.ts, including `confirmation:"DELETE ALL SESSIONS"` on reset). The 200 answer is the Worker's
(`{updated:true}` / `{reset:true, deleted}`) plus the `control` ack, exactly as hosted.

**Crash and retry.** Begin with a pending head and the same digest returns `apply:false` (041000 l.488-493). The
edge then re-drives steps 2 and 3, which are idempotent on `{version, digest}` (052000 replay branch). A different
control while one is pending answers 409 `control_pending`. That requires a distinct SQLSTATE (see #349 section 4.7
note). Until then the edge maps the 55000 to 503 `delivery_unavailable`, which is fail closed and retryable. A crash
between begin and complete leaves the head pending. That hides the user's buffered data (rule 2) and fences new
uploads until the same request is retried. This matches hosted behaviour.

**Exposure.** `/v1/custody/*` stays refused to clients at the edge (#316 allowlist). Only the edge's own outbound
calls reach it, and the Worker keeps verifying the HMAC.

**Startup refusals (edge):** the flag on and the Worker not configured with `INGEST_PRIVACY_AUTHORITY=supabase` plus
`INGEST_CUSTODY_SOURCE=frozen-supabase` (the edge checks this once through the epoch route: a 503
`privacy_authority_unavailable` refuses startup).

### 5.3 Genesis bootstrap (new, owner-run once, needs Khaliq's OK per install)

A greenfield self-host has no Neon. The 041000 activation still works with an **empty** snapshot. Steps, each an
existing owner function unless marked new:

1. **New:** the owner writes `sessions.ingest_frozen_source_completeness` with `org_count 0`, `record_rows 0` and an
   empty `neon_snapshot`. It also writes the `ingest_lazy_policy` singleton with a self-host-minted `control_epoch`
   (32 random bytes, hex), a `privacy_baseline_sha256` over the empty privacy set, and a `valid_until`. This step is
   the only new SQL-touching operator act. See Q1: the policy's `issuer` CHECK is `'relayhistory-cloud'`
   (055000 l.29).
2. `ingest_privacy_authority_fence({controlEpoch})`.
3. Build the user-session index `ingest_frozen_source_records_user_session` (CONCURRENTLY; trivial on an empty
   table). `_activate` requires it (041000 l.218-222).
4. `ingest_privacy_authority_activate` with these arguments:
   - `heads:[]`, `deletions:[]`, `resets:[]`;
   - `neonFingerprintSha256` = `ingest_privacy_authority_packet('[]','[]','[]')`;
   - `neonOwned: {rows:"0", sha256: sha256("")}` (the value `_owned_digest()` returns for an empty table).
5. **Per org, on enrollment:** an `ingest_buffer_admission` row with `control_epoch` = the policy epoch, which the
   ordered fence requires (052000 l.55-60). It is written by the existing buffered-admission owner step.

After step 4 the authority is `active` and one-way, exactly as hosted after the first control.

### 5.4 Why not let the edge write SQL directly

The edge holds no database credential today, and adding one widens its blast radius. Every write stays behind the
ingest role's SECURITY DEFINER functions, reached only through the HMAC-verified Worker. That is the same trust
boundary RHC uses in hosted.

### 5.5 Rule 4 in self-host: the parity row (new writer; Q2)

The read gate requires a `sessions.owner_buffered_parity` row per (org, ws, user), and the table ships empty with
**no writer** (043700 l.190-235, header item 5). In hosted, the row is an independent proof from the G1/G2 census
that the Supabase view matches the Neon era. A self-host has no Neon era, so the census has nothing to compare.

Proposal (needs a ruling, Q2):

- **New owner-only function:**
  `sessions.owner_buffered_parity_refresh(p_org text, p_ws text, p_user text) RETURNS jsonb`. It is SECURITY
  DEFINER, EXECUTE to `bulls96_session_ingest` only, and **refuses unless a self-host marker is set**. The marker is a
  new boolean `selfhost` on `ingest_privacy_authority`, written only by genesis. That way the function can never write
  a hosted proof.
- **What it writes.** It computes `owner_buffered_parity_expected(...)` for the current head and follows the 043700
  writer contract exactly:
  - with no prior row, insert on conflict do nothing;
  - with a prior row, compare-and-set on the exact prior tuple;
  - the same head with different digests is a refusal.
- **Callers.** There are two:
  1. The orchestrator, right after step 3 (complete), in its own call through a new signed Worker route.
  2. A periodic self-host job for users with buffered rows and no current parity row. This covers users who never
     issued a control and therefore have the no-head form.
- **Consequence to state plainly.** In self-host, rule 4 becomes a **consistency check** (receipts contiguous, digests
  recomputed at read time) rather than an independent census. Q2 asks whether that is acceptable, or whether
  owner-buffered reads must stay off in self-host.

## 6. (c) Operator erase front door (per held PR #349)

#349's design applies to self-host with these differences. Everything else stands as written there, including the
response, the replay semantics and the error table.

| #349 element | Self-host |
|---|---|
| Host: ingest Worker `POST /v1/operator/sessions/erase` behind `OPERATOR_SESSION_ERASE_ENABLED` | unchanged. It needs `INGEST_PRIVACY_AUTHORITY=supabase`, which genesis (section 5.3) makes true |
| Orchestration: begin, fence, complete in-Worker | unchanged. It is the same three steps as section 5.2, so the operator route and the user path share one orchestration module (proposal: `src/session-ingest/privacy-orchestrator.ts`, used by the Worker route and the edge) |
| Caller: control service principal, HMAC operator key | relay-control runs in self-host too, so this is unchanged. The private ingress is the self-host's in-cluster Service, not a Cloudflare route |
| Membership attestation (Q7 decided): control-signed, verified through relaycontrol introspection | unchanged: the introspection URL is in-cluster |
| Completion: control polls the read API for 404 | the same rule. The read API is the self-host service. Q2 of #349 (control-class read principal) is still a prerequisite |
| Buffered search hits covered by #338 | #338 (open) gates buffered search hits. Whether the self-host Postgres search path can return buffered hits at all is **unverified** (to check in the implementation PR). Until #338 lands, a self-host must not enable owner-buffered search |
| Replay detectability migration (Q8/Q9) | the same migration. It also gives self-host its `actor_ref` audit |

Self-host-specific addition: the operator reset must cover the **AlleyOop path** if a self-host runs it
(section 7.3). Otherwise "erased" would exclude courier-uploaded sessions. #349 does not cover that, because hosted
has no AlleyOop uploads.

## 7. (d) Per-session erase across stores

### 7.1 The primitive

A per-session erase in self-host is a **selection with `enabled:false`** for `[{source, session_id}]` under the
authority, through section 5.2. It is the user's own path, and also the operator path once #349's per-session
follow-up exists. Everything below hangs off the rows it writes:

- `upload_deletions` (target) and, for Neon-era sessions only, `ingest_frozen_source_deletions`;
- `ingest_fence_versions` bumped, which fires `ingest_publication_retire_fence`;
- the head and receipt, which hide buffered rows through the admission rule (3) and rule 5.

### 7.2 Store-by-store

| Store | Mechanism | Exists? | When unreadable / when gone |
|---|---|---|---|
| Delivery ledger (`ingest_record_revisions`, `ingest_session_ownership`) | `ingest_apply_selection` deletes revisions and ownership and writes fences | yes | in the control transaction |
| Canonical publication heads, `session_index`, `session_directory` | retire trigger: generation `retired`, pointer tombstoned, directory `deleted_at`, `short_summary` cleared (060000 l.450-478) | yes | in the control transaction. Read API 404 (`liveness.ts` `ERASED_SQL`) |
| Directory-derived rows (event times, desktop catalog, replay pages, brief documents, history lineage) | triggers on `session_index` / `session_directory` tombstone (021000 l.109-151, 031000 l.94-126, 020000 l.51-70, 024000 l.107, 026000 l.164, l.1336) | yes; **verify each cascade in the implementation's PGlite test** | in the same or the next projector transaction |
| Daily digest narratives | `ingest_privacy_authority_purge_derived` → `purge_daily_digest_narratives` (guarded; 045000) | guarded no-op until 045000 | head transaction |
| Session analysis (046000) | the target-apply tombstone forgets post-cutover sessions. `_apply` purges owned Neon-era sessions | guarded until 046000 | head transaction |
| Search index (Postgres `search_index.transcript_events`, or ClickHouse) | the session-pipeline scope reconcile erases a scope with no live pointer (`src/session-pipeline/search-index.ts` header). In self-host the writer is the PG consumer (#336, open) | yes in hosted; self-host depends on #336 | the next reconcile of the scope. The read API already drops sessions whose pointers are all tombstoned from search pages (`docs/session-read-api-design.md` section 4, rule 3) |
| Canonical S3 objects (manifests, chunks, publication generations) | the transcript eraser in `supabase` mode over the union `ingest_frozen_source_deletions ∪ upload_deletions` (`ingest_privacy_eraser_page`), deleting every version by version id (`neon-s3-erasure/eraser.ts`) | the job exists; **a self-host runner (SERVICE entry, Locker credentials) is new** | on the sweep cadence. The read API is already 404 |
| Buffered stages (`buffered-batches/`) | buffered GC redaction plus version deletion. `_complete` puts the user's stages at the queue head | yes; needs `INGEST_GC_ENABLED=true` and a self-host runner | hidden at read time immediately (rule 5b, plus rule 3 because the version moved); bytes gone after the GC tick |
| Staged live batches | erased after the redrive window; mid-commit fences re-armed via `upload_erasure_rearm` | yes | up to 48 h (`docs/session-ingest-design.md`) |
| AlleyOop Locker objects (courier path) | **none today** (section 7.3) | **no** | n/a |

**The contract for "erased"** follows #349 section 4.6. A read API 404 means unreadable everywhere the read API
serves from. "Bytes gone" is a separate, later state that this design does not signal synchronously.

### 7.3 Gap: the AlleyOop upload path has no deletion fence

If a self-host uses the AlleyOop courier (`/v1/pipeline/chunks`, `/v1/pipeline/manifests`;
`docs/local-stack.md` "Flow of one session"), there are three gaps:

- **No owner record.** Its sessions are not in `ingest_record_revisions` or `ingest_session_ownership`. So
  `ingest_apply_reset`'s owned set (030000 l.448-453) does not include them, and a reset does not fence them.
- **Deletion only by object deletion.** A courier session is tombstoned only when its manifest object is deleted
  (`src/hoopdb/index.ts` l.68-100, `pointer.deleted`).
- **Resurrection.** A later upload of the same session resurrects the directory row ("resurrects a tombstone only for
  a live pointer indexed after it", index.ts l.253-254). Nothing consults `upload_deletions`.

Proposal (Q4): if self-host runs both paths, add three things:

1. **Ownership.** AlleyOop's manifest commit records `(org, ws, user, origin = "alleyoop:" || machine_id, source,
   session_id)` into `ingest_session_ownership`, so reset covers it.
2. **Write fence.** It refuses chunk and manifest writes (409 `session_deleted`) for any (org, ws, user, source,
   session) in `upload_deletions` (any origin), or uploaded before the user's latest `upload_resets.reset_at`.
3. **Erase.** On a selection or reset, it tombstones the courier's `session_index` rows for the fenced sessions
   directly. The retire trigger only handles publication-managed rows.

If self-host runs only the session-ingest path (#316), AlleyOop live writes should stay disabled, and this gap is
closed by configuration. Section 10 lists it as a precondition.

## 8. (e) How owner-buffered reads gate on it in self-host

- **Wiring (new).** The service entry (`readAppFromSecret` with `entry: "service"`) builds `ownerBuffered` when
  `RELAYHISTORY_READ_OWNER_BUFFERED` is on. It builds it exactly as the Worker's `ownerBufferedDeps`
  (`packages/session-read-worker/src/index.ts` l.356-370) does, with two substitutions:
  - `gate: selfhostControlHead(tenant.run)` instead of `serviceBindingControlHead(env.HISTORY_CONTROL_HEAD)`;
  - `store`: the self-host Locker over the existing object-store package. The bucket and credentials come from the
    secret store, never a file.
- **The rest is unchanged:** `reads: sqlOwnerBufferedReads(run)`, the bounded zstd decompressor, and the canary
  allowlist `READ_API_OWNER_BUFFERED_WORKSPACES` on top of `READ_API_DIRECTORY_WORKSPACES`.
- **New selector flag** `RELAYHISTORY_READ_CONTROL_HEAD_SOURCE`, which takes one of two values:
  - `binding`: the Worker, and the only value the Worker accepts;
  - `selfhost`: the service, and the only value the service accepts.
  Unset, with owner-buffered on, the service fails startup. This makes the transport explicit and keeps a hosted
  deploy from ever reading its own DB as the authority.
- **Startup refusals (service, flag on):**
  - the selector is not `selfhost`;
  - `sessions.selfhost_control_head_for(text)` is missing, or EXECUTE is not held (a probe call as `bulls96_read_api`
    that must fail only with 42501 or 55006, never 42883);
  - the Locker store is not configured.
  The service mirrors the Worker's `ConfigError("HISTORY_CONTROL_HEAD")` (index.ts l.260).
- **Per request.** Rules 1-5 run exactly as on the Worker:
  1. observe;
  2. the RPCs with `headParams(authority)`;
  3. the S3 fetch;
  4. the RPC recheck;
  5. observe again, then `admissionVisible` per row and stage.
  Rules 4 and 5 are enforced inside `owner_buffered_read_scope` and the keep predicate. The read API gains no new
  privilege.
- **What changes for a user after a control.** Any selection or reset bumps the head version. Every buffered upload
  admitted before it is hidden until canonically published (043700 header, "Consequence (contract, accepted)"). A
  pending head hides everything buffered for that user until complete.

## 9. Security review notes

- **Trust boundaries.**
  - Client to edge: `sh_at_` introspection, `desktop-user` only, `rth:sync` for controls.
  - Edge to Worker: HMAC (`INGEST_SIGNING_KEY`), with tenant only from introspection.
  - Worker to DB: SECURITY DEFINER functions as `bulls96_session_ingest`.
  - Read API to DB: `bulls96_read_api`, NOSUPERUSER NOBYPASSRLS, enforced at startup.
- **No client-supplied authority.** The epoch, version, digest and head never come from a client. The control-head
  producer takes the user only from a transaction setting the read API binds from the verified identity.
- **Same-DB producer.** The read role can read the head only through one definer function that returns the caller's
  own row. The function exposes no other tenant's head, policy secrets or payloads. The `payload` column (session ids
  of a selection) is **not** in the wire.
- **Hosted isolation.**
  - `selfhost_control_head_for` checks only the authority and policy rows. On a hosted DB it would answer for real
    rows, which is why the selector flag refuses `selfhost` on the Worker.
  - The parity refresh refuses without the genesis-only `selfhost` marker.
  - Q3 asks whether the producer function should also refuse without that marker.
- **Fail-closed paths.** These include a missing function, an expired policy, a fenced authority, a pending head, a
  timeout, a shape drift and two observations that differ. All of them are `BufferedFenced`, never a 500 and never an
  emit.
- **Logs.** Logs carry reasons and codes only, never session ids, token values, digests or payloads (the existing
  owner-buffered rule).

## 10. Preconditions before any self-host enablement (each needs Khaliq's OK)

1. Genesis (section 5.3) applied, and the authority `active`.
2. 043700 applied, plus the user-keyed outbox index (043700 header, "Index").
3. The new producer migration applied, and EXECUTE verified.
4. Q2 decided, and the parity writer shipped if rule 4 stays.
5. #316 merged, and the edge's privacy-authority mode on.
6. The buffered GC and the transcript eraser running in self-host. Otherwise staged and S3 bytes outlive a reset (the
   runbook's activation blocker).
7. AlleyOop live writes disabled, or section 7.3 shipped (Q4).
8. #338 before any self-host buffered search.

## 11. Test plan (for the implementation PRs; no code now)

- **PGlite / real PG (synthetic tenants only):**
  - Producer: no head returns version "0" with nulls; a head returns the exact tuple; pending is reported; a foreign
    epoch, an expired policy and a fenced or absent authority give 55006; a non-`bulls96_read_api` caller gives 42501;
    another user's head is never returned (the bound user differs from the row).
  - Genesis: activation with empty arrays reaches `active`; `_release` from active-unchanged works and is one-way
    after the first control.
  - Orchestration: begin, fence, complete for selection and reset; a crash after begin resumes; same `request_id`
    replay returns the same count; a concurrent different control is refused; receipts stay contiguous.
  - Parity refresh: compare-and-set per the 043700 writer contract; it refuses without the `selfhost` marker; the
    read gate opens only with a current row.
  - Erase cascades: every store in section 7.2 that is in-DB is asserted after a selection `enabled:false`.
- **Unit (read API):**
  - `selfhostControlHead` maps 55006, 57014, 42501 and other errors to the right `BufferedFenced` reasons.
  - The parser is shared: a producer body with any extra key fences.
  - Two observations that differ fence.
  - Startup refusals for the selector, the missing function and the missing store.
- **Unit (edge):**
  - The epoch comes only from the signed route.
  - A client cannot reach `/v1/custody/*`.
  - Every failure in the three steps answers 503 or 409 and is retryable with the same body.
  - No token, session id or digest appears in logs.

## 12. Open questions (Khaliq / pipeline-lead)

1. **Issuer.** `CONTROL_HEAD_WIRE.constants.issuer` is `"relayhistory-cloud"`, and `ingest_lazy_policy.issuer` has
   CHECK `= 'relayhistory-cloud'`. Should self-host emit and store a distinct issuer (for example
   `"sessions-pipeline"`)? That needs a parser constant per transport and a policy CHECK migration. The alternative is
   to keep `"relayhistory-cloud"` as a protocol label. A distinct issuer is recommended, so a hosted consumer can never
   accept a self-host head.
2. **Rule 4 in self-host.** With no Neon era, the G2 parity row can only be a self-consistency proof. Options:
   - (a) the section 5.5 writer (refresh after complete, plus a periodic job), which keeps rule 4 as a gate;
   - (b) keep owner-buffered reads off in self-host until a real independent proof is defined;
   - (c) a self-host-only rule-4 waiver.
   Rule 4 removal was reversed for hosted on 2026-11-08, so this doc does not assume (c).
3. Should `selfhost_control_head_for` refuse unless the genesis `selfhost` marker is set, as a second guard against
   hosted misuse beyond the selector flag?
4. **Upload paths.** Which upload path(s) does a self-host run: session-ingest via #316, the AlleyOop courier, or both?
   If both, is section 7.3 (ownership, write fence, tombstone for courier sessions) in scope for the first self-host
   privacy release?
5. Who owns the genesis script and its `control_epoch` minting? What is the policy `valid_until` renewal model in
   self-host, given that hosted renews the lazy policy from RHC?
6. **Runners.** Should the transcript eraser and the buffered GC get `SERVICE=` entries in the services image, or run
   as Kubernetes CronJobs from the same image?
7. **Distinct SQLSTATEs.** Should the authority refusals get distinct SQLSTATEs (#349 note) as part of this work? The
   user path needs `control_pending` as 409 just as the operator path does.

