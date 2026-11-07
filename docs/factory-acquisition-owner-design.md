# Factory acquisition owner and grant contract (design only)

Status: **Draft, design-only.** No migration slot is assigned. The migration registry owner assigns a slot only
after the implementation PR exists, and only after the F-lane slot order. Everything below is dormant. The SQL
switch defaults off. The Worker flag `RELAYHISTORY_ACQUISITION_OWNER_ENABLED` is proposed, default unset (off). No
route is exposed and nothing is activated. This document does not authorize a deploy, schema apply, role creation,
grant, credential capture or enablement, and it implies no operational sign-off. File names such as
`<SLOT>_ingest_acquisition_owner.sql` are placeholders.

Evidence: `origin/main` `ddce77e0`; `M:<n>` = `supabase/migrations/<n>_*.sql`. The Cloud proposal was analysed only.

## 1. Problem

Local Droid JSONL and the Factory Sessions API can both describe one native session. Nothing on main stops two
acquisition paths from publishing the same `(source, session_id)`:
- Ownership, fence and tombstone keys include user and origin: `ingest_session_ownership` PK
  (M:20261104030000:8-12), `ingest_fence_versions` (:38-43), `upload_deletions` (M:20261104022000:158-167); record
  revisions are per origin (M:20261104022000:139-154).
- `ingest_publication_heads` is PK `(org,ws,user,origin,source,session)` and UNIQUE on physical key plus
  `machine_id` (M:20261104060000:15-25), derived from workspace, origin and user (:245; `src/session-ingest/writer.ts:148`).
  The legacy-owner check compares the same machine only (:266-269), and the directory projector lets "the latest
  live pointer win" (`src/hoopdb/session-directory-projector.ts:10`), so a lagged API upsert can replace newer evidence.
- `ingest_history_scope_current` (M:20261105026000:729-732) is dormant derived-history authority (:5-6, gated by
  `ingest_history_live()` :47-50) and does not gate admission.
- `OrderedControl` is a closed `{epoch,version,digest,kind:selection|reset}` (`src/session-ingest/control-protocol.ts:4-15`);
  unknown keys are dropped or answered 401 (`src/session-ingest/app.ts:93-108`); its only authority is the ingest
  HMAC (`app.ts:68-90`); its digest is a public sha256 (M:20261104052000:77-79). It cannot carry an install proof.

## 2. Non-goals

- No change to the Desktop upload path for unenrolled sources: route, edge, signed identity, HMAC and batch
  schema stay as on main. An enrolled source adds only the `acquisition` payload key, and only after P1 (§4.1).
- No reuse of the ingest HMAC signing key (`app.ts:60-74`), `rth:sync` (`app.ts:364-365`) or `OrderedControl`
  for install, revoke or grant presentation.
- No acceptance of cloud-issuer custody witnesses (`custody-protocol.ts:9`, `app.ts:252-253`) or of the
  recovery wire (`recovery-protocol.ts:1-9`, `app.ts:449`) as acquisition evidence.
- No manufactured drain proof (§7); until the producer ships, any handover is not-ready and only provably
  unclaimed sessions install.
- No change to 049 publication ownership, privacy selection/reset or erasure; deletions and tombstones are never
  blocked. No global write hold for other sources or providers.

## 3. Data model

Schema `sessions`, like the ingest ledger. Every table enables RLS and `REVOKE ALL ... FROM
PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder,bulls96_acquisition_installer`,
following M:20261104030000:13-16.

```sql
-- supabase/migrations/<SLOT>_ingest_acquisition_owner.sql  (placeholder; no slot assigned)
CREATE TABLE sessions.ingest_acquisition_switch(          -- dormant: install refused until true
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 install_enabled boolean NOT NULL DEFAULT false,changed_at timestamptz NOT NULL DEFAULT clock_timestamp());
INSERT INTO sessions.ingest_acquisition_switch DEFAULT VALUES;
CREATE TABLE sessions.ingest_acquisition_enrollment(      -- protected (org,ws,source); absent = main behavior
 org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,
 state text NOT NULL CHECK(state IN('enrolling','enrolled')),
 backfill_sha256 text CHECK(backfill_sha256~'^[0-9a-f]{64}$'),enrolled_at timestamptz,
 PRIMARY KEY(org_id,workspace_id,source),
 CHECK((state='enrolled')=(backfill_sha256 IS NOT NULL AND enrolled_at IS NOT NULL)));
CREATE TABLE sessions.ingest_acquisition_owners(          -- fence row: never deleted; revision+tombstone live HERE
 org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
 physical_source text NOT NULL,physical_session text NOT NULL,   -- ingest_publication_component() of source/session
 state text NOT NULL CHECK(state IN('unknown','active','draining','released')),
 revision bigint NOT NULL CHECK(revision>=0),                    -- last applied control revision; never decreases
 tombstone_revision bigint CHECK(tombstone_revision>0 AND tombstone_revision<=revision),
 owner_epoch bigint NOT NULL CHECK(owner_epoch>=0),              -- +1 per successful install; never decreases
 grant_id uuid,owner_kind text CHECK(owner_kind IN('cloud','native')),owner_user_id text,owner_origin_id text,
 row_version bigint NOT NULL DEFAULT 1 CHECK(row_version>0),updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(org_id,workspace_id,source,session_id),
 CHECK((state IN('active','draining'))=(grant_id IS NOT NULL AND owner_kind IS NOT NULL
   AND owner_user_id IS NOT NULL AND owner_origin_id IS NOT NULL)));
CREATE INDEX ingest_acquisition_owners_physical ON sessions.ingest_acquisition_owners(org_id,workspace_id,physical_source,physical_session);
CREATE UNIQUE INDEX ingest_acquisition_owners_physical_live ON sessions.ingest_acquisition_owners  -- §3 collision
 (org_id,workspace_id,physical_source,physical_session) WHERE state IN('active','draining');
CREATE TABLE sessions.ingest_acquisition_grants(          -- the replaceable binding; one row per install
 grant_id uuid PRIMARY KEY,                               -- = the install operation_uuid
 org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
 owner_epoch bigint NOT NULL CHECK(owner_epoch>0),predecessor_grant_id uuid REFERENCES sessions.ingest_acquisition_grants,
 owner_kind text NOT NULL CHECK(owner_kind IN('cloud','native')),owner_user_id text NOT NULL,owner_origin_id text NOT NULL,
 binding_uuid uuid,installation_id uuid,grant_sha256 text NOT NULL UNIQUE CHECK(grant_sha256~'^[0-9a-f]{64}$'),
 evidence_sha256 text NOT NULL CHECK(evidence_sha256~'^[0-9a-f]{64}$'),issuer_key_id text NOT NULL,
 expires_at timestamptz,state text NOT NULL CHECK(state IN('active','draining','released')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),released_at timestamptz,
 UNIQUE(org_id,workspace_id,source,session_id,owner_epoch),
 FOREIGN KEY(org_id,workspace_id,source,session_id) REFERENCES sessions.ingest_acquisition_owners ON DELETE RESTRICT,
 CHECK((owner_kind='cloud')=(binding_uuid IS NOT NULL AND installation_id IS NULL)),
 CHECK((owner_kind='native')=(installation_id IS NOT NULL AND binding_uuid IS NULL)));
CREATE TABLE sessions.ingest_acquisition_revoked_grants(  -- revoke-before-install target; immutable
 grant_id uuid PRIMARY KEY,org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
 owner_epoch bigint NOT NULL CHECK(owner_epoch>0),owner_kind text NOT NULL CHECK(owner_kind IN('cloud','native')),
 binding_uuid uuid,installation_id uuid,revision bigint NOT NULL CHECK(revision>0),
 evidence_sha256 text NOT NULL CHECK(evidence_sha256~'^[0-9a-f]{64}$'));
CREATE TABLE sessions.ingest_acquisition_operations(      -- idempotency ledger (install and revoke)
 operation_uuid uuid PRIMARY KEY,kind text NOT NULL CHECK(kind IN('install','revoke')),
 org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
 revision bigint NOT NULL CHECK(revision>0),request_sha256 text NOT NULL CHECK(request_sha256~'^[0-9a-f]{64}$'),
 result jsonb NOT NULL CHECK(jsonb_typeof(result)='object'),created_at timestamptz NOT NULL DEFAULT clock_timestamp());
ALTER TABLE sessions.ingest_claim_attempts ADD COLUMN acquisition jsonb NOT NULL DEFAULT '[]'
 CHECK(jsonb_typeof(acquisition)='array');                -- admitted [{source,session_id,owner_epoch,grant_id}]
ALTER TABLE sessions.ingest_publication_heads ADD COLUMN acquisition_epoch bigint NOT NULL DEFAULT 0
 CHECK(acquisition_epoch>=0);
```

**Physical collision.** `ingest_publication_component` maps many characters to `_` (M:20261104060000:85-95), so
distinct logical sessions can share a physical key. At most one `active`/`draining` owner per physical key is a DDL
invariant (the partial UNIQUE index). Install, revoke and drain also take the physical try-lock `LP` (§4.5)
exclusive, so the check and the owner write are one step under one lock; a residual `23505` maps to the terminal
`55000 physical_collision`.

**Owner states and transitions.** `unknown`: a pre-existing session found by the enrollment backfill, no grant;
it never becomes free and needs the drain producer before an install. `active`: the current grant may admit.
`draining`: revoked for new admission; accepted obligations may still publish. `released`: drain proven (§7). The
grant row mirrors `active`/`draining`/`released`. Transitions: absent or `released` → `active` (install); `active`
→ `draining` (revoke); `draining` → `released` (drain producer only). No transition deletes a fence row. The grant
carries no revision and no tombstone; those live on the fence row, so a replaced or deleted binding cannot reset them.

**Grant secret and hash.**
- Secret `G`: `rhg_` + 64 lowercase hex (32 CSPRNG bytes), per install. The issuer generates it (Cloud for Cloud
  owners; the native issuer for one installation, delivered only to it) and persists it encrypted with its intent
  (§4.2) before the first install call. The database stores only the hash and never sees `G` at install.
- Formula (the only one; §5.1 admission reuses it): `grant_sha256 = lowerhex(SHA-256(UTF-8(join(U+001F,
  ["relayhistory-acquisition-grant-v1", org_id, workspace_id, source, session_id, grant_id, G]))))`, where
  `grant_id` is the lowercase canonical UUID of the install operation.
- Encoding: identifiers can contain no control characters (M:20261104022000:236-238), so U+001F joins are
  unambiguous. This is the same convention as the machine id (M:20261104060000:245). An empty `workspace_id` keeps
  its position. Inputs are validated byte-exactly, never trimmed or case-folded.
- Shared vector (Cloud and SQL tests): org `org_test`, ws `ws_test`, `droid`, `sess-0001`, grant id
  `00000000-0000-4000-8000-000000000001`, `G` = `rhg_` + `0123456789abcdef`×4 →
  `eca3f7669845e8b736db441074fcbaa51183bfb167f7cc71003d4d564496d227`. Cross-scope negative: the same inputs with
  `sess-0002` → `359de6c40574725c3256fb529e69c51c4cdefbe06c1cd3558c1068b8bd9019b4`, which must not admit `sess-0001`.

**Assertion, evidence and request digest (no self-reference).** Every field below is a payload key.
- `A` = UTF-8 of the U+001F join of `["relayhistory-acquisition-assertion-install-v1", operationUuid, orgId,
  workspaceId, source, sessionId, revision, expectedRevision, expectedOwnerEpoch, expectedPredecessorGrantId,
  ownerKind, ownerUserId, ownerOriginId, bindingUuid, installationId, grantSha256, expiresAt, issuerKeyId]`, or for
  revoke `["relayhistory-acquisition-assertion-revoke-v1", operationUuid, orgId, workspaceId, source, sessionId,
  revision, targetGrantId, targetOwnerEpoch, targetOwnerKind, targetBindingUuid, targetInstallationId, issuerKeyId]`.
  `A` contains no evidence, signature or request digest.
- `S` = the 64-byte Ed25519 signature over exactly `A` by key `issuerKeyId`.
- `evidenceSha256` = lowerhex SHA-256(`A` ‖ 0x1F ‖ `S`), the completed envelope.
- `requestSha256` = lowerhex SHA-256(UTF-8(join(U+001F, ["relayhistory-acquisition-request-v1",
  lowerhex SHA-256(`A`), evidenceSha256]))).

The issuer persists `A`, `S` and `evidenceSha256` once and never re-signs on retry. The installer receives `S`,
verifies it over the rebuilt `A` and recomputes `evidenceSha256`; SQL rebuilds `A` and refuses a `requestSha256`
mismatch (pattern M:20261104052000:77-79). Encoding: decimal integers without leading zeros; absent optional values
`""`; `expiresAt` RFC 3339 UTC seconds or `""` (non-expiring). Vector (§3 grant vector scope, `operationUuid` = its
grant id, revision 1, expected 0/0/`""`, `cloud`, user `user_test`, origin `origin_test`, binding
`00000000-0000-4000-8000-000000000002`, no expiry, key `test-key-1` = RFC 8032 test key 1): SHA-256(`A`)
`54e325f1f60abce8e8666d2737de68903a7b4f564fed091e22806f939d3ffc75`, `S` begins `fa06607856b9cc7b`, evidence
`9fabebd5cbc741c975fbfd3bb58ea4f127a187240979a1c5ea5960df006138af`, request
`fc501fbafdb4618311ff3ecc20b82e75f0d005e4a652a1a09492931613ebac29`.

### 3.1 Functions

All functions take `p jsonb` and validate it with `ingest_exact_keys` and `ingest_identifier`. They are `SECURITY
DEFINER` with `SET search_path=pg_catalog,pg_temp`. Install and revoke keys are exactly their `A` fields plus
`evidenceSha256` and `requestSha256`.

| Function | Caller (EXECUTE) | Returns |
|---|---|---|
| `sessions.ingest_acquisition_install(p jsonb)` | `bulls96_acquisition_installer` only; guard `ingest_require_role` (M:20261104022000:220-233) | `{outcome:'installed'\|'replay',grantId,ownerEpoch,revision,state:'active'}` |
| `sessions.ingest_acquisition_revoke(p jsonb)` | same | `{outcome:'draining'\|'revoked_pending'\|'superseded'\|'replay',revision,ownerEpoch,state}` |
| `sessions.ingest_acquisition_status(p jsonb)` keys `orgId,workspaceId,source,sessionIds`(≤500) | same | per session `{state,revision,tombstoneRevision,ownerEpoch,ownerKind,grantId}`; never a grant hash |
| `sessions.ingest_acquisition_enroll_begin/complete(p jsonb)` | no role; owner-run only under an explicit apply OK | enrollment state |
| `sessions.ingest_acquisition_lock(org,ws,pairs jsonb,exclusive boolean)`, `_admit(...)`, `_fenced(...)` | owner-only internals, called inside the existing SD entrypoints, like M:20261104052000:21 | void / snapshot / mismatched pairs |

**Install, in order:**
1. Check the role, validate the keys, require `install_enabled` (revoke skips this) and recompute the digest.
2. Look up the operation. The same `operationUuid` with the same kind and digest returns the stored result. A
   different digest is `operation_conflict`.
3. Require an `enrolled` enrollment row, read with `FOR SHARE NOWAIT`.
4. Take the exclusive acquisition try-lock.
5. Take `LP` exclusive, read the owner row `FOR UPDATE`, then repeat step 2 so a just-committed twin replays.
6. Check revisions:
   - `operationUuid` in `ingest_acquisition_revoked_grants`, or `p.revision <= tombstone_revision` →
     `revoked_before_install`;
   - otherwise `p.revision <= revision` → `stale_revision`;
   - `expectedRevision` ≠ the current revision (0 if absent), or `revision` ≠ `expectedRevision + 1` →
     `revision_cas_lost`.
7. Check the predecessor (§4.3):
   - state `unknown` → `owner_unknown`;
   - `active` or `draining` → `handover_not_ready`;
   - `expectedOwnerEpoch` and `expectedPredecessorGrantId` must equal the current epoch and the latest grant
     (0 and null when absent), else `predecessor_mismatch`.
8. If `owner_epoch=0`, prove the session is fresh: no row for `(org,ws,source,session)` under any user or origin
   in `ingest_session_ownership`, `ingest_fence_versions`, `upload_deletions`, `ingest_record_revisions` or
   `ingest_publication_heads`, and no `session_index` row on its physical key. Otherwise `owner_unknown`.
9. Refuse another `active`/`draining` owner on the physical key (`physical_collision`, terminal).
10. Write the fence row (`active`, `owner_epoch+1`), the grant and the operation result in one transaction.

**Revoke** ignores the switch, and always names a concrete target: `targetGrantId`, `targetOwnerEpoch`,
`targetOwnerKind` and its `targetBindingUuid` or `targetInstallationId`. A missing target is `invalid_request`.
1. Run install steps 1-2 and 4-5.
2. If the grant row `targetGrantId` exists: its scope, epoch, kind and binding/installation must equal the target,
   else `42501 revoke_target_mismatch`. If it is the current `active` grant, require `p.revision > revision`
   (else `stale_revision`), set owner and grant `draining`, and set revision = tombstone = `p.revision`. If it is
   already `draining`/`released` or superseded, return `superseded` without change.
3. If it does not exist (revoke-before-install), insert the target into `ingest_acquisition_revoked_grants` and
   leave the owner row untouched (`revoked_pending`). A later install with that `operationUuid` refuses
   `revoked_before_install`.

A revoke therefore never affects another binding, owner kind or epoch. The installer refuses a Cloud key for a
native target and vice versa (`not_authorized`).

**Error contract.** The message is the stable code, prefixed `acquisition: `.

| SQLSTATE | Codes |
|---|---|
| `22023` | `invalid_request`, `request_digest_mismatch`, `acquisition_unexpected` |
| installer, before SQL | `not_authorized` (§4.1) |
| `42501` | `not_installer`, `not_owner`, `revoke_target_mismatch` |
| `55000` | `disabled`, `not_enrolled`, `owner_unknown`, `owner_draining`, `owner_released`, `revoked_before_install`, `handover_not_ready`, `grant_expired`, `physical_collision`, `owner_changed`, `drain_not_ready`, `enrollment_not_quiescent` |
| `40001` | `session_busy`, `revision_cas_lost`, `stale_revision`, `predecessor_mismatch`, `operation_conflict` |

## 4. Open conditions from the migration-owner installer-boundary review (2026-11-07)

**4.1 A trusted service derives and authorizes the binding; neither role nor predecessor is proof.** The
control-only role remains a *candidate* least-privilege boundary until the concrete producer is chosen. No broad
owner or global custody capability is reused. Each install or revoke carries `S` over `A` (§3), from an issuer that
derived the scope itself and decided per session. Roots of trust, each independent of the request: `S` verifies
against an issuer public key pinned in the installer's `issuerKeyId` registry (one registry per issuer class;
overlapping keys during rotation), never against a key named only by the request; `G` comes from the issuer's CSPRNG
and the database holds only its hash; the installer database password lives in 1Password.
- Cloud (answered, Q1): the Factory binding service derives `org, ws, user, source, session_id, binding_uuid` and
  Cloud's origin from its committed, authorized binding and connection (pinned computer route, binding/control
  generation). It never signs a request-supplied tuple. That binding selecting this SID is the authorization.
- Native: the native issuer (Q2) derives `org, ws, user` from token introspection plus the `installation_id` and
  origin it issued. In v1 it authorizes a SID only as a first claim of a fresh session (§3 step 8). A successor to
  any prior owner needs a release assertion from the predecessor's issuer naming this installation; v1 does not ship
  one (`handover_not_ready`). Residual: among one user's authenticated installations, first claim wins.
- `A` covers `expectedOwnerEpoch` and `expectedPredecessorGrantId`, so an assertion is bound to one predecessor.

**Order and codes.** Authorization fails first. The installer verifies signature, scope and issuer class (Cloud key:
`cloud` + `bindingUuid` only; native key: `native` + `installationId` only) and otherwise refuses with
`not_authorized` without calling SQL. SQL then checks role (`42501 not_installer`) and digest (`22023`), and only
then the revision and predecessor CAS (§3 steps 6-7). Those are ordering checks (`40001`/`55000`), never
authorization. `evidenceSha256` and `issuerKeyId` are stored on the grant. SQL does not verify Ed25519; a bare role
call without verified evidence is out of contract.

**Precondition P1 (not an open question).** A native user plus caller-supplied `origin_id` is not installation
proof. No source may be enrolled, and no protected `droid` admission may ship, until the native issuer delivers a
per-installation `G` only to that installation and Desktop presents it (§5.1). Native admission then runs exactly
the Cloud grant check. Unenrolled sources keep current behaviour.

**4.2 Cross-database consistency is by committed intent, not a distributed transaction.** Cloud first commits an
intent row, before any remote call: `operation_uuid, kind, scope, revision, expected_revision, expected_owner_epoch,
expected_predecessor_grant_id` (the predecessor pointer), `grant_sha256`, the encrypted `G`, the signed `A`/`S`,
`evidence_sha256`, `request_sha256` and `status` (`pending|applied|refused`). A reconciliation loop holds no local
mutex across the network. It sends `pending` intents per session in revision order, exactly and unchanged; marks
them `applied` on a result or `replay`; marks any other refusal `refused` (`stale_revision`, `revision_cas_lost`,
`predecessor_mismatch`, `operation_conflict` are terminal though `40001`, as at M:20261104052000:88-95); retries
only `session_busy` and transport failures; and reads `status` to adopt the authority's revision and tombstone. A
replayed install result is not proof the grant is active now; use `status`. A lost response is resolved by exact
replay of the same `operationUuid`. Unbind first commits a revoke intent naming the bound grant (§3 revoke target).
A `refused` intent is never edited; a new intent gets a new `operationUuid`. The native issuer uses the same loop.

**4.3 An unrelated grant cannot seize a session.** Seizure is prevented by §4.1 authorization: an unrelated
installation holds no assertion naming this SID and predecessor. Predecessor CAS only orders authorized requests: a
stale expectation gets `40001 predecessor_mismatch`; an `active`, `draining` or `unknown` session refuses with
`55000 handover_not_ready` or `owner_unknown`.

**4.4 An empty owner table is not proof of absence.** `enroll_begin` sets `enrolling` and records
`pg_current_xact_id()`; admission and the pointer guard (§5.5) then treat the source as enrolled. `enroll_complete`
refuses `55000 enrollment_not_quiescent` while any transaction older than that id is open (`pg_snapshot_xmin`), so
no legacy write that began unguarded can commit after the backfill. It then backfills an `unknown` row for every
`(source, session_id)` found in the step-8 tables, `session_index` and the live publication heads/pointers of that
physical source, records `backfill_sha256` over the sorted keys and sets `enrolled`. For an enrolled source,
admission treats no owner row, `unknown`, `released` and `draining` as not-ready, never as free. Pre-existing
sessions are never implicitly unowned; native upload resumes only through an authorized native install (§4.1).

**4.5 Lock graph (one order, used at every hook).** Existing rules: authority rows precede USER, BATCH and INTENT
(M:20261104050000:56; M:20261104052000:53); the user fence is `ingest_lock_fences` (M:20261104030000:26-33). `L0`
authority rows `FOR SHARE` → `L1` user fence → `L2` batch → `L3` stage intent → `L4` outbox → `L5` publication
pointer try-lock and head → `LA` acquisition → `LP` physical → owner row. `LA` is new and always the leaf of a hook:
the enrollment row `FOR SHARE NOWAIT`, then `pg_try_advisory_xact_lock_shared(hashtextextended(concat_ws(chr(31),
'96bulls:acquisition',org,ws,source,session),0))` per session in `(source, session_id) COLLATE "C"` order
(M:20261104030000:96). `LP` is the same try-lock over `'96bulls:acquisition-physical',org,ws,physical_source,
physical_session`, always after `LA`. Hooks never lock owner rows; they read them under `LA`. Install, revoke and
drain take only `LA` then `LP` exclusive and then the owner row `FOR UPDATE`, holding nothing else. `LA`/`LP` are
never waited on (`40001 session_busy`), and the only owner-row lockers hold only `LA`/`LP`, so no cycle exists.
Reset and selection (`L0`, `L1`) and buffered GC (`L1` per candidate, M:20261104055000:1557) take no `LA`, so
privacy and cleanup are never blocked. The pointer guard keeps its pointer → user `NOWAIT` order
(M:20261104060000:417-424) and takes `LP` shared itself, since a legacy caller holds no `LA`. §5 names the exact
position at each hook.

**4.6 Denial versus final refusal.** An admission denial raises inside the claim transaction before any durable
write. The `ON CONFLICT` insert in `ingest_lock_fences` (M:20261104030000:29) rolls back with it, so there are zero
ledger writes. Tests assert unchanged row counts in every ingest table. A refusal at buffered commit, publication
claim, publish or the pointer guard happens after the stage or image PUT. It is **not** side-effect free: the
objects, stage intents and their expiry/GC cleanup obligations stay; the head is not advanced and the pointer is not
written, so nothing becomes visible. Commit and claim refusals return `55000 owner_changed`; a guard refusal
surfaces as the existing publish `40001` (M:20261104060000:720).

## 5. Admission and publication recheck (exact hooks)

Current bodies are in M:20261104055000, which redefines the M:20261104050000 functions. Every `LA` below is taken
after that hook's existing locks, per §4.5.
1. `ingest_claim_batch_mode`. Current body M:20261104055000:1081-1313; first body M:20261104050000:85-130.
   - Existing `L1` (:1129) and batch locks, then `LA` over the session set of the epoch snapshot (:1302-1306),
     then `_admit` before the ownership insert (:1289) when `v_state IN('claimed','resume')`.
   - `_admit` applies the §4.4 not-ready rules, then the grant check, identical for both owner kinds (P1). Each
     protected session needs exactly one entry `{source,sessionId,grantId,ownerEpoch,grant}`, sorted, no
     duplicates. Strict validation, never normalization: `grant` ~ `^rhg_[0-9a-f]{64}$`, `grantId` a lowercase
     canonical UUID, `ownerEpoch` a positive decimal, `source`/`sessionId` byte-equal to a batch session.
   - `_admit` rebuilds the §3 grant preimage from the authenticated `k.org_id, k.workspace_id` and the entry, and
     requires its hash to equal `grant_sha256`. `grantId`/`ownerEpoch` must be current, `expires_at` null or in
     the future, and `k.user_id/k.origin_id` the owner's.
   - Any miss rejects the whole batch (`not_owner`, `grant_expired` or `owner_*`); nothing is dropped. The
     snapshot goes to `ingest_claim_attempts.acquisition` (:1308). An entry for an unenrolled source is
     `acquisition_unexpected`. Exact replays of committed receipts skip `_admit`. `ingest_claim_batch`
     (M:20261104050000:306) and `ingest_claim_buffered_batch` (055000:748) inherit this.
2. Accepted-stage recheck: `LA`, then `_fenced(...,'admit')` beside `ingest_attempt_fenced`, requiring `active`
   and the same epoch and grant, in `ingest_prepare_buffer_stage` (M:20261104055000:799 → :854),
   `ingest_commit_buffered_batch` (:894 → :914) and `ingest_commit_batch` (M:20261104060000:1017 → :1024).
3. `ingest_publication_claim` (M:20261104060000:179-305): `L1` (:196), batch and stage intent (:200-204), pointer
   try-lock (:259-260) and head, then `LA`. Then require the snapshot epoch to equal the current `owner_epoch`
   with state `active` or `draining`. Write `acquisition_epoch` on the head and add
   `h.acquisition_epoch<>v_owner_epoch` to the rollover predicate (:278).
4. `ingest_publication_publish` (:626): directory-events try-lock (:653), `ingest_publication_locked` (:656;
   stage, attempt and pointer try-lock), then `LA` and the recheck. Add
   `AND acquisition_epoch=(v->>'acquisitionEpoch')::bigint` to the head CAS (:712-716). The pointer CAS
   (:718-720) stays the final gate.
5. `ingest_publication_guard_pointer` (:407-448). Before the non-`srv_` early return (:412-414), for every live
   write (not `DELETE`, `deleted_at IS NULL`) whose org, workspace and physical source equal an enrollment row's
   (`ingest_publication_component(source)`) in any state, take `LP` shared (busy → `RETURN NULL`). Then require an
   `active`/`draining` owner on the physical key, an `srv_` machine (:245) and a head whose logical session,
   `acquisition_epoch` and user/origin equal the owner's (beside the final CAS, :440-444). Anything else (no owner,
   `unknown`, `released`, a legacy non-`srv_` writer, Q8) → `RETURN NULL`, fail-closed; publish then raises `40001`
   (:720) and a legacy writer's row is not written. Unenrolled sources keep today's guard. Tombstones and retire
   paths (:426-433, :450-482) are untouched.

## 6. Principals and roles

- **Control role `bulls96_acquisition_installer`.** `NOLOGIN NOINHERIT` in `supabase/bootstrap/session_ingest_roles.sql`
  (pattern `:42`) and its RDS/Cloud SQL variants; LOGIN via a new `acquisition_installer_role.sql` (pattern
  `privacy_eraser_role.sql:1-15`) plus `acquisition_installer_verify.sql`. Grants: `USAGE` on `sessions` and
  `EXECUTE` on install, revoke and status only, no table right. `bulls96_session_ingest` gets no `EXECUTE` on them
  (unlike privacy begin/complete, M:20261105041000:552).
- **Installer principal.** A separate service identity; its database password lives in 1Password, never on disk.
  It holds the pinned issuer public keys and is never the ingest signing key or `rth:sync`.
- **Grant presentation.** Both owner kinds present the `acquisition` array in the claim payload, inside the
  existing signed body; the edge passes it through and gives it no authority. Cloud uses its own publish path (Q3);
  native uses the Desktop upload of an enrolled source (P1). The pipeline never logs `G`.

## 7. Drain receipt producer (not in the first migration)

Two obligation classes for `(org, ws, source, session)` under epoch E:
- **Accepted** (acknowledged to the writer): a committed batch whose attempt snapshot names the session, an
  acknowledged live stage intent with a `buffered` outbox (M:20261104050000:20-50), an unpublished receipt seat
  (M:20261104053000:43-60), or a head with a live `build_lease_until`. Only these are handover-readiness evidence;
  they are published or explicitly reconciled, never deleted or expired to make a drain look empty.
- **Unacknowledged**: a `pending` claim or a prepared, unacknowledged stage. Not readiness evidence; they cannot
  become accepted under a `draining` epoch (§5.2 requires `active`), and existing GC cleans them up.

`draining → released` may be applied only by a producer that takes `LA` exclusive and proves from committed
ledger state, never from the owner table, that no accepted obligation remains. It records the evidence digest.
Until it ships with tests on real accepted stages, handover and drain return `handover_not_ready`/`drain_not_ready`,
`unknown` sessions stay not-ready, and only fresh sessions (§3 step 8) can be installed.

## 8. Open questions and implementation plan

Open questions. Answered by Cloud (review of 2026-11-07), now constraints:
1. Issuer: the Factory binding service (§4.1) with a dedicated server-managed Ed25519 key and a pinned `issuerKeyId`
   registry (not built yet); never the user's Factory key, the ingest HMAC, the audit purpose or the generic
   history assertion key. Rotation keeps overlapping keys until old intents reconcile; `A`/`S` are never re-signed.
3. Transport (partly): a separately authenticated Cloud publisher port at the History gateway, then the existing
   private Ingest binding. Desktop sync authority never becomes installer authority; the proof stays out of
   provider/canonical payloads, logs, traces and archives; both parsers agree; attempts pin grant and epoch and a
   retry never injects a new grant into an immutable outbox. Still open: exact route and credential.
5. `expiresAt` null = non-expiring in v1; a future TTL needs a reviewed renewal CAS. Expiry never releases ownership.
6. Enrollment is exactly `(org, ws, droid)`, dormant, never on API-key save or session bind; it waits for P1, protected
   admission on every writer path, the backfill and tested rollback/hold. Selected-session-first needs its own review.
7. Permanent `draining`/`unknown` before §7 is acceptable only for a dormant source and a bounded fresh-session
   canary, not a general release; readiness stays explicit; obligations never expire or delete.

Awaiting Desktop:
2. Which service issues native installations and their per-installation `G` in self-host (relayauth removed), and
   what setup channel delivers it? Blocks P1. Cloud: the Desktop MCP credential (`rth:read` + `mcp:desktop`) is at
   most an enrollment bootstrap candidate, not P1.
4. For a refused protected session (`owner_*`, `not_owner`), what HTTP status and code should Desktop hold on?
   It must not mark records uploaded or advance checkpoints.
8. Does any legacy non-`srv_` pointer writer still serve `droid`? Cloud has no active legacy Factory publisher;
   others are not ruled out. §5.5 refuses such live writes for enrolled sources.

Implementation plan. One PR, dormant:
1. Migration and rollback `<SLOT>_ingest_acquisition_owner.sql` (§3 DDL, functions, hooks, REVOKEs); bootstrap
   role files; function-boundary lane entries and verifiers (`scripts/sessions-function-boundary/`).
2. Tests on disposable Postgres: every error code; revoke-before-install; exact replay and operation conflict;
   predecessor seizure; a delayed revoke cannot touch another binding, kind or epoch (`revoke_target_mismatch`);
   enrolled no-row and `unknown` refusals; zero-write denial; post-PUT refusal that preserves objects;
   `enrollment_not_quiescent`; and a real two-connection, both-orders test for admission↔install, publication
   claim↔revoke, publish↔guard, legacy pointer insert/update↔install and ↔revoke, two installs on one physical
   key (`physical_collision`), buffered stage/commit↔revoke, reset↔admission and GC↔publication; the §3 grant
   and assertion vectors in both Cloud and SQL tests.
3. Indexes the step-8 scans need. Indexes built `CONCURRENTLY` are an operator step with no slot.

The migration registry owner assigns the slot after the PR exists, in F-lane order. Applying it, enrolling any
source and enabling the switch each need Khaliq's separate OK.
