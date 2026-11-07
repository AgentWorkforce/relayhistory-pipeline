# Factory acquisition owner and grant contract (design only)

Status: **Draft, design-only.** No migration slot is assigned (§8). Dormant: the SQL switch defaults off, the
proposed flag `RELAYHISTORY_ACQUISITION_OWNER_ENABLED` defaults unset, no route is exposed. This document authorizes
no deploy, schema apply, role, grant, credential capture or enablement, and implies no operational sign-off.
Evidence: `origin/main` `ddce77e0`; `M:<n>` = `supabase/migrations/<n>_*.sql`. External proposals: analysed only.

## 1. Problem

Local Droid JSONL and the Factory Sessions API can describe one native session; nothing on main stops two acquisition
paths publishing the same `(source, session_id)`. Ownership, fence, tombstone and revision keys include user and
origin (M:20261104030000:8-12, :38-43; M:20261104022000:139-167); publication heads are per `(user, origin)` with a
machine id from workspace, origin and user (M:20261104060000:15-25, :245; `src/session-ingest/writer.ts:147-149`);
the legacy-owner check compares the same machine only (:266-269); and the directory projector lets "the latest live
pointer win" (`src/hoopdb/session-directory-projector.ts:10`), so a lagged API upsert can replace newer evidence.
`ingest_history_scope_current` (M:20261105026000:729-732) is dormant, not an admission gate.

## 2. Non-goals

- No change to the upload path of unenrolled sources; an enrolled source adds only the `acquisition` key (P1, P2).
- No reuse of the ingest HMAC key (`app.ts:60-74`), `rth:sync` (`app.ts:364-365`), `OrderedControl`
  (`src/session-ingest/control-protocol.ts:4-15`, which cannot carry an install proof), custody witnesses
  (`custody-protocol.ts:9`) or the recovery wire (`recovery-protocol.ts:1-9`) as acquisition evidence.
- No manufactured drain proof (§7). No change to 049 publication ownership, privacy selection/reset or erasure;
  deletions and tombstones are never blocked. No global write hold for other sources or providers.

## 3. Data model

Schema `sessions`, like the ingest ledger. Every table: RLS and `REVOKE ALL ... FROM
PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder,bulls96_acquisition_installer`,
per M:20261104030000:13-16.

```sql
-- supabase/migrations/<SLOT>_ingest_acquisition_owner.sql  (placeholder; no slot assigned)
CREATE TABLE sessions.ingest_acquisition_switch(          -- dormant: install refused until true
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 install_enabled boolean NOT NULL DEFAULT false,changed_at timestamptz NOT NULL DEFAULT clock_timestamp());
INSERT INTO sessions.ingest_acquisition_switch DEFAULT VALUES;
CREATE TABLE sessions.ingest_acquisition_enrollment_gate( -- §4.8 interlock row; never deleted
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),generation bigint NOT NULL DEFAULT 0 CHECK(generation>=0));
INSERT INTO sessions.ingest_acquisition_enrollment_gate DEFAULT VALUES;
CREATE TABLE sessions.ingest_acquisition_enrollment(      -- protected (org,ws,source); absent = main behavior
 org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,
 state text NOT NULL CHECK(state IN('enrolling','enrolled')),PRIMARY KEY(org_id,workspace_id,source),
 backfill_sha256 text CHECK(backfill_sha256~'^[0-9a-f]{64}$'),enrolled_at timestamptz,
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
CREATE TABLE sessions.ingest_acquisition_grant_origins(   -- append-only origins of one native grant (§4.7)
 grant_id uuid NOT NULL REFERENCES sessions.ingest_acquisition_grants,origin_id text NOT NULL,
 revision bigint NOT NULL CHECK(revision>0),PRIMARY KEY(grant_id,origin_id));
CREATE TABLE sessions.ingest_acquisition_revoked_grants(  -- revoke-before-install target; immutable
 grant_id uuid PRIMARY KEY,org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
 owner_epoch bigint NOT NULL CHECK(owner_epoch>0),owner_kind text NOT NULL CHECK(owner_kind IN('cloud','native')),
 binding_uuid uuid,installation_id uuid,revision bigint NOT NULL CHECK(revision>0),
 evidence_sha256 text NOT NULL CHECK(evidence_sha256~'^[0-9a-f]{64}$'));
CREATE TABLE sessions.ingest_acquisition_revoked_lineages( -- §4.9 binding-lineage fence; immutable
 org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
 owner_kind text NOT NULL CHECK(owner_kind IN('cloud','native')),lineage_id uuid NOT NULL, -- binding_uuid|installation_id
 revision bigint NOT NULL CHECK(revision>0),PRIMARY KEY(org_id,workspace_id,source,session_id,owner_kind,lineage_id));
CREATE TABLE sessions.ingest_acquisition_operations(      -- idempotency ledger (install/revoke/rebind)
 operation_uuid uuid PRIMARY KEY,kind text NOT NULL CHECK(kind IN('install','revoke','rebind')),
 org_id text NOT NULL,workspace_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
 revision bigint NOT NULL CHECK(revision>0),request_sha256 text NOT NULL CHECK(request_sha256~'^[0-9a-f]{64}$'),
 result jsonb NOT NULL CHECK(jsonb_typeof(result)='object'),created_at timestamptz NOT NULL DEFAULT clock_timestamp());
ALTER TABLE sessions.ingest_claim_attempts ADD COLUMN acquisition jsonb NOT NULL DEFAULT '[]'
 CHECK(jsonb_typeof(acquisition)='array');                -- admitted [{source,session_id,owner_epoch,grant_id}]
ALTER TABLE sessions.ingest_publication_heads ADD COLUMN acquisition_epoch bigint NOT NULL DEFAULT 0 CHECK(acquisition_epoch>=0);
```

**Physical collision.** `ingest_publication_component` maps many characters to `_` (M:20261104060000:85-95), so
distinct logical sessions can share a physical key. At most one `active`/`draining` owner per physical key is a DDL
invariant (the partial UNIQUE index); install, revoke and drain also hold `LP` (§4.5) exclusive, so check and write
are one step, and a residual `23505` maps to terminal `55000 physical_collision`.

**Owner states and transitions.** `unknown`: found by the enrollment backfill, no grant, never free; needs the drain
producer before an install. `active`: the current grant may admit. `draining`: revoked for new admission; accepted
obligations may still publish. `released`: drain proven (§7). The grant row mirrors these. Install: absent or
`released` → `active`; revoke: `active` → `draining`; drain producer only: `draining` → `released`. Fence rows are
never deleted and hold revision and tombstone, so a replaced or deleted binding cannot reset them.

**Grant secret and hash.**
- Secret `G`: `rhg_` + 64 lowercase hex (32 CSPRNG bytes), per install, generated by the issuer (Cloud for Cloud
  owners; the native issuer for one installation, delivered only to it) and persisted encrypted with its intent
  (§4.2) before the first install call. The database stores only its hash.
- Formula (the only one; §5.1 admission reuses it): `grant_sha256 = lowerhex(SHA-256(UTF-8(join(U+001F,
  ["relayhistory-acquisition-grant-v1", org_id, workspace_id, source, session_id, grant_id, G]))))`, where `grant_id`
  is the lowercase canonical UUID of the install operation. Identifiers contain no control characters
  (M:20261104022000:236-238), so U+001F joins are unambiguous (as for the machine id, M:20261104060000:245); an empty
  `workspace_id` keeps its position; inputs are validated byte-exactly, never trimmed or case-folded.
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
- `S` = the 64-byte Ed25519 signature over exactly `A` by key `issuerKeyId`; `evidenceSha256` = lowerhex SHA-256(`A`
  ‖ 0x1F ‖ `S`), the completed envelope; `requestSha256` = lowerhex SHA-256(UTF-8(join(U+001F,
  ["relayhistory-acquisition-request-v1", lowerhex SHA-256(`A`), evidenceSha256]))).

The issuer persists `A`, `S` and `evidenceSha256` once and never re-signs on retry. The installer verifies `S` over
the rebuilt `A` and recomputes `evidenceSha256`; SQL rebuilds `A` and refuses a `requestSha256` mismatch (pattern
M:20261104052000:77-79). Encoding: decimal integers without leading zeros; absent optional values `""`; `expiresAt`
RFC 3339 UTC seconds or `""`. Vector (§3 grant vector scope, `operationUuid` = its grant id, revision 1, expected
0/0/`""`, `cloud`, user `user_test`, origin `origin_test`, binding `00000000-0000-4000-8000-000000000002`, no expiry,
key `test-key-1` = RFC 8032 test key 1): SHA-256(`A`)
`54e325f1f60abce8e8666d2737de68903a7b4f564fed091e22806f939d3ffc75`, `S` begins `fa06607856b9cc7b`, evidence
`9fabebd5cbc741c975fbfd3bb58ea4f127a187240979a1c5ea5960df006138af`, request
`fc501fbafdb4618311ff3ecc20b82e75f0d005e4a652a1a09492931613ebac29`.

### 3.1 Functions

All functions take `p jsonb`, validated by `ingest_exact_keys` and `ingest_identifier`. They are `SECURITY DEFINER`
with `SET search_path=pg_catalog,pg_temp`. Install, revoke and rebind keys are exactly their `A` fields,
`evidenceSha256` and `requestSha256`.

| Function | Caller (EXECUTE) | Returns |
|---|---|---|
| `sessions.ingest_acquisition_install(p jsonb)` | `bulls96_acquisition_installer` only; guard `ingest_require_role` (M:20261104022000:220-233) | `{outcome:'installed'\|'replay',grantId,ownerEpoch,revision,state:'active'}` |
| `sessions.ingest_acquisition_revoke(p jsonb)` | same | `{outcome:'draining'\|'revoked_pending'\|'superseded'\|'replay',revision,ownerEpoch,state}` |
| `sessions.ingest_acquisition_rebind_origin(p jsonb)` (§4.7) | same | `{outcome:'rebound'\|'replay',revision,ownerEpoch}` |
| `sessions.ingest_acquisition_status(p jsonb)` keys `orgId,workspaceId,source,sessionIds`(≤500) | same | per session `{state,revision,tombstoneRevision,ownerEpoch,ownerKind,grantId}`; never a grant hash |
| `sessions.ingest_acquisition_enroll_begin/complete(p jsonb)` | no role; owner-run only under an explicit apply OK | enrollment state |
| `sessions.ingest_acquisition_lock(org,ws,pairs jsonb,exclusive boolean)`, `_admit(...)`, `_fenced(...)` | owner-only internals, called inside the existing SD entrypoints, like M:20261104052000:21 | void / snapshot / mismatched pairs |

**Install, in order:**
1. Check the role, validate the keys, require `install_enabled` (revoke skips this) and recompute the digest.
2. Same `operationUuid`, kind and digest → stored result; other digest → `operation_conflict`.
3. Require an `enrolled` enrollment row, read with `FOR SHARE NOWAIT`.
4. Take the exclusive acquisition try-lock.
5. Take `LP` exclusive, read the owner row `FOR UPDATE`, then repeat step 2 so a just-committed twin replays.
6. Check revisions: `operationUuid` in `ingest_acquisition_revoked_grants`, its `(ownerKind,
   bindingUuid|installationId)` in `ingest_acquisition_revoked_lineages` for this session (§4.9) or `p.revision <=
   tombstone_revision` → `revoked_before_install`; else `p.revision <= revision` → `stale_revision`;
   `expectedRevision` ≠ the current revision (0 if absent) or `revision` ≠ `expectedRevision + 1` →
   `revision_cas_lost`.
7. Check the predecessor (§4.3): `unknown` → `owner_unknown`; `active`/`draining` → `handover_not_ready`;
   `expectedOwnerEpoch` and `expectedPredecessorGrantId` must equal the current epoch and the latest grant (0 and
   null when absent), else `predecessor_mismatch`.
8. If `owner_epoch=0`, prove the session is fresh: no row for `(org,ws,source,session)` under any user or origin
   in `ingest_session_ownership`, `ingest_fence_versions`, `upload_deletions`, `ingest_record_revisions` or
   `ingest_publication_heads`, and no `session_index` row on its physical key. Otherwise `owner_unknown`.
9. Refuse another `active`/`draining` owner on the physical key (`physical_collision`, terminal).
10. Write the fence row (`active`, `owner_epoch+1`), the grant and the operation result in one transaction.

**Revoke** ignores the switch, and always names a concrete target: `targetGrantId`, `targetOwnerEpoch`,
`targetOwnerKind` and its `targetBindingUuid` or `targetInstallationId`. A missing target is `invalid_request`.
1. Run install steps 1-2 and 4-5.
2. If the grant row `targetGrantId` exists, its scope, epoch, kind and binding/installation must equal the target
   (else `42501 target_mismatch`). The current `active` grant needs `p.revision > revision` (else `stale_revision`);
   owner and grant become `draining` and revision = tombstone = `p.revision`. An already `draining`/`released` or
   superseded grant returns `superseded` unchanged.
3. Otherwise (revoke-before-install) insert the target into `ingest_acquisition_revoked_grants`, owner row untouched
   (`revoked_pending`); that `operationUuid`'s install later refuses `revoked_before_install`. Both paths also insert
   the target's lineage into `ingest_acquisition_revoked_lineages` (`ON CONFLICT DO NOTHING`, §4.9). A revoke never
   affects another binding, kind or epoch; a Cloud key for a native target, or vice versa, is `not_authorized`.

**Error contract.** The message is the stable code, prefixed `acquisition: `.

| SQLSTATE | Codes |
|---|---|
| `22023` | `invalid_request`, `request_digest_mismatch`, `acquisition_unexpected`, `session_required` |
| installer, before SQL | `not_authorized` (§4.1) |
| `42501` | `not_installer`, `not_owner`, `target_mismatch` |
| `55000` | `disabled`, `not_enrolled`, `owner_unknown`, `owner_draining`, `owner_released`, `revoked_before_install`, `handover_not_ready`, `grant_expired`, `physical_collision`, `owner_changed`, `drain_not_ready`, `enrollment_not_quiescent`, `owner_other_principal` |
| `40001` | `session_busy`, `revision_cas_lost`, `stale_revision`, `predecessor_mismatch`, `operation_conflict`, `origin_cas_lost` |

## 4. Open conditions from the migration-owner installer-boundary review (2026-11-07)

**4.1 A trusted service derives and authorizes the binding; neither role nor predecessor is proof.** The control-only
role remains a *candidate* least-privilege boundary until the concrete producer is chosen; no broad owner or global
custody capability is reused. Each install, revoke or rebind carries `S` over `A` (§3, §4.7) from an issuer that
derived the scope itself and decided per session. Roots of trust, each independent of the request: `S` verifies
against an issuer public key pinned in the installer's `issuerKeyId` registry (one per issuer class; overlapping keys
during rotation), never a key named only by the request; `G` comes from the issuer's CSPRNG and the database holds
only its hash; the installer database password lives in 1Password.
- Cloud (answered, Q1): the Factory binding service derives `org, ws, user, source, session_id, binding_uuid` and
  Cloud's origin from its committed, authorized binding and connection (pinned computer route, binding/control
  generation), never from a request-supplied tuple; that binding selecting this SID is the authorization.
- Native: the native issuer (Q2) derives `org, ws, user` by token introspection, plus the `installation_id` and
  origin it issued. In v1 it authorizes a SID only as a first claim of a fresh session (§3 step 8); a successor needs
  a release assertion from the predecessor's issuer naming this installation, which v1 does not ship
  (`handover_not_ready`). Per user, first installation claim wins. Owner = installation, not origin (§4.7).

**Order and codes.** `A` covers `expectedOwnerEpoch` and `expectedPredecessorGrantId`, binding it to one predecessor.
Authorization fails first: the installer verifies signature, scope and issuer class (Cloud key: `cloud` +
`bindingUuid` only; native key: `native` + `installationId` only), else `not_authorized` without calling SQL. SQL
then checks role (`42501 not_installer`), digest (`22023`), and only then the revision and predecessor CAS (§3 steps
6-7), which are ordering (`40001`/`55000`), never authorization. `evidenceSha256` and `issuerKeyId` are stored on the
grant. SQL does not verify Ed25519; a bare role call without verified evidence is out of contract.

**Precondition P1 (not an open question).** A native user plus caller-supplied `origin_id` is not installation proof.
No source may be enrolled, and no protected `droid` admission may ship, until the native issuer delivers a
per-installation `installation_id` and `G` only to that installation, Desktop persists both across a local reset and
presents `G` (§5.1). Its shape and owner are decided (Q2); it is not built, which blocks activation, not this
contract. Native admission then runs exactly the Cloud grant check; unenrolled sources keep current behaviour.

**4.2 Cross-database consistency is by committed intent, not a distributed transaction.** Before any remote call the
issuer commits an intent row: the operation, scope, revision and CAS expectations, `grant_sha256`, the encrypted `G`,
the signed `A`/`S`, `evidence_sha256`, `request_sha256` and `status` (`pending|applied|refused`). A reconciliation
loop, holding no local mutex across the network, sends `pending` intents per session in revision order, exactly and
unchanged; marks them `applied` on a result or `replay`; marks other refusals `refused` (`stale_revision`,
`revision_cas_lost`, `predecessor_mismatch`, `operation_conflict`, `origin_cas_lost` are terminal though `40001`, as
at M:20261104052000:88-95); retries only `session_busy` and transport failures (a lost response by exact replay); and
reads `status` to adopt the authority's revision and tombstone (a replayed install is not proof the grant is active
now). Unbind first commits a revoke intent naming the bound grant. A `refused` intent is never edited; a new one gets
a new `operationUuid`. Cloud and the native issuer use the same loop. It reconciles control intents only; publication
after a durable receipt is the pipeline's job (§7).

**4.3 An unrelated grant cannot seize a session.** §4.1 authorization prevents it: an unrelated installation holds no
assertion naming this SID and predecessor. Predecessor CAS (§3 step 7) only orders authorized requests.

**4.4 An empty owner table is not proof of absence.** `enroll_begin` sets `enrolling` under the §4.8 interlock;
admission and the pointer guard (§5.5) then treat the source as enrolled. `enroll_complete` runs only on a committed
`enrolling` row, so every writer is guarded or already committed (§4.8); it backfills an `unknown` row for every
`(source, session_id)` in the step-8 tables, `session_index` and the live heads/pointers of that physical source,
records `backfill_sha256` over the sorted keys and sets `enrolled`. For an enrolled source, admission treats no owner
row, `unknown`, `released` and `draining` as not-ready, never free; native upload resumes only through an authorized
install (§4.1).

**4.5 Lock graph (one order, used at every hook).** Authority rows precede USER, BATCH and INTENT
(M:20261104050000:56; M:20261104052000:53); the user fence is `ingest_lock_fences` (M:20261104030000:26-33). Order:
`L0` authority rows `FOR SHARE` → `L1` user fence → `L2` batch → `L3` stage intent → `L4` outbox → `L5` publication
pointer try-lock and head → `LE` (§4.8) → `LA` → `LP` → owner row. `LA` (a hook's leaf): enrollment row `FOR SHARE
NOWAIT`, then `pg_try_advisory_xact_lock_shared(hashtextextended(concat_ws(chr(31),
'96bulls:acquisition',org,ws,source,session),0))` per session in `(source, session_id) COLLATE "C"` order
(M:20261104030000:96). `LP`: the same try-lock over
`'96bulls:acquisition-physical',org,ws,physical_source,physical_session`, after `LA`. Hooks read owner rows under
`LA` and never lock them; install, revoke, rebind and drain take only `LA`, then `LP` exclusive, then the owner row
`FOR UPDATE`. `LA`/`LP` are never waited on (`40001 session_busy`), so no cycle exists. Reset and selection (`L0`,
`L1`) and buffered GC (`L1` per candidate, M:20261104055000:1557) take no `LA`, so privacy and cleanup are never
blocked. The pointer guard keeps its pointer → user `NOWAIT` order (M:20261104060000:417-424), taking `LP` shared
itself (a legacy caller holds no `LA`).

**4.6 Denial versus final refusal.** An admission denial raises inside the claim transaction before any durable
write; the `ON CONFLICT` insert in `ingest_lock_fences` (M:20261104030000:29) rolls back with it, so no ledger row is
written. A refusal at buffered commit, publication claim, publish or the pointer guard follows the stage or image PUT
and is **not** side-effect free: objects, stage intents and their expiry/GC obligations stay, but the head is not
advanced and no pointer is written. A commit refusal returns `55000 owner_changed` before any receipt (§5.6); a
publication-claim or guard refusal (publish `40001`, M:20261104060000:720) comes after the receipt and is §7's.

**4.7 Origin rotation, shared machines, missing session ids (Desktop review).**
- Desktop mints a new `origin_id` on every session reset, local-only included (privacy, never blocked);
  `installation_id` and `G` survive it (P1), so a native owner is its installation and grant. Admission requires the
  current `owner_origin_id`; publication and the guard accept any origin in `ingest_acquisition_grant_origins`, so an
  old origin's accepted obligations still publish; until rebound, the new origin's admission is `not_owner`.
- Re-association is `rebind_origin`, run as install steps 1-2 (no switch) and 4-5, signed by the native issuer only
  after the installation authenticates as the same user and proves `G`. `A` =
  `["relayhistory-acquisition-assertion-rebind-v1", operationUuid, orgId, workspaceId, source, sessionId, revision,
  targetGrantId, targetOwnerEpoch, ownerUserId, installationId, oldOriginId, newOriginId, issuerKeyId]`. Under
  `LA`/`LP` and the owner row it requires the current `active`/`draining` native grant with equal epoch, user and
  installation (else `42501 target_mismatch`), `oldOriginId` = current origin (else terminal `40001 origin_cas_lost`)
  and `p.revision > revision`; sets revision and origin, appends it, keeps epoch. No `G`/other user: no seizure.
- Shared machine: two users of one workspace sharing files contend per `(org, ws, source, session)`; the first
  authorized claim wins. The other user gets admission `55000 owner_other_principal` (owner undisclosed), install
  `handover_not_ready`, no v1 path. Same user, other installation: `42501 not_owner`. Workspaces never contend.
- Enrolled-source records without `session_id` get `22023 session_required`, never exempt; §5.6 lists them with
  `sessionId` `""` for Desktop to hold.

**4.8 Enrollment interlock.** Numeric XID ordering (`pg_current_xact_id`, `pg_snapshot_xmin`) is never used for
visibility: a lower XID can commit later, and a snapshot can predate the XID its transaction allocates later. The
barrier is the row lock on `ingest_acquisition_enrollment_gate` plus a fresh read:
- `LE` (writers): every hook that consults enrollment (`_admit`, the §5.2 recheck, publication claim and publish, and
  the pointer guard for every live write before its non-`srv_` early return) first runs `SELECT generation FROM
  sessions.ingest_acquisition_enrollment_gate FOR SHARE NOWAIT` as its own statement and only then reads
  `ingest_acquisition_enrollment` in a later statement, never from an earlier result. `55P03` → `40001 session_busy`
  in every hook, the guard included (an error, never a silent skip: the source may be unenrolled). Under READ
  COMMITTED the later statement of these VOLATILE functions takes a new snapshot after the lock, so it sees any
  committed enrollment. Under REPEATABLE READ or SERIALIZABLE, `FOR SHARE` on a gate row updated by a transaction
  that committed after the writer's snapshot raises `40001` (could not serialize: concurrent update), so a retained
  snapshot never passes with a stale enrollment view.
- `enroll_begin` (owner-run): `SET LOCAL lock_timeout` (expiry → `55000 enrollment_not_quiescent`), then `UPDATE
  ... SET generation=generation+1` as its first lock, which waits until every `LE` holder has committed or aborted,
  then inserts `enrolling` and commits. It holds nothing else, and writers never wait on `LE`, so no cycle exists.
- Schedule (a), writer starts during an uncommitted begin: its `LE` fails `NOWAIT` (busy, retried after the commit,
  then guarded); if it locked `LE` first, begin waits for its commit, so its pointer is committed before `enrolling`
  and `enroll_complete`'s backfill sees it. Schedule (b), pre-enrollment REPEATABLE READ snapshot with a later XID:
  if it locked `LE` before begin, as (a); otherwise begin committed after its snapshot and its `LE` raises `40001`,
  and the retry sees `enrolling`.
- Cost: one shared row lock per hooked write (a multixact under concurrency); unenrolled sources see only that lock
  and, during an `enroll_begin`, a retryable busy.

**4.9 Binding-lineage fence (option (i)).** A revoke fences its target's lineage, `(org, ws, source, session, owner
kind, bindingUuid or installationId)`, not only `targetGrantId`. A delayed, still-signed install I1 for binding B
(refused while `disabled`, re-issued as I2 under a new `operationUuid` per §4.2, then I2 revoked on unbind, applied
or pending) therefore refuses `revoked_before_install` whatever its `operationUuid` or revision. The per-binding
epoch is binary: a revoked lineage never installs on that session again. Cloud mints a new `bindingUuid` per bind, so
a later bind is a new lineage (Cloud requirement; a reused one fails closed); a revoked native installation needs a
successor install with handover (`handover_not_ready` in v1). The kind and lineage id are already signed fields of
`A`, so `A`, the vectors and §4.2's re-issue stay unchanged. The fence touches no owner row, grant or other lineage,
so an unrelated binding's current owner is unaffected. Option (ii), one install UUID per binding lifetime, was
rejected: it forbids §4.2's re-issue after a terminal refusal and needs a separate recovery protocol.

## 5. Admission and publication recheck (exact hooks)

Current bodies: M:20261104055000, redefining M:20261104050000; each `LA` follows the hook's existing locks (§4.5).
1. `ingest_claim_batch_mode` (M:20261104055000:1081-1313; first body M:20261104050000:85-130): existing `L1` (:1129)
   and batch locks, then `LA` over the epoch snapshot's sessions (:1302-1306), then `_admit` before the ownership
   insert (:1289) when `v_state IN('claimed','resume')`.
   - `_admit` applies the §4.4 not-ready rules, then one grant check for both owner kinds (P1). Each protected
     session needs exactly one entry `{source,sessionId,grantId,ownerEpoch,grant}`, sorted, no duplicates, strictly
     validated, never normalized: `grant` ~ `^rhg_[0-9a-f]{64}$`, `grantId` a lowercase canonical UUID, `ownerEpoch`
     a positive decimal, `source`/`sessionId` byte-equal to a batch session. It rebuilds the §3 grant preimage from
     the authenticated `k.org_id, k.workspace_id` and the entry and requires its hash to equal `grant_sha256`;
     `grantId`/`ownerEpoch` must be current, `expires_at` null or future, `k.user_id` the owner's (else
     `owner_other_principal`) and `k.origin_id` its current origin.
   - Any miss rejects the whole batch (`not_owner`, `grant_expired` or `owner_*`); nothing is dropped. The
     snapshot goes to `ingest_claim_attempts.acquisition` (:1308). An entry for an unenrolled source is
     `acquisition_unexpected`. Exact replays of committed receipts skip `_admit`. `ingest_claim_batch`
     (M:20261104050000:306) and `ingest_claim_buffered_batch` (055000:748) inherit this.
2. Accepted-stage recheck: `LA`, then `_fenced(...,'admit')` beside `ingest_attempt_fenced`, requiring `active` and
   the same epoch and grant, in `ingest_prepare_buffer_stage` (M:20261104055000:799 → :854),
   `ingest_commit_buffered_batch` (:894 → :914) and `ingest_commit_batch` (M:20261104060000:1017 → :1024).
3. `ingest_publication_claim` (M:20261104060000:179-305): `L1` (:196), batch and intent (:200-204), pointer try-lock
   (:259-260) and head, then `LA`; require snapshot epoch = current `owner_epoch`, state `active` or `draining`; set
   the head's `acquisition_epoch`; add `h.acquisition_epoch<>v_owner_epoch` to the rollover predicate (:278).
4. `ingest_publication_publish` (:626): directory try-lock (:653), `ingest_publication_locked` (:656; stage, attempt
   and pointer try-lock), then `LA` and the recheck; add `AND acquisition_epoch=(v->>'acquisitionEpoch')::bigint` to
   the head CAS (:712-716); the pointer CAS (:718-720) stays final.
5. `ingest_publication_guard_pointer` (:407-448). Before the non-`srv_` early return (:412-414), for every live write
   (not `DELETE`, `deleted_at IS NULL`) whose org, workspace and physical source equal an enrollment row's
   (`ingest_publication_component(source)`) in any state, take `LP` shared (busy → `RETURN NULL`), then require an
   `active`/`draining` owner on the physical key, an `srv_` machine (:245) and a head whose logical session,
   `acquisition_epoch` and user equal the owner's and whose origin is in the grant's origins (beside the final CAS,
   :440-444). Anything else (no owner, `unknown`, `released`, a legacy non-`srv_` writer, Q8) → `RETURN NULL`,
   fail-closed; publish then raises `40001` (:720) and nothing is written. Unenrolled sources keep today's guard;
   tombstones and retire paths (:426-433, :450-482) are untouched.
6. **Writer-facing refusal (Q4, answered by Desktop).** The edge (`src/session-ingest/deliver.ts:16-18`) returns
   every acquisition refusal of a batch (`owner_*`, `not_owner`, `owner_other_principal`, `grant_expired`,
   `session_required`, `owner_changed`) as HTTP 409
   `{error:{code:'acquisition_refused'},refused:[{source,sessionId,code}]}` naming each one. Never
   `delivery_conflict` (:172-176; quarantines, moves the cursor), 400/422 (bisection suppresses records),
   401/403/412/423 (stalls the install), 404/410, 429 or a partial 2xx; today's Desktop retries this 409 as
   transient. Desktop's follow-up holds the named sessions ("waiting for ownership"), re-posts the rest under a new
   `batch_id` and resumes on owner change; `grant_expired` triggers renewal. `GET /v1/delivery/limits` (`app.ts:405`)
   advertises enrolled sources so Desktop holds their records until it has a grant. **P2:** this 409, the limits
   signal and Desktop's `droid` source with install support ship atomically with `droid` enrollment; earlier
   `enroll_begin` is out of contract.

## 6. Principals and roles

- **Control role `bulls96_acquisition_installer`.** `NOLOGIN NOINHERIT` in
  `supabase/bootstrap/session_ingest_roles.sql` (pattern `:42`) and its RDS/Cloud SQL variants; LOGIN via a new
  `acquisition_installer_role.sql` (pattern `privacy_eraser_role.sql:1-15`) plus `acquisition_installer_verify.sql`.
  Grants: `USAGE` on `sessions` and `EXECUTE` on install, revoke, rebind and status only, no table right;
  `bulls96_session_ingest` gets no `EXECUTE` on them (unlike privacy begin/complete, M:20261105041000:552). Its
  principal is a separate service identity holding the pinned issuer public keys, never the ingest signing key or
  `rth:sync`; its database password lives in 1Password, never on disk.
- **Grant presentation.** Both owner kinds present the `acquisition` array in the claim payload inside the existing
  signed body; the edge passes it through with no authority. Cloud uses its own publish path (Q3); native uses the
  Desktop batch body of an enrolled source (P1), never `sessions/selection`. `G` is never logged.
- **Native issuer (Q2).** A History-owned exchange route beside the ingest edge; callers authenticate through the
  deployment's control plane; one principal model; the Cloud MCP child credential is never upload authority or SID
  proof.

## 7. Drain receipt producer (not in the first migration)

Two obligation classes for `(org, ws, source, session)` under epoch E:
- **Accepted** (acknowledged to the writer): a committed batch whose attempt snapshot names the session, an
  acknowledged live stage intent with a `buffered` outbox (M:20261104050000:20-50), an unpublished receipt seat
  (M:20261104053000:43-60) or a head with a live `build_lease_until`. Only these are readiness evidence; they are
  published or reconciled, never deleted or expired to make a drain look empty.
- **Unacknowledged**: a `pending` claim or a prepared, unacknowledged stage; not readiness evidence, never accepted
  under `draining` (§5.2 requires `active`), cleaned up by existing GC.

**Post-receipt publication is a server obligation.** Desktop compacts acknowledged evidence and never re-sends it, so
the pipeline publishes or reconciles every accepted obligation itself (retry under its admitted epoch, which
`draining` allows; alert if it cannot); a later revoke or rebind never strands it and nothing waits on a writer.
`draining → released` may be applied only by a producer that takes `LA` exclusive and proves from committed ledger
state, never from the owner table, that no accepted obligation remains, recording the evidence digest. Until it ships
with tests on real accepted stages, handover and drain return `handover_not_ready`/`drain_not_ready`, `unknown`
sessions stay not-ready, and only fresh sessions (§3 step 8) can be installed.

## 8. Open questions and implementation plan

Open questions (reviews of 2026-11-07). Answered, now constraints:
1. (Cloud) Issuer: the Factory binding service (§4.1) with a dedicated server-managed Ed25519 key and pinned
   `issuerKeyId` registry (not built); never the user's Factory key, ingest HMAC, audit purpose or generic history
   assertion key. Rotation keeps overlapping keys until old intents reconcile; `A`/`S` are never re-signed.
2. (History owner lane, answered: shape + owner; unbuilt) The native issuer is a History-owned exchange route, a
   sibling of the session-ingest edge (#316), owned by the layered-sessions-pipeline lane (History side), per the
   self-host direction (relaycontrol is the control plane, relayauth removed). It mints `installation_id`, `G` and
   native install/revoke/rebind assertions only after authenticating the caller through the deployment's control
   plane: relaycontrol token introspection on self-host (#313); on the hosted path, the site's existing verified
   Cloud assertion (deprecated with the cloud repo; must not grow). One principal model scoped to
   site/user/workspace; no second customer key. The Cloud MCP child credential stays read/MCP only, never upload
   authority or SID proof. Gateway contract: specified alongside #316. Not built: blocks activation (P1), not this
   contract.
4. (Desktop) Refusal signalling: the typed 409 of §5.6, shipped atomically with enrollment (P2).
5. (Cloud) `expiresAt` null = non-expiring in v1; a TTL needs a reviewed renewal CAS Desktop can see and trigger
   (else a permanent 409 loop). Expiry never releases ownership.
6. (Cloud, Desktop) Enrollment is exactly `(org, ws, droid)`, dormant, never on API-key save or session bind, after
   P1, P2, protected admission on every writer path, the backfill and tested rollback/hold. Desktop has no `droid`
   source on main, so enrolling refuses nothing it sends today; per-org scope is not needed.
7. (Cloud) Permanent `draining`/`unknown` before §7 suits only a dormant source and a bounded fresh-session canary.
8. (Desktop, Cloud) Desktop writes no pointers and its batches use `srv_` machine ids (`writer.ts:147-149`), so §5.5
   does not refuse them; Cloud has no active legacy Factory publisher. Confirm no other non-`srv_` `droid` writer
   before enabling the guard.

Still open:
3. (Cloud, partly) A separately authenticated Cloud publisher port at the History gateway over the private Ingest
   binding; proof never in provider payloads, logs, traces or archives; attempts pin grant and epoch; a retry never
   injects a new grant into an immutable outbox. Open: exact route and credential.

Implementation plan. One PR, dormant:
1. Migration and rollback `<SLOT>_ingest_acquisition_owner.sql` (§3 DDL, functions, hooks, REVOKEs); bootstrap
   role files; function-boundary lane entries and verifiers (`scripts/sessions-function-boundary/`).
2. Tests on disposable Postgres: every error code; revoke-before-install; exact replay and operation conflict;
   predecessor seizure; `target_mismatch` for a delayed revoke of another binding, kind or epoch; enrolled no-row and
   `unknown` refusals; zero-write denial; post-PUT refusal preserving objects; `enrollment_not_quiescent`;
   second-user and missing-SID refusals; origin rotation keeping accepted obligations publishable; the edge 409
   shape; both §3 vectors in Cloud and SQL tests; and real two-connection, both-orders tests for admission↔install,
   publication claim↔revoke, publish↔guard, legacy pointer insert/update↔install/revoke, two installs on one physical
   key (`physical_collision`), buffered stage/commit↔revoke, reset↔admission, GC↔publication and rebind↔admission.
   - `binding_lineage_delayed_install_after_revoke` (§4.9): I1 for binding B refused `disabled`; switch on; I2 (new
     `operationUuid`, same B) installs, or is revoked before install; revoke I2; replaying I1's exact `A`/`S` →
     `revoked_before_install`; an install for binding C still succeeds.
   - Mandatory two-connection enrollment tests (§4.8). `enrollment_interlock_writer_during_uncommitted_begin`: C1
     runs `enroll_begin` uncommitted; C2 (READ COMMITTED) legacy pointer insert → busy, nothing written; reversed: C2
     inserts uncommitted, C1 `enroll_begin` blocks, C2 commits, C1 commits, `enroll_complete` backfills C2's session
     as `unknown`, C2's next write is refused.
   - `enrollment_interlock_retained_repeatable_read_snapshot`: C2 `BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1`
     (snapshot, no XID); C1 `enroll_begin` commits; C2 pointer insert → `40001`, nothing written; C2's retry sees
     `enrolling` and is refused. Both: no unguarded write commits after `enrolling`; the backfill covers every
     earlier one.
3. Indexes the step-8 scans need; `CONCURRENTLY` builds are an operator step with no slot.

The migration registry owner assigns the slot after the PR exists, in F-lane order. Applying it, enrolling any
source and enabling the switch each need Khaliq's separate OK.
