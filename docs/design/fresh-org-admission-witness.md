# Fresh-org admission witness for 062240

**Disposition: design review only.** The proposed exemption applies to the first admission of an org whose sole prior authority-table hit is `sessions.ingest_seed_source_epochs`. An epoch can be created by an index write. A current live index row can also follow a deletion and recreation. Neither establishes that accepting an old buffered upload respects the org’s deletion history.

## Creation and coverage contract

1. The authoritative creator commits an immutable fresh-creation event alongside the org record, using a transactional outbox or an equivalent replayable transaction record. Its contract binds issuer, source instance, creation versus import/restore, immutable org identity and incarnation, event ID and source position. A delayed event can be used only with positive proof that recording covered that incarnation from creation. Wall-clock order between systems is insufficient.
2. A trusted verifier authenticates the issuer and binds the event to a Sessions recording generation and an independently durable restore watermark. The owner must specify trust provisioning, revocation, source ordering, log retention and reconciliation. Duplicate delivery of identical evidence is idempotent. A conflicting payload for the same event or incarnation holds enrollment. Neither an ordinary ingest caller nor a client assertion may issue a witness.
3. Before declaring coverage, the verifier accounts for the relevant delete, consent, reset, fence, ownership and privacy sources from creation through a known cut. A source which can retain an upload from before a local deletion needs enforceable stale-upload rejection or evidence covering that route. If such evidence cannot exist, those clients stay ineligible. Empty tables, existing membership and retained object-version counts alone cannot certify coverage.
4. Sessions records an org as unknown until both the creation and privacy contracts are satisfied. A deletion, revoke, resurrection, source re-key or recording gap invalidates eligibility. A restore or recording generation change invalidates old coverage even if the database itself has rolled back. A verifier must fence replayed events across that boundary. Re-enrollment requires reviewed evidence.
5. At first admission, serialize on the org and its coverage record, recheck generation, current policy and privacy baseline, then execute the complete existing prior-authority scan. Only the seed-epoch hit may be exempted. Any other hit retains the current refusal. Create the admission and its audit row atomically. Keep the caller’s existing per-user checks, and record the witness identity and policy generation for subsequent review.

## Who must supply evidence

| Owner | Review deliverable |
|---|---|
| Cloud directory owner | Identify the actual creator, authenticated creation transaction, import/restore/recreate routes, and whether an incarnation can be reliably distinguished. Supply source pins and list uncovered routes. |
| Layered Control | Supply the matching relay-control implementation or contract for every Cloud change. Establish source ownership during cutover, atomic outbox semantics, replay horizon, restore watermark and any bypass writers. |
| Sessions pipeline and privacy/fence owners | Map deletion routes, including bare deletes, hard/soft delete and recreation, move/re-key, bulk copy and restore; establish lock order against ingest, erase and projection workers; qualify the audited exemption. |

A matching design across Cloud and Control does not establish which backend handled a particular creation. Source routing and freshness must be evidenced before activation. Issuance and a complete privacy baseline remain open in this review.

## Disposable qualification

| Fixture | Required result |
|---|---|
| Fresh attested org, uninterrupted coverage, seed-only | Exactly one admission and audit record with the current privacy baseline. |
| Missing/forged/wrong-issuer attestation, old generation, import, restored org, incomplete source history | Refusal before admission or audit. |
| Seed plus any other authority-table hit, or stale per-user authority | Existing refusal and transaction semantics preserved. |
| Delete and recreate with the same visible key; pre-trigger or client-local deletion | A live row cannot clear the privacy hold. Old buffered uploads cannot reappear. |
| Attestation delivery duplicated, delayed, reordered or conflicting | Identical replay is harmless; gaps or conflicts cannot advance coverage. |
| First claim and deletion in both lock orders; concurrent first claims; recording rotation during claim | Only valid serial outcomes. No resurrection, double admission or orphan audit; measure waits and deadlocks. |
| Downgrade or operator pause following admissions | New exemptions stop. Preserve prior admissions and audit. Refuse destructive rollback until disposition is reviewed. |

Run these cases on the integrated server bytes under the intended roles on PG15/16/17. Rehearse replica-mode ingestion, crash recovery and restore independently. Measure baseline versus enabled claim latency, contention and extra writes on the same disposable workload. Record the exact target generation, migration checksums, rollback and both reviews before proposing an apply.

## Existing evidence and limits

The parked 062240 candidate checks for live source rows and scans all other authority tables. Its PGlite suite exercised 38 cases (209 assertions). That suite does not consume a creator-issued witness. The older candidate and its race fixtures are retained separately. The 194-org operator repair supplies evidence for those individual repairs only. This branch adds no migration, identity grant or activation path.

An independent disposable PostgreSQL 16 run of the parked race suite passed three of five cases. Bare source-row deletion serialized in both lock orders, and duplicate first claims produced one admission and one audit row. Both cases using the self-host erase entry point failed their assertions. The integrated witness must pass those two cases through the actual erase entry point as well as the matrix above. PostgreSQL 15 and 17 results for this design are pending.

## Directory owner source review

The directory owner reviewed cached Cloud `e6a51900f6b5` and relay-control `c8fca01d77a3` (receipt `251254668272951296`). These pins describe source behavior. Effective deployment and routing remain unverified.

| Creation path | Candidate transaction boundary |
|---|---|
| Cloud additional organization | The browser session reaches `app/api/v1/organizations/route.ts:31` and `lib/auth/store.ts:399`. The org, owner membership and default workspace commit together. |
| Cloud signup | Verified OAuth reaches `lib/auth/oauth-sign-in.ts:144` and `lib/auth/store.ts:312`. User, identity and directory rows commit together. |
| Control self-serve or personal org | Account-session route `src/identity/routes.ts:1156` calls the directory creation paths in `src/identity/store.ts:305` and `:939`. Those transactions include a creation audit. |
| Control operator or bootstrap | Bootstrap bearer route `src/app.ts:2707` reaches `src/store.ts:319`. That path creates the org and owner without a creation audit. Startup bootstrap audits separately. |

Cloud emits telemetry after commit without awaiting delivery. Control imports preserve source UUIDs and creation timestamps (`scripts/cloud-import/mapping.ts:308`). Both creation audits and the import ledger (`0063_cloud_import_ledger.sql`) cascade when the org is deleted. A missing audit or import row therefore leaves prior existence unknown. The owner found no persistent incarnation record or recreation tombstone.

Layered Control owns the issuer contract. Cloud creation hooks need matching relay-control coverage until their retirement is verified. Signup, self-serve, reviewer, legacy self-host, bootstrap/operator, import, restore and manual writer paths each need either qualified coverage or explicit exclusion from automatic admission. Add a disposable case that imports an org, deletes it with cascading audit and ledger removal, and recreates its UUID. First admission must still refuse the old incarnation. Also pause outbox delivery between directory commit and first upload and verify that ingest refuses the exemption until it has accepted both the witness and the privacy baseline.

## Control owner source review

Control receipt `251254483694092288` covers relay-control `42bff2d94d0a9de4d40a1b97ccedef5bd4ad4f98` and Cloud `e6a51900f6b550c1c30926d5fda677d5ad0142b8`. The witness identifies the org incarnation at creation, before its first seed or batch. Delayed delivery must preserve that ordering through replayable evidence.

Cloud signup and additional-org creation share `packages/web/lib/auth/store.ts:272` (`insertOrganizationWithDefaultWorkspace`). Self-host identity and reviewer login have separate insertions in `selfhost/identity.ts:230` and `auth/reviewer-login/store.ts:95`. Control identified native insertions in `src/store.ts:347` and `src/identity/store.ts:342,542,939`. Its importer upserts source IDs at `scripts/cloud-import/importer.ts:218,633`. The owner found no incarnation issuer, replayable enrollment outbox or restore barrier in those paths. Imported IDs and timestamps can predate the observing store, as can rows recovered from a snapshot. Each route requires explicit qualification or exclusion. The 194 repaired admissions establish no baseline for later orgs.

### Deletion evidence and its scope

In Control, `src/app.ts` and `src/session-erasure.ts:319` persist one request ID per (org, workspace, source, session). Completion requires a validated upstream fence. Cloud's workspace session route and `lib/sessions/remove-upload.ts` remove a user's upload across proven origins through History's selection route, retry from existing fences, and refuse when `remove_upload` is gated. Receipt `251254490673414144` documents these source findings.

Both Control's `src/history-upload/credentials.ts` and its Cloud vendored counterpart read a journal sequence per (org, workspace, user). An absent journal yields zero. The verifier must distinguish that default from evidence that recording continuously covered every relevant source from creation. A successful removal receipt covers its targets only. Add negative fixtures in which one user's fence completes while another user's history is unknown, a journal is absent, and a local-only deletion predates upload. Each must keep automatic enrollment held even if the directory currently contains a live row.
