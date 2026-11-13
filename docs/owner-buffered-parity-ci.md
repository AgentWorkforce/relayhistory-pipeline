# Owner-buffered parity CI workflow (exact seven, dormant write, one-time discovery)

`.github/workflows/owner-buffered-parity.yml` runs the owner-buffered parity census (G1), the G2 write of
`sessions.owner_buffered_parity` and a read-only readback for **exactly seven** tenants, from CI only. It adds
no parity SQL: it drives the existing operator code (`scripts/owner-buffered-parity-operator.ts`,
`src/migration/owner-buffered-parity.ts`) through the single-tenant path, one tenant at a time. A one-time,
read-only `discovery` mode finds which seven buffered owners lack a canonical current parity row, so the
roster can be written (see [Discovery](#discovery-read-only-one-time)).

**Every dispatch, in any mode, needs Khaliq's own word.** Merging this workflow authorizes nothing.
**Enabling write is a separate reviewed PR** that flips one line
(`OWNER_PARITY_WRITE_ENABLED` in `src/migration/owner-buffered-parity-ci-contract.ts`) and nothing else.

## State at merge

- Write is dormant: `OWNER_PARITY_WRITE_ENABLED = false`. A write dispatch refuses `owner_parity_write_dormant`
  in the gate job and again in the database step **before any credential is read**.
- The roster `src/migration/owner-buffered-parity-roster.json` ships **empty**, so every census, write and
  readback dispatch refuses `roster_empty`. Adding the seven digests is a separate follow-up PR. `discovery`
  is the only mode that runs on the empty roster, and it refuses `roster_present` once the roster is filled.
- Census and readback stay callable once the roster is populated.

## Roster

The roster holds only digests: `tenantSha256` from the operator (sha256 over a fixed prefix and the tenant
tuple). Tenant identifiers never appear in the repository, a workflow input, a log line or an artifact.

The database step enumerates buffered tenants from the target ledger with the operator's own bulk cursor SQL
(`BULK_BUFFERED_TENANTS_SQL`, one read-only repeatable-read snapshot, bounded `FETCH` pages), hashes each row
and keeps only roster digests. Unlisted tenants are hashed and dropped; nothing else is done with them.
Unless all seven are found the run refuses `roster_incomplete`. The operator's `bulk` mode is not a mode of
this workflow and cannot be reached (`owner_parity_inputs`).

## Inputs

| Input | Values | Notes |
| --- | --- | --- |
| `mode` | `census` (default), `write`, `readback`, `discovery` | Anything else refuses `owner_parity_inputs`. |
| `sha` | 40 lowercase hex | Must equal the live `main` head and `github.sha`. |
| `census_run` | run id | Write only; empty otherwise (discovery included). |
| `confirmation` | text | Write only; empty otherwise (discovery included). |
| `census_source` | `frozen-supabase` (default), `neon` | See below. |

The generation is never an input. Census discovers the live PASS marker generation; write re-reads it.

### Census source

- `frozen-supabase` reads the sealed frozen copy inside the target project over a fenced read-only owner
  session, using the same `SUPABASE_MIGRATION_OWNER_URL`.
- `neon` **refuses `credentials_missing_key.neonUrl`**. No Neon secret exists for this workflow, and this
  change adds none. Creating one would need its own approval.

## Gates

The `prerequisite-gate` job (no environment) runs `scripts/owner-buffered-parity-github.ts`. The database job
runs on the existing `session-schema-production` environment, named through the gate's output, and repeats
every check with `--approved` before the secret step. The gate refuses unless:

1. the dormant switch allows the mode;
2. the repository id is `1437714333` and the ref is `refs/heads/main`;
3. `sha` equals `github.sha` and the live `main` head;
4. this is run attempt 1;
5. the actor and triggering actor are both the pinned dispatcher from `src/hoopdb/schema-release-contract.ts`;
6. the environment is protected and limited to `main`;
7. the push runs at `sha` are green: CI `checks`, and pg-proof `real PostgreSQL 15`, `16` and `17`;
8. for write, the census run:
   - is a successful first-attempt dispatch of this workflow at the same `sha`;
   - was started by the pinned dispatcher;
   - is less than 24h old;
   - holds exactly one unexpired `owner-buffered-parity-census` artifact.

Discovery passes exactly the census gates (1 to 7) and reads no census run.

Write also checks the confirmation's shape in the gate. The database step then checks it byte for byte.

Dispatches share the concurrency group `guarded-session-schema-production` with the other guarded operators.
Proof runs use a per-sha group.

## Modes

### `census` (read-only)

For each roster tenant, in roster order, the database step runs G1 (`readParityCensus`). It reads source
snapshot A, then the target, then source snapshot B, all on fenced read-only sessions.

When all seven are equal at the live generation, it uploads the census artifact `owner-buffered-parity-census`
(retention 1 day). The artifact holds:

- `sourceSha`, the live `generation`, `censusSource`, `capturedAt` and `censusRun`;
- a per-tenant `equal`/`mismatch` entry with the operator's public receipt fields (digests and public proof
  fields only);
- `descriptorSha256` over all of the above.

Any mismatch exits 3 and uploads no census artifact.

### `write` (dormant)

The workflow downloads the census artifact and checks it before any credential is read:

- the descriptor recomputes;
- `sha`, `census_run` and `census_source` match the dispatch;
- `capturedAt` is less than 24h old;
- the digests are exactly the roster, in roster order, and all are equal;
- `confirmation` is exactly:

```
WRITE-OWNER-PARITY <sha> GENERATION <generation> SOURCE <censusSource> DESCRIPTOR <descriptorSha256> CENSUS <census_run>
```

Then the live generation must still equal the census generation (`owner_parity_generation_mismatch`).

Per tenant, in roster order:

1. A fresh G1 must be equal, at that generation, and byte-equal to its census entry
   (`owner_parity_census_drift`).
2. `writeCurrentParityProof` writes the row.
3. An exact readback of the stored row must equal the fresh proof (`owner_parity_readback_mismatch`).

The first refusal stops the loop. Rows already written stay and are reported per digest. Fewer than seven
verified rows fails the run.

### `readback` (read-only)

Reports `equal`, `stale` or `missing` per digest. It exits 3 unless all seven are equal.

### `discovery` (read-only, one-time)

Discovery exists only to find the seven digests for the roster. The production SQL actor asked for it.

**Every discovery dispatch is Khaliq's own word to the production actor.** Merging the mode authorizes no
run. Its receipt authorizes nothing either: the roster is a **separate reviewed roster-only PR** written from
the discovery receipt, and census, write and readback then run against that roster as before.

It uses the same gates, environment and single secret as the other modes, and adds no secret, variable or
environment. It never writes:

- the target session is fenced read-only (`OWNER_PARITY_MODE_POLICY`: only `write` has a writable session);
- it never calls `writeCurrentParityProof`;
- it adds no query SQL (only the transaction, cursor and timeout statements around the existing cursor
  query).

The database work, in order:

1. Read the live PASS marker generation on the target (`BULK_TARGET_GENERATION_SQL`).
2. Open one `REPEATABLE READ READ ONLY` transaction on the enumerator session. Its first read is the
   generation again, which must equal step 1. Then the operator's bounded buffered-tenant cursor
   (`BULK_BUFFERED_TENANTS_SQL`, `FETCH` 100) lists every buffered owner in that snapshot.
3. Classify each owner with the canonical readback comparison: a fresh G1 (`readParityCensus`) and the stored
   row (`READ_OWNER_BUFFERED_PARITY_SQL`, `storedProof`):
   - no stored row is `missing`;
   - a stored row byte-equal to an equal fresh proof at the generation is canonical current;
   - anything else is `stale`.
4. Read the generation on the target again. It must still equal step 1.

Refusals specific to discovery:

| Code | When |
| --- | --- |
| `roster_present` | The committed roster lists any digest. A malformed roster still refuses `roster_invalid`. |
| `generation_unstable` | The generation inside the snapshot, after the comparison, or in a fresh equal proof differs from the one read first. |
| `unexpected_scope` | missing + stale is not exactly 7, or any cursor row is not a valid tenant tuple. Refused as soon as more than 7 are seen (counts so far only). |
| `owner_parity_mode_route` | Internal guard: discovery reached the census/write/readback runner, or another mode reached the discovery runner. |

Every other refusal stays (`owner_parity_inputs`, `owner_parity_census_run`, `owner_parity_confirmation`,
the gate codes, the credential codes). A census, write or readback dispatch still refuses `roster_empty`.

#### Discovery receipt

The receipt `owner-buffered-parity-discovery/v1` is uploaded as the artifact `owner-buffered-parity-discovery`
(retention 30 days) and published to the job summary. The summary step has no credential and re-validates the
file strictly before publishing it. The receipt holds:

- `status` and, on refusal, a fixed `code`;
- `runId`, `sourceSha`, `generation`, `censusSource` and `capturedAt`;
- counts: `scanned`, `canonicalCurrent`, `missing`, `stale` and `unparsable`;
- on success only, `owners`: exactly seven `{tenantSha256, disposition}` entries, sorted by digest, where the
  disposition is `missing` or `stale`;
- `credentialsPersisted: false`.

**A refused run carries counts only, never digests.** No receipt, summary or log line carries a tenant id,
workspace id, slug, email, row, SQL text or credential. The generic `owner-buffered-parity` receipt of a
discovery run carries the counts only (`found` = scanned, `equal` = canonical current) and no digests.

## Credentials

Only the one database step receives a secret: the existing `SUPABASE_MIGRATION_OWNER_URL`. No new secrets,
variables or environments are added.

The script works in two processes:

1. The parent checks everything that needs no credential: mode, dormant switch, roster, census, confirmation
   and checkout.
2. It removes the URL from its own environment.
3. It spawns the credential child with fixed argv and an allowlisted environment, with no secret and no token.
4. It writes `{"supabaseUrl": ...}` to the child's stdin pipe.
5. The child refuses any credential in its environment and reads the URL only through `readStdinCredentials`.

The URL never appears in argv, in the child's environment, in a log line or in a file. Every session is
fenced: pinned TLS, the parity-table owner, and read-only except for the write session.

## Receipt

The sanitized receipt `owner-buffered-parity` is uploaded on every run with retention 30 days. It holds:

- status, a fixed refusal code, mode, run id, `sourceSha`, `generation` and `censusSource`;
- counts;
- per-digest status;
- `credentialsPersisted: false`.

It holds no URLs, hosts, tenant identifiers or row contents. Stdout carries one aggregate line. A test asserts
that neither the receipt nor stdout contains a key named `orgId`, `workspaceId`, `userId`, `neonUrl` or
`supabaseUrl`.

## Proof and tests (no credentials)

Pull requests and pushes that touch these files run only the proof job. It has no secrets and no environment.

- `bun scripts/owner-buffered-parity-ci.ts --proof` builds PGlite over every migration with nine buffered
  tenants, seven of them on the roster, and a PASS marker. It proves census, write (enabled only by test
  injection) and readback, and that tenants 8 and 9 are never touched. It also proves these refusals:
  - dormant
  - `roster_empty`
  - `roster_incomplete`
  - wrong confirmation
  - bulk unreachable
  - stale census
  - sha mismatch
  - substituted digest
  - generation mismatch
  - discovery: `roster_present`, `unexpected_scope` (counts only), and on a fresh image with read-only
    sessions exactly seven missing/stale owners (six missing, one stale, two canonical current) with no write
    statement and the parity table unchanged
- `src/migration/__tests__/owner-buffered-parity-ci.test.ts` uses mocked sessions. It covers:
  - fail-closed partial write
  - readback mismatch
  - generation and source drift between census and write
  - bulk
  - dormant before any statement
  - the stdin-only credential hand-off
  - a parent process run checking that nothing secret is printed
- `src/migration/__tests__/owner-buffered-parity-discovery.test.ts` uses mocked sessions. It covers the
  exactly-seven scope (`unexpected_scope` with counts only), generation drift before, inside and after the
  snapshot (`generation_unstable`), `roster_present`, the mode dispatch table (write and bulk unreachable from
  discovery), read-only statement shape, receipt validation, sanitization against identifier fixtures, and a
  parent process run plus the summary step.
- `src/migration/__tests__/owner-buffered-parity-gate.test.ts` uses mocked GitHub. It covers every gate
  refusal, the contract validators and the workflow shape.
- `src/migration/__tests__/owner-buffered-parity-ci.pglite.test.ts` runs the proof.
- `bun scripts/aws-hardening-mutations.ts --only "owner parity"` checks that reverting each control turns a
  named test red.
