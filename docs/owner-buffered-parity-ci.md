# Owner-buffered parity CI workflow (exact seven, dormant write)

`.github/workflows/owner-buffered-parity.yml` runs the owner-buffered parity census (G1), the G2 write of
`sessions.owner_buffered_parity` and a read-only readback for **exactly seven** tenants, from CI only. It adds
no parity SQL: it drives the existing operator code (`scripts/owner-buffered-parity-operator.ts`,
`src/migration/owner-buffered-parity.ts`) through the single-tenant path, one tenant at a time.

**Every dispatch, in any mode, needs Khaliq's own word.** Merging this workflow authorizes nothing.
**Enabling write is a separate reviewed PR** that flips one line
(`OWNER_PARITY_WRITE_ENABLED` in `src/migration/owner-buffered-parity-ci-contract.ts`) and nothing else.

## State at merge

- Write is dormant: `OWNER_PARITY_WRITE_ENABLED = false`. A write dispatch refuses `owner_parity_write_dormant`
  in the gate job and again in the database step **before any credential is read**.
- The roster `src/migration/owner-buffered-parity-roster.json` ships **empty**, so every dispatch (census,
  write, readback) refuses `roster_empty`. Adding the seven digests is a separate follow-up PR.
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
| `mode` | `census` (default), `write`, `readback` | Anything else refuses `owner_parity_inputs`. |
| `sha` | 40 lowercase hex | Must equal the live `main` head and `github.sha`. |
| `census_run` | run id | Write only; empty otherwise. |
| `confirmation` | text | Write only; empty otherwise. |
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
- `src/migration/__tests__/owner-buffered-parity-ci.test.ts` uses mocked sessions. It covers:
  - fail-closed partial write
  - readback mismatch
  - generation and source drift between census and write
  - bulk
  - dormant before any statement
  - the stdin-only credential hand-off
  - a parent process run checking that nothing secret is printed
- `src/migration/__tests__/owner-buffered-parity-gate.test.ts` uses mocked GitHub. It covers every gate
  refusal, the contract validators and the workflow shape.
- `src/migration/__tests__/owner-buffered-parity-ci.pglite.test.ts` runs the proof.
- `bun scripts/aws-hardening-mutations.ts --only "owner parity"` checks that reverting each control turns a
  named test red.
