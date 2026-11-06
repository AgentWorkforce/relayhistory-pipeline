# Canonical publication canary

Status: source and offline proof only. This runbook does not authorize a
deployment, migration apply, credential or IAM change, publisher/GC activation,
frontier write, or read-path switch.

## Safety contract

The canary admits one exact organization/workspace and a complete plan of at
most eight stage tokens. The plan fixes its owner, expiry (at most 24 hours),
schema generation, object target, stage hashes and byte counts, and record,
session, time, request and byte budgets. `publication-canary.ts` inspects the
entire plan before its first claim. An out-of-scope stage must be returned as an
explicit `outside_canary_tenant` hold; an authority that calls it eligible is
refused, vetoes every claim in that plan, and is never claimed.

Every in-scope claim must atomically recheck the inspected evidence and bind the
exact `(owner_id, plan_id, plan_digest, stage_token, evidence_digest)` tuple
before returning the ordinary trusted publication ticket. The wrapper runs the
existing publisher and its source/head/pointer CAS. It reuses the normal RPC
ticket parser, then independently checks tenant, stage, generation, stage key,
head prefixes and plan counts. The object-store view can read only the exact
stage and attested prior-session namespaces, write only the attested workspace
paths in ticket-authorized generations, and cannot list, delete or presign. Any
limit or trust mismatch is a refusal, never a truncated success.

Outcomes are appended to a SHA-256-chained, fsync-backed metadata ledger. The
ledger is single-writer, rejects symlinks, torn tails, changed plans and changed
outcomes, and leaves a stale lock as an operator-visible hold. Production must
mount the ledger on durable storage; an ephemeral task filesystem is not an
accepted source of completion evidence. Restarting the same plan resumes only
its exact durable outcomes.

## Missing production authority

Do not build or run a production entry until the frontier owner supplies a
reviewed authority implementation of `PublicationCanaryAuthority`:

1. `inspect` is read-only and returns the trusted stage tenant/hash/bytes,
   eligibility or explicit hold, and a digest over the evidence used.
2. `claim` locks and rechecks the same evidence, proves the unexpired exact
   owner/plan binding, persists that binding, and claims the stage in the same
   transaction.
3. Ordinary publisher claims cannot bypass the binding for canary-owned work.
4. The binding and held cross-scope inventory are queryable as metadata-only
   evidence.

The proof script creates an isolated temporary binding table only inside its
throwaway database. That table is not a migration or a proposed production
fallback.

The deployable entry also needs the shared runtime factory being coordinated in
#271. Export the existing runtime construction as a narrow factory and have the
canary entry use it unchanged. It must retain the existing managed secret,
pinned database host, schema-generation check, asset digest checks, dedicated
role and object-store configuration. Do not add owner credentials, direct SQL,
an injected test store, or an alternate parser to the real entry. Until both
dependencies exist, the source is intentionally a library plus an offline
entry, not a runnable production binary.

## History adapter

`canonical-history-adapter.ts` is a default-off History-side client for the
existing `/v1/canonical/sessions/:id` replay endpoint. It never calls or falls
back to `/v1/events`; the public events route remains ClickHouse-backed. Enable
only for an exact org/workspace and an explicit list of at most 32
source/session pairs:

```text
HISTORY_CANONICAL_CANARY_ENABLED=true
HISTORY_CANONICAL_CANARY_ORIGIN=https://<canonical-reader-origin>
HISTORY_CANONICAL_CANARY_ORG_ID=<exact-org>
HISTORY_CANONICAL_CANARY_WORKSPACE_ID=<exact-workspace>
HISTORY_CANONICAL_CANARY_SCOPES_JSON=[{"source":"claude","sessionId":"<exact-session>"}]
HISTORY_CANONICAL_CANARY_DEADLINE_MS=7500
HISTORY_CANONICAL_CANARY_MAX_RESPONSE_BYTES=2097152
HISTORY_CANONICAL_CANARY_MAX_ROWS=200
```

The adapter accepts only HTTPS, a bounded response, artifact cursors, an exact
workspace response header, exact response identity/generation, and exact
source/session on every row. Tenant fields are checked when rows carry them;
the canonical endpoint normally strips them after its own row validation.
Deadline, transport and 5xx errors are typed retryable refusals. There is no
second backend attempt.

## Build and offline proof

Use Bun 1.4.2 with the frozen lockfile. The proof refuses arbitrary endpoints:
both fixture containers must carry label
`com.96bulls.proof=publication-canary-d`, expose only loopback ports, and use
the exact `publication-canary-*` names supplied in the environment. It creates
a fresh database, runs the repository migrations and role verifier, and uses a
versioned MinIO bucket. All transcript content and credentials are synthetic.

```sh
npm ci
/home/khaliqgant/.local/share/mise/installs/bun/1.4.2/bin/bun install --frozen-lockfile --backend=copy

PUBLICATION_CANARY_DISPOSABLE=1 \
PUBLICATION_CANARY_PROOF_OUT=/home/khaliqgant/relay-briefs/publication-canary-d-<run> \
PUBLICATION_CANARY_PG_CONTAINER=publication-canary-d-pg17-<run> \
PUBLICATION_CANARY_MINIO_CONTAINER=publication-canary-d-minio-<run> \
/home/khaliqgant/.local/share/mise/installs/bun/1.4.2/bin/bun scripts/proof-publication-canary.ts
```

`npm ci` is expected to refuse this repository because it has no npm lockfile;
record that refusal rather than generating one. The proof covers actual
buffered acceptance, complete inspection, atomic owner/plan binding, ordinary
publication and pointer CAS, a visible foreign-tenant hold, directory
projection, canonical replay publication, artifact hydration through the
History adapter, and a disable/enable privacy transition with GC racing a
delayed PUT. It requires the late publisher to fail, the pointer to remain
tombstoned and the late key to contain only the eraser's redaction marker.

The proof's ordered-control source journal is explicitly a synthetic fixture.
A production canary must consume the real source journal's stored control
version and digest.

## Deployment and least privilege plan

Keep the workload absent or suspended by default. After the authority and
shared factory are reviewed, build one immutable one-shot image from the exact
commit and record its image digest and embedded asset/schema generations. Run a
single task for one signed plan with a durable outcome mount. Concurrency above
one, an unbounded candidate scan, a periodic publisher schedule and automatic
GC are out of scope for the first canary.

The publisher keeps the existing dedicated ingest role and managed credential.
Its database endpoint stays pinned and TLS-verified. Object permissions are
limited to reads of the planned tenant's buffered source keys and writes to the
ticket-authorized upload generation; there is no `ListBucket`, delete,
presign, wildcard bucket or owner credential. KMS access is limited to the
bucket key and only the operations required by those reads/writes. The
canonical reader separately receives only its existing claim-bound RPC and
canonical artifact reads. History receives a managed service token for that
reader, not database or bucket credentials.

GC/erasure remains an independent role and process. Its future plan must limit
version listing and `DeleteObjectVersion`/redaction writes to normalized exact
tenant/generation prefixes and retain the existing generation, privacy and
lease fences. The first production publication canary must not activate it.

## Go/no-go and rollback

Before any live canary, require all of the following as fresh evidence:

- the frontier owner's atomic authority and queryable owner-plan binding;
- the shared runtime factory with managed credentials and all existing startup
  gates intact;
- an approved exact org/workspace/stage plan and budget;
- applied migration/ACL and schema-generation verification in the target;
- reviewed image digest, task definition, network policy, IAM and KMS diff;
- complete source coverage, current directory/frontier authority and no
  unexplained cross-scope eligible stage;
- measured production-size memory, latency and object-request headroom;
- a separately approved History route selection with ClickHouse public events
  unchanged;
- independent privacy/erasure review and an operator observing the run.

Any refusal, stale evidence, budget breach, incomplete outcome, unexpected
cross-scope eligibility, generation drift, pointer race, privacy transition or
reader mismatch is no-go. Rollback disables the History selection and leaves
the one-shot publisher suspended. Immutable artifacts and metadata are not
deleted as a rollback shortcut; normal generation-safe erasure/retention owns
them. Never repair a failed canary by bypassing its binding or broadening its
credentials.
