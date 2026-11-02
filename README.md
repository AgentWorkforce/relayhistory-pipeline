# Agent Relay sessions pipeline ("96Bulls")

**This repo is the sessions pipeline: the system that takes an AI coding
session from an agent's machine and turns it into stored, searchable,
summarized history.** "96Bulls" is the project's codename; the component
nicknames below are just labels.

What happens to a session, end to end:

1. **Collect:** an agent machine keeps its session history locally; a courier
   uploads it.
2. **Ingest and scrub:** the ingest API checks the caller (a RelayAuth token
   bound to that machine) and removes secrets and PII **before any byte is
   stored**.
3. **Store:** raw transcript chunks and a manifest go to object storage (S3 or
   GCS).
4. **Index:** each session gets a row in the Postgres session index (title,
   user, machine, repo, state, usage, a pointer to its objects).
5. **Enrich:** workers classify the session (`active` / `done` / `abandoned`)
   and write short and long summaries, encrypted at rest.

## How it relates to relayhistory-cloud

| | This repo (sessions pipeline) | relayhistory-cloud |
| --- | --- | --- |
| Role | **writes and processes** sessions: ingest, scrub, store, index, enrich | **serves reads** today: history recall, transcripts, memory |
| Runs as | portable services (one container image, Helm charts) on local Docker, AWS (EKS) or GCP (GKE) | a Cloudflare Worker |
| Data | content in object storage; index in Postgres (Supabase in production) | Agent Relay Cloud's Neon `sessions` schema |

Per decision 0002 (`docs/decisions/0002-relayhistory-onto-the-pipeline.md`),
RelayHistory is moving onto this pipeline. New writes go through here, history
is exported into this pipeline's storage, and the relayhistory-cloud Worker is
re-pointed to read from it. This pipeline becomes the system of record for
sessions; relayhistory-cloud becomes a read API in front of it.

## What this repo owns

- **Its own cloud resources**, as Alchemy stacks: `stacks/aws/` (EKS, S3, IAM,
  ingress), `stacks/gcp/` (GKE, GCS, Cloud SQL, IAM, network policy) and the
  local stack (`alchemy.run.ts`). No other repo defines this pipeline's
  infrastructure. Pippen Tools (`AgentWorkforce/pippen-tools`) only provides
  accounts, sandboxes and guardrails; this repo deploys its own stack into
  them.
- **Its services** (`src/`) and the pipeline framework (`packages/pipeline`,
  `packages/object-store`).
- **The session index schema and migrations** (`src/hoopdb/`, `supabase/`).
- **Its stages, proofs and runbooks** (`docs/`, `runbooks/`).

It does not own: the Helm charts (`AgentWorkforce/helm-charts`), RelayAuth
(token issuer), the read API (relayhistory-cloud), or the cloud organization
and sandbox accounts (Pippen Tools).

## Quick start

One entry point brings the whole pipeline up or down:

```
bin/96bulls up local      # stand everything up on this machine (Docker, via Alchemy)
bin/96bulls smoke local   # follow one session end to end and prove no PII leaks
bin/96bulls down local    # tear it all down
```

## Components (plain name first, codename second)

| Component | What it does | Code |
| --- | --- | --- |
| **Local history store** (Concourse) | the `history.db` an agent machine keeps (relayhistory's `ai-hist`) | `src/concourse/` (seeded from fixtures) |
| **Courier and ingest API** (AlleyOop) | the client that uploads, and the API that receives, checks the machine-bound token and writes | `src/alleyoop/` |
| **Scrubber** (Technical Foul Prevent) | removes secrets and PII before anything reaches storage | inside the ingest chunk hop (vendored `lib/scrub.ts`) |
| **Object storage** (Locker) | S3 or GCS behind one adapter (MinIO locally) | `src/locker/`, stacks |
| **Session index DB** (HoopDb) | Postgres: the session index, pipeline queue and events | `src/hoopdb/` |
| **State classifier** (Referee) | marks a session `active`, `done` or `abandoned` | `src/referee/` |
| **Summarizer** (Bob Costas) | short and long summaries, encrypted at rest | `src/bob-costas/` |

One `session_uuid` (UUIDv7, minted once on the client) and one pipeline
`trace_id` per session travel through every hop, every row and every span. See
`docs/observability-contract.md`.

## Layout

- `alchemy.run.ts`: the local stack. Cloud stacks are isolated and gated:
  `stacks/aws/alchemy.aws.run.ts` (`docs/aws-stage.md`) and
  `stacks/gcp/alchemy.gcp.run.ts` (`docs/gcp-stage.md`).
- `stacks/local/topology.ts`: names, ports, images, tenant ids, as data.
- `src/`: the services (Bun/TypeScript). One image, `SERVICE=` picks the entry (`docker/`).
- `packages/pipeline`, `packages/object-store`: the pipeline framework and object-store adapters, moved from relayhistory-cloud #210 and #211 (byte-identical at the move, except where `MOVE_NOTES.md` records a later change, such as scrub rule set 2 in `raw-chunks.ts`; it also has the mapping and the few stand-ins). The services import them through `src/shared/vendor.ts`.
- `vendor/relayhistory`: the `ai-hist` Rust CLI and transcript fixtures, for Concourse.
- `docs/`, `runbooks/`, `versions.lock.json`.

## Requirements

Docker, Bun >= 1.4, and a Rust toolchain (>= 1.89) for the one-time `ai-hist` build:

```
git submodule update --init --depth 1        # vendor/relayhistory: the ai-hist CLI and fixtures
bun install
bun run --filter @relayhistory/object-store build
(cd vendor/relayhistory && cargo build --release -p ai-hist-cli)
```

## Guardrails

Cloud stages are gated: an AWS or GCP stage applies only with Khaliq's explicit
approval, through guarded entry points (`docs/aws-stage.md`, `docs/gcp-stage.md`),
and refuses to run against an unexpected account or project. The local stage
needs no cloud credentials. `.96bulls/` holds local
secrets (mode 600) and is gitignored; `.alchemy/` holds state in plaintext and
is gitignored.
