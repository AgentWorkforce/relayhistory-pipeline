# 96Bulls

Infrastructure and services for the Agent Relay **sessions pipeline**, with one
entry point that brings the whole pipe up or down:

```
bin/96bulls up local      # stand everything up on this machine (Docker, via Alchemy)
bin/96bulls smoke local   # follow one session end to end and prove no PII leaks
bin/96bulls down local    # tear it all down
```

## The line-up

| Name | What it is | Here |
| --- | --- | --- |
| **Concourse** | the local `history.db` an agent machine keeps (relayhistory's `ai-hist`) | seeded from fixtures, `src/concourse/` |
| **AlleyOop** | local to object storage: the courier (client) and the ingest API | `src/alleyoop/` |
| **Technical Foul Prevent** | secrets/PII scrubbed *before* any byte reaches object storage | inside the ingest chunk hop (vendored `lib/scrub.ts`) |
| **Locker** | S3 behind the adapter (MinIO locally, S3 in AWS) | `src/locker/`, stack |
| **HoopDb** | Postgres with pgvector: the sessions index, pipeline queue, events | `src/hoopdb/` |
| **Referee** | session-state classifier (`active` / `done` / `abandoned`) | `src/referee/` |
| **Bob Costas** | short and long summaries | `src/bob-costas/` |

One `session_uuid` (UUIDv7, minted once on the client) and one pipeline
`trace_id` per session travel through every hop, every row and every span. See
`docs/observability-contract.md`.

## Layout

- `alchemy.run.ts`: the stack, every stage. `local` uses the Alchemy Docker provider; `aws-*` is declared and gated (`docs/aws-stage.md`).
- `stacks/local/topology.ts`: names, ports, images, tenant ids, as data.
- `src/`: the services (Bun/TypeScript). One image, `SERVICE=` picks the entry (`docker/`).
- `vendor/relayhistory-cloud`: the upstream pipeline framework and object-store adapters, pinned to the head of relayhistory-cloud PR #211 as a submodule. Never edited here. (PR #1 moves the framework into `packages/pipeline`; the services will switch to it when it lands.)
- `vendor/relayhistory`: the `ai-hist` Rust CLI and transcript fixtures, for Concourse.
- `docs/`, `runbooks/`, `versions.lock.json`.

## Requirements

Docker, Bun >= 1.4, and a Rust toolchain (>= 1.89) for the one-time `ai-hist` build:

```
git submodule update --init --depth 1
bun install
(cd vendor/relayhistory-cloud && npm ci && npm run build -w packages/object-store -w packages/cloud-client)
(cd vendor/relayhistory && cargo build --release -p ai-hist-cli)
```

## Guardrails

Nothing here creates a cloud resource, credential or IAM entity. The `aws-*`
stages refuse to run until that work is cleared. `.96bulls/` holds local
secrets (mode 600) and is gitignored; `.alchemy/` holds state in plaintext and
is gitignored.
