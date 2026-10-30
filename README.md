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

- `alchemy.run.ts`: the local stack. Cloud stacks are isolated and gated:
  `stacks/aws/alchemy.aws.run.ts` (`docs/aws-stage.md`) and
  `stacks/gcp/alchemy.gcp.run.ts` (`docs/gcp-stage.md`).
- `stacks/local/topology.ts`: names, ports, images, tenant ids, as data.
- `src/`: the services (Bun/TypeScript). One image, `SERVICE=` picks the entry (`docker/`).
- `packages/pipeline`, `packages/object-store`: the pipeline framework and object-store adapters, moved from relayhistory-cloud #210 and #211 (byte-identical; `MOVE_NOTES.md` has the mapping and the few stand-ins). The services import them through `src/shared/vendor.ts`.
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

Nothing here creates a cloud resource, credential or IAM entity. The `aws-*`
stages refuse to run until that work is cleared. `.96bulls/` holds local
secrets (mode 600) and is gitignored; `.alchemy/` holds state in plaintext and
is gitignored.
