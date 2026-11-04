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
- `packages/coordination-eval`: the coordination evidence rubric, bounded analysis
  engine and report renderer. `src/coordination-report` runs canonical workspace
  retrospectives inside the pipeline and persists draft reports in HoopDb; see
  the [runbook](runbooks/coordination-reports.md) and
  [review workflow](packages/coordination-eval/README.md). Local pilot artifacts
  stay in the gitignored `.coordination-eval/` directory.
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

## Destinations (hosted vs self-host)

Every place the runtime services call out or bind trust is an env var. Nothing
in `src/` or `packages/` sends to a hosted endpoint that a self-host cannot
replace. URLs that carry a credential or choose which signing keys are trusted
are validated at startup by one helper (`src/shared/destination-url.ts`): https
always, plain http only to loopback (`localhost`, `127.0.0.1`, `[::1]`), and
never with credentials, a query or a fragment in the URL. The error names the
variable and the rule, never the value.

`ALLOW_IN_CLUSTER_HTTP=true` (default off; any value other than `true`/`false`
fails startup) additionally allows plain http to a Kubernetes Service DNS name,
exactly `<svc>.<ns>.svc` or `<svc>.<ns>.svc.cluster.local` (RFC 1123 labels).
Public names, IP literals, single labels, `*.svc.example.com` and custom cluster
domains still need https. Set it only on pods that run inside the cluster; it
changes transport only. Audience checks are unchanged: the courier's
`RELAYAUTH_HISTORY_ORIGIN` and the ingest's `RELAYHISTORY_ORIGIN` stay exact
bare-origin matches, and the RelayAuth URL is never used as an audience.

| Env var | Service | Default | Validated | Self-host value (example) |
| --- | --- | --- | --- | --- |
| `INGEST_URL` | courier | required | shape always; transport rule when `RELAYAUTH_URL` or `CLOUD_URL` is set | `http://<release>-alleyoop-ingest.<ns>.svc:8080` (flag) or your https ingress |
| `RELAYAUTH_URL` | courier | unset (mode off) | transport rule | `http://relayauth.relayauth.svc:8787` (flag) |
| `RELAYAUTH_HISTORY_ORIGIN` | courier | required with `RELAYAUTH_URL` | exact bare origin + transport rule | the ingest's `RELAYHISTORY_ORIGIN`, byte for byte |
| `CLOUD_URL` | courier | unset (mode off) | transport rule | your Cloud origin; leave unset to use `RELAYAUTH_URL` |
| `AUTH_ISSUER` | courier (local key), ingest | `https://96bulls.local` | string compared with `iss` | RelayAuth's issuer (relayauth chart `issuer`) |
| `AUTH_AUDIENCE` | courier (local key), ingest | `relayhistory` | string | `relayhistory` |
| `RELAYAUTH_JWKS_URL` | ingest | `http://127.0.0.1:$INGEST_PORT/.well-known/jwks.json` (self-served local key) | transport rule when set | `http://relayauth.relayauth.svc:8787/.well-known/jwks.json` (flag) |
| `RELAYHISTORY_ORIGIN` | ingest | unset (legacy shared audience accepted) | exact bare origin + transport rule | the origin couriers mint for, e.g. `http://<release>-alleyoop-ingest.<ns>.svc:8080` (flag) |
| `RELAYAUTH_JWKS_URL`, `RELAYAUTH_ISSUER` | read API (Lambda, session-read-api) | `https://relayauth.dev/.well-known/jwks.json`, `https://relayauth.dev` (vendored defaults) | JWKS: transport rule when set | your RelayAuth JWKS and issuer |
| `READ_API_ORIGIN` | read API | unset (every request 401) | exact audience | the read API's own origin |
| `RELAYHISTORY_SELFHOST_READ_API` | read API | unset (off: `POST /v1/cli/login` and `GET /v1/sessions/:id/catalog` are not registered) | `true`/`false`/empty only, else startup fails | `true` on a self-host whose relaycontrol proxies History reads; login also needs `RELAYHISTORY_ACCEPT_RELAYCONTROL_TOKENS=true` (docs/session-read-api-design.md §2a) |
| `OPENAI_BASE_URL` | session-brief | `https://api.openai.com/v1` (calls `<base>/responses`) | transport rule (the request carries `OPENAI_API_KEY`) | an OpenAI-compatible gateway, e.g. `http://llm-gateway.ai.svc:4000/v1` (flag) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | all | unset: no export | not validated (no credential) | your collector, e.g. `http://otel-collector.observability.svc:4318` |
| `LOCKER_ENDPOINT` | all | S3 for `LOCKER_REGION` | GCS mode: own stricter check (`src/shared/locker-selection.ts`) | MinIO Service or object store |
| `RELAYHISTORY_SELFHOST_READ_API` | session-read-api | unset (dormant: `/healthz` only, every other route 503) | `true`/`false` only | `true` once `RELAYHISTORY_READ_API_SECRET` and the read API env above are set |
| `RELAYHISTORY_READ_API_SECRET` | session-read-api | required when enabled | the Lambda secret's JSON shape (`parseReadApiSecret`); the `supabase_url` login must be `NOSUPERUSER NOBYPASSRLS` (checked at startup; tenant isolation is forced RLS) | from the secret store (1Password), never on disk |
| `RELAYHISTORY_READ_API_PORT` | session-read-api | `8080` (parsed even when dormant) | TCP port 1-65535 | `8080` |
| `SEARCH_INDEX_BACKEND` | session-read-api (enabled) | `postgres` (no ClickHouse, no Lambda env) | `postgres` or `clickhouse` (registry names; anything else, or a name with no implementation in this build, fails startup naming only the variable) | `postgres`; `clickhouse` keeps the hosted binding and needs the secret's `clickhouse` block |
| `SEARCH_INDEX_POSTGRES_BACKEND_ID` | session-read-api (enabled, postgres) | required | `[a-zA-Z0-9._-]{1,128}` | the projection-state backend id your search writer records under; coverage is from `READ_API_COMPLETE_SINCE` |
| `SESSION_POSTGRES_TLS_PROFILE` | session-read-api (enabled) | required explicitly (no implicit Supabase pin) | `supabase`, or `custom-ca` with `SESSION_POSTGRES_CA_FILE`/`SESSION_POSTGRES_CA_SHA256` (docs/portable-lambda-runtime.md) | `custom-ca` with your Postgres CA bundle |
| `ALLOW_IN_CLUSTER_HTTP` | courier, ingest, read API, session-brief | `false` | `true`/`false` only | `true` on in-cluster pods that use `*.svc` URLs |

Hosted values that are not runtime destinations of a self-host: the vendored
PostHog default (`https://i.agentrelay.com`, `packages/pipeline/src/lib/logger.ts`)
and the vendored `PIPELINE_EVENTS_URL` / `SENTRY_DSN` sinks are inert because
`vendorBindings()` never passes their keys; the session pipeline Lambda's AWS
endpoints come from `AWS_REGION` (AWS stage only); `https://read-api.internal`
in the read API Lambda is a synthetic in-process request URL; the Cloudflare
session-read Worker takes its RelayAuth values from `wrangler.jsonc`.
`stacks/`, `benchmarks/`, `integration/` and the one-off `src/migration/` and
`src/proof/` scripts set their own values.

## Guardrails

Cloud stages are gated: an AWS or GCP stage applies only with Khaliq's explicit
approval, through guarded entry points (`docs/aws-stage.md`, `docs/gcp-stage.md`),
and refuses to run against an unexpected account or project. The local stage
needs no cloud credentials. `.96bulls/` holds local
secrets (mode 600) and is gitignored; `.alchemy/` holds state in plaintext and
is gitignored.

Amp provider commands reuse the ingest process host; see [persistent Amp transport](docs/amp-transport.md) for setup and the Cloud ownership boundary.

Image release and verification: [services image release guide](docs/services-image-release.md).
