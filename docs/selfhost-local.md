# Local single-user install (docker compose)

`deploy/selfhost-local/compose.yaml` runs the self-host sessions pipeline for
one user on one machine, behind one TLS edge on `127.0.0.1`. It is the
real-user counterpart of the `e2e/selfhost` acceptance stack, built from the
same image and env contract (`docs/selfhost-composition.md`).

| Service | Image | What it does |
| --- | --- | --- |
| `db` | `postgres:17` (no pgvector) | HoopDb, TLS on (S1). No host port |
| `local-init` | `alpine:3.20` (digest-pinned) | one-shot, root: gives the fs Locker directory to uid 1000 and the edge key to nginx (uid 101) |
| `migrate` | services image, `SERVICE=hoopdb-migrate` | one-shot, idempotent: shim roles, bootstrap, all migrations |
| `consumer-bootstrap` | services image | one-shot, idempotent: the pipeline consumer's role and grants |
| `roles` | `postgres:17` | one-shot, idempotent (`roles.sh`): service logins and their passwords |
| `selfhost-control` | services image, `SERVICE=selfhost-control` | the single-user control service (S2): answers token introspection for you only |
| `session-ingest` | services image | uploads; the only Locker writer, exactly one replica |
| `session-pipeline-consumer` | services image | indexes uploaded sessions into Postgres search (reads the Locker only) |
| `session-read-api` | services image | the read API |
| `proxy` | `nginx-unprivileged` (digest-pinned, as `versions.lock.json` `relayauth-tls-proxy`) | the only published port: TLS on `127.0.0.1:8443`, routes per `docs/selfhost-composition.md` section 2 (`nginx.conf`) |

Transcripts live in the `locker` volume through the fs Locker (S3,
`LOCKER_PROVIDER=fs`), and everything else in the `pgdata` volume. There is no
MinIO, no `locker-init` and no relaycontrol. The compose file sets only
existing settings; no runtime code changes come with it.

## Prerequisites

- Docker with Compose v2 (`docker compose version`).
- Bun (for the certificate and smoke scripts) and `openssl`.
- Optional: the 1Password CLI (`op`) to keep secrets off disk.
- A clone of this repository. Run every command below from its root.

## 1. Build the image

```sh
docker build -f docker/services.Dockerfile -t relayhistory-services:local .
```

The compose file never pulls this image (`pull_policy: never`). To use another
tag, set `RH_SERVICES_IMAGE`.

## 2. Certificates

Two throwaway CAs from `scripts/selfhost-local-ca.ts` (`docs/local-stack.md`,
"Local Postgres TLS in two commands"). Each CA key is deleted when the script
exits. `.96bulls/` is gitignored.

```sh
bun scripts/selfhost-local-ca.ts --out .96bulls/pg-tls
bun scripts/selfhost-local-ca.ts --out .96bulls/edge-tls --host localhost --host 127.0.0.1
```

The first prints `SESSION_POSTGRES_CA_SHA256=<hex>`: that hex is
`RH_PG_CA_SHA256`. The second prints a hash too. Ignore it, because nothing pins
the edge CA by hash. If you reach the edge by another name, add a `--host` for it, and use
that name in `RH_ORIGIN`. Then trust `.96bulls/edge-tls/ca.crt` in the client
you upload from (the OS trust store, or the client's own CA setting).

To rotate a certificate, mint into a new directory, point `RH_PG_TLS_DIR` (and
`RH_PG_CA_SHA256`) or `RH_EDGE_TLS_DIR` at it, and run
`rhc up -d --force-recreate`.

## 3. The env file

```sh
cp deploy/selfhost-local/.env.example deploy/selfhost-local/.env
```

`deploy/selfhost-local/.env` is gitignored; never commit it. Compose reads it
from the compose file's directory, so relative paths in it are relative to
`deploy/selfhost-local/`. Fill in:

- `RH_ORIGIN`: the origin your client uses, `https://localhost:8443` by default.
  It must match `RH_HTTPS_PORT` and a name on the edge certificate. Both the
  ingest and the read API check it as the token audience.
- `RH_PG_CA_SHA256`: from step 2.
- `RH_USER_SUB`, `RH_USER_ORG`, `RH_WORKSPACE`: who you are. They are not
  secret. The workspace may contain letters, digits and `_ . : -` only.
- The secrets (`RH_PG_PASSWORD`, `RH_INGEST_PASSWORD`, `RH_READ_PASSWORD`,
  `RH_CONSUMER_PASSWORD`, `RH_INTROSPECT_SECRET`, `RH_CONTROL_TOKEN`). Generate
  each with `openssl rand -hex 32`. `RH_CONTROL_TOKEN` is `sh_at_` followed by
  that hex.

Nothing credential-like should sit on disk. Store each secret in your password
manager, put its `op://` reference in `.env`, and run every compose command
below through `op run`; the resolved values override the references:

```sh
alias rhc='op run --env-file deploy/selfhost-local/.env -- docker compose -f deploy/selfhost-local/compose.yaml'
```

`RH_PG_PASSWORD` takes effect only when the `pgdata` volume is first created.
Changing it later needs an `ALTER ROLE postgres PASSWORD` first. The three
service passwords are re-applied by `roles` on every `up`.

Without `op`, use `alias rhc='docker compose -f deploy/selfhost-local/compose.yaml'`
and export the secrets in your shell instead of writing them into `.env`.
Compose refuses to start while any required value is empty and names the
variable.

## 4. Bring it up

```sh
rhc up -d
rhc ps
```

Start order: `db` becomes healthy, then `migrate`, `consumer-bootstrap` and
`roles` run once and exit 0, and `local-init` exits 0. Then `selfhost-control`
starts, then the ingest, consumer and read API, and finally the proxy. A
one-shot that exits non-zero stops everything after it. Check it with
`rhc logs migrate` (or `roles`, `local-init`). Every service logs the name of a
bad variable, never its value.

## 5. The control token

`selfhost-control` is the only identity authority (`docs/selfhost-composition.md`,
"Local single-user control"). At start, `deploy/selfhost-local/control-tokens.ts`
writes its tokens file from `RH_USER_SUB`, `RH_USER_ORG`, `RH_WORKSPACE` and
`RH_CONTROL_TOKEN`: one token, `token_id` `local-1`, scopes `rth:read` and
`rth:sync`, mode 0600. The file sits on a tmpfs, so the token never reaches
disk. The service process itself starts without `RH_CONTROL_TOKEN` in its
environment.

- Your client's bearer is `RH_CONTROL_TOKEN`, and its History origin is
  `RH_ORIGIN`. The token never expires. A refresh answers 401
  `refresh_not_supported`, as on every self-host.
- To rotate or revoke it, change `RH_CONTROL_TOKEN` in the password manager and
  run `rhc up -d selfhost-control`. The old token stops working at once.
  Ingest and the read API introspect every request
  (`READ_API_INTROSPECT_POSITIVE_TTL_MS=0`, and the ingest default is 0).
- To rotate `RH_INTROSPECT_SECRET`, change it and run `rhc up -d`. That
  recreates control, ingest and the read API together.

## 6. First upload smoke check

```sh
op run --env-file deploy/selfhost-local/.env -- bun deploy/selfhost-local/smoke.ts --ca .96bulls/edge-tls/ca.crt
```

It talks to `RH_ORIGIN` only, through the edge, trusts only `--ca`, and prints
one line per check:

1. `edge.probes-internal`: `GET /healthz` is 404 at the edge.
2. `upload.no-token`: an upload without a bearer is 401.
3. `upload.first`: one synthetic Desktop-shaped batch is 200 with a receipt.
4. `read.listed`: `GET /v1/sessions` lists it once the consumer has indexed it
   (up to 240 s).

Exit 0 means every check passed. It never prints the token. The synthetic
session stays in your history: its project is `/smoke/<marker>`.

## 7. Erase: which fence applies

`INGEST_S3_ERASE_FENCE` does not apply to this install. It needs
`INGEST_LEDGER=s3`, and this compose runs the default SQL ledger on the fs
Locker (`src/session-ingest/selfhost-config.ts`). What applies here:

- **Per-session erase:** `RELAYHISTORY_SELFHOST_SESSION_SELECTION=true` is on.
  Send `POST /v1/delivery/sessions/selection` with
  `{"account_id", "origin_id", "sessions": [{"source", "session_id"}], "enabled": false}`
  to fence the session's uploads and tombstone its pointers and directory row in
  one transaction (`docs/session-ingest-design.md`). After that the read API
  answers 404. It does **not** delete the bytes: the transcript stays in the
  `locker` volume.
- **The operator erase route** (`/internal/v1/session-erase/*`) is off and answers
  501. It is internal, and the edge returns 404 for it anyway.
- **The byte eraser** (`SERVICE=session-erase-bytes`) refuses the fs Locker. To
  remove bytes for good on this install, delete the whole history (teardown
  with `-v`, below).

## 8. Backup

Back up Postgres and the Locker together, with writes stopped, so neither
refers to something the other lacks. A backup holds your transcripts, so keep
it outside the repository.

```sh
mkdir -p ~/relayhistory-backup
rhc stop proxy session-ingest session-pipeline-consumer session-read-api
rhc exec -T db pg_dump -U postgres -Fc postgres > ~/relayhistory-backup/pg.dump
docker run --rm -v relayhistory-local_locker:/locker:ro -v ~/relayhistory-backup:/out \
  alpine:3.20 tar -C /locker -czf /out/locker.tgz .
rhc start session-ingest session-pipeline-consumer session-read-api proxy
```

To restore into a fresh install:

1. Run `rhc up -d` once, so the roles and schema exist.
2. Stop the same four services.
3. Run `rhc exec -T db pg_restore -U postgres -d postgres --clean --if-exists < ~/relayhistory-backup/pg.dump`.
4. Unpack the Locker:
   `docker run --rm -v relayhistory-local_locker:/locker -v ~/relayhistory-backup:/in:ro alpine:3.20 tar -C /locker -xzf /in/locker.tgz`.
5. Run `rhc up -d`.

## 9. Upgrade

```sh
git pull
docker build -f docker/services.Dockerfile -t relayhistory-services:local .
rhc up -d
```

`up -d` recreates the services on the new image and runs the one-shots again.
`migrate` applies only new migrations, and `roles` re-applies the logins. Take
a backup first. The image tag is local, so to roll back, rebuild the previous
commit. A migration that has already been applied is not undone by an older
image.

## 10. Teardown

```sh
rhc down       # stop and remove containers; volumes (history) are kept
rhc down -v    # also delete pgdata, locker and edge-tls: all history, for good
```

The certificates in `.96bulls/` and the secrets in your password manager are
not touched. Delete them yourself if you are done.

## Limits

- One user, one machine. The edge listens on `127.0.0.1` only. Publishing it
  further needs a real certificate and a review of `nginx.conf`.
- Exactly one `session-ingest`: the fs Locker serializes writes per process.
- The edge answers 404 to a path with a backslash, an encoded slash, dot or
  backslash, an empty segment or a dot segment, so the services' URL parser
  can never resolve a path differently from nginx's routing.
- Secrets reach the containers as environment variables. Anyone who can run
  `docker inspect` on this machine can read them.
- AlleyOop, the publisher, the indexer stages, the restamp tool and the
  operator erase refuse the fs Locker. None of them run here.
- Search is Postgres full text. Embeddings (pgvector) are not part of this
  install.
