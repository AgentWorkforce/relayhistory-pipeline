/**
 * The local stage, as data. Every container name, port, image and env name the
 * sessions pipeline uses locally lives here so `alchemy.run.ts`, the CLI's
 * readiness checks and the smoke test agree on one source.
 *
 * Basketball names (Khaliq's): Concourse = local history.db, AlleyOop = local to
 * object storage, Technical Foul = scrub before storage, Locker = S3 behind the
 * adapter, HoopDb = Postgres, Referee = session-state classifier, Bob Costas =
 * summaries.
 */
export const STACK = "96bulls";

/** Images pinned by tag now; digests go into versions.lock.json on first release. */
export const IMAGES = {
  hoopdb: "pgvector/pgvector:pg17",
  // MinIO's own registries went private in 2025; Chainguard's build is public. Pinned by digest.
  locker: "cgr.dev/chainguard/minio@sha256:268d68a2a3b6fc65f905a61f84d7eab6345ce6bff14f551e2b028ec31e6a1341",
  otelCollector: "otel/opentelemetry-collector-contrib:0.137.0",
  services: "96bulls/services:local",
  /** Root one-shot for volume permissions; Alchemy containers cannot set a user. */
  init: "alpine@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc",
} as const;

/** Host ports. Offset from the defaults so this stack never collides with other local work. */
export const PORTS = {
  hoopdb: 15432,
  locker: 19100,
  ingest: 18080,
  otelHttp: 14318,
} as const;

/** Container names, all prefixed so `docker ps` shows the stack at a glance. */
export const NAMES = {
  network: "96b-net",
  hoopdb: "96b-hoopdb",
  hoopdbData: "96b-hoopdb-data",
  hoopdbMigrate: "96b-hoopdb-migrate",
  locker: "96b-locker",
  lockerData: "96b-locker-data",
  lockerInit: "96b-locker-init",
  otel: "96b-otel-collector",
  telemetry: "96b-telemetry",
  telemetryInit: "96b-telemetry-init",
  ingest: "96b-alleyoop-ingest",
  indexer: "96b-hoopdb-indexer",
  referee: "96b-referee",
  bobCostas: "96b-bob-costas",
  courier: "96b-alleyoop-courier",
} as const;

export const TENANT = {
  orgId: "org_local",
  workspaceId: "ws_local",
  userId: "user_local",
  issuer: "https://96bulls.local",
} as const;

export const LOCKER_BUCKET = "locker";
export const HOOPDB_NAME = "hoopdb";
export const HOOPDB_USER = "hoop";

/** Paths inside the services image. */
export const IMAGE_PATHS = {
  app: "/app",
  telemetry: "/telemetry",
  concourse: "/concourse",
} as const;
