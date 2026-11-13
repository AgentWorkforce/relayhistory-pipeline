/**
 * The pipeline e2e stack: e2e/selfhost/compose.yaml with e2e/pipeline/compose.yaml layered over it, as a fresh
 * compose project per run (its own network and volumes, loopback-only ephemeral ports). Per-run credentials
 * (database, MinIO, introspection, the courier's signing key, the summary master key) are generated here, held
 * in memory and the compose environment only, and gone when the run ends. Only the Postgres CA and server
 * certificate touch disk, in the run's own .96bulls/e2e-* directory, removed on exit (as e2e/selfhost does).
 */
import { randomBytes } from "node:crypto";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { generateLocalKeys, type LocalKeys } from "../../src/alleyoop/auth.ts";
import { createS3Store } from "../../src/shared/vendor.ts";
import { createStack, DEFAULT_IMAGE, type StackSession } from "../selfhost/stack.ts";

export const SELFHOST_SERVICES = ["session-ingest", "session-ingest-s3", "session-pipeline-consumer", "session-read-api"] as const;
export const HOOP_SERVICES = ["alleyoop-ingest", "hoopdb-indexer", "referee", "bob-costas"] as const;
export const KEY_ROOT = "96bulls";
export const CANONICAL_BUCKET = "transcripts";
export const HOOP_BUCKET = "locker";
export const HOOP_TENANT = { orgId: "org_hoop", workspaceId: "ws_hoop", userId: "user_hoop", machineId: "m-e2e-pipeline" } as const;

export interface PipelineStack extends StackSession {
  project: string;
  /** The services image this run starts (and, unless E2E_SKIP_BUILD, builds and removes). */
  image: string;
  keys: LocalKeys;
  summaryMasterKeys: string;
  urls: { ingestS3: string; minio: string; alleyoop: string };
  store(bucket: string): ReturnType<typeof createS3Store>;
  /** psql as the database owner inside the db container: tab-separated rows. */
  sql(query: string): string[][];
  logs(service: string): string;
  resolvePipelineUrls(services: readonly string[]): void;
}

export function createPipelineStack(project = `rh-pipe-${randomBytes(4).toString("hex")}`): PipelineStack {
  const keys = generateLocalKeys();
  const summaryMasterKeys = `e2e-1:${randomBytes(32).toString("hex")}`;
  // A per-run image tag: two runs on one Docker host (two CI flows, or this beside e2e:selfhost) never start
  // containers from the other's build. E2E_SKIP_BUILD=1 runs an image built before, named by E2E_IMAGE.
  const image = process.env.E2E_SKIP_BUILD ? process.env.E2E_IMAGE?.trim() || DEFAULT_IMAGE : `96bulls/services:e2e-${project}`;
  const base = createStack({
    files: [fileURLToPath(new URL("compose.yaml", import.meta.url))],
    project,
    extraEnv: { E2E_JWKS_JSON: JSON.stringify(keys.jwks), E2E_SUMMARY_MASTER_KEYS: summaryMasterKeys, E2E_IMAGE: image },
  });
  const hostPort = (service: string, port: string) => {
    const out = base.compose(["port", service, port], { quiet: true, allowFail: true }).stdout.trim();
    const value = out.slice(out.lastIndexOf(":") + 1);
    // A container that exited reports port 0.
    if (!/^[1-9]\d*$/u.test(value)) throw new Error(`no host port for ${service}:${port} (is it running?)`);
    return `http://127.0.0.1:${value}`;
  };
  const stack = base as PipelineStack;
  stack.project = project;
  stack.image = image;
  stack.keys = keys;
  stack.summaryMasterKeys = summaryMasterKeys;
  stack.urls = { ingestS3: "", minio: "", alleyoop: "" };
  stack.resolvePipelineUrls = (services) => {
    stack.urls.minio = hostPort("minio", "9000");
    if (services.includes("session-ingest")) stack.resolveUrls();
    if (services.includes("session-ingest-s3")) stack.urls.ingestS3 = hostPort("session-ingest-s3", "8081");
    if (services.includes("alleyoop-ingest")) stack.urls.alleyoop = hostPort("alleyoop-ingest", "8080");
  };
  stack.store = (bucket) => createS3Store({
    bucket, region: "us-east-1", endpoint: stack.urls.minio,
    credentials: { accessKeyId: base.env.E2E_MINIO_USER!, secretAccessKey: base.env.E2E_MINIO_PASSWORD! },
  });
  stack.sql = (query) => {
    const r = base.compose(["exec", "-T", "db", "psql", "-U", "postgres", "-d", "postgres", "-X", "-q", "-At", "-F", "\t", "-v", "ON_ERROR_STOP=1", "-c", query], { quiet: true, allowFail: true });
    if (r.status !== 0) throw new Error(`psql failed (${r.status})`);
    return r.stdout.split("\n").filter(Boolean).map((line) => line.split("\t"));
  };
  stack.logs = (service) => base.compose(["logs", "--no-color", "--no-log-prefix", service], { quiet: true, allowFail: true }).stdout;
  return stack;
}
