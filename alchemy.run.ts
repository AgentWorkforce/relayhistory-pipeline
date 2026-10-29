/**
 * 96bulls infrastructure, one entry point for every stage.
 *
 *   96bulls up local      ->  alchemy deploy  --stage local
 *   96bulls down local    ->  alchemy destroy --stage local
 *
 * Stage `local` stands the whole sessions pipeline up as Docker containers on
 * this machine with the Alchemy Docker provider. Stages matching `aws-*` are
 * declared but refuse to run until Khaliq clears the AWS account work
 * (see docs/aws-stage.md); nothing in this file creates a cloud resource.
 */
import * as Alchemy from "alchemy";
import { Stage } from "alchemy";
import * as Docker from "@alchemy.run/docker";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { HOOPDB_NAME, HOOPDB_USER, IMAGES, IMAGE_PATHS, LOCKER_BUCKET, NAMES, PORTS, STACK, TENANT } from "./stacks/local/topology.ts";

const ROOT = fileURLToPath(new URL(".", import.meta.url));

/** Secrets and seed paths `96bulls up` prepares under .96bulls/<stage>/ before deploying. */
interface LocalInputs {
  hoopdbPassword: string;
  lockerAccessKeyId: string;
  lockerSecretAccessKey: string;
  jwksJson: string;
  jwtKid: string;
  jwtPrivateKeyPem: string;
  courierToken: string;
  machineId: string;
  concourseDir: string;
  telemetryDir: string;
  deploymentId: string;
}

function readInputs(stage: string): LocalInputs {
  const path = join(ROOT, ".96bulls", stage, "inputs.json");
  try {
    return JSON.parse(readFileSync(path, "utf8")) as LocalInputs;
  } catch {
    throw new Error(`96bulls: ${path} is missing; run \`bin/96bulls up ${stage}\` instead of alchemy directly`);
  }
}

export default Alchemy.Stack(
  STACK,
  {
    providers: Layer.mergeAll(Docker.providers()),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const stage = yield* Stage;
    if (stage.startsWith("aws")) {
      throw new Error("96bulls: AWS stages are not cleared yet (see docs/aws-stage.md); nothing was created");
    }
    if (stage !== "local" && !stage.startsWith("local-")) {
      throw new Error(`96bulls: unknown stage ${stage}; use local or local-<name>`);
    }
    const inputs = readInputs(stage);
    const suffix = stage === "local" ? "" : `-${stage.slice("local-".length)}`;
    const name = (base: string) => `${base}${suffix}`;

    const net = yield* Docker.Network("net", { name: name(NAMES.network) });
    const hoopdbData = yield* Docker.Volume("hoopdb-data", { name: name(NAMES.hoopdbData) });
    const lockerData = yield* Docker.Volume("locker-data", { name: name(NAMES.lockerData) });
    const telemetry = yield* Docker.Volume("telemetry", { name: name(NAMES.telemetry) });

    const hoopdbUrlInCluster = `postgres://${HOOPDB_USER}:${inputs.hoopdbPassword}@${name(NAMES.hoopdb)}:5432/${HOOPDB_NAME}`;
    const lockerEndpointInCluster = `http://${name(NAMES.locker)}:9000`;
    const otelEndpointInCluster = `http://${name(NAMES.otel)}:4318`;

    // HoopDb: Postgres 17 with pgvector.
    const hoopdb = yield* Docker.Container("hoopdb", {
      name: name(NAMES.hoopdb),
      image: IMAGES.hoopdb,
      environment: { POSTGRES_DB: HOOPDB_NAME, POSTGRES_USER: HOOPDB_USER, POSTGRES_PASSWORD: inputs.hoopdbPassword },
      ports: [{ external: `127.0.0.1:${PORTS.hoopdb}`, internal: 5432 }], // loopback only: the ingest API is the boundary
      volumes: [{ hostPath: hoopdbData.name, containerPath: "/var/lib/postgresql/data" }],
      networks: [{ name: net.name, aliases: [name(NAMES.hoopdb)] }],
      healthcheck: { cmd: ["CMD-SHELL", `pg_isready -U ${HOOPDB_USER} -d ${HOOPDB_NAME}`], interval: "5 seconds", timeout: "3 seconds", retries: 20 },
      restart: "unless-stopped",
      labels: { "96bulls.stack": stage, "96bulls.component": "hoopdb" },
      start: true,
    });

    // Locker: MinIO, S3 API only.
    const locker = yield* Docker.Container("locker", {
      name: name(NAMES.locker),
      image: IMAGES.locker,
      command: ["server", "/data", "--address", ":9000"],
      environment: { MINIO_ROOT_USER: inputs.lockerAccessKeyId, MINIO_ROOT_PASSWORD: inputs.lockerSecretAccessKey, MINIO_BROWSER: "off" },
      ports: [{ external: `127.0.0.1:${PORTS.locker}`, internal: 9000 }],
      volumes: [{ hostPath: lockerData.name, containerPath: "/data" }],
      networks: [{ name: net.name, aliases: [name(NAMES.locker)] }],
      healthcheck: { cmd: ["CMD-SHELL", 'mc alias set h http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 && mc ready h >/dev/null 2>&1'], interval: "5 seconds", timeout: "4 seconds", retries: 20 },
      restart: "unless-stopped",
      labels: { "96bulls.stack": stage, "96bulls.component": "locker" },
      start: true,
    });

    // The collector runs as uid 10001 and a fresh named volume is root-owned: open it up first.
    const telemetryInit = yield* Docker.Container("telemetry-init", {
      name: name(NAMES.telemetryInit),
      image: IMAGES.init,
      command: ["sh", "-c", `chmod 0777 ${IMAGE_PATHS.telemetry} && echo telemetry volume ready`],
      volumes: [{ hostPath: telemetry.name, containerPath: IMAGE_PATHS.telemetry }],
      restart: "no",
      labels: { "96bulls.stack": stage, "96bulls.component": "telemetry-init" },
      start: true,
    });

    // OpenTelemetry collector: OTLP in, file + debug out. Depends on telemetry-init through its label.
    const otel = yield* Docker.Container("otel-collector", {
      name: name(NAMES.otel),
      image: IMAGES.otelCollector,
      command: ["--config=/etc/otelcol/config.yaml"],
      ports: [{ external: `127.0.0.1:${PORTS.otelHttp}`, internal: 4318 }],
      volumes: [
        { hostPath: join(ROOT, "docker/otel-collector.yaml"), containerPath: "/etc/otelcol/config.yaml", readOnly: true },
        { hostPath: telemetry.name, containerPath: IMAGE_PATHS.telemetry },
      ],
      networks: [{ name: net.name, aliases: [name(NAMES.otel)] }],
      restart: "unless-stopped",
      labels: { "96bulls.stack": stage, "96bulls.component": "otel-collector", "96bulls.after": telemetryInit.name },
      start: true,
    });

    // The services image, built from this checkout (vendored sources included).
    const services = yield* Docker.Image("services", {
      name: "96bulls/services",
      tag: stage,
      skipPush: true,
      build: { context: ROOT, dockerfile: join(ROOT, "docker/services.Dockerfile") },
    });

    const common = {
      HOOPDB_URL: hoopdbUrlInCluster,
      LOCKER_ENDPOINT: lockerEndpointInCluster,
      LOCKER_BUCKET,
      LOCKER_REGION: "us-east-1",
      LOCKER_ACCESS_KEY_ID: inputs.lockerAccessKeyId,
      LOCKER_SECRET_ACCESS_KEY: inputs.lockerSecretAccessKey,
      TENANT_ORG_ID: TENANT.orgId,
      WORKSPACE_ID: TENANT.workspaceId,
      OTEL_EXPORTER_OTLP_ENDPOINT: otelEndpointInCluster,
      DEPLOYMENT: "selfhost",
      DEPLOYMENT_ID: inputs.deploymentId,
      ENVIRONMENT: "local",
    };
    const service = (id: string, containerName: string, serviceName: string, extra: Record<string, string> = {}, opts: { ports?: Array<{ external: number | string; internal: number }>; volumes?: Array<{ hostPath: string; containerPath: string; readOnly?: boolean }>; restart?: "no" | "unless-stopped" } = {}) =>
      Docker.Container(id, {
        name: containerName,
        image: services,
        environment: { ...common, SERVICE: serviceName, OTEL_SERVICE_NAME: serviceName, ...extra },
        networks: [{ name: net.name, aliases: [containerName] }],
        restart: opts.restart ?? "unless-stopped",
        ...(opts.ports ? { ports: opts.ports } : {}),
        ...(opts.volumes ? { volumes: opts.volumes } : {}),
        labels: { "96bulls.stack": stage, "96bulls.component": serviceName },
        // Workers serve /healthz on WORKER_HEALTH_PORT (8081); the ingest on 8080. One probe for both.
        ...(serviceName === "hoopdb-migrate" || serviceName === "locker-init" || serviceName === "alleyoop-courier"
          ? {}
          : { healthcheck: { cmd: ["CMD-SHELL", `bun -e "fetch('http://127.0.0.1:${serviceName === "alleyoop-ingest" ? 8080 : 8081}/healthz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"`], interval: "15 seconds", timeout: "5 seconds", retries: 3 } }),
        start: true,
      });

    // Locker init: the bucket, created once (SigV4, no SDK); retries until MinIO answers. A Job in every stage.
    yield* service("locker-init", name(NAMES.lockerInit), "locker-init", {}, { restart: "no" });

    // HoopDb migrations: one-shot, idempotent, no hooks. Exits 0 when the ledger is complete.
    yield* service("hoopdb-migrate", name(NAMES.hoopdbMigrate), "hoopdb-migrate", {}, { restart: "no" });

    // AlleyOop ingest (Technical Foul runs inside it, before any byte reaches the Locker).
    const ingest = yield* service("alleyoop-ingest", name(NAMES.ingest), "alleyoop-ingest", {
      INGEST_PORT: "8080",
      JWKS_JSON: inputs.jwksJson,
      AUTH_ISSUER: TENANT.issuer,
    }, { ports: [{ external: `127.0.0.1:${PORTS.ingest}`, internal: 8080 }] });

    yield* service("hoopdb-indexer", name(NAMES.indexer), "hoopdb-indexer");
    yield* service("referee", name(NAMES.referee), "referee");
    yield* service("bob-costas", name(NAMES.bobCostas), "bob-costas", { BOB_COSTAS_MODEL: "off" });

    // AlleyOop courier: watches the seeded Concourse (history.db) and uploads.
    yield* service("alleyoop-courier", name(NAMES.courier), "alleyoop-courier", {
      CONCOURSE_DB: `${inputs.concourseDir}/history.db`,
      COURIER_STATE: `${inputs.concourseDir}/.alleyoop-courier.json`,
      INGEST_URL: `http://${name(NAMES.ingest)}:8080`,
      // The courier only talks to AlleyOop now; it never receives Locker credentials.
      LOCKER_ACCESS_KEY_ID: "",
      LOCKER_SECRET_ACCESS_KEY: "",
      // Short-lived tokens minted in memory from the stack's own key; no 7-day token in the container.
      JWT_KID: inputs.jwtKid,
      JWT_PRIVATE_KEY_PEM: inputs.jwtPrivateKeyPem,
      AUTH_ISSUER: TENANT.issuer,
      USER_ID: TENANT.userId,
      MACHINE_ID: inputs.machineId,
      COURIER_INTERVAL_MS: "5000",
      COURIER_CLOSE_AFTER_MINUTES: "30",
    }, {
      // Mounted at the same absolute path so raw_path values in history.db stay valid.
      volumes: [{ hostPath: inputs.concourseDir, containerPath: inputs.concourseDir }],
    });

    return {
      stage,
      hoopdb: { host: "127.0.0.1", port: PORTS.hoopdb, database: HOOPDB_NAME, user: HOOPDB_USER, container: hoopdb.name },
      locker: { endpoint: `http://127.0.0.1:${PORTS.locker}`, bucket: LOCKER_BUCKET, container: locker.name },
      ingest: { url: `http://127.0.0.1:${PORTS.ingest}`, container: ingest.name },
      otel: { endpoint: `http://127.0.0.1:${PORTS.otelHttp}`, container: otel.name, telemetryVolume: telemetry.name },
    };
  }),
);
