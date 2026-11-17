/**
 * SERVICE=coordination-advisor (docs/coordination-live-advisories.md). Default off: unless
 * COORDINATION_ADVISOR_ENABLED is exactly "true" the process starts, logs that it is disabled, and
 * idles without opening HoopDb, ClickHouse or the relay: no reads and no writes.
 *
 * Enabled, it runs on runWorker (live gate, health, shutdown) with no stages and one side tick: the
 * scan in scan.ts every COORDINATION_ADVISOR_INTERVAL_SECONDS. Facts come from ClickHouse
 * (COORDINATION_ADVISOR_FACTS_BACKEND=clickhouse) or from HoopDb through the 20261105062070
 * service_role readers (postgres). Cooldown and cap state is held in memory and seeded from
 * coordination_advisories_recent_v1 (062070) before each workspace writes, so a restart keeps both
 * once 062070 is applied; without it a restart resets them. The upsert never re-inserts an existing
 * row or touches an acked one, so a restart never repeats a DM.
 */
import { parseSecret } from "../session-pipeline/lambda.ts";
import { createClickHouseClient } from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { clickhouseCoordinationBackend, type CoordinationBackend } from "../session-read-api/coordination-backend.ts";
import { sqlLiveness, type SqlQuery, type TenantRunner } from "../session-read-api/liveness.ts";
import { ADVISOR_COORDINATION_SQL, postgresCoordinationBackend } from "../session-read-api/postgres/coordination-query.ts";
import { log } from "../shared/log.ts";
import { runWorker, type WorkerOptions } from "../shared/worker.ts";
import { advisorConfig, advisorEnabled, COORDINATION_ADVISOR_ENABLED, type AdvisorConfig } from "./config.ts";
import { httpRelaySender, type RelaySender } from "./relay-sender.ts";
import { AdvisorLimits } from "./rubric.ts";
import { advisorTick, sqlAdvisorStore } from "./scan.ts";

export const ADVISOR_SERVICE = "coordination-advisor";

export interface AdvisorIo {
  runWorker(opts: WorkerOptions): Promise<unknown>;
  /** Keeps a disabled process alive until SIGTERM/SIGINT. */
  idle(): Promise<void>;
  /** Validates the backend's configuration now (refusing to start) and returns its per-HoopDb factory. */
  backend(cfg: AdvisorConfig, env: Record<string, string | undefined>): (db: SqlQuery) => CoordinationBackend;
  sender(cfg: AdvisorConfig): RelaySender | undefined;
}

export async function advisorMain(env: Record<string, string | undefined>, io: AdvisorIo): Promise<"disabled" | "ran"> {
  if (!advisorEnabled(env)) {
    log.info(`coordination advisor disabled (${COORDINATION_ADVISOR_ENABLED} is not "true"); no reads or writes`, { service: ADVISOR_SERVICE, state: "disabled" });
    await io.idle();
    return "disabled";
  }
  // Every refusal (malformed value, DM on without its token or URL, an unavailable backend) happens here, before HoopDb opens.
  const cfg = advisorConfig(env);
  const sender = io.sender(cfg);
  const backendFor = io.backend(cfg, env);
  const limits = new AdvisorLimits(cfg);
  // The allowlist size only, never its ids. Empty: the worker runs and scans nothing.
  log.info(`coordination advisor enabled; workspaces=${cfg.workspaces === "*" ? "all" : cfg.workspaces.size}`,
    { service: ADVISOR_SERVICE, state: cfg.relayDm.enabled ? "dm_on" : "dm_off", kind: cfg.factsBackend });
  await io.runWorker({
    service: ADVISOR_SERVICE,
    stages: [],
    handlers: () => ({}),
    tick: advisorTick((db) => ({ store: sqlAdvisorStore(db), backend: backendFor(db), limits, sender, workspaces: cfg.workspaces }), { intervalMs: cfg.intervalMs }),
  });
  return "ran";
}

/**
 * The postgres facts backend over the 062070 service_role readers, on the worker's own HoopDb
 * connection: the org is an explicit argument, so no read-API tenant settings. The scan never asks
 * for window readiness, and the read-API readiness functions are not granted to service_role.
 */
export function postgresAdvisorBackend(db: SqlQuery): CoordinationBackend {
  const run: TenantRunner = (_o, _w, work) => work(db);
  return {
    ...postgresCoordinationBackend({ run, liveness: sqlLiveness(run) }, ADVISOR_COORDINATION_SQL),
    windowReadiness: () => Promise.reject(new Error("coordination advisor: window readiness is not used by the worker")),
  };
}

export const productionIo: AdvisorIo = {
  runWorker,
  idle: () => new Promise<void>((resolve) => {
    const keep = setInterval(() => {}, 1 << 30);
    const stop = () => { clearInterval(keep); resolve(); };
    process.once("SIGTERM", stop);
    process.once("SIGINT", stop);
  }),
  backend(cfg, env) {
    if (cfg.factsBackend === "postgres") return postgresAdvisorBackend;
    const raw = env.SESSION_PIPELINE_SECRET_JSON;
    if (!raw) throw new Error("coordination advisor: the clickhouse facts backend needs SESSION_PIPELINE_SECRET_JSON");
    const secret = parseSecret(raw);
    if (!secret.clickhouse) throw new Error("coordination advisor: SESSION_PIPELINE_SECRET_JSON has no clickhouse");
    const ch = createClickHouseClient(secret.clickhouse);
    // The worker runs as service_role, which reads session_index directly: no read-API tenant settings.
    return (db) => clickhouseCoordinationBackend(ch, sqlLiveness((_o, _w, work) => work(db)));
  },
  sender: (cfg) => cfg.relayDm.enabled ? httpRelaySender({ url: cfg.relayDm.url, token: cfg.relayDm.token }) : undefined,
};

if (import.meta.main) await advisorMain(process.env, productionIo);
