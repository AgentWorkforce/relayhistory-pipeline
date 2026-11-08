/**
 * SERVICE=session-publication (docker/entrypoint.sh): the canonical publisher
 * (publication-main.ts, `--continuous`) as a long-running container with the
 * worker probe shape (src/shared/worker.ts serveHealth), so an orchestrator can
 * run it as a Deployment next to the other services. Default off.
 *
 *   INGEST_PUBLISH_ENABLED   unset or "false": dormant. Exactly "true": enabled. Anything else
 *                            fails startup (publication-runtime.ts publicationEnabled).
 *   WORKER_HEALTH_PORT       probe port, default 8081 (the workers' port), parsed even when dormant.
 *
 * Dormant: no config, credential, database or store work. The process stays up,
 * GET /healthz and GET /readyz answer 200 {"ok":true,"enabled":false}, and SIGTERM exits 0.
 * Enabled: the selected target's config (publicationConfigForTarget) is validated before the probe port opens, so a bad
 * or missing target fails startup (exit 1, the publication-main error line); then
 *   GET /healthz  200 {"ok":true,"enabled":true,"reason":"starting"|"ok"} while the last
 *                 cycle ended (or the service started) within the cycle budget
 *                 (INGEST_PUBLISH_BUDGET_MS) plus the idle wait and 60 s; 503
 *                 {"ok":false,"reason":"loop_stalled"} past it.
 *   GET /readyz   200 after the first healthy cycle; 503 {"ok":false,"reason":"starting"} before.
 * Any other path is 404 and any other method 405. A failed cycle rejects
 * publicationMain: the process exits 1 and the orchestrator restarts it, as
 * publication-main does on a host. SIGTERM/SIGINT stop the loop (publication-main
 * cancels owned IO) and exit 0. Publication GC (`--gc`) is not this service.
 *
 * The runtime is selected by INGEST_PUBLICATION_TARGET (publication-target.ts): unset
 * or "aws" is the existing host runtime (AWS/Supabase target, IMDSv2 credentials,
 * publication-runtime.ts publicationRuntimeConfig); "portable" is the self-host runtime
 * (publication-portable.ts). docs/selfhost-composition.md records the self-host stack.
 */
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { publicationEnabled } from "./publication-runtime.ts";
import { publicationConfigForTarget } from "./publication-portable.ts";
import { publicationMain } from "./publication-main.ts";

export const PUBLICATION_HEALTH_PORT_VAR = "WORKER_HEALTH_PORT";
/** publication-main's idle wait between empty cycles. */
const IDLE_WAIT_MS = 5_000;
const STALL_MARGIN_MS = 60_000;

export interface PublicationHealthState {
  enabled: boolean;
  /** Epoch ms of service start or the end of the last healthy cycle. */
  lastProgressAt: number;
  cycles: number;
}

export function publicationHealthPort(env: NodeJS.ProcessEnv): number {
  const raw = env[PUBLICATION_HEALTH_PORT_VAR]?.trim();
  if (!raw) return 8081;
  const value = Number(raw);
  if (!/^\d+$/.test(raw) || !Number.isInteger(value) || value < 1 || value > 65_535)
    throw Object.assign(new Error(`96bulls: ${PUBLICATION_HEALTH_PORT_VAR} must be a TCP port`), { code: "publication_health_port_invalid" });
  return value;
}

export function publicationLiveness(s: PublicationHealthState, now: number, maxAgeMs: number) {
  if (!s.enabled) return { ok: true, enabled: false };
  if (now - s.lastProgressAt > maxAgeMs) return { ok: false, enabled: true, reason: "loop_stalled" };
  return { ok: true, enabled: true, reason: s.cycles > 0 ? "ok" : "starting" };
}

export function publicationReadiness(s: PublicationHealthState) {
  if (!s.enabled) return { ok: true, enabled: false };
  return s.cycles > 0 ? { ok: true, enabled: true } : { ok: false, enabled: true, reason: "starting" };
}

export function publicationHealthServer(s: PublicationHealthState, maxAgeMs: number, now: () => number = Date.now): Server {
  return createServer((req, res) => {
    const path = (req.url ?? "").split("?")[0];
    if (path !== "/healthz" && path !== "/readyz") return void res.writeHead(404).end();
    if (req.method !== "GET" && req.method !== "HEAD") return void res.writeHead(405, { allow: "GET" }).end();
    const v = path === "/healthz" ? publicationLiveness(s, now(), maxAgeMs) : publicationReadiness(s);
    res.writeHead(v.ok ? 200 : 503, { "content-type": "application/json" });
    res.end(req.method === "HEAD" ? undefined : JSON.stringify(v));
  });
}

export interface PublicationServiceOptions {
  host?: string;
  port?: number;
  /** Replaces publicationMain (tests). */
  main?: typeof publicationMain;
  /** Aborting it stops the service as SIGTERM does (tests). */
  signal?: AbortSignal;
  now?: () => number;
  /** Called once the probe port is open. */
  onListening?: (port: number) => void;
}

/** Resolves when the service stops cleanly; rejects on a startup error or a failed cycle. */
export async function publicationService(
  args: string[] = process.argv.slice(2),
  env: NodeJS.ProcessEnv = process.env,
  options: PublicationServiceOptions = {},
): Promise<{ enabled: boolean; cycles: number }> {
  if (args.length !== 1 || args[0] !== "--continuous") throw Object.assign(new Error("publication_arguments"), { code: "publication_arguments" });
  const now = options.now ?? Date.now;
  const port = publicationHealthPort(env);
  const enabled = publicationEnabled(env, "publish");
  // Enabled: the target is validated before anything listens, so a misconfiguration is a startup failure.
  const maxAgeMs = enabled ? publicationConfigForTarget(env).budgetMs + IDLE_WAIT_MS + STALL_MARGIN_MS : 0;
  const state: PublicationHealthState = { enabled, lastProgressAt: now(), cycles: 0 };
  const stop = new AbortController();
  const onSignal = () => stop.abort();
  process.once("SIGTERM", onSignal);
  process.once("SIGINT", onSignal);
  if (options.signal) {
    if (options.signal.aborted) stop.abort();
    else options.signal.addEventListener("abort", onSignal, { once: true });
  }
  const server = publicationHealthServer(state, maxAgeMs, now);
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(options.port ?? port, options.host ?? "0.0.0.0", () => {
        server.off("error", reject);
        resolve();
      });
    });
    const bound = (server.address() as AddressInfo).port;
    console.log(JSON.stringify({ kind: "canonical-publication", mode: "publish", service: "session-publication", enabled, port: bound }));
    options.onListening?.(bound);
    if (!enabled) {
      await new Promise<void>((resolve) => (stop.signal.aborted ? resolve() : stop.signal.addEventListener("abort", () => resolve(), { once: true })));
      return { enabled: false, cycles: 0 };
    }
    try {
      await (options.main ?? publicationMain)(["--continuous"], env, {
        onCycle: () => {
          state.cycles++;
          state.lastProgressAt = now();
        },
        signal: stop.signal,
      });
    } catch (error) {
      // publication-main rejects with the abort reason when it is stopped: a requested stop is clean.
      if (!stop.signal.aborted) throw error;
    }
    return { enabled: true, cycles: state.cycles };
  } finally {
    stop.abort();
    process.removeListener("SIGTERM", onSignal);
    process.removeListener("SIGINT", onSignal);
    options.signal?.removeEventListener("abort", onSignal);
    // Probes are stateless: drop keep-alive probe connections so stop is not held open by them.
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
      server.closeAllConnections?.();
    });
  }
}

if (import.meta.main)
  publicationService().catch((error) => {
    const raw = error?.code;
    const code = typeof raw === "string" && /^[A-Za-z0-9_]{1,64}$/.test(raw) ? raw : "publication_failed";
    console.error(JSON.stringify({ kind: "canonical-publication", service: "session-publication", healthy: false, code }));
    process.exitCode = 1;
  });
