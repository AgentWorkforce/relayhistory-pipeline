/**
 * The S3 ledger re-arm sweeper (rearm-sweeper.ts): process entry for a one-shot job, a long-running loop (self-host pod,
 * `SERVICE=s3-ledger-rearm-sweeper`) and the hosted Lambda (rearm-sweeper-lambda.ts). DEFAULT OFF.
 *
 *   INGEST_REARM_SWEEPER_ENABLED     exactly "true" enables; unset or "false" is off (exits 0 with {"enabled":false}
 *                                    before reading any other setting or credential); anything else fails startup
 *   INGEST_REARM_BUCKET              the transcripts bucket (the S3 ledger and the canonical layout)
 *   INGEST_REARM_REGION              signing region
 *   INGEST_REARM_ENDPOINT            bare origin of an S3-compatible store (MinIO); unset = AWS S3
 *   INGEST_REARM_KEY_ROOT            object key root, default "96bulls" (= INGEST_KEY_ROOT)
 *   INGEST_REARM_ORGS                optional comma-separated org ids (org scope); unset = every org
 *   INGEST_REARM_DRY_RUN             "1": classify and count only (or `--dry-run`)
 *   INGEST_REARM_CONCURRENCY         markers at once, 1..16, default 4
 *   INGEST_REARM_MAX_MARKERS         markers needing work per pass, 1..10000, default 500
 *   INGEST_REARM_INTERVAL_MS         loop interval, 10000..86400000, default 600000 (or `--interval-ms <n>`)
 *   INGEST_REARM_CREDENTIALS         "role" (default): the execution role's session keys from the environment
 *                                    (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN, as Lambda and ECS
 *                                    inject them); "keys": INGEST_REARM_S3_ACCESS_KEY_ID[_FILE] and
 *                                    INGEST_REARM_S3_SECRET_ACCESS_KEY[_FILE] (a mounted secret; re-read every pass);
 *                                    "podIdentity": EKS Pod Identity, through the SAME helper as every Locker client
 *                                    (src/shared/locker.ts podIdentityCredentials: AWS_CONTAINER_CREDENTIALS_FULL_URI,
 *                                    trusted endpoints only, and AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE). The store
 *                                    refreshes them; the eraser takes them per pass, and a pass ends 5 min before they
 *                                    expire. IRSA (web identity) is not supported: the shared helper has no such mode.
 *
 * `--loop` runs a pass every interval until SIGTERM / SIGINT (the current pass finishes first); an incomplete pass's
 * next one continues where it stopped (the durable cursor in the bucket; a dry run keeps it in memory). Without it: one pass, exit 0 when complete (or dry run / disabled), 3 when
 * incomplete or any marker failed (rerun), 2 refused / misconfigured, 4 any other failure. One JSON line per pass, counts
 * and codes only.
 */
import { initScrubEngine } from "../../packages/pipeline/src/lib/scrub-engine/index.ts";
import { readFileSync, statSync } from "node:fs";
import { isAbsolute } from "node:path";
import { createS3Store } from "../../packages/object-store/src/index.ts";
import { createS3Eraser, type PagedEraser, type S3EraserConfig } from "../migration/neon-s3-erasure/eraser.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import type { ObjectStore } from "../shared/vendor.ts";
import { lambdaCredentials } from "../session-pipeline/aws-json.ts";
import { podIdentityCredentials, trustedCredentialsUri } from "../shared/locker.ts";
import type { AwsCredentials } from "../../packages/object-store/src/sigv4.ts";
import { readRearmCursor, rearmCursorKey, rearmScopes, RearmSweepRefused, REARM_MAX_CONCURRENCY, sweepRearmMarkers, writeRearmCursor, type RearmSweepOptions, type RearmSweepResult, type RearmSweepResume } from "./rearm-sweeper.ts";

export const REARM_ENABLE_FLAG = "INGEST_REARM_SWEEPER_ENABLED";
const refuse = (code: string): never => { throw new RearmSweepRefused(code); };

/** Exactly "true" enables; unset, "" or "false" is off; anything else refuses (a typo never means either). */
export function rearmSweeperEnabled(env: NodeJS.ProcessEnv): boolean {
  const raw = env[REARM_ENABLE_FLAG];
  if (raw === undefined || raw === "" || raw === "false") return false;
  if (raw === "true") return true;
  return refuse("flag_invalid");
}

export interface RearmSweeperConfig {
  bucket: string;
  region: string;
  endpoint?: string;
  keyRoot: string;
  orgs?: string[];
  dryRun: boolean;
  concurrency: number;
  maxMarkers: number;
  intervalMs: number;
  credentials: RearmCredentialMode;
}
export type RearmCredentialMode = "role" | "keys" | "podIdentity";

const int = (raw: string | undefined, dflt: number, min: number, max: number): number => {
  const n = Number(raw === undefined || raw === "" ? dflt : raw);
  if (!Number.isInteger(n) || n < min || n > max) refuse("config_invalid");
  return n;
};

export function rearmSweeperConfig(env: NodeJS.ProcessEnv, argv: readonly string[] = []): RearmSweeperConfig {
  const bucket = env.INGEST_REARM_BUCKET ?? "", region = env.INGEST_REARM_REGION ?? "";
  if (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket) || !/^[a-z0-9-]{1,63}$/.test(region)) refuse("config_invalid");
  let endpoint: string | undefined;
  if (env.INGEST_REARM_ENDPOINT) {
    try { endpoint = assertDestinationUrl("INGEST_REARM_ENDPOINT", env.INGEST_REARM_ENDPOINT, destinationPolicy(env), { allowPath: false }).origin; }
    catch { refuse("endpoint_invalid"); }
  }
  const keyRoot = env.INGEST_REARM_KEY_ROOT?.trim() || "96bulls";
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(keyRoot)) refuse("config_invalid");
  let orgs: string[] | undefined;
  if (env.INGEST_REARM_ORGS !== undefined && env.INGEST_REARM_ORGS !== "") {
    orgs = env.INGEST_REARM_ORGS.split(",").map((o) => o.trim());
    if (orgs.some((o) => !/^[A-Za-z0-9._-]{1,128}$/.test(o) || o.startsWith("."))) refuse("config_invalid");
  }
  const dry = env.INGEST_REARM_DRY_RUN;
  if (dry !== undefined && dry !== "" && dry !== "0" && dry !== "1") refuse("config_invalid");
  const at = argv.indexOf("--interval-ms");
  if (at >= 0 && (!argv[at + 1] || argv[at + 1]!.startsWith("--"))) refuse("config_invalid");
  const credentials = env.INGEST_REARM_CREDENTIALS || "role";
  if (credentials !== "role" && credentials !== "keys" && credentials !== "podIdentity") refuse("config_invalid");
  return {
    bucket, region, ...(endpoint ? { endpoint } : {}), keyRoot, ...(orgs ? { orgs } : {}),
    dryRun: dry === "1" || argv.includes("--dry-run"),
    concurrency: int(env.INGEST_REARM_CONCURRENCY, 4, 1, REARM_MAX_CONCURRENCY),
    maxMarkers: int(env.INGEST_REARM_MAX_MARKERS, 500, 1, 10_000),
    intervalMs: int(at >= 0 ? argv[at + 1] : env.INGEST_REARM_INTERVAL_MS, 600_000, 10_000, 86_400_000),
    credentials: credentials as RearmCredentialMode,
  };
}

function secret(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name], file = env[`${name}_FILE`];
  if (value !== undefined && file !== undefined) refuse("secret_source_ambiguous");
  let text = value ?? "";
  if (file !== undefined) {
    if (!isAbsolute(file)) refuse("secret_invalid");
    try {
      const st = statSync(file);
      if (!st.isFile() || st.size < 1 || st.size > 65536) refuse("secret_invalid");
      text = readFileSync(file, "utf8").replace(/\r?\n$/, "");
    } catch (e) { if (e instanceof RearmSweepRefused) throw e; refuse("secret_invalid"); }
  }
  // eslint-disable-next-line no-control-regex
  if (!text || /[\u0000-\u001f\u007f\s]/.test(text)) refuse("secret_invalid");
  return text;
}

/** Time a pass keeps before its credentials expire (the eraser holds one snapshot for the whole pass). */
export const REARM_CREDENTIAL_MARGIN_MS = 5 * 60_000;

/**
 * The pass's credentials. Never logged, never written. `provider` is what the store uses (Pod Identity: refreshed by
 * the store's cache before expiry); `snapshot` is what the eraser holds for the pass. A missing or untrusted Pod Identity
 * endpoint refuses (exit 2); an endpoint that answers an error throws a retryable failure (exit 4, the loop retries).
 */
export async function rearmSweeperCredentials(env: NodeJS.ProcessEnv, cfg: Pick<RearmSweeperConfig, "credentials">, fetcher: typeof fetch = fetch):
  Promise<{ snapshot: AwsCredentials; provider: () => Promise<AwsCredentials> }> {
  if (cfg.credentials === "podIdentity") {
    const uri = env.AWS_CONTAINER_CREDENTIALS_FULL_URI;
    if (!uri) refuse("credentials_missing");
    if (!trustedCredentialsUri(uri!)) refuse("credentials_endpoint_untrusted");
    const provider = () => podIdentityCredentials(env, fetcher);
    const snapshot = await provider();
    if (!snapshot.accessKeyId || !snapshot.secretAccessKey) refuse("credentials_missing");
    return { snapshot, provider };
  }
  let snapshot: AwsCredentials;
  if (cfg.credentials === "role") {
    try { snapshot = lambdaCredentials(env); } catch { return refuse("credentials_missing"); }
  } else snapshot = { accessKeyId: secret(env, "INGEST_REARM_S3_ACCESS_KEY_ID"), secretAccessKey: secret(env, "INGEST_REARM_S3_SECRET_ACCESS_KEY") };
  return { snapshot, provider: async () => snapshot };
}

export interface RearmSweeperOverrides {
  /** Test-only ports; the process entries never take them from input. */
  store?: (config: Parameters<typeof createS3Store>[0]) => ObjectStore;
  eraser?: (config: S3EraserConfig) => PagedEraser;
  now?: () => number;
  /** The Pod Identity endpoint's fetch (tests point it at a fake container endpoint). */
  fetcher?: typeof fetch;
}

/** `cursor`: "saved" (the position is in the bucket), "contended" (another sweeper saved first) or "memory" (dry run: never written). */
export type RearmPassOutput = { enabled: false } | ({ enabled: true; cursor: "saved" | "contended" | "memory" } & Omit<RearmSweepResult, "resume">);

/**
 * One pass with fresh credentials. With the flag off nothing else is read and no client is built. A real pass starts
 * where the durable cursor (rearm-sweeper.ts rearmCursorKey) says and saves where it stopped (start over once complete),
 * so successive invocations walk the namespace and wrap around. A dry run writes nothing: it uses `resume` (in memory).
 */
export async function runRearmSweeperPass(env: NodeJS.ProcessEnv, argv: readonly string[], overrides: RearmSweeperOverrides = {},
  resume: RearmSweepResume | null = null, extra: Pick<RearmSweepOptions, "deadlineAt"> = {}): Promise<{ output: RearmPassOutput; resume: RearmSweepResume | null }> {
  if (!rearmSweeperEnabled(env)) return { output: { enabled: false }, resume: null };
  const cfg = rearmSweeperConfig(env, argv);
  const { snapshot, provider } = await rearmSweeperCredentials(env, cfg, overrides.fetcher);
  const s3 = { bucket: cfg.bucket, region: cfg.region, ...(cfg.endpoint ? { endpoint: cfg.endpoint } : {}), credentials: snapshot };
  const store = (overrides.store ?? createS3Store)({ ...s3, credentials: provider }) as unknown as ObjectStore;
  const eraser = (overrides.eraser ?? createS3Eraser)(s3);
  const now = overrides.now ?? Date.now;
  // Expiring credentials (Pod Identity): the eraser's snapshot must outlive the pass.
  if (snapshot.expiresAt !== undefined) {
    const until = snapshot.expiresAt.getTime() - REARM_CREDENTIAL_MARGIN_MS;
    if (!(until > now())) refuse("credentials_expiring");
    extra = { ...extra, deadlineAt: Math.min(extra.deadlineAt ?? Infinity, until) };
  }
  const cursorKey = rearmCursorKey(cfg.keyRoot, rearmScopes(cfg.keyRoot, cfg.orgs ?? null));
  const saved = cfg.dryRun ? null : await readRearmCursor(store, cursorKey);
  // Saved after every finished marker whose erase ran and at every page end (onProgress), then once at the end; each
  // save is If-Match the previous one. Once another sweeper wins a save, this pass stops saving (its position stands).
  let version = saved?.version, contended = false;
  const persist = async (position: RearmSweepResume | null) => {
    if (contended) return;
    const v = await writeRearmCursor(store, cursorKey, position, version, now());
    if (v === null) contended = true; else version = v;
  };
  const r = await sweepRearmMarkers({ store, eraser }, {
    keyRoot: cfg.keyRoot, bucket: cfg.bucket, dryRun: cfg.dryRun, concurrency: cfg.concurrency, maxMarkers: cfg.maxMarkers,
    ...(cfg.orgs ? { orgs: cfg.orgs } : {}), now, ...extra, ...(saved ? { onProgress: persist } : {}),
  }, saved ? saved.resume : resume);
  const { resume: next, ...output } = r;
  if (saved) await persist(next);
  const cursor = saved === null ? "memory" as const : contended ? "contended" as const : "saved" as const;
  return { output: { enabled: true, cursor, ...output }, resume: next };
}

/** 0: complete (or disabled, or dry run) with no failed marker; 3: rerun (incomplete, or a marker failed). */
export function exitCodeForPass(o: RearmPassOutput): 0 | 3 {
  return !o.enabled || (o.complete && o.counts.failed === 0) ? 0 : 3;
}
export function exitCodeForError(e: unknown): 2 | 4 {
  return e instanceof RearmSweepRefused ? 2 : 4;
}
const errorLine = (e: unknown) => ({ event: "rearm_sweep", error: e instanceof RearmSweepRefused ? e.message : "rearm_sweep_failed" });

/** `--loop`: a pass every interval until `stopped()`, continuing an incomplete pass. A failed pass is logged and retried. */
export async function runRearmSweeperLoop(env: NodeJS.ProcessEnv, argv: readonly string[], log: (line: unknown) => void, stopped: () => boolean,
  sleep: (ms: number) => Promise<void>, overrides: RearmSweeperOverrides = {}): Promise<void> {
  if (!rearmSweeperEnabled(env)) { log({ enabled: false }); return; }
  const { intervalMs } = rearmSweeperConfig(env, argv); // refuses a bad config before the first pass
  let resume: RearmSweepResume | null = null;
  while (!stopped()) {
    try {
      const r = await runRearmSweeperPass(env, argv, overrides, resume);
      resume = r.resume;
      log({ event: "rearm_sweep", ...r.output });
    } catch (e) {
      if (e instanceof RearmSweepRefused) throw e;
      log(errorLine(e));
    }
    if (stopped()) break;
    await sleep(intervalMs);
  }
}

if (import.meta.main) {
  const argv = process.argv.slice(2);
  if (argv.includes("--loop")) {
    let stop = false, wake: (() => void) | undefined;
    const halt = () => { stop = true; wake?.(); };
    process.on("SIGTERM", halt);
    process.on("SIGINT", halt);
    const sleep = (ms: number) => new Promise<void>((r) => { const t = setTimeout(r, ms); wake = () => { clearTimeout(t); r(); }; });
    // Scrub engine (docs/scrub-engine.md, "Default flip"): the pinned wasm-bindgen engine, read from the repo checkout.
    initScrubEngine().then(() => runRearmSweeperLoop(process.env, argv, (l) => console.log(JSON.stringify(l)), () => stop, sleep))
      .then(() => process.exit(0))
      .catch((e) => { console.error(JSON.stringify(errorLine(e))); process.exit(exitCodeForError(e)); });
  } else {
    initScrubEngine().then(() => runRearmSweeperPass(process.env, argv))
      .then(({ output }) => { console.log(JSON.stringify({ event: "rearm_sweep", ...output })); process.exit(exitCodeForPass(output)); })
      .catch((e) => { console.error(JSON.stringify(errorLine(e))); process.exit(exitCodeForError(e)); });
  }
}
