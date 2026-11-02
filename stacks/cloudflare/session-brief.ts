/**
 * The model-written session briefs for sessions-prod: one Cloudflare Worker,
 * `96bulls-session-brief`, with a cron trigger and no way in. Each invocation runs
 * one bounded tick of the brief engine (src/session-brief/cloudflare.ts, which
 * reuses src/session-brief/worker.ts briefTick unchanged).
 *
 *   cron "* * * * *" --> 96bulls-session-brief (no route, no URL, no domain)
 *       --> Supabase via HYPERDRIVE (bulls96_runtime, Supavisor session pooler, caching off)
 *       --> S3 transcripts, read-only (transcript-reader key: ListBucket, GetObject 96bulls/*\/v1/*)
 *       --> OpenAI Responses API (OPENAI_API_KEY)
 *
 * Fed by the #121 session pipeline Lambda, which arms one wakeup per indexed
 * revision once its env has SESSION_BRIEF_ENABLED="true" (stacks/aws/session-pipeline.ts).
 * Deploy this first: wakeups armed with no Worker only wait (and nothing prunes them).
 *
 * Security invariant (assertPrivateBriefWorker, tested and mutation-checked): no
 * route, no custom domain, no workers.dev URL, no preview URLs. The cron is the
 * only trigger; the entry exports `scheduled` and nothing else.
 *
 * Credentials. Alchemy never holds one (its state is plaintext):
 *   - Supabase: a Hyperdrive config the lead creates out of band (the bulls96_runtime
 *     password lives only in Cloudflare); this stack looks it up by id.
 *   - OpenAI and the transcript-reader S3 key: the account's Secrets Store (created
 *     out of band from 1Password), bound by store id and secret name. Not
 *     `wrangler secret put`: this Alchemy version uploads every Worker version
 *     without `keep_bindings`, so a secret set on the script out of band would be
 *     dropped by the next deploy. A Secrets Store binding is part of the declared
 *     bindings and carries no value; at runtime it is `await env.OPENAI_API_KEY.get()`.
 *
 * Pattern: #154's cf-sessions-prod Alchemy stage (stacks/cloudflare/session-ingest.ts),
 * in its own config (alchemy.session-brief.run.ts) so neither change waits on the other.
 */
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import { briefConfig } from "../../src/session-brief/config.ts";

export const SESSION_BRIEF_WORKER = "96bulls-session-brief";
export const SESSION_BRIEF_STAGE = "cf-sessions-prod";
/** Pinned: a later date changes runtime behaviour, so it moves only in a reviewed update. */
export const COMPATIBILITY_DATE = "2026-10-01";
/** pg over Hyperdrive (cloudflare:sockets), node:zlib zstd, node:crypto. */
export const COMPATIBILITY_FLAGS = ["nodejs_compat"] as const;
/** Every minute: the wakeup debounce is 10 minutes, so a brief lands at most a minute after it is due. */
export const SESSION_BRIEF_CRONS = ["* * * * *"] as const;
/**
 * CPU per invocation. A tick briefs at most SESSION_BRIEF_BATCH (5) sessions of at most
 * WORKER_MAX_SESSION_BYTES raw each: zstd and JSON parsing is the CPU, the model call is
 * wall time. Twice the cron default (30 s), so a batch of large sessions is never cut off.
 */
export const SESSION_BRIEF_CPU_MS = 60_000;
/**
 * Raw bytes one session may read in the Worker. An isolate has 128 MB; a session's text is
 * held while its events are built (up to about twice the raw bytes as JS strings, plus one
 * chunk's decompressed copy). A larger session is recorded `session_too_large`, never read.
 */
export const WORKER_MAX_SESSION_BYTES = 16 * 1024 * 1024;
export const TRANSCRIPT_BUCKET = "96bulls-aws-sessions-prod-transcripts-941433935742";
export const TRANSCRIPT_REGION = "us-east-1";
/** Secrets Store secret names (values created out of band from 1Password). */
export const OPENAI_KEY_SECRET = "96bulls-session-brief-openai-api-key";
export const S3_ACCESS_KEY_SECRET = "96bulls-session-brief-s3-access-key-id";
export const S3_SECRET_KEY_SECRET = "96bulls-session-brief-s3-secret-access-key";

/** Plan resources. The Hyperdrive lookup and the Secrets Store bindings have no state row. */
export const SESSION_BRIEF_RESOURCES = ["session-brief"] as const;

export interface SessionBriefInputs {
  /** The Cloudflare account the Worker, its Hyperdrive config and its Secrets Store live in. */
  accountId: string;
  /** The Hyperdrive config the lead created (Supabase session pooler, bulls96_runtime, caching off). */
  hyperdriveId: string;
  /** The account Secrets Store holding the OpenAI key and the transcript-reader key. */
  secretsStoreId: string;
  /** false: deploy with SESSION_BRIEF_ENABLED="false" (the kill switch, an update). Default true. */
  enabled?: boolean;
}

const HEX32 = /^[0-9a-f]{32}$/;
const KEYS = ["accountId", "hyperdriveId", "secretsStoreId", "enabled"];

export function assertSessionBriefInputs(p: unknown, refuse: (why: string) => never): asserts p is SessionBriefInputs {
  if (p === null || typeof p !== "object" || Array.isArray(p) || Object.getPrototypeOf(p) !== Object.prototype) refuse("sessionBrief inputs must be a plain object");
  const v = p as Record<string, unknown>;
  for (const k of Object.keys(v)) if (!KEYS.includes(k)) refuse(`sessionBrief.${k} is not a known setting`);
  for (const k of ["accountId", "hyperdriveId", "secretsStoreId"] as const)
    if (typeof v[k] !== "string" || !HEX32.test(v[k] as string)) refuse(`sessionBrief.${k} must be a 32-character lowercase hex id`);
  if (v.enabled !== undefined && typeof v.enabled !== "boolean") refuse("sessionBrief.enabled must be true or false");
}

/** A Secrets Store secret this stack does not own: bound by store and name, never a value. */
export function secretsStoreSecret(storeId: string, secretName: string) {
  return { Type: "Cloudflare.SecretsStore.Secret" as const, LogicalId: `external:${secretName}`, storeId, secretName };
}

/**
 * Plain vars: ids, switches and the brief limits. Every SESSION_BRIEF_* limit is the
 * code default (src/session-brief/config.ts; the global cap is 20000/day, Khaliq
 * 2026-11-02) except the session byte bound, which the Worker's memory sets.
 * Written out so the reviewed plan shows them; a test pins them to briefConfig().
 */
export function sessionBriefVars(p: SessionBriefInputs) {
  const d = briefConfig({ SESSION_BRIEF_ENABLED: "true" });
  return {
    SESSION_BRIEF_ENABLED: p.enabled === false ? "false" : "true",
    SESSION_BRIEF_GLOBAL_DAILY_CAP: String(d.globalDailyCap),
    SESSION_BRIEF_BACKGROUND_DAILY_BUDGET: String(d.backgroundDailyBudget),
    SESSION_BRIEF_SESSION_DAILY_CAP: String(d.sessionDailyCap),
    SESSION_BRIEF_THROTTLE_SECONDS: String(d.throttleSeconds),
    SESSION_BRIEF_MAX_ATTEMPTS: String(d.maxAttempts),
    SESSION_BRIEF_BATCH: String(d.batch),
    SESSION_BRIEF_LEASE_SECONDS: String(d.leaseSeconds),
    SESSION_BRIEF_SWEEP_SECONDS: String(d.sweepSeconds),
    SESSION_BRIEF_MAX_SESSION_BYTES: String(WORKER_MAX_SESSION_BYTES),
    LOCKER_BUCKET: TRANSCRIPT_BUCKET,
    LOCKER_REGION: TRANSCRIPT_REGION,
    OTEL_SERVICE_NAME: "96bulls-session-brief",
    DEPLOYMENT: SESSION_BRIEF_STAGE,
    DEPLOYMENT_ID: SESSION_BRIEF_STAGE,
  };
}

/** The Worker's props. `HYPERDRIVE` is supplied by the stage, so the invariants are checked on exactly what is deployed. */
export function sessionBriefWorkerProps(p: SessionBriefInputs, bindings: { HYPERDRIVE: unknown }) {
  return {
    name: SESSION_BRIEF_WORKER,
    main: "src/session-brief/cloudflare.ts",
    compatibility: { date: COMPATIBILITY_DATE, flags: [...COMPATIBILITY_FLAGS] },
    crons: [...SESSION_BRIEF_CRONS],
    limits: { cpuMs: SESSION_BRIEF_CPU_MS },
    // Workers Logs: the tick's allowlisted JSON lines (ids, codes, counts) and invocation status.
    observability: { enabled: true },
    // No way in: the cron is the only trigger (assertPrivateBriefWorker).
    workersDev: false as const,
    routes: [] as never[],
    // null (not omitted): Alchemy then detaches any custom domain attached out of band.
    domain: null,
    env: {
      ...sessionBriefVars(p),
      HYPERDRIVE: bindings.HYPERDRIVE,
      OPENAI_API_KEY: secretsStoreSecret(p.secretsStoreId, OPENAI_KEY_SECRET),
      LOCKER_ACCESS_KEY_ID: secretsStoreSecret(p.secretsStoreId, S3_ACCESS_KEY_SECRET),
      LOCKER_SECRET_ACCESS_KEY: secretsStoreSecret(p.secretsStoreId, S3_SECRET_KEY_SECRET),
    },
  };
}

export type SessionBriefWorkerProps = ReturnType<typeof sessionBriefWorkerProps>;

/**
 * No public surface (no workers.dev or preview URL, no route, no custom domain, no
 * preview/version parent, no Access app, no assets), exactly the reviewed cron, and the
 * runtime it needs (nodejs_compat at the pinned date, a CPU limit).
 */
export function assertPrivateBriefWorker(props: Record<string, unknown>, refuse: (why: string) => never): void {
  if (props.name !== SESSION_BRIEF_WORKER) refuse(`the brief Worker must be named ${SESSION_BRIEF_WORKER}`);
  if (props.main !== "src/session-brief/cloudflare.ts") refuse("the brief Worker's entry must be src/session-brief/cloudflare.ts (scheduled only)");
  if (props.workersDev !== false) refuse("workers.dev must be disabled (workersDev: false): the brief Worker has no way in");
  if (!Array.isArray(props.routes) || props.routes.length !== 0) refuse("the brief Worker must have no routes");
  if (props.domain !== null) refuse("the brief Worker must have no custom domain (domain: null detaches any)");
  for (const k of ["url", "preview", "version", "access", "namespace", "assets"])
    if (props[k] !== undefined) refuse(`the brief Worker must not set ${k}`);
  const crons = props.crons;
  if (!Array.isArray(crons) || crons.length !== SESSION_BRIEF_CRONS.length || crons.some((c, i) => c !== SESSION_BRIEF_CRONS[i]))
    refuse(`the brief Worker's crons must be exactly ${JSON.stringify(SESSION_BRIEF_CRONS)}`);
  const compat = props.compatibility as { date?: unknown; flags?: unknown } | undefined;
  if (compat?.date !== COMPATIBILITY_DATE) refuse(`the compatibility date must be pinned to ${COMPATIBILITY_DATE}`);
  if (!Array.isArray(compat?.flags) || !compat.flags.includes("nodejs_compat") || compat.flags.includes("no_nodejs_compat"))
    refuse("the brief Worker needs nodejs_compat (pg over cloudflare:sockets, node:zlib zstd, node:crypto)");
  if ((props.limits as { cpuMs?: unknown } | undefined)?.cpuMs !== SESSION_BRIEF_CPU_MS) refuse(`the brief Worker's CPU limit must be ${SESSION_BRIEF_CPU_MS} ms`);
}

/** Names whose values would be credentials: they must be bindings, never literals Alchemy state would hold. */
const SECRET_NAME = /SECRET|PASSWORD|TOKEN|ACCESS_KEY|API_KEY|PRIVATE|CREDENTIAL|DATABASE_URL|CONNECTION|HOOPDB_URL/i;

/**
 * No credential is a literal in the Worker's env (a literal would be uploaded from, and
 * recorded in, Alchemy state). Credential-named entries must be Secrets Store descriptors;
 * every other plain value is a string with no URL userinfo; the database is reached only
 * through the Hyperdrive binding.
 */
export function assertNoSecretLiterals(env: Record<string, unknown>, refuse: (why: string) => never): void {
  for (const [k, v] of Object.entries(env)) {
    const isSecretBinding = typeof v === "object" && v !== null && (v as { Type?: unknown }).Type === "Cloudflare.SecretsStore.Secret";
    if (SECRET_NAME.test(k) && !isSecretBinding) refuse(`env ${k} must be a Secrets Store binding, never a value`);
    if (typeof v === "string" && /:\/\/[^/\s]*@/.test(v)) refuse(`env ${k} carries URL credentials`);
    if (typeof v === "string" && /^(?:sk-|AKIA[0-9A-Z]{16})/.test(v)) refuse(`env ${k} looks like a credential`);
    if (typeof v === "object" && v !== null && "value" in v) refuse(`env ${k} carries a value`);
  }
  for (const k of ["OPENAI_API_KEY", "LOCKER_ACCESS_KEY_ID", "LOCKER_SECRET_ACCESS_KEY"]) if (!(k in env)) refuse(`env ${k} is missing (Secrets Store binding)`);
  if (!Cloudflare.Hyperdrive.isHyperdriveConnection(env.HYPERDRIVE)) refuse("env HYPERDRIVE must be the Hyperdrive lookup (the only database path)");
}

export type SessionBriefPlanPhase = "session brief rollout" | "session brief update" | "post-apply";

/**
 * The deploy plan contract (same `Plan:` summary and `[resource] action` line format as
 * the AWS stages and #154): exactly the brief's one resource; rollout creates it, update
 * changes it in place, post-apply changes nothing. Never a replacement or deletion. The
 * detailed plan must list the same action and must not show a credential.
 */
export function assertSessionBriefPlanContract(planText: string, detailedPlanText: string): SessionBriefPlanPhase {
  const refuse = (why: string): never => {
    throw new Error(`96bulls: ${SESSION_BRIEF_STAGE} session brief plan contract refused: ${why}. Nothing was changed.`);
  };
  const parse = (text: string, label: string) => {
    const summaries = text.split(/\r?\n/).filter((l) => l.startsWith("Plan:"));
    if (summaries.length !== 1) refuse(`${label} plan: expected exactly one Plan: summary, found ${summaries.length}`);
    const ops = new Map<string, string>();
    for (const line of text.split(/\r?\n/)) {
      const m = /^\s*\[([^\]]+)\]\s+(\S+)\s*$/.exec(line);
      if (!m) continue;
      if (ops.has(m[1]!)) refuse(`${label} plan: resource ${m[1]} appears more than once`);
      ops.set(m[1]!, m[2]!);
    }
    for (const r of ops.keys()) if (!(SESSION_BRIEF_RESOURCES as readonly string[]).includes(r)) refuse(`${label} plan: unexpected resource ${r}`);
    for (const r of SESSION_BRIEF_RESOURCES) if (!ops.has(r)) refuse(`${label} plan: missing resource ${r}`);
    for (const [r, a] of ops) if (a !== "create" && a !== "update" && a !== "noop") refuse(`${label} plan: ${r} has forbidden action ${a}`);
    const counts = new Map<string, number>();
    for (const a of ops.values()) if (a !== "noop") counts.set(a, (counts.get(a) ?? 0) + 1);
    const summary = summaries[0]!;
    const clauses = [...summary.matchAll(/(\d+) to ([a-z-]+)/g)].map((m) => [m[2]!, Number(m[1])] as const);
    if (clauses.length === 0) {
      if (!/^Plan:\s*(?:0 changes|no changes)\s*$/i.test(summary) || counts.size) refuse(`${label} plan: summary does not match the resource lines`);
    } else {
      const said = new Map(clauses);
      if (said.size !== clauses.length || said.size !== counts.size) refuse(`${label} plan: summary does not match the resource lines`);
      for (const [a, n] of counts) if (said.get(a) !== n) refuse(`${label} plan: summary says ${said.get(a) ?? 0} ${a} but resource lines contain ${n}`);
    }
    return { summary, ops };
  };
  const compact = parse(planText, "compact");
  const detailed = parse(detailedPlanText, "detailed");
  if (compact.summary !== detailed.summary) refuse("compact and detailed plan summaries differ");
  for (const r of SESSION_BRIEF_RESOURCES) if (compact.ops.get(r) !== detailed.ops.get(r)) refuse(`compact and detailed plans disagree on ${r}`);
  // A credential in the planned props would be one Alchemy state now holds.
  if (/secret_text|AKIA[0-9A-Z]{16}|\bsk-(?:proj-)?[A-Za-z0-9_-]{16,}|-----BEGIN [A-Z ]*PRIVATE KEY|postgres(?:ql)?:\/\/[^\s"]*:[^\s"@]*@/.test(detailedPlanText))
    refuse("the detailed plan shows a credential value or a secret_text binding");
  const action = compact.ops.get("session-brief");
  if (action === "noop") return "post-apply";
  if (action === "create") return "session brief rollout";
  return "session brief update";
}

/** Declares the brief Worker (its one resource) with its cron. */
export const sessionBriefStage = (p: SessionBriefInputs) =>
  Effect.gen(function* () {
    const refuse = (why: string): never => {
      throw new Error(`96bulls: ${SESSION_BRIEF_STAGE}: ${why}. Nothing was created.`);
    };
    const props = sessionBriefWorkerProps(p, {
      HYPERDRIVE: Cloudflare.Hyperdrive.lookup("HYPERDRIVE", { hyperdriveId: p.hyperdriveId }),
    });
    assertPrivateBriefWorker(props, refuse);
    assertNoSecretLiterals(props.env, refuse);
    yield* Cloudflare.Worker("session-brief", props as never);
    return { workerName: SESSION_BRIEF_WORKER, crons: [...SESSION_BRIEF_CRONS], enabled: props.env.SESSION_BRIEF_ENABLED };
  });

/**
 * Compile-time: the runtime settings above are the installed provider's own types (the
 * stage passes the props `as never`, because the env mixes descriptors and literals).
 */
const _settingsTypecheck: Pick<Cloudflare.WorkerProps, "compatibility" | "crons" | "limits" | "observability" | "workersDev" | "routes"> = (() => {
  const { compatibility, crons, limits, observability, workersDev, routes } = sessionBriefWorkerProps(
    { accountId: "0".repeat(32), hyperdriveId: "0".repeat(32), secretsStoreId: "0".repeat(32) },
    { HYPERDRIVE: undefined },
  );
  return { compatibility, crons, limits, observability, workersDev, routes };
})();
void _settingsTypecheck;
