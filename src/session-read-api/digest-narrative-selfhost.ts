/**
 * The daily digest narrative on the self-host read API service (lambda.ts, entry "service"):
 * the same resolveDigestNarrative / generateNarrative / buildNarrativePrompt and the same
 * 045000 cache as the hosted narrative Worker, with generation in-process. Default off.
 *
 *   READ_API_DIGEST_ENABLED            exactly "1" (as the read Worker reads it): build the
 *                                      narrative deps. Anything else: nothing here is read or built.
 *   READ_API_DIGEST_NARRATIVE_MODEL    unset or blank: provider_not_configured (no model call,
 *                                      no writer pool). Otherwise the chat-completions model id;
 *                                      an id outside MODEL_PATTERN (session-brief / Bob Costas)
 *                                      refuses startup without echoing the value.
 *   OPENAI_API_KEY                     the model credential, shared with session-brief and Bob
 *                                      Costas. Unset, blank or ending in "-disabled": no provider.
 *   OPENAI_BASE_URL                    default https://api.openai.com/v1; calls go to
 *                                      <base>/chat/completions. https, loopback, or an in-cluster
 *                                      Service with ALLOW_IN_CLUSTER_HTTP=true (destination-url.ts).
 *   READ_API_DIGEST_NARRATIVE_DB_URL   the narrative writer login (bulls96_digest_narrative, the
 *                                      only role 045000 grants EXECUTE on digest_narrative_upsert).
 *                                      Unset: the narrative is read-only from the cache (hits are
 *                                      served, a miss is provider_not_configured, no model call).
 *
 * Trust boundary. The read pool (bulls96_read_api, READ ONLY) only ever reads the cache through
 * digest_narrative_read. Writes go through a separate pool (max 1 connection, the role's 5 s
 * statement_timeout, SESSION_POSTGRES_TLS_PROFILE trust, discrete fields so nothing in the URL
 * query reaches pg) that runs exactly one statement, UPSERT_SQL. The writer role can upsert for
 * any tenant; the tenant always comes from the verified identity the digest route was called
 * with, never from a request body. Startup refuses a writer login that is not exactly
 * bulls96_digest_narrative or that is superuser / BYPASSRLS.
 *
 * Generation runs detached from the read: the read waits at most the 3 s deadline, while the
 * model call keeps its own 10 s timeout and caches on success (the hosted Worker's waitUntil).
 * Concurrent reads of one key share one generation, and at most MAX_INFLIGHT generations run
 * per process.
 *
 * Never logs or throws prompts, model output, request or response bodies, provider error text,
 * URLs or the key. The prompt is scrubbed before it is sent and the narrative is scrubbed
 * before it is cached or returned.
 */
import pg from "pg";
import { log as defaultLog } from "../shared/log.ts";
import { scrubText } from "../shared/vendor.ts";
import { assertDestinationUrl, destinationPolicy } from "../shared/destination-url.ts";
import { postgresConnectionString, postgresTlsOptions } from "../shared/postgres-tls.ts";
import { DEFAULT_MODEL_BASE_URL, MODEL_PATTERN, modelKey } from "../session-brief/config.ts";
import {
  UPSERT_SQL,
  generateNarrative,
  sqlNarrativeCache,
  type DigestNarrativeDeps,
  type ExtractionProvider,
  type NarrativeGenerator,
  type NarrativeRequest,
  type NarrativeResult,
  type NarrativeRow,
} from "./digest-narrative.ts";
import type { TenantRunner } from "./liveness.ts";

export const READ_API_DIGEST_ENABLED_VAR = "READ_API_DIGEST_ENABLED";
export const READ_API_DIGEST_NARRATIVE_MODEL_VAR = "READ_API_DIGEST_NARRATIVE_MODEL";
export const READ_API_DIGEST_NARRATIVE_DB_URL_VAR = "READ_API_DIGEST_NARRATIVE_DB_URL";
/** The only role 20261105045000 grants EXECUTE on sessions.digest_narrative_upsert. */
export const NARRATIVE_WRITER_ROLE = "bulls96_digest_narrative";
/** digest_narrative_role.sql pins statement_timeout = 5s on the role; the pool sends the same. */
export const NARRATIVE_WRITER_STATEMENT_TIMEOUT_MS = 5_000;
export const MAX_INFLIGHT = 4;
/** A chat-completions answer for a 2-4 sentence narrative is a few KB; anything far above is refused. */
export const MAX_RESPONSE_BYTES = 256 * 1024;

/** READ_API_DIGEST_ENABLED, as the read Worker reads it: exactly "1" (trimmed) is on. */
export function digestEnabled(env: NodeJS.ProcessEnv): boolean {
  return env[READ_API_DIGEST_ENABLED_VAR]?.trim() === "1";
}

/** READ_API_DIGEST_NARRATIVE_MODEL: null when unset/blank; throws (never echoing) on an invalid id. */
export function digestNarrativeModel(env: NodeJS.ProcessEnv): string | null {
  const raw = (env[READ_API_DIGEST_NARRATIVE_MODEL_VAR] ?? "").trim();
  if (!raw) return null;
  if (!MODEL_PATTERN.test(raw)) throw new Error(`96bulls: ${READ_API_DIGEST_NARRATIVE_MODEL_VAR} is not a valid model id`);
  return raw;
}

/** <OPENAI_BASE_URL>/chat/completions under the shared destination policy; throws naming the variable only. */
export function chatCompletionsUrl(env: NodeJS.ProcessEnv): string {
  const raw = env.OPENAI_BASE_URL?.trim() || DEFAULT_MODEL_BASE_URL;
  const url = assertDestinationUrl("OPENAI_BASE_URL", raw, destinationPolicy(env));
  return `${url.origin}${url.pathname.replace(/\/+$/, "")}/chat/completions`;
}

export class NarrativeProviderError extends Error {
  constructor(readonly code: "http_status" | "transport" | "too_large" | "output_invalid", readonly status?: number) {
    super(`digest narrative provider: ${code}${status === undefined ? "" : ` ${status}`}`);
    this.name = "NarrativeProviderError";
  }
}

/**
 * An OpenAI-compatible chat-completions provider (JSON mode), the hosted Worker's request shape.
 * redirect "manual" keeps the key off any redirect target (a 3xx is then a non-2xx refusal).
 * Errors carry a closed code and the HTTP status only; a non-2xx body is never read.
 */
export function openAiChatNarrativeProvider(o: { model: string; apiKey: string; url: string; fetch?: typeof fetch }): ExtractionProvider {
  const fetcher = o.fetch ?? ((i: string, init?: RequestInit) => fetch(i, init));
  return {
    model: o.model,
    async extract(prompt, signal) {
      let response: Response;
      try {
        response = await fetcher(o.url, {
          method: "POST",
          redirect: "manual",
          headers: { authorization: `Bearer ${o.apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: o.model, temperature: 0, response_format: { type: "json_object" },
            messages: [{ role: "user", content: scrubText(prompt) }],
          }),
          signal,
        });
      } catch {
        throw new NarrativeProviderError("transport");
      }
      if (!response.ok) {
        await response.body?.cancel().catch(() => {});
        throw new NarrativeProviderError("http_status", response.status);
      }
      const declared = Number(response.headers.get("content-length") ?? "0");
      if (declared > MAX_RESPONSE_BYTES) {
        await response.body?.cancel().catch(() => {});
        throw new NarrativeProviderError("too_large");
      }
      let content: unknown;
      try {
        const text = await response.text();
        if (text.length > MAX_RESPONSE_BYTES) throw new NarrativeProviderError("too_large");
        content = (JSON.parse(text) as { choices?: Array<{ message?: { content?: unknown } }> })?.choices?.[0]?.message?.content;
      } catch (e) {
        if (e instanceof NarrativeProviderError) throw e;
        throw new NarrativeProviderError(signal.aborted ? "transport" : "output_invalid");
      }
      if (typeof content !== "string" || !content.trim()) throw new NarrativeProviderError("output_invalid");
      let parsed: unknown;
      try {
        parsed = JSON.parse(content);
      } catch {
        throw new NarrativeProviderError("output_invalid");
      }
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new NarrativeProviderError("output_invalid");
      const narrative = (parsed as Record<string, unknown>).narrative;
      // Scrubbed here, so generateNarrative bounds, caches and returns only scrubbed text.
      return { narrative: typeof narrative === "string" ? scrubText(narrative) : narrative };
    },
  };
}

/**
 * The writer pool's target: discrete fields from the sanitized URL (postgresConnectionString drops
 * TLS/options spellings and refuses host/user/... overrides), so nothing in the query string reaches
 * pg. Missing user, password or database refuses (pg would fall back to PG* env or .pgpass).
 */
export function narrativeWriterPoolTarget(raw: string): pg.PoolConfig {
  const fail = () => new Error(`session read api: ${READ_API_DIGEST_NARRATIVE_DB_URL_VAR} needs a postgres URL with a user, password and database`);
  let target: URL;
  try {
    target = new URL(postgresConnectionString(raw));
  } catch { throw fail(); }
  let user: string, password: string, database: string;
  try {
    user = decodeURIComponent(target.username);
    password = decodeURIComponent(target.password);
    database = decodeURIComponent(target.pathname.replace(/^\//u, ""));
  } catch { throw fail(); }
  if (!user || !password || !database || database.includes("/") || target.hash) throw fail();
  return { host: target.hostname.replace(/^\[(.*)\]$/u, "$1"), port: target.port ? Number(target.port) : 5432, user, password, database };
}

export const WRITER_LOGIN_SQL = `SELECT session_user::text AS session_user, current_user::text AS current_user,
  current_setting('role') AS role_setting, r.rolsuper, r.rolbypassrls
  FROM pg_roles r WHERE r.rolname = current_user`;

/** Refuses a writer login that is not exactly the narrative role, or that is elevated. Names the property only. */
export async function assertNarrativeWriterRole(sql: { query<R>(text: string, params?: unknown[]): Promise<{ rows: R[] }> }): Promise<void> {
  const { rows } = await sql.query<Record<string, unknown>>(WRITER_LOGIN_SQL);
  const r = rows.length === 1 ? rows[0]! : undefined;
  const refuse = (what: string) => new Error(`session read api: narrative writer ${what} (the ${READ_API_DIGEST_NARRATIVE_DB_URL_VAR} login must be ${NARRATIVE_WRITER_ROLE})`);
  if (!r) throw refuse("role_unknown");
  if (r.session_user !== NARRATIVE_WRITER_ROLE || r.current_user !== NARRATIVE_WRITER_ROLE || r.role_setting !== "none") throw refuse("role_mismatch");
  if (r.rolsuper !== false || r.rolbypassrls !== false) throw refuse("role_elevated");
}

/** The writer's one statement. Exported for tests. */
export function narrativeUpsert(pool: { query(text: string, params: unknown[]): Promise<unknown> }) {
  return async (row: NarrativeRow): Promise<void> => {
    await pool.query(UPSERT_SQL, [row.orgId, row.workspaceId, row.day, row.tz, row.scopeKey, row.statsHash, row.text, row.model, row.generatedAt.toISOString()]);
  };
}

type Logger = (message: string, fields: Record<string, unknown>) => void;

/**
 * In-process generator: the read's AbortSignal is deliberately ignored, so a read that gives up
 * at its deadline does not cancel the model call (generateNarrative's own 10 s timeout bounds it)
 * and a slow miss still lands in the cache. generateNarrative never rejects.
 */
export function inProcessNarrativeGenerator(deps: {
  provider: ExtractionProvider;
  upsert(row: NarrativeRow): Promise<void>;
  timeoutMs?: number;
  maxInflight?: number;
  log?: Logger;
}): NarrativeGenerator {
  const inflight = new Map<string, Promise<NarrativeResult>>();
  const max = deps.maxInflight ?? MAX_INFLIGHT;
  const log = deps.log ?? ((m, f) => defaultLog.info(m, f));
  return {
    generate(req: NarrativeRequest) {
      const key = JSON.stringify([req.orgId, req.day, req.tz, req.scopeKey, req.statsHash]);
      const running = inflight.get(key);
      if (running) return running;
      if (inflight.size >= max) return Promise.reject(new Error("narrative_busy"));
      const work = generateNarrative(req, { provider: deps.provider, upsert: deps.upsert, ...(deps.timeoutMs ? { timeoutMs: deps.timeoutMs } : {}) })
        .then((r) => { log("digest narrative", { outcome: r.unavailable ?? "generated" }); return r; })
        // Never rejects: a read that already gave up at its deadline leaves nobody awaiting this.
        .catch((): NarrativeResult => ({ text: null, model: null, generatedAt: null, cached: false, unavailable: "provider_error" }))
        .finally(() => inflight.delete(key));
      inflight.set(key, work);
      return work;
    },
  };
}

export interface SelfhostNarrative {
  deps: DigestNarrativeDeps;
  close(): Promise<void>;
}

/**
 * The self-host narrative deps, or undefined when READ_API_DIGEST_ENABLED is not "1" (nothing else
 * is read then). The cache always reads through `readRun` (the read pool). A generator exists only
 * with a model, a key and the writer URL; the writer pool is opened and its login checked only then.
 */
export async function selfhostNarrativeFromEnv(
  env: NodeJS.ProcessEnv,
  readRun: TenantRunner,
  options: { fetch?: typeof fetch; log?: Logger } = {},
): Promise<SelfhostNarrative | undefined> {
  if (!digestEnabled(env)) return undefined;
  const cache = sqlNarrativeCache(readRun);
  const model = digestNarrativeModel(env);
  if (!model) return { deps: { cache }, close: async () => {} };
  const url = chatCompletionsUrl(env);
  const apiKey = modelKey(env);
  const dbUrl = env[READ_API_DIGEST_NARRATIVE_DB_URL_VAR]?.trim();
  if (!apiKey || !dbUrl) return { deps: { cache }, close: async () => {} };
  const target = narrativeWriterPoolTarget(dbUrl);
  const writer = new pg.Pool({
    ...target,
    max: 1,
    application_name: "96bulls-digest-narrative-writer",
    ssl: postgresTlsOptions(env),
    idleTimeoutMillis: 60_000,
    connectionTimeoutMillis: 2_000,
    statement_timeout: NARRATIVE_WRITER_STATEMENT_TIMEOUT_MS,
    options: `-c statement_timeout=${NARRATIVE_WRITER_STATEMENT_TIMEOUT_MS}`, // Nonempty: pg otherwise uses PGOPTIONS.
  });
  writer.on("error", (err) => defaultLog.error("digest narrative writer pool error", { error_code: (err as { code?: string }).code ?? "pool_error" }));
  try {
    await assertNarrativeWriterRole(writer);
  } catch (error) {
    await writer.end().catch(() => {});
    throw error;
  }
  const provider = openAiChatNarrativeProvider({ model, apiKey, url, ...(options.fetch ? { fetch: options.fetch } : {}) });
  return {
    deps: { cache, generator: inProcessNarrativeGenerator({ provider, upsert: narrativeUpsert(writer), ...(options.log ? { log: options.log } : {}) }) },
    close: () => writer.end(),
  };
}
