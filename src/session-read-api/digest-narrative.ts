/**
 * The daily digest narrative off Neon: relayhistory `lib/narrative.ts` @ c7de2163 with the
 * cache in Supabase (`sessions.daily_digest_narratives`, migration 20261105045000) and
 * generation in a separate, service-bound narrative Worker, so the read API stays read-only.
 *
 *   read side   resolveDigestNarrative: no_activity without any call; else the cache through
 *               digest_narrative_read (bulls96_read_api); on a miss, a stale hash or
 *               refresh=true, the narrative Worker (DIGEST_NARRATIVE service binding).
 *   write side  generateNarrative (the narrative Worker): the model call with relayhistory's
 *               10 s timeout and prompt, then digest_narrative_upsert as bulls96_digest_narrative.
 *
 * Every failure is a 200 narrative with `unavailable` set (relayhistory's contract), never a
 * 501: with Cloud's SESSIONS_READ_96BULLS_FALLBACK=none there is nothing to fall back to.
 * Cloud bounds a 96Bulls read at 4 s, below relayhistory's inline 10 s, so the read side waits
 * at most `deadlineMs` (default 3 s) for the Worker. The Worker always finishes generating and
 * caching in `waitUntil`, so a slow miss is `provider_error` once and a cache hit next read.
 *
 * The prompt omits stats.outcomes.reverted ("drop and rebuild": no producer after the freeze).
 * Nothing here logs content.
 */
import type { TenantRunner } from "./liveness.ts";
import type { DailyDigest } from "./digest-query.ts";

export type NarrativeUnavailableReason = "provider_not_configured" | "provider_error" | "no_activity";

export interface NarrativeResult {
  text: string | null;
  model: string | null;
  generatedAt: string | null;
  cached: boolean;
  unavailable: NarrativeUnavailableReason | null;
}

export const NARRATIVE_TIMEOUT_MS = 10_000;
export const NARRATIVE_READ_DEADLINE_MS = 3_000;
export const MAX_NARRATIVE_TEXT = 16_384;
export const MAX_NARRATIVE_PROJECTS = 10;
export const MAX_NARRATIVE_EPICS = 10;
export const MAX_NARRATIVE_SESSIONS = 15;

const UNAVAILABLE = { text: null, model: null, generatedAt: null, cached: false } as const;
const unavailable = (unavailable: NarrativeUnavailableReason): NarrativeResult => ({ ...UNAVAILABLE, unavailable });

/** relayhistory's workspace scope key; every 96Bulls digest is workspace-scoped. */
export function narrativeScopeKey(workspaceId: string, projectId: string | null): string {
  return `workspace:${JSON.stringify([workspaceId, projectId])}`;
}

export interface CachedNarrative {
  statsHash: string;
  text: string;
  model: string | null;
  generatedAt: string | null;
}

export interface NarrativeCache {
  /** Null on a miss (or before the migration is applied). Throws on a read failure. */
  read(orgId: string, workspaceId: string, day: string, tz: string, scopeKey: string): Promise<CachedNarrative | null>;
}

export interface NarrativeRequest {
  orgId: string;
  workspaceId: string;
  day: string;
  tz: string;
  projectId: string | null;
  scopeKey: string;
  statsHash: string;
  digest: DailyDigest;
}

export interface NarrativeGenerator {
  generate(req: NarrativeRequest, signal: AbortSignal): Promise<NarrativeResult>;
}

export interface DigestNarrativeDeps {
  cache?: NarrativeCache;
  generator?: NarrativeGenerator;
  deadlineMs?: number;
}

/** A NarrativeResult exactly as the cloud-client 0.2.0 `narrativeResult` schema has it, or null. */
export function parseNarrativeResult(v: unknown): NarrativeResult | null {
  if (!v || typeof v !== "object" || Array.isArray(v)) return null;
  const r = v as Record<string, unknown>;
  const keys = Object.keys(r).sort().join(",");
  if (keys !== "cached,generatedAt,model,text,unavailable") return null;
  const str = (x: unknown) => x === null || typeof x === "string";
  if (!str(r.text) || !str(r.model) || !str(r.generatedAt) || typeof r.cached !== "boolean") return null;
  if (r.generatedAt !== null && !Number.isFinite(Date.parse(String(r.generatedAt)))) return null;
  if (r.unavailable !== null && !["provider_not_configured", "provider_error", "no_activity"].includes(String(r.unavailable))) return null;
  if (typeof r.text === "string" && r.text.length > MAX_NARRATIVE_TEXT) return null;
  return r as unknown as NarrativeResult;
}

export async function resolveDigestNarrative(
  deps: DigestNarrativeDeps,
  params: Omit<NarrativeRequest, "digest" | "scopeKey"> & { refresh: boolean },
  digest: DailyDigest,
): Promise<NarrativeResult> {
  if (digest.stats.sessionCount === 0 && digest.stats.eventCount === 0) return unavailable("no_activity");
  const scopeKey = narrativeScopeKey(params.workspaceId, params.projectId);
  if (!params.refresh && deps.cache) {
    let cached: CachedNarrative | null;
    try {
      cached = await deps.cache.read(params.orgId, params.workspaceId, params.day, params.tz, scopeKey);
    } catch {
      return unavailable("provider_error");
    }
    if (cached && cached.statsHash === params.statsHash)
      return { text: cached.text, model: cached.model, generatedAt: cached.generatedAt, cached: true, unavailable: null };
  }
  if (!deps.generator) return unavailable("provider_not_configured");
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const result = await Promise.race([
      deps.generator.generate({ ...params, scopeKey, digest }, controller.signal),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error("narrative_deadline"));
          controller.abort();
        }, deps.deadlineMs ?? NARRATIVE_READ_DEADLINE_MS);
      }),
    ]);
    return parseNarrativeResult(result) ?? unavailable("provider_error");
  } catch {
    return unavailable("provider_error");
  } finally {
    clearTimeout(timer);
  }
}

/** relayhistory parseNarrative. */
export function parseNarrative(raw: unknown): string | null {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  const value = (raw as Record<string, unknown>).narrative;
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

/** relayhistory buildNarrativePrompt, with stats.outcomes.reverted omitted (no producer). */
export function buildNarrativePrompt(req: Pick<NarrativeRequest, "day" | "tz" | "projectId" | "digest">): string {
  const { digest } = req;
  const { reverted: _omitted, ...outcomes } = digest.stats.outcomes;
  const payload = {
    date: req.day,
    tz: req.tz,
    scope: req.projectId,
    stats: { ...digest.stats, outcomes },
    projects: digest.projects.slice(0, MAX_NARRATIVE_PROJECTS).map((p) => ({
      projectId: p.projectId, sessionCount: p.sessionCount, eventCount: p.eventCount, costUsdMicros: p.costUsdMicros,
    })),
    epics: digest.epics.slice(0, MAX_NARRATIVE_EPICS).map((e) => ({
      projectId: e.projectId, branch: e.branch, sessionCount: e.sessionCount, eventCount: e.eventCount,
    })),
    sessions: digest.sessions.slice(0, MAX_NARRATIVE_SESSIONS).map((s) => ({
      projectId: s.projectId, summary: s.summary, eventCount: s.eventCount, models: s.models,
    })),
  };
  return [
    "You are writing a short daily wrap-up for an engineering team's internal work log.",
    "Summarise what happened in 2-4 sentences of plain prose. No markdown, no bullet points, no headings.",
    "Ground every claim in the JSON below. Do not invent projects, numbers, or outcomes that are not present in it.",
    "The lists below may be truncated for length; do not claim they are the complete or only activity of the day.",
    "",
    'Return ONLY raw JSON: {"narrative": "..."}. No prose outside the JSON, no markdown fence.',
    "",
    JSON.stringify(payload),
  ].join("\n");
}

export interface ExtractionProvider {
  readonly model: string;
  extract(prompt: string, signal: AbortSignal): Promise<unknown>;
}

export interface NarrativeRow {
  orgId: string; workspaceId: string; day: string; tz: string; scopeKey: string; statsHash: string;
  text: string; model: string; generatedAt: Date;
}

/**
 * The narrative Worker's work (relayhistory resolveNarrative's generate-and-cache half): one
 * model call under `timeoutMs`, a parsed narrative, then the upsert. Any failure is
 * `provider_error` and nothing generated is returned without having been cached.
 */
export async function generateNarrative(
  req: NarrativeRequest,
  deps: { provider: ExtractionProvider | null; upsert(row: NarrativeRow): Promise<void>; now?: () => Date; timeoutMs?: number },
): Promise<NarrativeResult> {
  if (req.digest.stats.sessionCount === 0 && req.digest.stats.eventCount === 0) return unavailable("no_activity");
  if (!deps.provider) return unavailable("provider_not_configured");
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let raw: unknown;
  try {
    raw = await Promise.race([
      deps.provider.extract(buildNarrativePrompt(req), controller.signal),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error("narrative provider timed out"));
          controller.abort();
        }, deps.timeoutMs ?? NARRATIVE_TIMEOUT_MS);
      }),
    ]);
  } catch {
    return unavailable("provider_error");
  } finally {
    clearTimeout(timer);
  }
  const text = parseNarrative(raw);
  if (!text || text.length > MAX_NARRATIVE_TEXT) return unavailable("provider_error");
  const generatedAt = (deps.now ?? (() => new Date()))();
  try {
    await deps.upsert({ orgId: req.orgId, workspaceId: req.workspaceId, day: req.day, tz: req.tz, scopeKey: req.scopeKey, statsHash: req.statsHash, text, model: deps.provider.model, generatedAt });
  } catch {
    return unavailable("provider_error");
  }
  return { text, model: deps.provider.model, generatedAt: generatedAt.toISOString(), cached: false, unavailable: null };
}

export const DEFAULT_EXTRACTION_MODEL = "gpt-4o-mini";
export const DEFAULT_EXTRACTION_API_URL = "https://api.openai.com/v1/chat/completions";

/** relayhistory extractionProviderFromEnv / OpenAiExtractionProvider (chat completions, JSON mode). */
export function extractionProviderFromEnv(
  env: { EXTRACTION_API_KEY?: string; OPENAI_API_KEY?: string; EXTRACTION_API_URL?: string; EXTRACTION_MODEL?: string },
  fetcher: typeof fetch = (i, init) => fetch(i, init),
): ExtractionProvider | null {
  const apiKey = env.EXTRACTION_API_KEY?.trim() || env.OPENAI_API_KEY?.trim();
  if (!apiKey) return null;
  const model = env.EXTRACTION_MODEL?.trim() || DEFAULT_EXTRACTION_MODEL;
  const apiUrl = env.EXTRACTION_API_URL?.trim() || DEFAULT_EXTRACTION_API_URL;
  return {
    model,
    async extract(prompt, signal) {
      const response = await fetcher(apiUrl, {
        method: "POST",
        headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
        body: JSON.stringify({ model, temperature: 0, response_format: { type: "json_object" }, messages: [{ role: "user", content: prompt }] }),
        signal,
      });
      if (!response.ok) throw new Error(`extraction provider http ${response.status}`);
      const body = (await response.json()) as { choices?: Array<{ message?: { content?: string } }> };
      const content = body.choices?.[0]?.message?.content;
      if (typeof content !== "string" || !content.trim()) throw new Error("extraction provider returned no content");
      return JSON.parse(content) as unknown;
    },
  };
}

const READ_INSTALLED_SQL = "SELECT to_regprocedure('sessions.digest_narrative_read(text,date,text,text)') IS NOT NULL AS installed";
const READ_SQL = `SELECT stats_hash, narrative_text, narrative_model,
    to_char(narrative_generated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS generated_at
  FROM sessions.digest_narrative_read($1, $2::date, $3, $4)`;

/** The cache through the read role, inside the request's tenant-bound READ ONLY transaction. */
export function sqlNarrativeCache(run: TenantRunner): NarrativeCache {
  return {
    async read(orgId, workspaceId, day, tz, scopeKey) {
      return run(orgId, workspaceId, async (q) => {
        const installed = await q.query<{ installed: boolean }>(READ_INSTALLED_SQL, []);
        if (!installed.rows[0]?.installed) return null;
        const r = await q.query<{ stats_hash: string; narrative_text: string; narrative_model: string | null; generated_at: string | null }>(READ_SQL, [workspaceId, day, tz, scopeKey]);
        const row = r.rows[0];
        return row ? { statsHash: row.stats_hash, text: row.narrative_text, model: row.narrative_model, generatedAt: row.generated_at } : null;
      });
    },
  };
}

export const UPSERT_SQL = "SELECT sessions.digest_narrative_upsert($1, $2, $3::date, $4, $5, $6, $7, $8, $9::timestamptz)";

/** The read Worker's side of the DIGEST_NARRATIVE service binding (no public route exists). */
export function serviceNarrativeGenerator(binding: { fetch(input: string, init?: RequestInit): Promise<Response> }): NarrativeGenerator {
  return {
    async generate(req, signal) {
      const r = await binding.fetch("https://digest-narrative.internal/v1/narrative", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(req),
        signal,
      });
      if (!r.ok) throw new Error(`narrative worker ${r.status}`);
      const parsed = parseNarrativeResult(await r.json());
      if (!parsed) throw new Error("narrative worker result out of contract");
      return parsed;
    },
  };
}

const ID = /^[A-Za-z0-9._:-]{1,255}$/;

/**
 * The narrative Worker's request check. Tenant ids, day, tz and the scope key must agree,
 * and `statsHash` must be the hash of the digest sent with it, so a cached narrative is
 * always keyed by the numbers it was written from. Null = refuse (400).
 */
export async function parseNarrativeRequest(v: unknown, hash: (d: DailyDigest, ws: string) => Promise<string>): Promise<NarrativeRequest | null> {
  if (!v || typeof v !== "object" || Array.isArray(v)) return null;
  const r = v as Record<string, unknown>;
  if (typeof r.orgId !== "string" || !ID.test(r.orgId) || typeof r.workspaceId !== "string" || !ID.test(r.workspaceId)) return null;
  if (typeof r.day !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(r.day) || typeof r.tz !== "string" || !r.tz || r.tz.length > 128) return null;
  if (!(r.projectId === null || (typeof r.projectId === "string" && r.projectId.length <= 512))) return null;
  if (r.scopeKey !== narrativeScopeKey(r.workspaceId, r.projectId as string | null)) return null;
  if (typeof r.statsHash !== "string" || !/^[0-9a-f]{64}$/.test(r.statsHash)) return null;
  const d = r.digest as DailyDigest | undefined;
  if (!d || typeof d !== "object" || !d.stats || typeof d.stats !== "object" || !Array.isArray(d.projects) || !Array.isArray(d.epics) || !Array.isArray(d.sessions)) return null;
  if ((await hash(d, r.workspaceId)) !== r.statsHash) return null;
  return { orgId: r.orgId, workspaceId: r.workspaceId, day: r.day, tz: r.tz, projectId: r.projectId as string | null, scopeKey: r.scopeKey, statsHash: r.statsHash, digest: d };
}
