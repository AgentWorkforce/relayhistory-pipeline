/**
 * The daily digest narrative Worker (lane F4). It has no public route: it is reached only via
 * the read Worker's DIGEST_NARRATIVE service binding. It holds what the read API must not
 * hold: the model key and the narrative writer role.
 *
 *   POST /v1/narrative  NarrativeRequest -> NarrativeResult (src/session-read-api/digest-narrative.ts)
 *
 * Generation runs in ctx.waitUntil, so it finishes and caches even when the read Worker
 * stops waiting (its deadline is below Cloud's 4 s read timeout).
 *
 * Env (wrangler.digest-narrative.jsonc). Nothing is deployed or bound by this repository:
 *   NARRATIVE_DB        Hyperdrive to Supabase as bulls96_digest_narrative (password in
 *                       1Password; the lead creates the config)
 *   EXTRACTION_API_KEY  secret, from 1Password (or OPENAI_API_KEY); absent = provider_not_configured
 *   EXTRACTION_MODEL / EXTRACTION_API_URL  vars; defaults gpt-4o-mini / OpenAI chat completions
 * Never logs content: codes and outcomes only.
 */
import "../../pipeline/src/lib/scrub-engine/hosted-worker-grant.ts"; // FIRST import: module-load TS grant (docs/scrub-engine.md "Default flip")
import pg from "pg";
import { computeStatsHash } from "../../../src/session-read-api/digest-query.ts";
import {
  UPSERT_SQL,
  extractionProviderFromEnv,
  generateNarrative,
  parseNarrativeRequest,
  type NarrativeRow,
} from "../../../src/session-read-api/digest-narrative.ts";
import { logLine } from "../../../src/shared/log.ts";
import { declareScrubTsCapability } from "../../pipeline/src/lib/scrub-engine/ts-capability.ts";

export interface NarrativeEnv {
  NARRATIVE_DB?: Hyperdrive;
  EXTRACTION_API_KEY?: string;
  OPENAI_API_KEY?: string;
  EXTRACTION_MODEL?: string;
  EXTRACTION_API_URL?: string;
}

export interface NarrativeOverrides {
  upsert?: (row: NarrativeRow) => Promise<void>;
  fetcher?: typeof fetch;
}

const SERVICE = "96bulls-digest-narrative";
const MAX_BODY_BYTES = 1024 * 1024;
const log = (message: string, fields: Record<string, unknown>) => console.log(logLine("info", message, { service: SERVICE, ...fields }));
const reply = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", "cache-control": "private, no-store" } });

async function upsertVia(hyperdrive: Hyperdrive, row: NarrativeRow): Promise<void> {
  const client = new pg.Client({ connectionString: hyperdrive.connectionString, application_name: SERVICE, connectionTimeoutMillis: 2_000 });
  client.on("error", () => {});
  await client.connect();
  try {
    await client.query(UPSERT_SQL, [row.orgId, row.workspaceId, row.day, row.tz, row.scopeKey, row.statsHash, row.text, row.model, row.generatedAt.toISOString()]);
  } finally {
    await client.end().catch(() => {});
  }
}

export async function handleNarrative(request: Request, env: NarrativeEnv, ctx: Pick<ExecutionContext, "waitUntil">, o: NarrativeOverrides = {}): Promise<Response> {
  const url = new URL(request.url);
  if (request.method !== "POST" || url.pathname !== "/v1/narrative") return reply(404, { error: { code: "not_found" } });
  const length = Number(request.headers.get("content-length") ?? "0");
  if (length > MAX_BODY_BYTES) return reply(413, { error: { code: "too_large" } });
  let body: unknown;
  try {
    const text = await request.text();
    if (text.length > MAX_BODY_BYTES) return reply(413, { error: { code: "too_large" } });
    body = JSON.parse(text);
  } catch {
    return reply(400, { error: { code: "bad_request" } });
  }
  const req = await parseNarrativeRequest(body, computeStatsHash);
  if (!req) return reply(400, { error: { code: "bad_request" } });
  const upsert = o.upsert ?? (env.NARRATIVE_DB ? (row: NarrativeRow) => upsertVia(env.NARRATIVE_DB!, row) : null);
  const provider = upsert ? extractionProviderFromEnv(env, o.fetcher) : null;
  const work = generateNarrative(req, { provider, upsert: upsert ?? (async () => { throw new Error("no writer"); }) });
  ctx.waitUntil(work.then((r) => log("digest narrative", { outcome: r.unavailable ?? "generated" }), () => {}));
  return reply(200, await work);
}

export default {
  fetch(request, env, ctx) {
    declareScrubTsCapability("hosted-worker"); // idempotent; the module-load grant above is the primary one
    return handleNarrative(request, env, ctx);
  },
} satisfies ExportedHandler<NarrativeEnv>;
