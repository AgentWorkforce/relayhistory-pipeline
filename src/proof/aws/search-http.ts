/**
 * V1 search the way a customer's request reaches it: the caller's own RelayAuth
 * token is exchanged for a 60-second tenant search token (RelayAuth #116,
 * `POST /v1/tokens/session-directory-search`), which PostgREST verifies before
 * running `sessions.search_session_directory` as the Worker role (#73's
 * self-hosted profile). This is the only search evidence that counts toward a
 * gate; the psql `SET ROLE` path is a diagnostic.
 *
 * The exchange key comes from the environment only (PROOF_SEARCH_EXCHANGE_KEY,
 * piped from 1Password) and travels only through `kubectl port-forward`.
 */
import type { Fetch } from "./ingest.ts";
import type { SearchRow } from "./probes.ts";

export const SEARCH_EXCHANGE_KEY_ENV = "PROOF_SEARCH_EXCHANGE_KEY";

export class SearchHttpError extends Error {
  constructor(readonly code: string, readonly status: number) {
    super(`search ${code} (${status})`);
  }
}

async function code(res: Response): Promise<string> {
  const b = (await res.json().catch(() => ({}))) as { code?: unknown; error?: { code?: unknown } };
  const c = typeof b.code === "string" ? b.code : typeof b.error?.code === "string" ? b.error.code : `http_${res.status}`;
  return String(c).slice(0, 64);
}

export async function exchangeSearchToken(o: { relayauthUrl: string; exchangeKey: string; subjectToken: string; workspaceId: string; fetch?: Fetch }): Promise<string> {
  const f = o.fetch ?? fetch;
  const res = await f(`${o.relayauthUrl}/v1/tokens/session-directory-search`, {
    method: "POST",
    headers: { "x-api-key": o.exchangeKey, "content-type": "application/json" },
    body: JSON.stringify({ subjectToken: o.subjectToken, workspaceId: o.workspaceId }),
  });
  if (res.status !== 201) throw new SearchHttpError(await code(res), res.status);
  const b = (await res.json()) as { accessToken?: unknown };
  if (typeof b.accessToken !== "string" || !b.accessToken) throw new SearchHttpError("token_missing", res.status);
  return b.accessToken;
}

/** One RPC call through PostgREST; latency is client-measured over the tunnel. */
export async function postgrestSearch(o: { postgrestUrl: string; token: string; workspaceId: string; q: string; fetch?: Fetch; now?: () => number }): Promise<{ rows: SearchRow[]; ms: number }> {
  const f = o.fetch ?? fetch;
  const now = o.now ?? (() => performance.now());
  const t0 = now();
  const res = await f(`${o.postgrestUrl}/rpc/search_session_directory`, {
    method: "POST",
    headers: { authorization: `Bearer ${o.token}`, "content-profile": "sessions", "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify({ p_workspace_id: o.workspaceId, p_query: o.q }),
  });
  const ms = now() - t0;
  if (res.status !== 200) throw new SearchHttpError(await code(res), res.status);
  const rows = (await res.json()) as Array<{ session_id?: unknown; org_id?: unknown; short_summary?: unknown }>;
  if (!Array.isArray(rows)) throw new SearchHttpError("rows_not_array", res.status);
  return { rows: rows.map((r) => ({ session_id: String(r.session_id), org_id: String(r.org_id), short_summary: typeof r.short_summary === "string" ? r.short_summary : null })), ms };
}
