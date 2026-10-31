/**
 * The transcript-search gate: full-transcript search, end to end, against a
 * configurable backend. V1 (HoopDb Postgres) has no transcript search, so this
 * gate is separate from the metadata/short_summary gate and the overall proof
 * cannot pass without it.
 *
 * Backend: ClickHouse or ObsessionDB (ClickHouse-compatible), both over the
 * ClickHouse HTTP interface (a synthetic ClickHouse Cloud service is approved
 * for the proof). Tenant isolation must be enforced by the backend, not by this
 * harness: each tenant queries with its own credentials and the query carries
 * NO tenant predicate. A row policy (or equivalent) must keep tenant A's
 * credentials from reading tenant B's rows; if it does not, the gate fails.
 *
 * Feed: `pipeline` (the only gate evidence) waits for the product to project
 * planted transcripts, ingested through the authenticated ingest, into the
 * backend (no V2 projector exists in this repository yet, so this times out
 * honestly). `harness-synthetic` inserts the planted transcript text itself
 * with a separate writer credential: a diagnostic of the backend only, so the
 * gate is BLOCKED, never PASS. Freshness is measured from landed to searchable.
 */
import type { PlantedSession } from "./corpus.ts";
import type { Fetch } from "./ingest.ts";
import { crossTenantHits, tenantKey, type ProbeObservation, type Tenant } from "./leak-scan.ts";
import { stageLatency, summarize, type LatencySummary } from "./stats.ts";

export type TranscriptFeed = "pipeline" | "harness-synthetic";

export type TranscriptBackendKind = "none" | "clickhouse" | "obsessiondb";

export interface TranscriptConfig {
  backend: TranscriptBackendKind;
  feed: TranscriptFeed;
  /** HTTPS endpoint of the ClickHouse HTTP interface (ClickHouse Cloud, self-managed ClickHouse, or ObsessionDB). */
  endpoint?: string;
  database?: string;
  table?: string;
  /**
   * Names of the environment variables (secrets piped from 1Password at run
   * time) that hold credentials; never the credentials themselves.
   *   tenants: JSON {"<orgId>/<workspaceId>": "user:password"}, one read-only user per tenant
   *   writer:  "user:password", synthetic feed only
   */
  credentials: { tenants: string; writer?: string };
}

export interface TranscriptRow {
  session_id: string;
  org_id: string;
}

export interface TranscriptSearch {
  rows: TranscriptRow[];
  clientMs: number;
  /** Backend-reported execution time when the backend provides one. */
  serverMs?: number;
}

export interface TranscriptBackend {
  /** What the report says this gate exercised, e.g. "ClickHouse Cloud abc.clickhouse.cloud". */
  readonly label: string;
  search(tenant: Tenant, query: string): Promise<TranscriptSearch>;
  /** Synthetic feed only: write the planted transcript rows. */
  insert?(rows: ReadonlyArray<{ org_id: string; workspace_id: string; session_id: string; content: string }>): Promise<void>;
}

/** Exactly what the report says the transcript gate exercised: kind + host + table + feed, or "none". */
export const backendLabel = (c: TranscriptConfig): string => {
  if (c.backend === "none" || !c.endpoint) return "none";
  const host = new URL(c.endpoint).hostname;
  const kind = c.backend === "obsessiondb" ? "ObsessionDB" : /\.clickhouse\.cloud$/.test(host) ? "ClickHouse Cloud" : "ClickHouse";
  return `${kind} ${host} (${c.database}.${c.table}; feed: ${c.feed})`;
};

const IDENT = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/;

export function validateTranscriptConfig(c: TranscriptConfig): string[] {
  if (c.backend === "none") return [];
  const issues: string[] = [];
  if (c.backend !== "clickhouse" && c.backend !== "obsessiondb") return [`transcriptSearch.backend must be none, clickhouse or obsessiondb`];
  try {
    if (new URL(c.endpoint ?? "").protocol !== "https:") issues.push("transcriptSearch.endpoint must be https");
  } catch {
    issues.push("transcriptSearch.endpoint is not a URL");
  }
  if (!IDENT.test(c.database ?? "") || !IDENT.test(c.table ?? "")) issues.push("transcriptSearch database/table must be plain identifiers");
  const envName = /^[A-Z][A-Z0-9_]{2,63}$/;
  if (!envName.test(c.credentials?.tenants ?? "")) issues.push("transcriptSearch.credentials.tenants must name an environment variable");
  if (c.feed === "harness-synthetic" && !envName.test(c.credentials?.writer ?? "")) issues.push("the synthetic feed needs transcriptSearch.credentials.writer (an environment variable name)");
  return issues;
}

export interface Credential {
  user: string;
  password: string;
}

/** Per-tenant read credentials and the synthetic writer, read from the environment variables the config names. */
export function credentialsFromEnv(c: TranscriptConfig, env: Record<string, string | undefined>): { tenants: Map<string, Credential>; writer?: Credential; secrets: string[] } {
  const split = (v: string): Credential => {
    const i = v.indexOf(":");
    if (i <= 0) throw new Error("ClickHouse credentials must be user:password");
    return { user: v.slice(0, i), password: v.slice(i + 1) };
  };
  const tenants = new Map<string, Credential>();
  const secrets: string[] = [];
  const raw = env[c.credentials.tenants];
  if (raw) for (const [k, v] of Object.entries(JSON.parse(raw) as Record<string, string>)) {
    const cred = split(v);
    tenants.set(k, cred);
    secrets.push(cred.password, v);
  }
  const w = c.credentials.writer ? env[c.credentials.writer] : undefined;
  const writer = w ? split(w) : undefined;
  if (writer) secrets.push(writer.password, w!);
  return { tenants, writer, secrets };
}

export function clickhouseBackend(o: { url: string; database: string; table: string; tenants: Map<string, Credential>; writer?: Credential; label: string; fetch?: Fetch; now?: () => number }): TranscriptBackend {
  const f = o.fetch ?? fetch;
  const now = o.now ?? (() => performance.now());
  if (!IDENT.test(o.database) || !IDENT.test(o.table)) throw new Error("unsafe ClickHouse identifier");
  const auth = (c: Credential) => ({ "X-ClickHouse-User": c.user, "X-ClickHouse-Key": c.password });
  return {
    label: o.label,
    async search(tenant, query) {
      const cred = o.tenants.get(tenantKey(tenant));
      if (!cred) throw Object.assign(new Error("no ClickHouse credential for tenant"), { code: "no_tenant_credential" });
      // No tenant predicate on purpose: isolation must come from the backend's row policy.
      const sql = `SELECT session_id, org_id FROM ${o.database}.${o.table} WHERE positionCaseInsensitive(content, {q:String}) > 0 LIMIT 51 FORMAT JSON`;
      const u = new URL(o.url);
      u.searchParams.set("param_q", query);
      u.searchParams.set("readonly", "1");
      const t0 = now();
      const res = await f(u.toString(), { method: "POST", headers: { ...auth(cred), "content-type": "text/plain" }, body: sql });
      const clientMs = now() - t0;
      if (!res.ok) throw Object.assign(new Error("ClickHouse search failed"), { code: `http_${res.status}` });
      const body = (await res.json()) as { data?: TranscriptRow[]; statistics?: { elapsed?: number } };
      return { rows: body.data ?? [], clientMs, ...(typeof body.statistics?.elapsed === "number" ? { serverMs: body.statistics.elapsed * 1000 } : {}) };
    },
    ...(o.writer
      ? {
          async insert(rows) {
            const u = new URL(o.url);
            u.searchParams.set("query", `INSERT INTO ${o.database}.${o.table} (org_id, workspace_id, session_id, content) FORMAT JSONEachRow`);
            const res = await f(u.toString(), { method: "POST", headers: { ...auth(o.writer!), "content-type": "application/x-ndjson" }, body: rows.map((r) => JSON.stringify(r)).join("\n") });
            if (!res.ok) throw Object.assign(new Error("ClickHouse insert failed"), { code: `http_${res.status}` });
          },
        }
      : {}),
  };
}

/* ------------------------------------------------------------- verdict */

export interface TranscriptObservation {
  sessionId: string;
  landedAtMs?: number;
  /** First time the tenant's own search found the session's transcript-only canary. */
  searchableAtMs?: number;
  queryMs: number[];
  queryErrors: number;
}

export interface TranscriptVerdict {
  failures: string[];
  metrics: {
    backend: string;
    feed: TranscriptFeed;
    planted: number;
    searchable: number;
    landedToSearchableMs: LatencySummary;
    queryMs: LatencySummary;
    crossTenantProbes: number;
    crossTenantProbeErrors: number;
    crossTenantHits: number;
  };
}

/**
 * Pass requires: every planted session's transcript-only text is found by its
 * own tenant (positive control and freshness), and no tenant's credentials
 * return another tenant's session or canary.
 */
export function evaluateTranscriptGate(
  backend: string,
  feed: TranscriptFeed,
  planted: readonly PlantedSession[],
  obs: ReadonlyMap<string, TranscriptObservation>,
  crossProbes: readonly ProbeObservation[],
): TranscriptVerdict {
  const failures: string[] = [];
  const all = planted.map((p) => obs.get(p.sessionId));
  const missing = planted.filter((p) => obs.get(p.sessionId)?.searchableAtMs === undefined);
  if (missing.length) failures.push(`${missing.length}/${planted.length} planted transcripts never became searchable by their own tenant; first: ${missing.slice(0, 5).map((p) => p.sessionId).join(", ")}`);
  const owner = new Map(planted.map((p) => [p.sessionId, p.tenant]));
  const canaries = new Map<string, string[]>();
  for (const p of planted) canaries.set(tenantKey(p.tenant), [...(canaries.get(tenantKey(p.tenant)) ?? []), p.transcriptCanary]);
  const { hits } = crossTenantHits(crossProbes, owner, canaries);
  for (const h of hits) failures.push(`cross-tenant transcript hit: ${h.caller} saw ${h.owner} (${h.subject})`);
  if (crossProbes.length === 0) failures.push("no cross-tenant transcript probes ran");
  // An errored probe observed nothing; counting it as a clean empty result would fake isolation.
  const errored = crossProbes.filter((p) => p.status !== 200);
  if (errored.length) failures.push(`${errored.length}/${crossProbes.length} cross-tenant transcript probe(s) errored (${[...new Set(errored.map((p) => String(p.status)))].join(", ")}); isolation unproven`);
  const queryErrors = all.reduce((n, o) => n + (o?.queryErrors ?? 0), 0);
  const query = summarize(all.flatMap((o) => o?.queryMs ?? []), queryErrors);
  if (queryErrors) failures.push(`${queryErrors} transcript search request(s) errored`);
  return {
    failures,
    metrics: {
      backend,
      feed,
      planted: planted.length,
      searchable: planted.length - missing.length,
      landedToSearchableMs: stageLatency(all, (o) => o?.landedAtMs, (o) => o?.searchableAtMs),
      queryMs: query,
      crossTenantProbes: crossProbes.length,
      crossTenantProbeErrors: errored.length,
      crossTenantHits: hits.length,
    },
  };
}
