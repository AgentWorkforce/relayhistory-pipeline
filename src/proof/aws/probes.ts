/**
 * Read-only probes against the proof cluster, expressed as Steps so a dry run
 * prints them verbatim. SQL runs inside the CNPG primary with psql over local
 * peer auth: no database credential leaves the cluster or touches this machine.
 *
 * Every value interpolated into SQL is validated against a strict pattern first.
 */
import type { ProofConfig } from "./config.ts";
import type { Step } from "./exec.ts";
import type { Tenant } from "./leak-scan.ts";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const IDENT = /^[A-Za-z0-9_.:-]{1,128}$/;
const TERM = /^[a-z0-9]{3,200}$/;

export function sqlList(values: readonly string[], kind: "uuid" | "text"): string {
  if (values.length === 0) throw new Error("empty id list");
  for (const v of values) if (!(kind === "uuid" ? UUID : IDENT).test(v)) throw new Error(`refusing unsafe ${kind} literal`);
  return `ARRAY[${values.map((v) => `'${v}'`).join(",")}]::${kind}[]`;
}

const ident = (v: string) => {
  if (!IDENT.test(v)) throw new Error("refusing unsafe identifier literal");
  return `'${v}'`;
};

export const kubectl = (c: ProofConfig, ...args: string[]) => ["kubectl", "--context", c.kubeContext, ...args];

export function primaryStep(c: ProofConfig): Step {
  return {
    id: "hoopdb.primary",
    argv: kubectl(c, "-n", c.namespace, "get", "clusters.postgresql.cnpg.io", c.hoopdb.cluster, "-o", "jsonpath={.status.currentPrimary}"),
    mutating: false,
    touches: [`k8s:${c.namespace}/cnpg-cluster/${c.hoopdb.cluster}`],
    approval: "operator",
  };
}

/** psql inside the primary; SQL goes on stdin so it never shows up in a process list. */
export function sqlStep(c: ProofConfig, id: string, primary: string, sql: string): Step {
  return {
    id,
    argv: kubectl(c, "-n", c.namespace, "exec", "-i", primary || "<hoopdb primary>", "-c", "postgres", "--", "psql", "-d", c.hoopdb.database, "-X", "-At", "-v", "ON_ERROR_STOP=1", "-f", "-"),
    stdin: sql,
    mutating: false,
    touches: [`hoopdb:${c.hoopdb.cluster}/${c.hoopdb.database} (read)`],
    approval: "operator",
  };
}

/** Server-side stage timestamps (ms since epoch) for each trace. */
export function stageTimesSql(traceIds: readonly string[]): string {
  return `SELECT coalesce(json_agg(r), '[]') FROM (
  SELECT trace_id::text AS trace_id, stage, (extract(epoch FROM min(at)) * 1000)::bigint AS at_ms
    FROM sessions.pipeline_events
   WHERE trace_id = ANY(${sqlList(traceIds, "uuid")}) AND status = 'ok'
     AND stage IN ('object-landed', 'session-indexed', 'summary-short')
   GROUP BY 1, 2) r;`;
}

/**
 * Is the V1 search RPC callable the way the proof calls it, positionally as
 * (p_workspace_id text, p_query text)? Matched by name and leading arguments,
 * not by an exact signature: the session-directory contract
 * (SESSION_DIRECTORY_SEARCH_RPC, #38) defines 19 arguments with every one after
 * the first two defaulted, and an exact `(text,text)` lookup never matched it.
 * A function whose first two inputs are not (p_workspace_id text, p_query text),
 * or that has a later argument without a default, does not count.
 */
export const SEARCH_RPC_PRESENT_SQL = `EXISTS (
    SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = 'sessions' AND p.proname = 'search_session_directory'
       AND p.pronargs >= 2
       AND p.proargtypes[0] = 'pg_catalog.text'::pg_catalog.regtype AND p.proargtypes[1] = 'pg_catalog.text'::pg_catalog.regtype
       AND p.proargnames[1:2] = ARRAY['p_workspace_id', 'p_query']::text[]
       AND p.pronargdefaults >= p.pronargs - 2)`;

/** HoopDb index rows and directory summaries for the planted sessions. */
export function indexStateSql(sessionIds: readonly string[]): string {
  return `SELECT json_build_object(
  'index', (SELECT coalesce(json_agg(json_build_object('session_id', session_id, 'org_id', org_id, 'workspace_id', workspace_id)), '[]')
              FROM sessions.session_index WHERE session_id = ANY(${sqlList(sessionIds, "text")})),
  'directory_present', to_regclass('sessions.session_directory') IS NOT NULL
    AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'sessions' AND table_name = 'session_directory' AND column_name = 'short_summary'),
  'search_rpc_present', ${SEARCH_RPC_PRESENT_SQL});`;
}

/** Separate query: it references session_directory, which may not exist (then indexStateSql says so first). */
export function directorySql(sessionIds: readonly string[]): string {
  return `SELECT coalesce(json_agg(json_build_object(
  'session_id', session_id, 'org_id', org_id, 'short_summary', short_summary,
  'generated_at_ms', (extract(epoch FROM short_summary_generated_at) * 1000)::bigint,
  'deleted', deleted_at IS NOT NULL)), '[]')
  FROM sessions.session_directory WHERE session_id = ANY(${sqlList(sessionIds, "text")});`;
}

/**
 * V1 search as one tenant, many queries in one read-only transaction: SET ROLE
 * to the Worker role and set request.jwt.claims for this tenant only, then
 * roll back. Exercises the RPC's tenant binding exactly; does not exercise
 * JWT signature checks (#63). `ms` is server-side execution time of each RPC
 * call, measured with clock_timestamp(), so kubectl exec overhead is excluded.
 */
export function searchBatchSql(t: Tenant, queries: ReadonlyArray<{ tag: string; q: string }>): string {
  if (!IDENT.test(t.orgId) || !IDENT.test(t.workspaceId)) throw new Error("refusing unsafe tenant literal");
  if (queries.length === 0) throw new Error("empty search batch");
  // The RPC pins iss/aud/lifetime to sessions.session_directory_search_token_policy,
  // which only the owner can read, so the claims are built from it before SET ROLE.
  // One policy row and one captured epoch: exp = iat + the policy's own
  // max_lifetime_seconds (1..60), so a stricter policy is honoured and the
  // lifetime cannot straddle a second boundary.
  const claims = `SELECT set_config('request.jwt.claims', json_build_object(
  'role', 'session_directory_search_worker',
  'iss', p.issuer,
  'aud', p.audience,
  'iat', n.iat,
  'exp', n.iat + p.max_lifetime_seconds,
  'app_metadata', json_build_object('org_id', '${t.orgId}', 'workspace_ids', json_build_array('${t.workspaceId}')))::text, true)
  FROM sessions.session_directory_search_token_policy AS p,
       (SELECT floor(extract(epoch FROM clock_timestamp()))::bigint AS iat) AS n
 WHERE p.singleton \\g /dev/null`;
  const body = queries.map(({ tag, q }) => {
    if (!TERM.test(q)) throw new Error("refusing unsafe search term");
    if (!IDENT.test(tag)) throw new Error("refusing unsafe tag");
    return `SELECT set_config('proof.t0', clock_timestamp()::text, true) \\g /dev/null
SELECT json_build_object('tag', '${tag}',
  'rows', coalesce(json_agg(json_build_object('session_id', r.session_id, 'org_id', r.org_id, 'short_summary', r.short_summary)), '[]'),
  'ms', extract(epoch FROM clock_timestamp() - current_setting('proof.t0')::timestamptz) * 1000)
  FROM sessions.search_session_directory(${ident(t.workspaceId)}, '${q}') r;`;
  });
  return ["BEGIN READ ONLY;", claims, "SET LOCAL ROLE session_directory_search_worker;", ...body, "ROLLBACK;", ""].join("\n");
}

export interface SearchResultLine {
  tag: string;
  rows: SearchRow[];
  ms: number;
}

/** Every JSON object line psql printed, in order. */
export function jsonLines<T>(stdout: string): T[] {
  const out: T[] = [];
  for (const l of stdout.split("\n")) {
    const t = l.trim();
    if (!t.startsWith("{") && !t.startsWith("[")) continue;
    try {
      out.push(JSON.parse(t) as T);
    } catch {
      /* not ours */
    }
  }
  return out;
}

export interface SearchRow {
  session_id: string;
  org_id: string;
  short_summary: string | null;
}

/** psql -At prints one JSON document per SELECT; the last non-empty line is ours. */
export function lastJson<T>(stdout: string): T | undefined {
  const lines = stdout.split("\n").map((l) => l.trim()).filter((l) => l.startsWith("{") || l.startsWith("["));
  if (!lines.length) return undefined;
  try {
    return JSON.parse(lines.at(-1)!) as T;
  } catch {
    return undefined;
  }
}

/** S3: the manifest object exists (HEAD only, never GET of transcript bytes). */
export function s3HeadStep(c: ProofConfig, key: string): Step {
  return {
    id: "s3.head-manifest",
    argv: ["aws", "s3api", "head-object", "--region", c.region, "--bucket", c.locker.bucket, "--key", key, "--output", "json"],
    mutating: false,
    touches: [`s3:${c.locker.bucket}/${key} (head)`],
    approval: "operator",
  };
}

/** One listing per planted machine prefix: cheaper than N HEADs, and only keys come back. */
export function s3ListStep(c: ProofConfig, prefix: string): Step {
  return {
    id: "s3.list-run-prefix",
    argv: ["aws", "s3api", "list-objects-v2", "--region", c.region, "--bucket", c.locker.bucket, "--prefix", prefix, "--query", "Contents[].{Key:Key,LastModified:LastModified}", "--output", "json"],
    mutating: false,
    touches: [`s3:${c.locker.bucket}/${prefix}* (list)`],
    approval: "operator",
  };
}
