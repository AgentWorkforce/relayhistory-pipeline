/**
 * Ingest DB-error alert (runbooks/history-620-operations.md, "Ingest DB-error alert"): one Cloudflare
 * Notifications custom alert (`custom_analytics`, a SQL API query over Workers Logs) that fires when more than
 * 20% of the production buffered-ingest Worker's deliveries failed with a database error over the last 2 minutes,
 * notifying one generic webhook destination (Agent Relay).
 *
 * Signal, per request line of the private Worker `96bulls-session-ingest` (src/session-ingest/app.ts acceptBatch
 * and the recovery route; one terminal line per request):
 *   numerator    `route` in ROUTES, `outcome` = `delivery_unavailable` (the 503) and `db_error_class` in the
 *                closed DB_ERROR_CLASSES set (src/session-ingest/db-error-class.ts), i.e. the 503 carries a
 *                database-client diagnosis, and that is not a KNOWN per-org/per-user 55000 refusal: `db_refusal`
 *                not in EXCLUDED_REFUSALS (every DB_REFUSALS class except the generation fence and `other`), so
 *                one stuck user's `user_seal_missing` (etc.) cannot page, while an unrecognised 55000 (`other`,
 *                e.g. a reworded or new fence message) still counts: the fleet-wide fence outage this alert
 *                exists for must page. A 503 without a DB diagnosis (object store, custody, admission) is not
 *                counted.
 *   denominator  every line with `route` in ROUTES (a missing `outcome` reads as ''), minus the capture-policy
 *                report lines that reuse the route
 *                (`outcome` missing / stale / session_predates), floored at MIN_DELIVERIES so a quiet window
 *                cannot fire on one or two failures (below the floor the value is failures / MIN_DELIVERIES).
 * A claim converted to 429 `delivery_busy` (INGEST_CLAIM_LOCK_BUSY / INGEST_CLAIM_TIMEOUT_BUSY) is designed
 * back-pressure, not a failed delivery: it counts in the denominator only. So does a `buffer_intent` /
 * `buffer_receipt` lock wait converted to 429 by INGEST_BUFFER_LOCK_BUSY (55P03 or 40P01), and, with
 * INGEST_BUFFER_TIMEOUT_BUSY also on, a statement timeout there (57014); this query does not see it. Those stay visible through the `ingest.lock_alert` line (src/session-ingest/app.ts lockAlertFields,
 * `staged_intent` / `staged_ack`), which for that conversion also fires for 40P01. A lock wait absorbed by an
 * INGEST_BUFFER_LOCK_RETRY retry shows only on its `ingest.buffer_lock_retry` warn line.
 *
 * The Worker profile (stacks/cloudflare/session-ingest.ts, config/cloudflare/private-ingest-production.json) and
 * the private-ingest release contract are untouched: the alert only reads the Worker's existing log lines. It is
 * applied by .github/workflows/ingest-error-alert.yml (scripts/ingest-error-alert.ts), never by the Worker deploy.
 *
 * Pure apart from the injected Cloudflare client. Errors carry fixed codes only; the webhook URL and secret are
 * never read, printed or stored here (Cloudflare never returns the secret).
 */
import { createHash } from "node:crypto";
import { DB_ERROR_CLASSES } from "./db-error-class.ts";
import { DB_REFUSALS, GENERATION_REFUSALS } from "./db-refusal-class.ts";

export const INGEST_ERROR_ALERT = Object.freeze({
  /** Deterministic policy name: the apply finds the live policy by it. */
  name: "96bulls-session-ingest-db-error-rate",
  /** Same value as stacks/cloudflare/session-ingest.ts SESSION_INGEST_WORKER (pinned by the test). */
  worker: "96bulls-session-ingest",
  routes: Object.freeze(["delivery.batches", "turns.batches", "delivery.recovery"] as const),
  outcome: "delivery_unavailable",
  /** Capture-policy report lines that share the request's route (capture-policy.ts UploadViolation). */
  reportOutcomes: Object.freeze(["missing", "stale", "session_predates"] as const),
  /** Fire when the ratio is strictly above this. */
  threshold: 0.2,
  window: "2m",
  windowMinutes: 2,
  every: "1m",
  minDeliveries: 20,
  /** Re-notify while still firing. */
  alertInterval: "30m",
  /** The protected GitHub environment holding the alerting token and the destination id. */
  environment: "ingest-alerting-production",
  workflow: ".github/workflows/ingest-error-alert.yml",
  /** The Notifications webhook destination name the operator creates (generic type: Agent Relay). */
  destinationName: "96bulls-ingest-alerts-agent-relay",
});

const ACCOUNT = /^[0-9a-f]{32}$/u;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const WEBHOOK_ID = /^[0-9a-f]{32}$|^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;
const SHA = /^[0-9a-f]{40}$/u;

export function refuseIngestErrorAlert(code: string): never {
  throw Object.assign(new Error(code), { code });
}
const refuse: (code: string) => never = refuseIngestErrorAlert;

const list = (values: readonly string[]) => values.map((v) => `'${v}'`).join(", ");
/** `db_refusal` classes that do NOT count: the known per-org/per-user refusals. Absent (''), the generation fence
 * and `other` (an unrecognised 55000, possibly a reworded fence) count. In DB_REFUSALS order. */
export const EXCLUDED_REFUSALS: readonly string[] = Object.freeze(
  DB_REFUSALS.filter((r) => r !== "other" && !GENERATION_REFUSALS.has(r)),
);

/** The SQL API query: one row, one numeric `value` column (the DB-error delivery ratio over the window). */
export function ingestErrorAlertQuery(accountId: string): string {
  if (!ACCOUNT.test(accountId)) refuse("ingest_error_alert_account");
  const A = INGEST_ERROR_ALERT;
  return [
    // A missing attribute reads as '' through ifNull whether the dataset returns '' or NULL for an absent key.
    `SELECT toFloat64(countIf(ifNull(attributes['outcome'], '') = '${A.outcome}'`,
    `    AND ifNull(attributes['db_error_class'], '') IN (${list(DB_ERROR_CLASSES)})`,
    `    AND ifNull(attributes['db_refusal'], '') NOT IN (${list(EXCLUDED_REFUSALS)})))`,
    `  / greatest(count(), ${A.minDeliveries}) AS value`,
    `FROM logs.workersLogs`,
    `WHERE accountTag = '${accountId}'`,
    `  AND scriptName = '${A.worker}'`,
    `  AND timestamp >= NOW() - INTERVAL '${A.windowMinutes}' MINUTE`,
    `  AND attributes['route'] IN (${list(A.routes)})`,
    `  AND ifNull(attributes['outcome'], '') NOT IN (${list(A.reportOutcomes)})`,
  ].join("\n");
}

/** The exact Notifications policy body (POST/PUT /accounts/{id}/alerting/v3/policies), wire names. */
export interface IngestErrorAlertPolicy {
  name: string;
  alert_type: "custom_analytics";
  enabled: true;
  description: string;
  alert_interval: string;
  mechanisms: { webhooks: { id: string }[] };
  filters: Record<string, string[]>;
}

export function ingestErrorAlertPolicy(accountId: string, webhookId: string): IngestErrorAlertPolicy {
  if (!WEBHOOK_ID.test(webhookId)) refuse("ingest_error_alert_webhook_id");
  const A = INGEST_ERROR_ALERT;
  return {
    name: A.name,
    alert_type: "custom_analytics",
    enabled: true,
    description: `DB-error 503s above ${A.threshold * 100}% of ${A.worker} deliveries over ${A.window} ` +
      `(runbooks/history-620-operations.md, Ingest DB-error alert)`,
    alert_interval: A.alertInterval,
    mechanisms: { webhooks: [{ id: webhookId }] },
    filters: {
      source: ["sql_api"],
      selectors: [ingestErrorAlertQuery(accountId)],
      execution_interval: [A.every],
      alert_trigger_preferences: ["threshold"],
      alert_trigger_preferences_operator: [">"],
      alert_trigger_preferences_value: [String(A.threshold)],
      alert_trigger_preferences_short_window: [A.window],
    },
  };
}

/** Canonical JSON (sorted keys) so the digest is stable across key order. */
function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object")
    return Object.fromEntries(Object.keys(value).sort().map((k) => [k, canonical((value as Record<string, unknown>)[k])]));
  return value;
}
export function ingestErrorAlertDigest(policy: IngestErrorAlertPolicy): string {
  return createHash("sha256").update(JSON.stringify(canonical(policy))).digest("hex");
}

/** A live policy reduced to the fields the body owns, in the body's shape (extra live keys ignored). */
export function observedPolicy(live: Record<string, any>): IngestErrorAlertPolicy {
  const filters: Record<string, string[]> = {};
  const desiredKeys = Object.keys(ingestErrorAlertPolicy("0".repeat(32), "0".repeat(32)).filters);
  for (const k of desiredKeys) if (Array.isArray(live.filters?.[k])) filters[k] = live.filters[k].map(String);
  return {
    name: live.name,
    alert_type: live.alert_type,
    enabled: live.enabled,
    description: live.description ?? "",
    alert_interval: live.alert_interval ?? "",
    mechanisms: { webhooks: (live.mechanisms?.webhooks ?? []).map((w: { id?: unknown }) => ({ id: String(w?.id) })) },
    filters,
  } as IngestErrorAlertPolicy;
}

/** Field names that differ between desired and observed (names only: never values). */
export function policyDrift(desired: IngestErrorAlertPolicy, live: IngestErrorAlertPolicy): string[] {
  const out: string[] = [];
  for (const k of ["name", "alert_type", "enabled", "description", "alert_interval"] as const)
    if (desired[k] !== live[k]) out.push(k);
  if (JSON.stringify(desired.mechanisms) !== JSON.stringify(live.mechanisms)) out.push("mechanisms");
  for (const k of Object.keys(desired.filters).sort())
    if (JSON.stringify(desired.filters[k]) !== JSON.stringify(live.filters[k])) out.push("filters." + k);
  return out;
}

/** Cloudflare v4 client limited to this account's alerting API: returns `result`, throws a fixed code. */
export type AlertingApi = (method: "GET" | "POST" | "PUT", path: string, body?: unknown) => Promise<any>;

export function cloudflareAlertingApi(
  token: string,
  accountId: string,
  mode: "plan" | "apply",
  fetcher: typeof fetch = fetch,
): AlertingApi {
  if (!token) refuse("ingest_error_alert_token");
  if (!ACCOUNT.test(accountId)) refuse("ingest_error_alert_account");
  return async (method, path, body) => {
    if (!/^\/alerting\/v3\/(?:policies(?:\/[0-9a-f-]{32,36})?|destinations\/webhooks\/[0-9a-f-]{32,36})$/u.test(path))
      refuse("ingest_error_alert_api_path");
    if (method !== "GET" && (mode !== "apply" || !path.startsWith("/alerting/v3/policies")))
      refuse("ingest_error_alert_plan_write");
    const response = await fetcher(`https://api.cloudflare.com/client/v4/accounts/${accountId}${path}`, {
      method,
      headers: { Authorization: "Bearer " + token, ...(body === undefined ? {} : { "Content-Type": "application/json" }) },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      redirect: "error",
      signal: AbortSignal.timeout(20_000),
    });
    const json = (await response.json().catch(() => null)) as
      | { success?: boolean; result?: unknown; result_info?: { total_pages?: unknown; total_count?: unknown } }
      | null;
    if (!response.ok || json?.success !== true) refuse(response.status === 404 ? "ingest_error_alert_not_found" : "ingest_error_alert_api");
    // The policy list is read in one page; a list Cloudflare reports as longer is refused, never half-read.
    const info = json!.result_info;
    if (path === "/alerting/v3/policies" && method === "GET" && info &&
        ((typeof info.total_pages === "number" && info.total_pages > 1) ||
         (typeof info.total_count === "number" && Array.isArray(json!.result) && info.total_count > json!.result.length)))
      refuse("ingest_error_alert_paginated");
    return json!.result;
  };
}

export interface IngestErrorAlertPlan {
  action: "create" | "update" | "noop";
  policyId: string | null;
  drift: string[];
  digest: string;
  policy: IngestErrorAlertPolicy;
}

/** Read-only: the destination exists and is generic, and the live policy (by name) is absent, drifted or equal. */
export async function planIngestErrorAlert(api: AlertingApi, accountId: string, webhookId: string): Promise<IngestErrorAlertPlan> {
  const policy = ingestErrorAlertPolicy(accountId, webhookId);
  const destination = await api("GET", "/alerting/v3/destinations/webhooks/" + webhookId);
  if (destination?.type !== "generic" || destination?.name !== INGEST_ERROR_ALERT.destinationName)
    refuse("ingest_error_alert_destination");
  const live = await api("GET", "/alerting/v3/policies");
  if (!Array.isArray(live)) refuse("ingest_error_alert_api");
  const named = live.filter((p: { name?: unknown }) => p?.name === policy.name);
  if (named.length > 1) refuse("ingest_error_alert_duplicate_policy");
  const digest = ingestErrorAlertDigest(policy);
  if (!named.length) return { action: "create", policyId: null, drift: [], digest, policy };
  const id = String(named[0].id);
  if (!UUID.test(id)) refuse("ingest_error_alert_api");
  if (named[0].alert_type !== "custom_analytics") refuse("ingest_error_alert_name_taken");
  const drift = policyDrift(policy, observedPolicy(named[0]));
  return { action: drift.length ? "update" : "noop", policyId: id, drift, digest, policy };
}

/** `APPLY-INGEST-DB-ERROR-ALERT <sha> BODY <digest>`: binds an apply to this commit and this exact body. */
export function ingestErrorAlertConfirmation(sha: string, digest: string): string {
  return `APPLY-INGEST-DB-ERROR-ALERT ${sha} BODY ${digest}`;
}

/** Re-plan, check the confirmation, create or update once, then read back and require zero drift. A refusal after
 * the write (lost response, readback drift) can leave the new or updated policy live: re-plan to learn the true
 * state before anything else. */
export async function applyIngestErrorAlert(
  api: AlertingApi,
  input: { accountId: string; webhookId: string; sha: string; confirmation: string },
): Promise<IngestErrorAlertPlan & { readback: "equal" }> {
  if (!SHA.test(input.sha)) refuse("ingest_error_alert_sha");
  const plan = await planIngestErrorAlert(api, input.accountId, input.webhookId);
  if (input.confirmation !== ingestErrorAlertConfirmation(input.sha, plan.digest)) refuse("ingest_error_alert_confirmation");
  let id = plan.policyId;
  if (plan.action === "create") {
    const created = await api("POST", "/alerting/v3/policies", plan.policy);
    id = String(created?.id ?? "");
    if (!UUID.test(id)) refuse("ingest_error_alert_api");
  } else if (plan.action === "update") {
    await api("PUT", "/alerting/v3/policies/" + id, plan.policy);
  }
  const readback = observedPolicy(await api("GET", "/alerting/v3/policies/" + id));
  if (policyDrift(plan.policy, readback).length) refuse("ingest_error_alert_readback");
  return { ...plan, policyId: id, readback: "equal" };
}

export interface IngestErrorAlertDispatch {
  mode: "plan" | "apply";
  sha: string;
  confirmation: string;
}

/** The workflow inputs (env), checked before any credential is read. Plan takes no confirmation. */
export function ingestErrorAlertDispatch(env: Record<string, string | undefined>): IngestErrorAlertDispatch {
  const mode = env.INGEST_ALERT_MODE, sha = env.INGEST_ALERT_SHA ?? "", confirmation = env.INGEST_ALERT_CONFIRMATION ?? "";
  if (mode !== "plan" && mode !== "apply") refuse("ingest_error_alert_mode");
  if (!SHA.test(sha)) refuse("ingest_error_alert_sha");
  if (mode === "plan" && confirmation !== "") refuse("ingest_error_alert_confirmation");
  if (mode === "apply" && !/^APPLY-INGEST-DB-ERROR-ALERT [0-9a-f]{40} BODY [0-9a-f]{64}$/u.test(confirmation))
    refuse("ingest_error_alert_confirmation");
  return { mode, sha, confirmation };
}

/** The account comes only from the tracked private-ingest profile; the environment must name the same one. */
export function ingestErrorAlertAccount(profile: unknown, env: Record<string, string | undefined>): string {
  const accountId = (profile as { sessionIngest?: { accountId?: unknown } } | null)?.sessionIngest?.accountId;
  if (typeof accountId !== "string" || !ACCOUNT.test(accountId)) refuse("ingest_error_alert_account");
  if (env.CLOUDFLARE_ACCOUNT_ID !== accountId) refuse("ingest_error_alert_account_mismatch");
  return accountId;
}

/** The query's predicates over one parsed log line (tests pin the Worker's real lines against them). */
export function countsAsDelivery(line: Record<string, unknown>): boolean {
  return (INGEST_ERROR_ALERT.routes as readonly unknown[]).includes(line.route) &&
    !(INGEST_ERROR_ALERT.reportOutcomes as readonly unknown[]).includes(line.outcome);
}
export function countsAsDbErrorDelivery(line: Record<string, unknown>): boolean {
  return countsAsDelivery(line) && line.outcome === INGEST_ERROR_ALERT.outcome &&
    (DB_ERROR_CLASSES as readonly unknown[]).includes(line.db_error_class) &&
    !EXCLUDED_REFUSALS.includes(typeof line.db_refusal === "string" ? line.db_refusal : "");
}
/** The value the query computes over a window's lines. */
export function ingestErrorAlertValue(lines: readonly Record<string, unknown>[]): number {
  const total = lines.filter(countsAsDelivery);
  return total.filter(countsAsDbErrorDelivery).length / Math.max(total.length, INGEST_ERROR_ALERT.minDeliveries);
}
