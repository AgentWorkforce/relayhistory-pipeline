import { createHash, randomUUID } from "node:crypto";
import { scopeSemanticSql, type SemanticBoundedSql } from "../search-features/semantic-copy-postgres.ts";
import type { ReceiptPosition, ReconciliationSource } from "./publication-reconciliation.ts";
import type { PublicationTarget } from "./publication-protocol.ts";
import { runPublicationRetryTurn } from "./publication-retry.ts";
import { postgresPublicationRetryStore } from "./publication-retry-postgres.ts";
import { validatedPublicationScope, type PublicationScopeBinding } from "./publication-scoped-rpc.ts";

/** Renewing an admission for the same scope does not erase its retry/audit state. */
export function publicationFleetScopeKey(binding: PublicationScopeBinding): string {
  const { tenant, target } = binding;
  return createHash("sha256").update(JSON.stringify(["publication-retry-scope-v1",
    tenant.orgId, tenant.workspaceId, tenant.userId, tenant.originId,
    target.generation, target.provider, target.bucket, target.region, target.keyRoot])).digest("hex");
}

export interface PublicationFleetOptions {
  enabled: boolean;
  /** Stable owner-configured deployment key, shared by every replica. */
  fleetId: string;
  target: PublicationTarget;
  /** Independently admitted scopes. Candidate discovery is never a source of authorization. */
  bindings: readonly PublicationScopeBinding[];
  receiptLimit: number;
  budgetMs: number;
  signal?: AbortSignal;
  /** Bind all inventory, claim and publication operations to this exact admission. */
  sourceFor(binding: PublicationScopeBinding, signal: AbortSignal):
    (ReconciliationSource & { scope: string }) | Promise<ReconciliationSource & { scope: string }>;
}

// Keep a per-organization origin cursor. Prune entries outside the current
// roster at each reservation so durable state stays bounded across roster churn.
const reserve = `WITH roster AS (
    SELECT * FROM jsonb_to_recordset($2::jsonb) AS r(org_key text,scope_key text)
  ) INSERT INTO sessions.publication_fleet_cursors AS f(fleet,last_org,last_scope,origins,lease_token,lease_until)
  SELECT $1,r.org_key,r.scope_key,jsonb_build_object(r.org_key,r.scope_key),$3,clock_timestamp()+$4::int * interval '1 millisecond'
  FROM roster r ORDER BY r.org_key COLLATE "C",r.scope_key COLLATE "C" LIMIT 1
  ON CONFLICT(fleet) DO UPDATE SET (last_org,last_scope,origins,lease_token,lease_until) = (
    SELECT chosen.org_key,chosen.scope_key,
      (SELECT coalesce(jsonb_object_agg(p.key,p.value),'{}'::jsonb) FROM jsonb_each(f.origins) p
       WHERE EXISTS(SELECT 1 FROM roster r WHERE r.org_key=p.key)) || jsonb_build_object(chosen.org_key,chosen.scope_key),
      excluded.lease_token,clock_timestamp()+$4::int * interval '1 millisecond'
    FROM (
      SELECT r.org_key,r.scope_key FROM roster r ORDER BY
        (r.org_key COLLATE "C" <= f.last_org COLLATE "C"),r.org_key COLLATE "C",
        (r.scope_key COLLATE "C" <= coalesce(f.origins->>r.org_key,'') COLLATE "C"),r.scope_key COLLATE "C" LIMIT 1
    ) chosen
  ) WHERE f.lease_token IS NULL OR f.lease_until<=clock_timestamp()
  RETURNING last_scope, floor(extract(epoch FROM clock_timestamp())*1000)::bigint::text AS now_ms`;

// The host source must cancel its transport. Also bound waiting for a faulty
// adapter; its late result cannot resume this turn or advance the checkpoint.
function sourceCall<T>(signal: AbortSignal, work: () => T | Promise<T>): Promise<T> {
  signal.throwIfAborted();
  return new Promise<T>((resolve,reject) => {
    const abort = () => reject(signal.reason);
    signal.addEventListener("abort",abort,{once:true});
    Promise.resolve().then(() => {signal.throwIfAborted(); return work();})
      .then(resolve,reject).finally(() => signal.removeEventListener("abort",abort));
  });
}

/** One scope, one retry/scan turn and at most receiptLimit reconciliation calls.
 * The reservation advances the durable cursor before I/O, so a failing source or
 * a crashed process cannot pin the fleet to one scope. With M stable organizations,
 * each gets one turn per M reservations. Its K origins each get one per M*K. Publication has
 * its own claim/lease/privacy fences; the scheduling lease grants no authority. */
export async function runPublicationFleetTurn(db: SemanticBoundedSql, options: PublicationFleetOptions):
  Promise<"disabled" | "empty" | "busy" | "committed" | "conflict"> {
  if (options.enabled !== true) return "disabled";
  if (typeof options.fleetId !== "string" || !/^[a-f0-9]{64}$/.test(options.fleetId) ||
      !Array.isArray(options.bindings) || options.bindings.length > 128 ||
      !Number.isInteger(options.receiptLimit) || options.receiptLimit < 1 || options.receiptLimit > 20 ||
      !Number.isInteger(options.budgetMs) || options.budgetMs < 1 || options.budgetMs > 60_000)
    throw Error("invalid publication fleet configuration");
  const { fleetId, receiptLimit, budgetMs, sourceFor } = options;
  const deadline = Date.now() + budgetMs;
  const timeout = AbortSignal.timeout(budgetMs);
  const signal = options.signal ? AbortSignal.any([options.signal,timeout]) : timeout;
  signal.throwIfAborted();
  const roster = new Map<string,PublicationScopeBinding>();
  for (const entry of options.bindings) {
    const copy = validatedPublicationScope(entry,options.target);
    const scope = publicationFleetScopeKey(copy);
    if (roster.has(scope)) throw Error("duplicate publication fleet scope");
    roster.set(scope,copy);
  }
  if (!roster.size) return "empty";
  const sql = scopeSemanticSql(db,{signal,deadline,now:Date.now});
  const lease = randomUUID();
  const schedule = Array.from(roster, ([scope_key, binding]) => ({
    org_key: createHash("sha256").update(binding.tenant.orgId).digest("hex"), scope_key,
  }));
  // Small recovery margin beyond the cancellation deadline for a client winding down.
  const { rows } = await sql.query<{ last_scope:string; now_ms:string }>(reserve,[fleetId,JSON.stringify(schedule),lease,budgetMs+5000]);
  if (!rows.length) return "busy";
  const selected = rows[0]!;
  try {
    const binding = roster.get(selected.last_scope);
    if (rows.length !== 1 || !binding) throw Error("invalid publication fleet reservation");
    signal.throwIfAborted();
    const source = await sourceCall(signal,() => sourceFor(binding,signal));
    // Each scheduling/transport boundary rechecks cancellation; the host source
    // must also propagate the signal through its in-flight SQL and object I/O.
    const checked = { scope: source.scope,
      highest() { return sourceCall(signal,() => source.highest()); },
      page(after: ReceiptPosition | null, through: ReceiptPosition, limit: number) {
        return sourceCall(signal,() => source.page(after,through,limit));
      },
      reconcile(token: string) { return sourceCall(signal,() => source.reconcile(token)); },
    };
    return await runPublicationRetryTurn(selected.last_scope,checked,postgresPublicationRetryStore(sql,selected.last_scope,{fleet:fleetId,token:lease}),receiptLimit,Number(selected.now_ms));
  } finally {
    // If time expired, let the short lease expire. Unlock only this reservation.
    if (!signal.aborted && Date.now() < deadline)
      await sql.query("UPDATE sessions.publication_fleet_cursors SET lease_token=NULL,lease_until=NULL WHERE fleet=$1 AND lease_token=$2", [fleetId,lease]);
  }
}
