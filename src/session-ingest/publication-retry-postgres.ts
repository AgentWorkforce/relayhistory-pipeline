import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { validateReconciliationState, type ReceiptPosition, type ReconciliationState } from "./publication-reconciliation.ts";
import { initialRetryCheckpoint, validateRetry, type ReceiptRetry, type ReceiptRetryStore } from "./publication-retry.ts";

type RetryRow = { token: string; attempts: number; state: ReceiptRetry["state"]; due_ms: string };
const stageToken = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const fields = "stage_token AS token, attempts, state, due_ms::text";

function retryFromRow(row: RetryRow) {
  const retry = { token: row.token, attempts: row.attempts, state: row.state, due: Number(row.due_ms) };
  validateRetry(retry, row.token);
  return retry;
}

// Both CAS paths return a row only to the winning writer. Retry mutations
// depend on that row; a constraint/connection failure cannot partially commit.
// A lost acknowledgement still requires a full scoped reload by the caller.
const insertFirst = (guard: string) => `INSERT INTO sessions.publication_retry_checkpoints(scope,version,scan,retry_turn)
  SELECT $1,$2::bigint,$3::jsonb,$4::boolean WHERE $5::bigint=0${guard}
  ON CONFLICT(scope) DO NOTHING RETURNING scope`;
const advance = (guard: string) => `UPDATE sessions.publication_retry_checkpoints
  SET version=$2::bigint,scan=$3::jsonb,retry_turn=$4::boolean
  WHERE scope=$1 AND version=$5::bigint${guard} RETURNING scope`;
interface FleetReservation { fleet: string; token: string }

function commitSql(cas: string, fenced: boolean) {
  // Serialize checkpoint advancement against another replica taking over an expired lease.
  const guard = fenced ? `reservation AS (SELECT 1 FROM sessions.publication_fleet_cursors
    WHERE fleet=$7 AND lease_token=$8 AND last_scope=$1 AND lease_until>clock_timestamp() FOR UPDATE),` : "";
  return `WITH ${guard} won AS (${cas}), changes AS (
    SELECT * FROM jsonb_to_recordset($6::jsonb) AS c(token uuid, attempts int, state text, due_ms bigint)
  ), upserts AS (
    INSERT INTO sessions.publication_receipt_retries(scope,stage_token,attempts,state,due_ms)
    SELECT won.scope,c.token,c.attempts,c.state,c.due_ms FROM won CROSS JOIN changes c WHERE c.state IS NOT NULL
    ON CONFLICT(scope,stage_token) DO UPDATE SET attempts=excluded.attempts,state=excluded.state,due_ms=excluded.due_ms
  ), deletions AS (
    DELETE FROM sessions.publication_receipt_retries r USING won,changes c
    WHERE r.scope=won.scope AND r.stage_token=c.token AND c.state IS NULL
  ) SELECT EXISTS(SELECT 1 FROM won) AS committed`;
}

/** Default-unwired scheduling storage. The caller binds the digest of its exact
 * authorized target and tenant when constructing the store. No claim authority. */
export function postgresPublicationRetryStore(db: SqlExecutor, boundScope: string, lease?: FleetReservation): ReceiptRetryStore {
  if (!/^[a-f0-9]{64}$/.test(boundScope)) throw Error("invalid receipt scope");
  function checkScope(scope: string) {
    if (scope !== boundScope) throw Error("receipt scope mismatch");
  }
  return {
    async load(scope) {
      checkScope(scope);
      const { rows } = await db.query<{ version: string; scan: ReconciliationState; retry_turn: boolean }>(
        "SELECT version::text, scan, retry_turn FROM sessions.publication_retry_checkpoints WHERE scope=$1", [scope]);
      if (!rows.length) return initialRetryCheckpoint();
      const { version, scan, retry_turn } = rows[0]!;
      validateReconciliationState(scan);
      const n = Number(version);
      if (!Number.isSafeInteger(n) || n < 1 || typeof retry_turn !== "boolean") throw Error("invalid stored checkpoint");
      return { version: n, scan, retryTurn: retry_turn };
    },
    async lookup(scope, token) {
      checkScope(scope);
      if (!stageToken.test(token)) throw Error("invalid receipt token");
      const { rows } = await db.query<RetryRow>(
        `SELECT ${fields} FROM sessions.publication_receipt_retries WHERE scope=$1 AND stage_token=$2`, [scope, token]);
      return rows.length ? retryFromRow(rows[0]!) : null;
    },
    async due(scope, now, limit) {
      checkScope(scope);
      if (!Number.isSafeInteger(now) || now < 0 || !Number.isInteger(limit) || limit < 1 || limit > 20)
        throw Error("invalid retry budget");
      const { rows } = await db.query<RetryRow>(
        `SELECT ${fields} FROM sessions.publication_receipt_retries
          WHERE scope=$1 AND state='retry' AND due_ms <= $2 ORDER BY publication_receipt_retries.due_ms,stage_token LIMIT $3`, [scope, now, limit]);
      return rows.map(retryFromRow);
    },
    async commit(scope, version, next, changes) {
      checkScope(scope);
      if (!Number.isSafeInteger(version) || version < 0 || !Number.isSafeInteger(next.version) ||
          next.version !== version + 1 || typeof next.retryTurn !== "boolean" || changes.size > 20)
        throw Error("invalid checkpoint transition");
      validateReconciliationState(next.scan);
      // Only persist the scheduling positions. Never serialize arbitrary source fields.
      const position = (p: ReceiptPosition | null) => p === null ? null : { createdAt: p.createdAt, stageToken: p.stageToken };
      const scan = { lane: next.scan.lane, catchup: { after: position(next.scan.catchup.after), through: position(next.scan.catchup.through) },
        tailAfter: position(next.scan.tailAfter) };
      const mutations = Array.from(changes, ([token, retry]) => {
        if (!stageToken.test(token)) throw Error("invalid receipt token");
        if (!retry) return { token, state: null };
        validateRetry(retry, token);
        return { token, state: retry.state, attempts: retry.attempts, due_ms: retry.due };
      });
      const guard = lease ? " AND EXISTS (SELECT 1 FROM reservation)" : "";
      const cas = version === 0 ? insertFirst(guard) : advance(guard);
      const { rows } = await db.query<{ committed: boolean }>(commitSql(cas, !!lease),
        [scope, next.version, JSON.stringify(scan), next.retryTurn, version, JSON.stringify(mutations),
          ...(lease ? [lease.fleet, lease.token] : [])]);
      if (rows.length !== 1 || typeof rows[0]!.committed !== "boolean") throw Error("unknown checkpoint outcome");
      return rows[0]!.committed;
    },
  };
}
