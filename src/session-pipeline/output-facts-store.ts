/**
 * Where a pass's output facts (output-facts.ts outputFactsFromScope) are kept.
 *
 * - clickhouseOutputFactsStore: today's ClickHouse functions, called exactly as before
 *   (SEARCH_INDEX_OUTPUT_FACTS=1 with the ClickHouse search binding).
 * - postgresOutputFactsStore: the self-host Postgres tables of migration 20261105045020,
 *   written through its SECURITY DEFINER functions on the consumer's transaction
 *   (RELAYHISTORY_SELFHOST_OUTPUT_FACTS=true, consumer-config.ts). One transaction holds
 *   the directory events and the facts, so a failed pass leaves neither.
 *
 * Ranges are ClickHouse DateTime64(6) text ("YYYY-MM-DD HH:MM:SS.ffffff", UTC) on both
 * sides of the interface, as the builder emits them.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import type { ClickHouseClient } from "./vendor/search-index/clickhouse.ts";
import {
  eraseOutputFacts,
  outputFactsSchema,
  previousOutputFactsState,
  reconcileOutputFacts,
  recordOutputFactsState,
  type EventFactRow,
  type FactsRange,
  type OutputFactRow,
  type OutputFactsScope,
  type StatePhase,
} from "./output-facts.ts";

export interface OutputFactsReconcileInput {
  eventFacts: EventFactRow[];
  outputFacts: OutputFactRow[];
  /** False when a source could not be read exactly: the reconcile then never deletes. */
  complete: boolean;
  /** The builder's reasons (OutputFacts.reasons). */
  reasons: string[];
}

export interface OutputFactsReconcileResult {
  inserted: number;
  deleted: number;
  complete: boolean;
  reasons: string[];
  /** False when the store refused the pass as stale (an older source version): nothing changed. */
  applied: boolean;
}

export interface OutputFactsStore {
  /** The scope's recorded span and highest recorded version (0 when none). */
  previousState(scope: OutputFactsScope): Promise<{ range: FactsRange; version: number }>;
  /** Make the scope's facts equal the input; deletes only when complete. */
  reconcile(scope: OutputFactsScope, input: OutputFactsReconcileInput, sourceVersion: number): Promise<OutputFactsReconcileResult>;
  /** Record the scope's facts state; false when refused as stale. */
  recordState(
    scope: OutputFactsScope,
    state: { complete: boolean; at: Date; range: FactsRange; phase?: StatePhase; sourceVersion: number },
  ): Promise<boolean>;
  /** Erasure: every fact (and, where the store keeps one per scope, state row) of the scope goes. */
  erase(scope: OutputFactsScope): Promise<void>;
  /** `present`: the fact tables exist (erasure must purge them); `ready`: a pass may write. */
  schemaReady(): Promise<{ ready: boolean; present: boolean }>;
}

/** Today's ClickHouse functions behind the interface, unchanged. */
export function clickhouseOutputFactsStore(ch: ClickHouseClient, opts: { maxRows: number; insertBatch: number }): OutputFactsStore {
  return {
    previousState: (scope) => previousOutputFactsState(ch, scope),
    async reconcile(scope, input) {
      // The ClickHouse rows carry their ReplacingMergeTree version already (the pass base).
      const r = await reconcileOutputFacts(ch, scope, { events: input.eventFacts, facts: input.outputFacts, complete: input.complete, reasons: input.reasons }, opts);
      return { ...r, applied: true };
    },
    async recordState(scope, s) {
      await recordOutputFactsState(ch, scope, s.complete, s.at, s.range, s.phase ?? "final", s.sourceVersion);
      return true;
    },
    erase: (scope) => eraseOutputFacts(ch, scope),
    schemaReady: () => outputFactsSchema(ch),
  };
}

/** DateTime64(6) text (UTC, no zone) -> an ISO instant Postgres reads the same in any TimeZone. */
export function pgInstant(chText: string): string {
  if (!/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?$/.test(chText)) throw new Error("output_facts_invalid_time");
  return `${chText.replace(" ", "T")}Z`;
}

const CH_TEXT = (column: string) => `to_char(${column} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')`;
const scopeArgs = (scope: OutputFactsScope) => [scope.orgId, scope.workspaceId, scope.harness, scope.sessionIds];

function sourceVersionOf(v: number): number {
  if (!Number.isSafeInteger(v) || v < 0) throw new Error("output_facts_invalid_source_version");
  return v;
}

/**
 * The self-host Postgres store on one transaction (migration 20261105045020). The source
 * version is the session directory event_projection_epoch the pass read: a pass older than the
 * stored one is a no-op in the database. There is no pending phase: the writes are
 * transactional, so a dead pass leaves the prior state intact.
 */
export function postgresOutputFactsStore(tx: SqlExecutor): OutputFactsStore {
  return {
    async previousState(scope) {
      const { rows } = await tx.query<{ lo: string | null; hi: string | null; v: string | number | null }>(
        `SELECT ${CH_TEXT("s.min_ts")} AS lo, ${CH_TEXT("s.max_ts")} AS hi, s.source_version AS v
           FROM sessions.output_facts_state($1, $2, $3, $4::text[], $5) AS s`,
        [...scopeArgs(scope), scope.canonicalId],
      );
      const r = rows[0];
      return { range: r?.lo && r.hi ? { min: r.lo, max: r.hi } : null, version: Number(r?.v ?? 0) };
    },
    async reconcile(scope, input, sourceVersion) {
      const v = sourceVersionOf(sourceVersion);
      // `version` is the ClickHouse row version: the Postgres rows carry source_version instead.
      const events = input.eventFacts.map(({ version: _v, ...row }) => ({ ...row, ts: pgInstant(row.ts) }));
      const facts = input.outputFacts.map(({ version: _v, ...row }) => ({ ...row, ts: pgInstant(row.ts) }));
      const { rows } = await tx.query<{ applied: boolean; inserted: string | number; deleted: string | number }>(
        `SELECT applied, inserted, deleted FROM sessions.output_facts_reconcile($1, $2, $3, $4::text[], $5, $6::jsonb, $7::jsonb, $8, $9::bigint)`,
        [...scopeArgs(scope), scope.canonicalId, JSON.stringify(events), JSON.stringify(facts), input.complete, v],
      );
      const r = rows[0];
      const applied = r?.applied === true;
      return {
        inserted: Number(r?.inserted ?? 0),
        deleted: Number(r?.deleted ?? 0),
        complete: applied && input.complete,
        reasons: applied ? [...new Set(input.reasons)] : [...new Set([...input.reasons, "output_facts_stale_source_version"])],
        applied,
      };
    },
    async recordState(scope, s) {
      const v = sourceVersionOf(s.sourceVersion);
      const { rows } = await tx.query<{ ok: boolean }>(
        `SELECT sessions.output_facts_record_state($1, $2, $3, $4::text[], $5, $6, $7::timestamptz, $8::timestamptz, $9::bigint, $10::timestamptz) AS ok`,
        [...scopeArgs(scope), scope.canonicalId, s.complete && (s.phase ?? "final") === "final",
          s.range ? pgInstant(s.range.min) : null, s.range ? pgInstant(s.range.max) : null, v, s.at.toISOString()],
      );
      return rows[0]?.ok === true;
    },
    async erase(scope) {
      await tx.query(`SELECT sessions.output_facts_erase($1, $2, $3, $4::text[])`, scopeArgs(scope));
    },
    // The tables come with the migration the consumer requires before it serves.
    schemaReady: async () => ({ ready: true, present: true }),
  };
}
