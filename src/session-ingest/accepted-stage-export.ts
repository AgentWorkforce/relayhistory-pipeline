/**
 * The one-time accepted-stage export (docs/decisions/0005-s3-native-upload-ledger.md, "The buffered backlog",
 * step 0b). S3 alone cannot tell an accepted stage from an abandoned one: a stage object can exist after
 * `putBufferedStage` while its receipt was never committed (for example the consent gate refused it between the
 * PUT and the ACK). This reads, once and read-only, which stages SQL acknowledged and records them in S3, so the
 * drain (buffered-drain.ts) writes ONLY those:
 *
 *   acknowledged  ingest_buffer_outbox (state 'buffered') -> its active_stage_token's ingest_stage_intents row
 *                 (receipt_state 'acknowledged', cleanup_state 'live': the publisher's own selection,
 *                 20261104060000) whose ingest_batches row is committed with the same digest. The active stage
 *                 token is the CURRENT stage of the batch, so a GC replacement is named and its original is not.
 *   per stage     a create-only sidecar `<stage key>.accepted.json` {v, kind, stage_sha256} next to the stage:
 *                 the drain sees it in the same listing as the stage and checks the sha against the bytes.
 *   per org       `<keyRoot>/.drain/accepted-stages/v1/<org>.complete.json`, written only once every acknowledged
 *                 stage of that org has its sidecar. Without it the drain writes nothing of the org.
 *
 * SQL: one `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` snapshot, checked with `SHOW
 * transaction_read_only`, always ended by ROLLBACK; SELECTs only, keyset-paged by stage key, optionally scoped to
 * a set of orgs. S3: create-only sidecars (an identical one is a no-op; a different one is `conflict` and the
 * org stays incomplete), so a rerun converges. Counts only: never a key, tenant id or hash.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import { ACCEPTED_SUFFIX, acceptedCompleteKey, parseAcceptedComplete, parseAcceptedMarker, parseStagedKey, type AcceptedComplete, type AcceptedMarker } from "./buffered-drain.ts";
import { liveMachineId } from "./writer.ts";
import { layoutComponent } from "../shared/layout-component.ts";

const ORG = /^(?!\.)[A-Za-z0-9._-]{1,128}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const sha256Hex = (v: string) => createHash("sha256").update(v).digest("hex");
export const MAX_EXPORT_CONCURRENCY = 16;

export interface ExportSql {
  query(sql: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
}

export interface ExportOptions {
  sql: ExportSql;
  store: ObjectStore;
  keyRoot: string;
  dryRun: boolean;
  orgs?: string[];
  pageSize?: number;
  concurrency?: number;
  statementTimeoutMs?: number;
  nowMs?: () => number;
}

export interface ExportSummary {
  mode: "dry_run" | "execute";
  /** Acknowledged stages SQL lists. */
  rows: number;
  written: number;
  already: number;
  would_write: number;
  /** A row whose stage key, tenant or sha does not bind (never written). */
  invalid: number;
  /** A sidecar already there naming other bytes (never overwritten). */
  conflict: number;
  failed: number;
  orgs: { complete: number; would_complete: number; incomplete: number };
}

/** Orgs with any stage at all, so an org whose stages were all refused still completes (with 0). */
const ORGS = `
SELECT DISTINCT org_id FROM sessions.ingest_stage_intents
WHERE ($1::text[] IS NULL OR org_id = ANY($1::text[]))
ORDER BY org_id`;
const ACKNOWLEDGED_PAGE = `
SELECT s.stage_key, s.stage_sha256, s.claim_token::text AS claim_token, s.org_id, s.workspace_id, s.user_id, s.origin_id, s.digest
FROM sessions.ingest_buffer_outbox AS o
JOIN sessions.ingest_stage_intents AS s ON s.stage_token = o.active_stage_token
JOIN sessions.ingest_batches AS b
  ON b.org_id = o.org_id AND b.workspace_id = o.workspace_id AND b.origin_id = o.origin_id AND b.batch_id = o.batch_id
WHERE o.state = 'buffered' AND s.receipt_state = 'acknowledged' AND s.cleanup_state = 'live'
  AND b.state = 'committed' AND b.digest = o.digest
  AND ($1::text[] IS NULL OR o.org_id = ANY($1::text[]))
  AND s.stage_key > $2::text
ORDER BY s.stage_key
LIMIT $3::int`;

async function mapLimit<T>(items: readonly T[], limit: number, fn: (item: T) => Promise<void>): Promise<void> {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) await fn(items[next++]!);
  }));
}

/** The stage key must be exactly the one this row's tenant, batch and bytes produce (staging.ts stagedKey). */
function binds(keyRoot: string, r: Record<string, unknown>): boolean {
  const key = r.stage_key, sha = r.stage_sha256;
  if (typeof key !== "string" || typeof sha !== "string" || !HEX64.test(sha)) return false;
  const p = parseStagedKey(key);
  if (!p || p.keyRoot !== keyRoot || p.orgId !== r.org_id || typeof r.workspace_id !== "string" || typeof r.origin_id !== "string" ||
      typeof r.user_id !== "string" || typeof r.digest !== "string" || typeof r.claim_token !== "string") return false;
  return p.workspace === layoutComponent(r.workspace_id) && p.machine === liveMachineId(r.workspace_id, r.origin_id, r.user_id) &&
    p.batch32 === sha256Hex(r.digest).slice(0, 32) && p.claim32 === sha256Hex(r.claim_token).slice(0, 32) && p.content16 === sha.slice(0, 16);
}

export async function exportAcceptedStages(opts: ExportOptions): Promise<ExportSummary> {
  const pageSize = opts.pageSize ?? 1_000;
  const concurrency = opts.concurrency ?? 8;
  const statementTimeoutMs = opts.statementTimeoutMs ?? 30_000;
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 10_000) throw new Error("export_page_size_invalid");
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > MAX_EXPORT_CONCURRENCY) throw new Error("export_concurrency_invalid");
  if (!Number.isInteger(statementTimeoutMs) || statementTimeoutMs < 1_000 || statementTimeoutMs > 600_000) throw new Error("export_statement_timeout_invalid");
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(opts.keyRoot)) throw new Error("export_key_root_invalid");
  if (opts.orgs) for (const org of opts.orgs) if (!ORG.test(org)) throw new Error("export_org_invalid");
  const orgScope = opts.orgs && opts.orgs.length ? [...new Set(opts.orgs)] : null;
  // The dry run's defence in depth: every mutation throws.
  const store: ObjectStore = opts.dryRun
    ? new Proxy(opts.store, {
        get(target, prop) {
          if (prop === "put" || prop === "delete") return async () => { throw new Error("export_dry_run_write_refused"); };
          const value = (target as unknown as Record<PropertyKey, unknown>)[prop];
          return typeof value === "function" ? (value as (...a: unknown[]) => unknown).bind(target) : value;
        },
      })
    : opts.store;
  const summary: ExportSummary = {
    mode: opts.dryRun ? "dry_run" : "execute", rows: 0, written: 0, already: 0, would_write: 0, invalid: 0, conflict: 0, failed: 0,
    orgs: { complete: 0, would_complete: 0, incomplete: 0 },
  };
  /** Per org: acknowledged stages, and whether any of them could not be recorded. */
  const perOrg = new Map<string, { stages: number; bad: boolean }>();
  const { sql } = opts;

  await sql.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    const ro = (await sql.query("SHOW transaction_read_only")).rows[0]?.transaction_read_only;
    if (ro !== "on") throw new Error("export_not_read_only");
    await sql.query(`SET LOCAL statement_timeout = ${statementTimeoutMs}`);
    for (const r of (await sql.query(ORGS, [orgScope])).rows) {
      if (typeof r.org_id === "string") perOrg.set(r.org_id, { stages: 0, bad: !ORG.test(r.org_id) });
    }
    let cursor = "";
    for (;;) {
      const rows = (await sql.query(ACKNOWLEDGED_PAGE, [orgScope, cursor, pageSize])).rows;
      if (!rows.length) break;
      cursor = String(rows[rows.length - 1]!.stage_key);
      await mapLimit(rows, concurrency, async (r) => {
        summary.rows++;
        const org = typeof r.org_id === "string" ? r.org_id : "";
        const tally = perOrg.get(org) ?? { stages: 0, bad: true };
        perOrg.set(org, tally);
        tally.stages++;
        if (!binds(opts.keyRoot, r)) {
          summary.invalid++;
          tally.bad = true;
          return;
        }
        const key = (r.stage_key as string) + ACCEPTED_SUFFIX;
        const marker: AcceptedMarker = { v: 1, kind: "buffered-accepted", stage_sha256: r.stage_sha256 as string };
        try {
          const existing = await readMarker(store, key);
          if (existing === "invalid" || (existing && existing.stage_sha256 !== marker.stage_sha256)) {
            summary.conflict++;
            tally.bad = true;
            return;
          }
          if (existing) {
            summary.already++;
            return;
          }
          if (opts.dryRun) {
            summary.would_write++;
            return;
          }
          try {
            await store.put(objectKey(key), encoder.encode(JSON.stringify(marker)), { ifNoneMatch: true, contentType: "application/json" });
            summary.written++;
          } catch (error) {
            if (!isObjectStoreError(error, "already_exists") && !isObjectStoreError(error, "precondition_failed")) throw error;
            // A concurrent writer: count it as already there only if it names the same bytes.
            const now = await readMarker(store, key);
            if (now && now !== "invalid" && now.stage_sha256 === marker.stage_sha256) summary.already++;
            else { summary.conflict++; tally.bad = true; }
          }
        } catch {
          summary.failed++;
          tally.bad = true;
        }
      });
    }
  } finally {
    await sql.query("ROLLBACK").catch(() => {});
  }
  // Completion objects, after every sidecar of the org landed.
  const nowMs = (opts.nowMs ?? Date.now)();
  for (const [org, t] of [...perOrg].sort(([a], [b]) => (a < b ? -1 : 1))) {
    if (t.bad || !ORG.test(org)) {
      summary.orgs.incomplete++;
      // Fail closed on a rerun: replace an earlier completion object with one the drain reads as missing.
      if (!opts.dryRun && ORG.test(org))
        await store.put(objectKey(acceptedCompleteKey(opts.keyRoot, org)), encoder.encode(JSON.stringify({ v: 1, kind: "buffered-accepted-incomplete", completedAtMs: nowMs })), { contentType: "application/json" }).catch(() => {});
      continue;
    }
    if (opts.dryRun) {
      summary.orgs.would_complete++;
      continue;
    }
    const doc: AcceptedComplete = { v: 1, kind: "buffered-accepted-complete", stages: t.stages, completedAtMs: nowMs };
    try {
      // A rerun that found the same stages keeps the existing completion object (no rewrite).
      const prior = await store.getObject(objectKey(acceptedCompleteKey(opts.keyRoot, org))).then(async (g) => parseAcceptedComplete(decoder.decode(await readAll(g.body))), () => null);
      if (prior && prior.stages === t.stages) {
        summary.orgs.complete++;
        continue;
      }
      await store.put(objectKey(acceptedCompleteKey(opts.keyRoot, org)), encoder.encode(JSON.stringify(doc)), { contentType: "application/json" });
      summary.orgs.complete++;
    } catch {
      summary.orgs.incomplete++;
    }
  }
  return summary;
}

async function readMarker(store: ObjectStore, key: string): Promise<AcceptedMarker | "invalid" | null> {
  try {
    const got = await store.getObject(objectKey(key));
    const bytes = await readAll(got.body);
    if (bytes.byteLength > 64 * 1024) return "invalid";
    return parseAcceptedMarker(decoder.decode(bytes)) ?? "invalid";
  } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    if (error instanceof TypeError) return "invalid"; // not UTF-8
    throw error;
  }
}
