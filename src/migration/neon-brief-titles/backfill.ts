/**
 * One-shot backfill of the session-list brief titles from Neon into Supabase
 * sessions.session_briefs (feature F2; migration 20261105043000_brief_titles.sql).
 *
 * Source: Cloud's exact selection (packages/web/app/dashboard/sessions/
 * session-brief-titles.ts), generalised from one (org, workspace, [sessions]) call to every
 * scope: per (org, workspace, source_provider, session) the latest request (created_at DESC,
 * id DESC) that is resolved, whose brief job is completed, of that workspace and provider,
 * with evidenceWorkspaceIds exactly [workspace]. Cloud shows `btrim(result.title)` when it is
 * non-blank; the same value is carried here.
 *
 * Target rules (LOAD_SQL), all in the statement, so a concurrent native brief always wins:
 *   - only where the session has a live pointer, locked FOR SHARE until COMMIT (as the
 *     native writer does), so an erasure either waits for this write (and its forget
 *     trigger then deletes the row) or wins first (and the row is skipped);
 *   - inserted when no brief row exists;
 *   - on an existing row, written only when it has no title and is not quarantined, or is
 *     itself a backfill (a rerun). A native title is never overwritten, and a quarantine
 *     (the native stage decided the text must not exist) is never refilled.
 * A backfilled row: engine_version 'neon-backfill:v1' (so the provenance trigger labels it
 * origin = 'neon_backfill'), legacy_job_id = the Neon job id, revision 0 and empty source
 * revisions (session_brief reports it 'stale', never 'ready'), and a brief_fingerprint that
 * no native input can have (sha256 of a backfill-only string), so the native worker never
 * "reuses" it instead of calling the model. The directory title is not touched.
 *
 * Nothing here logs titles or descriptions: reports carry counts, reasons and session keys.
 */
import { createHash } from "node:crypto";
import type { Queryable } from "../relayhistory-export.ts";

export const BACKFILL_ENGINE_VERSION = "neon-backfill:v1";
export const BACKFILL_SCRUB_RULESET = "relayhistory-scrub";
export const LOAD_BATCH_ROWS = 500;

/** Every Neon brief-title candidate Cloud would show, one per (org, workspace, source, session). */
export const NEON_SELECT_SQL = `SELECT DISTINCT ON (request.org_id, job.workspace_id, job.source_provider, request.session_id)
       request.org_id, job.workspace_id, job.source_provider, request.session_id,
       job.id::text AS job_id,
       job.result->>'title' AS title,
       job.result->>'description' AS description,
       job.result->>'state' AS state,
       job.result->>'promptVersion' AS prompt_version,
       job.summary_version,
       job.result#>>'{backend,model}' AS model,
       job.summary_model_version,
       job.result->>'generatedAt' AS generated_at,
       to_char(job.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS completed_at,
       job.result->>'messageCount' AS message_count,
       job.result->>'shortenedMessages' AS shortened_messages,
       job.result#>>'{backend,inputTokens}' AS input_tokens,
       job.result#>>'{backend,outputTokens}' AS output_tokens
  FROM sessions.session_analysis_requests request
  JOIN sessions.session_analysis_jobs job ON job.id = request.brief_job_id
   AND job.org_id = request.org_id AND job.session_id = request.session_id
 WHERE request.status = 'resolved' AND job.status = 'completed' AND job.kind = 'brief'
   AND job.workspace_id IS NOT NULL
   AND job.source->'evidenceWorkspaceIds' = jsonb_build_array(job.workspace_id)
 ORDER BY request.org_id, job.workspace_id, job.source_provider, request.session_id,
          request.created_at DESC, request.id DESC`;

/** One NEON_SELECT_SQL row (every value as text, as selected). */
export interface NeonBriefRow {
  org_id: string;
  workspace_id: string;
  source_provider: string;
  session_id: string;
  job_id: string;
  title: string | null;
  description: string | null;
  state: string | null;
  prompt_version: string | null;
  summary_version: string | null;
  model: string | null;
  summary_model_version: string | null;
  generated_at: string | null;
  completed_at: string | null;
  message_count: string | null;
  shortened_messages: string | null;
  input_tokens: string | null;
  output_tokens: string | null;
}

/** One target row for LOAD_SQL. */
export interface BackfillRow {
  org_id: string;
  workspace_id: string;
  harness: string;
  session_id: string;
  legacy_job_id: string;
  title: string;
  description: string;
  state: string | null;
  prompt_version: string;
  model: string;
  generated_at: string;
  message_count: number | null;
  shortened_messages: number | null;
  input_tokens: number;
  output_tokens: number;
  fingerprint: string;
}

export type SkipReason =
  | "title_blank"
  | "title_invalid"
  | "description_invalid"
  | "state_invalid"
  | "prompt_version_invalid"
  | "model_invalid"
  | "generated_at_invalid"
  | "key_invalid";

const HARNESS = /^[a-z0-9][a-z0-9._-]{0,63}$/u;
const VERSION = /^[a-z0-9][a-z0-9._:-]{0,63}$/u;
const MODEL = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/u;
const JOB_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const STATES = new Set(["in-progress", "completed", "blocked", "abandoned", "answered"]);
const LINE_BREAK = /[\r\n]/u;

const count = (raw: string | null): number | null => {
  if (raw === null || !/^\d{1,15}$/u.test(raw)) return null;
  return Number(raw);
};

/** The fingerprint no native brief input can have (native fingerprints hash the model input). */
export function backfillFingerprint(jobId: string): string {
  return createHash("sha256").update(`96bulls:${BACKFILL_ENGINE_VERSION}:${jobId}`).digest("hex");
}

/** The Cloud-visible title (btrim, non-blank), as Cloud renders it. Null: Cloud shows none. */
export function cloudTitle(raw: string | null): string | null {
  if (typeof raw !== "string") return null;
  const t = raw.trim();
  return t ? t : null;
}

/** A Neon row as a target row, or why it cannot be one (never the text itself). */
export function toBackfillRow(n: NeonBriefRow): { row: BackfillRow } | { skip: SkipReason } {
  if (!HARNESS.test(n.source_provider) || !n.session_id || n.session_id.length > 255 || !n.org_id || !n.workspace_id || !JOB_ID.test(n.job_id))
    return { skip: "key_invalid" };
  const title = cloudTitle(n.title);
  if (title === null) return { skip: "title_blank" };
  if (title.length > 90 || LINE_BREAK.test(title)) return { skip: "title_invalid" };
  const description = typeof n.description === "string" ? n.description.trim() : "";
  if (!description || description.length > 320 || LINE_BREAK.test(description)) return { skip: "description_invalid" };
  const state = n.state === null || n.state === "" ? null : n.state;
  if (state !== null && !STATES.has(state)) return { skip: "state_invalid" };
  const prompt = n.prompt_version ?? n.summary_version;
  if (!prompt || !VERSION.test(prompt)) return { skip: "prompt_version_invalid" };
  const model = n.model ?? n.summary_model_version;
  if (!model || !MODEL.test(model)) return { skip: "model_invalid" };
  const at = [n.generated_at, n.completed_at].find((v) => typeof v === "string" && Number.isFinite(Date.parse(v)));
  if (!at) return { skip: "generated_at_invalid" };
  return {
    row: {
      org_id: n.org_id,
      workspace_id: n.workspace_id,
      harness: n.source_provider,
      session_id: n.session_id,
      legacy_job_id: n.job_id,
      title,
      description,
      state,
      prompt_version: prompt,
      model,
      generated_at: new Date(Date.parse(at)).toISOString(),
      message_count: count(n.message_count),
      shortened_messages: count(n.shortened_messages),
      input_tokens: count(n.input_tokens) ?? 0,
      output_tokens: count(n.output_tokens) ?? 0,
      fingerprint: backfillFingerprint(n.job_id),
    },
  };
}

/**
 * One batch. Returns one row per input key with its outcome:
 *   inserted | updated (an empty native row filled, or a backfill rerun) |
 *   skipped_native | skipped_quarantined | skipped_no_pointer
 */
export const LOAD_SQL = `WITH input AS (
  SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(
    org_id text, workspace_id text, harness text, session_id text, legacy_job_id text,
    title text, description text, state text, prompt_version text, model text,
    generated_at timestamptz, message_count integer, shortened_messages integer,
    input_tokens bigint, output_tokens bigint, fingerprint text)
), locked AS (
  -- Every live pointer of the batch's sessions, held until COMMIT (FOR SHARE cannot carry
  -- DISTINCT, so the de-duplication is the next step).
  SELECT p.org_id, p.workspace_id, p.harness, p.session_id
    FROM sessions.session_index AS p
    JOIN input AS i ON p.org_id = i.org_id AND p.workspace_id = i.workspace_id
                   AND p.harness = i.harness AND p.session_id = i.session_id
   WHERE p.deleted_at IS NULL
     FOR SHARE OF p
), live AS (
  SELECT DISTINCT org_id, workspace_id, harness, session_id FROM locked
), existing AS (
  SELECT b.org_id, b.workspace_id, b.harness, b.session_id, b.title IS NOT NULL AS titled,
         b.origin, b.status
    FROM sessions.session_briefs AS b
    JOIN input AS i ON b.org_id = i.org_id AND b.workspace_id = i.workspace_id
                   AND b.harness = i.harness AND b.session_id = i.session_id
), written AS (
  INSERT INTO sessions.session_briefs AS b
    (org_id, workspace_id, harness, session_id, status, revision, source_revisions,
     title, description, state, brief_revision, brief_source_revisions, brief_fingerprint,
     scrub_ruleset, scrub_version, prompt_version, engine_version, model, message_count,
     shortened_messages, input_tokens, output_tokens, generated_at, legacy_job_id,
     attempted_at, updated_at)
  SELECT i.org_id, i.workspace_id, i.harness, i.session_id, 'ready', 0, '{}'::jsonb,
         i.title, i.description, i.state, 0, '{}'::jsonb, i.fingerprint,
         '${BACKFILL_SCRUB_RULESET}', 1, i.prompt_version, '${BACKFILL_ENGINE_VERSION}', i.model,
         i.message_count, i.shortened_messages, i.input_tokens, i.output_tokens,
         i.generated_at, i.legacy_job_id, now(), now()
    FROM input AS i
    JOIN live AS l USING (org_id, workspace_id, harness, session_id)
  ON CONFLICT (org_id, workspace_id, harness, session_id) DO UPDATE SET
    title = excluded.title, description = excluded.description, state = excluded.state,
    brief_revision = excluded.brief_revision,
    brief_source_revisions = excluded.brief_source_revisions,
    brief_fingerprint = excluded.brief_fingerprint, scrub_ruleset = excluded.scrub_ruleset,
    scrub_version = excluded.scrub_version, prompt_version = excluded.prompt_version,
    engine_version = excluded.engine_version, model = excluded.model,
    message_count = excluded.message_count, shortened_messages = excluded.shortened_messages,
    input_tokens = excluded.input_tokens, output_tokens = excluded.output_tokens,
    generated_at = excluded.generated_at, legacy_job_id = excluded.legacy_job_id,
    updated_at = now()
  WHERE (b.title IS NULL AND b.status <> 'quarantined') OR b.origin = 'neon_backfill'
  RETURNING b.org_id, b.workspace_id, b.harness, b.session_id
)
SELECT i.org_id, i.workspace_id, i.harness, i.session_id,
       CASE WHEN w.session_id IS NOT NULL THEN CASE WHEN e.session_id IS NULL THEN 'inserted' ELSE 'updated' END
            WHEN l.session_id IS NULL THEN 'skipped_no_pointer'
            WHEN e.status = 'quarantined' AND NOT e.titled THEN 'skipped_quarantined'
            ELSE 'skipped_native' END AS outcome
  FROM input AS i
  LEFT JOIN live AS l USING (org_id, workspace_id, harness, session_id)
  LEFT JOIN existing AS e USING (org_id, workspace_id, harness, session_id)
  LEFT JOIN written AS w USING (org_id, workspace_id, harness, session_id)`;

export type LoadOutcome = "inserted" | "updated" | "skipped_native" | "skipped_quarantined" | "skipped_no_pointer";

export interface BackfillReport {
  mode: "dry-run" | "execute";
  candidates: number;
  skipped: Partial<Record<SkipReason, number>>;
  outcomes: Record<LoadOutcome, number>;
  batches: number;
}

/** A transaction-capable target connection (a pg Client or a checked-out pool client). */
export interface TargetClient extends Queryable {}

/**
 * Loads `rows` in transactions of LOAD_BATCH_ROWS. Dry run: each batch runs exactly as in
 * execute mode and then ROLLBACKs, so the counts are the real outcomes at that moment and
 * nothing is kept.
 */
export async function loadBackfill(target: TargetClient, source: readonly NeonBriefRow[], mode: "dry-run" | "execute"): Promise<BackfillReport> {
  const report: BackfillReport = {
    mode,
    candidates: source.length,
    skipped: {},
    outcomes: { inserted: 0, updated: 0, skipped_native: 0, skipped_quarantined: 0, skipped_no_pointer: 0 },
    batches: 0,
  };
  const rows: BackfillRow[] = [];
  const keys = new Set<string>();
  for (const n of source) {
    const r = toBackfillRow(n);
    if ("skip" in r) {
      report.skipped[r.skip] = (report.skipped[r.skip] ?? 0) + 1;
      continue;
    }
    const key = JSON.stringify([r.row.org_id, r.row.workspace_id, r.row.harness, r.row.session_id]);
    // DISTINCT ON makes this impossible from Neon; a fixture may still repeat a key.
    if (keys.has(key)) throw new Error("backfill: duplicate session key in the source");
    keys.add(key);
    rows.push(r.row);
  }
  for (let at = 0; at < rows.length; at += LOAD_BATCH_ROWS) {
    const batch = rows.slice(at, at + LOAD_BATCH_ROWS);
    await target.query("BEGIN");
    try {
      await target.query("SET LOCAL statement_timeout = 20000");
      await target.query("SET LOCAL lock_timeout = 2000");
      const { rows: out } = await target.query<{ outcome: LoadOutcome }>(LOAD_SQL, [JSON.stringify(batch)]);
      if (out.length !== batch.length) throw new Error("backfill: outcome count differs from the batch");
      for (const o of out) report.outcomes[o.outcome] += 1;
      await target.query(mode === "execute" ? "COMMIT" : "ROLLBACK");
    } catch (error) {
      await target.query("ROLLBACK").catch(() => undefined);
      throw error;
    }
    report.batches += 1;
  }
  return report;
}

/** The SP title Cloud would be served for one Neon candidate key (session_brief_titles_for's exact-id rule). */
export const VERIFY_SQL = `SELECT i.org_id, i.workspace_id, i.harness, i.session_id, b.title, b.origin
  FROM jsonb_to_recordset($1::jsonb) AS i(org_id text, workspace_id text, harness text, session_id text)
  LEFT JOIN sessions.session_briefs AS b
    ON b.org_id = i.org_id AND b.workspace_id = i.workspace_id AND b.harness = i.harness
   AND b.session_id = i.session_id AND b.title IS NOT NULL
   AND EXISTS (SELECT 1 FROM sessions.session_index AS p
                WHERE p.org_id = b.org_id AND p.workspace_id = b.workspace_id
                  AND p.harness = b.harness AND p.session_id = b.session_id AND p.deleted_at IS NULL)`;

export interface VerifyReport {
  candidates: number;
  /** Cloud-visible Neon titles (non-blank after btrim). */
  compared: number;
  equal: number;
  /** SP has no title for the key (not backfilled, skipped, or no live pointer). */
  missing: number;
  /** SP serves a native brief title instead (expected after native regeneration). */
  native: number;
  /** SP serves a backfilled title that differs from Neon: always a defect. */
  different: number;
  /** Up to 20 keys per class, identifiers only. */
  samples: { missing: string[][]; different: string[][] };
}

/** Read-only parity: every Cloud-visible Neon title must equal the title SP serves. */
export async function verifyBackfill(target: Queryable, source: readonly NeonBriefRow[]): Promise<VerifyReport> {
  const report: VerifyReport = { candidates: source.length, compared: 0, equal: 0, missing: 0, native: 0, different: 0, samples: { missing: [], different: [] } };
  const wanted = source.flatMap((n) => {
    const title = cloudTitle(n.title);
    return title === null ? [] : [{ key: { org_id: n.org_id, workspace_id: n.workspace_id, harness: n.source_provider, session_id: n.session_id }, title }];
  });
  report.compared = wanted.length;
  for (let at = 0; at < wanted.length; at += LOAD_BATCH_ROWS) {
    const batch = wanted.slice(at, at + LOAD_BATCH_ROWS);
    const { rows } = await target.query<{ org_id: string; workspace_id: string; harness: string; session_id: string; title: string | null; origin: string | null }>(
      VERIFY_SQL,
      [JSON.stringify(batch.map((w) => w.key))],
    );
    const got = new Map(rows.map((r) => [JSON.stringify([r.org_id, r.workspace_id, r.harness, r.session_id]), r]));
    for (const w of batch) {
      const id = [w.key.org_id, w.key.workspace_id, w.key.harness, w.key.session_id];
      const r = got.get(JSON.stringify(id));
      if (!r || r.title === null) {
        report.missing += 1;
        if (report.samples.missing.length < 20) report.samples.missing.push(id);
      } else if (r.origin === "native") {
        report.native += 1;
      } else if (r.title === w.title) {
        report.equal += 1;
      } else {
        report.different += 1;
        if (report.samples.different.length < 20) report.samples.different.push(id);
      }
    }
  }
  return report;
}

/** The Neon source: NEON_SELECT_SQL in the caller's READ ONLY session. */
export async function extractNeon(neon: Queryable): Promise<NeonBriefRow[]> {
  const { rows } = await neon.query<NeonBriefRow>(NEON_SELECT_SQL);
  return rows;
}
