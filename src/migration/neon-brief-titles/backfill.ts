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
 * Key resolution, the same as sessions.session_brief_titles_for (the read path): the Neon
 * session id is the RelayHistory (logical) id, coalesce(canonical_session_id, session_id) of a
 * live SP pointer. An object-layout-encoded pointer (physical `sess_abc` for `sess/abc`)
 * carries it in canonical_session_id; the brief is written on the physical session id.
 *
 * Parity: every title Cloud shows is carried (Cloud shows any non-blank trimmed title). Only
 * the title has to be valid (1-90 characters: relayhistory's brief schema maxLength); the
 * rest is best effort, and a backfilled row may have no description or state, a line break
 * in the title, or placeholder prompt/model labels ('neon-unknown' / 'unknown') when Neon's
 * are not representable. Skipped, and so reported missing by --verify: a blank title (Cloud
 * shows none either, not compared), a key that is not a harness name, and a title over 90.
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
 * "reuses" it instead of calling the model. The directory title is not touched, now or on a
 * later re-projection: the directory projector reads native briefs only
 * (src/hoopdb/session-directory-projector.ts), so a backfilled title never reaches
 * session_directory.title, search_text or the brief-document/search publication. Its two
 * readers are sessions.session_brief_titles_for (Cloud's session list) and sessions.session_brief()
 * (the /brief route, which reports it 'stale' with a NULL description and state); see
 * docs/brief-titles.md.
 *
 * The brief sweep (store.ts sweepUnbriefed) arms only sessions with no brief row, so a
 * backfilled session is not re-briefed by the sweep. That is intended: the Neon title keeps
 * showing until the session gets new evidence, whose indexing arms a wakeup, and the native
 * brief then replaces it (origin becomes native).
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
       to_char(request.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS requested_at,
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
  /** request.created_at: NOT NULL in Neon, the last generated_at fallback. */
  requested_at?: string | null;
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
  description: string | null;
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

export type SkipReason = "title_blank" | "title_too_long" | "generated_at_invalid" | "key_invalid";

/** Placeholder labels for a backfilled brief whose Neon prompt or model is not representable. */
export const UNKNOWN_PROMPT_VERSION = "neon-unknown";
export const UNKNOWN_MODEL = "unknown";

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
  // Code points, as the CHECK's char_length counts (not UTF-16 units).
  if ([...title].length > 90) return { skip: "title_too_long" };
  const description = typeof n.description === "string" ? n.description.trim() : "";
  const state = n.state !== null && STATES.has(n.state) ? n.state : null;
  const prompt = [n.prompt_version, n.summary_version].find((v) => typeof v === "string" && VERSION.test(v)) ?? UNKNOWN_PROMPT_VERSION;
  const model = [n.model, n.summary_model_version].find((v) => typeof v === "string" && MODEL.test(v)) ?? UNKNOWN_MODEL;
  const at = [n.generated_at, n.completed_at, n.requested_at].find((v) => typeof v === "string" && Number.isFinite(Date.parse(v)));
  if (!at) return { skip: "generated_at_invalid" };
  return {
    row: {
      org_id: n.org_id,
      workspace_id: n.workspace_id,
      harness: n.source_provider,
      session_id: n.session_id,
      legacy_job_id: n.job_id,
      title,
      // Kept only within the native bounds; otherwise a title-only legacy brief.
      description: description && [...description].length <= 320 && !LINE_BREAK.test(description) ? description : null,
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
  -- Every live pointer resolving to an input's RelayHistory id (unencoded: session_id with no
  -- canonical id; encoded: canonical_session_id), held until COMMIT. FOR SHARE cannot carry
  -- DISTINCT, so the de-duplication is the next step.
  SELECT p.org_id, p.workspace_id, p.harness, p.session_id AS physical_id, i.session_id AS history_id
    FROM sessions.session_index AS p
    JOIN input AS i ON p.org_id = i.org_id AND p.workspace_id = i.workspace_id AND p.harness = i.harness
                   AND ((p.session_id = i.session_id AND p.canonical_session_id IS NULL)
                        OR p.canonical_session_id = i.session_id)
   WHERE p.deleted_at IS NULL
     FOR SHARE OF p
), targets AS (
  -- One writer per physical session (a physical id resolves to one RelayHistory id; if mixed
  -- pointers ever disagree, the lowest id wins deterministically and the other is skipped).
  SELECT DISTINCT ON (org_id, workspace_id, harness, physical_id) org_id, workspace_id, harness, physical_id, history_id
    FROM locked ORDER BY org_id, workspace_id, harness, physical_id, history_id
), existing AS (
  SELECT b.org_id, b.workspace_id, b.harness, b.session_id AS physical_id,
         b.title IS NOT NULL AS titled, b.status
    FROM sessions.session_briefs AS b
    JOIN targets AS t ON b.org_id = t.org_id AND b.workspace_id = t.workspace_id
                     AND b.harness = t.harness AND b.session_id = t.physical_id
), written AS (
  INSERT INTO sessions.session_briefs AS b
    (org_id, workspace_id, harness, session_id, status, revision, source_revisions,
     title, description, state, brief_revision, brief_source_revisions, brief_fingerprint,
     scrub_ruleset, scrub_version, prompt_version, engine_version, model, message_count,
     shortened_messages, input_tokens, output_tokens, generated_at, legacy_job_id,
     attempted_at, updated_at)
  SELECT t.org_id, t.workspace_id, t.harness, t.physical_id, 'ready', 0, '{}'::jsonb,
         i.title, i.description, i.state, 0, '{}'::jsonb, i.fingerprint,
         '${BACKFILL_SCRUB_RULESET}', 1, i.prompt_version, '${BACKFILL_ENGINE_VERSION}', i.model,
         i.message_count, i.shortened_messages, i.input_tokens, i.output_tokens,
         i.generated_at, i.legacy_job_id, now(), now()
    FROM input AS i
    JOIN targets AS t ON t.org_id = i.org_id AND t.workspace_id = i.workspace_id
                     AND t.harness = i.harness AND t.history_id = i.session_id
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
  RETURNING b.org_id, b.workspace_id, b.harness, b.session_id AS physical_id
), per_input AS (
  SELECT i.org_id, i.workspace_id, i.harness, i.session_id,
         bool_or(w.physical_id IS NOT NULL) AS any_write,
         bool_or(w.physical_id IS NOT NULL AND e.physical_id IS NULL) AS any_insert,
         bool_or(t.physical_id IS NOT NULL) AS any_live,
         bool_or(e.status = 'quarantined' AND NOT e.titled) AS any_quarantine
    FROM input AS i
    LEFT JOIN targets AS t ON t.org_id = i.org_id AND t.workspace_id = i.workspace_id
                          AND t.harness = i.harness AND t.history_id = i.session_id
    LEFT JOIN existing AS e ON e.org_id = t.org_id AND e.workspace_id = t.workspace_id
                           AND e.harness = t.harness AND e.physical_id = t.physical_id
    LEFT JOIN written AS w ON w.org_id = t.org_id AND w.workspace_id = t.workspace_id
                          AND w.harness = t.harness AND w.physical_id = t.physical_id
   GROUP BY i.org_id, i.workspace_id, i.harness, i.session_id
)
SELECT org_id, workspace_id, harness, session_id,
       CASE WHEN any_write THEN CASE WHEN any_insert THEN 'inserted' ELSE 'updated' END
            WHEN NOT any_live THEN 'skipped_no_pointer'
            WHEN any_quarantine THEN 'skipped_quarantined'
            ELSE 'skipped_native' END AS outcome
  FROM per_input`;

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

/**
 * The title SP serves for each Neon candidate key: sessions.session_brief_titles_for's exact
 * resolution and preference (logical id through canonical_session_id; native first, then
 * newest; trimmed, never blank), without its role guard (verify runs as the owner, READ ONLY).
 */
export const VERIFY_SQL = `SELECT i.org_id, i.workspace_id, i.harness, i.session_id, hit.title, hit.origin,
       EXISTS (SELECT 1 FROM sessions.session_index AS p
                WHERE p.org_id = i.org_id AND p.workspace_id = i.workspace_id AND p.harness = i.harness
                  AND p.deleted_at IS NULL
                  AND ((p.session_id = i.session_id AND p.canonical_session_id IS NULL)
                       OR p.canonical_session_id = i.session_id)) AS live
  FROM jsonb_to_recordset($1::jsonb) AS i(org_id text, workspace_id text, harness text, session_id text)
  LEFT JOIN LATERAL (
    SELECT btrim(b.title) AS title, b.origin
      FROM (
        SELECT p.session_id FROM sessions.session_index AS p
         WHERE p.org_id = i.org_id AND p.workspace_id = i.workspace_id AND p.harness = i.harness
           AND p.session_id = i.session_id AND p.canonical_session_id IS NULL AND p.deleted_at IS NULL
        UNION
        SELECT p.session_id FROM sessions.session_index AS p
         WHERE p.org_id = i.org_id AND p.workspace_id = i.workspace_id AND p.harness = i.harness
           AND p.canonical_session_id = i.session_id AND p.deleted_at IS NULL
      ) AS c
      JOIN sessions.session_briefs AS b
        ON b.org_id = i.org_id AND b.workspace_id = i.workspace_id AND b.harness = i.harness
       AND b.session_id = c.session_id
     WHERE b.title IS NOT NULL AND btrim(b.title) <> ''
     ORDER BY (b.origin = 'native') DESC, b.generated_at DESC, b.session_id
     LIMIT 1
  ) AS hit ON true`;

/**
 * Parity holds only with nothing missing, nothing different and complete, well-formed verify
 * output. no_pointer keys are excluded from the comparison and reported, not failed.
 */
export function verifyPassed(r: VerifyReport): boolean {
  return r.missing === 0 && r.different === 0 && r.malformed === 0;
}

export interface VerifyReport {
  candidates: number;
  /** Cloud-visible Neon titles (non-blank after btrim). */
  compared: number;
  equal: number;
  /** SP has a live pointer for the key but serves no title (not backfilled or skipped): a defect. */
  missing: number;
  /**
   * A present, well-formed verify row says no live pointer resolves to the key (erased,
   * tombstoned, or not copied). These keys are EXCLUDED from the title comparison. This is not
   * a proof of copy completeness: whether every session was copied is the copy verifier's job.
   */
  no_pointer: number;
  /**
   * The verify query's output did not cover the batch exactly: a key with no row, a duplicate
   * or unrequested row, or a row of the wrong shape (live not a boolean, a title with no live
   * pointer). VERIFY_SQL returns exactly one row per key, so any of these fails the run.
   */
  malformed: number;
  /** SP serves a native brief title instead (expected after native regeneration). */
  native: number;
  /** SP serves a backfilled title that differs from Neon: always a defect. */
  different: number;
  /** Up to 20 keys per class, identifiers only. */
  samples: { missing: string[][]; different: string[][]; no_pointer: string[][]; malformed: string[][] };
}

/** Read-only parity: every Cloud-visible Neon title must equal the title SP serves. */
type VerifyRow = { org_id: unknown; workspace_id: unknown; harness: unknown; session_id: unknown; title: unknown; origin: unknown; live: unknown };

/** One VERIFY_SQL row's shape, exactly: a row that is not this is malformed, never evidence. */
function wellFormed(r: VerifyRow): boolean {
  if (typeof r.org_id !== "string" || typeof r.workspace_id !== "string" || typeof r.harness !== "string" || typeof r.session_id !== "string") return false;
  if (typeof r.live !== "boolean") return false;
  if (r.title === null) return r.origin === null;
  // A served title implies a live pointer and a provenance label.
  return typeof r.title === "string" && r.live === true && (r.origin === "native" || r.origin === "neon_backfill");
}

/** Read-only parity: every Cloud-visible Neon title against the title SP serves. */
export async function verifyBackfill(target: Queryable, source: readonly NeonBriefRow[]): Promise<VerifyReport> {
  const report: VerifyReport = {
    candidates: source.length, compared: 0, equal: 0, missing: 0, no_pointer: 0, malformed: 0, native: 0, different: 0,
    samples: { missing: [], different: [], no_pointer: [], malformed: [] },
  };
  const seen = new Map<string, string>();
  const wanted: Array<{ key: { org_id: string; workspace_id: string; harness: string; session_id: string }; title: string }> = [];
  for (const n of source) {
    const title = cloudTitle(n.title);
    if (title === null) continue;
    const id = JSON.stringify([n.org_id, n.workspace_id, n.source_provider, n.session_id]);
    // DISTINCT ON makes this impossible from Neon; a fixture may still repeat a key.
    if (seen.has(id)) throw new Error("verify: duplicate session key in the source");
    seen.set(id, title);
    wanted.push({ key: { org_id: n.org_id, workspace_id: n.workspace_id, harness: n.source_provider, session_id: n.session_id }, title });
  }
  report.compared = wanted.length;
  const malformed = (id: string[]) => {
    report.malformed += 1;
    if (report.samples.malformed.length < 20) report.samples.malformed.push(id);
  };
  for (let at = 0; at < wanted.length; at += LOAD_BATCH_ROWS) {
    const batch = wanted.slice(at, at + LOAD_BATCH_ROWS);
    const { rows } = await target.query<VerifyRow>(VERIFY_SQL, [JSON.stringify(batch.map((w) => w.key))]);
    const asked = new Set(batch.map((w) => JSON.stringify([w.key.org_id, w.key.workspace_id, w.key.harness, w.key.session_id])));
    const got = new Map<string, VerifyRow>();
    const bad = new Set<string>();
    for (const r of rows) {
      const id = JSON.stringify([r.org_id, r.workspace_id, r.harness, r.session_id]);
      if (!asked.has(id)) { malformed([String(r.org_id), String(r.workspace_id), String(r.harness), String(r.session_id)]); continue; }
      if (got.has(id) || !wellFormed(r)) bad.add(id);
      got.set(id, r);
    }
    for (const w of batch) {
      const id = [w.key.org_id, w.key.workspace_id, w.key.harness, w.key.session_id];
      const key = JSON.stringify(id);
      const r = got.get(key);
      if (!r || bad.has(key)) {
        malformed(id);
      } else if (r.title === null && r.live === false) {
        report.no_pointer += 1;
        if (report.samples.no_pointer.length < 20) report.samples.no_pointer.push(id);
      } else if (r.title === null) {
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
