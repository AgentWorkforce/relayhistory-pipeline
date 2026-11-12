/**
 * The only write path for `sessions.session_directory.long_summary`
 * (supabase/migrations/20261105060000_session_long_summary.sql).
 *
 * Modeled on the short summary writer (session-directory-summary.ts): the text is
 * session data derived from transcript content, so it is stored only after the active
 * scrub engine has run over it again (same engine, same ruleset binding), and the scrub
 * ruleset, version, producer and generated_at are recorded beside it. The writer fails
 * closed: an empty or over-length summary, a scrub that throws, and a scrub that is not
 * stable on its own output are refused and nothing is written. The database repeats the
 * length, provenance and "never without a live short summary" checks.
 *
 * The long summary is not searchable: it is never part of `search_text`, and writing it
 * does not touch `projected_at`. It is read only through
 * `sessions.session_long_summary_for` (bulls96_read_api).
 *
 * Off by default: Bob Costas calls this only when SESSION_LONG_SUMMARY_PROJECTION_ENABLED
 * is exactly `true` (src/bob-costas/main.ts). No summary text is ever put in a log line,
 * an error message or telemetry.
 */
import type { SqlExecutor } from "@rhc/lib/pipeline/queue.ts";
import { SCRUB_RULESET, SCRUB_RULESET_VERSION } from "../shared/vendor.ts";
import { ScrubberError, ScrubEngineUnavailable, activeScrubEngine } from "@rhc/lib/scrub-engine/index.ts";
import { meaningfulChars } from "./search-query.ts";
import type { SessionDirectoryKey } from "./session-directory-summary.ts";

/** Hard bound, in Unicode code points (PostgreSQL `char_length`). */
export const LONG_SUMMARY_MAX_CHARS = 8000;

/** The directory source alphabet, shared with the short summary. */
const SOURCE_PATTERN = /^[a-z0-9][a-z0-9._/-]{0,63}$/;

export type LongSummaryRejection =
  | "empty"
  | "over_length"
  | "scrub_failed"
  | "scrub_unstable"
  | "invalid_source";

/** Refusal. The message names the reason only, never the text. */
export class LongSummaryRejected extends Error {
  constructor(readonly reason: LongSummaryRejection) {
    super(`long_summary rejected: ${reason}`);
    this.name = "LongSummaryRejected";
  }
}

export type ScrubbedLongSummary = {
  text: string;
  scrubRuleset: typeof SCRUB_RULESET;
  scrubVersion: number;
};

export type LongSummaryInput = SessionDirectoryKey & {
  summary: string;
  /** Producer and version, e.g. `bob-costas/extractive/v1`. */
  source: string;
  generatedAt: Date;
};

const codePoints = (value: string) => Array.from(value).length;

/**
 * Scrub and bound one long summary. `scrub` is a parameter only so tests can prove a
 * failing or unstable scrub is refused; without it the active scrub engine scrubs and
 * `scrubVersion` is that engine's own ruleset version, exactly as for the short summary.
 * Refuses rather than truncates: a cut can split a credential below the scrubber's match
 * length, and a silently shortened summary is not what the producer wrote.
 */
export function scrubLongSummary(
  summary: string,
  scrub?: (value: string) => string,
): ScrubbedLongSummary {
  const trimmed = summary.trim();
  if (meaningfulChars(trimmed) === 0) throw new LongSummaryRejected("empty");
  if (codePoints(trimmed) > LONG_SUMMARY_MAX_CHARS)
    throw new LongSummaryRejected("over_length");
  let text: string;
  let scrubVersion = SCRUB_RULESET_VERSION;
  if (!scrub) {
    // Outside the try: an unavailable engine is retryable, not a refusal of this summary.
    const engine = activeScrubEngine();
    scrub = (value) => engine.scrubText(value);
    scrubVersion = engine.rulesetVersion;
  }
  try {
    text = scrub(trimmed);
    if (typeof text !== "string") throw new LongSummaryRejected("scrub_failed");
    // A fixed point proves the stored text is scrubbed under these rules.
    if (scrub(text) !== text) throw new LongSummaryRejected("scrub_unstable");
  } catch (error) {
    if (error instanceof LongSummaryRejected) throw error;
    if (error instanceof ScrubEngineUnavailable || (error instanceof ScrubberError && error.retryable))
      throw error;
    throw new LongSummaryRejected("scrub_failed");
  }
  text = text.trim();
  if (meaningfulChars(text) === 0) throw new LongSummaryRejected("empty");
  if (codePoints(text) > LONG_SUMMARY_MAX_CHARS)
    throw new LongSummaryRejected("over_length");
  return { text, scrubRuleset: SCRUB_RULESET, scrubVersion };
}

/** The live directory row of the logical session (canonical, or the legacy alias row). */
const TARGET = `WITH target AS (
       SELECT d.session_id
         FROM sessions.session_directory AS d
        WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3
          AND (d.session_id = $4 OR EXISTS (
            SELECT 1 FROM sessions.session_index AS p
             WHERE p.org_id = d.org_id AND p.workspace_id = d.workspace_id
               AND p.harness = d.harness AND p.session_id = $4
               AND coalesce(p.canonical_session_id, p.session_id) = d.session_id))
        ORDER BY (d.session_id <> $4) DESC LIMIT 1
     )`;

/**
 * Store a long summary on a live directory row that already carries a short summary.
 * An older `generatedAt` never replaces a newer long summary. Returns whether a row was
 * written: `false` when the row is absent, tombstoned, has no short summary yet, or holds
 * a newer long summary. A refusal clears an older stored long summary under the same
 * generated_at fence (it describes content the producer has since replaced) and rethrows.
 */
export async function writeLongSummary(
  db: SqlExecutor,
  input: LongSummaryInput,
  scrub?: (value: string) => string,
): Promise<{ written: boolean; scrubVersion: number }> {
  let scrubbed: ScrubbedLongSummary;
  try {
    if (!SOURCE_PATTERN.test(input.source))
      throw new LongSummaryRejected("invalid_source");
    scrubbed = scrubLongSummary(input.summary, scrub);
  } catch (error) {
    if (error instanceof LongSummaryRejected)
      await clearOlderLongSummary(db, input, input.generatedAt);
    throw error;
  }
  const { rows } = await db.query<{ written: boolean }>(
    `${TARGET}
     UPDATE sessions.session_directory AS d
        SET long_summary = $5,
            long_summary_scrub_ruleset = $6,
            long_summary_scrub_version = $7,
            long_summary_source = $8,
            long_summary_generated_at = $9::timestamptz,
            long_summary_updated_at = now()
       FROM target AS t
      WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3 AND d.session_id = t.session_id
        AND d.deleted_at IS NULL AND d.short_summary IS NOT NULL
        AND (d.long_summary_generated_at IS NULL OR d.long_summary_generated_at <= $9::timestamptz)
      RETURNING d.long_summary IS NOT NULL AS written`,
    [
      input.orgId,
      input.workspaceId,
      input.harness,
      input.sessionId,
      scrubbed.text,
      scrubbed.scrubRuleset,
      scrubbed.scrubVersion,
      input.source,
      input.generatedAt.toISOString(),
    ],
  );
  return { written: rows[0]?.written === true, scrubVersion: scrubbed.scrubVersion };
}

/** Clear the long summary only if it is not newer than `notAfter` (the generated_at fence). */
async function clearOlderLongSummary(
  db: SqlExecutor,
  key: SessionDirectoryKey,
  notAfter: Date,
): Promise<boolean> {
  const { rows } = await db.query<{ cleared: boolean }>(
    `${TARGET}
     UPDATE sessions.session_directory AS d
        SET long_summary = NULL,
            long_summary_scrub_ruleset = NULL,
            long_summary_scrub_version = NULL,
            long_summary_source = NULL,
            long_summary_generated_at = NULL,
            long_summary_updated_at = NULL
       FROM target AS t
      WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3 AND d.session_id = t.session_id
        AND d.long_summary IS NOT NULL
        AND d.long_summary_generated_at <= $5::timestamptz
      RETURNING true AS cleared`,
    [key.orgId, key.workspaceId, key.harness, key.sessionId, notAfter.toISOString()],
  );
  return rows.length > 0;
}

/** Remove a row's long summary (a non-ready long summary object). No-op when empty. */
export async function clearLongSummary(
  db: SqlExecutor,
  key: SessionDirectoryKey,
): Promise<boolean> {
  const { rows } = await db.query<{ cleared: boolean }>(
    `${TARGET}
     UPDATE sessions.session_directory AS d
        SET long_summary = NULL,
            long_summary_scrub_ruleset = NULL,
            long_summary_scrub_version = NULL,
            long_summary_source = NULL,
            long_summary_generated_at = NULL,
            long_summary_updated_at = NULL
       FROM target AS t
      WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.harness = $3 AND d.session_id = t.session_id
        AND d.long_summary IS NOT NULL
      RETURNING true AS cleared`,
    [key.orgId, key.workspaceId, key.harness, key.sessionId],
  );
  return rows.length > 0;
}
