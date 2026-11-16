/**
 * Bob Costas: summaries. Consumes `summary-short` and `summary-long`. Reads the
 * scrubbed chunks from the Locker (never the unscrubbed source), builds a
 * digest and summarises it.
 *
 * The summary is stored as an encrypted object next to the session's raw,
 * `summary/{short,long}.json.enc` (`summary-objects.ts`, ported from upstream),
 * under the workspace key. `sessions.session_summaries` holds only the pointer:
 * object key, ciphertext SHA-256, key id, status and revision, never the text
 * or the model name. Object first, then pointer, inside one transaction holding
 * a per-session advisory lock, so two runs never publish in different orders.
 * Idempotent by revision and raw content hash.
 */
import { createHash } from "node:crypto";
import {
  PipelineError,
  parseSessionKey,
  type ManifestReader,
  type ManifestV1,
  type ObjectStore,
  type RawPointer,
  type StageHandler,
} from "../shared/vendor.ts";
import type { HoopDb } from "../shared/db.ts";
import { ctxOf } from "../shared/enrich.ts";
import { log } from "../shared/log.ts";
import { assertReaderScrubMarker } from "../../packages/pipeline/src/lib/scrub-engine/reader-policy.ts";
import { canonicalManifestIdentity } from "../shared/manifest-identity.ts";
import type { DigestInputTruncation, SessionDigest, Summarizer } from "./summarizer.ts";
import { readSessionDigest, type SummaryInputCaps } from "./session-input.ts";
import { hasColumns, SUMMARY_COVERAGE_COLUMNS } from "../summary-regen/detect.ts";
import { regenActive, type RegenReason } from "../summary-regen/settings.ts";
import { clampAttempts, maxDueAttempts, nextRetryAt } from "../summary-regen/backoff.ts";
import { BOB_COSTAS_PIPELINE_VERSION } from "./versions.ts";
import { RETRYABLE_FALLBACKS, bobCostasRegenFromEnv, reasonFromWorkKey, type BobCostasRegen } from "./regen.ts";
import {
  ShortSummaryRejected,
  clearShortSummary,
  writeShortSummary,
  type SessionDirectoryKey,
  type ShortSummaryInput,
} from "../hoopdb/session-directory-summary.ts";
import {
  clearSummaryObject,
  readSummaryObject,
  summaryObjectKey,
  writeSummaryObject,
  type SummaryKind,
  type SummaryRecord,
  type SummaryStatus,
  type WorkspaceKeyring,
} from "./summary-objects.ts";

export interface BobCostasDeps {
  db: HoopDb;
  reader: ManifestReader;
  store: ObjectStore;
  summarizer: Summarizer;
  keyring: WorkspaceKeyring;
  now?: () => Date;
  /** The session_directory short_summary projection; defaults to the SQL writer on `db`. */
  directory?: ShortSummaryDirectory;
  /** READER_MAX_RULESET; undefined (default) keeps the legacy presence-only scrub check. */
  readerMaxRuleset?: number;
  /** The summary input caps (BOB_COSTAS_INPUT_SAMPLING, BOB_COSTAS_MAX_*); defaults to DEFAULT_SUMMARY_INPUT_CAPS. */
  inputCaps?: SummaryInputCaps;
  /**
   * Summaries regeneration flags (push A). undefined reads them from the environment when the
   * handler is built; null forces them off. With every flag off the handler runs today's queries only.
   */
  regen?: BobCostasRegen | null;
}

/** Where Bob Costas publishes the bounded, scrubbed short summary for search (G6). */
export interface ShortSummaryDirectory {
  write(input: ShortSummaryInput): Promise<{ written: boolean }>;
  clear(key: SessionDirectoryKey): Promise<boolean>;
}

export function sqlShortSummaryDirectory(db: HoopDb): ShortSummaryDirectory {
  return {
    write: (input) => writeShortSummary(db, input),
    clear: (key) => clearShortSummary(db, key),
  };
}

/**
 * The directory provenance for a summary model: `bob-costas/<model>`, folded into the
 * directory's source alphabet (lower-case `[a-z0-9._/-]`, at most 64 characters), since a
 * model id may hold upper case or `:` (MODEL_PATTERN) that the directory would refuse.
 * `extractive/v1` is unchanged.
 */
export function directorySource(model: string): string {
  return `bob-costas/${model.toLowerCase().replace(/[^a-z0-9._/-]/g, "-")}`.slice(0, 64);
}

/**
 * Publish (or clear) the directory short summary. Idempotent: writeShortSummary only
 * replaces an older-or-equal generated_at, clearShortSummary is a no-op when empty.
 */
async function publishShortSummary(
  directory: ShortSummaryDirectory,
  key: SessionDirectoryKey,
  ready: { summary: string; model: string; generatedAt: Date } | null,
  traceId: string,
): Promise<void> {
  if (!ready) {
    await directory.clear(key);
    return;
  }
  try {
    await directory.write({
      ...key,
      summary: ready.summary,
      source: directorySource(ready.model),
      generatedAt: ready.generatedAt,
    });
  } catch (err) {
    if (!(err instanceof ShortSummaryRejected)) throw err;
    log.info("short summary not projected", {
      stage: "summary-short",
      reason: err.reason,
      trace_id: traceId,
    });
  }
}

/**
 * The content hash a cleared-then-failed outcome (`failAfterPublish`) is recorded under: a
 * valid SHA-256 that never equals the raw's, so a replay of the same revision does not take
 * the summary_current shortcut and reads the session again (it may succeed once the caps or
 * the chunks change) instead of leaving the summary cleared until the next revision.
 */
export const notCurrentContent = (contentSha256: string) =>
  createHash("sha256").update(`bob-costas:not-current:${contentSha256}`).digest("hex");

/** SHA-256 of the manifest as indexed: the `summary-manifest` fence for the object. */
export function manifestSha256(m: ManifestV1): string {
  return createHash("sha256").update(JSON.stringify(m)).digest("hex");
}

interface Outcome {
  status: SummaryStatus;
  summary?: string;
  model?: string;
  tokensIn: number;
  tokensOut: number;
  records: number;
  /** Set when an input cap fired that makes the digest's counts lower bounds. */
  input?: DigestInputTruncation;
  /** User turns read but not kept (sampling on): the counts are exact, the turns a sample. */
  turnsDropped?: number;
  /**
   * Publish this (text-free) outcome, clearing any earlier summary, then fail the delivery
   * with this code: a session of delivery envelopes that could not be read whole (a summary
   * kept from before might show content since deleted or revised), or a capped read that
   * found no record. Recorded under `notCurrentContent`, so a replay reads again.
   */
  failAfterPublish?: string;
  /** User turns in the digest the summary was built from (undefined: no digest). */
  turns?: number;
  /** The model summariser's fallback reason, when it published extractive text instead. */
  fallbackReason?: string;
}

/**
 * A session read for the summary: no digest (quarantined, unsupported, empty) or the digest. `failAfterPublish`
 * on a quarantined read is the code the delivery fails with after the text-free outcome is published (Outcome).
 */
type Digested =
  | { status: "quarantined"; failAfterPublish?: string }
  | { status: "unsupported" | "empty" }
  | { status: "digest"; digest: SessionDigest };

/**
 * The session's location, including the deployment prefix the manifest lives
 * under (`<PIPELINE_KEY_PREFIX>[/<org>]`, E3): chunks and the summary object
 * sit beside the manifest, never at the unprefixed `v1/...` key.
 */
function sessionLocation(m: ManifestV1, manifestKey: string) {
  const s = m.session;
  const prefix = parseSessionKey(manifestKey)?.prefix;
  return {
    ...(prefix ? { prefix } : {}),
    workspaceId: s.workspace_id,
    machineId: s.machine_id,
    harness: s.harness,
    sessionId: s.session_id,
    startDate: s.started_at.slice(0, 10),
  };
}

/** Reads the scrubbed chunks and digests them; no model call. */
async function loadDigest(m: ManifestV1, deps: BobCostasDeps, manifestKey: string): Promise<Digested> {
  // Scrub quarantine: an unscrubbed session is never read.
  if (!m.raw.scrub) return { status: "quarantined" };
  // A set maximum refuses a ruleset newer than this reader supports before any chunk GET.
  assertReaderScrubMarker(m.raw.scrub, deps.readerMaxRuleset);
  if (m.raw.format === "sqlite-rows") return { status: "unsupported" };
  // Streamed chunk by chunk within the input caps (session-input.ts): never the whole
  // session in memory, whatever its chunk count or size.
  let digest;
  try {
    digest = await readSessionDigest({
      manifest: m,
      location: sessionLocation(m, manifestKey),
      store: deps.store,
      ...(deps.readerMaxRuleset === undefined ? {} : { readerMaxRuleset: deps.readerMaxRuleset }),
      ...(deps.inputCaps ? { caps: deps.inputCaps } : {}),
    });
  } catch (err) {
    // Privacy first: clear the envelope session's summary, then dead-letter.
    if (err instanceof PipelineError && err.code === "summary_input_incomplete")
      return { status: "quarantined", failAfterPublish: err.code };
    throw err;
  }
  if (digest.records === 0) {
    // A capped read that found nothing says nothing about the session: never publish it as
    // `empty`. Any earlier summary may be stale too, so it is cleared, then the delivery fails.
    if (digest.input) return { status: "quarantined", failAfterPublish: "summary_input_unreadable" };
    return { status: "empty" };
  }
  return { status: "digest", digest };
}

async function buildOutcome(
  kind: SummaryKind,
  m: ManifestV1,
  deps: BobCostasDeps,
  manifestKey: string,
  orgId: string,
  preloaded?: Digested,
): Promise<Outcome> {
  const none = { tokensIn: 0, tokensOut: 0, records: 0 };
  const read = preloaded ?? (await loadDigest(m, deps, manifestKey));
  if (read.status === "quarantined" && read.failAfterPublish) return { status: "quarantined", ...none, failAfterPublish: read.failAfterPublish };
  if (read.status !== "digest") return { status: read.status, ...none, ...(read.status === "empty" ? { turns: 0 } : {}) };
  const digest = read.digest;
  const s = m.session;
  // Ids only: the model summariser's per-org budget and per-session cap key on these.
  // The summariser never throws for a model failure; it falls back to extractive.
  const summary = await deps.summarizer.summarize(kind, digest, {
    orgId,
    sessionKey: JSON.stringify([s.workspace_id, s.harness, canonicalManifestIdentity(m).sessionId]),
    traceId: m.trace_id,
  });
  return {
    status: "ready",
    summary: summary.text,
    model: summary.modelId,
    tokensIn: summary.tokensIn,
    tokensOut: summary.tokensOut,
    records: digest.records,
    ...(digest.input ? { input: digest.input } : {}),
    ...(digest.userTurnsSampled ? { turnsDropped: digest.userTurnCount - digest.userTurns.length } : {}),
    // Every user turn read (userTurnCount), not only those a sample kept: with sampling off the two are equal.
    turns: digest.userTurnCount,
    ...(summary.fallbackReason ? { fallbackReason: summary.fallbackReason } : {}),
  };
}

/** The pointer row as the regeneration-aware handler reads it (hoopdb 0006 columns). */
interface CoverageRow {
  covered_turns: number | null;
  failure_code: string | null;
  degraded: boolean;
  attempts: number;
  next_retry_at: Date | string | null;
  config_hash: string | null;
  pipeline_version: string | null;
}

const SESSION_ACTIVITY_SQL = `SELECT state, last_activity_at < $6 AS idle FROM sessions.session_index
        WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5`;

/** ended (done/abandoned) or idle past SUMMARY_REGEN_IDLE_SECONDS, from session_index; null when neither. */
async function sessionQuiet(deps: BobCostasDeps, pk: unknown[], idleSeconds: number, now: Date): Promise<"ended_uncovered" | "idle_uncovered" | null> {
  const r = await deps.db.query<{ state: string; idle: boolean }>(SESSION_ACTIVITY_SQL, [
    ...pk,
    new Date(now.getTime() - idleSeconds * 1000).toISOString(),
  ]);
  const row = r.rows[0];
  if (!row) return null;
  if (row.state === "done" || row.state === "abandoned") return "ended_uncovered";
  return row.idle === true ? "idle_uncovered" : null;
}

/**
 * Whether a current, ready summary is due for regeneration anyway (flags (1), (2), (2b)). Returns the
 * reason, plus the digest when it had to be read for the turn check so the rebuild does not read it again.
 * A legacy row (covered_turns NULL) that is not due is stamped with the turns it covers, so the sweep's
 * "uncovered" branch does not queue it again.
 */
async function regenDue(
  regen: BobCostasRegen,
  row: CoverageRow & { status: string },
  m: ManifestV1,
  deps: BobCostasDeps,
  manifestKey: string,
  pk: unknown[],
  kind: SummaryKind,
  now: Date,
): Promise<{ reason: RegenReason; digest?: Digested } | null> {
  if (row.status !== "ready") return null;
  const { turns, retry } = regen.settings;
  if (retry.configReset && row.degraded && row.config_hash !== regen.configHash) return { reason: "config_changed" };
  const attempts = Number(row.attempts);
  if (
    retry.enabled &&
    row.degraded &&
    attempts >= 1 &&
    attempts <= maxDueAttempts(retry.backoffSeconds) &&
    row.next_retry_at !== null &&
    new Date(row.next_retry_at).getTime() <= now.getTime()
  )
    return { reason: "retry_backoff" };
  // Same revision and content: a stamped row covers exactly the turns it was built from. Only a row
  // written before the migration (covered_turns NULL, "uncovered") needs the digest.
  if (!turns.enabled || row.covered_turns !== null) return null;
  const digest = await loadDigest(m, deps, manifestKey);
  // No digest (empty, or a manifest no longer readable): nothing to regenerate from; the stamp of 0
  // below keeps the sweep's "uncovered" branch from queueing the row again every tick.
  const turnsNow = digest.status === "digest" ? digest.digest.userTurnCount : 0;
  if (digest.status === "digest" && turnsNow >= turns.threshold) return { reason: "turns_gained", digest };
  const quiet = turnsNow > 0 ? await sessionQuiet(deps, pk, turns.idleSeconds, now) : null;
  if (quiet) return { reason: quiet, digest };
  await deps.db.query(
    `UPDATE sessions.session_summaries SET covered_turns = $8
      WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5 AND kind=$6
        AND revision=$7 AND covered_turns IS NULL`,
    [...pk, kind, m.revision, turnsNow],
  );
  return null;
}

export function bobCostasHandler(
  kind: SummaryKind,
  deps: BobCostasDeps,
): StageHandler {
  const regenConfig = deps.regen === undefined ? bobCostasRegenFromEnv() : deps.regen;
  const coverage = regenConfig && regenActive(regenConfig.settings) ? regenConfig : null;
  return async (message, recorder) => {
    const pointer = message.payload as RawPointer;
    if (!pointer || typeof pointer.manifestKey !== "string")
      throw new PipelineError(
        "invalid_pointer",
        `summary-${kind} payload is not a raw pointer`,
        false,
      );
    const read = await deps.reader.read(pointer.manifestKey);
    if (!read) return { status: "skipped", reason: "manifest_missing" };
    const m = read.manifest;
    const s = m.session;
    const pk = [
      message.ctx.orgId,
      s.workspace_id,
      s.harness,
      s.session_id,
      s.machine_id,
    ];
    // With a coverage flag on and hoopdb 0006 applied, the row also carries the coverage columns;
    // otherwise (flags off, or the migration not yet applied) the query is today's, unchanged.
    const regen =
      coverage && (await hasColumns(deps.db, "sessions.session_summaries", SUMMARY_COVERAGE_COLUMNS)) ? coverage : null;
    type PointerRow = {
      content_sha256: string;
      revision: string;
      status: string;
      key_id: string | null;
      object_key: string | null;
      object_sha256: string | null;
    };
    const existing = regen
      ? await deps.db.query<PointerRow & CoverageRow>(
          `SELECT content_sha256, revision, status, key_id, object_key, object_sha256,
                  covered_turns, failure_code, degraded, attempts, next_retry_at, config_hash, pipeline_version
             FROM sessions.session_summaries
            WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5 AND kind=$6`,
          [...pk, kind],
        )
      : await deps.db.query<PointerRow>(
          `SELECT content_sha256, revision, status, key_id, object_key, object_sha256 FROM sessions.session_summaries
        WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5 AND kind=$6`,
          [...pk, kind],
        );
    const row = existing.rows[0];
    if (row && Number(row.revision) > m.revision)
      return { status: "skipped", reason: "stale_revision" };
    const ids = { orgId: message.ctx.orgId, workspaceId: s.workspace_id };
    // Current only if it is the same revision and content and, when ready, under the current key
    // (after a rotation the summary is re-encrypted rather than stranded on the old key) and at
    // the object key beside this manifest (a session moved under a prefix is rebuilt there).
    const currentKeyId = (await deps.keyring.currentKey(ids)).keyId;
    const expectedKey = summaryObjectKey(
      sessionLocation(m, pointer.manifestKey),
      kind,
    );
    const isCurrent =
      row &&
      Number(row.revision) === m.revision &&
      row.content_sha256 === m.raw.content_sha256 &&
      (row.status !== "ready" ||
        (row.key_id === currentKeyId && row.object_key === expectedKey));
    // Flags (1), (2), (2b): a current row may still be due (uncovered turns, a retry, a config change).
    const due =
      isCurrent && regen
        ? await regenDue(regen, row as PointerRow & CoverageRow, m, deps, pointer.manifestKey, pk, kind, deps.now?.() ?? new Date())
        : null;
    if (isCurrent && !due) {
      // The encrypted object and pointer are current, but a previous run may have
      // failed after COMMIT and before the directory write. Reconcile the directory
      // from the published object on every retry.
      if (kind === "short") {
        const directory = deps.directory ?? sqlShortSummaryDirectory(deps.db);
        const dirKey = {
          orgId: message.ctx.orgId,
          workspaceId: s.workspace_id,
          harness: s.harness,
          sessionId: canonicalManifestIdentity(m).sessionId,
        };
        if (row.status !== "ready") {
          await publishShortSummary(directory, dirKey, null, m.trace_id);
        } else {
          // Always authenticate and republish the current object, even when the row
          // already holds a summary: it may be an older revision's text left behind by
          // a failed write. writeShortSummary's generated_at fence makes this idempotent
          // and never lets an older object replace a newer one.
          // Integrity failures are permanent: clear first (no possibly stale text stays
          // searchable), then fail non-retryably so the message goes to the DLQ instead
          // of retrying forever.
          const integrity = async (message: string): Promise<never> => {
            // Ids and hashes only: the object key is a path, so it is logged as its SHA-256.
            log.warn("short summary object integrity failure; dead-lettering", {
              stage: "summary-short",
              error_code: "summary_object_integrity",
              retryable: false,
              org_id: dirKey.orgId,
              workspace_id: dirKey.workspaceId,
              harness: dirKey.harness,
              session_id: dirKey.sessionId,
              machine_id: s.machine_id,
              revision: m.revision,
              trace_id: m.trace_id,
              key_sha256: row.object_key
                ? createHash("sha256").update(row.object_key).digest("hex")
                : undefined,
            });
            await publishShortSummary(directory, dirKey, null, m.trace_id);
            throw new PipelineError("summary_object_integrity", message, false);
          };
          if (!row.object_key || !row.object_sha256)
            await integrity("a ready short summary pointer has no object");
          let published: SummaryRecord | null;
          try {
            published = await readSummaryObject({
              store: deps.store,
              keyring: deps.keyring,
              ids,
              key: row.object_key!,
              sha256: row.object_sha256!,
              kind,
              revision: m.revision,
            });
          } catch (err) {
            // Record the original failure first, so a failing clear cannot mask it. Then
            // clear (best effort) and rethrow the original error with its own retry
            // class: a transient store error retries; a non-retryable keyring error (an
            // unknown or retired key id) dead-letters instead of looping.
            const original = err instanceof PipelineError ? err : null;
            log.warn("short summary object read failed", {
              stage: "summary-short",
              error_code: original?.code ?? "summary_object_read_failed",
              retryable: original?.retryable ?? true,
              session_id: dirKey.sessionId,
              revision: m.revision,
              trace_id: m.trace_id,
            });
            try {
              await publishShortSummary(directory, dirKey, null, m.trace_id);
            } catch (clearErr) {
              log.warn("short summary clear after read failure also failed", {
                stage: "summary-short",
                error_code:
                  clearErr instanceof PipelineError
                    ? clearErr.code
                    : "summary_directory_clear_failed",
                session_id: dirKey.sessionId,
                trace_id: m.trace_id,
              });
            }
            throw err;
          }
          if (
            published?.status !== "ready" ||
            !published.summary ||
            !published.model
          ) {
            return await integrity(
              "the current short summary object is missing or cannot be authenticated",
            );
          }
          await publishShortSummary(
            directory,
            dirKey,
            {
              summary: published.summary,
              model: published.model,
              generatedAt: new Date(published.built_at),
            },
            m.trace_id,
          );
        }
      }
      return { status: "skipped", reason: "summary_current" };
    }

    const outcome = await buildOutcome(kind, m, deps, pointer.manifestKey, message.ctx.orgId, due?.digest);
    const loc = sessionLocation(m, pointer.manifestKey);
    const key = summaryObjectKey(loc, kind);
    const current = { revision: m.revision, manifestSha256: manifestSha256(m) };
    const record: SummaryRecord = {
      v: 1,
      kind,
      revision: m.revision,
      status: outcome.status,
      ...(outcome.status === "ready"
        ? {
            summary: outcome.summary!,
            model: outcome.model!,
            source_content_sha256: m.raw.content_sha256,
            ...(outcome.input ? { input_truncated: true as const } : {}),
            ...(outcome.turnsDropped ? { turns_sampled: true as const } : {}),
          }
        : {}),
      built_at: (deps.now?.() ?? new Date()).toISOString(),
      trace_id: m.trace_id,
    };
    const ctx = ctxOf(message);

    const client = await deps.db.pool.connect();
    let published: { sha256: string; keyId: string } | undefined;
    try {
      await client.query("BEGIN");
      await client.query(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        [`96bulls:summary:${key}`],
      );
      // Under the lock, a newer revision may have published meanwhile: it owns the object.
      const locked = regen
        ? await client.query<{ revision: string; degraded: boolean; attempts: number; config_hash: string | null }>(
            `SELECT revision, degraded, attempts, config_hash FROM sessions.session_summaries
              WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5 AND kind=$6`,
            [...pk, kind],
          )
        : await client.query<{ revision: string }>(
            `SELECT revision FROM sessions.session_summaries
          WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5 AND kind=$6`,
            [...pk, kind],
          );
      if (locked.rows[0] && Number(locked.rows[0].revision) > m.revision) {
        await client.query("ROLLBACK");
        return { status: "skipped", reason: "stale_revision" };
      }
      if (outcome.status === "ready") {
        const written = await writeSummaryObject({
          store: deps.store,
          keyring: deps.keyring,
          ids,
          key,
          record,
          current,
        });
        if (written.outcome !== "written") {
          await client.query("ROLLBACK");
          return { status: "skipped", reason: "stale_revision" };
        }
        published = { sha256: written.sha256, keyId: written.keyId };
      } else {
        // Overwrite any earlier summary with a text-free record; create nothing otherwise.
        await clearSummaryObject({
          store: deps.store,
          keyring: deps.keyring,
          ids,
          key,
          record,
          current,
        });
      }
      const upsertParams = [
        ...pk,
        kind,
        ctx.session_uuid ?? null,
        m.trace_id,
        m.revision,
        outcome.failAfterPublish ? notCurrentContent(m.raw.content_sha256) : m.raw.content_sha256,
        outcome.status,
        published ? String(key) : null,
        published?.sha256 ?? null,
        published?.keyId ?? null,
        outcome.tokensIn,
        outcome.tokensOut,
      ];
      if (regen) {
        // Coverage: the turns this summary covers, and the retry state of a model fallback. Attempts
        // count consecutive retryable fallbacks under one config (a config change starts again at 0);
        // failure n of L waits backoff[n-1], failure L+1 is terminal (next_retry_at NULL).
        const prior = (locked.rows[0] ?? null) as unknown as { degraded: boolean; attempts: number; config_hash: string | null } | null;
        const { retry } = regen.settings;
        // degraded marks published extractive text a later run may improve (retry or config change);
        // the attempt counter and next_retry_at move only with the retry flag on.
        const degraded =
          outcome.status === "ready" && outcome.fallbackReason !== undefined && RETRYABLE_FALLBACKS.has(outcome.fallbackReason);
        const retryable = retry.enabled && degraded;
        const priorAttempts =
          prior && prior.degraded && (!retry.configReset || prior.config_hash === regen.configHash) ? Number(prior.attempts) : 0;
        const attempts = retryable ? clampAttempts(priorAttempts + 1) : 0;
        const nextRetry = retryable ? nextRetryAt(attempts, retry.backoffSeconds, deps.now?.() ?? new Date()) : null;
        await client.query(
          `INSERT INTO sessions.session_summaries
             (org_id, workspace_id, harness, session_id, machine_id, kind, session_uuid, trace_id, revision,
              content_sha256, status, object_key, object_sha256, key_id, tokens_in, tokens_out, generated_at,
              covered_turns, failure_code, degraded, attempts, next_retry_at, config_hash, pipeline_version)
           VALUES ($1,$2,$3,$4,$5,$6,$7::uuid,$8::uuid,$9,$10,$11,$12,$13,$14,$15,$16, now(),
                   $17::integer, $18, $19::boolean, $20::smallint, $21::timestamptz, $22, $23)
           ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id, kind) DO UPDATE SET
             session_uuid = coalesce(excluded.session_uuid, sessions.session_summaries.session_uuid),
             trace_id = excluded.trace_id, revision = excluded.revision, content_sha256 = excluded.content_sha256,
             status = excluded.status, object_key = excluded.object_key, object_sha256 = excluded.object_sha256,
             key_id = excluded.key_id, tokens_in = excluded.tokens_in, tokens_out = excluded.tokens_out, generated_at = now(),
             covered_turns = excluded.covered_turns, failure_code = excluded.failure_code, degraded = excluded.degraded,
             attempts = excluded.attempts, next_retry_at = excluded.next_retry_at, config_hash = excluded.config_hash,
             pipeline_version = excluded.pipeline_version`,
          [
            ...upsertParams,
            outcome.turns ?? null,
            outcome.fallbackReason ?? null,
            degraded,
            attempts,
            nextRetry ? nextRetry.toISOString() : null,
            regen.configHash,
            BOB_COSTAS_PIPELINE_VERSION,
          ],
        );
      } else await client.query(
        `INSERT INTO sessions.session_summaries
           (org_id, workspace_id, harness, session_id, machine_id, kind, session_uuid, trace_id, revision,
            content_sha256, status, object_key, object_sha256, key_id, tokens_in, tokens_out, generated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7::uuid,$8::uuid,$9,$10,$11,$12,$13,$14,$15,$16, now())
         ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id, kind) DO UPDATE SET
           session_uuid = coalesce(excluded.session_uuid, sessions.session_summaries.session_uuid),
           trace_id = excluded.trace_id, revision = excluded.revision, content_sha256 = excluded.content_sha256,
           status = excluded.status, object_key = excluded.object_key, object_sha256 = excluded.object_sha256,
           key_id = excluded.key_id, tokens_in = excluded.tokens_in, tokens_out = excluded.tokens_out, generated_at = now()`,
        upsertParams,
      );
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
    // G6: the short summary also feeds the tenant-scoped directory search. The writer
    // re-scrubs and bounds it and records provenance; a refused summary is logged by
    // reason only (the encrypted object above is unaffected), and any other summary
    // status clears a stale one.
    if (kind === "short") {
      const directory = deps.directory ?? sqlShortSummaryDirectory(deps.db);
      const dirKey = {
        orgId: message.ctx.orgId,
        workspaceId: s.workspace_id,
        harness: s.harness,
        sessionId: canonicalManifestIdentity(m).sessionId,
      };
      await publishShortSummary(
        directory,
        dirKey,
        outcome.status === "ready"
          ? {
              summary: outcome.summary!,
              model: outcome.model!,
              generatedAt: new Date(record.built_at),
            }
          : null,
        m.trace_id,
      );
    }
    // Ids, counts and statuses only: never the summary or the model's output.
    recorder.log.info("summary written", {
      kind,
      status: outcome.status,
      tokens_in: outcome.tokensIn,
      tokens_out: outcome.tokensOut,
    });
    // Why this run regenerated, when a regeneration flag caused it (closed set; absent otherwise).
    let regenerationReason: RegenReason | undefined = due?.reason;
    if (!regenerationReason && regen) {
      const queued = reasonFromWorkKey((message as { workKey?: unknown }).workKey);
      const prior = row as (PointerRow & CoverageRow) | undefined;
      if (queued === "retry_backoff" || queued === "config_changed") regenerationReason = queued;
      else if (
        regen.settings.turns.enabled &&
        prior?.covered_turns != null &&
        outcome.turns !== undefined &&
        outcome.turns - Number(prior.covered_turns) >= regen.settings.turns.threshold
      )
        regenerationReason = "turns_gained";
      else if (queued === "turns")
        regenerationReason =
          (await sessionQuiet(deps, pk, regen.settings.turns.idleSeconds, deps.now?.() ?? new Date())) ?? undefined;
    }
    log.info("bob costas summary", {
      stage: `summary-${kind}`,
      kind,
      status: outcome.status,
      model_id: outcome.model,
      tokens_in: outcome.tokensIn,
      tokens_out: outcome.tokensOut,
      trace_id: m.trace_id,
      revision: m.revision,
      session_uuid: ctx.session_uuid,
      count: outcome.records,
      // Counts only: how much of a capped session the digest read, and how many turns it kept.
      ...(outcome.input
        ? {
            reason: "input_truncated",
            chunks_read: outcome.input.chunksRead,
            chunks_total: outcome.input.chunksTotal,
            bytes_read: outcome.input.bytesRead,
            names_dropped: outcome.input.namesDropped,
          }
        : {}),
      ...(outcome.turnsDropped ? { turns_dropped: outcome.turnsDropped } : {}),
      ...(outcome.failAfterPublish ? { reason: outcome.failAfterPublish, error_code: outcome.failAfterPublish, retryable: false } : {}),
      ...(regenerationReason ? { regeneration_reason: regenerationReason } : {}),
    });
    if (outcome.failAfterPublish)
      throw new PipelineError(outcome.failAfterPublish, "the summary input could not be read; the summary was cleared", false);
  };
}
