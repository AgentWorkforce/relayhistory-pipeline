/**
 * Pure verdicts for the planted-session trace, V1 search scope and the #38
 * stale-summary case. The CLI collects observations; these decide.
 */
import type { PlantedSession } from "./corpus.ts";
import { summaryLeaks } from "./leak-scan.ts";
import { stageLatency, summarize, type LatencySummary } from "./stats.ts";

export interface StageTimes {
  objectLanded?: number;
  indexed?: number;
  summary?: number;
}

export interface SessionObservation {
  sessionId: string;
  s3Present: boolean;
  indexRow: boolean;
  times: StageTimes;
  shortSummary?: string | null;
  /** Did a search for the session's summaryTerm, as its own tenant, return it? */
  searchFound?: boolean;
  searchLatencyMs?: number;
  searchError?: string;
}

export interface TraceVerdict {
  failures: string[];
  metrics: {
    planted: number;
    stageCounts: { s3: number; index: number; summary: number; search: number };
    landedToIndexedMs: LatencySummary;
    landedToSummaryMs: LatencySummary;
    searchQueryMs: LatencySummary;
  };
}

const MAX_LISTED = 20;

function listed(failures: string[], label: string, ids: string[]) {
  if (!ids.length) return;
  failures.push(`${label}: ${ids.length} session(s)${ids.length > MAX_LISTED ? `; first ${MAX_LISTED}` : ""}: ${ids.slice(0, MAX_LISTED).join(", ")}`);
}

/**
 * Each planted session must be traceable S3 object -> HoopDb index row ->
 * short_summary populated (and scrubbed) -> found by V1 search as its own
 * tenant. `forbiddenInSummary` is the fake credential plus transcript-only text.
 */
export function evaluatePlantedTrace(
  planted: readonly PlantedSession[],
  obs: ReadonlyMap<string, SessionObservation>,
  opts: { expectedSummaries?: ReadonlyMap<string, string>; forbiddenInSummary: (p: PlantedSession) => string[]; searchChecked: boolean },
): TraceVerdict {
  const failures: string[] = [];
  const missing = { s3: [] as string[], index: [] as string[], summary: [] as string[], search: [] as string[], unexpected: [] as string[], leak: [] as string[] };
  const rows = planted.map((p) => ({ p, o: obs.get(p.sessionId) }));
  for (const { p, o } of rows) {
    if (!o?.s3Present) missing.s3.push(p.sessionId);
    if (!o?.indexRow) missing.index.push(p.sessionId);
    if (!o?.shortSummary) missing.summary.push(p.sessionId);
    else {
      if (summaryLeaks(o.shortSummary, opts.forbiddenInSummary(p)).length) missing.leak.push(p.sessionId);
      const want = opts.expectedSummaries?.get(p.sessionId);
      if (want !== undefined && o.shortSummary !== want) missing.unexpected.push(p.sessionId);
    }
    if (opts.searchChecked && !o?.searchFound) missing.search.push(p.sessionId);
  }
  listed(failures, "S3 manifest object missing", missing.s3);
  listed(failures, "HoopDb index row missing", missing.index);
  listed(failures, "short_summary not populated", missing.summary);
  listed(failures, "short_summary differs from the expected extractive summary", missing.unexpected);
  listed(failures, "short_summary carries planted secret or transcript-only text", missing.leak);
  if (opts.searchChecked) listed(failures, "V1 search did not return the session to its own tenant", missing.search);
  const all = rows.map((r) => r.o);
  const searchSamples = all.filter((o) => o?.searchLatencyMs !== undefined && !o.searchError).map((o) => o!.searchLatencyMs!);
  const searchErrors = opts.searchChecked ? all.filter((o) => !o || o.searchError || o.searchLatencyMs === undefined).length : 0;
  return {
    failures,
    metrics: {
      planted: planted.length,
      stageCounts: {
        s3: planted.length - missing.s3.length,
        index: planted.length - missing.index.length,
        summary: planted.length - missing.summary.length,
        search: opts.searchChecked ? planted.length - missing.search.length : 0,
      },
      landedToIndexedMs: stageLatency(all, (o) => o?.times.objectLanded, (o) => o?.times.indexed),
      landedToSummaryMs: stageLatency(all, (o) => o?.times.objectLanded, (o) => o?.times.summary),
      searchQueryMs: summarize(searchSamples, searchErrors),
    },
  };
}

/**
 * Check 4. Full-transcript search is UNSUPPORTED on V1 and the report says so.
 * The probe that backs it: searching each tenant's transcript-only canary must
 * return nothing. A hit means transcript text reached search_text, which breaks
 * the session-search-index contract; that is a failure, not "support".
 */
export function evaluateV1Scope(transcriptProbes: ReadonlyArray<{ hits: number; error?: string }>): { status: "unsupported" | "fail" | "blocked"; failures: string[] } {
  if (transcriptProbes.length === 0) return { status: "blocked", failures: [] };
  const failures: string[] = [];
  const hits = transcriptProbes.reduce((n, p) => n + p.hits, 0);
  const errors = transcriptProbes.filter((p) => p.error);
  if (hits > 0) failures.push(`V1 search returned ${hits} row(s) for transcript-only text: transcript content reached search_text`);
  if (errors.length) failures.push(`${errors.length} transcript-scope probe(s) errored: ${[...new Set(errors.map((e) => e.error))].join(", ")}`);
  return { status: failures.length ? "fail" : "unsupported", failures };
}

/**
 * The #38 HOLD regression (99e96d1, 134afa8): after a session is re-ingested
 * and its summary invalidated, the directory must not keep serving the old
 * revision's short_summary. Acceptable end states: the new revision's text, or
 * no summary at all (cleared). Search must agree with the row.
 */
export interface StaleObservation {
  sessionId: string;
  rev1Summary: string;
  rev1GeneratedAtMs?: number;
  /** Server time the revision-2 manifest landed (object-landed ok). */
  rev2LandedAtMs?: number;
  /** Expected revision-2 summary when summaries are predictable; otherwise "anything but rev1". */
  rev2Expected?: string;
  current?: string | null;
  currentGeneratedAtMs?: number;
  /** short_summary texts the tenant's search returned for this session. */
  searchSummaries: Array<string | null>;
  deadlineReached: boolean;
}

export type StaleVerdict = { state: "ok" | "pending" | "stale"; reason: string };

export function evaluateStaleSummary(o: StaleObservation): StaleVerdict {
  const servedStale = o.current === o.rev1Summary && o.rev2Expected !== o.rev1Summary;
  const searchStale = o.searchSummaries.some((s) => s === o.rev1Summary && o.rev2Expected !== o.rev1Summary);
  if (searchStale && o.current !== o.rev1Summary) return { state: "stale", reason: "search served the old revision's summary while the row had moved on" };
  if (!servedStale && !searchStale) {
    if (o.current === null || o.current === undefined) return { state: "ok", reason: "summary cleared" };
    if (o.rev2Expected !== undefined && o.current !== o.rev2Expected) return { state: "stale", reason: "summary is neither revision 1 nor the expected revision 2 text" };
    if (o.rev1GeneratedAtMs !== undefined && o.currentGeneratedAtMs !== undefined && o.currentGeneratedAtMs < o.rev1GeneratedAtMs)
      return { state: "stale", reason: "summary generated_at went backwards" };
    return { state: "ok", reason: "revision 2 summary served" };
  }
  return o.deadlineReached
    ? { state: "stale", reason: "old revision's summary still served after the re-ingest deadline" }
    : { state: "pending", reason: "waiting for revision 2's summary" };
}
