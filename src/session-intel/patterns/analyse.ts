/** Descriptive fact profiles -> bounded candidate discovery -> connected activity groups. No detectors or models. */
import type { SnapshotSession } from "../replay/snapshot.ts";
import { canonicalJson, sha256Hex, textCompare } from "../replay/util.ts";
import { PatternError } from "./corpus.ts";
import { ANALYSIS_BOUNDS, ANALYSIS_VERSION, PATTERN_FORMAT, type AnalysisOptions, type Corpus, type EvidencePair, type PatternCandidate, type PatternReport, type ProfileRow, type SessionRef } from "./types.ts";

const ref = (s: SnapshotSession): SessionRef => ({ harness: s.harness, sessionId: s.sessionId });
const refKey = (s: SessionRef) => JSON.stringify([s.harness, s.sessionId]);
const scopeOf = (s: SnapshotSession): PatternCandidate["scope"] | null => s.repo ? { kind: "repo", value: s.repo } : s.projectId ? { kind: "project", value: s.projectId } : null;
const blank = (key: string): ProfileRow => ({ key, sessions: 0, sessionsWithWrites: 0, sessionsWithCommits: 0, sessionsWithOutcomes: 0, sessionsWithClaims: 0, sessionsWithSummaries: 0, writes: 0, commits: 0, shaShapedCommits: 0, outcomes: 0, claims: 0 });
const row = (map: Map<string, ProfileRow>, key: string) => { if (!map.has(key)) map.set(key, blank(key)); return map.get(key)!; };
const boundedInt = (n: number, min: number, max: number) => { if (!Number.isSafeInteger(n) || n < min || n > max) throw new PatternError("analysis parameter outside its integer bounds"); return n; };
interface Write { session: SnapshotSession; path: string; at: string; ms: number; eventId: string; branch: string | null }
interface FileActivity { scope: PatternCandidate["scope"]; path: string; events: Write[]; sessions: Set<string>; harnesses: Set<string> }
interface Held { candidate: PatternCandidate; paths: Set<string>; branches: Set<string> }
const evidenceTime = (e: EvidencePair) => e.a.at > e.b.at ? e.a.at : e.b.at;
const evidenceCompare = (a: EvidencePair, b: EvidencePair) => textCompare(evidenceTime(a), evidenceTime(b)) || textCompare(canonicalJson(a), canonicalJson(b));

/** Pattern definitions and parameters are versioned independently of alert scoring. */
export function analyseCorpus(corpus: Corpus, options: AnalysisOptions = {}): PatternReport {
  const nearbyMs = boundedInt(options.nearbyMs ?? 30 * 60_000, 0, 24 * 60 * 60_000);
  const maxComparisons = boundedInt(options.maxComparisons ?? 200_000, 1, 5_000_000);
  const maxCandidates = boundedInt(options.maxCandidates ?? 100, 1, 1_000);
  const parameters = { version: ANALYSIS_VERSION, nearbyMs, maxComparisons, maxCandidates };
  const definition = { ...parameters, hash: sha256Hex(canonicalJson(parameters)) };
  const inWindow = (ts: string) => ts >= corpus.window.since && ts < corpus.window.until;
  const byHarness = new Map<string, ProfileRow>(), byRepository = new Map<string, ProfileRow>();
  const all = blank("all"), files = new Map<string, FileActivity>(), days = new Map<string, { writes: number; sessions: Set<string> }>();
  const spanEdges: Array<[number, number]> = [];
  let sessionsOutsideWindow = 0, factsOutsideWindow = 0, duplicateWrites = 0, sessionsWithoutScope = 0, zeroLengthSpans = 0;
  for (const s of corpus.sessions) {
    const scope = scopeOf(s), key = refKey(s), seen = new Map<string, string>();
    let writes = 0;
    for (const w of s.writes) {
      if (!inWindow(w.ts)) { factsOutsideWindow++; continue; }
      const eventKey = JSON.stringify([w.eventId, w.path, w.ts]);
      const metadata = JSON.stringify([w.kind, w.branch ?? s.branch]);
      if (seen.has(eventKey)) {
        if (seen.get(eventKey) !== metadata) throw new PatternError("conflicting duplicate write evidence");
        duplicateWrites++; continue;
      }
      seen.set(eventKey, metadata); writes++;
      const day = w.ts.slice(0, 10);
      if (!days.has(day)) days.set(day, { writes: 0, sessions: new Set() });
      days.get(day)!.writes++; days.get(day)!.sessions.add(key);
      if (!scope) continue;
      const fileKey = JSON.stringify([scope.kind, scope.value, w.path]);
      if (!files.has(fileKey)) files.set(fileKey, { scope, path: w.path, events: [], sessions: new Set(), harnesses: new Set() });
      const file = files.get(fileKey)!;
      file.events.push({ session: s, path: w.path, at: w.ts, ms: Date.parse(w.ts), eventId: w.eventId, branch: w.branch ?? s.branch });
      file.sessions.add(key); file.harnesses.add(s.harness);
    }
    const count = (facts: Array<{ ts?: string; at?: string }>) => facts.filter((f) => {
      const ok = inWindow((f.ts ?? f.at)!); if (!ok) factsOutsideWindow++; return ok;
    }).length;
    const commits = count(s.commits), outcomes = count(s.outcomes), claims = count(s.claims);
    const shaShapedCommits = s.commits.filter((c) => inWindow(c.ts) && /^[a-f0-9]{7,64}$/iu.test(c.sha)).length;
    const summaries = count(s.summaryTimeline) > 0;
    const usage = count(s.usage); // Coverage only; not interpreted as wasted work.
    const start = Math.max(Date.parse(s.createdAt), Date.parse(corpus.window.since));
    const end = Math.min(Date.parse(s.lastActivityAt), Date.parse(corpus.window.until));
    if (start >= end && !inWindow(s.createdAt) && !(writes || commits || outcomes || claims || summaries || usage)) {
      sessionsOutsideWindow++; continue;
    }
    if (!scope) sessionsWithoutScope++;
    for (const p of [all, row(byHarness, s.harness), row(byRepository, scope ? JSON.stringify(scope) : "unscoped")]) {
      p.sessions++; p.writes += writes; p.commits += commits; p.outcomes += outcomes; p.claims += claims;
      p.shaShapedCommits += shaShapedCommits;
      if (writes) p.sessionsWithWrites++; if (commits) p.sessionsWithCommits++; if (outcomes) p.sessionsWithOutcomes++;
      if (claims) p.sessionsWithClaims++; if (summaries) p.sessionsWithSummaries++;
    }
    if (start === end) zeroLengthSpans++;
    if (start < end) spanEdges.push([start, 1], [end, -1]);
  }
  spanEdges.sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  let spans = 0, peakSessionSpans = 0;
  for (const [, delta] of spanEdges) { spans += delta; peakSessionSpans = Math.max(peakSessionSpans, spans); }
  const held = new Map<string, Held>();
  let comparisons = 0, comparisonLimit = false, pairLimit = false;
  scan: for (const [, file] of [...files].sort(([a], [b]) => textCompare(a, b))) {
    const events = file.events.sort((a, b) => a.ms - b.ms || textCompare(refKey(a.session), refKey(b.session)) || textCompare(a.eventId, b.eventId));
    // Sliding per-session queues avoid visiting quadratic same-session pairs. Every
    // other-session queue visited contains a comparison charged to the budget.
    const active = new Map<string, { events: Write[]; head: number }>();
    let left = 0;
    for (const current of events) {
      while (left < events.length && current.ms - events[left]!.ms > nearbyMs) {
        const old = events[left++]!, key = refKey(old.session), queue = active.get(key)!;
        queue.head++;
        if (queue.head === queue.events.length) active.delete(key);
      }
      const ownKey = refKey(current.session);
      for (const [key, queue] of active) {
        if (key === ownKey) continue;
        for (let j = queue.head; j < queue.events.length; j++) {
          if (comparisons >= maxComparisons) { comparisonLimit = true; break scan; }
          comparisons++;
          let a = queue.events[j]!, b = current;
          if (textCompare(refKey(a.session), refKey(b.session)) > 0) [a, b] = [b, a];
          const id = sha256Hex(canonicalJson(["nearby_shared_file_activity", ANALYSIS_VERSION, nearbyMs, file.scope, ref(a.session), ref(b.session)]));
          const observed = a.at > b.at ? a.at : b.at;
          if (!held.has(id)) {
            if (held.size >= ANALYSIS_BOUNDS.pairs) { pairLimit = true; continue; }
            const sameHarness = a.session.harness === b.session.harness;
            const direct = sameHarness && (a.session.parentSessionIds.includes(b.session.sessionId) || b.session.parentSessionIds.includes(a.session.sessionId));
            const sibling = sameHarness && a.session.parentSessionIds.some((p) => b.session.parentSessionIds.includes(p));
            held.set(id, { paths: new Set(), branches: new Set(), candidate: {
              id, kind: "nearby_shared_file_activity", review: "unreviewed", scope: file.scope,
              participants: [ref(a.session), ref(b.session)], sharedFiles: 0, eventPairs: 0,
              firstObservedAt: observed, lastObservedAt: observed, closestGapMs: Math.abs(a.ms - b.ms),
              sessionSpanOverlapMs: Math.max(0, Math.min(Date.parse(a.session.lastActivityAt), Date.parse(b.session.lastActivityAt), Date.parse(corpus.window.until))
                - Math.max(Date.parse(a.session.createdAt), Date.parse(b.session.createdAt), Date.parse(corpus.window.since))),
              parentLink: direct ? "direct" : sibling ? "shared_parent" : "not_observed", branchRelation: "unknown", evidence: [],
            } });
          }
          const h = held.get(id)!, c = h.candidate;
          h.paths.add(file.path); c.sharedFiles = h.paths.size; c.eventPairs++;
          c.firstObservedAt = c.firstObservedAt < observed ? c.firstObservedAt : observed;
          c.lastObservedAt = c.lastObservedAt > observed ? c.lastObservedAt : observed;
          c.closestGapMs = Math.min(c.closestGapMs, Math.abs(a.ms - b.ms));
          h.branches.add(a.branch && b.branch ? a.branch === b.branch ? "same" : "different" : "unknown");
          c.branchRelation = h.branches.size > 1 ? "mixed" : [...h.branches][0] as PatternCandidate["branchRelation"];
          const evidence = (w: Write) => ({ session: ref(w.session), eventId: w.eventId, path: w.path, at: w.at });
          c.evidence.push({ a: evidence(a), b: evidence(b) });
          c.evidence.sort(evidenceCompare); c.evidence.length = Math.min(c.evidence.length, ANALYSIS_BOUNDS.evidencePairs);
        }
      }
      if (!active.has(ownKey)) active.set(ownKey, { events: [], head: 0 });
      active.get(ownKey)!.events.push(current);
    }
  }
  const candidates = [...held.values()].map((h) => h.candidate).sort((a, b) => b.sharedFiles - a.sharedFiles || a.closestGapMs - b.closestGapMs || textCompare(a.id, b.id));
  const reasons = [...(comparisonLimit ? ["comparison_budget"] : []), ...(pairLimit ? ["candidate_pair_budget"] : [])];
  const groups = activityGroups(candidates);
  const { sessions: _, ...input } = corpus;
  return {
    format: PATTERN_FORMAT, definition, input: { ...input, sessions: corpus.sessions.length },
    coverage: { workspaceCompleteness: "not_established", quality: "not_evaluated", sessionsOutsideWindow,
      filesTotal: files.size, filesOmitted: Math.max(0, files.size - ANALYSIS_BOUNDS.files),
      repositoriesTotal: byRepository.size, repositoriesOmitted: Math.max(0, byRepository.size - ANALYSIS_BOUNDS.repositories),
      groupsTotal: groups.length, groupsOmitted: Math.max(0, groups.length - ANALYSIS_BOUNDS.groups), factsOutsideWindow, duplicateWrites, sessionsWithoutScope,
      comparisons, scanComplete: reasons.length === 0, reasons, candidatePairsRetained: candidates.length, candidatesOmittedFromReport: Math.max(0, candidates.length - maxCandidates) },
    profile: { all, byHarness: [...byHarness.values()].sort((a, b) => textCompare(a.key, b.key)),
      byRepository: [...byRepository.values()].sort((a, b) => b.sessions - a.sessions || textCompare(a.key, b.key)).slice(0, ANALYSIS_BOUNDS.repositories), peakSessionSpans, zeroLengthSpans,
      days: [...days].sort(([a], [b]) => textCompare(a, b)).map(([day, d]) => ({ day, writes: d.writes, sessionsWithWrites: d.sessions.size })),
      files: [...files.values()].sort((a, b) => b.sessions.size - a.sessions.size || b.events.length - a.events.length || textCompare(JSON.stringify([a.scope, a.path]), JSON.stringify([b.scope, b.path])))
        .slice(0, ANALYSIS_BOUNDS.files).map((f) => ({ scope: f.scope, path: f.path, writes: f.events.length, sessions: f.sessions.size, harnesses: f.harnesses.size })),
    },
    candidates: candidates.slice(0, maxCandidates), activityGroups: groups.slice(0, ANALYSIS_BOUNDS.groups),
    limitations: [
      "Exploratory pool, not a representative or independently labelled workspace study. Input receipt declarations do not establish workspace completeness.",
      "Write facts are edit intents, not confirmation of filesystem success. Nearby file activity is not proof of duplicate work, conflict or harm.",
      "Commit facts can be attempts with unknown keys. SHA-shaped keys are metadata, not independent proof of successful or merged changes.",
      "Session spans are time between first and last records, not continuous execution or measured agent concurrency.",
      "Activity groups are transitive connected components of retained candidate edges, not semantic workstreams or confirmed planned fleets.",
      "No alert quality, precision, recall, priority ranking or useful-warning lead time is measured here; use independent labels and the replay scorer.",
      ...(!all.claims ? ["No claim facts in this pool; intent/decision/constraint patterns cannot be assessed."] : []),
      ...(!all.outcomes ? ["No outcome facts in this pool; overlap consequences cannot be assessed."] : []),
      ...(reasons.length ? ["Discovery was censored by analysis budgets; retained candidates and groups are partial."] : []),
    ],
  };
}

function activityGroups(candidates: PatternCandidate[]): PatternReport["activityGroups"] {
  const neighbours = new Map<string, Set<string>>(), refs = new Map<string, SessionRef>();
  for (const c of candidates) {
    const [a, b] = c.participants.map(refKey) as [string, string];
    if (!neighbours.has(a)) neighbours.set(a, new Set()); if (!neighbours.has(b)) neighbours.set(b, new Set());
    neighbours.get(a)!.add(b); neighbours.get(b)!.add(a); refs.set(a, c.participants[0]); refs.set(b, c.participants[1]);
  }
  const seen = new Set<string>(), groups: PatternReport["activityGroups"] = [];
  for (const start of [...neighbours.keys()].sort(textCompare)) {
    if (seen.has(start)) continue;
    const todo = [start], members: string[] = []; let degree = 0;
    seen.add(start);
    while (todo.length) {
      const key = todo.pop()!; members.push(key); degree += neighbours.get(key)!.size;
      for (const next of neighbours.get(key)!) if (!seen.has(next)) { seen.add(next); todo.push(next); }
    }
    members.sort(textCompare);
    groups.push({ id: sha256Hex(canonicalJson(members)), sessions: members.length, candidateEdges: degree / 2,
      members: members.slice(0, ANALYSIS_BOUNDS.members).map((k) => refs.get(k)!), membersOmitted: Math.max(0, members.length - ANALYSIS_BOUNDS.members) });
  }
  return groups.sort((a, b) => b.sessions - a.sessions || textCompare(a.id, b.id));
}
