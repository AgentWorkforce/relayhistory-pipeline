/** Private, content-free review artefacts; no labels or score are manufactured. */
import type { PatternReport } from "./types.ts";

const cell = (s: string) => s.replace(/&/gu, "&amp;").replace(/</gu, "&lt;").replace(/[\r\n\t]/gu, " ").replace(/\|/gu, "\\|").replace(/`/gu, "\\`");

export function reviewQueue(report: PatternReport) {
  return { format: "session-intel-pattern-review/1", definitionHash: report.definition.hash,
    inputManifests: report.input.inputs.map((i) => i.manifestSha256), deleteBy: report.input.deleteBy,
    warning: "Exploratory candidates selected from observed file activity, not independent labels or an unbiased quality-evaluation sample.",
    questions: ["Do the sessions address the same task?", "Was the coordination or parallel work planned?", "Is there independent evidence of harm or wasted work?", "At what time would a warning have helped?", "What alternative explanation fits the evidence?"],
    cases: report.candidates.map((candidate) => ({ candidate, verdict: null, reviewer: null, outcomeEvidence: [], notes: null })),
  };
}

export function reportMarkdown(r: PatternReport): string {
  const a = r.profile.all;
  const lines = ["# Workspace session patterns", "", "Exploratory observations only. Alert quality has not been evaluated.", "",
    `Window: ${r.input.window.since} to ${r.input.window.until}. Delete by ${r.input.deleteBy}.`,
    `${r.input.sessions} unique sessions; ${r.input.duplicateSessions} identical duplicates removed; ${r.input.inputs.length} input snapshots.`,
    `Discovery scan: ${r.coverage.scanComplete ? "within its declared budgets" : "CENSORED"}; ${r.coverage.comparisons} comparisons. Workspace completeness is not established.`, "",
    ...(r.coverage.actionScope ? [
      `Scope mode: ${r.coverage.actionScope.mode}. ${r.coverage.actionScope.records} in-window action records; ${r.coverage.actionScope.missing} missing, ${r.coverage.actionScope.unattributed} unattributed, ${r.coverage.actionScope.conflicting} conflicting; ${r.coverage.actionScope.late} available after fact time.`,
      `${r.coverage.actionScope.writesWithoutTarget} writes lack a comparable repository target; ${r.coverage.actionScope.claimsWithoutActionScope} claims have no per-action scope. Late declarations are retrospective evidence, not early warnings.`, "",
    ] : []),
    "## Evidence coverage", "", "| Harness | Sessions | With edits | With commits | With outcomes | With claims | Edit facts |", "|---|---:|---:|---:|---:|---:|---:|",
    ...r.profile.byHarness.map((p) => `| ${cell(p.key)} | ${p.sessions} | ${p.sessionsWithWrites} | ${p.sessionsWithCommits} | ${p.sessionsWithOutcomes} | ${p.sessionsWithClaims} | ${p.writes} |`), "",
    `Total: ${a.writes} distinct edit-intent facts, ${a.commits} commit facts (${a.shaShapedCommits} with SHA-shaped keys), ${a.outcomes} outcome facts and ${a.claims} claims.`,
    `Peak overlapping session spans: ${r.profile.peakSessionSpans}. Spans are not measured simultaneous execution.`, "",
    "## Candidate activity", "", `${r.coverage.candidatePairsRetained} candidate pairs retained; ${r.candidates.length} selected for review; ${r.coverage.candidatesOmittedFromReport} omitted from this report.`,
    `${r.activityGroups.length} of ${r.coverage.groupsTotal} activity groups shown; ${r.coverage.groupsOmitted} omitted (transitive graph components, not semantic workstreams).`,
    `${r.profile.files.length} of ${r.coverage.filesTotal} files shown; ${r.coverage.filesOmitted} omitted. ${r.profile.byRepository.length} of ${r.coverage.repositoriesTotal} repository/project rows shown; ${r.coverage.repositoriesOmitted} omitted.`,
    `${a.sessions} sessions contribute to the in-window profile; ${r.coverage.sessionsOutsideWindow} wholly outside-window sessions excluded.`, "",
    "| Candidate | Scope | Shared files | Event pairs | Closest gap (s) | Parent link | Branch relation |", "|---|---|---:|---:|---:|---|---|",
    ...r.candidates.map((c) => `| ${c.id.slice(0, 12)} | ${cell(c.scope.kind)}:${cell(c.scope.value)} | ${c.sharedFiles} | ${c.eventPairs} | ${c.closestGapMs / 1_000} | ${c.parentLink} | ${c.branchRelation} |`), "",
    "The JSON report and review queue carry bounded event references for every selected candidate. No session titles, summaries or claim text are copied.", "",
    "## Limits and next evidence", "", ...r.limitations.map((l) => `- ${l}`), "",
    "Review candidates against their source evidence. Build an independently sampled and labelled evaluation frame before using the existing replay scorer. Do not tune or score a held-out frame from this exploratory queue.", "",
  ];
  return lines.join("\n");
}
