import { sessionKey, type Collection, type Dataset } from "./contracts.ts";
const cell = (s: string) => s.replace(/[\u0000-\u001f]/g, " ").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll("|", "\\|").replaceAll("`", "\\`").replaceAll("[", "\\[").replaceAll("]", "\\]");
export function renderAlerts(result: Collection, dataset: Dataset): string {
  const names = new Map(dataset.sessions.map(s => [sessionKey(s), `${s.title} (${s.userId}; ${s.id})`]));
  const lines = ["# Candidate coordination alerts", "", "Collection only. No messages or notifications were sent. Candidates require review and fresh access checks before delivery.", "",
    `Source: ${dataset.provenance.kind}. Mode: ${result.mode}. As of: ${result.asOf}.`,
    `Reader: ${cell(dataset.provenance.reader)}. Retain until: ${result.expiresAt}.`,
    `Coverage: ${result.coverage.assessedPairs}/${result.coverage.candidatePairs} candidate pairs assessed; ${result.coverage.omittedPairs} omitted; ${result.coverage.failedPairs} failed; ${result.coverage.limitedContextPairs} used limited model context. Source complete: ${result.coverage.sourceComplete}.`,
    `Evidence with unknown arrival time: ${result.coverage.unknownAvailability}. Retrospective results do not establish when a live detector could have alerted.`, "",
    "| ID | Opportunity | Sessions | Confidence | Why connect | Evidence |", "| --- | --- | --- | --- | --- | --- |"];
  for (const a of result.alerts) lines.push(`| ${a.id.slice(0, 12)} | ${a.kind} | ${a.sessionKeys.map(k => cell(names.get(k) ?? k)).join(" / ")} | ${a.confidence} | ${cell(a.reason)} | ${a.citations.map(c => `${cell(c.evidenceId)}: ${cell(c.quote)}`).join("; ")} |`);
  if (!result.alerts.length) lines.push("", "No candidate alerts in this run. This does not establish absence of useful coordination opportunities.");
  lines.push("", "## Assessments", "", "| Pair | Decision | Reason |", "| --- | --- | --- |");
  for (const a of result.assessments) lines.push(`| ${a.pairId.slice(0, 12)} | ${a.verdict.decision} | ${cell(a.verdict.reason)} |`);
  return lines.join("\n") + "\n";
}
