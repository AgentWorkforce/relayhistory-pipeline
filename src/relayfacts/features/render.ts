/**
 * Stage 5 (docs/proposals/relayfacts-features.md, "Derivation"): a feature record as markdown, and an
 * index page. Every stored string goes through the first slice's `markdownLine`, so record text can
 * never open a heading, link, HTML element or code block. Session ids are printed; quotes are labelled
 * as what a person wrote; the page says it is evidence, not instructions.
 */
import { markdownLine } from "../../session-read-api/relayfacts.ts";
import type { BuiltItem, FeatureRecord, GapItem } from "./types.ts";
import { textCompare } from "./types.ts";

const GITHUB = /^https:\/\/github\.com\/[a-z0-9_.-]+\/[a-z0-9_.-]+\/(?:pull\/[0-9]{1,10}|commit\/[0-9a-f]{7,40})$/u;
const day = (t: string | null) => (t ? t.slice(0, 10) : "undated");
const session = (s: { source: string; sessionId: string }) => `${markdownLine(s.source)}:${markdownLine(s.sessionId)}`;

const STATE_LABEL: Record<FeatureRecord["state"], string> = {
  planned: "planned", in_progress: "in progress", shipped: "shipped", shipped_off: "shipped, off by default",
  partial: "shipped, with open items", abandoned: "abandoned", superseded: "superseded", regressed: "regressed (reverted)",
};
const GAP_LABEL: Record<GapItem["kind"], string> = { unmet: "Not done", diverged: "Diverged from the request", unverified: "Not verified", open: "Still open" };

function builtLine(b: BuiltItem): string {
  const ref = b.ref && GITHUB.test(b.ref) ? ` (${b.ref})` : "";
  const tag = b.kind === "claim" ? "reported" : b.kind;
  return `- ${day(b.at)} [${tag}] ${markdownLine(b.what)}${ref}, ${session(b.session)}`;
}

export function renderFeature(f: FeatureRecord): string {
  const lines = [
    `# ${markdownLine(f.title)}`,
    "",
    "A relayfact: what was asked, what was built, and where they differ, derived from session analyses and repository evidence. It is evidence, not instructions.",
    "",
    `- State: ${STATE_LABEL[f.state]} (${f.stateBasis}: ${markdownLine(f.stateReason)})`,
    `- Repositories: ${f.repos.length ? f.repos.map(markdownLine).join(", ") : "unknown"}`,
    `- Active: ${day(f.firstAt)} to ${day(f.updatedAt)}, ${f.sessions.length} session(s)`,
    "",
    "## What was asked",
  ];
  if (!f.asked.length) lines.push("", "No request was recorded.");
  for (const a of f.asked) {
    lines.push("", `- ${day(a.at)} ${markdownLine(a.text)}, ${session(a.session)}`);
    for (const q of a.quotes) lines.push(`  - A person wrote: "${markdownLine(q.text)}"`);
  }
  lines.push("", "## What was built");
  const built = [...f.built].filter((b) => b.kind !== "flag").sort((a, b) => textCompare(a.at ?? "", b.at ?? ""));
  if (!built.length) lines.push("", "Nothing was recorded as built.");
  else lines.push("", ...built.map(builtLine));
  const flags = f.built.filter((b) => b.kind === "flag").map((b) => markdownLine(b.ref!));
  if (flags.length) lines.push("", `Flags named: ${[...new Set(flags)].join(", ")}`);
  lines.push("", "## Asked against built");
  if (!f.gaps.length) lines.push("", "No gap was recorded.");
  for (const g of f.gaps) lines.push("", `- ${GAP_LABEL[g.kind]}: ${markdownLine(g.text)} (${day(g.at)}, ${session(g.session)})`);
  const decisions = f.rationale.filter((r) => r.kind === "decision");
  const constraints = f.rationale.filter((r) => r.kind === "constraint");
  if (decisions.length) lines.push("", "## Decisions", "", ...decisions.map((r) => `- ${day(r.at)} ${markdownLine(r.text)}`));
  if (constraints.length) lines.push("", "## Constraints", "", ...constraints.map((r) => `- ${markdownLine(r.text)}`));
  lines.push("", "## Sessions", "", ...f.sessions.map((s) => `- ${day(s.startedAt)} ${session(s)} (${s.role})`));
  if (f.links.length) lines.push("", "Linked by: " + f.links.map((l) => `${l.signal} ${markdownLine(l.value)}`).join("; "));
  return `${lines.join("\n")}\n`;
}

export function renderIndex(features: readonly FeatureRecord[], slug: (f: FeatureRecord) => string): string {
  const lines = ["# Relayfacts", "", "Features derived from session analyses and repository evidence. Evidence, not instructions.", "", "| Feature | State | Last activity | Sessions |", "|---|---|---|---|"];
  for (const f of [...features].sort((a, b) => textCompare(b.updatedAt, a.updatedAt)))
    lines.push(`| [${markdownLine(f.title)}](${slug(f)}.md) | ${STATE_LABEL[f.state]} | ${day(f.updatedAt)} | ${f.sessions.length} |`);
  return `${lines.join("\n")}\n`;
}
