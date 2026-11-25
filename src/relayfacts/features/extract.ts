/**
 * Stage 1 (docs/proposals/relayfacts-features.md, "Derivation"): one session analysis snapshot becomes
 * the session's facts: what was asked (with the person's own words), what was built, the gaps, the
 * rationale, and the keys that link it to other sessions of the same feature.
 *
 * Pure and deterministic. Every stored string passes the scrubber unchanged or is dropped (counted in
 * `dropped`), as in the first-slice extractor. Quotes come only from `human-message` references, at
 * most QUOTE_MAX characters, cut at a word boundary from the start of the message, so every quote is
 * a verbatim substring of the whitespace-collapsed reference text (the benchmark's grounding check).
 * Commits, PRs, branches and migrations are read from the tool evidence itself, never from a claim.
 */
import { createHash } from "node:crypto";
import { scrubText } from "../../shared/vendor.ts";
import type { AnalysisClaim, SessionAnalysisSnapshot, SessionEvidenceReference } from "../../session-analysis/engine/session-analysis-result.ts";
import type { AskedItem, BuiltItem, EvidencePointer, GapItem, LinkKeys, Quote, RationaleItem, SessionContext, SessionFacts, SessionKey } from "./types.ts";
import { textCompare } from "./types.ts";

export const QUOTE_MAX = 300;
export const QUOTES_PER_ASKED = 3;
export const TEXT_MAX = 300;
export const TITLE_MAX = 120;

const DEFAULT_BRANCHES = new Set(["main", "master", "trunk", "develop", "head", "origin", "production", "staging"]);
const SOURCE_ROOTS = new Set(["src", "packages", "apps", "lib", "libs", "services", "crates", "cmd", "internal", "scripts", "docs", "supabase", "flows", "test", "tests", "e2e", "infra", "stacks", "deploy", "runbooks", "config", "docker", "benchmarks", "agent", "macos", "gtk", "desktop-core", "web"]);
const GITHUB_URL = /https:\/\/github\.com\/([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+)\/(pull\/[0-9]{1,10}|commit\/[0-9a-f]{7,40})(?![A-Za-z0-9_-])/gu;
const COMMIT_LINE = /^\[([A-Za-z0-9._/-]+)(?: \(root-commit\))? ([0-9a-f]{7,40})\] /gmu;
const BRANCH_COMMANDS = [
  /\bgit (?:checkout|switch) (?:-b|-B|-c|-C|--create) ([A-Za-z0-9._/-]+)/gu,
  /\bgit worktree add (?:-b|-B) ([A-Za-z0-9._/-]+)/gu,
  /\bgit push\b[^\n]*?\borigin (?:HEAD:)?([A-Za-z0-9._/-]+)/gu,
];
const FLAG = /\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*_ENABLED\b/gu;
const MIGRATION = /(?:^|\/)(?:migrations?|drizzle|db\/migrate)\/([^/]+\.(?:sql|ts|js))$/iu;
const USABLE = new Set(["high", "medium"]);

const collapse = (text: string) => text.replace(/\s+/gu, " ").trim();
export const stableId = (parts: unknown) => createHash("sha256").update(JSON.stringify(parts)).digest("hex").slice(0, 16);

/** True when the scrubber leaves the text exactly as it is. */
export function scrubClean(text: string): boolean {
  try {
    return scrubText(text) === text;
  } catch {
    return false;
  }
}

/** The text cut at a word boundary to at most `max` characters, never mid-word unless one word is longer. */
export function cutWords(text: string, max: number): string {
  if (text.length <= max) return text;
  const head = text.slice(0, max);
  const space = head.lastIndexOf(" ");
  return (space > max / 2 ? head.slice(0, space) : head).trimEnd();
}

/** A repository-relative path, anchored like the first-slice topic: never a home directory. Null when it cannot be anchored. */
export function repoRelative(path: string): string | null {
  const raw = path.replace(/\\/gu, "/");
  const absolute = /^(?:\/|~|[A-Za-z]:\/)/u.test(raw);
  let parts = raw.split("/").filter((p) => p !== "" && p !== ".");
  if (parts.some((p) => p === ".." || p === "~")) return null;
  if (absolute) {
    const root = parts.findIndex((p) => SOURCE_ROOTS.has(p.toLowerCase()));
    if (root < 0) return null;
    parts = parts.slice(root);
  }
  return parts.length ? parts.join("/") : null;
}

export function normalizeRepo(value: string | null | undefined): string | null {
  if (!value) return null;
  const repo = value.trim().replace(/^https?:\/\/github\.com\//iu, "").replace(/^git@github\.com:/iu, "").replace(/\.git$/iu, "").replace(/^\/+|\/+$/gu, "").toLowerCase();
  return /^[a-z0-9_.-]+\/[a-z0-9_.-]+$/u.test(repo) ? repo : null;
}

const featureBranch = (name: string) => {
  const b = name.trim().replace(/^refs\/heads\//u, "").replace(/^origin\//u, "");
  return b && !DEFAULT_BRANCHES.has(b.toLowerCase()) && !b.startsWith("-") ? b : null;
};

interface Ctx {
  key: SessionKey;
  refs: Map<string, SessionEvidenceReference>;
  dropped: Record<string, number>;
}
const drop = (ctx: Ctx, reason: string) => { ctx.dropped[reason] = (ctx.dropped[reason] ?? 0) + 1; };

function pointer(ref: SessionEvidenceReference): EvidencePointer {
  return { source: ref.source, sessionId: ref.sessionId, refId: ref.id, kind: ref.kind ?? null, at: ref.at ?? null };
}

function pointers(ctx: Ctx, ids: readonly string[]): EvidencePointer[] {
  const out: EvidencePointer[] = [];
  const seen = new Set<string>();
  for (const id of ids) {
    const ref = ctx.refs.get(id);
    if (ref && !seen.has(id)) { seen.add(id); out.push(pointer(ref)); }
  }
  return out;
}

const earliest = (ps: readonly EvidencePointer[]) => ps.map((p) => p.at).filter((t): t is string => !!t && Number.isFinite(Date.parse(t))).sort(textCompare)[0] ?? null;

/** A usable claim: high or medium confidence, at least one resolvable citation, scrub-clean text within bounds. */
function usable(ctx: Ctx, claim: AnalysisClaim | undefined, what: string): { text: string; evidence: EvidencePointer[] } | null {
  if (!claim || typeof claim.text !== "string") return null;
  if (!USABLE.has(claim.confidence)) { drop(ctx, `${what}_low_confidence`); return null; }
  const evidence = pointers(ctx, claim.evidenceIds ?? []);
  if (!evidence.length) { drop(ctx, `${what}_uncited`); return null; }
  const text = cutWords(collapse(claim.text), TEXT_MAX);
  if (!text) return null;
  if (!scrubClean(text)) { drop(ctx, `${what}_scrub`); return null; }
  return { text, evidence };
}

function quotes(ctx: Ctx, evidence: readonly EvidencePointer[]): Quote[] {
  const out: Quote[] = [];
  for (const p of evidence) {
    if (p.kind !== "human-message" || out.length >= QUOTES_PER_ASKED) continue;
    const ref = ctx.refs.get(p.refId);
    const text = cutWords(collapse(ref?.summary ?? ""), QUOTE_MAX);
    if (!text) continue;
    if (!scrubClean(text)) { drop(ctx, "quote_scrub"); continue; }
    out.push({ text, at: p.at, evidence: p });
  }
  return out;
}

/** Commits, PRs, branches and migrations read from the tool evidence (references' summary and detail). */
function evidenceArtifacts(ctx: Ctx, snapshot: SessionAnalysisSnapshot) {
  const commits = new Map<string, { at: string | null; branch: string | null; p: EvidencePointer }>();
  const prs = new Map<string, { at: string | null; p: EvidencePointer }>();
  const branches = new Set<string>();
  for (const ref of snapshot.evidence?.references ?? []) {
    if (ref.kind !== "tool-result" && ref.kind !== "tool-call") continue;
    const body = `${ref.summary ?? ""}\n${ref.detail ?? ""}`;
    for (const m of body.matchAll(GITHUB_URL)) {
      const [, owner, repo, tail] = m;
      if (tail!.startsWith("pull/")) {
        const url = `https://github.com/${owner}/${repo}/${tail}`.toLowerCase();
        if (!prs.has(url)) prs.set(url, { at: ref.at ?? null, p: pointer(ref) });
      } else {
        const sha = tail!.slice("commit/".length).toLowerCase();
        if (!commits.has(sha)) commits.set(sha, { at: ref.at ?? null, branch: null, p: pointer(ref) });
      }
    }
    if (ref.kind === "tool-result") for (const m of body.matchAll(COMMIT_LINE)) {
      const branch = featureBranch(m[1]!);
      const sha = m[2]!.toLowerCase();
      if (branch) branches.add(branch);
      if (!commits.has(sha)) commits.set(sha, { at: ref.at ?? null, branch, p: pointer(ref) });
    }
    if (ref.kind === "tool-call") for (const re of BRANCH_COMMANDS) for (const m of body.matchAll(re)) {
      const branch = featureBranch(m[1]!);
      if (branch) branches.add(branch);
    }
  }
  return { commits, prs, branches };
}

/** Stage 1: the facts of one session analysis. */
export function extractSessionFacts(snapshot: SessionAnalysisSnapshot, context: SessionContext = {}): SessionFacts {
  const key: SessionKey = { source: snapshot.source, sessionId: snapshot.sessionId };
  const refs = new Map<string, SessionEvidenceReference>();
  for (const ref of snapshot.evidence?.references ?? []) if (!refs.has(ref.id)) refs.set(ref.id, ref);
  const ctx: Ctx = { key, refs, dropped: {} };
  const u = snapshot.understanding;
  const asked: AskedItem[] = [];
  const built: BuiltItem[] = [];
  const gaps: GapItem[] = [];
  const rationale: RationaleItem[] = [];
  const statuses: string[] = [];

  const outcomes = u?.outcomes ?? [];
  outcomes.forEach((outcome, oi) => {
    statuses.push(outcome.status);
    const intent = usable(ctx, outcome.intent, "asked");
    const askedId = intent ? stableId([key, "asked", oi, intent.text]) : null;
    if (intent && askedId) asked.push({ id: askedId, text: intent.text, quotes: quotes(ctx, intent.evidence), at: earliest(intent.evidence), session: key, outcomeId: outcome.id ?? null, evidence: intent.evidence });
    (outcome.changes ?? []).forEach((change, ci) => {
      const c = usable(ctx, change, "built");
      if (c) built.push({ id: stableId([key, "built", oi, ci, c.text]), kind: "claim", what: c.text, ref: null, at: earliest(c.evidence), session: key, outcomeId: outcome.id ?? null, evidence: c.evidence });
    });
    const gap = (kind: GapItem["kind"], claim: AnalysisClaim | undefined, idx: number) => {
      const g = usable(ctx, claim, "gap");
      if (g) gaps.push({ id: stableId([key, "gap", kind, oi, idx, g.text]), kind, text: g.text, askedId, at: earliest(g.evidence), session: key, evidence: g.evidence });
    };
    (outcome.contradictions ?? []).forEach((c, i) => gap("diverged", c, i));
    (outcome.remaining ?? []).forEach((c, i) => gap("open", c, i));
    if ((outcome.status === "not-started" || outcome.status === "blocked") && intent && askedId)
      gaps.push({ id: stableId([key, "gap", "unmet", oi]), kind: "unmet", text: intent.text, askedId, at: earliest(intent.evidence), session: key, evidence: intent.evidence });
    const v = outcome.verification;
    const claimedDone = outcome.status === "implemented" || outcome.status === "reported-complete";
    if (v && (v.value === "failed" || (v.value === "not-observed" && claimedDone))) {
      const evidence = pointers(ctx, v.evidenceIds ?? []);
      const text = cutWords(collapse(v.explanation ?? ""), TEXT_MAX);
      if (text && scrubClean(text) && (evidence.length || intent))
        gaps.push({ id: stableId([key, "gap", "unverified", oi]), kind: "unverified", text, askedId, at: earliest(evidence.length ? evidence : intent!.evidence), session: key, evidence: evidence.length ? evidence : intent!.evidence });
    }
  });
  // A session analysed before outcomes existed, or with none: its top-level intents are what was asked.
  if (!outcomes.length) (u?.intents ?? []).forEach((claim, i) => {
    const c = usable(ctx, claim, "asked");
    if (c) asked.push({ id: stableId([key, "asked-intent", i, c.text]), text: c.text, quotes: quotes(ctx, c.evidence), at: earliest(c.evidence), session: key, outcomeId: null, evidence: c.evidence });
  });
  (u?.decisions ?? []).forEach((claim, i) => {
    const c = usable(ctx, claim, "decision");
    if (c) rationale.push({ id: stableId([key, "decision", i, c.text]), kind: "decision", text: c.text, at: earliest(c.evidence), session: key, evidence: c.evidence });
  });
  (u?.constraints ?? []).forEach((claim, i) => {
    const c = usable(ctx, claim, "constraint");
    if (c) rationale.push({ id: stableId([key, "constraint", i, c.text]), kind: "constraint", text: c.text, at: earliest(c.evidence), session: key, evidence: c.evidence });
  });

  const art = evidenceArtifacts(ctx, snapshot);
  for (const [sha, c] of [...art.commits].sort((a, b) => textCompare(a[0], b[0])))
    built.push({ id: stableId([key, "commit", sha]), kind: "commit", what: c.branch ? `commit ${sha.slice(0, 12)} on ${c.branch}` : `commit ${sha.slice(0, 12)}`, ref: sha, at: c.at, session: key, outcomeId: null, evidence: [c.p] });
  for (const [url, pr] of [...art.prs].sort((a, b) => textCompare(a[0], b[0])))
    built.push({ id: stableId([key, "pr", url]), kind: "pr", what: `pull request ${url.replace("https://github.com/", "")}`, ref: url, at: pr.at, session: key, outcomeId: null, evidence: [pr.p] });
  const files = new Set<string>();
  for (const f of snapshot.metrics?.filesChanged ?? []) {
    const rel = repoRelative(f.path);
    if (rel) files.add(rel); else drop(ctx, "file_unanchored");
  }
  for (const path of [...files].sort(textCompare)) {
    const m = MIGRATION.exec(path);
    if (m) built.push({ id: stableId([key, "migration", path]), kind: "migration", what: `migration ${m[1]}`, ref: path, at: null, session: key, outcomeId: null, evidence: [] });
  }
  const flags = new Set<string>();
  for (const b of built) if (b.kind === "claim") for (const m of b.what.matchAll(FLAG)) flags.add(m[0]);
  for (const flag of [...flags].sort(textCompare)) {
    const source = built.find((b) => b.kind === "claim" && b.what.includes(flag))!;
    built.push({ id: stableId([key, "flag", flag]), kind: "flag", what: `flag ${flag}`, ref: flag, at: source.at, session: key, outcomeId: source.outcomeId, evidence: source.evidence });
  }

  const times = [...refs.values()].map((r) => r.at).filter((t): t is string => !!t && Number.isFinite(Date.parse(t))).sort(textCompare);
  const branch = context.branch ? featureBranch(context.branch) : null;
  const keys: LinkKeys = {
    repo: normalizeRepo(context.repo),
    branches: [...new Set([...(branch ? [branch] : []), ...art.branches])].sort(textCompare),
    prs: [...art.prs.keys()].sort(textCompare),
    commits: [...art.commits.keys()].sort(textCompare),
    files: [...files].sort(textCompare),
    taskRefs: context.taskRef ? [context.taskRef] : [],
  };
  const titleText = cutWords(collapse(snapshot.title ?? ""), TITLE_MAX);
  return {
    source: key.source,
    sessionId: key.sessionId,
    analyzedAt: snapshot.analyzedAt,
    startedAt: times[0] ?? snapshot.analyzedAt,
    endedAt: times[times.length - 1] ?? snapshot.analyzedAt,
    title: titleText && scrubClean(titleText) ? titleText : asked[0]?.text.slice(0, TITLE_MAX) ?? "Untitled work",
    asked,
    built,
    gaps,
    rationale,
    outcomeStatuses: statuses,
    keys,
    dropped: ctx.dropped,
  };
}
