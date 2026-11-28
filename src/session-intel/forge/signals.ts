/**
 * Forge outcome signals (docs/session-intel-forge-outcomes.md). Each one is pure and deterministic and emits
 * candidates for adjudication, never labels. They read outcomes only: no session data and no detector output.
 */
import { sha256Hex, textCompare } from "../replay/util.ts";
import type { Candidate, CandidateSide, Commit, PullRequest } from "./types.ts";

const candidateId = (repo: string, signal: string, refs: string[]) =>
  sha256Hex(JSON.stringify([repo, signal, [...refs].sort(textCompare)])).slice(0, 16);
const crossIdentity = (sides: CandidateSide[]) => new Set(sides.map((s) => s.identity)).size > 1;
const byTime = (a: { at: string }, b: { at: string }) => textCompare(a.at, b.at);
const commitSide = (c: Commit): CandidateSide => ({ identity: c.author, ref: `commit:${c.sha}`, at: c.authoredAt, title: c.subject });
const prSide = (p: PullRequest, at = p.createdAt): CandidateSide => ({ identity: p.author, ref: `pr:${p.number}`, at, title: p.title });

/** `<dir>/<number>_<name>.sql` under a migrations or drizzle directory. */
const MIGRATION = /^(.*?(?:^|\/)(?:migrations?|drizzle|db\/migrate))\/((\d{3,})[_-][^/]+\.sql)$/i;

/**
 * Two different migration files claiming one number in one directory. Often a landing race between unrelated
 * work rather than duplicated work; the candidate records both names so adjudication can tell them apart.
 */
export function migrationNumberCollisions(repo: string, commits: Commit[]): Candidate[] {
  const firstByName = new Map<string, { dir: string; number: string; name: string; commit: Commit }>();
  for (const c of [...commits].sort((a, b) => textCompare(a.authoredAt, b.authoredAt) || textCompare(a.sha, b.sha))) {
    if (c.parents.length > 1) continue;
    for (const f of c.files) {
      const m = f.status === "A" ? MIGRATION.exec(f.path) : null;
      if (!m) continue;
      const key = `${m[1]}\u0000${m[2]}`;
      if (!firstByName.has(key)) firstByName.set(key, { dir: m[1]!, number: m[3]!, name: m[2]!, commit: c });
    }
  }
  const groups = new Map<string, Array<{ name: string; commit: Commit }>>();
  for (const v of firstByName.values()) {
    const key = `${v.dir}#${v.number}`;
    groups.set(key, [...(groups.get(key) ?? []), { name: v.name, commit: v.commit }]);
  }
  const out: Candidate[] = [];
  for (const [key, rows] of groups) {
    if (rows.length < 2 || new Set(rows.map((r) => r.commit.sha)).size < 2) continue;
    const sides = rows.map((r) => ({ ...commitSide(r.commit), title: `${r.name} | ${r.commit.subject}` })).sort(byTime);
    out.push({
      id: candidateId(repo, "migration_number_collision", [key, ...rows.map((r) => r.name)]), repo,
      signal: "migration_number_collision", sides, crossIdentity: crossIdentity(sides),
      harmAt: sides[1]!.at, artifacts: rows.map((r) => `${key.split("#")[0]}/${r.name}`).sort(textCompare), detail: key,
    });
  }
  return out.sort(byHarm);
}

const REVERTS = /This reverts commit ([0-9a-f]{7,40})/g;
/** Merge-train or bisect reverts are process, not overlap; they are tagged so adjudication can drop them fast. */
const PROCESS_REVERT = /severe review finding|merge train bisect|from trunk-train\//i;

/** A revert of another identity's commit; a commit reverted by two different reverts is tagged `repeated`. */
export function crossIdentityReverts(repo: string, commits: Commit[]): Candidate[] {
  const bySha = new Map<string, Commit>();
  for (const c of commits) bySha.set(c.sha, c);
  const resolve = (sha: string) => bySha.get(sha) ?? [...bySha.values()].find((c) => c.sha.startsWith(sha));
  const revertsOf = new Map<string, Commit[]>();
  for (const c of commits) {
    for (const m of new Set([...c.body.matchAll(REVERTS)].map((x) => x[1]!))) {
      const orig = resolve(m);
      if (!orig || orig.sha === c.sha) continue;
      revertsOf.set(orig.sha, [...(revertsOf.get(orig.sha) ?? []), c]);
    }
  }
  const out: Candidate[] = [];
  for (const [sha, reverts] of revertsOf) {
    const orig = bySha.get(sha)!;
    const reverters = new Set(reverts.map((r) => r.author));
    const repeated = reverts.length > 1 && reverters.size > 1;
    for (const r of reverts) {
      const sides = [commitSide(orig), commitSide(r)].sort(byTime);
      if (!crossIdentity(sides) && !repeated) continue;
      const tags = [PROCESS_REVERT.test(`${r.subject}\n${r.body}`) ? "process" : "manual", repeated ? "repeated" : "single"];
      out.push({
        id: candidateId(repo, "cross_identity_revert", [orig.sha, r.sha]), repo, signal: "cross_identity_revert",
        sides, crossIdentity: crossIdentity(sides), harmAt: r.authoredAt,
        artifacts: [...new Set(orig.files.map((f) => f.path))].sort(textCompare).slice(0, 50), detail: tags.join(","),
      });
    }
  }
  return out.sort(byHarm);
}

const SUPERSEDE = /supersed\w*|duplicat\w* of|replaced by|in favou?r of|already (?:landed|on trunk|in trunk|merged|contains)|closing as (?:a )?duplicate/i;
const PR_REF = /(?:^|[^\w/])#(\d{1,6})\b/g;

/** A PR closed unmerged with supersede language, linked to the merged PRs it names. */
export function supersededPrs(repo: string, prs: PullRequest[], opts: { skipHeadRef?: RegExp } = {}): Candidate[] {
  const byNumber = new Map(prs.map((p) => [p.number, p]));
  const out: Candidate[] = [];
  for (const p of prs) {
    if (p.state !== "CLOSED" || !p.closedAt || (opts.skipHeadRef && opts.skipHeadRef.test(p.headRef))) continue;
    const texts = [p.body, ...p.comments.map((c) => c.body)];
    const phrase = texts.map((t) => SUPERSEDE.exec(t)?.[0]).find(Boolean);
    if (!phrase) continue;
    const successors = [...new Set(texts.flatMap((t) => [...t.matchAll(PR_REF)].map((m) => Number(m[1]))))]
      .map((n) => byNumber.get(n)).filter((q): q is PullRequest => !!q && q.number !== p.number && q.state === "MERGED");
    const sides = [prSide(p), ...successors.map((q) => prSide(q, q.mergedAt ?? q.createdAt))].sort(byTime);
    out.push({
      id: candidateId(repo, "superseded_pr", [`pr:${p.number}`]), repo, signal: "superseded_pr", sides,
      crossIdentity: crossIdentity(sides), harmAt: p.closedAt, artifacts: (p.files ?? []).slice(0, 50), detail: phrase.toLowerCase(),
    });
  }
  return out.sort(byHarm);
}

/**
 * Comments in which an agent or person reports that two lines of work collided: a contradicting test, two copies
 * of one function, a disagreement found while rebasing. These are the strongest written evidence of overlap.
 */
/**
 * Phrases tuned on the 2026-07..11 AgentWorkforce PR threads (docs/session-intel-forge-outcomes.md, "Phrase
 * list"). Generic review language ("contradicts the docstring", "could duplicate work") is deliberately absent:
 * on that corpus it was 80% merge-bot noise.
 */
export const COLLISION = new RegExp([
  "behaviou?r collision", "genuine disagreement", "asserts the opposite", "both definitions",
  "independently (?:added|implemented|wrote|built|fixed)", "duplicate work on my part", "duplicated (?:the |that |this )?work",
  "same fix,? (?:arrived|landed) first", "(?:arrived|landed) first", "beat (?:me|us) to it",
  "same (?:change|fix|work) (?:as|in) #", "already (?:fixed|implemented|landed) (?:on|in|by|via) (?:main|trunk|#|`?[0-9a-f]{7})",
  "another (?:agent|session|lane|worker) (?:is|was|had|has) (?:already )?(?:working|doing|fixing|implementing|landed)",
  "conflicts? with #\\d+ (?:which|that) (?:also|already)", "undoes #\\d", "reverts? (?:the|their|its) (?:fix|change) from #\\d",
].join("|"), "i");
const SHA_REF = /\b([0-9a-f]{7,40})\b/g;

export function collisionNotes(repo: string, prs: PullRequest[], commits: Commit[]): Candidate[] {
  const byNumber = new Map(prs.map((p) => [p.number, p]));
  const commitByPrefix = (s: string) => commits.find((c) => c.sha.startsWith(s));
  const out: Candidate[] = [];
  for (const p of prs) {
    for (const note of [{ author: p.author, at: p.createdAt, body: p.body }, ...p.comments]) {
      const m = COLLISION.exec(note.body);
      if (!m) continue;
      const refs: CandidateSide[] = [];
      for (const r of note.body.matchAll(PR_REF)) {
        const q = byNumber.get(Number(r[1]));
        if (q && q.number !== p.number) refs.push(prSide(q));
      }
      for (const r of note.body.matchAll(SHA_REF)) {
        const c = commitByPrefix(r[1]!);
        if (c) refs.push(commitSide(c));
      }
      const unique = new Map([prSide(p), ...refs].map((s) => [s.ref, s]));
      const sides = [...unique.values()].sort(byTime);
      out.push({
        id: candidateId(repo, "collision_note", [`pr:${p.number}`, note.at]), repo, signal: "collision_note", sides,
        crossIdentity: crossIdentity(sides), harmAt: note.at, artifacts: (p.files ?? []).slice(0, 50), detail: m[0].toLowerCase(),
      });
    }
  }
  return out.sort(byHarm);
}

function byHarm(a: Candidate, b: Candidate) { return textCompare(a.harmAt, b.harmAt) || textCompare(a.id, b.id); }
