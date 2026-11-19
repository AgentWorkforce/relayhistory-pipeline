/**
 * Topics for the conflict detectors (docs/session-intel-conflicts.md, "Candidates"). A topic is a
 * decisionTopic-style slug: the first two directory segments of a repository path, lowercased and hyphenated
 * (src/session-analysis/decision-records.ts `decisionTopic`, reimplemented here so this module stays offline
 * and outside session-analysis). A path with no directory has no topic.
 */

/** The roots an absolute path is cut at, as decisionTopic does, so no home directory becomes a topic. */
const SOURCE_ROOTS = new Set(["src", "packages", "apps", "lib", "libs", "services", "crates", "cmd", "internal", "scripts", "docs", "supabase", "flows", "test", "tests", "e2e", "infra"]);
const TOPIC_MAX = 64;
const SLUG = /^[a-z0-9][a-z0-9-]{0,63}$/u;

function slugOf(dirs: string[]): string | null {
  const slug = dirs.slice(0, 2).join("-").toLowerCase()
    .replace(/[^a-z0-9-]+/gu, "-").replace(/-{2,}/gu, "-").replace(/^-+|-+$/gu, "");
  const cut = slug.slice(0, TOPIC_MAX).replace(/-+$/u, "");
  return SLUG.test(cut) ? cut : null;
}

function segments(path: string): string[] | null {
  const raw = path.replace(/\\/gu, "/");
  const absolute = /^(?:\/|~|[A-Za-z]:\/)/u.test(raw);
  let parts = raw.split("/").filter((p) => p !== "" && p !== ".");
  if (parts.some((p) => p === ".." || p === "~")) return null;
  if (absolute) {
    const root = parts.findIndex((p) => SOURCE_ROOTS.has(p.toLowerCase()));
    if (root < 0) return null;
    parts = parts.slice(root);
  }
  return parts;
}

/** A written file's topic: its first two directories. `README.md` has none. */
export function pathTopic(path: string): string | null {
  const parts = segments(path);
  if (!parts) return null;
  return slugOf(parts.slice(0, -1));
}

/**
 * The directory segments of a write's topic (`supabase-migrations` -> supabase, migrations), lowercased. Used
 * to match a claim's words against what the other side wrote.
 */
export function topicDirs(path: string): string[] {
  const parts = segments(path);
  return parts ? parts.slice(0, -1).slice(0, 2).map((p) => p.toLowerCase()) : [];
}

/** A path-like token in claim text: has a `/`, no spaces; globs allowed. */
const PATH_TOKEN = /(?:^|[\s("'`])((?:\*{1,2}\/|[A-Za-z0-9_.*-]+\/)[A-Za-z0-9_.*/-]*)/gu;
const FILE_LIKE = /\.[A-Za-z0-9]{1,8}$/u;

/** The path-like tokens of a claim's text, trailing punctuation removed. */
export function pathTokens(text: string): string[] {
  const out: string[] = [];
  for (const m of text.matchAll(PATH_TOKEN)) {
    const tok = m[1]!.replace(/[.,;:)]+$/u, "");
    if (tok.includes("/") && tok.length <= 512) out.push(tok);
  }
  return out;
}

/**
 * The topics a claim names by path: each path token without globs, treated as a directory unless its last
 * segment looks like a file (`src/api/routes.ts` -> src-api; `supabase/migrations` -> supabase-migrations).
 */
export function claimPathTopicDirs(text: string): Map<string, string[]> {
  const out = new Map<string, string[]>();
  for (const tok of pathTokens(text)) {
    if (tok.includes("*")) continue;
    const parts = segments(tok);
    if (!parts?.length) continue;
    const dirs = FILE_LIKE.test(parts.at(-1)!) ? parts.slice(0, -1) : parts;
    const slug = slugOf(dirs);
    if (slug && !out.has(slug)) out.set(slug, dirs.slice(0, 2).map((d) => d.toLowerCase()));
  }
  return out;
}
export const claimPathTopics = (text: string): string[] => [...claimPathTopicDirs(text).keys()];

/** Directory names too generic to tie a claim's word to a topic. */
const GENERIC_DIRS = new Set(["src", "lib", "libs", "packages", "apps", "app", "docs", "test", "tests", "scripts", "internal", "pkg", "cmd",
  "core", "common", "shared", "utils", "util", "e2e", "infra", "services", "crates", "flows", "fixtures", "__tests__"]);

/** The lowercase words of a text, including the segments of its path and glob tokens. */
export function words(text: string): string[] {
  return text.toLowerCase().split(/[^a-z0-9_-]+/u).filter(Boolean);
}

const sameWord = (a: string, b: string) => a === b || `${a}s` === b || a === `${b}s`;

/**
 * The topics among `known` (the other side's write topics, with their directory segments) that a claim names
 * by a word: a word of at least 4 characters equal to a non-generic directory segment, plural-tolerant
 * ("no new migrations" names supabase-migrations).
 */
export function claimKeywordTopics(text: string, known: ReadonlyMap<string, readonly string[]>): string[] {
  const ws = words(text).filter((w) => w.length >= 4 && !GENERIC_DIRS.has(w));
  const out: string[] = [];
  for (const [topic, dirs] of known)
    if (dirs.some((d) => !GENERIC_DIRS.has(d) && ws.some((w) => sameWord(w, d)))) out.push(topic);
  return out;
}

/** A claim's text, parsed once: its path topics (with directories) and its candidate keywords. */
export interface ParsedClaimText { pathDirs: Map<string, string[]>; keywords: string[] }
const PARSED_MAX = 20_000;
const parsed = new Map<string, ParsedClaimText>();
/**
 * Memoised per claim text (bounded; the oldest entry is dropped first). Pair matching asks for the same claim's
 * topics once per pair, so without this every live claim was re-parsed for every pair.
 */
export function parseClaimText(text: string): ParsedClaimText {
  let p = parsed.get(text);
  if (!p) {
    p = { pathDirs: claimPathTopicDirs(text), keywords: words(text).filter((w) => w.length >= 4 && !GENERIC_DIRS.has(w)) };
    if (parsed.size >= PARSED_MAX) parsed.delete(parsed.keys().next().value!);
    parsed.set(text, p);
  }
  return p;
}

/**
 * All of a claim's topics in the context of the other side: the paths it names, and the other side's known
 * topics (its write topics and its claims' path topics) that it names by a word.
 */
export function claimTopics(text: string, otherKnown: ReadonlyMap<string, readonly string[]>): string[] {
  const p = parseClaimText(text);
  const out = new Set(p.pathDirs.keys());
  for (const [topic, dirs] of otherKnown)
    if (dirs.some((d) => !GENERIC_DIRS.has(d) && p.keywords.some((w) => sameWord(w, d)))) out.add(topic);
  return [...out];
}

/**
 * The topics a claim brings to the pair (its path topics, with their directories) and that it names in the
 * other side's known topics. `topic` is a stored claim's own slug (the M1 extractor's decisionTopic); it
 * counts like a path topic. `general` (decisionTopic's "no file") is never a topic.
 */
export function claimKnownDirs(c: { text: string; topic?: string }): Map<string, string[]> {
  // A copy of the memoised parse (the caller may add to it).
  const m = new Map(parseClaimText(c.text).pathDirs);
  if (c.topic && c.topic !== "general" && !m.has(c.topic)) m.set(c.topic, []);
  return m;
}
export function topicsOfClaim(c: { text: string; topic?: string }, otherKnown: ReadonlyMap<string, readonly string[]>): string[] {
  const out = new Set(claimTopics(c.text, otherKnown));
  if (c.topic && c.topic !== "general") out.add(c.topic);
  return [...out];
}

/** The write topics of a list of writes, each with its directory segments. */
export function writeTopics(writes: readonly { path: string }[]): Map<string, string[]> {
  const out = new Map<string, string[]>();
  for (const w of writes) {
    const t = pathTopic(w.path);
    if (t && !out.has(t)) out.set(t, topicDirs(w.path));
  }
  return out;
}
