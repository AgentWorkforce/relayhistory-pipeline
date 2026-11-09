import { type Candidate, type Scope, type Session, pairId, sessionKey } from "./contracts.ts";
import { fileKey, repositoryKey, topicTerms } from "./evidence.ts";
const intersection = (a: Set<string>, b: Set<string>) => [...a].filter(x => b.has(x)).sort();
function features(s: Session) {
  const files = new Set<string>(), writes = new Set<string>(), tasks = new Set<string>();
  for (const e of s.evidence) {
    for (const f of e.files) {
      const path = fileKey(f.path);
      if (path) { files.add(path); if (f.operation === "write") writes.add(path); }
    }
    for (const task of e.taskRefs) tasks.add(task);
  }
  // Only windowed evidence feeds retrieval, never a current title/summary in a historical replay.
  return { files, writes, tasks, terms: topicTerms(s.evidence.map(e => e.text).join(" ")), repo: repositoryKey(s.repository) };
}
export function findCandidates(scope: Scope, sessions: Session[], recentAfter = -Infinity): Candidate[] {
  const found: Candidate[] = [];
  const sorted = [...sessions].sort((a, b) => sessionKey(a) < sessionKey(b) ? -1 : sessionKey(a) > sessionKey(b) ? 1 : 0);
  const fs = sorted.map(features);
  for (let i = 0; i < sorted.length; i++) for (let j = i + 1; j < sorted.length; j++) {
    const a = sorted[i]!, b = sorted[j]!;
    if (a.userId === b.userId || a.relatedSessionKeys.includes(sessionKey(b)) || b.relatedSessionKeys.includes(sessionKey(a))) continue;
    if (Math.max(...a.evidence.map(e => Date.parse(e.at)), ...b.evidence.map(e => Date.parse(e.at))) < recentAfter) continue;
    const fa = fs[i]!, fb = fs[j]!;
    const sameRepository = fa.repo !== null && fa.repo === fb.repo;
    const sharedWrites = sameRepository ? intersection(fa.writes, fb.writes) : [];
    const sharedFiles = sameRepository ? intersection(fa.files, fb.files) : [];
    const sharedTasks = intersection(fa.tasks, fb.tasks);
    const terms = intersection(fa.terms, fb.terms);
    if (!sharedFiles.length && !sharedTasks.length && terms.length < 3) continue;
    found.push({ id: pairId(scope, sessionKey(a), sessionKey(b)), left: a, right: b,
      signals: { sharedWrites, sharedFiles, sharedTasks, topicTerms: terms.slice(0, 20), sameRepository },
      rank: sharedWrites.length * 10 + sharedTasks.length * 8 + sharedFiles.length * 2 + Math.min(terms.length, 8) });
  }
  return found.sort((a, b) => b.rank - a.rank || a.id.localeCompare(b.id));
}
