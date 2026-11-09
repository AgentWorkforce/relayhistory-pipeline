import { type Dataset, type Session, timestamp } from "./contracts.ts";

/** Canonical remote identity; local directories and repository basenames never match. */
export function repositoryKey(raw: string | null): string | null {
  if (!raw) return null;
  const remote = raw.replace(/^git@([^:]+):/, "https://$1/");
  try {
    const u = new URL(remote);
    if (!["https:", "http:", "ssh:"].includes(u.protocol) || u.password || (u.protocol !== "ssh:" && u.username) || u.search || u.hash) return null;
    const path = u.pathname.replace(/\/$/, "").replace(/\.git$/, "");
    return path.split("/").filter(Boolean).length >= 2 ? `${u.host.toLowerCase()}${path}` : null;
  } catch { return null; }
}
export function fileKey(path: string): string | null {
  const p = path.replaceAll("\\", "/").replace(/^\.\//, "");
  if (!p || p.startsWith("/") || /^[A-Za-z]:/.test(p) || p.split("/").some(v => v === ".." || !v)) return null;
  if (/(^|\/)(node_modules|vendor|dist|build)\//.test(p) || /(^|\/)(package-lock\.json|bun\.lockb?|yarn\.lock|Cargo\.lock)$/.test(p)) return null;
  return p;
}
const STOP = new Set("the and for with this that from into have will should could working work agent agents session sessions implement implementation update fix task using same user users file files code project please need make add test tests build want their about then when also does doing something change changes been being would".split(" "));
export function topicTerms(text: string): Set<string> {
  return new Set((text.toLowerCase().match(/[a-z][a-z0-9_-]{3,}/g) ?? []).filter(v => !STOP.has(v)));
}
export function evidenceAt(dataset: Dataset, asOf: string, mode: "retrospective" | "replay", lookbackHours: number) {
  if (!timestamp(asOf) || !Number.isFinite(lookbackHours) || lookbackHours < 1 || lookbackHours > 168) throw new Error("invalid_window");
  const cutoff = Date.parse(asOf), since = cutoff - lookbackHours * 3_600_000;
  let unknownAvailability = 0;
  const sessions: Session[] = [];
  for (const session of dataset.sessions) {
    if (session.visibility !== "shared") continue;
    const evidence = session.evidence.filter(e => {
      const at = Date.parse(e.at);
      if (at < since || at > cutoff) return false;
      if (e.availableAt === null) unknownAvailability++;
      return mode === "retrospective" || (e.availableAt !== null && Date.parse(e.availableAt) <= cutoff);
    });
    if (evidence.length) sessions.push({ ...session, evidence,
      relatedSessionKeys: mode === "replay" && (!session.relationshipsAvailableAt || Date.parse(session.relationshipsAvailableAt) > cutoff) ? [] : session.relatedSessionKeys });
  }
  return { sessions, unknownAvailability };
}
