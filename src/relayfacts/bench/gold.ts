/**
 * Gold labels for one corpus (docs/proposals/relayfacts-features.md, "Gold labels"). Written by a
 * labeller that never sees generator output, frozen before the first scored run, and bound to the
 * corpus by the sha256 of its manifest.
 *
 *   {"format": "relayfacts-gold/1", "corpus": "<manifest sha256>", "asOf": "<iso>",
 *    "coverage": "complete" | "partial",
 *    "features": [{"id", "title", "split": "dev" | "heldout", "state",
 *                  "sessions": [{"source", "sessionId"}],
 *                  "asked": [{"source", "sessionId", "text"}],
 *                  "gaps": [{"kind", "source", "sessionId", "text"}]}]}
 *
 * `coverage: "complete"` says every qualifying corpus session is in some gold feature, so every
 * generated record counts for precision; "partial" scores precision only over records that touch a
 * labelled session.
 */
import { FEATURE_STATES, type FeatureState, type GapKind, type SessionKey } from "../features/types.ts";
import { CorpusError } from "./corpus.ts";

export const GOLD_FORMAT = "relayfacts-gold/1";
const GAP_KINDS: readonly GapKind[] = ["unmet", "diverged", "unverified", "open"];

export interface GoldFeature {
  id: string;
  title: string;
  split: "dev" | "heldout";
  state: FeatureState;
  sessions: SessionKey[];
  asked: Array<SessionKey & { text: string }>;
  gaps: Array<SessionKey & { kind: GapKind; text: string }>;
}

export interface Gold {
  format: typeof GOLD_FORMAT;
  corpus: string;
  asOf: string;
  coverage: "complete" | "partial";
  features: GoldFeature[];
}

const key = (v: Record<string, unknown>, where: string): SessionKey => {
  if (typeof v.source !== "string" || typeof v.sessionId !== "string") throw new CorpusError(`${where}: source and sessionId required`);
  return { source: v.source, sessionId: v.sessionId };
};

export function parseGold(raw: unknown, corpusSha256: string): Gold {
  const g = raw as Record<string, unknown>;
  if (!g || g.format !== GOLD_FORMAT) throw new CorpusError(`gold: format must be ${GOLD_FORMAT}`);
  if (g.corpus !== corpusSha256) throw new CorpusError("gold: labels name a different corpus");
  if (typeof g.asOf !== "string" || !Number.isFinite(Date.parse(g.asOf))) throw new CorpusError("gold: bad asOf");
  if (g.coverage !== "complete" && g.coverage !== "partial") throw new CorpusError("gold: coverage must be complete or partial");
  if (!Array.isArray(g.features)) throw new CorpusError("gold: features must be an array");
  const ids = new Set<string>();
  const owner = new Map<string, string>();
  const features = (g.features as Record<string, unknown>[]).map((f, i) => {
    const where = `gold feature ${i}`;
    if (typeof f.id !== "string" || ids.has(f.id)) throw new CorpusError(`${where}: id missing or repeated`);
    ids.add(f.id);
    if (f.split !== "dev" && f.split !== "heldout") throw new CorpusError(`${where}: split must be dev or heldout`);
    if (!FEATURE_STATES.includes(f.state as FeatureState)) throw new CorpusError(`${where}: unknown state`);
    const sessions = (f.sessions as Record<string, unknown>[] ?? []).map((s) => key(s, where));
    if (!sessions.length) throw new CorpusError(`${where}: no sessions`);
    for (const s of sessions) {
      const k = `${s.source}\u0000${s.sessionId}`;
      if (owner.has(k)) throw new CorpusError(`${where}: a session belongs to two gold features`);
      owner.set(k, f.id);
    }
    const asked = (f.asked as Record<string, unknown>[] ?? []).map((a) => ({ ...key(a, where), text: String(a.text ?? "") }));
    const gaps = (f.gaps as Record<string, unknown>[] ?? []).map((a) => {
      if (!GAP_KINDS.includes(a.kind as GapKind)) throw new CorpusError(`${where}: unknown gap kind`);
      return { ...key(a, where), kind: a.kind as GapKind, text: String(a.text ?? "") };
    });
    return { id: f.id, title: String(f.title ?? ""), split: f.split as GoldFeature["split"], state: f.state as FeatureState, sessions, asked, gaps };
  });
  return { format: GOLD_FORMAT, corpus: corpusSha256, asOf: g.asOf, coverage: g.coverage, features };
}
