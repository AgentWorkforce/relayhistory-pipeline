/**
 * Seeded transcript corpus over corpus.ts, with needles of the kinds people search for,
 * and a fixed versioned query set. The oracle reads the generator, never an engine.
 */
import { createHash } from "node:crypto";
import { generateCorpus, PLANTED_SECRET, tenantCanary, type CorpusParams } from "../corpus.ts";
import type { SearchRecord } from "./store.ts";

export const QUERY_SET_VERSION = "2026-10-31.1";
const LONG_PAD = 5_000;

/** class, query as typed, text planted, share of sessions. `beyondPrefix` sits past 4 KB. */
export const NEEDLES = [
  { id: "phrase", cls: "phrase", query: "flaky retry loop", plant: "flaky retry loop", share: 0.01 },
  { id: "error", cls: "error", query: "ECONNRESET at RelayClient.flush", plant: "ECONNRESET at RelayClient.flush (relay-client.ts:214:9)", share: 0.008 },
  { id: "path", cls: "file_path", query: "packages/pipeline/src/lib/pipeline/raw-chunks.ts", plant: "edit packages/pipeline/src/lib/pipeline/raw-chunks.ts:435", share: 0.008 },
  { id: "pr", cls: "pr_number", query: "PR #4821", plant: "merged PR #4821 into main", share: 0.003 },
  { id: "case", cls: "case_fold", query: "camelhandlererror", plant: "threw CamelHandlerError on boot", share: 0.004 },
  { id: "substring", cls: "substring", query: "oekraa", plant: "zoekraakvlak", share: 0.004 },
  { id: "cjk", cls: "cjk", query: "検索テスト", plant: "結果は検索テストです", share: 0.004 },
  { id: "accent", cls: "non_ascii", query: "café-übung", plant: "siehe café-übung heute", share: 0.004 },
  { id: "short", cls: "short_term", query: "q7", plant: "flag q7 set", share: 0.002 },
  { id: "punct", cls: "punctuation", query: "100%_done", plant: "stage 100%_done reached", share: 0.002 },
  { id: "tail", cls: "beyond_4kb", query: "tailbeacon", plant: "tailbeacon", share: 0.004, beyondPrefix: true },
  { id: "common", cls: "common", query: "deploy", plant: "deploy finished", share: 0.25 },
] as const;
export type Needle = (typeof NEEDLES)[number];

const unit = (id: string, salt: string) => createHash("sha256").update(`${salt}:${id}`).digest().readUInt32BE(0) / 2 ** 32;

export interface OracleHit { eventId: string; ts: string; sessionId: string }
export class Oracle {
  counts = new Map<string, number>();
  private top = new Map<string, OracleHit[]>();
  constructor(private depth = 20) {}
  add(key: string, hit: OracleHit) {
    this.counts.set(key, (this.counts.get(key) ?? 0) + 1);
    const list = this.top.get(key) ?? [];
    list.push(hit);
    list.sort((a, b) => (a.ts === b.ts ? (a.eventId < b.eventId ? 1 : -1) : a.ts < b.ts ? 1 : -1));
    if (list.length > this.depth) list.pop();
    this.top.set(key, list);
  }
  newest(key: string) { return this.top.get(key) ?? []; }
}
export const oracleKey = (orgId: string, needle: string) => `${orgId}|${needle}`;

/** Bulk path: skips the scrubber, so the planted secret is dropped here. G1 proves scrubbing. */
export function* transcriptBatches(p: CorpusParams, oracle: Oracle, size = 20_000): Generator<SearchRecord[]> {
  let batch: SearchRecord[] = [];
  for (const s of generateCorpus(p)) {
    if (s.deleted) continue;
    const hits = NEEDLES.filter((n) => unit(s.sessionId, n.id) < n.share);
    const canary = unit(s.sessionId, "canary") < 0.02 ? tenantCanary(s.orgId, s.workspaceId) : null;
    s.events.forEach((e, n) => {
      const parts = [e.content.replace(PLANTED_SECRET, "")];
      if (n === 0) for (const h of hits) parts.push("beyondPrefix" in h ? `${"x ".repeat(LONG_PAD / 2)}${h.plant}` : h.plant);
      if (n === 0 && canary) parts.push(canary);
      const text = parts.join(" ");
      const ts = e.ts.toISOString();
      if (n === 0) for (const h of hits) oracle.add(oracleKey(s.orgId, h.id), { eventId: e.eventId, ts, sessionId: s.sessionId });
      batch.push({
        orgId: s.orgId, workspaceId: s.workspaceId, harness: s.harness, sessionId: s.sessionId, machineId: s.machineIds[0]!,
        eventId: e.eventId, revision: 1, chunkSeq: 0, recordSeq: n, ts, kind: "message", scrubVersion: 1, text,
      });
    });
    if (batch.length >= size) { yield batch; batch = []; }
  }
  if (batch.length) yield batch;
}
