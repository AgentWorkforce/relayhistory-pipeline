/** Offline bundle boundary: duplicate/unknown keys never become trusted evidence. */
import { constants } from "node:fs";
import { open } from "node:fs/promises";
import type { CanonicalFrontierCapture, FrontierExportBinding } from "./frontier-plan.ts";
export const MAX_FRONTIER_BUNDLE_BYTES = 64 * 1024 * 1024;
const invalid = (): never => { throw Object.assign(new Error("Invalid offline frontier evidence"), { code: "frontier_plan_evidence_invalid" }); };

/** Parse the JSON grammar with a per-object duplicate-key check before information is lost. */
export function parseFrontierJson(text: string): unknown {
  let at = 0, nodes = 0;
  const numberPattern = /-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/y;
  const space = () => { while (at < text.length && /[ \t\r\n]/.test(text[at]!)) at++; };
  const string = (): string => {
    const start = at++;
    while (at < text.length) {
      const c = text[at++];
      if (c === '"') { try { return JSON.parse(text.slice(start, at)); } catch { return invalid(); } }
      if (c === "\\") { if (at >= text.length) return invalid(); at++; }
    }
    return invalid();
  };
  const value = (depth: number): unknown => {
    if (depth > 64 || ++nodes > 1_000_000) return invalid();
    space();
    const c = text[at];
    if (c === '"') return string();
    if (c === "{") {
      at++; space();
      const out: Record<string, unknown> = Object.create(null), seen = new Set<string>();
      if (text[at] === "}") { at++; return out; }
      while (at < text.length) {
        if (text[at] !== '"') return invalid();
        const key = string();
        if (seen.has(key)) return invalid();
        seen.add(key); space();
        if (text[at++] !== ":") return invalid();
        out[key] = value(depth + 1); space();
        const end = text[at++];
        if (end === "}") return out;
        if (end !== ",") return invalid();
        space();
      }
      return invalid();
    }
    if (c === "[") {
      at++; space(); const out: unknown[] = [];
      if (text[at] === "]") { at++; return out; }
      while (at < text.length) {
        out.push(value(depth + 1)); space();
        const end = text[at++];
        if (end === "]") return out;
        if (end !== ",") return invalid();
        space();
      }
      return invalid();
    }
    for (const [word, result] of [["true", true], ["false", false], ["null", null]] as const) {
      if (text.startsWith(word, at)) { at += word.length; return result; }
    }
    numberPattern.lastIndex = at;
    const number = numberPattern.exec(text);
    if (!number) return invalid();
    at += number[0].length;
    const n = Number(number[0]);
    if (!Number.isSafeInteger(n)) return invalid();
    return n;
  };
  const result = value(0); space();
  if (at !== text.length) return invalid();
  return result;
}

const keys = (value: unknown, names: string): Record<string, unknown> => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return invalid();
  const wanted = names.split(" "), actual = Object.keys(value);
  if (wanted.length !== actual.length || wanted.some(k => !Object.hasOwn(value, k))) return invalid();
  return value as Record<string, unknown>;
};
const many = (value: unknown, check: (item: unknown) => unknown): void => {
  if (!Array.isArray(value)) return invalid();
  for (const item of value) check(item);
};
const tenant = (v: unknown) => keys(v, "orgId workspaceId userId originId");
const authority = (v: unknown) => keys(v, "sourceIdentitySha256 sourceGenerationSha256 sourceHead controlEpoch privacyBaselineSha256 privacyVersion");
const scan = (v: unknown) => keys(v, "kind adapter querySha256 snapshotSha256 startedAt finishedAt pageLimit maxPages pagesRead pageItemCounts itemCount byteCount checkpointCount pageChainSha256 itemsSha256 openingStateSha256 closingStateSha256 terminalCursor complete");

export function frontierBundle(value: unknown): { capture: CanonicalFrontierCapture; exportBindings: FrontierExportBinding[] } {
  const bundle = keys(value, "capture exportBindings");
  const capture = keys(bundle.capture, "protocol captureId startedAt capturedAt scope target expectedOrigins inventoryScan origins");
  keys(capture.scope, "orgId workspaceId");
  keys(capture.target, "bucket region keyRoot schemaGeneration");
  many(capture.expectedOrigins, v => keys(v, "userId originId")); scan(capture.inventoryScan);
  many(capture.origins, v => {
    const o = keys(v, "tenant opening closing sourceRecordCount privacyRowCount scans exportRoots recordMaxima receiptDecisions");
    tenant(o.tenant); authority(o.opening); authority(o.closing); many(o.scans, scan);
    many(o.exportRoots, r => {
      const root = keys(r, "tenant kind provider bucket keyRoot manifestKey source sessionId machineId versionId manifestRevision contentSha256 recordCount inventoryComplete versioning");
      tenant(root.tenant);
    });
    many(o.recordMaxima, r => {
      const record = keys(r, "tenant recordId revision revisionId digest kind source sessionId operation authority exportRootSha256 exportContentSha256 exportEvidenceSha256");
      tenant(record.tenant);
    });
    many(o.receiptDecisions, r => {
      const decision = keys(r, "tenant recordId revision revisionId source sessionId seat bodyAccepted candidateDigest firstDigest");
      tenant(decision.tenant);
    });
  });
  many(bundle.exportBindings, v => keys(v, "exportRootSha256 recordsSha256"));
  return bundle as unknown as { capture: CanonicalFrontierCapture; exportBindings: FrontierExportBinding[] };
}

/** Bound reads while consuming the same opened regular file, including growth after stat. */
export async function readFrontierBundle(path: string) {
  const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const stat = await handle.stat();
    if (!stat.isFile() || stat.size > MAX_FRONTIER_BUNDLE_BYTES) return invalid();
    const chunks: Buffer[] = []; let size = 0;
    while (true) {
      const chunk = Buffer.alloc(Math.min(64 * 1024, MAX_FRONTIER_BUNDLE_BYTES + 1 - size));
      const { bytesRead } = await handle.read(chunk, 0, chunk.length, null);
      if (!bytesRead) break;
      size += bytesRead;
      if (size > MAX_FRONTIER_BUNDLE_BYTES) return invalid();
      chunks.push(chunk.subarray(0, bytesRead));
    }
    let text: string;
    try { text = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks, size)); }
    catch { return invalid(); }
    return frontierBundle(parseFrontierJson(text));
  } finally { await handle.close(); }
}
