import { comparePublicTurnRows } from "../shared/legacy-turn-source.ts";
/** Legacy public evidence preserved explicitly in the canonical raw stream. */
import type { RelayhistoryStreamRecord } from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { CanonicalReadError, type CanonicalIdentity } from "./canonical.ts";
export const LEGACY_SOURCE_TABLES = [
  "conversation_turns",
  "session_links",
  "session_outcomes",
] as const;
const kinds = new Map<string, string>([
  ["legacy_conversation_turn", "conversation_turns"],
  ["legacy_session_link", "session_links"],
  ["legacy_session_outcome", "session_outcomes"],
]);
/** A record outside the authenticated scope, or a malformed coverage marker:
 * the whole read is refused (a 503 the caller falls back on, never a 500). */
function fail(): never {
  throw new CanonicalReadError("legacy_evidence_invalid");
}
const clock = (v: unknown, nullable = false) =>
  (nullable && v === null) ||
  (typeof v === "string" && Number.isFinite(Date.parse(v)));
/** Neon `integer`: any int4, negative included (activation gate 6). */
const int4 = (v: unknown) =>
  typeof v === "number" &&
  Number.isSafeInteger(v) &&
  v >= -2147483648 &&
  v <= 2147483647;
export function canonicalLegacy(
  records: RelayhistoryStreamRecord[],
  identity: CanonicalIdentity,
) {
  let coverage = false,
    rejected = 0;
  const rows: Array<{ table: string; row: Record<string, unknown> }> = [];
  for (const record of records) {
    const table = kinds.get(record.kind);
    if (!table && record.kind !== "legacy_scope") continue;
    const p = record.payload;
    if (
      !p ||
      p.org_id !== identity.orgId ||
      p.workspace_id !== identity.workspaceId ||
      p.source !== identity.source ||
      p.session_id !== identity.sessionId
    )
      fail();
    if (record.kind === "legacy_scope") {
      if (
        p.version !== 1 ||
        record.origin_id !== "legacy:scope" ||
        record.record_id !== "legacy:scope:coverage" ||
        !Array.isArray(p.tables) ||
        JSON.stringify([...p.tables].sort()) !==
          JSON.stringify([...LEGACY_SOURCE_TABLES].sort())
      )
        fail();
      coverage = true;
      continue;
    }
    if (record.origin_id !== "legacy:" + table) fail();
    // A row the Neon schema could not hold is reported (counted, and coverage
    // becomes unknown), never a failure of the whole read (activation gate 6).
    let valid = true;
    if (table === "conversation_turns") {
      if (
        typeof p.id !== "string" ||
        "legacy:" + table + ":" + p.id !== record.record_id ||
        (p.legacy_stored_source !== undefined &&
          typeof p.legacy_stored_source !== "string") ||
        !int4(p.turn_index) ||
        !["user", "assistant", "system"].includes(String(p.role)) ||
        !["session_owner", "content", "actor_name", "actor_role"].every(
          (k) => typeof p[k] === "string",
        ) ||
        !clock(p.ts)
      )
        valid = false;
    } else if (table === "session_links") {
      if (
        typeof p.id !== "string" ||
        "legacy:" + table + ":" + p.id !== record.record_id ||
        !/^[1-9][0-9]{0,18}$/u.test(p.id) ||
        BigInt(p.id) > 9223372036854775807n ||
        !["link_kind", "link_ref"].every((k) => typeof p[k] === "string") ||
        !clock(p.link_ts, true)
      )
        valid = false;
    } else if (table === "session_outcomes") {
      if (
        typeof p.commit_sha !== "string" ||
        "legacy:" + table + ":" + p.commit_sha !== record.record_id ||
        !p.commit_sha ||
        typeof p.reverted !== "boolean" ||
        !clock(p.shipped_at, true) ||
        !clock(p.created_at)
      )
        valid = false;
    }
    if (!valid) {
      rejected += 1;
      continue;
    }
    rows.push({ table: table!, row: p });
  }
  const orderedTurns = rows
    .filter((r) => r.table === "conversation_turns")
    .sort((a, b) => comparePublicTurnRows(a.row, b.row));
  let next = 0;
  return {
    rows: rows.map((r) =>
      r.table === "conversation_turns" ? orderedTurns[next++]! : r,
    ),
    coverage:
      coverage && rejected === 0
        ? { version: 1 as const, tables: [...LEGACY_SOURCE_TABLES] }
        : null,
    rejected,
  };
}

export const isLegacySourceTable = (
  table: string,
): table is (typeof LEGACY_SOURCE_TABLES)[number] =>
  (LEGACY_SOURCE_TABLES as readonly string[]).includes(table);
const byteOrder = (a: unknown, b: unknown) =>
  Buffer.compare(Buffer.from(String(a ?? "")), Buffer.from(String(b ?? "")));
const legacyOrder: Record<
  (typeof LEGACY_SOURCE_TABLES)[number],
  (a: Record<string, unknown>, b: Record<string, unknown>) => number
> = {
  conversation_turns: comparePublicTurnRows,
  session_links: (a, b) => {
    const x = BigInt(String(a.id)),
      y = BigInt(String(b.id));
    return x < y ? -1 : x > y ? 1 : 0;
  },
  session_outcomes: (a, b) =>
    byteOrder(a.commit_sha, b.commit_sha) ||
    byteOrder(a.workspace_id, b.workspace_id),
};
/**
 * Neon key order for a legacy table (byte = C order; links by bigint id;
 * outcomes by commit_sha), reversed for `desc`. In place (activation gate 9).
 */
export function sortLegacyRows<T extends { row: Record<string, unknown> }>(
  table: (typeof LEGACY_SOURCE_TABLES)[number],
  rows: T[],
  order: "asc" | "desc",
): T[] {
  const sign = order === "asc" ? 1 : -1;
  return rows.sort((a, b) => sign * legacyOrder[table](a.row, b.row));
}
