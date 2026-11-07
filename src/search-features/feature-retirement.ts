import type {
  ClickHouseClient,
  QueryParams,
} from "../session-pipeline/vendor/search-index/clickhouse.ts";
import { EVENT_FEATURES_TABLE } from "./event-schema.ts";
import { BRIEF_FEATURES_TABLE } from "./brief-schema.ts";
import { DOCUMENT_FEATURES_TABLE } from "./document-schema.ts";

type FeatureTable = typeof EVENT_FEATURES_TABLE | typeof BRIEF_FEATURES_TABLE |
  typeof DOCUMENT_FEATURES_TABLE;
type RetirementCommand = {
  query: string;
  options: NonNullable<Parameters<ClickHouseClient["command"]>[1]>;
};

/** The same read-visibility cleanup for legacy and canonical publication.
 * Tombstones alone do not hide older ReplacingMergeTree rows from plain reads.
 * Keep the tombstone and later resurrections; remove only older exact keys.
 * Lightweight deletes hide rows from ordinary reads, not an attestation that
 * inactive parts, backups or source objects have been physically erased.
 * Callers must await every command before recording a durable page ACK.
 */
export function featureRetirementCommands(
  table: FeatureTable,
  rows: readonly Record<string, unknown>[],
): RetirementCommand[] {
  if (table !== EVENT_FEATURES_TABLE && table !== BRIEF_FEATURES_TABLE &&
      table !== DOCUMENT_FEATURES_TABLE)
    throw new Error("invalid feature retirement table");
  const erased = rows.filter((r) => r.deleted === 1);
  const commands: RetirementCommand[] = [];
  // Canonical pages contain up to 1,000 rows. Retain the legacy 200-row
  // mutation bound and allow lease/cancellation fences between commands.
  for (let offset = 0; offset < erased.length; offset += 200) {
    const params: QueryParams = {};
    const where = erased.slice(offset, offset + 200).map((r, n) => {
      if (table === EVENT_FEATURES_TABLE) {
        const terms = ["org_id", "workspace_id", "source", "session_id",
          "machine_id", "kind", "event_id", "ts"].map((f, i) => {
          const name = "k" + n + "_" + i;
          params[name] = r[f] as string;
          return f === "ts"
            ? f + "=parseDateTime64BestEffort({" + name + ":String},6,'UTC')"
            : f + "={" + name + ":String}";
        });
        params["v" + n] = r.version as string;
        terms.push("version<{v" + n + ":UInt64}");
        return "(" + terms.join(" AND ") + ")";
      }
      for (const [f, k] of [["org_id", "o"], ["workspace_id", "w"],
        ["source", "h"], ["session_id", "s"], ["version", "v"]] as const)
        params[k + n] = r[f] as string;
      return `(org_id={o${n}:String} AND workspace_id={w${n}:String} AND source={h${n}:String} AND session_id={s${n}:String} AND version<{v${n}:UInt64})`;
    });
    commands.push({
      query: `DELETE FROM ${table} WHERE ${where.join(" OR ")}`,
      options: {
        params,
        settings: { lightweight_deletes_sync: 2, max_query_size: 1048576 },
      },
    });
  }
  return commands;
}
