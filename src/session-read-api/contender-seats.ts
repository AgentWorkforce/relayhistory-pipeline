/**
 * `contender_seats` on the search pass lines (the backport lineage's 9279264f): what the acceptance refusal of a
 * whole read looked like, computed on the side from records the reader already holds, only on the failure path.
 * The fold itself (relayhistory-transform lastAccepted) is unchanged.
 *
 * A refused group is two or more contending records with one record id (the grouping of
 * relayhistoryNewestAcrossOrigins on main: convergence events and legacy records never contend) of which at least one
 * has no accepted_seq. The fold refuses exactly those groups.
 *
 * - `all_unseated`: no refused group has a seated member (one with accepted_seq);
 * - `seated_unseated`: every refused group has a seated member;
 * - `both`: refused groups of both kinds;
 * - `catalog_key`: the pass ended with the refusal but no record id group was refused; every refusal came from the
 *   catalog-key fold in projectRelayhistorySession.
 */
import { CONVERGENCE_EVENT_KIND } from "../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import {
  isLegacyRelayhistoryRecord,
  relayhistoryRecordKey,
  type RelayhistoryStreamRecord,
} from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";

/** The fold's refusal when same-record contenders from different origins lack accepted_seq. */
export const ACCEPTANCE_MISSING = "relayhistory_acceptance_missing";

export const CONTENDER_SEATS = ["all_unseated", "seated_unseated", "both", "catalog_key"] as const;
export type ContenderSeats = (typeof CONTENDER_SEATS)[number];
export type RecordIdSeats = Exclude<ContenderSeats, "catalog_key">;

/** The word for record id groups, given each group's members' accepted_seq; undefined when no group is refused. */
export function seatsOfGroups(groups: Iterable<readonly (string | undefined)[]>): RecordIdSeats | undefined {
  let unseatedOnly = false;
  let mixed = false;
  for (const group of groups) {
    if (group.length < 2) continue;
    let seated = 0;
    for (const seq of group) if (seq !== undefined) seated++;
    if (seated === group.length) continue;
    if (seated === 0) unseatedOnly = true;
    else mixed = true;
  }
  if (unseatedOnly && mixed) return "both";
  if (unseatedOnly) return "all_unseated";
  if (mixed) return "seated_unseated";
  return undefined;
}

/** The record id fold's input, as normalizeRelayhistoryRecords sees it: each contending seat's accepted_seq, grouped by record id. */
function seatGroupsOfRecords(records: Iterable<RelayhistoryStreamRecord>): Map<string, (string | undefined)[]> {
  const latest = new Map<string, { revision: bigint; seq: string | undefined; contends: boolean; recordId: string }>();
  for (const r of records) {
    const key = relayhistoryRecordKey(r);
    let revision: bigint;
    try { revision = BigInt(r.revision); } catch { continue; }
    const prior = latest.get(key);
    if (!prior || revision > prior.revision) {
      latest.set(key, { revision, seq: r.accepted_seq, contends: r.kind !== CONVERGENCE_EVENT_KIND && !isLegacyRelayhistoryRecord(r), recordId: r.record_id });
      continue;
    }
    if (revision === prior.revision) prior.seq = r.accepted_seq ?? prior.seq;
  }
  const groups = new Map<string, (string | undefined)[]>();
  for (const seat of latest.values()) {
    if (!seat.contends) continue;
    const group = groups.get(seat.recordId);
    if (group) group.push(seat.seq);
    else groups.set(seat.recordId, [seat.seq]);
  }
  return groups;
}

/**
 * The word for the record set handed to normalizeRelayhistoryRecords, over the seats its cross-origin fold sees: per
 * record key the newest revision, with accepted_seq merged across copies of that same revision, tombstones
 * included, convergence events and legacy records left out, grouped by record id. Call it only after that fold
 * refused with ACCEPTANCE_MISSING.
 */
export function contenderSeatsOfRecords(records: Iterable<RelayhistoryStreamRecord>): RecordIdSeats | undefined {
  return seatsOfGroups(seatGroupsOfRecords(records).values());
}

/**
 * The record ids of the refused groups over the same seats (two or more contenders, at least one without
 * accepted_seq), sorted. Each is a group the cross-origin fold (lastAccepted) refuses for as long as no record with
 * that record id is added to the set: the group is built only from records carrying the id. Call it only after
 * that fold refused with ACCEPTANCE_MISSING; empty when every refusal came from the catalog-key fold.
 */
export function refusedRecordIdsOfRecords(records: Iterable<RelayhistoryStreamRecord>): string[] {
  const out: string[] = [];
  for (const [recordId, group] of seatGroupsOfRecords(records)) if (group.length >= 2 && group.some((seq) => seq === undefined)) out.push(recordId);
  return out.sort();
}
