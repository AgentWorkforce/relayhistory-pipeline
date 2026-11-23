/**
 * The drain's seated lines (buffered-drain.ts seatLine, v3 sidecar) against the publication path's seat rules: the same
 * bytes for the same line and seat, and the reader's fold refusing two copies of one revision with different seats.
 */
import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { seatLine } from "../buffered-drain.ts";
import { fitLine } from "../oversize.ts";
import { acceptedReceiptSeat, injectReceiptSeat, parseReceiptSeatSnapshot } from "../publication-seats.ts";
import { normalizeRelayhistoryRecords, parseRelayhistoryRecord } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";

const HEX = "a".repeat(64), OTHER = "b".repeat(64), SEAT = "9007199254740994";
const digest = createHash("sha256").update("native-r:1").digest("hex");
const record = { ord: 0, source: "claude", session_id: "session:1", record_id: "native-r", revision_id: "revision-1", revision: 1, digest };
const stage = { key: "unused", sha256: HEX, bytes: 1, groupOrdinals: [[0]] };
const snapshot = (seat: string) => parseReceiptSeatSnapshot({ controlEpoch: HEX, sourceIdentitySha256: OTHER, allocatedHighWater: "9007199254740993", lastAssignedSeat: "9007199254740999",
  records: [{ ord: 0, seat, bodyAccepted: true, candidateDigest: digest, firstDigest: digest, resetEpoch: "0", fenceEpoch: "2" }] }, [record], stage);
const line = (origin: string, text: string, received = "2030-01-02T01:00:00.000Z") =>
  JSON.stringify({ kind: "session_event", origin_id: origin, record_id: "native-r", revision: "1", operation: "upsert", received_at: received, payload: { text, counter: 9007199254740993, ts_ms: 1893546000000 } });
const meta = ["revision-1", digest, "claude", "session:1"] as [string, string, string, string];

test("drain seatLine gives publication's bytes for the same line and seat", () => {
  for (const raw of [line("origin-a", "plain"), line("origin-a", "literal é \"q\""), line("origin-a", "x".repeat(300_000))]) {
    const published = injectReceiptSeat(fitLine(raw), acceptedReceiptSeat(snapshot(SEAT), 0, { resetEpoch: "0", fenceEpoch: "2" }), "origin-a");
    expect(seatLine(raw, meta, { seat: SEAT, digest }, "origin-a")).toBe(published);
  }
});

test("a drain-seated copy and a differently seated publication copy of one revision: relayhistory_acceptance_conflict", () => {
  const drained = parseRelayhistoryRecord(seatLine(line("origin-a", "same"), meta, { seat: SEAT, digest }, "origin-a"));
  const published = parseRelayhistoryRecord(injectReceiptSeat(fitLine(line("origin-a", "same")), acceptedReceiptSeat(snapshot("9007199254740995"), 0), "origin-a"));
  for (const rows of [[drained, published], [published, drained]]) {
    let code: unknown;
    try { normalizeRelayhistoryRecords(rows); } catch (e) { code = (e as { code?: unknown }).code; }
    expect(code).toBe("relayhistory_acceptance_conflict");
  }
  // An equal seat, or one copy unseated (a line stored before v3), folds to the one seat.
  const same = parseRelayhistoryRecord(injectReceiptSeat(fitLine(line("origin-a", "same")), acceptedReceiptSeat(snapshot(SEAT), 0), "origin-a"));
  expect(normalizeRelayhistoryRecords([drained, same]).map((r) => r.accepted_seq)).toEqual([SEAT]);
  const unseated = parseRelayhistoryRecord(fitLine(line("origin-a", "same")));
  expect(normalizeRelayhistoryRecords([unseated, drained]).map((r) => r.accepted_seq)).toEqual([SEAT]);
});
