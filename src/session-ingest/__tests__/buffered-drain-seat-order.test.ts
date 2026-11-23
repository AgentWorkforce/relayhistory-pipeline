/**
 * The drain seats a line after fitLine, as publication-image.ts does (`injectReceiptSeat(fitLine(line), ...)`). The
 * real fitLine re-serializes a line in place, so either order gives the same bytes for every line it is given today;
 * this file swaps in a fitLine whose output shows the order, so a reorder in seatLine cannot pass unnoticed.
 */
import { expect, mock, test } from "bun:test";
import { createHash } from "node:crypto";
import * as real from "../oversize.ts";

const fitted = (line: string) => line.trimEnd().slice(0, -1) + ',"fitted":true}';
mock.module("../oversize.ts", () => ({ ...real, fitLine: fitted }));

test("seatLine appends the seat to fitLine's output, never fits an already seated line", async () => {
  const { seatLine } = await import("../buffered-drain.ts");
  const digest = createHash("sha256").update("r1:1").digest("hex");
  const line = JSON.stringify({ kind: "session_event", origin_id: "origin-1", record_id: "r1", revision: "1", operation: "upsert", received_at: "2030-01-02T00:00:00.000Z", payload: { text: "t" } });
  const out = seatLine(line, ["r1:r1", digest, "claude", "sess-1"], { seat: "5", digest }, "origin-1");
  expect(out).toBe(line.slice(0, -1) + ',"fitted":true,"accepted_seq":"5"}');
});
