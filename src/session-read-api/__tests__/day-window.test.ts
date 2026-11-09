/**
 * resolveDayWindow, ported verbatim from relayhistory (day-window.ts). These cases are
 * relayhistory test/digest.test.ts @ c7de2163 "resolveDayWindow", copied unchanged (vitest
 * `it` → bun `test`), so the DST gap and ambiguity behaviour is pinned to the original.
 */
import { describe, expect, test as it } from "bun:test";
import { DigestInputError, resolveDayWindow } from "../day-window.ts";

describe("daily digest", () => {
  describe("resolveDayWindow", () => {
    it("defaults to UTC and to today's UTC date when nothing is supplied", () => {
      const now = new Date("2026-09-05T12:00:00.000Z");
      const window = resolveDayWindow(undefined, undefined, now);
      expect(window).toMatchObject({
        date: "2026-09-05",
        tz: "UTC",
        since: new Date("2026-09-05T00:00:00.000Z"),
        until: new Date("2026-09-06T00:00:00.000Z"),
      });
    });

    it("computes the day's default from `tz`, not from the server's UTC day", () => {
      // 2026-09-05T02:00:00Z is already 2026-09-05 11:00 in Tokyo (UTC+9) but still
      // 2026-09-04 22:00 in Los Angeles (UTC-7 in September) — a UTC-day default would
      // give both callers the same, wrong answer for one of them.
      const now = new Date("2026-09-05T02:00:00.000Z");
      expect(resolveDayWindow(undefined, "Asia/Tokyo", now).date).toBe(
        "2026-09-05",
      );
      expect(resolveDayWindow(undefined, "America/Los_Angeles", now).date).toBe(
        "2026-09-04",
      );
    });

    it("computes UTC boundaries for a positive-offset zone", () => {
      const window = resolveDayWindow("2026-09-05", "Asia/Tokyo");
      expect(window.since.toISOString()).toBe("2026-09-04T15:00:00.000Z");
      expect(window.until.toISOString()).toBe("2026-09-05T15:00:00.000Z");
    });

    it("computes UTC boundaries for a DST-observing negative-offset zone", () => {
      // America/New_York is UTC-4 (EDT) in September.
      const window = resolveDayWindow("2026-09-05", "America/New_York");
      expect(window.since.toISOString()).toBe("2026-09-05T04:00:00.000Z");
      expect(window.until.toISOString()).toBe("2026-09-06T04:00:00.000Z");
    });

    it("rejects an unparseable date with DigestInputError, not a silent fallback", () => {
      expect(() => resolveDayWindow("not-a-date", "UTC")).toThrow(
        DigestInputError,
      );
      expect(() => resolveDayWindow("2026-13-01", "UTC")).toThrow(
        DigestInputError,
      );
      // 2026 is not a leap year: Feb 30 and Feb 29 are both not real dates.
      expect(() => resolveDayWindow("2026-02-30", "UTC")).toThrow(
        DigestInputError,
      );
    });

    it("rejects an unknown time zone with DigestInputError", () => {
      expect(() => resolveDayWindow("2026-09-05", "Mars/Olympus_Mons")).toThrow(
        DigestInputError,
      );
    });

    it("starts the day at the clock-jump instant when local midnight falls in a spring-forward gap", () => {
      // Chile begins DST on 2026-09-06: clocks jump from 2026-09-05T23:59:59-04:00 straight
      // to 2026-09-06T01:00:00-03:00 (verified via `zdump -v America/Santiago`). Local
      // midnight for Sept 6 never exists — the first instant that is actually Sept 6 there
      // is the jump itself, 2026-09-06T04:00:00Z, not the fixed-point iteration's stale
      // 03:00Z (which formats back to Sept 5 23:00 local, the previous day's last hour).
      const window = resolveDayWindow("2026-09-06", "America/Santiago");
      expect(window.since.toISOString()).toBe("2026-09-06T04:00:00.000Z");
    });

    it("ends the previous day at the same clock-jump instant, not a UTC-shifted guess", () => {
      // The day before a spring-forward gap has the identical hazard on its `until`: Sept
      // 5's window must close at the exact instant Sept 6 begins in Santiago.
      const window = resolveDayWindow("2026-09-05", "America/Santiago");
      expect(window.until.toISOString()).toBe("2026-09-06T04:00:00.000Z");
    });

    it("starts the day at the earlier of two instants when local midnight is ambiguous (fall-back at midnight)", () => {
      // Jordan ended DST on 2019-10-25 by turning the clock back exactly at local midnight:
      // `zdump -v Asia/Amman` shows 2019-10-24T21:59:59Z = 2019-10-25T00:59:59 EEST, then
      // 2019-10-24T22:00:00Z = 2019-10-25T00:00:00 EET — so 2019-10-25 00:00:00 occurs twice,
      // first under EEST at 2019-10-24T21:00:00Z and again under EET an hour later. The day
      // must begin at the FIRST (earlier) instant so the wall-clock hour 00:00-00:59 EEST
      // isn't dropped from the 25th.
      const window = resolveDayWindow("2019-10-25", "Asia/Amman");
      expect(window.since.toISOString()).toBe("2019-10-24T21:00:00.000Z");
    });

    it("ends the previous day at the same earlier ambiguous instant", () => {
      const window = resolveDayWindow("2019-10-24", "Asia/Amman");
      expect(window.until.toISOString()).toBe("2019-10-24T21:00:00.000Z");
    });
  });
});
