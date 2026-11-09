/**
 * The digest's civil-day window: relayhistory `lib/digest.ts` @ c7de2163,
 * `DigestInputError` through `wallClockPartsAt`, copied VERBATIM (the 400
 * messages, DST gap and ambiguity handling are part of the contract). Pure; no I/O.
 * Do not edit in place: re-copy from relayhistory if it changes.
 */
export class DigestInputError extends Error {}

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

export interface DigestWindow {
  /** `YYYY-MM-DD`, the civil day in `tz` — echoed back verbatim in the response. */
  date: string;
  tz: string;
  /** Inclusive start of the day, in UTC. */
  since: Date;
  /** Exclusive end of the day, in UTC — the instant local midnight of the NEXT day begins. */
  until: Date;
}

/**
 * Resolve `?date`/`?tz` into a UTC instant window, or throw `DigestInputError`.
 *
 * A missing `date` defaults to "today in `tz`" — computed in that zone, not read off the
 * server clock's UTC day, so a caller west of Greenwich near midnight gets their own today
 * rather than tomorrow's. A missing `tz` defaults to UTC, matching the spec's table.
 */
export function resolveDayWindow(
  dateParam: string | undefined,
  tzParam: string | undefined,
  now: Date = new Date(),
): DigestWindow {
  const tz = tzParam?.trim() || "UTC";
  if (!isValidTimeZone(tz)) {
    throw new DigestInputError(`tz is not a recognised IANA time zone: ${tz}`);
  }

  const date = dateParam?.trim() || todayInTimeZone(tz, now);
  if (!DATE_RE.test(date)) {
    throw new DigestInputError(`date must be YYYY-MM-DD: ${date}`);
  }
  const [year, month, day] = date.split("-").map(Number) as [
    number,
    number,
    number,
  ];
  if (!isRealCalendarDate(year, month, day)) {
    throw new DigestInputError(`date is not a real calendar date: ${date}`);
  }

  return {
    date,
    tz,
    since: zonedMidnightUtc(date, tz),
    until: zonedMidnightUtc(addCalendarDays(date, 1), tz),
  };
}

function isValidTimeZone(tz: string): boolean {
  try {
    // Throws RangeError for anything that isn't a real IANA zone name; this is the
    // standard way to validate one without shipping a zone database ourselves.
    new Intl.DateTimeFormat("en-US", { timeZone: tz });
    return true;
  } catch {
    return false;
  }
}

function isRealCalendarDate(year: number, month: number, day: number): boolean {
  const roundTrip = new Date(Date.UTC(year, month - 1, day));
  return (
    roundTrip.getUTCFullYear() === year &&
    roundTrip.getUTCMonth() === month - 1 &&
    roundTrip.getUTCDate() === day
  );
}

/** `en-CA` is the one built-in locale whose numeric date format is already `YYYY-MM-DD`. */
function todayInTimeZone(tz: string, now: Date): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: tz,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(now);
}

/** Pure calendar arithmetic on the `YYYY-MM-DD` label — never touches a time zone. */
function addCalendarDays(date: string, days: number): string {
  const [year, month, day] = date.split("-").map(Number) as [
    number,
    number,
    number,
  ];
  return new Date(Date.UTC(year, month - 1, day + days))
    .toISOString()
    .slice(0, 10);
}

/**
 * The UTC instant that starts local `date` in `tz` — ordinarily local midnight, but not
 * always: a zone that moves its clock AT midnight can make that wall-clock instant not
 * exist at all (spring forward — a gap, e.g. America/Santiago on 2026-09-06, which jumps
 * straight from 2026-09-05T23:59:59-04:00 to 2026-09-06T01:00:00-03:00) or exist twice
 * (fall back — ambiguous, e.g. Asia/Amman on 2019-10-25, which reads 00:00:00 once under
 * EEST and again, an hour later, under EET).
 *
 * There is no direct JS API for this, so it is solved with the platform's `Intl` offset
 * lookups. A naive fixed-point search — guess an instant, ask the platform what wall-clock
 * time that instant reads as in `tz`, correct the guess by how far off it was, repeat —
 * converges correctly when `date`'s midnight exists exactly once, but cannot tell a gap or
 * an ambiguity apart from ordinary convergence: for a gap it quietly settles on the offset
 * from the WRONG side of the transition (landing on the previous day's last hour instead of
 * throwing or advancing), and for an ambiguity it settles on whichever of the two valid
 * instants the iteration happens to land on first, not necessarily the earlier one. So
 * instead, the offset in effect on the calendar day just before `date` and the offset in
 * effect just after are sampled independently, and each is used to build a candidate
 * instant for `date`'s midnight; only then is it possible to tell whether zero, one, or two
 * of them actually read back as `date`'s midnight in `tz`.
 */
function zonedMidnightUtc(date: string, tz: string): Date {
  const [year, month, day] = date.split("-").map(Number) as [
    number,
    number,
    number,
  ];
  const target = Date.UTC(year, month - 1, day, 0, 0, 0, 0);

  // A rough, possibly-wrong-by-the-transition-size estimate of `date`'s midnight, used only
  // to anchor two samples a half day to either side of it. Even right on top of a gap or an
  // ambiguity this is off by at most the size of the clock jump (minutes to a couple of
  // hours in every real zone), so a half day of margin keeps both anchors safely inside the
  // calendar days before and after `date` — never straddling the transition themselves —
  // regardless of how far `tz`'s offset is from UTC.
  const roughOffset = offsetMs(target, tz);
  const roughGuess = target - roughOffset;
  const HALF_DAY_MS = 12 * 60 * 60 * 1000;

  const offsetBeforeDate = offsetMs(roughGuess - HALF_DAY_MS, tz);
  const offsetAfterDate = offsetMs(roughGuess + HALF_DAY_MS, tz);

  const candidateBefore = target - offsetBeforeDate;
  const candidateAfter = target - offsetAfterDate;
  const isDateMidnight = (epochMs: number) => {
    const wc = wallClockPartsAt(epochMs, tz);
    return (
      wc.year === year &&
      wc.month === month &&
      wc.day === day &&
      wc.hour === 0 &&
      wc.minute === 0 &&
      wc.second === 0
    );
  };

  const beforeIsValid = isDateMidnight(candidateBefore);
  const afterIsValid = isDateMidnight(candidateAfter);

  if (beforeIsValid && afterIsValid) {
    // Ambiguous: `tz` falls back across `date`'s midnight, so it happens twice (e.g.
    // Asia/Amman on 2019-10-25, which reads 00:00:00 once under EEST and again, an hour
    // later, under EET). Start the day at the earlier instant so the wall-clock hour
    // between them is counted as part of `date`, not silently dropped from it.
    return new Date(Math.min(candidateBefore, candidateAfter));
  }
  if (beforeIsValid) return new Date(candidateBefore);
  if (afterIsValid) return new Date(candidateAfter);
  if (offsetBeforeDate === offsetAfterDate) {
    // No transition was found in the sampled window and yet the straightforward candidate
    // didn't read back as `date`'s midnight — should not happen for any real IANA zone, but
    // this is a safer failure than looping forever: fall back to the candidate rather than
    // never returning.
    return new Date(candidateBefore);
  }

  // Gap: `tz` springs forward across `date`'s midnight, so it never occurs (e.g.
  // America/Santiago on 2026-09-06, which jumps straight from 2026-09-05T23:59:59-04:00 to
  // 2026-09-06T01:00:00-03:00). `date` begins at the first instant that actually exists on
  // it — the instant the clock jumps to — found by bisecting the offset change between the
  // two known-good anchors rather than assuming any particular jump size.
  return new Date(
    findOffsetChange(roughGuess - HALF_DAY_MS, roughGuess + HALF_DAY_MS, tz),
  );
}

/**
 * UTC-minus-local offset, in ms, in effect at `epochMs` in `tz` (positive east of UTC).
 *
 * `Intl` only reports wall-clock time to whole-second precision, so `epochMs`'s own
 * sub-second remainder has to be re-attached to the reconstructed "wall clock as if UTC"
 * value by hand — every real IANA offset is a whole number of seconds, so that remainder is
 * identical in both. Skipping this turns the result into a sawtooth that is off by up to
 * 999ms depending only on where `epochMs` falls within its second, which is harmless for
 * the day-boundary math above (real offset changes land on whole minutes) but would make
 * `findOffsetChange`'s bisection below converge on essentially a random sub-second point
 * near the transition instead of the transition itself.
 */
function offsetMs(epochMs: number, tz: string): number {
  const flooredSecondMs = Math.floor(epochMs / 1000) * 1000;
  const subSecondMs = epochMs - flooredSecondMs;
  const wallClock = wallClockPartsAt(flooredSecondMs, tz);
  const asIfUtc =
    Date.UTC(
      wallClock.year,
      wallClock.month - 1,
      wallClock.day,
      wallClock.hour,
      wallClock.minute,
      wallClock.second,
    ) + subSecondMs;
  return asIfUtc - epochMs;
}

/**
 * Bisects `[lo, hi]` for the first instant whose `tz` offset differs from `lo`'s, to
 * millisecond precision. Requires the offset at `lo` and `hi` to already differ — every
 * caller here has just confirmed that via `offsetBeforeDate !== offsetAfterDate`.
 */
function findOffsetChange(lo: number, hi: number, tz: string): number {
  const offsetAtLo = offsetMs(lo, tz);
  while (hi - lo > 1) {
    const mid = Math.floor((lo + hi) / 2);
    if (offsetMs(mid, tz) === offsetAtLo) {
      lo = mid;
    } else {
      hi = mid;
    }
  }
  return hi;
}

function wallClockPartsAt(
  epochMs: number,
  tz: string,
): {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
} {
  const formatted = new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    hourCycle: "h23",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).formatToParts(new Date(epochMs));

  const part = (type: string) =>
    formatted.find((p) => p.type === type)?.value ?? "0";
  return {
    year: Number(part("year")),
    month: Number(part("month")),
    day: Number(part("day")),
    // `h23` still renders midnight as "24" in some ICU builds; normalise it.
    hour: Number(part("hour")) % 24,
    minute: Number(part("minute")),
    second: Number(part("second")),
  };
}

