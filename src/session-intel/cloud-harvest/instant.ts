/**
 * The one parser for admitted times (harvest windows and deadlines, bench scopes, batch deadlines): a canonical
 * ISO-8601 UTC instant, `YYYY-MM-DDTHH:MM:SS[.fraction]Z`, whose calendar fields are real and whose precision is
 * at most a millisecond. `Date.parse` rolls impossible dates over (February 30 becomes March 2, 24:00 the next day)
 * and silently drops sub-millisecond digits; here both are refused, so an admitted bound is exactly what was
 * written. Event times, which may carry micro- or nanoseconds, are compared at full precision by `eventNanos`.
 */
const CANONICAL = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$/;

function fields(m: RegExpExecArray): number | null {
  const [y, mo, d, h, mi, s] = m.slice(1, 7).map(Number) as [number, number, number, number, number, number];
  const t = Date.UTC(y, mo - 1, d, h, mi, s);
  const back = new Date(t);
  // Any rollover (day or month overflow, hour 24, minute or second 60) changes a field.
  if (back.getUTCFullYear() !== y || back.getUTCMonth() !== mo - 1 || back.getUTCDate() !== d
    || back.getUTCHours() !== h || back.getUTCMinutes() !== mi || back.getUTCSeconds() !== s) return null;
  return t;
}

/**
 * Milliseconds since the epoch, or NaN when `v` is not a canonical, real UTC instant of at most millisecond
 * precision (digits past the millisecond must be zeros).
 */
export function parseInstant(v: unknown): number {
  if (typeof v !== "string") return NaN;
  const m = CANONICAL.exec(v);
  if (!m) return NaN;
  const frac = m[7] ?? "";
  if (/[1-9]/.test(frac.slice(3))) return NaN;
  const t = fields(m);
  return t === null ? NaN : t + Number(frac.padEnd(3, "0").slice(0, 3));
}

/** `parseInstant`, throwing with `what` when the value is not admitted. */
export function instantOf(v: unknown, what: string): number {
  const t = parseInstant(v);
  if (!Number.isFinite(t)) throw new Error(`${what} is not a canonical UTC timestamp with a real calendar date and at most millisecond precision`);
  return t;
}

const EVENT = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/;

/**
 * An event time in nanoseconds since the epoch, at its full precision (up to nine fractional digits, `Z` or a
 * `±HH:MM` offset), or null when it is not such a time (the caller treats null as outside any window).
 */
export function eventNanos(v: unknown): bigint | null {
  if (typeof v !== "string") return null;
  const m = EVENT.exec(v);
  if (!m) return null;
  const t = fields(m as RegExpExecArray);
  if (t === null) return null;
  let ns = BigInt(t) * 1_000_000n + BigInt((m[7] ?? "").padEnd(9, "0"));
  if (m[8] !== "Z") {
    const sign = m[8]![0] === "-" ? -1n : 1n, [oh, om] = m[8]!.slice(1).split(":").map(Number) as [number, number];
    if (oh > 23 || om > 59) return null;
    ns -= sign * BigInt(oh * 3600 + om * 60) * 1_000_000_000n;
  }
  return ns;
}

/**
 * An event time as one fixed-width UTC string, `YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ`, at its full precision, or null
 * when `eventNanos` rejects it. Every time the bench compares (event facts, directory rows, forge PRs and their
 * histories) goes through this, so string order is time order: mixing widths (`…00Z`, `…00.000Z`, nine digits)
 * would sort a later instant first, and normalizing through `Date` would drop sub-millisecond order.
 */
export function canonicalTs(v: unknown): string | null {
  const ns = eventNanos(v);
  if (ns === null || ns < 0n) return null;
  const s = ns / 1_000_000_000n, frac = ns % 1_000_000_000n;
  const iso = new Date(Number(s) * 1000).toISOString();
  if (!/^\d{4}-/.test(iso)) return null;
  return `${iso.slice(0, 19)}.${frac.toString().padStart(9, "0")}Z`;
}
