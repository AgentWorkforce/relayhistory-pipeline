/**
 * The one parser for admitted times (harvest windows and deadlines, bench scopes, batch deadlines): a canonical
 * ISO-8601 UTC instant, `YYYY-MM-DDTHH:MM:SS[.fraction]Z`, whose calendar fields are real. `Date.parse` rolls
 * impossible dates over (February 30 becomes March 2, 24:00 becomes the next day); here they are refused.
 */
const CANONICAL = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$/;

/** Milliseconds since the epoch, or NaN when `v` is not a canonical, real UTC instant. */
export function parseInstant(v: unknown): number {
  if (typeof v !== "string") return NaN;
  const m = CANONICAL.exec(v);
  if (!m) return NaN;
  const [y, mo, d, h, mi, s] = m.slice(1, 7).map(Number) as [number, number, number, number, number, number];
  const ms = m[7] ? Number(m[7].padEnd(3, "0").slice(0, 3)) : 0;
  const t = Date.UTC(y, mo - 1, d, h, mi, s, ms);
  const back = new Date(t);
  // Any rollover (day or month overflow, hour 24, minute or second 60) changes a field.
  if (back.getUTCFullYear() !== y || back.getUTCMonth() !== mo - 1 || back.getUTCDate() !== d
    || back.getUTCHours() !== h || back.getUTCMinutes() !== mi || back.getUTCSeconds() !== s) return NaN;
  return t;
}

/** `parseInstant`, throwing with `what` when the value is not admitted. */
export function instantOf(v: unknown, what: string): number {
  const t = parseInstant(v);
  if (!Number.isFinite(t)) throw new Error(`${what} is not a canonical UTC timestamp with a real calendar date`);
  return t;
}
