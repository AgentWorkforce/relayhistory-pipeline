/** Raw field-by-field UTF-8 order for NEW audit identities and RPC cursors.
 * Existing planner output ordering is a different frozen contract.
 * The caller still validates identifier eligibility; this compares scalar bytes.
 */
export function frontierUtf8TupleCompare(a: readonly string[], b: readonly string[]): number {
  const scalar = (s: unknown): s is string => typeof s === "string" && !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(s);
  if (![...a, ...b].every(scalar)) throw new Error("frontier_invalid_utf8_tuple");
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    const c = Buffer.compare(Buffer.from(a[i]!), Buffer.from(b[i]!));
    if (c) return c;
  }
  return a.length - b.length;
}
