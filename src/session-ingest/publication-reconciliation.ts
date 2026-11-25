// Receipt inventory schedules rechecks. A scan position is never publication authority.
export interface ReceiptPosition {
  /** Immutable outbox creation time formatted in UTC with exactly six fractional digits. */
  createdAt: string;
  /** Immutable stage intent key, used with that time as a unique scan position. */
  stageToken: string;
}

export interface ReconciliationState {
  lane: "catchup" | "tail";
  catchup: { after: ReceiptPosition | null; through: ReceiptPosition | null };
  tailAfter: ReceiptPosition | null;
}

export const initialReconciliationState = (): ReconciliationState => ({
  lane: "catchup", catchup: { after: null, through: null }, tailAfter: null,
});

type ReceiptOutcome = "published" | "already_published" | "held" | "missing";

/** All three operations must use the same immutable, authorized target/scope. */
export interface ReconciliationSource {
  highest(): Promise<ReceiptPosition | null>;
  /** Complete ordered keyset page, after < position <= through. Empty means EOF. */
  page(after: ReceiptPosition | null, through: ReceiptPosition, limit: number): Promise<ReceiptPosition[]>;
  /** Re-read current eligibility, privacy and authority inside the normal idempotent claim/publish path. */
  reconcile(stageToken: string): Promise<ReceiptOutcome>;
}

function checkPosition(p: ReceiptPosition) {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/.test(p.createdAt) ||
      !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(p.stageToken))
    throw Error("invalid receipt position");
}

function compare(a: ReceiptPosition, b: ReceiptPosition) {
  if (a.createdAt !== b.createdAt) return a.createdAt < b.createdAt ? -1 : 1;
  return a.stageToken < b.stageToken ? -1 : Number(a.stageToken > b.stageToken);
}

/** One bounded page. Persist the returned state with a scoped CAS after success.
 * Retrying a lost page/checkpoint can repeat every call; reconciliation must be idempotent.
 * The catch-up high bound stays fixed through EOF, then scanning starts at the beginning.
 * This revisits late commits and held rows that have since acquired valid authority. */
export async function reconcilePublicationPage(
  state: ReconciliationState, source: ReconciliationSource, limit: number,
) {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 20) throw Error("invalid receipt limit");
  if (state.lane !== "catchup" && state.lane !== "tail") throw Error("invalid receipt lane");
  const { after: prior, through: upper } = state.catchup;
  if (prior) checkPosition(prior);
  if (upper) checkPosition(upper);
  if (state.tailAfter) checkPosition(state.tailAfter);
  if (prior && (!upper || compare(prior, upper) > 0)) throw Error("invalid receipt checkpoint");
  const catchup = state.lane === "catchup";
  const next = { ...state, lane: catchup ? "tail" as const : "catchup" as const };
  const counts = { published: 0, already_published: 0, held: 0, missing: 0 };
  const after = catchup ? state.catchup.after : state.tailAfter;
  const through = (catchup && state.catchup.through) || await source.highest();
  if (through) checkPosition(through);
  if (!through) return { next, counts };
  // On initial catch-up, reserve the tail lane for arrivals beyond this snapshot's upper key.
  if (catchup && !next.tailAfter) next.tailAfter = through;
  if (!catchup && after && compare(after, through) >= 0) return { next, counts };
  const page = await source.page(after, through, limit);
  if (page.length > limit) throw Error("receipt page exceeds budget");
  let last = after;
  for (const row of page) {
    checkPosition(row);
    if ((last && compare(last, row) >= 0) || compare(row, through) > 0)
      throw Error("receipt page outside scan range");
    last = row;
  }
  for (const row of page) {
    const result = await source.reconcile(row.stageToken);
    if (!Object.hasOwn(counts, result)) throw Error("unknown receipt outcome");
    counts[result]++;
  }
  if (catchup) next.catchup = page.length ? { after: last, through } : { after: null, through: null };
  else next.tailAfter = last;
  return { next, counts };
}
