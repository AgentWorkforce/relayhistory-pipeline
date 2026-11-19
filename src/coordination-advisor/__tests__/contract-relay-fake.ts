/**
 * A recording relay sender for the advisor tests that models the Relaycast delivery contract
 * (docs/session-intel-dm-transport.md on the spec branch, "Advisor rule"): an advisory counts as delivered when
 * its delivery state is in the SET {delivered, consumed, replied}, or accepted_at is set. There is no ordered
 * chain to rely on: a delivery can be `failed` with accepted_at kept (expiry after acceptance), and a later
 * `failed` never undoes a delivery. relay-sender.ts is not changed: this fake implements its RelaySender
 * interface, resolving `send` only for a delivered outcome, so the scan marks only those rows delivered and
 * leaves the rest pending on the pull path.
 */
import type { RelayDm, RelaySender } from "../relay-sender.ts";

export type DeliveryState = "queued" | "delivered" | "consumed" | "replied" | "failed";
export interface DeliveryOutcome { state: DeliveryState | null; acceptedAt: string | null }

/** The contract's delivered set. */
export const DELIVERED_STATES: ReadonlySet<DeliveryState> = new Set(["delivered", "consumed", "replied"]);
/** The advisor rule: in the set, or accepted (whatever the state now says). A null delivery (mailbox full) is not delivered. */
export const isDelivered = (o: DeliveryOutcome) => (o.state !== null && DELIVERED_STATES.has(o.state)) || o.acceptedAt !== null;

export class ContractRelayFake implements RelaySender {
  readonly sent: Array<RelayDm & { outcome: DeliveryOutcome }> = [];
  /** The outcome of the next sends; default delivered. */
  outcome: DeliveryOutcome = { state: "delivered", acceptedAt: null };
  /** Recipients with no agent (Cloud has none for the session): not attached, nothing is sent. */
  readonly unattached = new Set<string>();
  async send(dm: RelayDm): Promise<void> {
    if (this.unattached.has(`${dm.to.harness}:${dm.to.sessionId}`)) throw Object.assign(new Error("agent_not_found"), { status: 404 });
    const outcome = { ...this.outcome };
    this.sent.push({ ...dm, outcome });
    if (!isDelivered(outcome)) throw new Error(`not delivered (${outcome.state ?? "no delivery"})`);
  }
}
