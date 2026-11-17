/**
 * The HIGH advisory push (docs/coordination-live-advisories.md, "Push: HIGH by Relay DM"). The worker
 * marks a row delivered only after `send` resolves; a rejected send leaves it pending for the pull.
 */
import { ADVISORY_MESSAGE_MAX, ADVISORY_SENDER } from "./template.ts";

export interface RelayDm {
  /** The recipient's tenant: a session id alone is not unique across workspaces. */
  orgId: string;
  workspaceId: string;
  to: { harness: string; sessionId: string };
  text: string;
  advisoryId: string;
}

export interface RelaySender {
  send(dm: RelayDm): Promise<void>;
}

/** Test and dry-run sender: records every DM; `fail` makes the next sends reject. */
export class InMemoryRelaySender implements RelaySender {
  readonly sent: RelayDm[] = [];
  fail = false;
  async send(dm: RelayDm): Promise<void> {
    if (this.fail) throw new Error("relay send failed");
    this.sent.push(dm);
  }
}

const loopback = (host: string) => host === "localhost" || host === "127.0.0.1" || host === "[::1]";

/**
 * UNVERIFIED: a minimal HTTP sender. It POSTs one JSON DM to COORDINATION_ADVISOR_RELAY_URL with the
 * bearer token. The payload names the recipient's org and workspace with the session. The Relay
 * endpoint and payload shape have not been verified against a live relay; keep
 * COORDINATION_ADVISOR_RELAY_DM_ENABLED off until they are. https is required except on loopback, and a
 * redirect is an error, so the bearer never follows one. The token is never logged or put in an error
 * message, and the response body is discarded unread.
 */
export function httpRelaySender(opts: { url: string; token: string; fetch?: typeof fetch; timeoutMs?: number }): RelaySender {
  const url = new URL(opts.url);
  if (url.protocol !== "https:" && !(url.protocol === "http:" && loopback(url.hostname)))
    throw new Error("coordination advisor: COORDINATION_ADVISOR_RELAY_URL must be https (http only on loopback)");
  const doFetch = opts.fetch ?? fetch;
  return {
    async send(dm) {
      if (dm.text.length > ADVISORY_MESSAGE_MAX) throw new Error("coordination advisor: DM exceeds the message bound");
      const res = await doFetch(url, {
        method: "POST",
        headers: { "content-type": "application/json", authorization: `Bearer ${opts.token}` },
        body: JSON.stringify({
          from: ADVISORY_SENDER, orgId: dm.orgId, workspaceId: dm.workspaceId, to: dm.to, text: dm.text, advisoryId: dm.advisoryId,
        }),
        redirect: "error",
        signal: AbortSignal.timeout(opts.timeoutMs ?? 10_000),
      });
      await res.body?.cancel().catch(() => {});
      if (!res.ok) throw new Error(`coordination advisor: relay send answered ${res.status}`);
    },
  };
}
