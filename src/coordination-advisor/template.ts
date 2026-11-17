/**
 * The advisory message (docs/coordination-live-advisories.md, "Push: HIGH by Relay DM"). Shared by
 * the worker's DM and the read API's `message` field. Its only variable content is ids, timestamps,
 * kinds and artifacts (paths, migration names, commit shas): never transcript text. Hard bound:
 * ADVISORY_MESSAGE_MAX characters.
 */
export const ADVISORY_MESSAGE_MAX = 600;
export const ADVISORY_SENDER = "relayhistory-coordination";

export interface AdvisoryMessageInput {
  advisoryId: string;
  urgency: "high" | "low";
  kind: string;
  artifact: string;
  harness: string;
  sessionId: string;
  peerHarness: string;
  peerSessionId: string;
  /** ISO timestamp: the earliest evidence, else the row's created_at. */
  firstSeen: string;
}

/** Fixed per kind for HIGH; LOW always "may be working near". */
const HIGH_VERB: Record<string, string> = {
  duplicate_work: "wrote migration",
  superseded_work: "reverted or were reverted on",
  concurrent_edit: "wrote",
};

// One line per field: control characters (newlines) cannot forge template lines.
const flat = (value: string) => value.replace(/[\u0000-\u001f\u007f]+/g, " ").trim();
function clip(value: string, max: number): string {
  const v = flat(value);
  return v.length <= max ? v : `${v.slice(0, Math.max(0, max - 3))}...`;
}

export function advisoryMessage(m: AdvisoryMessageInput): string {
  const self = `${clip(m.harness, 32)}:${clip(m.sessionId, 96)}`;
  const peer = `${clip(m.peerHarness, 32)}:${clip(m.peerSessionId, 96)}`;
  const kind = clip(m.kind, 64);
  const ts = clip(m.firstSeen, 40);
  const id = clip(m.advisoryId, 64);
  const high = m.urgency === "high";
  const head = high ? `${ADVISORY_SENDER}: possible duplicate work.` : `${ADVISORY_SENDER}: possible nearby work.`;
  const verb = high ? `both ${HIGH_VERB[m.kind] ?? "wrote"}` : "may be working near";
  const ask = high ? " Please check with the other agent before continuing." : "";
  const build = (artifact: string) => [
    head,
    `Your session ${self} and ${peer} ${verb} ${artifact}`,
    `(${kind}, first seen ${ts}).${ask}`,
    `Advisory ${id}; ack it at the end of your turn.`,
  ].join("\n");
  const fixed = build("").length;
  const text = build(clip(m.artifact, Math.max(8, ADVISORY_MESSAGE_MAX - fixed)));
  return text.length <= ADVISORY_MESSAGE_MAX ? text : text.slice(0, ADVISORY_MESSAGE_MAX);
}
