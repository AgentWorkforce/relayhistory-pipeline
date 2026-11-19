/**
 * The advisory message (docs/coordination-live-advisories.md, "Push: HIGH by Relay DM"). Shared by
 * the worker's DM and the read API's `message` field. Its variable content is ids, timestamps,
 * kinds and artifacts (paths, migration names, commit shas): never transcript text. The conflict kinds
 * (M2-4) may also carry claim ids and claim excerpts; an excerpt is passed only after the scrubber left it
 * unchanged (excerpts.ts), and is cut here to 200 characters. Hard bound: ADVISORY_MESSAGE_MAX characters.
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
  /** Conflict kinds only: the cited claim ids. */
  claimIds?: string[];
  /** Conflict kinds only: scrub-clean claim excerpts (excerpts.ts), at most 200 characters each. */
  excerpts?: MessageExcerpt[];
}

export interface MessageExcerpt { label: string; text: string }
export const EXCERPT_MESSAGE_MAX = 200;

/** The conflict kinds' heads and verbs (M2-4). */
const CONFLICT_TEXT: Record<string, { head: string; verb: string }> = {
  constraint_violation: { head: "possible constraint violation.", verb: "may be acting against a constraint on" },
  intent_conflict: { head: "possible conflicting plans.", verb: "have conflicting claims about" },
  overlap: { head: "possible nearby work.", verb: "may be working near" },
};

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
  if (Object.hasOwn(CONFLICT_TEXT, m.kind)) return conflictMessage(m);
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

/**
 * A conflict kind's message: the fixed lines, then the claim ids and the excerpts while they fit. The ids and
 * excerpts are optional extras: the read API, which has neither, gets the same message without them.
 */
function conflictMessage(m: AdvisoryMessageInput): string {
  const t = CONFLICT_TEXT[m.kind]!;
  const self = `${clip(m.harness, 32)}:${clip(m.sessionId, 96)}`;
  const peer = `${clip(m.peerHarness, 32)}:${clip(m.peerSessionId, 96)}`;
  const high = m.urgency === "high";
  const ask = high ? " Please check with the other agent before continuing." : "";
  const build = (artifact: string) => [
    `${ADVISORY_SENDER}: ${t.head}`,
    `Your session ${self} and ${peer} ${t.verb} ${artifact}`,
    `(${clip(m.kind, 64)}, first seen ${clip(m.firstSeen, 40)}).${ask}`,
    `Advisory ${clip(m.advisoryId, 64)}; ack it at the end of your turn.`,
  ];
  const fixed = build("").join("\n").length;
  const lines = build(clip(m.artifact, Math.max(8, Math.min(200, ADVISORY_MESSAGE_MAX - fixed))));
  const fits = (extra: string) => [...lines, extra].join("\n").length <= ADVISORY_MESSAGE_MAX;
  const ids = (m.claimIds ?? []).slice(0, 4).map((x) => clip(x, 64));
  if (ids.length && fits(`Claims: ${ids.join(", ")}`)) lines.push(`Claims: ${ids.join(", ")}`);
  for (const e of (m.excerpts ?? []).slice(0, 4)) {
    const label = clip(e.label, 16);
    const room = ADVISORY_MESSAGE_MAX - [...lines, `${label}: ""`].join("\n").length;
    if (room < 24) break;
    const line = `${label}: "${clip(e.text, Math.min(EXCERPT_MESSAGE_MAX, room))}"`;
    if (fits(line)) lines.push(line);
  }
  const text = lines.join("\n");
  return text.length <= ADVISORY_MESSAGE_MAX ? text : text.slice(0, ADVISORY_MESSAGE_MAX);
}
