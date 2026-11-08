/**
 * Opaque event-page cursors for the session read API.
 *
 * Namespaced `b96e1:` so a caller (Cloud) can keep pagination sticky: a cursor
 * from this service is never sent to relayhistory, whose reader treats an
 * unknown cursor as "first page" (a silent restart, so duplicated results), and
 * a relayhistory cursor (`events-v2:` or the legacy `<iso>|<id>`) is refused
 * here as `foreign_cursor` instead of being guessed at.
 *
 * The position is the full ClickHouse sort-key suffix after the tenant columns
 * (ts in epoch milliseconds, then session_id, source, machine_id, kind,
 * event_id), so a page boundary is exact even inside one millisecond.
 */

export const CURSOR_PREFIX = "b96e1:";

export interface EventPosition {
  tsMs: number;
  sessionId: string;
  source: string;
  machineId: string;
  kind: string;
  eventId: string;
}

export class CursorError extends Error {
  constructor(readonly code: "invalid_cursor" | "foreign_cursor") {
    super(code === "foreign_cursor" ? "cursor was issued by another backend" : "cursor is not valid");
    this.name = "CursorError";
  }
}

const MAX_CURSOR_CHARS = 4096;

export function encodeCursor(p: EventPosition): string {
  const body = JSON.stringify([p.tsMs, p.sessionId, p.source, p.machineId, p.kind, p.eventId]);
  return CURSOR_PREFIX + Buffer.from(body, "utf8").toString("base64url");
}

/**
 * The owner-buffered events backend's marker (owner-buffered.ts readOwnerBufferedEvents): a
 * buffered page's cursor is the same `b96e1:` wire with the same six-field position plus this
 * seventh element. The position (identity and order) is the canonical one, but a buffered
 * cursor is never continued on canonical rows, nor a canonical cursor on buffered rows: the
 * events route refuses that backend transition (409 cursor_backend_transition) instead of
 * restarting, skipping, or continuing into a canonical transcript whose completeness against
 * the buffered set it cannot know. decodeCursor refuses a seven-element cursor (invalid_cursor)
 * exactly as before; only decodeBackendCursor, called only when the owner-buffered events
 * branch is eligible, reads the marker.
 */
export const BUFFERED_CURSOR_MARKER = "ob1";

export function encodeBufferedCursor(p: EventPosition): string {
  const body = JSON.stringify([p.tsMs, p.sessionId, p.source, p.machineId, p.kind, p.eventId, BUFFERED_CURSOR_MARKER]);
  return CURSOR_PREFIX + Buffer.from(body, "utf8").toString("base64url");
}

/** `undefined` for no cursor. Throws CursorError for a foreign or malformed one. */
export function decodeCursor(raw: string | undefined | null): EventPosition | undefined {
  return decodeWith(raw, false)?.position;
}

/**
 * decodeCursor that also accepts a buffered cursor and says which backend issued it.
 * `undefined` for no cursor; CursorError for a foreign or malformed one.
 */
export function decodeBackendCursor(raw: string | undefined | null): { backend: "canonical" | "buffered"; position: EventPosition } | undefined {
  return decodeWith(raw, true);
}

function decodeWith(raw: string | undefined | null, buffered: boolean): { backend: "canonical" | "buffered"; position: EventPosition } | undefined {
  if (raw === undefined || raw === null || raw === "") return undefined;
  if (!raw.startsWith(CURSOR_PREFIX)) throw new CursorError("foreign_cursor");
  if (raw.length > MAX_CURSOR_CHARS) throw new CursorError("invalid_cursor");
  let value: unknown;
  try {
    value = JSON.parse(Buffer.from(raw.slice(CURSOR_PREFIX.length), "base64url").toString("utf8"));
  } catch {
    throw new CursorError("invalid_cursor");
  }
  const marked = buffered && Array.isArray(value) && value.length === 7 && value[6] === BUFFERED_CURSOR_MARKER;
  if (!Array.isArray(value) || value.length !== (marked ? 7 : 6)) throw new CursorError("invalid_cursor");
  const [tsMs, sessionId, source, machineId, kind, eventId] = value as unknown[];
  if (typeof tsMs !== "number" || !Number.isSafeInteger(tsMs) || tsMs < 0) throw new CursorError("invalid_cursor");
  for (const s of [sessionId, source, machineId, kind, eventId])
    if (typeof s !== "string" || s.length > 1024) throw new CursorError("invalid_cursor");
  if (!sessionId || !source || !eventId) throw new CursorError("invalid_cursor");
  return {
    backend: marked ? "buffered" : "canonical",
    position: { tsMs, sessionId: sessionId as string, source: source as string, machineId: machineId as string, kind: kind as string, eventId: eventId as string },
  };
}
