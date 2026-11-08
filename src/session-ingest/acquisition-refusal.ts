import { DeliveryError } from "./vendor/delivery/delivery-contracts.ts";
/**
 * The writer-facing acquisition refusal (docs/factory-acquisition-owner-design.md §5.6;
 * runbook docs/ingest-acquisition-owner.md).
 *
 * The ledger's admission (`ingest_acquisition_admit`) and accepted-stage recheck
 * (`ingest_acquisition_fenced`) reject a whole batch by raising
 * `acquisition: <code>` with DETAIL = the JSON array `[{source,sessionId,code}]`
 * of every refused session. The edge answers it as HTTP 409
 * `{error:{code:'acquisition_refused',...},refused:[{source,sessionId,code}]}`:
 * never `delivery_conflict` (quarantines, moves the cursor), 400/422 (bisection
 * suppresses records), 401/403/412/423 (stalls the install), 404/410, 429 or a
 * partial 2xx. Today's Desktop retries this 409 as transient.
 *
 * DORMANT: the database raises these only for an enrolled (org, workspace, source),
 * and no source is enrolled; `ACQUISITION_ENROLLED_SOURCES` is empty.
 *
 * Parsing is closed: only our own exact message, an expected SQLSTATE, and a
 * well-formed, bounded DETAIL become a 409. Anything else stays the ledger's 503.
 * The refused list echoes only the caller's own (source, sessionId) pairs; it is
 * returned to the caller and never logged.
 */

/** The sources whose records Desktop must hold until it has a grant (GET /v1/delivery/limits).
 * Empty: enrollment is an owner step behind Khaliq's explicit OK and ships with Desktop's P2. */
export const ACQUISITION_ENROLLED_SOURCES: readonly string[] = Object.freeze([]);

/** Refusal code -> the SQLSTATE the database raises it with (`ingest_acquisition_refuse`). */
const REFUSAL_SQLSTATE: ReadonlyMap<string, string> = new Map([
  ["owner_unknown", "55000"],
  ["owner_draining", "55000"],
  ["owner_released", "55000"],
  ["owner_other_principal", "55000"],
  ["grant_expired", "55000"],
  ["owner_changed", "55000"],
  ["not_owner", "42501"],
  ["session_required", "22023"],
]);

export const ACQUISITION_REFUSAL_CODES: readonly string[] = Object.freeze([...REFUSAL_SQLSTATE.keys()]);

export interface AcquisitionRefusal { readonly source: string; readonly sessionId: string; readonly code: string }

/** A batch-wide refusal; nothing of the batch was accepted. */
export class AcquisitionRefusedError extends DeliveryError {
  constructor(readonly refused: readonly AcquisitionRefusal[]) {
    super("acquisition_refused", 409, "Delivery refused until this machine owns the named sessions");
  }
}

const MESSAGE = /^acquisition: ([a-z_]{1,64})$/;
/** The admission accepts at most 500 sessions per batch; DETAIL is bounded accordingly. */
const MAX_REFUSED = 1000;
const MAX_DETAIL_BYTES = 1 << 20;
const MAX_FIELD = 4096;

function read(target: unknown, key: "code" | "message" | "detail"): string | undefined {
  try {
    if (target === null || (typeof target !== "object" && typeof target !== "function")) return undefined;
    const v: unknown = (target as Record<string, unknown>)[key];
    return typeof v === "string" ? v : undefined;
  } catch {
    return undefined;
  }
}

/** The 409 for a database acquisition refusal, or undefined (the caller keeps its 503 path). Never throws. */
export function acquisitionRefusalFrom(error: unknown): AcquisitionRefusedError | undefined {
  try {
    const m = MESSAGE.exec(read(error, "message") ?? "");
    const first = m?.[1];
    if (first === undefined || REFUSAL_SQLSTATE.get(first) !== read(error, "code")) return undefined;
    const detail = read(error, "detail");
    if (detail === undefined || detail.length > MAX_DETAIL_BYTES) return undefined;
    const parsed: unknown = JSON.parse(detail);
    if (!Array.isArray(parsed) || parsed.length === 0 || parsed.length > MAX_REFUSED) return undefined;
    const refused: AcquisitionRefusal[] = [];
    for (const item of parsed) {
      if (item === null || typeof item !== "object" || Array.isArray(item)) return undefined;
      const keys = Object.keys(item).sort();
      if (keys.length !== 3 || keys[0] !== "code" || keys[1] !== "sessionId" || keys[2] !== "source") return undefined;
      const { source, sessionId, code } = item as Record<string, unknown>;
      if (typeof source !== "string" || typeof sessionId !== "string" || typeof code !== "string") return undefined;
      if (source === "" || source.length > MAX_FIELD || sessionId.length > MAX_FIELD || !REFUSAL_SQLSTATE.has(code)) return undefined;
      // Only session_required names a source without a session.
      if ((sessionId === "") !== (code === "session_required")) return undefined;
      refused.push(Object.freeze({ source, sessionId, code }));
    }
    if (refused[0]!.code !== first) return undefined;
    return new AcquisitionRefusedError(Object.freeze(refused));
  } catch {
    return undefined;
  }
}
