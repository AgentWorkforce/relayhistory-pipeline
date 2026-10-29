/**
 * Keyed telemetry ids: machine and session ids never leave the platform raw.
 *
 * Anything that reaches external telemetry (Worker log lines and their
 * PostHog export, OTLP and Sentry error reports) carries `trace_id` and the
 * tenant (`org_id`, `workspace_id`) raw, and the machine and session ids as
 * `h1_<HMAC-SHA256(key, "<kind>:<id>")>` truncated to 128 bits. The hash is
 * deterministic, so one machine or session is the same token in every tool and
 * every line, and a whole session stays traceable to its customer through the
 * raw `trace_id` + `org_id`. The raw ids live only in service-role storage
 * (`pipeline_events`, `pipeline_jobs`, `pipeline_dead_letters`, queue messages),
 * next to the same `trace_id`.
 *
 * Operators map between the two with the key: the ops route
 * `POST /v1/internal/pipeline/telemetry-ids` hashes a known raw id so its
 * telemetry can be searched, and a hashed line's `trace_id` leads back to the
 * raw ids in `pipeline_events`.
 *
 * The key is `PIPELINE_TELEMETRY_ID_KEY`, a Worker secret (at least 32
 * characters), never in the repo or a config file. Without one, machine and
 * session ids are left out of telemetry entirely: fail closed. Rotating the key
 * changes every token, so searches across a rotation need both keys.
 */
import { createHmac } from "node:crypto";
import type { TraceContext } from "./trace.js";

export const TELEMETRY_ID_PREFIX = "h1_";
/** Shorter keys are refused (treated as unset), so a placeholder never hashes. */
export const MIN_TELEMETRY_ID_KEY_LENGTH = 32;

export type TelemetryIdKind = "machine" | "session";

export function telemetryIdKey(value: string | undefined): string | undefined {
  return value && value.length >= MIN_TELEMETRY_ID_KEY_LENGTH
    ? value
    : undefined;
}

/** The token for one id. The kind separates domains: a machine and a session with one id differ. */
export function telemetryIdHash(
  key: string,
  kind: TelemetryIdKind,
  value: string,
): string {
  return `${TELEMETRY_ID_PREFIX}${createHmac("sha256", key)
    .update(`${kind}:${value}`)
    .digest("hex")
    .slice(0, 32)}`;
}

/**
 * The context as telemetry may carry it: machine and session ids replaced by
 * their tokens with a key, dropped without one. Trace, tenant and harness stay.
 */
export function telemetryContext(
  ctx: TraceContext,
  key: string | undefined,
): TraceContext {
  const { machineId, sessionId, ...rest } = ctx;
  if (!key) return rest;
  return {
    ...rest,
    ...(machineId
      ? { machineId: telemetryIdHash(key, "machine", machineId) }
      : {}),
    ...(sessionId
      ? { sessionId: telemetryIdHash(key, "session", sessionId) }
      : {}),
  };
}

/** A `m=` or `s=` path segment of a canonical object key (`.../m=<id>/.../s=<id>/...`). */
const KEY_ID_SEGMENT = /(^|\/)(m|s)=([^/@]+)/g;
const SEGMENT_KIND = { m: "machine", s: "session" } as const;

/**
 * A string as telemetry may carry it: every machine (`m=`) and session (`s=`)
 * segment of an object key in it replaced by that id's token, or by `-`
 * without a key. Work keys, dead letter ids and output keys embed the object
 * key, so the raw ids would otherwise ride along in attributes.
 */
export function telemetryString(value: string, key: string | undefined) {
  return value.replace(
    KEY_ID_SEGMENT,
    (_match, lead: string, segment: "m" | "s", encoded: string) => {
      if (!key) return `${lead}${segment}=-`;
      let id = encoded;
      try {
        id = decodeURIComponent(encoded);
      } catch {
        // A malformed escape is hashed as written.
      }
      return `${lead}${segment}=${telemetryIdHash(key, SEGMENT_KIND[segment], id)}`;
    },
  );
}

/** Log fields as telemetry may carry them: `telemetryString` on every string value. */
export function telemetryFields<T extends Record<string, unknown>>(
  fields: T,
  key: string | undefined,
): T {
  const out: Record<string, unknown> = {};
  for (const [name, value] of Object.entries(fields))
    out[name] = typeof value === "string" ? telemetryString(value, key) : value;
  return out as T;
}
