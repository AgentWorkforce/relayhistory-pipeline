import { MODEL_PATTERN } from "../session-brief/config.ts";

/** Operator-controlled scope and spend limits; invocation bodies cannot enlarge them. */
export function coordinationConfig(env: NodeJS.ProcessEnv = process.env) {
  if (env.COORDINATION_REPORT_ENABLED !== "true") return null;
  const identifier = (name: string) => {
    const value = env[name];
    if (!value || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(value))
      throw new Error(`coordination_config_invalid_${name}`);
    return value;
  };
  const limit = (name: string, fallback: number, max: number) => {
    const value = env[name];
    if (value === undefined) return fallback;
    if (!/^[1-9][0-9]{0,5}$/.test(value) || Number(value) > max)
      throw new Error(`coordination_config_invalid_${name}`);
    return Number(value);
  };
  const model = env.COORDINATION_REPORT_MODEL ?? "gpt-6.1-sol";
  if (!MODEL_PATTERN.test(model))
    throw new Error("coordination_config_invalid_model");
  return {
    scope: {
      organizationId: identifier("COORDINATION_REPORT_ORG_ID"),
      workspaceId: identifier("COORDINATION_REPORT_WORKSPACE_ID"),
    },
    model,
    maxSessions: limit("COORDINATION_REPORT_MAX_SESSIONS", 500, 1000),
    maxEvents: limit("COORDINATION_REPORT_MAX_EVENTS", 10000, 10000),
    maxPackets: limit("COORDINATION_REPORT_MAX_PACKETS", 12, 24),
    maxCalls: limit("COORDINATION_REPORT_MAX_CALLS", 12, 24),
    concurrency: limit("COORDINATION_REPORT_CONCURRENCY", 4, 4),
  };
}

export function parseCoordinationInvocation(value: unknown, now = new Date()) {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("coordination_request_invalid");
  const v = value as Record<string, unknown>;
  if (
    Object.keys(v).sort().join(",") !== "idempotencyKey,since,until" ||
    typeof v.idempotencyKey !== "string" ||
    !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(v.idempotencyKey)
  )
    throw new Error("coordination_request_invalid");
  const timestamp = (t: unknown): t is string =>
    typeof t === "string" &&
    Number.isFinite(Date.parse(t)) &&
    new Date(t).toISOString() === t;
  if (
    !timestamp(v.since) ||
    !timestamp(v.until) ||
    v.since >= v.until ||
    Date.parse(v.until) > now.getTime() ||
    Date.parse(v.until) - Date.parse(v.since) > 31 * 86400000
  )
    throw new Error("coordination_request_window_invalid");
  return {
    idempotencyKey: v.idempotencyKey,
    window: { since: v.since, until: v.until },
  };
}
