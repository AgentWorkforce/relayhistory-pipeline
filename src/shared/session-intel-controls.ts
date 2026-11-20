/** Default-deny, uncached controls for optional intelligence; never a base-summary gate. */
import type { SqlQuery } from "../session-read-api/liveness.ts";
import { log } from "./log.ts";

export const SESSION_INTEL_FEATURES = ["advisor", "conflicts", "dm", "claims", "regen"] as const;
export type SessionIntelFeature = typeof SESSION_INTEL_FEATURES[number];
export type SessionIntelControls = ReadonlyMap<SessionIntelFeature, boolean>;
export const CONTROLS_SQL = "SELECT feature, enabled FROM sessions.session_intel_controls_v1($1,$2)";
export const CONTROLS_TIMEOUT_MS = 2_000;
export const controlsOff = (): SessionIntelControls => new Map(SESSION_INTEL_FEATURES.map((f) => [f, false]));
export interface SessionIntelControlsReader {
  read(orgId: string, workspaceId: string): Promise<SessionIntelControls>;
}
type ControlWarning = "unavailable" | "timeout" | "busy" | "malformed";

/** One reader per pool. A hung query cannot create an unbounded queue of new queries. */
export function sessionIntelControls(db: SqlQuery, opts: {
  timeoutMs?: number;
  warn?: (reason: ControlWarning, sqlstate?: string) => void;
} = {}): SessionIntelControlsReader {
  let busy = false;
  const warn = opts.warn ?? ((reason: ControlWarning, sqlstate?: string) => log.warn("session intelligence controls off", { reason, count: 1, ...(sqlstate ? { sqlstate } : {}) }));
  return {
    async read(orgId, workspaceId) {
      if (busy) { warn("busy"); return controlsOff(); }
      busy = true;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = Symbol("timeout");
      try {
        const query = Promise.resolve().then(() => db.query<{ feature: unknown; enabled: unknown }>(CONTROLS_SQL, [orgId, workspaceId]))
          .finally(() => { busy = false; });
        const result = await Promise.race([query, new Promise<typeof timeout>((resolve) => {
          timer = setTimeout(() => resolve(timeout), opts.timeoutMs ?? CONTROLS_TIMEOUT_MS);
        })]);
        if (result === timeout) { warn("timeout"); return controlsOff(); }
        const enabled = new Map(controlsOff());
        const seen = new Set<string>();
        for (const row of result.rows) {
          if (typeof row.feature !== "string" || !SESSION_INTEL_FEATURES.includes(row.feature as SessionIntelFeature)
              || typeof row.enabled !== "boolean" || seen.has(row.feature)) {
            warn("malformed"); return controlsOff();
          }
          seen.add(row.feature);
          enabled.set(row.feature as SessionIntelFeature, row.enabled);
        }
        return enabled;
      } catch (error) {
        const code = (error as { code?: unknown } | null)?.code;
        warn("unavailable", typeof code === "string" && /^[0-9A-Z]{5}$/u.test(code) ? code : undefined);
        return controlsOff();
      } finally {
        if (timer !== undefined) clearTimeout(timer);
      }
    },
  };
}
