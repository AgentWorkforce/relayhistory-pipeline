/**
 * The wave-1 scope check, its own workflow step (.github/workflows/backlog-ops.yml "Check the wave-1 enabled scope").
 * It runs for the marker and hold executes only, before every step that holds another secret, and is given the dispatch
 * inputs and the BACKLOG_OPS_ORGS secret alone. It refuses (exit 1, a fixed code, never an org) unless the scope is
 * EXACTLY the reviewed enabled pair (scripts/backlog-ops.ts backlogOpsScopeCheck; src/session-ingest/backlog-ops.ts
 * requireScopeEnabled). The credential step and the tool check it again.
 */
import { backlogOpsScopeCheck } from "./backlog-ops.ts";

try { console.log("backlog_ops_scope_check " + backlogOpsScopeCheck(process.env)); }
catch (error) {
  const code = (error as { code?: unknown })?.code;
  console.error("backlog_ops_refused " + (typeof code === "string" && /^[a-z0-9_]{1,64}$/u.test(code) ? code : "failed"));
  process.exitCode = 1;
}
