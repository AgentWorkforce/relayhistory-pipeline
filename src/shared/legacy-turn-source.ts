/** Scoped public conversation-turn attribution. Mirrors RHC selectedSessionTurns,
 * not the overloaded stored source column. No I/O, query execution or owner access.
 * SQL fragments accept only trusted aliases/positional bind parameters. */
function ref(alias: string) {
  if (!/^[a-z_][a-z0-9_]{0,62}$/u.test(alias))
    throw new Error("turn source SQL alias invalid");
  return '"' + alias + '"';
}
function columns(alias: string) {
  if (alias.length > 32) throw new Error("turn source SQL alias too long");
  return ref(alias);
}
/**
 * Which convergence events count as evidence for a turn (activation gate 5).
 *  - "turn" (default): events in the turn's own workspace, as relayhistory's
 *    workspace-scoped reads (`conversationSourceForScope(true)`) and every
 *    canonical export/read, which are always workspace scoped;
 *  - "any": events in any workspace, as relayhistory's unscoped reads
 *    (`conversationSourceForScope(false)`);
 *  - "$n": `COALESCE(e.workspace_id,'') = $n`, exactly the public oracle's
 *    `p_event_workspace` (0074 `selected_session_turn_ids`).
 */
export type EventWorkspace = "turn" | "any" | `$${number}`;
function eventWorkspaceSql(e: string, t: string, mode: EventWorkspace): string {
  if (mode === "turn") return `AND ${e}.workspace_id=${t}.workspace_id`;
  if (mode === "any") return "";
  if (!/^\$[1-9][0-9]*$/u.test(mode))
    throw new Error("turn source SQL parameter invalid");
  return `AND COALESCE(${e}.workspace_id,'')=${mode}`;
}
export function publicTurnSourceSql(
  alias: string,
  eventWorkspace: EventWorkspace = "turn",
): string {
  const t = columns(alias),
    e = ref(alias + "_owner_event");
  return `COALESCE(NULLIF(${t}.metadata->>'nativeCli',''),NULLIF(${t}.metadata->>'source',''),
 CASE WHEN ${t}.source<>'' AND ${t}.source<>${t}.session_owner THEN ${t}.source END,
 CASE WHEN EXISTS(SELECT 1 FROM sessions.convergence_events ${e}
  WHERE ${e}.org_id=${t}.org_id ${eventWorkspaceSql(e, t, eventWorkspace)}
   AND ${e}.session_id=${t}.session_id AND ${e}.source=${t}.session_owner)
 THEN ${t}.session_owner END)`;
}
/** Discovery may resolve an untagged turn from exactly one event source in the
 * same tenant/workspace/session. NULL is an explicit unbound coverage state. */
export function resolvedPublicTurnSourceSql(
  alias: string,
  eventWorkspace: EventWorkspace = "turn",
): string {
  const t = columns(alias),
    e = ref(alias + "_fallback_event");
  return `COALESCE(${publicTurnSourceSql(alias, eventWorkspace)},(SELECT min(${e}.source)
 FROM sessions.convergence_events ${e} WHERE ${e}.org_id=${t}.org_id
 ${eventWorkspaceSql(e, t, eventWorkspace)} AND ${e}.session_id=${t}.session_id
 HAVING count(DISTINCT ${e}.source)=1))`;
}
/** Exact current source-selected public replay rule, including the unambiguous
 * fallback. Callers separately enforce org/workspace/session and valid provider. */
export function selectedPublicTurnPredicateSql(
  alias: string,
  sourceParameter: string,
  eventWorkspace: EventWorkspace = "turn",
): string {
  if (!/^\$[1-9][0-9]*$/u.test(sourceParameter))
    throw new Error("turn source SQL parameter invalid");
  const t = columns(alias),
    e = ref(alias + "_other_event"),
    attributed = publicTurnSourceSql(alias, eventWorkspace);
  return `(${attributed}=${sourceParameter} OR (${attributed} IS NULL AND NOT EXISTS(
 SELECT 1 FROM sessions.convergence_events ${e} WHERE ${e}.org_id=${t}.org_id
 ${eventWorkspaceSql(e, t, eventWorkspace)} AND ${e}.session_id=${t}.session_id
 AND ${e}.source<>${sourceParameter})))`;
}

const orderEncoder = new TextEncoder();
/** UTF-8 byte order, which is code-point order: Postgres `COLLATE "C"` and
 * Neon's C.UTF-8 default (activation gate 9). Never locale order. */
export function textOrder(a: unknown, b: unknown) {
  const x = orderEncoder.encode(String(a ?? "")),
    y = orderEncoder.encode(String(b ?? ""));
  for (let i = 0; i < Math.min(x.length, y.length); i++)
    if (x[i] !== y[i]) return x[i]! - y[i]!;
  return x.length - y.length;
}
/** The public turn tuple uses the persisted source label, not the attributed
 * harness. Exporters retain it privately when canonicalizing row.source. */
export function comparePublicTurnRows(
  a: Record<string, unknown>,
  b: Record<string, unknown>,
): number {
  const key = (r: Record<string, unknown>) =>
    r.legacy_stored_source ?? r.source;
  const id =
    /^[0-9]+$/u.test(String(a.id)) && /^[0-9]+$/u.test(String(b.id))
      ? BigInt(String(a.id)) < BigInt(String(b.id))
        ? -1
        : BigInt(String(a.id)) > BigInt(String(b.id))
          ? 1
          : 0
      : textOrder(a.id, b.id);
  return (
    Number(a.turn_index) - Number(b.turn_index) ||
    textOrder(a.workspace_id, b.workspace_id) ||
    textOrder(key(a), key(b)) ||
    id
  );
}
