/** Test helpers for the workspace-synthesis job and route tests (synthetic, built in memory). */
/**
 * The fixture with fx-s01 delegating to a child: one assistant reference from the child session, and a related
 * session id with no reference. Built here, never written back to the pinned fixture.
 */
export function withChild(c: any) {
  const e = c.sessions.find((x: { sessionId: string }) => x.sessionId === "fx-s01");
  const ev = e.result.analysis.evidence;
  const at = ev.references[0].at;
  ev.references.push({ id: "event:fx-claude:fx-s01-child:a1", source: "fx-claude", kind: "assistant-message", sessionId: "fx-s01-child", at, summary: "Fixture child reply.", detail: null, truncated: false, isError: null, toolCallId: null, locator: { eventUid: "fx-uid-child-a1", toolUseId: null, fileEditId: null } });
  for (const k of ev.coverage.byKind) if (k.kind === "assistant-message") { k.available++; k.included++; }
  ev.relatedSessionIds = ["fx-s01-child", "fx-s01-spawned"];
  return c;
}

/** The fixture with fx-s01 naming `n` related sessions (`fx-zz-rel-00000` ...), each with no reference of its own. */
export function withRelated(c: any, n: number) {
  const e = c.sessions.find((x: { sessionId: string }) => x.sessionId === "fx-s01");
  e.result.analysis.evidence.relatedSessionIds = Array.from({ length: n }, (_, i) => `fx-zz-rel-${String(i).padStart(5, "0")}`);
  return c;
}

/** The fixture moved to another workspace id (every entry follows the wrapper). */
export function inWorkspace(c: any, workspace: string) {
  c.workspace = workspace;
  for (const e of c.sessions) e.workspace = workspace;
  return c;
}
