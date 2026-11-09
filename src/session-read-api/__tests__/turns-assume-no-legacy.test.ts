/** READ_API_TURNS_ASSUME_NO_LEGACY: a no-Neon deployment serves canonical turns without the legacy marker; the default still refuses. */
import { expect, test } from "bun:test";
import { TurnsInputError, turnRows } from "../session-turns-source.ts";
import type { CanonicalRows } from "../session-timing-source.ts";

const identity = { orgId: "org_1", workspaceId: "ws_1", sessionId: "sess_1" };
const row = {
  id: "t1", org_id: "org_1", workspace_id: "ws_1", session_id: "sess_1", source: "claude", session_owner: "user_1",
  turn_index: 0, role: "user", content: "hello", actor_name: "A", actor_role: "owner", metadata: { nativeCli: "claude" }, ts: "2026-11-01T10:00:00.000Z",
};
const snapshot = { rows: [{ table: "conversation_turns", row }], legacyCoverage: null } as unknown as CanonicalRows;
const snapshots = [{ harness: "claude", snapshot }];

test("no legacy marker: refused by default, as before", () => {
  expect(() => turnRows(identity, snapshots, undefined)).toThrow(TurnsInputError);
  expect(() => turnRows(identity, snapshots, undefined, { assumeNoLegacy: false })).toThrow(TurnsInputError);
});

test("no legacy marker, deployment declares no legacy source: the canonical turns are served", () => {
  expect(turnRows(identity, snapshots, undefined, { assumeNoLegacy: true }).map((r) => r.id)).toEqual(["t1"]);
});

test("the flag does not relax the scope check", () => {
  const foreign = [{ harness: "claude", snapshot: { rows: [{ table: "conversation_turns", row: { ...row, org_id: "org_2" } }], legacyCoverage: null } as unknown as CanonicalRows }];
  expect(() => turnRows(identity, foreign, undefined, { assumeNoLegacy: true })).toThrow(TurnsInputError);
});
