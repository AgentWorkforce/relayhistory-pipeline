import { expect, test } from "bun:test";
import { canonicalLegacy } from "../canonical-legacy.ts";
import type { RelayhistoryStreamRecord } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
const identity = {
  orgId: "o",
  workspaceId: "w",
  source: "claude",
  sessionId: "s",
};
const record = (
  payload: Record<string, unknown>,
): RelayhistoryStreamRecord => ({
  kind: "legacy_conversation_turn",
  origin_id: "legacy:conversation_turns",
  record_id: "legacy:conversation_turns:id",
  revision: "1",
  operation: "upsert",
  received_at: "2026-11-01T00:00:00Z",
  payload: {
    org_id: "o",
    workspace_id: "w",
    source: "claude",
    session_id: "s",
    id: "id",
    turn_index: 0,
    role: "assistant",
    session_owner: "owner",
    content: "body",
    actor_name: "Agent",
    actor_role: "owner",
    ts: "2026-11-01T00:00:00.123456Z",
    ...payload,
  },
});
test.each(["org_id", "workspace_id", "source", "session_id"])(
  "legacy %s cannot cross manifest scope",
  (field) => {
    expect(() =>
      canonicalLegacy([record({ [field]: "foreign" })], identity),
    ).toThrow();
  },
);
test("absence of explicit legacy coverage remains unknown", () => {
  expect(canonicalLegacy([record({})], identity).coverage).toBeNull();
});
test("coverage requires every table exactly once", () => {
  const r = record({
    version: 1,
    tables: ["conversation_turns", "session_links"],
  });
  r.kind = "legacy_scope";
  r.origin_id = "legacy:scope";
  r.record_id = "legacy:scope:coverage";
  expect(() => canonicalLegacy([r], identity)).toThrow();
});

test("gate 6: schema-legal values (negative turn_index, empty link_ref) are accepted", () => {
  const link: RelayhistoryStreamRecord = {
    ...record({}),
    kind: "legacy_session_link",
    origin_id: "legacy:session_links",
    record_id: "legacy:session_links:7",
    payload: { org_id: "o", workspace_id: "w", source: "claude", session_id: "s", id: "7", link_kind: "commit", link_ref: "", link_ts: null },
  };
  const result = canonicalLegacy([record({ turn_index: -3 }), link], identity);
  expect(result.rows.map((r) => r.table)).toEqual(["conversation_turns", "session_links"]);
  expect(result.rejected).toBe(0);
});

test("gate 6: an invalid row is counted and makes coverage unknown, never failing the read", () => {
  const marker: RelayhistoryStreamRecord = {
    ...record({ version: 1, tables: ["conversation_turns", "session_links", "session_outcomes"] }),
    kind: "legacy_scope",
    origin_id: "legacy:scope",
    record_id: "legacy:scope:coverage",
  };
  const result = canonicalLegacy([record({ role: "robot" }), record({ turn_index: 2 ** 31 }), marker], identity);
  expect(result.rows).toEqual([]);
  expect(result.rejected).toBe(2);
  expect(result.coverage).toBeNull();
  expect(canonicalLegacy([marker], identity).coverage).not.toBeNull();
});

test("gate 6: a record outside the scope refuses with a CanonicalReadError (a fallback, not a 500)", async () => {
  const { CanonicalReadError } = await import("../canonical.ts");
  expect(() => canonicalLegacy([record({ org_id: "foreign" })], identity)).toThrow(CanonicalReadError);
});
