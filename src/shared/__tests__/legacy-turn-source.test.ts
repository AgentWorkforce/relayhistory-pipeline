import { beforeAll, afterAll, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import { readFileSync } from "node:fs";
import {
  comparePublicTurnRows,
  publicTurnSourceSql,
  resolvedPublicTurnSourceSql,
  selectedPublicTurnPredicateSql,
} from "../legacy-turn-source.ts";
const pg = new PGlite();
const cases = [
  {
    name: "native-metadata-over-column",
    source: "codex",
    owner: "alice",
    metadata: { nativeCli: "claude" },
    events: [],
    want: "claude",
  },
  {
    name: "source-metadata-over-column",
    source: "claude",
    owner: "alice",
    metadata: { source: "codex" },
    events: [],
    want: "codex",
  },
  {
    name: "empty-native-uses-source-tag",
    source: "codex",
    owner: "alice",
    metadata: { nativeCli: "", source: "claude" },
    events: [],
    want: "claude",
  },
  {
    name: "explicit-nonowner-source",
    source: "codex",
    owner: "alice",
    metadata: {},
    events: [],
    want: "codex",
  },
  {
    name: "owner-is-known-provider",
    source: "codex",
    owner: "codex",
    metadata: {},
    events: ["codex"],
    want: "codex",
  },
  {
    name: "human-owner-unique-event",
    source: "alice",
    owner: "alice",
    metadata: {},
    events: ["claude"],
    want: "claude",
  },
  {
    name: "other-workspace-owner-is-not-proof",
    source: "codex",
    owner: "codex",
    metadata: {},
    events: ["claude"],
    want: "claude",
    foreign: "codex",
  },
  {
    name: "untagged-turn-only-unbound",
    source: "alice",
    owner: "alice",
    metadata: {},
    events: [],
    want: null,
  },
  {
    name: "two-event-providers-unbound",
    source: "alice",
    owner: "alice",
    metadata: {},
    events: ["claude", "codex"],
    want: null,
  },
];
beforeAll(async () => {
  await pg.exec(
    "CREATE SCHEMA sessions;CREATE TABLE sessions.convergence_events(org_id text,workspace_id text,session_id text,source text);CREATE TABLE sessions.conversation_turns(id uuid,org_id text,workspace_id text,session_id text,source text,session_owner text,metadata jsonb,turn_index integer DEFAULT 1);",
  );
  await pg.exec(
    readFileSync(
      new URL("./fixtures/public-selected-turns.sql", import.meta.url),
      "utf8",
    ),
  );
  for (let i = 0; i < cases.length; i++) {
    const c = cases[i]!;
    await pg.query(
      "INSERT INTO sessions.conversation_turns(id,org_id,workspace_id,session_id,source,session_owner,metadata) VALUES($1,'org','ws',$2,$3,$4,$5::jsonb)",
      [
        "00000000-0000-4000-8000-" + String(i + 1).padStart(12, "0"),
        c.name,
        c.source,
        c.owner,
        JSON.stringify(c.metadata),
      ],
    );
    for (const source of c.events)
      await pg.query(
        "INSERT INTO sessions.convergence_events VALUES('org','ws',$1,$2)",
        [c.name, source],
      );
    if ("foreign" in c)
      await pg.query(
        "INSERT INTO sessions.convergence_events VALUES('org','foreign',$1,$2)",
        [c.name, c.foreign],
      );
    await pg.query(
      "INSERT INTO sessions.convergence_events VALUES('foreign','ws',$1,'codex')",
      [c.name],
    );
  }
});
afterAll(async () => {
  await pg.close();
});
test("selected source predicate matches actual public SQL oracle across overloaded sources and scoped fallback", async () => {
  for (const c of cases)
    for (const source of ["claude", "codex", "other"]) {
      const expected = (
        await pg.query<{ ids: string[] }>(
          "SELECT sessions.selected_session_turn_ids('org','ws','ws',$1,$2) AS ids",
          [source, c.name],
        )
      ).rows[0]!.ids;
      const actual = (
        await pg.query<{ id: string }>(
          `SELECT t.id FROM sessions.conversation_turns t WHERE t.org_id='org' AND t.workspace_id='ws' AND t.session_id=$2 AND ${selectedPublicTurnPredicateSql("t", "$1")}`,
          [source, c.name],
        )
      ).rows.map((r) => r.id);
      expect(actual).toEqual(expected);
    }
});
test("discovery resolves metadata/non-owner/known-owner then unique scoped events and preserves unbound NULL", async () => {
  for (const c of cases) {
    const result = (
      await pg.query<{ source: string | null }>(
        `SELECT ${resolvedPublicTurnSourceSql("t")} AS source FROM sessions.conversation_turns t WHERE t.org_id='org' AND t.workspace_id='ws' AND t.session_id=$1`,
        [c.name],
      )
    ).rows[0]!.source;
    expect(result).toBe(c.want);
  }
});
test("base attribution does not reinterpret a human owner as provider or inherit another workspace", async () => {
  for (const name of [
    "human-owner-unique-event",
    "other-workspace-owner-is-not-proof",
    "untagged-turn-only-unbound",
  ]) {
    expect(
      (
        await pg.query<{ source: string | null }>(
          `SELECT ${publicTurnSourceSql("t")} AS source FROM sessions.conversation_turns t WHERE session_id=$1`,
          [name],
        )
      ).rows[0]!.source,
    ).toBeNull();
  }
});
test("SQL helpers refuse caller fragments and ambiguous identifier truncation", () => {
  expect(() => publicTurnSourceSql("t;DROP TABLE x")).toThrow();
  expect(() => publicTurnSourceSql("t".repeat(33))).toThrow();
  expect(() => selectedPublicTurnPredicateSql("t", "'claude'")).toThrow();
  expect(() => selectedPublicTurnPredicateSql("t", "$1 OR true")).toThrow();
  expect(() => publicTurnSourceSql("t".repeat(32))).not.toThrow();
});

test("normalized turn rows retain the exact public persisted-source order rather than UUID order", async () => {
  const { canonicalLegacy } =
    await import("../../session-read-api/canonical-legacy.ts");
  const a = "aaaaaaaa-0000-4000-8000-000000000001",
    b = "bbbbbbbb-0000-4000-8000-000000000002";
  await pg.query(
    `INSERT INTO sessions.conversation_turns(id,org_id,workspace_id,session_id,source,session_owner,metadata) VALUES($1,'org','ws','sort-source','z-label','human','{"nativeCli":"claude"}'),($2,'org','ws','sort-source','a-label','human','{"nativeCli":"claude"}')`,
    [a, b],
  );
  const expected = (
    await pg.query<{ id: string }>(
      "SELECT id FROM sessions.conversation_turns WHERE id=ANY(sessions.selected_session_turn_ids('org','ws','ws','claude','sort-source')) ORDER BY turn_index,workspace_id,source,id",
    )
  ).rows.map((r) => r.id);
  const rows = [
    { id: a, legacy_stored_source: "z-label" },
    { id: b, legacy_stored_source: "a-label" },
  ].map((r) => ({
    ...r,
    org_id: "org",
    workspace_id: "ws",
    session_id: "sort-source",
    source: "claude",
    session_owner: "human",
    turn_index: 1,
    role: "assistant",
    content: "kept",
    actor_name: "Agent",
    actor_role: "owner",
    metadata: { nativeCli: "claude" },
    ts: "2026-11-01T00:00:00.000001Z",
  }));
  expect([...rows].sort(comparePublicTurnRows).map((r) => r.id)).toEqual(
    expected,
  );
  const result = canonicalLegacy(
    rows.map((payload) => ({
      kind: "legacy_conversation_turn",
      origin_id: "legacy:conversation_turns",
      record_id: "legacy:conversation_turns:" + payload.id,
      payload,
    })) as any,
    {
      orgId: "org",
      workspaceId: "ws",
      source: "claude",
      sessionId: "sort-source",
    },
  );
  expect(result.rows.map((r) => r.row.id)).toEqual(expected);
  expect(expected).toEqual([b, a]);
});

test("gate 5: the $n event-workspace mode equals the public oracle when turn and event workspaces differ", async () => {
  // Turns live in workspace 'default' (a token without a workspace); their events in '' and 'ws'.
  await pg.exec(`INSERT INTO sessions.conversation_turns(id,org_id,workspace_id,session_id,source,session_owner,metadata) VALUES
 ('cccccccc-0000-4000-8000-000000000001','org','default','ws-split','alice','alice','{}'),
 ('cccccccc-0000-4000-8000-000000000002','org','default','ws-split','codex','codex','{}');
 INSERT INTO sessions.convergence_events VALUES('org','','ws-split','codex'),('org',NULL,'ws-split','codex'),('org','ws','ws-split','claude');`);
  for (const eventWorkspace of ["", "ws", "default"])
    for (const source of ["claude", "codex", "other"]) {
      const expected = (
        await pg.query<{ ids: string[] }>(
          "SELECT sessions.selected_session_turn_ids('org','default',$3,$1,$2) AS ids",
          [source, "ws-split", eventWorkspace],
        )
      ).rows[0]!.ids;
      const actual = (
        await pg.query<{ id: string }>(
          `SELECT t.id FROM sessions.conversation_turns t WHERE t.org_id='org' AND t.workspace_id='default' AND t.session_id=$2 AND ${selectedPublicTurnPredicateSql("t", "$1", "$3")} ORDER BY t.id`,
          [source, "ws-split", eventWorkspace],
        )
      ).rows.map((r) => r.id);
      expect(actual).toEqual(expected);
    }
  // The default ("turn") mode is relayhistory's workspace-scoped rule: events of 'default' only, so none here.
  const scoped = await pg.query<{ s: string | null }>(
    `SELECT ${publicTurnSourceSql("t")} AS s FROM sessions.conversation_turns t WHERE t.session_id='ws-split' AND t.source='codex'`,
  );
  expect(scoped.rows[0]!.s).toBeNull();
  // "any" is relayhistory's unscoped rule: an owner event in any workspace counts.
  const any = await pg.query<{ s: string | null }>(
    `SELECT ${publicTurnSourceSql("t", "any")} AS s FROM sessions.conversation_turns t WHERE t.session_id='ws-split' AND t.source='codex'`,
  );
  expect(any.rows[0]!.s).toBe("codex");
  expect(() => publicTurnSourceSql("t", "$0" as "$1")).toThrow();
});

test('gate 9: the JS tiebreak is Postgres COLLATE "C" order, never locale order', async () => {
  const { textOrder } = await import("../legacy-turn-source.ts");
  const values = ["b", "B", "a", "_", "Z", "é", "e", "ä", "aa", "a-", "a_", "𝔸", "ß", "10", "9", ""];
  const pgOrder = (
    await pg.query<{ v: string }>(
      `SELECT v FROM unnest($1::text[]) v ORDER BY v COLLATE "C"`,
      [values],
    )
  ).rows.map((r) => r.v);
  expect([...values].sort(textOrder)).toEqual(pgOrder);
});

test("gate 9: legacy table rows sort in Neon key order and reverse for desc", async () => {
  const { sortLegacyRows } = await import("../../session-read-api/canonical-legacy.ts");
  const links = [{ row: { id: "10" } }, { row: { id: "9" } }, { row: { id: "9007199254740993" } }];
  expect(sortLegacyRows("session_links", [...links], "asc").map((r) => r.row.id)).toEqual(["9", "10", "9007199254740993"]);
  expect(sortLegacyRows("session_links", [...links], "desc").map((r) => r.row.id)).toEqual(["9007199254740993", "10", "9"]);
  const turns = [0, 2, 1].map((i) => ({ row: { id: `00000000-0000-4000-8000-00000000000${i}`, turn_index: i, workspace_id: "w", source: "claude" } }));
  expect(sortLegacyRows("conversation_turns", [...turns], "desc").map((r) => r.row.turn_index)).toEqual([2, 1, 0]);
  const outcomes = [{ row: { commit_sha: "b" } }, { row: { commit_sha: "B" } }, { row: { commit_sha: "a" } }];
  expect(sortLegacyRows("session_outcomes", [...outcomes], "asc").map((r) => r.row.commit_sha)).toEqual(["B", "a", "b"]);
});
