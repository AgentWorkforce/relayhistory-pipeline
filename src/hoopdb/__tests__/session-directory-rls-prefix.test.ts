/**
 * Direct authenticated list queries under the real RLS policy (every supabase migration
 * applied, so jwt_org_id/jwt_workspace_ids/member_of are the production definitions),
 * with the leakproof agent-prefix bounds of helm-chart-chief's option A ruling: the
 * text_pattern_ops range is the Index Cond on session_directory_agent_recent_idx, and the
 * escaped LIKE is only the exact recheck. In-process PGlite; synthetic data.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import {
  AGENT_PREFIX_BENCHMARK_GATE,
  SESSION_DIRECTORY_LIST_COLUMNS,
  SESSION_DIRECTORY_QUERIES,
  agentPrefixRange,
  normalizeAgentName,
  agentExactValue,
  NonAsciiAgentFilterError,
  NonAsciiAgentPrefixError,
  textPatternUpperBound,
} from "../search-index-contract.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const MAX = String.fromCodePoint(0x10ffff);
const TOKEN_ISSUER = "https://issuer.test/session-directory";

async function database(): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
    await db.exec(read(`supabase/migrations/${file}`));
  }
  await db.query(
    "INSERT INTO sessions.session_directory_search_token_policy (issuer, audience) VALUES ($1, $2)",
    [TOKEN_ISSUER, "session-directory-search"],
  );
  return db;
}

async function asAuthenticated<T>(
  db: PGlite,
  run: () => Promise<T>,
  org = "o",
  workspaces = ["w"],
): Promise<T> {
  await db.query("SELECT set_config('request.jwt.claims', $1, false)", [
    JSON.stringify({
      role: "authenticated",
      app_metadata: { org_id: org, workspace_ids: workspaces },
    }),
  ]);
  await db.exec("SET ROLE authenticated");
  try {
    return await run();
  } finally {
    await db.exec("RESET ROLE");
    await db.query("SELECT set_config('request.jwt.claims', '', false)");
  }
}

async function insertAgents(
  db: PGlite,
  names: string[],
  org = "o",
  workspace = "w",
) {
  for (const [i, name] of names.entries()) {
    await db.query(
      `INSERT INTO sessions.session_directory
         (org_id, workspace_id, harness, session_id, created_at, last_activity_at, agent_name, agent_name_normalized)
       VALUES ($1, $2, 'codex', $3, now(), now() + $4 * interval '1 second', $5::text, lower(btrim($5::text)))`,
      [org, workspace, `s-${String(i).padStart(4, "0")}`, i, name],
    );
  }
}

const prefixArgs = (value: string, limit = 50) => {
  const range = agentPrefixRange(value);
  return ["o", "w", range.lower, range.upper, range.pattern, limit];
};

/** LIKE prefix pattern for an already-normalized value, escaping % _ and backslash. */
const likePrefix = (value: string) => `${value.replace(/[\\%_]/g, "\\$&")}%`;

/**
 * The bounded query with bounds computed straight from an already-normalized
 * (lowercase) prefix, bypassing agentPrefixRange's ASCII-only guard: exercises the
 * byte-order carry on stored Unicode names.
 */
async function rawBoundHits(db: PGlite, lower: string): Promise<string[]> {
  const rows = await asAuthenticated(
    db,
    async () =>
      (
        await db.query<{ agent_name_normalized: string }>(
          SESSION_DIRECTORY_QUERIES.agentPrefix,
          [
            "o",
            "w",
            lower,
            textPatternUpperBound(lower),
            likePrefix(lower),
            50,
          ],
        )
      ).rows,
  );
  return rows.map((r) => r.agent_name_normalized).sort();
}

/** The tenant-bound RPC as PostgREST would call it for the same tenant. */
async function rpcAgentHits(
  db: PGlite,
  agent: string,
  match: "exact" | "prefix",
): Promise<string[]> {
  await db.query("SELECT set_config('request.jwt.claims', $1, false)", [
    JSON.stringify({
      role: "session_directory_search_worker",
      iss: TOKEN_ISSUER,
      aud: "session-directory-search",
      iat: Math.floor(Date.now() / 1000),
      exp: Math.floor(Date.now() / 1000) + 60,
      app_metadata: { org_id: "o", workspace_ids: ["w"] },
    }),
  ]);
  await db.exec("SET ROLE session_directory_search_worker");
  try {
    const { rows } = await db.query<{ agent_name: string }>(
      "SELECT * FROM sessions.search_session_directory(p_workspace_id => 'w', p_agent => $1, p_agent_match => $2)",
      [agent, match],
    );
    return rows.map((r) => r.agent_name).sort();
  } finally {
    await db.exec("RESET ROLE");
    await db.query("SELECT set_config('request.jwt.claims', '', false)");
  }
}

async function prefixHits(db: PGlite, value: string): Promise<string[]> {
  const rows = await asAuthenticated(
    db,
    async () =>
      (
        await db.query<{ agent_name_normalized: string }>(
          SESSION_DIRECTORY_QUERIES.agentPrefix,
          prefixArgs(value),
        )
      ).rows,
  );
  return rows.map((r) => r.agent_name_normalized).sort();
}

describe("leakproof operator flags the prefix design depends on", () => {
  test("textlike is not leakproof; texteq and the text_pattern_ops bounds are", async () => {
    const db = await database();
    try {
      const { rows } = await db.query<{
        proname: string;
        proleakproof: boolean;
      }>(
        `SELECT proname, proleakproof FROM pg_proc
          WHERE proname IN ('textlike', 'texteq', 'text_pattern_ge', 'text_pattern_lt') ORDER BY proname`,
      );
      expect(rows).toEqual([
        { proname: "text_pattern_ge", proleakproof: true },
        { proname: "text_pattern_lt", proleakproof: true },
        { proname: "texteq", proleakproof: true },
        { proname: "textlike", proleakproof: false },
      ]);
    } finally {
      await db.close();
    }
  });
});

describe("direct list queries", () => {
  test("name only the column-granted metadata and never a wildcard", () => {
    const grant =
      /GRANT SELECT \(([^)]*)\) ON sessions\.session_directory TO authenticated/.exec(
        read(
          "supabase/migrations/20261030020000_session_directory_short_summary.sql",
        ),
      );
    expect(grant).not.toBeNull();
    const granted = grant![1]!
      .split(",")
      .map((c) => c.trim())
      .filter(Boolean);
    expect(granted).toHaveLength(24);
    expect([...SESSION_DIRECTORY_LIST_COLUMNS]).toEqual(granted);
    for (const [name, sql] of Object.entries(SESSION_DIRECTORY_QUERIES)) {
      expect(sql, name).not.toMatch(/\*/);
      expect(sql, name).not.toMatch(/\b(short_summary|search_text)\b/);
    }
  });

  test("exact emits neither LIKE nor a range; prefix emits both bounds plus the LIKE recheck", () => {
    expect(SESSION_DIRECTORY_QUERIES.agentExact).not.toMatch(
      /\bLIKE\b|~>=~|~<~/,
    );
    expect(SESSION_DIRECTORY_QUERIES.agentExact).toMatch(
      /agent_name_normalized = \$3/,
    );
    expect(SESSION_DIRECTORY_QUERIES.agentPrefix).toMatch(
      /agent_name_normalized ~>=~ \$3/,
    );
    expect(SESSION_DIRECTORY_QUERIES.agentPrefix).toMatch(
      /agent_name_normalized ~<~ \$4/,
    );
    expect(SESSION_DIRECTORY_QUERIES.agentPrefix).toMatch(
      /agent_name_normalized LIKE \$5 ESCAPE/,
    );
  });
});

describe("agent prefix bounds and recheck", () => {
  let db: PGlite;
  const NAMES = [
    "a_bc",
    "axbc",
    "a_b",
    "ab%x",
    "abcx",
    "ab\\y",
    "abzy",
    "ab%",
    "ab\\",
    `ab${MAX}z`,
    `ab${MAX}`,
    `ab${String.fromCodePoint(0x10fffe)}`,
    "ac",
    "abd",
    `ab${String.fromCodePoint(0xd7ff)}q`,
    `ab${String.fromCodePoint(0xe000)}`,
    `ab${String.fromCodePoint(0xd7fe)}`,
    "café-one",
    "cafe-two",
    "cafг",
    "日本語-agent",
    "日本-x",
    "Ägent-Nord",
    "ägent-süd",
    "ΟΔΟΣ-Agent",
    "İstanbul-X",
  ];
  beforeAll(async () => {
    db = await database();
    await insertAgents(db, NAMES);
    await db.exec("ANALYZE sessions.session_directory");
  }, 60_000);
  afterAll(async () => {
    await db?.close();
  });

  test("the bounds use the raw prefix; only the recheck is escaped", () => {
    expect(agentPrefixRange("A_b")).toEqual({
      lower: "a_b",
      upper: "a_c",
      pattern: "a\\_b%",
    });
    expect(agentPrefixRange("ab%")).toEqual({
      lower: "ab%",
      upper: "ab&",
      pattern: "ab\\%%",
    });
    expect(agentPrefixRange("ab\\")).toEqual({
      lower: "ab\\",
      upper: "ab]",
      pattern: "ab\\\\%",
    });
  });

  test("'a_b' matches a_bc and a_b, never axbc", async () => {
    expect(await prefixHits(db, "a_b")).toEqual(["a_b", "a_bc"]);
  });

  test("'ab%' and 'ab\\' match literally (2-character 'a%' and 'a\\' are below the 3-character minimum)", async () => {
    expect(await prefixHits(db, "ab%")).toEqual(["ab%", "ab%x"]);
    expect(await prefixHits(db, "ab\\")).toEqual(["ab\\", "ab\\y"]);
    expect(() => agentPrefixRange("a%")).toThrow(RangeError);
    expect(() => agentPrefixRange("a\\")).toThrow(RangeError);
  });

  test("Unicode: a trailing U+10FFFF carries into the previous code point; the surrogate gap is skipped", async () => {
    // The byte-order carry on stored Unicode names (raw lowercase bounds).
    expect(textPatternUpperBound(`ab${MAX}`)).toBe("ac");
    expect(await rawBoundHits(db, `ab${MAX}`)).toEqual(
      [`ab${MAX}`, `ab${MAX}z`].sort(),
    );
    const d7ff = `ab${String.fromCodePoint(0xd7ff)}`;
    expect(textPatternUpperBound(d7ff)).toBe(
      `ab${String.fromCodePoint(0xe000)}`,
    );
    expect(await rawBoundHits(db, d7ff)).toEqual([`${d7ff}q`]);
    expect(await rawBoundHits(db, "日本語")).toEqual(["日本語-agent"]);
    // ASCII prefixes still match stored non-ASCII names exactly.
    expect(await prefixHits(db, "caf")).toEqual(
      ["cafe-two", "café-one", "cafг"].sort(),
    );
    // The direct helper refuses every non-ASCII prefix instead of guessing.
    for (const value of [`ab${MAX}`, d7ff, "日本語", "café"]) {
      expect(() => agentPrefixRange(value)).toThrow(NonAsciiAgentPrefixError);
    }
  });

  test("an uppercase non-ASCII prefix finds its rows through the RPC, never through the bounded query", async () => {
    // Where PostgreSQL lower() and JavaScript toLowerCase() disagree (Greek final
    // sigma, dotted capital I), bounds computed in TypeScript would silently miss
    // the stored row. Prove the disagreement on this engine, so the case is real.
    for (const [name, prefix] of [
      ["ΟΔΟΣ-Agent", "ΟΔΟΣ"],
      ["İstanbul-X", "İSTANBUL"],
    ] as const) {
      const sqlLower = (
        await db.query<{ v: string }>("SELECT lower(btrim($1)) AS v", [prefix])
      ).rows[0]!.v;
      expect(sqlLower, name).not.toBe(prefix.trim().toLowerCase());
      const naive = await rawBoundHits(db, prefix.trim().toLowerCase());
      expect(
        naive.some((n) => n === name.toLowerCase() || n.startsWith(sqlLower)),
        name,
      ).toBe(false);
      // The direct bounded path refuses it, pointing at the RPC...
      expect(() => agentPrefixRange(prefix)).toThrow(NonAsciiAgentPrefixError);
      expect(() => agentPrefixRange(prefix)).toThrow(
        /search_session_directory/,
      );
      // ...and the RPC, which lowers the input with the same SQL lower() as the
      // stored value, finds the row.
      expect(await rpcAgentHits(db, prefix, "prefix"), name).toEqual([name]);
      expect(
        await rpcAgentHits(db, ` ${name.toUpperCase()} `, "exact"),
        name,
      ).toEqual([name]);
    }
    // Where the two agree (Ä here), the direct path still refuses: agreement is
    // collation-dependent, so the bounded query never takes non-ASCII input.
    expect(() => agentPrefixRange("ÄGENT")).toThrow(NonAsciiAgentPrefixError);
    expect(await rpcAgentHits(db, "ÄGENT", "prefix")).toEqual(
      ["Ägent-Nord", "ägent-süd"].sort(),
    );
  });

  test("exact: a non-ASCII name is refused on the direct path and found through the RPC", async () => {
    for (const [name, input] of [
      ["ΟΔΟΣ-Agent", "ΟΔΟΣ-AGENT"],
      ["İstanbul-X", "İSTANBUL-X"],
    ] as const) {
      const sqlLower = (
        await db.query<{ v: string }>("SELECT lower(btrim($1)) AS v", [input])
      ).rows[0]!.v;
      const jsLower = input.trim().toLowerCase();
      expect(sqlLower, name).not.toBe(jsLower);
      // A JavaScript-normalized exact value would silently miss the stored row.
      const naive = await asAuthenticated(
        db,
        async () =>
          (
            await db.query(SESSION_DIRECTORY_QUERIES.agentExact, [
              "o",
              "w",
              jsLower,
              50,
            ])
          ).rows,
      );
      expect(naive, name).toEqual([]);
      // So the direct helpers refuse it, pointing at the RPC's exact match...
      expect(() => agentExactValue(input)).toThrow(NonAsciiAgentFilterError);
      expect(() => agentExactValue(input)).toThrow(/p_agent_match => 'exact'/);
      expect(() => normalizeAgentName(input)).toThrow(NonAsciiAgentFilterError);
      // ...which finds it, because it lowers with the same SQL lower() as storage.
      expect(await rpcAgentHits(db, input, "exact"), name).toEqual([name]);
      // And the SQL-lowered value is what the stored column holds.
      const exact = await asAuthenticated(
        db,
        async () =>
          (
            await db.query<{ agent_name: string }>(
              SESSION_DIRECTORY_QUERIES.agentExact,
              ["o", "w", sqlLower, 50],
            )
          ).rows,
      );
      expect(
        exact.map((r) => r.agent_name),
        name,
      ).toEqual([name]);
    }
    // ASCII exact values pass straight through.
    expect(agentExactValue("  Helm-Chief ")).toBe("helm-chief");
    expect(() => agentExactValue("   ")).toThrow(RangeError);
  });

  test("refuses the all-max prefix and an empty or short prefix", () => {
    expect(() => textPatternUpperBound(MAX.repeat(3))).toThrow(RangeError);
    expect(() => agentPrefixRange(MAX.repeat(3))).toThrow(RangeError);
    expect(() => agentPrefixRange("")).toThrow(RangeError);
    expect(() => agentPrefixRange("   ")).toThrow(RangeError);
    expect(() => agentPrefixRange("ab")).toThrow(RangeError);
  });

  test("property: bounded results equal plain LIKE semantics for generated prefixes", async () => {
    const alphabet = [
      "a",
      "b",
      "_",
      "%",
      "\\",
      "é",
      "日",
      MAX,
      String.fromCodePoint(0xd7ff),
      "z",
    ];
    const ascii = (v: string) => /^[\u0000-\u007f]*$/.test(v);
    let seed = 42;
    const rand = () => {
      seed = (seed * 1103515245 + 12345) % 2 ** 31;
      return seed / 2 ** 31;
    };
    const pick = () => alphabet[Math.floor(rand() * alphabet.length)]!;
    const generated = Array.from({ length: 160 }, () =>
      Array.from({ length: 3 + Math.floor(rand() * 4) }, pick).join(""),
    );
    const prop = await database();
    try {
      const unique = [...new Set(generated)];
      await insertAgents(prop, unique);
      const prefixes = [
        ...new Set([
          ...unique.slice(0, 40).map((n) => Array.from(n).slice(0, 3).join("")),
          ...Array.from({ length: 40 }, () =>
            Array.from({ length: 3 }, pick).join(""),
          ),
        ]),
      ].filter((p) => !Array.from(p).every((c) => c === MAX));
      expect(prefixes.length).toBeGreaterThan(40);
      let nonEmpty = 0;
      for (const prefix of prefixes) {
        // ASCII prefixes go through the direct helper; non-ASCII ones are refused
        // there and checked with raw lowercase bounds (the generated names are
        // already lowercase, so both normalizations agree on them).
        if (!ascii(prefix))
          expect(() => agentPrefixRange(prefix)).toThrow(
            NonAsciiAgentPrefixError,
          );
        const range = ascii(prefix)
          ? agentPrefixRange(prefix)
          : {
              lower: prefix,
              upper: textPatternUpperBound(prefix),
              pattern: likePrefix(prefix),
            };
        const bounded = (
          await asAuthenticated(
            prop,
            async () =>
              (
                await prop.query<{ agent_name_normalized: string }>(
                  SESSION_DIRECTORY_QUERIES.agentPrefix,
                  ["o", "w", range.lower, range.upper, range.pattern, 50],
                )
              ).rows,
          )
        )
          .map((r) => r.agent_name_normalized)
          .sort();
        // Plain LIKE semantics, as owner and without bounds.
        const like = (
          await prop.query<{ agent_name_normalized: string }>(
            `SELECT agent_name_normalized FROM sessions.session_directory
            WHERE org_id = 'o' AND workspace_id = 'w' AND deleted_at IS NULL
              AND agent_name_normalized LIKE $1 ESCAPE '\\'`,
            [range.pattern],
          )
        ).rows
          .map((r) => r.agent_name_normalized)
          .sort();
        const startsWith = unique
          .filter((n) => n.startsWith(range.lower))
          .sort();
        expect(bounded, JSON.stringify(prefix)).toEqual(like.slice(0, 51));
        expect(like, JSON.stringify(prefix)).toEqual(startsWith);
        if (bounded.length) nonEmpty += 1;
      }
      expect(nonEmpty).toBeGreaterThan(20);
    } finally {
      await prop.close();
    }
  }, 120_000);
});

type Plan = { [key: string]: unknown; Plans?: Plan[] };
const walk = (plan: Plan): Plan[] => [
  plan,
  ...(plan.Plans ?? []).flatMap(walk),
];

describe("authenticated plans under the real RLS policy (10,000 rows)", () => {
  let db: PGlite;
  beforeAll(async () => {
    db = await database();
    await db.exec(`
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at, agent_name, agent_name_normalized)
      SELECT 'o', 'w', 'codex', 's-' || n,
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             '2026-10-01T00:00:00Z'::timestamptz + n * interval '1 second',
             CASE WHEN n <= 5100 THEN 'age-' || n ELSE 'helm-' || n END,
             CASE WHEN n <= 5100 THEN 'age-' || n ELSE 'helm-' || n END
        FROM generate_series(1, 10000) AS n;
      INSERT INTO sessions.session_directory
        (org_id, workspace_id, harness, session_id, created_at, last_activity_at, agent_name, agent_name_normalized)
      SELECT 'other', 'w', 'codex', 'x-' || n, now(), now(), 'helm-' || n, 'helm-' || n FROM generate_series(1, 500) AS n;
      ANALYZE sessions.session_directory;
    `);
  }, 60_000);
  afterAll(async () => {
    await db?.close();
  });

  async function explain(sql: string, params: unknown[]) {
    const result = await db.query<Record<string, unknown>>(
      `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`,
      params,
    );
    const plan = (result.rows[0]?.["QUERY PLAN"] as Array<{ Plan: Plan }>)[0]!
      .Plan;
    const nodes = walk(plan);
    const scans = nodes.filter((n) => /Scan$/.test(String(n["Node Type"])));
    const examined = scans.reduce(
      (t, n) =>
        t +
        Number(n["Actual Rows"] ?? 0) +
        Number(n["Rows Removed by Filter"] ?? 0),
      0,
    );
    return {
      nodes,
      returned: Number(plan["Actual Rows"] ?? 0),
      examined,
      buffers:
        Number(plan["Shared Hit Blocks"] ?? 0) +
        Number(plan["Shared Read Blocks"] ?? 0),
    };
  }
  const report = (
    label: string,
    owner: Awaited<ReturnType<typeof explain>>,
    user: Awaited<ReturnType<typeof explain>>,
  ) => {
    const shape = (p: typeof owner) =>
      p.nodes
        .map((n) =>
          [
            n["Node Type"],
            n["Index Name"],
            n["Index Cond"] ? `cond ${n["Index Cond"]}` : null,
            n["Filter"] ? `filter ${n["Filter"]}` : null,
          ]
            .filter(Boolean)
            .join(" | "),
        )
        .join("  ->  ");
    console.info(
      `[G6 RLS plan] ${label}\n  owner:         returned=${owner.returned} examined=${owner.examined} buffers=${owner.buffers} :: ${shape(owner)}\n  authenticated: returned=${user.returned} examined=${user.examined} buffers=${user.buffers} :: ${shape(user)}`,
    );
  };

  test("exact: agent_recent_idx Index Cond with equality, no LIKE", async () => {
    const args = ["o", "w", "helm-9999", 50];
    const owner = await explain(SESSION_DIRECTORY_QUERIES.agentExact, args);
    const user = await asAuthenticated(db, () =>
      explain(SESSION_DIRECTORY_QUERIES.agentExact, args),
    );
    report("exact helm-9999", owner, user);
    const scan = user.nodes.find(
      (n) => n["Index Name"] === "session_directory_agent_recent_idx",
    );
    expect(String(scan?.["Index Cond"])).toContain(
      "agent_name_normalized = 'helm-9999'",
    );
    expect(user.nodes.some((n) => n["Node Type"] === "Seq Scan")).toBe(false);
    expect(user.returned).toBe(1);
  });

  test("selective prefix: the bounds are the agent_recent_idx Index Cond and LIKE is only a recheck", async () => {
    const args = prefixArgs("helm-99");
    const owner = await explain(SESSION_DIRECTORY_QUERIES.agentPrefix, args);
    const user = await asAuthenticated(db, () =>
      explain(SESSION_DIRECTORY_QUERIES.agentPrefix, args),
    );
    report("selective prefix helm-99", owner, user);
    expect(user.nodes.some((n) => n["Node Type"] === "Seq Scan")).toBe(false);
    const scan = user.nodes.find(
      (n) => n["Index Name"] === "session_directory_agent_recent_idx",
    )!;
    expect(scan).toBeDefined();
    const cond = String(scan["Index Cond"]);
    expect(cond).toContain("agent_name_normalized ~>=~ 'helm-99'");
    expect(cond).toContain("agent_name_normalized ~<~ 'helm-9:'");
    expect(cond).not.toContain("~~ ");
    expect(String(scan["Filter"] ?? "")).toContain("~~");
    expect(user.returned).toBe(50);
    expect(user.examined / user.returned).toBeLessThanOrEqual(2.5);
  });

  test("broad prefix: within the row-amplification gate", async () => {
    const args = prefixArgs("age", AGENT_PREFIX_BENCHMARK_GATE.pageSize);
    const owner = await explain(SESSION_DIRECTORY_QUERIES.agentPrefix, args);
    const user = await asAuthenticated(db, () =>
      explain(SESSION_DIRECTORY_QUERIES.agentPrefix, args),
    );
    report("broad prefix age", owner, user);
    expect(user.nodes.some((n) => n["Node Type"] === "Seq Scan")).toBe(false);
    expect(user.returned).toBe(AGENT_PREFIX_BENCHMARK_GATE.pageSize);
    expect(user.examined / user.returned).toBeLessThanOrEqual(
      AGENT_PREFIX_BENCHMARK_GATE.maxIndexRowsExaminedPerResult,
    );
  });

  test("a cross-tenant read returns 0 rows for every direct list query", async () => {
    const params: Record<string, unknown[]> = {
      workspaceRecent: ["o", "w", 50],
      harnessRecent: ["o", "w", "codex", 50],
      projectRecent: ["o", "w", "p", 50],
      withoutProjectRecent: ["o", "w", 50],
      agentExact: ["o", "w", "helm-9999", 50],
      agentPrefix: prefixArgs("helm-99"),
      createdRange: ["o", "w", "2000-01-01", "2100-01-01", 50],
      stateRecent: ["o", "w", "unknown", 50],
      userRecent: ["o", "w", "u", 50],
      machineRecent: ["o", "w", "m", 50],
    };
    expect(Object.keys(params).sort()).toEqual(
      Object.keys(SESSION_DIRECTORY_QUERIES).sort(),
    );
    // As tenant "o", the same query sees rows; a different org or an unclaimed workspace sees none.
    const own = await asAuthenticated(
      db,
      async () =>
        (
          await db.query(
            SESSION_DIRECTORY_QUERIES.workspaceRecent,
            params.workspaceRecent,
          )
        ).rows,
    );
    expect(own.length).toBe(50);
    for (const [org, workspaces] of [
      ["other", ["w"]],
      ["o", ["not-w"]],
      ["", ["w"]],
    ] as const) {
      for (const [name, sql] of Object.entries(SESSION_DIRECTORY_QUERIES)) {
        const rows = await asAuthenticated(
          db,
          async () => (await db.query(sql, params[name]!)).rows,
          org,
          [...workspaces],
        );
        expect(rows, `${name} as ${org}/${workspaces}`).toEqual([]);
      }
    }
  });
});
