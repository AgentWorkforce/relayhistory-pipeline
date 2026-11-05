import { expect, it } from "vitest";
import { canonicalClaimReader } from "../src/canonical.ts";
import sql from "../../../supabase/migrations/20261105023000_session_analysis_publications.sql?raw";

/** Argument names of `CREATE FUNCTION sessions.<name>(...)` as written in the migration. */
function sqlArgs(name: string): string[] {
  const match = new RegExp(
    String.raw`CREATE (?:OR REPLACE )?FUNCTION sessions\.${name}\(([^)]*)\)`,
  ).exec(sql);
  if (!match) throw new Error(`no definition for sessions.${name}`);
  return match[1]!.split(",").map((arg) => arg.trim().split(/\s+/u)[0]!);
}

const token = "header.payload.signature";
const env = {
  CANONICAL_SUPABASE_URL: "https://db.test",
  CANONICAL_SUPABASE_API_KEY: "anon",
} as never;
const identity = {
  orgId: "org-a",
  workspaceId: "ws-a",
  source: "codex",
  sessionId: "one",
};

for (const rpc of [
  "request_session_analysis_cache",
  "session_directory_analysis_cache",
] as const) {
  it(`${rpc} POSTs exactly the SQL signature's argument names (PostgREST matches by name)`, async () => {
    const seen: { url: string; body: Record<string, unknown> }[] = [];
    const fetcher = (async (input: RequestInfo | URL, init?: RequestInit) => {
      seen.push({ url: String(input), body: JSON.parse(String(init?.body)) });
      return new Response("{}", {
        headers: { "content-type": "application/json" },
      });
    }) as typeof fetch;
    const read = canonicalClaimReader(
      env,
      token,
      new AbortController().signal,
      rpc,
      fetcher,
    );
    await read(identity);
    expect(seen).toHaveLength(1);
    expect(seen[0]!.url).toBe(`https://db.test/rest/v1/rpc/${rpc}`);
    const args = sqlArgs(rpc);
    expect(args).toHaveLength(3);
    expect(Object.keys(seen[0]!.body).sort()).toEqual([...args].sort());
    expect(Object.values(seen[0]!.body).sort()).toEqual(
      ["codex", "one", "ws-a"].sort(),
    );
    expect(seen[0]!.body[args[0]!]).toBe("ws-a");
    expect(seen[0]!.body[args[1]!]).toBe("codex");
    expect(seen[0]!.body[args[2]!]).toBe("one");
  });
}
