/**
 * List readiness on GET /v1/sessions (READ_API_LIST_READINESS_ENABLED, migration 20261105062200), through
 * createReadApi and the real sqlDirectory over a recording tenant runner:
 * - the flag-off golden: with the flag off, and with it on but neither param, every statement, parameter, status
 *   and response byte equals fixtures/list-readiness-flag-off.golden.json, which was recorded on origin/main
 *   (list-readiness-golden-harness.ts); that includes main's b96s1 / b96q1 cursors, so main's q fingerprints too;
 * - flag off: either param, with any value, is 501 param_<name> and reaches no SQL;
 * - flag on: the grammar (only "true"; anything else or repeated is 400), the readiness functions and their
 *   p_ready_only, the response fields, the X-Relayhistory-Ready-Only header, and the cursor binding: a readyOnly
 *   page's cursor is the marked b96s1 4-tuple, the pre-readiness decoders refuse it unchanged, and a cursor
 *   replayed across a readyOnly change (plain or q, either direction, flag on or off) is invalid_cursor.
 * The SQL behaviour itself (predicate before the keyset and LIMIT, page walks, grants) is
 * src/hoopdb/__tests__/session-directory-list-readiness.pglite.test.ts.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { createReadApi, READY_ONLY_HEADER } from "../app.ts";
import {
  DIRECTORY_LIST_READINESS_SQL,
  DIRECTORY_LIST_SQL,
  DIRECTORY_QUERY_READINESS_SQL,
  DIRECTORY_QUERY_SQL,
  LIST_FOR_SIGNATURE,
  LIST_READINESS_FOR_SIGNATURE,
  QUERY_FOR_SIGNATURE,
  QUERY_READINESS_FOR_SIGNATURE,
  READY_LIST_CURSOR_MARKER,
  decodeCompositeListCursor,
  decodeListCursor,
  decodeQueryCursor,
  decodeReadyListCursor,
  encodeCompositeListCursor,
  encodeListCursor,
  encodeQueryCursor,
  encodeReadyListCursor,
  sessionFromDirectoryRow,
  sqlDirectory,
  type DirectoryListRow,
} from "../directory.ts";
import { parseSessionQuery, queryFingerprint, type SessionQueryTerms } from "../list-query.ts";
import type { TenantRunner } from "../liveness.ts";
import { goldenRecord, type GoldenEntry } from "./list-readiness-golden-harness.ts";
import { memoryLiveness } from "./support.ts";

const GOLDEN = JSON.parse(readFileSync(join(import.meta.dir, "fixtures/list-readiness-flag-off.golden.json"), "utf8")) as GoldenEntry[];

const row = (session: string, cursorTs: string, ready: boolean, projectedAt: string | null): DirectoryListRow & { cursor_ts: string } => ({
  org_id: "org-a", workspace_id: "ws-a", harness: "codex", session_id: session,
  created_at: new Date("2026-10-01T00:00:00Z"), last_activity_at: new Date(cursorTs), cursor_ts: cursorTs,
  title: null, task_title: null, agent_names: [], project_id: null, project_key: null, project_key_method: null,
  git_repo: null, git_branch: null, initial_commit: null, originator: null, agent_version: null, models: [],
  state: "unknown", event_count: 1, input_tokens: 0, output_tokens: 0, cost_usd_micros: null, machine_ids: [],
  user_ids: ["u-1"], parent_session_ids: [], child_session_count: 0, short_summary: null, short_summary_generated_at: null,
  catalog_projected_at: null, projected_at: null, events_ready: ready, event_projected_at: projectedAt === null ? null : new Date(projectedAt),
});
const ROWS = [
  row("s-3", "2026-10-05T12:00:00.123456Z", true, "2026-10-05T13:00:00Z"),
  row("s-2", "2026-10-04T12:00:00.000001Z", false, "2026-10-04T13:00:00Z"),
  row("s-1", "2026-10-03T12:00:00Z", false, null),
];

/** createReadApi over a recording sqlDirectory; `installed` = what to_regprocedure answers. */
function harness(deps: Record<string, unknown> = {}, installed = true) {
  const statements: Array<{ text: string; params: unknown[] }> = [];
  const run: TenantRunner = async (_o, _w, work) => work({
    async query<R>(text: string, params?: unknown[]) {
      statements.push({ text, params: params ?? [] });
      if (text.includes("to_regprocedure")) return { rows: [{ installed }] as R[] };
      return { rows: ROWS as unknown as R[] };
    },
  });
  const app = createReadApi({
    verify: async () => ({ userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] }),
    ch: {} as never,
    liveness: memoryLiveness().liveness,
    completeSinceMs: 0,
    directory: sqlDirectory(run),
    directoryWorkspaces: new Set(["ws-a"]),
    sessionQuery: true,
    ...deps,
  } as never);
  const get = async (path: string) => {
    statements.length = 0;
    const r = await app.request(`/v1/sessions?workspace=ws-a${path}`, { headers: { authorization: "Bearer t", "x-correlation-id": "c-1" } });
    return { status: r.status, headers: r.headers, body: (await r.json()) as Record<string, any>, statements: [...statements] };
  };
  return { get };
}
const ON = { listReadiness: true };
const Q = `&q=${encodeURIComponent("hello world")}`;

describe("flag off is the pre-readiness read API", () => {
  test("golden: flag off and flag on without either param answer every recorded request byte for byte as origin/main", async () => {
    expect(GOLDEN.length).toBe(13);
    for (const extra of [{}, { listReadiness: false }, { listReadiness: true }]) expect(await goldenRecord(extra)).toEqual(GOLDEN);
  });

  test("the golden names only the pre-readiness functions and SQL", () => {
    const texts = GOLDEN.flatMap((e) => e.statements.map((s) => s.text));
    const params = GOLDEN.flatMap((e) => e.statements.flatMap((s) => s.params));
    expect(texts).toContain(DIRECTORY_LIST_SQL);
    expect(texts).toContain(DIRECTORY_QUERY_SQL);
    expect(params).toContain(LIST_FOR_SIGNATURE);
    expect(params).toContain(QUERY_FOR_SIGNATURE);
    expect(JSON.stringify(GOLDEN)).not.toContain("readiness");
  });

  test("a readyOnly-off q fingerprint is the one origin/main put in its b96q1 cursor", () => {
    const q = GOLDEN.find((e) => e.request.includes("q=hello%20world%20-draft") && !e.request.includes("cursor="))!;
    const cursor = decodeQueryCursor(JSON.parse(q.body).nextCursor);
    if (typeof cursor !== "object" || cursor === undefined) throw new Error("golden q page has no cursor");
    const parsed = parseSessionQuery("hello world -draft source:codex");
    if (parsed.kind !== "terms") throw new Error("q did not parse");
    expect(cursor.fingerprint).toBe("4d43b8f1ba2266040645f96389a9712e");
    expect(queryFingerprint(parsed.terms, {})).toBe(cursor.fingerprint);
    expect(queryFingerprint(parsed.terms, {}, false)).toBe(cursor.fingerprint);
    expect(queryFingerprint(parsed.terms, {}, true)).not.toBe(cursor.fingerprint);
  });

  test("either param, any value, flag off (absent or false): 501 param_<name>, no SQL", async () => {
    for (const deps of [{}, { listReadiness: false }]) {
      const { get } = harness(deps);
      for (const name of ["readyOnly", "includeReadiness"]) {
        for (const value of ["true", "false", "1", "", "TRUE", "yes"]) {
          for (const suffix of ["", Q]) {
            const r = await get(`&${name}=${encodeURIComponent(value)}${suffix}`);
            expect([name, value, suffix, r.status]).toEqual([name, value, suffix, 501]);
            expect(r.body.error).toEqual({ code: "unsupported_by_backend", message: expect.any(String), reason: `param_${name}` });
            expect(r.statements).toEqual([]);
            expect(r.headers.get(READY_ONLY_HEADER)).toBeNull();
          }
        }
        const twice = await get(`&${name}=true&${name}=true`);
        expect(twice.status).toBe(501);
      }
    }
  });
});

describe("flag on: the grammar", () => {
  test("only the exact \"true\"; any other value, or the param repeated, is 400 naming it", async () => {
    const { get } = harness(ON);
    for (const name of ["readyOnly", "includeReadiness"]) {
      for (const value of ["false", "1", "", "TRUE", "True", " true", "yes", "on"]) {
        const r = await get(`&${name}=${encodeURIComponent(value)}`);
        expect([name, value, r.status, r.body.error]).toEqual([name, value, 400, { code: "bad_request", message: `${name} must be true` }]);
        expect(r.statements).toEqual([]);
      }
      const twice = await get(`&${name}=true&${name}=true`);
      expect([twice.status, twice.body.error.message]).toEqual([400, `${name} must be given at most once`]);
      expect((await get(`&${name}=true`)).status).toBe(200);
    }
  });
});

describe("flag on: what each param does", () => {
  const listCall = (s: Array<{ text: string; params: unknown[] }>) => s.find((x) => x.text.includes("FROM sessions.list_session_directory"))!;
  const queryCall = (s: Array<{ text: string; params: unknown[] }>) => s.find((x) => x.text.includes("FROM sessions.query_session_directory"))!;

  test("includeReadiness alone: the readiness function with p_ready_only false, eventsReady / eventsProjectedAt on every row, the plain cursor, no header", async () => {
    const { get } = harness(ON);
    const r = await get("&includeReadiness=true&limit=2");
    expect(r.status).toBe(200);
    expect(r.statements[0]!.params).toEqual([LIST_READINESS_FOR_SIGNATURE]);
    expect(listCall(r.statements).text).toBe(DIRECTORY_LIST_READINESS_SQL);
    expect(listCall(r.statements).params.at(-1)).toBe(false);
    expect(listCall(r.statements).params).toHaveLength(17);
    expect(r.body.sessions.map((s: any) => [s.sessionId, s.eventsReady, s.eventsProjectedAt])).toEqual([
      ["s-3", true, "2026-10-05T13:00:00.000Z"],
      ["s-2", false, "2026-10-04T13:00:00.000Z"],
    ]);
    expect(r.body.nextCursor).toBe(encodeListCursor({ lastActivityAt: "2026-10-04T12:00:00.000001Z", sessionId: "s-2", harness: "codex" }));
    expect(r.headers.get(READY_ONLY_HEADER)).toBeNull();
    // Point 7: the list response carries no count or total that could disagree with a readyOnly page.
    expect(Object.keys(r.body).sort()).toEqual(["correlationId", "nextCursor", "sessions"]);
  });

  test("readyOnly alone: p_ready_only true, no readiness fields, the marked cursor, X-Relayhistory-Ready-Only: applied", async () => {
    const { get } = harness(ON);
    const r = await get("&readyOnly=true&limit=2");
    expect(r.status).toBe(200);
    expect(listCall(r.statements).text).toBe(DIRECTORY_LIST_READINESS_SQL);
    expect(listCall(r.statements).params.at(-1)).toBe(true);
    for (const s of r.body.sessions) expect(s).not.toHaveProperty("eventsReady");
    expect(r.body.nextCursor).toBe(encodeReadyListCursor({ lastActivityAt: "2026-10-04T12:00:00.000001Z", sessionId: "s-2", harness: "codex" }));
    expect(r.headers.get(READY_ONLY_HEADER)).toBe("applied");
    expect(Object.keys(r.body).sort()).toEqual(["correlationId", "nextCursor", "sessions"]);
  });

  test("both together; and the q read: query_session_directory_readiness_for with $28 = readyOnly", async () => {
    const { get } = harness(ON);
    const both = await get("&readyOnly=true&includeReadiness=true&limit=2");
    expect(listCall(both.statements).params.at(-1)).toBe(true);
    expect(both.body.sessions[0]).toMatchObject({ eventsReady: true });
    expect(both.headers.get(READY_ONLY_HEADER)).toBe("applied");
    for (const [extra, readyOnly] of [["&readyOnly=true", true], ["&includeReadiness=true", false]] as const) {
      const r = await get(`${Q}${extra}&limit=2`);
      expect(r.status).toBe(200);
      expect(r.statements[0]!.params).toEqual([QUERY_READINESS_FOR_SIGNATURE]);
      expect(queryCall(r.statements).text).toBe(DIRECTORY_QUERY_READINESS_SQL);
      expect(queryCall(r.statements).params).toHaveLength(28);
      expect(queryCall(r.statements).params.at(-1)).toBe(readyOnly);
      expect(r.headers.get(READY_ONLY_HEADER)).toBe(readyOnly ? "applied" : null);
    }
  });

  test("migration 062200 not applied: either param is 501 directory_not_migrated; without them the old functions still serve", async () => {
    const { get } = harness(ON, false);
    for (const p of ["&readyOnly=true", "&includeReadiness=true", `${Q}&readyOnly=true`]) {
      const r = await get(p);
      expect([p, r.status, r.body.error.reason]).toEqual([p, 501, "directory_not_migrated"]);
      expect(r.headers.get(READY_ONLY_HEADER)).toBeNull();
    }
  });

  test("the readiness SQL is the pre-readiness SQL plus two columns, the readiness function and p_ready_only only", () => {
    const strip = (sql: string) => sql
      .replace(",\n       events_ready, event_projected_at", "")
      .replace("_readiness_for(", "_for(")
      .replace(/, p_ready_only => \$\d+\)$/u, ")");
    expect(strip(DIRECTORY_LIST_READINESS_SQL)).toBe(DIRECTORY_LIST_SQL);
    expect(strip(DIRECTORY_QUERY_READINESS_SQL)).toBe(DIRECTORY_QUERY_SQL);
    expect(DIRECTORY_LIST_READINESS_SQL).toContain("p_ready_only => $17)");
    expect(DIRECTORY_QUERY_READINESS_SQL).toContain("p_ready_only => $28)");
  });

  test("a readiness row outside its shape is a parse error, never a guessed value", () => {
    const good = ROWS[0]!;
    expect(sessionFromDirectoryRow(good, { includeReadiness: true })).toMatchObject({ eventsReady: true, eventsProjectedAt: "2026-10-05T13:00:00.000Z" });
    for (const bad of [{ events_ready: undefined }, { events_ready: "true" as never }, { events_ready: null as never }, { event_projected_at: "not a time" }])
      expect(() => sessionFromDirectoryRow({ ...good, ...bad }, { includeReadiness: true })).toThrow("session directory: parse");
    expect(sessionFromDirectoryRow({ ...good, events_ready: undefined })).not.toHaveProperty("eventsReady");
  });
});

describe("the cursor binds readyOnly", () => {
  const position = { lastActivityAt: "2026-10-04T12:00:00.000001Z", sessionId: "s-2", harness: "codex" };
  const marked = encodeReadyListCursor(position);
  const plain = encodeListCursor(position);
  const composite = encodeCompositeListCursor(position, { ts: "2026-10-04T12:00:00.000001Z", sessionId: "b-1", source: "claude" });

  test("the marked cursor is the b96s1 4-tuple; the pre-readiness decoders refuse it as they are", () => {
    const body = JSON.parse(atob(marked.slice("b96s1:".length).replace(/-/gu, "+").replace(/_/gu, "/")));
    expect(body).toEqual([position.lastActivityAt, position.sessionId, position.harness, READY_LIST_CURSOR_MARKER]);
    expect(READY_LIST_CURSOR_MARKER).toBe("ready1");
    // Pinned so that a later decoder edit cannot start accepting a readyOnly cursor.
    expect(decodeListCursor(marked)).toBe("invalid");
    expect(decodeCompositeListCursor(marked)).toBe("invalid");
    expect(decodeQueryCursor(marked)).toBe("foreign");
    // And the readyOnly decoder accepts only the marked form.
    expect(decodeReadyListCursor(marked)).toEqual(position);
    expect(decodeReadyListCursor(plain)).toBe("invalid");
    expect(decodeReadyListCursor(composite)).toBe("invalid");
    expect(decodeReadyListCursor(encodeQueryCursor({ ...position, fingerprint: "0".repeat(32) }))).toBe("foreign");
    expect(decodeReadyListCursor(`b96s1:${btoa(JSON.stringify([position.lastActivityAt, position.sessionId, position.harness, "ready2"]))}`)).toBe("invalid");
    expect(decodeReadyListCursor(undefined)).toBeUndefined();
    expect(decodeReadyListCursor("sessions-v2:x")).toBe("foreign");
  });

  test("a readyOnly cursor without readyOnly is invalid_cursor, flag on and flag off; a plain cursor with readyOnly is invalid_cursor", async () => {
    const invalid = { code: "bad_request", message: "cursor is not valid" };
    for (const deps of [{}, ON]) {
      const { get } = harness(deps);
      for (const extra of ["", deps === ON ? "&includeReadiness=true" : ""]) {
        const r = await get(`${extra}&cursor=${encodeURIComponent(marked)}`);
        expect([deps, extra, r.status, r.body.error]).toEqual([deps, extra, 400, invalid]);
      }
    }
    const { get } = harness(ON);
    for (const c of [plain, composite]) {
      const r = await get(`&readyOnly=true&cursor=${encodeURIComponent(c)}`);
      expect([r.status, r.body.error]).toEqual([400, invalid]);
    }
    const ok = await get(`&readyOnly=true&cursor=${encodeURIComponent(marked)}`);
    expect(ok.status).toBe(200);
    const listParams = ok.statements.find((x) => x.text.includes("FROM sessions.list_session_directory"))!.params;
    expect(listParams.slice(10, 13)).toEqual([position.lastActivityAt, position.sessionId, position.harness]);
    // includeReadiness alone never changes the cursor: the plain cursor continues.
    expect((await get(`&includeReadiness=true&cursor=${encodeURIComponent(plain)}`)).status).toBe(200);
  });

  test("a q cursor replayed across a readyOnly change, either direction, is invalid_cursor; includeReadiness does not change it", async () => {
    const { get } = harness(ON);
    const withReady = (await get(`${Q}&readyOnly=true&limit=2`)).body.nextCursor as string;
    const without = (await get(`${Q}&limit=2`)).body.nextCursor as string;
    expect(withReady).not.toBe(without);
    const terms = (parseSessionQuery("hello world") as { terms: SessionQueryTerms }).terms;
    expect((decodeQueryCursor(without) as { fingerprint: string }).fingerprint).toBe(queryFingerprint(terms, {}));
    expect((decodeQueryCursor(withReady) as { fingerprint: string }).fingerprint).toBe(queryFingerprint(terms, {}, true));
    for (const [cursor, extra] of [[withReady, ""], [withReady, "&includeReadiness=true"], [without, "&readyOnly=true"]] as const) {
      const r = await get(`${Q}${extra}&cursor=${encodeURIComponent(cursor)}`);
      expect([extra, r.status, r.body.error?.message]).toEqual([extra, 400, "cursor is not valid"]);
    }
    expect((await get(`${Q}&readyOnly=true&cursor=${encodeURIComponent(withReady)}`)).status).toBe(200);
    expect((await get(`${Q}&includeReadiness=true&cursor=${encodeURIComponent(without)}`)).status).toBe(200);
    // A marked plain cursor is not a q cursor either.
    expect((await get(`${Q}&readyOnly=true&cursor=${encodeURIComponent(marked)}`)).body.error?.message).toBe("cursor is not valid");
  });
});
