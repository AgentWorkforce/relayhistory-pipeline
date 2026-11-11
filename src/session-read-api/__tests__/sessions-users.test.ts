/**
 * GET /v1/sessions/users: auth, the workspace check (403), the attestation header, the
 * lastTsAfter/lastTsBefore validation and span of GET /v1/sessions and /v1/sessions/activity,
 * the 501s (not configured, not migrated, no aggregate), the truncation cap, and a body
 * carrying only user ids, counts, times and project names. The SQL itself is
 * src/hoopdb/__tests__/session-directory-users.test.ts.
 */
import { expect, test } from "bun:test";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { DIRECTORY_USERS_MAX, DirectoryError, type Directory, type DirectoryUserRow } from "../directory.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };
const AFTER = "2026-10-01T00:00:00.000Z";
const BEFORE = "2026-10-31T00:00:00.000Z";
const PATH = `/v1/sessions/users?workspace=ws-a&lastTsAfter=${AFTER}&lastTsBefore=${BEFORE}`;

const ROWS: DirectoryUserRow[] = [
  { user_id: "u-2", sessions: "7", last_session_at: new Date("2026-10-20T10:00:00Z"), top_projects: [{ name: "github.com/acme/api", sessions: 5 }, { name: "p-b", sessions: "2" }] },
  { user_id: "u-1", sessions: 3, last_session_at: "2026-10-05T00:00:00.123Z", top_projects: [] },
];

function api(o: { rows?: DirectoryUserRow[] | null; fail?: unknown; users?: false; directory?: false; identity?: VerifiedIdentity; allowed?: boolean } = {}) {
  const calls: unknown[][] = [];
  const unexpected = async (): Promise<never> => { throw new Error("only users() is read"); };
  const directory: Directory = { list: unexpected, activity: unexpected, query: unexpected, queryActivity: unexpected };
  if (o.users !== false)
    directory.users = async (...args) => {
      calls.push(args);
      if (o.fail) throw o.fail;
      return o.rows === undefined ? ROWS : o.rows;
    };
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return o.identity ?? ID;
    },
    ch: { database: "x" } as never,
    liveness: memoryLiveness().liveness,
    completeSinceMs: 0,
    log: () => {},
    directory: o.directory === false ? undefined : directory,
    directoryWorkspaces: o.allowed === false ? new Set() : new Set(["ws-a"]),
  });
  const get = (path = PATH, token: string | null = "good") => app.request(path, { headers: token ? { authorization: `Bearer ${token}` } : {} });
  return { get, calls };
}
const errorCode = async (r: Response) => ((await r.json()) as { error?: { code?: string; message?: string } }).error;

test("per-user counts from one aggregate call, tenant from the token, attested, nothing but ids, counts, times and project names", async () => {
  const a = api();
  const r = await a.get();
  expect(r.status).toBe(200);
  expect(r.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");
  const body = await r.json();
  expect(body).toEqual({
    lastTsAfter: AFTER,
    lastTsBefore: BEFORE,
    users: [
      { userId: "u-2", sessions: 7, lastSessionAt: "2026-10-20T10:00:00.000Z", topProjects: [{ name: "github.com/acme/api", sessions: 5 }, { name: "p-b", sessions: 2 }] },
      { userId: "u-1", sessions: 3, lastSessionAt: "2026-10-05T00:00:00.123Z", topProjects: [] },
    ],
    truncated: false,
  });
  // Exactly these keys at every level: no titles, summaries, session ids or other content.
  expect(Object.keys(body).sort()).toEqual(["lastTsAfter", "lastTsBefore", "truncated", "users"]);
  for (const u of body.users) {
    expect(Object.keys(u).sort()).toEqual(["lastSessionAt", "sessions", "topProjects", "userId"]);
    for (const p of u.topProjects) expect(Object.keys(p).sort()).toEqual(["name", "sessions"]);
  }
  expect(a.calls).toEqual([["org-a", "ws-a", new Date(AFTER), new Date(BEFORE), DIRECTORY_USERS_MAX]]);
});

test("caps the users at DIRECTORY_USERS_MAX and marks truncated when the aggregate returned one more", async () => {
  const many = (n: number): DirectoryUserRow[] =>
    Array.from({ length: n }, (_, i) => ({ user_id: `u-${String(i).padStart(4, "0")}`, sessions: 1, last_session_at: new Date(AFTER), top_projects: [] }));
  const full = await (await api({ rows: many(DIRECTORY_USERS_MAX + 1) }).get()).json();
  expect(full.users).toHaveLength(DIRECTORY_USERS_MAX);
  expect(full.users.at(-1).userId).toBe(`u-${String(DIRECTORY_USERS_MAX - 1).padStart(4, "0")}`);
  expect(full.truncated).toBe(true);
  const exact = await (await api({ rows: many(DIRECTORY_USERS_MAX) }).get()).json();
  expect(exact.users).toHaveLength(DIRECTORY_USERS_MAX);
  expect(exact.truncated).toBe(false);
  // More than limit + 1 rows breaks the contract: never served.
  expect((await api({ rows: many(DIRECTORY_USERS_MAX + 2) }).get()).status).toBe(501);
});

test("auth, workspace (required, once, equal to the token's), scope and canary precede the read", async () => {
  for (const [opts, path, token, status] of [
    [{}, PATH, null, 401],
    [{}, PATH, "bad", 401],
    [{}, PATH.replace("workspace=ws-a", "workspace=ws-b"), "good", 403],
    [{}, PATH.replace("workspace=ws-a&", ""), "good", 501],
    [{}, PATH.replace("workspace=ws-a", "workspace=ws-a&workspace=ws-a"), "good", 400],
    [{ identity: { ...ID, scopes: ["rth:sync"] } }, PATH, "good", 403],
    [{ allowed: false }, PATH, "good", 501],
  ] as const) {
    const a = api(opts);
    const r = await a.get(path, token);
    expect(r.status).toBe(status);
    expect(a.calls).toHaveLength(0);
  }
  // The 403 for another workspace never reads it.
  const other = await api().get(PATH.replace("workspace=ws-a", "workspace=ws-b"));
  expect((await errorCode(other))?.code).toBe("forbidden");
});

test("lastTsAfter/lastTsBefore: both required, dates, ordered, at most 92 UTC days; other parameters are not ignored", async () => {
  const base = "/v1/sessions/users?workspace=ws-a";
  for (const q of [
    "",
    `&lastTsAfter=${AFTER}`,
    `&lastTsBefore=${BEFORE}`,
    `&lastTsAfter=nope&lastTsBefore=${BEFORE}`,
    `&lastTsAfter=${BEFORE}&lastTsBefore=${AFTER}`,
    `&lastTsAfter=${AFTER}&lastTsAfter=${AFTER}&lastTsBefore=${BEFORE}`,
    "&lastTsAfter=2026-01-01T00:00:00Z&lastTsBefore=2026-11-01T00:00:00Z",
  ]) {
    const a = api();
    const r = await a.get(base + q);
    expect(r.status).toBe(400);
    expect(a.calls).toHaveLength(0);
  }
  // 92 UTC days is the maximum span (as /v1/sessions/activity); 93 is refused.
  expect((await api().get(`${base}&lastTsAfter=2026-08-01T00:00:00Z&lastTsBefore=2026-10-31T23:59:59Z`)).status).toBe(200);
  expect((await api().get(`${base}&lastTsAfter=2026-08-01T00:00:00Z&lastTsBefore=2026-11-01T00:00:00Z`)).status).toBe(400);
  // A filter this aggregate does not apply is a 501, never silently dropped.
  for (const extra of ["&user=u-1", "&source=codex", "&limit=10", "&q=x"]) {
    const a = api();
    const r = await a.get(PATH + extra);
    expect(r.status).toBe(501);
    expect(a.calls).toHaveLength(0);
  }
});

test("501 when the backend cannot answer: no directory, no aggregate, function not installed, out-of-contract rows", async () => {
  expect((await api({ directory: false }).get()).status).toBe(501);
  const noUsers = await api({ users: false }).get();
  expect(noUsers.status).toBe(501);
  expect((await errorCode(noUsers))?.message ?? "").toContain("directory_users_not_supported");
  const notMigrated = await api({ rows: null }).get();
  expect(notMigrated.status).toBe(501);
  expect(JSON.stringify(await errorCode(notMigrated))).toContain("directory_not_migrated");
  for (const bad of [
    [{ user_id: "", sessions: 1, last_session_at: AFTER, top_projects: [] }],
    [{ user_id: "u", sessions: -1, last_session_at: AFTER, top_projects: [] }],
    [{ user_id: "u", sessions: 1, last_session_at: "nope", top_projects: [] }],
    [{ user_id: "u", sessions: 1, last_session_at: AFTER, top_projects: [{ name: "a", sessions: 1 }, { name: "b", sessions: 1 }, { name: "c", sessions: 1 }, { name: "d", sessions: 1 }] }],
    [{ user_id: "u", sessions: 1, last_session_at: AFTER, top_projects: [{ name: null, sessions: 1 }] }],
  ] as DirectoryUserRow[][])
    expect((await api({ rows: bad }).get()).status).toBe(501);
  // Postgres refusals: an invalid argument or the tenant pin are 503, never an empty answer.
  expect((await api({ fail: new DirectoryError("invalid_argument") }).get()).status).toBe(503);
  expect((await api({ fail: new DirectoryError("refused") }).get()).status).toBe(503);
});
