/**
 * GET /v1/session-bindings (migration 20261105062380): the flag (501 when off), auth, the workspace check (403), the
 * attestation header, the `session` parsing and bounds, the 501s (not configured, not migrated, no reader), the body
 * shape, and that a malformed reader row is a 5xx parse error, never a partial answer. The SQL itself is
 * src/hoopdb/__tests__/session-directory-binding.pglite.test.ts (and .postgres.test.ts on real PostgreSQL).
 */
import { expect, test } from "bun:test";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { DIRECTORY_BINDINGS_MAX, sessionBindings, type Directory, type DirectoryBindingRow } from "../directory.ts";
import { memoryLiveness } from "./support.ts";

const ID: VerifiedIdentity = { userId: "u-1", orgId: "org-a", workspaceId: "ws-a", scopes: ["rth:read"] };
const B1 = "0b6a3f0e-2c1d-4e5f-8a9b-0c1d2e3f4a5b";
const B2 = "9f8e7d6c-5b4a-4c3d-9e2f-1a0b9c8d7e6f";
const ROWS: DirectoryBindingRow[] = [
  { harness: "claude", session_id: "s:with:colons", session_binding_id: B2, session_binding_revision: "41", session_binding_created_at: "2026-11-01T09:00:00.000001Z" },
  { harness: "codex", session_id: "s1", session_binding_id: B1, session_binding_revision: "7", session_binding_created_at: new Date("2026-11-02T10:00:00Z") },
];
const PATH = "/v1/session-bindings?workspace=ws-a&session=codex:s1&session=claude:s:with:colons&session=codex:gone";

function api(o: { rows?: DirectoryBindingRow[] | null; enabled?: boolean; bindings?: false; directory?: false; allowed?: boolean } = {}) {
  const calls: unknown[][] = [];
  const unexpected = async (): Promise<never> => { throw new Error("only bindings() is read"); };
  const directory: Directory = { list: unexpected, activity: unexpected, query: unexpected, queryActivity: unexpected };
  if (o.bindings !== false)
    directory.bindings = async (...args) => {
      calls.push(args);
      return o.rows === undefined ? ROWS : o.rows;
    };
  const app = createReadApi({
    verify: async (t) => {
      if (t !== "good") throw new AuthError(401, "invalid_token", "token is not valid");
      return ID;
    },
    ch: { database: "x" } as never,
    liveness: memoryLiveness().liveness,
    completeSinceMs: 0,
    log: () => {},
    directory: o.directory === false ? undefined : directory,
    directoryWorkspaces: o.allowed === false ? new Set() : new Set(["ws-a"]),
    ...(o.enabled === false ? {} : { sessionBindings: true }),
  });
  const get = (path = PATH, token: string | null = "good") => app.request(path, { headers: token ? { authorization: `Bearer ${token}` } : {} });
  return { get, calls };
}
const error = async (r: Response) => ((await r.json()) as { error?: { code?: string; message?: string } }).error;

test("the live bindings of the listed sessions, tenant from the token, attested, exactly the binding fields", async () => {
  const a = api();
  const r = await a.get();
  expect(r.status).toBe(200);
  expect(r.headers.get("x-relayhistory-workspace-id")).toBe("ws-a");
  const body = await r.json();
  expect(body).toEqual({
    bindings: [
      { source: "claude", sessionId: "s:with:colons", sessionBindingId: B2, bindingRevision: "41", bindingCreatedAt: "2026-11-01T09:00:00.000Z" },
      { source: "codex", sessionId: "s1", sessionBindingId: B1, bindingRevision: "7", bindingCreatedAt: "2026-11-02T10:00:00.000Z" },
    ],
  });
  for (const b of body.bindings) expect(Object.keys(b).sort()).toEqual(["bindingCreatedAt", "bindingRevision", "sessionBindingId", "sessionId", "source"]);
  // Split at the first ':' only; duplicates are answered once.
  expect(a.calls).toEqual([["org-a", "ws-a", [
    { harness: "codex", sessionId: "s1" }, { harness: "claude", sessionId: "s:with:colons" }, { harness: "codex", sessionId: "gone" },
  ]]]);
  const dup = api();
  await dup.get("/v1/session-bindings?workspace=ws-a&session=codex:s1&session=codex:s1");
  expect(dup.calls).toEqual([["org-a", "ws-a", [{ harness: "codex", sessionId: "s1" }]]]);
});

test("off unless the flag is on: 501 session_bindings_not_enabled, and the reader is never called", async () => {
  const a = api({ enabled: false });
  const r = await a.get();
  expect(r.status).toBe(501);
  expect((await error(r))?.message).toContain("session_bindings_not_enabled");
  expect(a.calls).toEqual([]);
});

test("auth and workspace: 401 without a valid token, 403 for another workspace", async () => {
  expect((await api().get(PATH, null)).status).toBe(401);
  expect((await api().get(PATH, "bad")).status).toBe(401);
  expect((await api().get("/v1/session-bindings?workspace=ws-b&session=codex:s1")).status).toBe(403);
});

test("session parsing and bounds are 400; an unknown parameter is 501", async () => {
  for (const bad of ["", "&session=nocolon", "&session=:s1", "&session=codex:", "&session=Codex:s1", "&session=codex:%01x", `&session=codex:${"x".repeat(256)}`]) {
    const r = await api().get(`/v1/session-bindings?workspace=ws-a${bad}`);
    expect([bad, r.status]).toEqual([bad, 400]);
  }
  const many = Array.from({ length: DIRECTORY_BINDINGS_MAX + 1 }, (_, i) => `session=codex:s${i}`).join("&");
  expect((await api().get(`/v1/session-bindings?workspace=ws-a&${many}`)).status).toBe(400);
  const max = Array.from({ length: DIRECTORY_BINDINGS_MAX }, (_, i) => `session=codex:s${i}`).join("&");
  expect((await api({ rows: [] }).get(`/v1/session-bindings?workspace=ws-a&${max}`)).status).toBe(200);
  const r = await api().get("/v1/session-bindings?workspace=ws-a&session=codex:s1&source=codex");
  expect(r.status).toBe(501);
  expect((await error(r))?.message).toContain("param_source");
});

test("501 when the directory is not configured, the backend has no reader, or the migration is not applied", async () => {
  expect((await error(await api({ directory: false }).get()))?.message).toContain("directory_not_configured");
  expect((await error(await api({ bindings: false }).get()))?.message).toContain("session_bindings_not_supported");
  expect((await error(await api({ rows: null }).get()))?.message).toContain("session_bindings_not_migrated");
  expect((await api({ allowed: false }).get()).status).toBe(501);
});

test("a reader row that answers no requested key, repeats one, or is malformed is a parse error, never a partial answer", () => {
  const keys = [{ harness: "codex", sessionId: "s1" }];
  const ok = ROWS[1]!;
  expect(sessionBindings([ok], keys)).toHaveLength(1);
  expect(() => sessionBindings([{ ...ok, session_id: "other" }], keys)).toThrow();
  expect(() => sessionBindings([ok, ok], keys)).toThrow();
  expect(() => sessionBindings([{ ...ok, session_binding_id: "not-a-uuid" }], keys)).toThrow();
  expect(() => sessionBindings([{ ...ok, session_binding_revision: "0" }], keys)).toThrow();
  expect(() => sessionBindings([{ ...ok, session_binding_revision: "-3" }], keys)).toThrow();
  expect(() => sessionBindings([{ ...ok, session_binding_created_at: "never" }], keys)).toThrow();
});
