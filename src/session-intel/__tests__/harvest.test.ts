/**
 * The read-API harvest (docs/session-intel-harvest.md) against an in-process fake read API with synthetic
 * sessions only: pagination to exhaustion and resume, the workspace attestation, the token's privacy,
 * Retry-After, the request cap, the unavailable-field receipt, the routes it must never call, no text in the
 * output, and harvest followed by the unchanged replay CLI.
 */
import { afterAll, describe, expect, test } from "bun:test";
import { appendFileSync, existsSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { Readable } from "node:stream";
import { carriesTokenFragment, HARVEST_ROUTES, parseReadApi, ReadApiClient, redact, retryAfterMs, TOKEN_ENV } from "../harvest/client.ts";
import { EXIT_CAPPED, failureMessage, main, type HarvestIo } from "../harvest/cli.ts";
import { RECEIPT_FILE, repoKey, sessionFacts, STATE_FILE, type FrameRow } from "../harvest/harvest.ts";
import { Hasher, IMPORT_BOUNDS } from "../import/normalize.ts";
import { mappingPath } from "../import/writer.ts";
import { main as replayMain } from "../replay/cli.ts";
import { loadSnapshot } from "../replay/snapshot.ts";
import { FIXTURE_LABELS, tmp } from "./helpers.ts";

const WS = "ws_fx_harvest";
const TOKEN = "rth_st_read_FIXTURETOKENabcdefghijklmnopqrstuvwxyz0123456789";
const SALT = "fixture-harvest-salt-0123456789";
const NOW = new Date("2026-11-04T00:00:00.000Z");
const SINCE = "2026-11-01T00:00:00.000Z", UNTIL = "2026-11-03T00:00:00.000Z";
const DELETE_BY = "2026-11-10T00:00:00.000Z";
const REPO = resolve(import.meta.dir, "..", "..", "..");
const at = (d: string, hm: string) => `2026-11-${d}T${hm}:00.000Z`;

interface FxEvent { eventId: string; ts: string; kind: string; filesTouched: string[]; projectId?: string | null }
interface FxSession {
  source: string; sessionId: string; firstTs: string; lastTs: string; projectId: string | null;
  catalog?: { repoUrl: string | null; gitBranch: string | null; parentSessionIds: string[]; workspaceId?: string };
  events?: FxEvent[]; eventsUnsupported?: string;
}
/** Text the fake server returns in every text field; none of it may reach any output. */
const TEXT = (s: string) => `FIXTURE_TEXT_${s}`;

const MIGRATION = "supabase/migrations/0042_add_index.sql";
const SESSIONS: FxSession[] = [
  { source: "claude", sessionId: "fx-h-a", firstTs: at("01", "09:00"), lastTs: at("01", "10:00"), projectId: "fixture-proj",
    catalog: { repoUrl: "https://example.invalid/fixture-org/acme-api.git", gitBranch: "feat/fx-a", parentSessionIds: [] },
    events: [
      { eventId: "ev-a-1", ts: at("01", "09:05"), kind: "message", filesTouched: [] },
      { eventId: "ev-a-2", ts: at("01", "09:10"), kind: "tool_call", filesTouched: [MIGRATION] },
      { eventId: "ev-a-3", ts: at("01", "09:20"), kind: "tool_call", filesTouched: ["/Users/fixture-user/src-checkouts/acme-api/src/widgets/util.ts"] },
    ] },
  { source: "codex", sessionId: "fx-h-b", firstTs: at("01", "09:25"), lastTs: at("01", "10:30"), projectId: "fixture-proj",
    // A remote with embedded credentials: only the repository name is ever kept.
    catalog: { repoUrl: "https://FIXTURE_CRED_user:FIXTURE_CRED_pass@example.invalid/fixture-org/acme-api.git", gitBranch: "feat/fx-b", parentSessionIds: [] },
    events: [{ eventId: "ev-b-1", ts: at("01", "09:31"), kind: "tool_call", filesTouched: [MIGRATION] }] },
  { source: "claude", sessionId: "fx-h-c", firstTs: at("02", "08:00"), lastTs: at("02", "09:00"), projectId: "fixture-proj",
    catalog: { repoUrl: "https://example.invalid/fixture-org/acme-api.git", gitBranch: "feat/fx-a", parentSessionIds: ["fx-h-a"] },
    events: [
      { eventId: "ev-c-0", ts: at("02", "08:05"), kind: "tool_call", filesTouched: ["/home/fixture-user/work/acme-api/src/widgets/util.ts", "/home/fixture-user/notes.txt"] },
      { eventId: "ev-c-1", ts: at("02", "08:10"), kind: "tool_call", filesTouched: ["docs/guide.md"] },
      // After --until: never stored, and the paging stops here.
      { eventId: "ev-c-2", ts: "2026-11-03T01:00:00.000Z", kind: "tool_call", filesTouched: ["docs/late.md"] },
    ] },
  { source: "opencode", sessionId: "fx-h-d", firstTs: at("02", "10:00"), lastTs: at("02", "11:00"), projectId: null, eventsUnsupported: "session_partial" },
  // Before the window: the server's filter leaves it out.
  { source: "claude", sessionId: "fx-h-old", firstTs: "2026-10-20T09:00:00.000Z", lastTs: "2026-10-20T10:00:00.000Z", projectId: "fixture-proj", events: [] },
];

/** The fixture without fx-h-d (whose events are 501), so a harvest of it can be complete. */
const COMPLETE = SESSIONS.filter((s) => s.sessionId !== "fx-h-d");

interface ServerOptions {
  pageSize?: number; eventPageSize?: number; workspaceHeader?: string; catalogWorkspace?: string;
  retryAfter?: { times: number; value: string; status?: number }; redirect?: boolean; failAfter?: number;
  errorCode?: string; status?: number; outputUnsupported?: boolean; sessions?: FxSession[];
  /** X-Relayhistory-List-Served-By for the n-th frame page served (1-based); null or absent sends none. */
  servedBy?: (page: number) => string | null;
  /** X-Sessions-Read-Backend on every response: `96bulls` by default (the 96Bulls read API); null sends none. */
  readBackend?: string | null;
  /** An empty first page (zero rows and a cursor), and an empty last page (zero rows, no cursor). */
  leadingEmpty?: boolean; trailingEmpty?: boolean;
  /** X-Relayhistory-List-Mode for the n-th frame page served; null or absent sends none. */
  listMode?: (page: number) => string | null;
  /** X-Relayhistory-Buffered on every response. */
  buffered?: string;
}
interface LoggedRequest { path: string; params: URLSearchParams; authorization: string | null }

function fakeReadApi(opts: ServerOptions = {}) {
  const log: LoggedRequest[] = [];
  let retries = opts.retryAfter?.times ?? 0;
  const pageSize = opts.pageSize ?? 2, eventPageSize = opts.eventPageSize ?? 2;
  let framePages = 0;
  const json = (body: unknown, status = 200, headers: Record<string, string> = {}) => Response.json(body, {
    status, headers: {
      "x-relayhistory-workspace-id": opts.workspaceHeader ?? WS,
      ...(opts.readBackend === null ? {} : { "x-sessions-read-backend": opts.readBackend ?? "96bulls" }),
      ...(opts.buffered === undefined ? {} : { "x-relayhistory-buffered": opts.buffered }), ...headers,
    },
  });
  const server = Bun.serve({
    hostname: "127.0.0.1", port: 0,
    fetch(req) {
      const url = new URL(req.url);
      log.push({ path: url.pathname, params: url.searchParams, authorization: req.headers.get("authorization") });
      if (req.headers.get("authorization") !== `Bearer ${TOKEN}`) return Response.json({ error: { code: "invalid_token", message: "no" } }, { status: 401 });
      if (url.searchParams.get("workspace") !== WS) return Response.json({ error: { code: "forbidden", message: "no" } }, { status: 403 });
      if (opts.failAfter !== undefined && log.length > opts.failAfter) return json({ error: { code: "bad_request", message: "simulated" } }, 400);
      if (opts.status) return json({ error: { code: opts.errorCode ?? "bad_request", message: `echo ${TOKEN}`, reason: opts.errorCode } }, opts.status);
      if (retries > 0) { retries--; return json({ error: { code: "busy" } }, opts.retryAfter!.status ?? 429, { "retry-after": opts.retryAfter!.value }); }
      if (opts.redirect) return new Response(null, { status: 302, headers: { location: `${url.origin}/elsewhere` } });
      const p = url.pathname;
      if (p === "/v1/sessions") {
        expect(url.searchParams.get("limit")).toBe("200");
        const after = url.searchParams.get("lastTsAfter")!, before = url.searchParams.get("firstTsBefore")!;
        const rows = (opts.sessions ?? SESSIONS).filter((s) => s.lastTs >= after && s.firstTs < before).sort((a, b) => (a.lastTs < b.lastTs ? 1 : -1));
        const n = ++framePages;
        const served = opts.servedBy?.(n) ?? null, mode = opts.listMode?.(n) ?? null;
        const listHeader: Record<string, string> = {
          ...(served === null ? {} : { "x-relayhistory-list-served-by": served }),
          ...(mode === null ? {} : { "x-relayhistory-list-mode": mode }),
        };
        const cursor = url.searchParams.get("cursor");
        if (opts.leadingEmpty && cursor === null) return json({ sessions: [], nextCursor: "0", correlationId: "fx" }, 200, listHeader);
        const offset = Number(cursor ?? "0");
        const page = rows.slice(offset, offset + pageSize);
        const more = offset + pageSize < rows.length || (opts.trailingEmpty === true && offset < rows.length);
        return json({
          sessions: page.map((s) => ({
            title: TEXT(`title_${s.sessionId}`), taskTitle: TEXT(`task_${s.sessionId}`), shortSummary: TEXT(`summary_${s.sessionId}`),
            sessionId: s.sessionId, userIds: ["fixture-user-id"], source: s.source, projectId: s.projectId, machineIds: ["fixture-machine"],
            firstTs: s.firstTs, lastTs: s.lastTs, workState: null, eventCount: s.events?.length ?? 0, kinds: [], models: ["fixture-model"],
            ...(s.catalog ? { catalog: { workspaceId: opts.catalogWorkspace ?? WS, title: TEXT(`ctitle_${s.sessionId}`), projectKey: "acme-api", ...s.catalog } } : {}),
          })),
          nextCursor: more ? String(offset + pageSize) : null, correlationId: "fx",
        }, 200, listHeader);
      }
      const m = /^\/v1\/sessions\/([^/]+)\/events$/u.exec(p);
      if (m) {
        const s = (opts.sessions ?? SESSIONS).find((x) => x.sessionId === decodeURIComponent(m[1]!) && x.source === url.searchParams.get("source"));
        if (!s) return json({ error: { code: "not_found" } }, 404);
        if (s.eventsUnsupported) return json({ error: { code: "unsupported_by_backend", reason: s.eventsUnsupported } }, 501);
        const offset = Number(url.searchParams.get("cursor") ?? "0");
        const events = (s.events ?? []).slice(offset, offset + eventPageSize);
        return json({
          sessionId: s.sessionId,
          // Full content despite maxContent: the client must not keep it anyway.
          events: events.map((e) => ({ ...e, sessionId: s.sessionId, source: s.source, type: "x", machineId: "fixture-machine", userId: "fixture-user-id",
            content: TEXT(`content_${e.eventId}`), contentTruncated: false, tags: [], toolName: "Write", toolStatus: null, taskTitle: TEXT("event_task"),
            projectId: e.projectId === undefined ? s.projectId : e.projectId, taskRef: {} })),
          nextCursor: offset + eventPageSize < (s.events ?? []).length ? String(offset + eventPageSize) : null, correlationId: "fx",
        });
      }
      if (p === "/v1/sessions/output") {
        if (opts.outputUnsupported) return json({ error: { code: "unsupported_by_backend", reason: "output_workspace_not_enabled" } }, 501);
        return json({ since: SINCE, until: UNTIL, output: { agentMessages: 7, commits: 3, linesAdded: 10, linesRemoved: 2, commitsWithoutLineCounts: 1 } });
      }
      // Detector output and anything else: answered, so a call would be visible in the log.
      return json({ overlaps: [], advisories: [] });
    },
  });
  servers.push(server);
  return { url: `http://127.0.0.1:${server.port}`, log, opts };
}
const servers: { stop(force?: boolean): void }[] = [];
afterAll(() => { for (const s of servers) s.stop(true); });

function io(over: Partial<HarvestIo> = {}) {
  const lines: string[] = [];
  const sleeps: number[] = [];
  const value: HarvestIo = {
    out: (l) => lines.push(l), err: (l) => lines.push(l), now: () => NOW, env: { [TOKEN_ENV]: TOKEN },
    stdin: Readable.from([SALT]), sleep: async (ms) => { sleeps.push(ms); }, ...over,
  };
  return { lines, sleeps, io: value };
}
const argv = (api: string, out: string, extra: string[] = []) => [
  "--read-api", api, "--workspace", WS, "--since", SINCE, "--until", UNTIL, "--out", out, "--salt-stdin", "--delete-by", DELETE_BY, ...extra,
];
const newOut = () => join(tmp("si-harvest-"), "snap");
/** Everything the harvest wrote: the snapshot files, the mapping and any state left behind. */
function written(out: string): string {
  const files = existsSync(out) ? readdirSync(out).map((f) => readFileSync(join(out, f), "utf8")) : [];
  if (existsSync(mappingPath(out))) files.push(readFileSync(mappingPath(out), "utf8"));
  const parent = resolve(out, "..");
  for (const f of readdirSync(parent)) if (f !== "snap" && !f.endsWith(".mapping.json")) {
    const p = join(parent, f);
    if (statSync(p).isFile()) files.push(readFileSync(p, "utf8"));
  }
  return files.join("\n");
}
const receipt = (out: string) => JSON.parse(readFileSync(join(out, RECEIPT_FILE), "utf8"));

describe("harvest", () => {
  test("pages the directory to exhaustion and writes a snapshot the M0 loader accepts, with the receipt in the manifest", async () => {
    const api = fakeReadApi();
    const out = newOut();
    const q = io();
    expect(await main(argv(api.url, out), q.io)).toBe(0);
    const snap = await loadSnapshot(out, { now: NOW });
    expect(Object.keys(snap.manifest.files).sort()).toEqual([RECEIPT_FILE, "sessions.jsonl"]);
    expect(snap.manifest.window).toEqual({ since: SINCE, until: UNTIL });
    expect(snap.manifest.deleteBy).toBe(DELETE_BY);
    const h = new Hasher(SALT);
    expect(snap.sessions.map((s) => `${s.harness}:${s.sessionId}`).sort()).toEqual(
      [["claude", "fx-h-a"], ["claude", "fx-h-c"], ["codex", "fx-h-b"], ["opencode", "fx-h-d"]].map(([s, id]) => `${s}:${h.session(s!, id!)}`).sort());
    const a = snap.sessions.find((s) => s.sessionId === h.session("claude", "fx-h-a"))!;
    expect(a).toMatchObject({ orgId: h.org("org_readapi"), workspaceId: h.workspace(WS), projectId: h.project("fixture-proj"), repo: h.repo("acme-api"),
      branch: h.branch("feat/fx-a"), title: null, userId: null, taskRef: null, summaryTimeline: [], commits: [], outcomes: [], usage: [], claims: [],
      createdAt: at("01", "09:00"), lastActivityAt: at("01", "10:00") });
    // The absolute path has no cwd to strip, so it is anchored at its source root; segments are hashed.
    expect(a.writes.map((w) => [w.path.startsWith("supabase/migrations/0042_"), w.ts, w.kind, w.branch])).toEqual([
      [true, at("01", "09:10"), "tool_call", h.branch("feat/fx-a")], [false, at("01", "09:20"), "tool_call", h.branch("feat/fx-a")]]);
    expect(a.writes[1]!.path).toMatch(/^src\/[0-9a-f]{12}\/[0-9a-f]{12}\.ts$/u);
    const c = snap.sessions.find((s) => s.sessionId === h.session("claude", "fx-h-c"))!;
    expect(c.parentSessionIds).toEqual([h.session("claude", "fx-h-a")]);
    // The home-directory path is anchored at its source root; the one outside any source root is dropped.
    expect(c.writes.map((w) => w.path)).toEqual([a.writes[1]!.path, expect.stringMatching(/^docs\/[0-9a-f]{12}\.md$/u)]);
    const r = receipt(out);
    expect(r).toMatchObject({
      format: "session-intel-harvest-receipt/1", complete: false, workspaceId: h.workspace(WS), window: { since: SINCE, until: UNTIL },
      frame: { pages: 2, exhausted: true, sessions: 4, bySource: { claude: 2, codex: 1, opencode: 1 },
        byDay: { "2026-11-01": { claude: 1, codex: 1 }, "2026-11-02": { claude: 1, opencode: 1 } }, sessionsWithoutCatalog: 1, duplicateRows: 0 },
      facts: { sessionsRead: 3, sessionsNotRead: 0, sessionsWithoutEvents: { session_partial: 1 }, writesRead: 5, droppedPaths: 1 },
      snapshot: { sessions: 4, writes: 5 },
      output: { counts: { agentMessages: 7, commits: 3, linesAdded: 10, linesRemoved: 2, commitsWithoutLineCounts: 1 } },
      // Counted on answered (2xx) responses: every request but fx-h-d's 501.
      server: { headers: { "x-sessions-read-backend": { "96bulls": api.log.length - 1 } } },
    });
    // Two pages of fx-h-a's events, one of fx-h-b's, two of fx-h-c's (read to exhaustion; its last event is past --until), fx-h-d's 501.
    expect(r.requests.byRoute).toEqual({ "/v1/sessions": 2, "/v1/sessions/:sessionId/events": 6, "/v1/sessions/output": 1 });
    expect(r.requests.total).toBe(api.log.length);
    expect(q.lines.join("\n")).toContain("\"complete\":false");
    // Owner-only files, the mapping beside the snapshot, no state left behind.
    for (const f of readdirSync(out)) expect(statSync(join(out, f)).mode & 0o077).toBe(0);
    expect(statSync(mappingPath(out)).mode & 0o077).toBe(0);
    expect(existsSync(join(out, STATE_FILE))).toBe(false);
    const mapping = JSON.parse(readFileSync(mappingPath(out), "utf8"));
    expect(mapping.format).toBe("session-intel-harvest-mapping/1");
    expect(mapping.tenant).toEqual({ orgId: "org_readapi", workspaceId: WS });
    expect(mapping.sourceEventIds[h.session("claude", "fx-h-a")]).toEqual(["ev-a-2", "ev-a-3"]);
  });

  test("every request carries the workspace and the token in the Authorization header only; events are asked for ascending with content cut to one character", async () => {
    const api = fakeReadApi();
    expect(await main(argv(api.url, newOut()), io().io)).toBe(0);
    for (const r of api.log) {
      expect(r.authorization).toBe(`Bearer ${TOKEN}`);
      expect(r.params.get("workspace")).toBe(WS);
      expect(`${r.path}?${r.params}`).not.toContain(TOKEN.slice(0, 12));
    }
    for (const r of api.log.filter((x) => x.path.endsWith("/events")))
      expect([r.params.get("order"), r.params.get("maxContent"), r.params.get("limit")]).toEqual(["asc", "1", "1000"]);
    const frame = api.log.filter((x) => x.path === "/v1/sessions");
    expect(frame.map((x) => [x.params.get("lastTsAfter"), x.params.get("firstTsBefore"), x.params.get("cursor")])).toEqual([[SINCE, UNTIL, null], [SINCE, UNTIL, "2"]]);
  });

  test("never calls the overlaps or advisories routes, and the client refuses any coordination route", async () => {
    const api = fakeReadApi();
    expect(await main(argv(api.url, newOut()), io().io)).toBe(0);
    expect(api.log.length).toBeGreaterThan(0);
    expect(api.log.filter((r) => r.path.includes("/coordination"))).toEqual([]);
    expect(new Set(api.log.map((r) => r.path.replace(/^\/v1\/sessions\/[^/]+\/events$/u, "/v1/sessions/:sessionId/events")))).toEqual(new Set(HARVEST_ROUTES));
    const client = new ReadApiClient({ base: parseReadApi(api.url), token: TOKEN, workspace: WS, maxRequests: 10, concurrency: 1 });
    for (const route of ["/v1/coordination/overlaps", "/v1/coordination/advisories"])
      await expect(client.get(route as never, [])).rejects.toThrow("refused a route outside the harvest's list");
    // A session id cannot smuggle a coordination path either: it is one encoded segment.
    const before = api.log.length;
    await client.get("/v1/sessions/:sessionId/events", [["source", "claude"]], "../../coordination/overlaps");
    expect(api.log.slice(before).map((r) => r.path)).toEqual(["/v1/sessions/..%2F..%2Fcoordination%2Foverlaps/events"]);
    // A dot-segment id would be resolved by the URL parser into another route: refused before any request.
    for (const id of ["..", "."]) await expect(client.get("/v1/sessions/:sessionId/events", [], id)).rejects.toThrow("changes the request path");
    expect(api.log).toHaveLength(before + 1);
  });

  test("no title, summary, task title, content, user, machine or model text reaches any output", async () => {
    const api = fakeReadApi();
    const out = newOut();
    const q = io();
    expect(await main(argv(api.url, out), q.io)).toBe(0);
    const text = `${written(out)}\n${q.lines.join("\n")}`;
    expect(text).not.toContain("FIXTURE_TEXT_");
    for (const raw of ["fixture-user", "fixture-machine", "fixture-model", "acme-api", "feat/fx", "fixture-proj\"", "\"fx-h-a\"", "widgets", "util.ts", "add_index", SALT])
      expect(written(join(out)).replace(readFileSync(mappingPath(out), "utf8"), "")).not.toContain(raw);
    // The raw ids are only in the private mapping, beside the snapshot.
    expect(readFileSync(mappingPath(out), "utf8")).toContain("fx-h-a");
  });

  test("the token never appears in stdout, stderr, any file or any error, and is removed from process.env", async () => {
    const api = fakeReadApi();
    const out = newOut();
    const q = io({ env: undefined });
    process.env[TOKEN_ENV] = TOKEN;
    expect(await main(argv(api.url, out), q.io)).toBe(0);
    expect(process.env[TOKEN_ENV]).toBeUndefined();
    expect(`${written(out)}\n${q.lines.join("\n")}`).not.toContain(TOKEN.slice(0, 16));
    // A server that echoes the token in its error code and message.
    for (const opts of [{ status: 400, errorCode: TOKEN }, { status: 401, errorCode: TOKEN }, { status: 500, errorCode: TOKEN }, { redirect: true }, { workspaceHeader: "ws_other" }] as ServerOptions[]) {
      const bad = fakeReadApi(opts);
      const o = newOut();
      const err = await main(argv(bad.url, o), io().io).then(() => null, (e: unknown) => e as Error);
      expect(err).not.toBeNull();
      expect(err!.message).not.toContain(TOKEN.slice(0, 16));
      expect(failureMessage(err, TOKEN)).not.toContain(TOKEN.slice(0, 16));
      expect(written(o)).not.toContain(TOKEN.slice(0, 16));
    }
    expect(redact(`a ${TOKEN} b ${TOKEN.slice(0, 40)}`, TOKEN)).not.toContain(TOKEN.slice(0, 16));
    // A message cut at the start carries the token's tail; that is redacted too.
    expect(redact(`x ${TOKEN.slice(-40)} y`, TOKEN)).not.toContain(TOKEN.slice(-16));
    // A short server code carrying any 8 characters of the token, its tail included, is never kept.
    expect(carriesTokenFragment(`E_${TOKEN.slice(-9)}`, TOKEN)).toBe(true);
    expect(carriesTokenFragment("E_RATE_LIMITED", TOKEN)).toBe(false);
  });

  test("a catalog repo URL yields the repository name, never its credentials", () => {
    expect(repoKey("https://example.invalid/fixture-org/acme-api.git")).toBe("acme-api");
    expect(repoKey("https://FIXTURE_CRED_user:FIXTURE_CRED_pass@example.invalid/fixture-org/acme-api.git")).toBe("acme-api");
    expect(repoKey("git@example.invalid:fixture-org/acme-api.git")).toBe("acme-api");
    // No path after the host: the last segment would be the password or the host; nothing is kept.
    expect(repoKey("https://FIXTURE_CRED_user:FIXTURE_CRED_pass@example.invalid")).toBeNull();
    expect(repoKey("https://FIXTURE_CRED_tok@example.invalid/")).toBeNull();
    // A query or fragment never becomes the name.
    expect(repoKey("https://example.invalid/fixture-org/acme-api?access_token=FIXTURE_CRED_tok")).toBe("acme-api");
    expect(repoKey("https://example.invalid/fixture-org/acme-api#FIXTURE_CRED_tok")).toBe("acme-api");
    // An `@` left after stripping userinfo is refused rather than guessed.
    expect(repoKey("https://example.invalid/a@FIXTURE_CRED_tok")).toBeNull();
    // `;params`, percent escapes, text after a line break and a bare host:SECRET never become the name.
    expect(repoKey("https://example.invalid/fixture-org/acme-api.git;token=FIXTURE_CRED_tok")).toBe("acme-api");
    expect(repoKey("https://example.invalid/fixture-org/acme-api%3Btoken=FIXTURE_CRED_tok")).toBeNull();
    expect(repoKey("https://example.invalid/fixture-org/acme-api\nFIXTURE_CRED_tok")).toBeNull();
    expect(repoKey("example.invalid:FIXTURE_CRED_tok")).toBeNull();
    expect(repoKey("https://u:pa/ss@example.invalid/fixture-org/acme-api")).toBeNull();
    expect(repoKey("/srv/git/acme-api.git")).toBe("acme-api");
  });

  test("a response for another workspace is refused, by header or by a row's catalog", async () => {
    for (const opts of [{ workspaceHeader: "ws_other" }, { catalogWorkspace: "ws_other" }]) {
      const api = fakeReadApi(opts);
      await expect(main(argv(api.url, newOut()), io().io)).rejects.toThrow(/workspace/u);
    }
    // A missing header is no attestation.
    const api = fakeReadApi({ workspaceHeader: "" });
    await expect(main(argv(api.url, newOut()), io().io)).rejects.toThrow("did not attest --workspace");
  });

  test("redirects are never followed", async () => {
    const api = fakeReadApi({ redirect: true });
    await expect(main(argv(api.url, newOut()), io().io)).rejects.toThrow("redirects are never followed");
    expect(api.log.map((r) => r.path)).toEqual(["/v1/sessions"]);
  });

  test("Retry-After is honoured on 429 and 503, counted, and a wait over the bound fails instead of being cut short", async () => {
    for (const status of [429, 503]) {
      const api = fakeReadApi({ retryAfter: { times: 2, value: "7", status } });
      const out = newOut();
      const q = io();
      expect(await main(argv(api.url, out), q.io)).toBe(0);
      expect(q.sleeps).toEqual([7_000, 7_000]);
      expect(receipt(out).requests).toMatchObject({ retries: 2, retryAfterHonoured: 2, waitedMs: 14_000, total: api.log.length });
    }
    const date = new Date(NOW.getTime() + 30_000).toUTCString();
    expect(retryAfterMs(date, NOW.getTime())).toBe(30_000);
    const slow = fakeReadApi({ retryAfter: { times: 1, value: "3600" } });
    await expect(main(argv(slow.url, newOut()), io().io)).rejects.toThrow("asked to wait 3600s");
  });

  test("the request cap stops the harvest with its state kept; a rerun with a higher cap resumes without repeating pages", async () => {
    const api = fakeReadApi();
    const out = newOut();
    const first = io();
    expect(await main(argv(api.url, out, ["--max-requests", "3"]), first.io)).toBe(EXIT_CAPPED);
    expect(api.log).toHaveLength(3);
    expect(readdirSync(out)).toEqual([STATE_FILE]);
    expect(statSync(join(out, STATE_FILE)).mode & 0o077).toBe(0);
    expect(existsSync(mappingPath(out))).toBe(false);
    expect(first.lines.join("\n")).toContain("request cap (--max-requests 3)");
    // The cap counts the whole harvest: the same cap again makes no request at all.
    expect(await main(argv(api.url, out, ["--max-requests", "3"]), io().io)).toBe(EXIT_CAPPED);
    expect(api.log).toHaveLength(3);
    expect(await main(argv(api.url, out), io().io)).toBe(0);
    const r = receipt(out);
    expect(r.requests.total).toBe(api.log.length);
    expect(r.runs).toBe(3);
    // No page was asked for twice.
    const asked = api.log.map((x) => `${x.path}?${x.params.get("source") ?? ""}:${x.params.get("cursor") ?? ""}`);
    expect(new Set(asked).size).toBe(asked.length);
    const fresh = newOut();
    expect(await main(argv(fakeReadApi().url, fresh), io().io)).toBe(0);
    expect(readFileSync(join(out, "sessions.jsonl"), "utf8")).toBe(readFileSync(join(fresh, "sessions.jsonl"), "utf8"));
  });

  test("a stopped harvest's state holds ids and repo-relative paths only: no text, remote credentials, home path, token or salt", async () => {
    const api = fakeReadApi();
    const out = newOut();
    // Frame (2), output (1), then fx-h-d (501) and both pages of fx-h-c, one at a time; the cap stops fx-h-b.
    expect(await main(argv(api.url, out, ["--max-requests", "6", "--concurrency", "1"]), io().io)).toBe(EXIT_CAPPED);
    const stateText = readFileSync(join(out, STATE_FILE), "utf8");
    expect(stateText).toContain("\"src/widgets/util.ts\"");
    expect(stateText).toContain("\"repo\":\"acme-api\"");
    for (const raw of ["FIXTURE_TEXT_", "FIXTURE_CRED_", "example.invalid", "/home/", "fixture-user", "notes.txt", TOKEN.slice(0, 16), SALT]) expect(stateText).not.toContain(raw);
  });

  test("--partial writes what was read when the cap is reached, and the receipt says so", async () => {
    const api = fakeReadApi();
    const out = newOut();
    expect(await main(argv(api.url, out, ["--max-requests", "4", "--partial"]), io().io)).toBe(0);
    const r = receipt(out);
    expect(r).toMatchObject({ complete: false, frame: { exhausted: true, sessions: 4 }, requests: { total: 4 } });
    expect(r.facts.sessionsNotRead).toBeGreaterThan(0);
    expect(r.notes.join(" ")).toContain("request cap reached (--max-requests 4)");
    expect((await loadSnapshot(out, { now: NOW })).sessions).toHaveLength(4);
  });

  test("resumes after a crash, including a torn last state line, with the same snapshot as an uninterrupted run", async () => {
    const api = fakeReadApi({ failAfter: 4 });
    const out = newOut();
    await expect(main(argv(api.url, out), io().io)).rejects.toThrow("answered 400");
    expect(readdirSync(out)).toEqual([STATE_FILE]);
    appendFileSync(join(out, STATE_FILE), "{\"t\":\"facts\",\"record\":{\"sou");
    const before = api.log.length;
    api.opts.failAfter = undefined;
    // A resume that stops again (the cap) and a second resume: the torn line was cut, not appended to.
    expect(await main(argv(api.url, out, ["--max-requests", String(before + 1)]), io().io)).toBe(EXIT_CAPPED);
    for (const line of readFileSync(join(out, STATE_FILE), "utf8").trim().split("\n")) expect(() => JSON.parse(line)).not.toThrow();
    expect(await main(argv(api.url, out), io().io)).toBe(0);
    // The frame was not read again.
    expect(api.log.slice(before).filter((r) => r.path === "/v1/sessions")).toEqual([]);
    const fresh = newOut();
    expect(await main(argv(fakeReadApi().url, fresh), io().io)).toBe(0);
    expect(readFileSync(join(out, "sessions.jsonl"), "utf8")).toBe(readFileSync(join(fresh, "sessions.jsonl"), "utf8"));
    // Another window in the same --out is refused rather than mixed in.
    const other = newOut();
    expect(await main(argv(fakeReadApi().url, other, ["--max-requests", "1"]), io().io)).toBe(EXIT_CAPPED);
    await expect(main([...argv(api.url, other)].map((a) => (a === UNTIL ? "2026-11-02T12:00:00.000Z" : a)), io().io)).rejects.toThrow("other --read-api");
  });

  test("the receipt names the facts no allowed route exposes, and an output route that is not served", async () => {
    const api = fakeReadApi({ outputUnsupported: true });
    const out = newOut();
    expect(await main(argv(api.url, out), io().io)).toBe(0);
    const r = receipt(out);
    expect(r.output).toEqual({ unavailable: "output_workspace_not_enabled" });
    expect(r.unavailable.map((u: { field: string }) => u.field)).toEqual(["commits", "outcomes", "usage", "writes.branch", "cwd", "orgId", "contractVersion"]);
    expect(r.withheld).toContain("title");
    expect(r.facts.sessionsWithoutEvents).toEqual({ session_partial: 1 });
    expect(r.notes.join(" ")).toContain("commits, outcomes and usage are not exposed");
  });

  test("end to end: a complete harvest, then the replay CLI raises the duplicate migration", async () => {
    // Without fx-h-d (its events are 501) the harvest is complete, and every page carries the read API's marker.
    const api = fakeReadApi({ sessions: COMPLETE });
    const out = newOut();
    expect(await main(argv(api.url, out), io().io)).toBe(0);
    expect(receipt(out)).toMatchObject({ complete: true, incompleteReasons: [], sourceAttribution: "read_worker" });
    const evalDir = tmp("si-harvest-eval-");
    const q = io();
    expect(await replayMain(["replay", "--snapshot", out, "--out", join(evalDir, "replay")], { out: q.io.out, err: q.io.err, now: NOW })).toBe(0);
    const alerts = JSON.parse(readFileSync(join(evalDir, "replay", "alerts.json"), "utf8"));
    const h = new Hasher(SALT);
    const pair = [`claude:${h.session("claude", "fx-h-a")}`, `codex:${h.session("codex", "fx-h-b")}`].sort();
    expect(alerts.alerts.filter((x: { kind: string }) => x.kind === "duplicate_work").map(({ urgency, artifact, pair: p }: Record<string, unknown>) => ({ urgency, artifact, pair: p })))
      .toEqual([{ urgency: "high", artifact: "number:0042", pair }]);
    expect(alerts.detectors[0].diagnostics).toMatchObject({ failed: 0, rejected: 0 });
  });
});

describe("harvest guards", () => {
  test("--read-api is https, or http on loopback only, with no credentials, query or fragment", () => {
    expect(parseReadApi("https://read.example.invalid/base/").href).toBe("https://read.example.invalid/base");
    expect(parseReadApi("http://127.0.0.1:8080").protocol).toBe("http:");
    expect(parseReadApi("http://localhost:8080").protocol).toBe("http:");
    for (const bad of ["http://read.example.invalid", "http://10.0.0.1", "ftp://read.example.invalid", "file:///etc/passwd", "https://u:p@read.example.invalid",
      "https://read.example.invalid/?x=1", "https://read.example.invalid/#f", "read.example.invalid", "http://127.0.0.1.example.invalid"])
      expect(() => parseReadApi(bad)).toThrow();
  });

  test("the token comes only from the environment and must look like one; argv is never echoed", async () => {
    const api = fakeReadApi();
    await expect(main(argv(api.url, newOut()), io({ env: {} }).io)).rejects.toThrow(`${TOKEN_ENV} is not set`);
    await expect(main(argv(api.url, newOut()), io({ env: { [TOKEN_ENV]: "short" } }).io)).rejects.toThrow("not a valid bearer token");
    const err = await main([...argv(api.url, newOut()), "--token", TOKEN], io().io).then(() => null, (e: Error) => e);
    expect(err!.message).toContain("Arguments are never echoed");
    expect(err!.message).not.toContain(TOKEN.slice(0, 16));
    expect(api.log).toEqual([]);
  });

  test("--out inside the repository, a non-empty --out, a long --delete-by, a long window and a missing salt are refused before any request", async () => {
    const api = fakeReadApi();
    await expect(main(argv(api.url, join(REPO, "tmp-harvest-out")), io().io)).rejects.toThrow("inside the repository");
    const full = tmp("si-harvest-full-");
    appendFileSync(join(full, "other"), "x");
    await expect(main(argv(api.url, full), io().io)).rejects.toThrow("not empty");
    await expect(main(argv(api.url, newOut()).map((a) => (a === DELETE_BY ? "2026-11-20T00:00:00.000Z" : a)), io().io)).rejects.toThrow("at most 14 days");
    await expect(main(argv(api.url, newOut()).map((a) => (a === SINCE ? "2026-06-01T00:00:00.000Z" : a)), io().io)).rejects.toThrow("at most 120 days");
    await expect(main(argv(api.url, newOut()).filter((a) => a !== "--salt-stdin"), io().io)).rejects.toThrow("--salt-stdin is required");
    await expect(main(argv(api.url, newOut()), io({ stdin: Readable.from(["short"]) }).io)).rejects.toThrow("--salt-stdin must carry");
    expect(api.log).toEqual([]);
  });
});

/** A client whose fetch is injected: it records the raw URL of every request and serves `pages` in turn. */
function recordingClient(base: string, pages: unknown[] = []) {
  const urls: string[] = [];
  let i = 0;
  const f = (async (input: string | URL | Request) => {
    urls.push(input instanceof URL ? input.href : typeof input === "string" ? input : input.url);
    const body = pages.length ? pages[Math.min(i++, pages.length - 1)] : { sessions: [], nextCursor: null };
    return Response.json(body, { headers: { "x-relayhistory-workspace-id": WS } });
  }) as typeof fetch;
  return { urls, client: new ReadApiClient({ base: parseReadApi(base), token: TOKEN, workspace: WS, maxRequests: 1_000, concurrency: 1, fetch: f }) };
}

describe("censoring at the per-session write bound", () => {
  const BOUND = IMPORT_BOUNDS.facts;
  const P = { readApi: "x", workspace: WS, since: SINCE, until: UNTIL, deleteBy: DELETE_BY, requireListBackend: null };
  const row: FrameRow = { source: "claude", sessionId: "fx-big", firstTs: at("01", "09:00"), lastTs: at("01", "10:00"), projectId: null, repo: null, gitBranch: null, parentSessionIds: [], catalog: false, eventCount: null };
  /** One page of `events` events with `files` paths each, all inside the window. */
  const page = (events: number, files: number, nextCursor: string | null, from = 0) => ({
    sessionId: "fx-big", nextCursor,
    events: Array.from({ length: events }, (_, e) => ({
      eventId: `ev-${from + e}`, sessionId: "fx-big", source: "claude", kind: "tool_call", ts: at("01", "09:30"),
      filesTouched: Array.from({ length: files }, (_, j) => `src/f${j}.ts`),
    })),
  });
  const run = async (pages: unknown[]) => {
    const { urls, client } = recordingClient("https://example.invalid", pages);
    return { rec: await sessionFacts(client, row, P, { n: 0 }), urls };
  };

  test("exactly at the bound with a cursor remaining: censored, drop count unknown, the next page never read", async () => {
    expect(1_000 * 20).toBe(BOUND);
    const { rec, urls } = await run([page(1_000, 20, "more")]);
    expect(rec).toMatchObject({ writesCensored: true, censorReason: "writes_bound_reached_cursor_remaining", droppedWrites: null, truncatedWrites: 0, pages: 1 });
    expect(rec.writes).toHaveLength(BOUND);
    expect(urls).toHaveLength(1);
  });

  test("over the bound within a page with a cursor remaining: censored, drop count unknown", async () => {
    const { rec } = await run([page(1_000, 21, "more")]);
    expect(rec).toMatchObject({ writesCensored: true, censorReason: "writes_bound_exceeded_cursor_remaining", droppedWrites: null, truncatedWrites: 1_000 });
    expect(rec.writes).toHaveLength(BOUND);
  });

  test("over the bound on the last page: censored with the exact drop count", async () => {
    const { rec } = await run([page(1_000, 21, null)]);
    expect(rec).toMatchObject({ writesCensored: true, censorReason: "writes_bound_exceeded", droppedWrites: 1_000, truncatedWrites: 1_000 });
  });

  test("exactly at the bound with the cursor exhausted, or under it across pages: not censored, every page read", async () => {
    expect((await run([page(1_000, 20, null)])).rec).toMatchObject({ writesCensored: false, censorReason: null, droppedWrites: null });
    const under = await run([page(500, 19, "p2"), page(500, 19, null, 500)]);
    expect(under.rec).toMatchObject({ writesCensored: false, pages: 2 });
    expect(under.rec.writes).toHaveLength(1_000 * 19);
    expect(under.urls).toHaveLength(2);
  });

  test("an event past --until no longer stops the paging: the pages after it are read", async () => {
    const late = { ...page(1, 1, "p2"), events: [{ ...page(1, 1, null).events[0]!, ts: "2026-11-03T01:00:00.000Z" }] };
    const { rec, urls } = await run([late, page(1, 1, null, 1)]);
    expect(urls).toHaveLength(2);
    expect(rec).toMatchObject({ writesCensored: false, eventsOutsideWindow: 1 });
    expect(rec.writes).toHaveLength(1);
  });

  test("end to end: a censored session makes the harvest incomplete, and the receipt counts it", async () => {
    const big: FxSession = {
      source: "claude", sessionId: "fx-h-big", firstTs: at("01", "09:00"), lastTs: at("01", "12:00"), projectId: "fixture-proj",
      events: Array.from({ length: 1_001 }, (_, e) => ({ eventId: `ev-big-${e}`, ts: at("01", "09:30"), kind: "tool_call", filesTouched: Array.from({ length: 20 }, (_, j) => `src/f${j}.ts`) })),
    };
    const api = fakeReadApi({ sessions: [big], eventPageSize: 1_000 });
    const out = newOut();
    expect(await main(argv(api.url, out), io().io)).toBe(0);
    const r = receipt(out);
    expect(r.complete).toBe(false);
    expect(r.facts).toMatchObject({ sessionsCensored: 1, censoredByReason: { writes_bound_reached_cursor_remaining: 1 }, sessionsWithUnknownDrops: 1, droppedWritesKnown: 0 });
    expect(r.notes.join(" ")).toContain("1 sessions' writes were cut at 20000 per session");
    // The second events page was never asked for.
    expect(api.log.filter((x) => x.path.endsWith("/events"))).toHaveLength(1);
  });
});

describe("request provenance across runs", () => {
  /** The receipt's view of a server log: requests per route. */
  const fromLog = (log: LoggedRequest[]) => {
    const byRoute: Record<string, number> = {};
    for (const r of log) {
      const k = r.path.replace(/^\/v1\/sessions\/[^/]+\/events$/u, "/v1/sessions/:sessionId/events");
      byRoute[k] = (byRoute[k] ?? 0) + 1;
    }
    return byRoute;
  };

  test("after an error and a resume, every count is restored and matches the server; an uninterrupted run differs only by the failed request", async () => {
    const api = fakeReadApi({ failAfter: 4, retryAfter: { times: 1, value: "2" } });
    const out = newOut();
    // One worker, so exactly one request fails.
    const one = ["--concurrency", "1"];
    await expect(main(argv(api.url, out, one), io().io)).rejects.toThrow("answered 400");
    api.opts.failAfter = undefined;
    expect(await main(argv(api.url, out, one), io().io)).toBe(0);
    const r = receipt(out).requests;
    expect(r).toMatchObject({ partial: false, total: api.log.length, byRoute: fromLog(api.log), retries: 1, retryAfterHonoured: 1, waitedMs: 2_000 });
    expect(r.partialReason).toBeUndefined();
    // Headers are counted on 2xx only: everything but the 429, the 400 and fx-h-d's 501.
    expect(receipt(out).server.headers["x-sessions-read-backend"]).toEqual({ "96bulls": api.log.length - 3 });
    const fresh = fakeReadApi({ retryAfter: { times: 1, value: "2" } });
    const freshOut = newOut();
    expect(await main(argv(fresh.url, freshOut, one), io().io)).toBe(0);
    const f = receipt(freshOut).requests;
    expect(r.total).toBe(f.total + 1);
    expect({ ...r.byRoute, "/v1/sessions/:sessionId/events": r.byRoute["/v1/sessions/:sessionId/events"] - 1 }).toEqual(f.byRoute);
    expect([r.retries, r.retryAfterHonoured, r.waitedMs]).toEqual([f.retries, f.retryAfterHonoured, f.waitedMs]);
  });

  test("a run that ended without its final counts (a hard crash) makes the request counts explicitly partial", async () => {
    const api = fakeReadApi({ failAfter: 4 });
    const out = newOut();
    await expect(main(argv(api.url, out), io().io)).rejects.toThrow("answered 400");
    // Simulate a kill: the run's final line never made it to disk.
    const path = join(out, STATE_FILE);
    const lines = readFileSync(path, "utf8").trim().split("\n").filter((l) => !l.includes("\"t\":\"runEnd\""));
    writeFileSync(path, `${lines.join("\n")}\n`, { mode: 0o600 });
    api.opts.failAfter = undefined;
    expect(await main(argv(api.url, out), io().io)).toBe(0);
    const r = receipt(out);
    expect(r.requests.partial).toBe(true);
    expect(r.requests.partialReason).toContain("lower bound");
    expect(r.requests.total).toBeLessThanOrEqual(api.log.length);
    expect(r.notes.join(" ")).toContain("request counts are partial");
  });
});

describe("request paths", () => {
  test.each([
    ["https://example.invalid", ""], ["https://example.invalid/", ""], ["https://example.invalid//", ""],
    ["https://example.invalid/api", "/api"], ["https://example.invalid/api/", "/api"], ["https://example.invalid//api//", "/api"],
  ])("base %s joins every route with exactly one slash", async (base, prefix) => {
    const { urls, client } = recordingClient(base);
    await client.get("/v1/sessions", [["limit", "200"]]);
    await client.get("/v1/sessions/output", []);
    await client.get("/v1/sessions/:sessionId/events", [], "fx-h-a");
    expect(urls).toEqual([
      `https://example.invalid${prefix}/v1/sessions?workspace=${WS}&limit=200`,
      `https://example.invalid${prefix}/v1/sessions/output?workspace=${WS}`,
      `https://example.invalid${prefix}/v1/sessions/fx-h-a/events?workspace=${WS}`,
    ]);
    for (const u of urls) expect(new URL(u).pathname).not.toContain("//");
  });
});

describe("frame source attribution (X-Relayhistory-List-Served-By)", () => {
  const strict = (set: string) => ["--require-list-backend", set];
  const sources = (out: string) => receipt(out).frame.sources;

  test("strict, every page union: complete and verified, with the per-provider page tally", async () => {
    const api = fakeReadApi({ sessions: COMPLETE, servedBy: () => "union", readBackend: null });
    const out = newOut();
    const q = io();
    expect(await main(argv(api.url, out, strict("union,supabase")), q.io)).toBe(0);
    expect(receipt(out)).toMatchObject({ complete: true, incompleteReasons: [], sourceAttribution: "verified" });
    expect(sources(out)).toMatchObject({ attribution: "verified", required: ["supabase", "union"], gated: true, offendingPages: 0,
      listServedBy: { union: { pages: 2, firstPage: 1, lastPage: 2 } }, readBackend: { missing: { pages: 2, firstPage: 1, lastPage: 2 } } });
    expect(q.lines.join("\n")).toContain("\"sourceAttribution\":\"verified\"");
  });

  test("strict, mixed union and supabase, both allowed: complete", async () => {
    const api = fakeReadApi({ sessions: COMPLETE, servedBy: (n) => (n === 1 ? "union" : "supabase") });
    const out = newOut();
    expect(await main(argv(api.url, out, strict("union,supabase")), io().io)).toBe(0);
    expect(receipt(out).complete).toBe(true);
    expect(sources(out).listServedBy).toEqual({ supabase: { pages: 1, firstPage: 2, lastPage: 2 }, union: { pages: 1, firstPage: 1, lastPage: 1 } });
  });

  test("strict, one neon page: exits non-zero with the diagnostic receipt and snapshot written; the replay refuses it", async () => {
    const api = fakeReadApi({ sessions: COMPLETE, servedBy: (n) => (n === 2 ? "neon" : "union") });
    const out = newOut();
    const q = io();
    expect(await main(argv(api.url, out, strict("union")), q.io)).toBe(4);
    expect(q.lines.join("\n")).toContain("frame_backend_unverified: 1 of 2 frame pages");
    const r = receipt(out);
    expect(r).toMatchObject({ complete: false, incompleteReasons: ["frame_backend_unverified"], sourceAttribution: "unverified" });
    expect(r.frame.sources).toMatchObject({ offendingPages: 1, listServedBy: { neon: { pages: 1, firstPage: 2, lastPage: 2 }, union: { pages: 1, firstPage: 1, lastPage: 1 } } });
    expect(r.notes.join(" ")).toContain("frame_backend_unverified");
    expect((await loadSnapshot(out, { now: NOW })).sessions).toHaveLength(3);
    const evalDir = tmp("si-harvest-eval-");
    const rq = io();
    await expect(replayMain(["replay", "--snapshot", out, "--out", join(evalDir, "replay")], { out: rq.io.out, err: rq.io.err, now: NOW }))
      .rejects.toThrow("complete:false (frame_backend_unverified); refused");
    expect(existsSync(join(evalDir, "replay", "alerts.json"))).toBe(false);
  });

  test("--allow-incomplete-coverage replays it as incomplete, and held-out scoring refuses that replay", async () => {
    const api = fakeReadApi({ sessions: COMPLETE, servedBy: (n) => (n === 2 ? "neon" : "union") });
    const out = newOut();
    expect(await main(argv(api.url, out, strict("union")), io().io)).toBe(4);
    const evalDir = tmp("si-harvest-eval-");
    const rq = io();
    const rio = { out: rq.io.out, err: rq.io.err, now: NOW };
    // Incomplete: exit 1, with the coverage gap named in alerts.json.
    expect(await replayMain(["replay", "--snapshot", out, "--out", join(evalDir, "replay"), "--allow-incomplete-coverage"], rio)).toBe(1);
    const alerts = JSON.parse(readFileSync(join(evalDir, "replay", "alerts.json"), "utf8"));
    expect(alerts.incomplete).toEqual(["coverage: the harvest's coverage receipt says complete:false (frame_backend_unverified)"]);
    await expect(replayMain(["score", "--alerts", join(evalDir, "replay", "alerts.json"), "--labels", FIXTURE_LABELS, "--out", join(evalDir, "score"), "--split", "heldout"], rio))
      .rejects.toThrow("held-out is never scored from an incomplete replay");
    expect(existsSync(join(evalDir, "score", "scored-hashes.json"))).toBe(false);
  });

  test("--detectors and --allow-incomplete-coverage together: both detectors run, and the replay is still incomplete", async () => {
    const api = fakeReadApi({ sessions: COMPLETE, servedBy: (n) => (n === 2 ? "neon" : "union") });
    const out = newOut();
    expect(await main(argv(api.url, out, strict("union")), io().io)).toBe(4);
    const evalDir = tmp("si-harvest-eval-");
    const rq = io();
    const rio = { out: rq.io.out, err: rq.io.err, now: NOW };
    const args = ["replay", "--snapshot", out, "--out", join(evalDir, "replay"), "--detectors", "coordination-advisor,conflict-rule", "--allow-incomplete-coverage"];
    expect(await replayMain(args, rio)).toBe(1);
    const alerts = JSON.parse(readFileSync(join(evalDir, "replay", "alerts.json"), "utf8"));
    expect(alerts.detectors.map((d: { name: string }) => d.name)).toEqual(["coordination-advisor", "conflict-rule"]);
    expect(alerts.incomplete).toContain("coverage: the harvest's coverage receipt says complete:false (frame_backend_unverified)");
    // Without the flag the same detectors are still refused before any replay.
    await expect(replayMain(args.slice(0, -1), rio)).rejects.toThrow("refused");
  });

  test("strict, a page with no header: incomplete, counted as missing", async () => {
    const api = fakeReadApi({ sessions: COMPLETE, servedBy: (n) => (n === 1 ? "union" : null) });
    const out = newOut();
    expect(await main(argv(api.url, out, strict("union")), io().io)).toBe(4);
    expect(receipt(out).complete).toBe(false);
    expect(sources(out)).toMatchObject({ offendingPages: 1, listServedBy: { missing: { pages: 1, firstPage: 2, lastPage: 2 }, union: { pages: 1 } } });
  });

  test("strict, an unknown store name is offending; an unknown name in the flag is refused before any request", async () => {
    const api = fakeReadApi({ sessions: COMPLETE, servedBy: () => "mystery" });
    const out = newOut();
    expect(await main(argv(api.url, out, strict("union")), io().io)).toBe(4);
    expect(sources(out)).toMatchObject({ offendingPages: 2, listServedBy: { mystery: { pages: 2 } } });
    const none = fakeReadApi();
    await expect(main(argv(none.url, newOut(), strict("union,mystery")), io().io)).rejects.toThrow("--require-list-backend must be a comma list");
    expect(none.log).toEqual([]);
  });

  test("every page counts, empty ones included: an empty neon first page and an empty last page", async () => {
    const api = fakeReadApi({ sessions: COMPLETE, leadingEmpty: true, trailingEmpty: true, servedBy: (n) => (n === 1 ? "neon" : "union") });
    const out = newOut();
    expect(await main(argv(api.url, out, strict("union")), io().io)).toBe(4);
    const r = receipt(out);
    // Leading empty, two pages of rows, trailing empty: four pages.
    expect(r.frame.pages).toBe(4);
    expect(r.frame.sources).toMatchObject({ offendingPages: 1, listServedBy: { neon: { pages: 1, firstPage: 1, lastPage: 1 }, union: { pages: 3, firstPage: 2, lastPage: 4 } } });
    expect(r.frame.sessions).toBe(3);
  });

  test("a mixed fallback across a resume: union, neon, a crash, then union is incomplete with the right tallies", async () => {
    const api = fakeReadApi({ sessions: COMPLETE, pageSize: 1, failAfter: 2, servedBy: (n) => (n === 2 ? "neon" : "union") });
    const out = newOut();
    await expect(main(argv(api.url, out, strict("union")), io().io)).rejects.toThrow("answered 400");
    api.opts.failAfter = undefined;
    expect(await main(argv(api.url, out, strict("union")), io().io)).toBe(4);
    const r = receipt(out);
    expect(r).toMatchObject({ complete: false, incompleteReasons: ["frame_backend_unverified"], runs: 2 });
    expect(r.frame.sources).toMatchObject({ offendingPages: 1,
      listServedBy: { neon: { pages: 1, firstPage: 2, lastPage: 2 }, union: { pages: 2, firstPage: 1, lastPage: 3 } } });
    expect(r.frame.pages).toBe(3);
    // Pages 1 and 2 were not read again: the only repeat is page 3, whose first attempt was the failure.
    expect(api.log.filter((x) => x.path === "/v1/sessions").map((x) => x.params.get("cursor"))).toEqual([null, "1", "2", "2"]);
  });

  test("the setting is bound to the harvest: a record-only start cannot resume as strict, nor a strict one with another set", async () => {
    const api = fakeReadApi({ sessions: COMPLETE, servedBy: () => "union" });
    const recordOnly = newOut();
    expect(await main(argv(api.url, recordOnly, ["--max-requests", "1"]), io().io)).toBe(EXIT_CAPPED);
    await expect(main(argv(api.url, recordOnly, strict("union")), io().io)).rejects.toThrow("other --read-api");
    const strictOut = newOut();
    expect(await main(argv(api.url, strictOut, [...strict("union"), "--max-requests", "1"]), io().io)).toBe(EXIT_CAPPED);
    await expect(main(argv(api.url, strictOut, strict("union,supabase")), io().io)).rejects.toThrow("other --read-api");
    await expect(main(argv(api.url, strictOut), io().io)).rejects.toThrow("other --read-api");
    // The same set in another order is the same setting.
    expect(await main(argv(api.url, strictOut, strict(" union ")), io().io)).toBe(0);
  });

  test("no flag: the tallies are recorded but nothing is gated; without the read API's marker the source is unverified and the replay refuses it", async () => {
    const api = fakeReadApi({ sessions: COMPLETE, servedBy: () => "neon", readBackend: null });
    const out = newOut();
    expect(await main(argv(api.url, out), io().io)).toBe(0);
    const r = receipt(out);
    expect(r).toMatchObject({ complete: true, incompleteReasons: [], sourceAttribution: "unverified" });
    expect(r.frame.sources).toMatchObject({ gated: false, required: null, offendingPages: 0, listServedBy: { neon: { pages: 2, firstPage: 1, lastPage: 2 } } });
    expect(r.notes.join(" ")).toContain("not representative proof");
    const evalDir = tmp("si-harvest-eval-");
    const rq = io();
    await expect(replayMain(["replay", "--snapshot", out, "--out", join(evalDir, "replay")], { out: rq.io.out, err: rq.io.err, now: NOW }))
      .rejects.toThrow("source attribution is unverified");
  });
});

describe("server-supplied keys never hit inherited properties", () => {
  const INHERITED = ["__proto__", "constructor", "toString", "hasOwnProperty"] as const;
  /** Own entries in order, from the parsed receipt: what was actually written, inherited keys excluded. */
  const own = (o: object) => Object.entries(o);
  const pristine = () => {
    for (const k of ["pages", "firstPage", "lastPage"]) expect(Object.hasOwn(Object.prototype, k)).toBe(false);
    expect(({} as Record<string, unknown>).pages).toBeUndefined();
    expect(typeof Object.prototype.toString).toBe("function");
    expect(typeof Object.prototype.hasOwnProperty).toBe("function");
  };

  test.each(INHERITED)("%s as served-by, read backend, list mode and buffered value, across a crash and a resume", async (value) => {
    // Page 1 union, page 2 the key, a crash on page 3, then a resume serving union: three one-row pages.
    const api = fakeReadApi({
      sessions: COMPLETE, pageSize: 1, failAfter: 2, servedBy: (n) => (n === 2 ? value : "union"),
      readBackend: value, listMode: () => value, buffered: value,
    });
    const out = newOut();
    await expect(main(argv(api.url, out, ["--require-list-backend", "union", "--concurrency", "1"]), io().io)).rejects.toThrow("answered 400");
    pristine();
    api.opts.failAfter = undefined;
    expect(await main(argv(api.url, out, ["--require-list-backend", "union", "--concurrency", "1"]), io().io)).toBe(4);
    pristine();
    const text = readFileSync(join(out, RECEIPT_FILE), "utf8");
    const r = JSON.parse(text);
    expect(r).toMatchObject({ complete: false, incompleteReasons: ["frame_backend_unverified"], sourceAttribution: "unverified" });
    const s = r.frame.sources;
    expect(s.offendingPages).toBe(1);
    expect(own(s.listServedBy)).toEqual([[value, { pages: 1, firstPage: 2, lastPage: 2 }], ["union", { pages: 2, firstPage: 1, lastPage: 3 }]]
      .sort(([a], [b]) => ((a as string) < (b as string) ? -1 : 1)));
    expect(own(s.readBackend)).toEqual([[value, { pages: 3, firstPage: 1, lastPage: 3 }]]);
    expect(own(s.listMode)).toEqual([[value, { pages: 3, firstPage: 1, lastPage: 3 }]]);
    // The header counts cover every 2xx response across both runs (all but the one failed 400), restored on resume.
    const headers = r.server.headers;
    const ok = (h: string) => own(headers[h]);
    expect(ok("x-sessions-read-backend")).toEqual([[value, api.log.length - 1]]);
    expect(ok("x-relayhistory-buffered")).toEqual([[value, api.log.length - 1]]);
    expect(ok("x-relayhistory-list-mode")).toEqual([[value, 3]]);
    expect(own(r.requests.byRoute).map(([k]) => k)).toEqual(["/v1/sessions", "/v1/sessions/:sessionId/events", "/v1/sessions/output"]);
    expect(r.requests.partial).toBe(false);
    // Raw JSON: the key is written like any other key.
    expect(text).toContain(`"${value}": {`);
  });

  test.each(INHERITED)("%s as a refusal reason, and a harness named constructor, are ordinary counts", async (value) => {
    const sessions: FxSession[] = [...COMPLETE,
      { source: "constructor", sessionId: "fx-h-k", firstTs: at("02", "10:00"), lastTs: at("02", "11:00"), projectId: null, eventsUnsupported: value }];
    const api = fakeReadApi({ sessions });
    const out = newOut();
    expect(await main(argv(api.url, out), io().io)).toBe(0);
    pristine();
    const r = receipt(out);
    expect(own(r.frame.bySource)).toEqual([["claude", 2], ["codex", 1], ["constructor", 1]]);
    expect(own(r.frame.byDay["2026-11-02"])).toEqual([["claude", 1], ["constructor", 1]]);
    expect(own(r.facts.sessionsWithoutEvents)).toEqual([[value, 1]]);
    expect(r.incompleteReasons).toEqual(["events_not_served"]);
  });
});
