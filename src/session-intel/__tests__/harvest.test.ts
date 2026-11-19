/**
 * The read-API harvest (docs/session-intel-harvest.md) against an in-process fake read API with synthetic
 * sessions only: pagination to exhaustion and resume, the workspace attestation, the token's privacy,
 * Retry-After, the request cap, the unavailable-field receipt, the routes it must never call, no text in the
 * output, and harvest followed by the unchanged replay CLI.
 */
import { afterAll, describe, expect, test } from "bun:test";
import { appendFileSync, existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import { Readable } from "node:stream";
import { HARVEST_ROUTES, parseReadApi, ReadApiClient, redact, retryAfterMs, TOKEN_ENV } from "../harvest/client.ts";
import { EXIT_CAPPED, failureMessage, main, type HarvestIo } from "../harvest/cli.ts";
import { RECEIPT_FILE, STATE_FILE } from "../harvest/harvest.ts";
import { Hasher } from "../import/normalize.ts";
import { mappingPath } from "../import/writer.ts";
import { main as replayMain } from "../replay/cli.ts";
import { loadSnapshot } from "../replay/snapshot.ts";
import { tmp } from "./helpers.ts";

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

interface ServerOptions {
  pageSize?: number; eventPageSize?: number; workspaceHeader?: string; catalogWorkspace?: string;
  retryAfter?: { times: number; value: string; status?: number }; redirect?: boolean; failAfter?: number;
  errorCode?: string; status?: number; outputUnsupported?: boolean;
}
interface LoggedRequest { path: string; params: URLSearchParams; authorization: string | null }

function fakeReadApi(opts: ServerOptions = {}) {
  const log: LoggedRequest[] = [];
  let retries = opts.retryAfter?.times ?? 0;
  const pageSize = opts.pageSize ?? 2, eventPageSize = opts.eventPageSize ?? 2;
  const json = (body: unknown, status = 200, headers: Record<string, string> = {}) => Response.json(body, {
    status, headers: { "x-relayhistory-workspace-id": opts.workspaceHeader ?? WS, "x-sessions-read-backend": "96bulls", ...headers },
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
        const rows = SESSIONS.filter((s) => s.lastTs >= after && s.firstTs < before).sort((a, b) => (a.lastTs < b.lastTs ? 1 : -1));
        const offset = Number(url.searchParams.get("cursor") ?? "0");
        const page = rows.slice(offset, offset + pageSize);
        return json({
          sessions: page.map((s) => ({
            title: TEXT(`title_${s.sessionId}`), taskTitle: TEXT(`task_${s.sessionId}`), shortSummary: TEXT(`summary_${s.sessionId}`),
            sessionId: s.sessionId, userIds: ["fixture-user-id"], source: s.source, projectId: s.projectId, machineIds: ["fixture-machine"],
            firstTs: s.firstTs, lastTs: s.lastTs, workState: null, eventCount: s.events?.length ?? 0, kinds: [], models: ["fixture-model"],
            ...(s.catalog ? { catalog: { workspaceId: opts.catalogWorkspace ?? WS, title: TEXT(`ctitle_${s.sessionId}`), projectKey: "acme-api", ...s.catalog } } : {}),
          })),
          nextCursor: offset + pageSize < rows.length ? String(offset + pageSize) : null, correlationId: "fx",
        });
      }
      const m = /^\/v1\/sessions\/([^/]+)\/events$/u.exec(p);
      if (m) {
        const s = SESSIONS.find((x) => x.sessionId === decodeURIComponent(m[1]!) && x.source === url.searchParams.get("source"));
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
    // Two pages of fx-h-a's events, one of fx-h-b's, two of fx-h-c's (stopped at the first event past --until), fx-h-d's 501.
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

  test("end to end: harvest, then the unchanged replay CLI raises the duplicate migration", async () => {
    const api = fakeReadApi();
    const out = newOut();
    expect(await main(argv(api.url, out), io().io)).toBe(0);
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
