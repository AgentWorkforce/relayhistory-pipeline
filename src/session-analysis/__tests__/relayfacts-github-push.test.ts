/** Relayfacts GitHub push: pure module against an injected fake fetch. No network, no real token. */
import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import {
  parseRelayfactsPushConfig,
  pushRelayfactsDocument,
  pushRelayfactsTopic,
  relayfactsDocumentPath,
  type RelayfactsPushDeps,
  type RelayfactsTarget,
} from "../relayfacts-github-push.ts";

const TOKEN = "ghs_FAKE_TEST_TOKEN_must_never_leak_0123456789";
const WORKSPACE = "ws-fake-workspace-id-0001";
const TOPIC = "src-session-read-api";
const MARKDOWN = "# Decisions: src-session-read-api\n\nRecorded decisions for this topic.\n";
const target: RelayfactsTarget = { owner: "acme", repo: "widgets", branch: "main", dir: "relayfacts" };
const PATH = "relayfacts/src-session-read-api.md";
const URL_BASE = "https://api.github.com/repos/acme/widgets/contents/relayfacts/src-session-read-api.md";

interface Recorded { url: string; method: string; headers: Record<string, string>; body: unknown }
type Reply = Response | Error | ((req: Recorded) => Response | Error);

function fakeFetch(replies: Reply[]) {
  const requests: Recorded[] = [];
  const fn = (async (input: string | URL | Request, init?: RequestInit) => {
    const headers: Record<string, string> = {};
    new Headers(init?.headers).forEach((v, k) => { headers[k] = v; });
    const req: Recorded = {
      url: String(input),
      method: init?.method ?? "GET",
      headers,
      body: typeof init?.body === "string" ? JSON.parse(init.body) : undefined,
    };
    requests.push(req);
    let next = replies.shift();
    if (next === undefined) throw new Error("unexpected request");
    if (typeof next === "function") next = next(req);
    if (next instanceof Error) throw next;
    return next;
  }) as typeof fetch;
  return { fn, requests };
}

const json = (status: number, body: unknown, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });
const fileBody = (content: string, sha = "sha-old") =>
  ({ type: "file", sha, encoding: "base64", content: Buffer.from(content, "utf8").toString("base64").replace(/(.{60})/g, "$1\n") });

function deps(replies: Reply[], ...tokenArg: [string | undefined] | []) {
  const token = tokenArg.length === 0 ? TOKEN : tokenArg[0];
  const f = fakeFetch(replies);
  const events: Record<string, unknown>[] = [];
  const d: RelayfactsPushDeps = { fetch: f.fn, token: () => token, now: () => new Date("2026-11-17T00:00:00Z"), log: (e) => events.push(e) };
  return { d, requests: f.requests, events };
}

function assertNoLeak(events: Record<string, unknown>[]) {
  for (const e of events) {
    const text = JSON.stringify(e);
    expect(text).not.toContain(TOKEN);
    expect(text).not.toContain(WORKSPACE);
    expect(text).not.toContain("Recorded decisions");
    expect(e.outcome).toBeString();
    expect("status" in e).toBe(true);
    expect(e.repository).toBe("acme/widgets");
    expect(e.path).toBeString();
  }
}

const input = { target, path: PATH, markdown: MARKDOWN, topic: TOPIC, workspace: WORKSPACE };

describe("parseRelayfactsPushConfig", () => {
  test("flag off or not exactly 1 returns null", () => {
    expect(parseRelayfactsPushConfig({})).toBeNull();
    for (const v of ["0", "true", "yes", " 1", "1 ", ""]) {
      expect(parseRelayfactsPushConfig({ RELAYFACTS_GITHUB_PUSH_ENABLED: v, RELAYFACTS_GITHUB_REPOS: "a/b" })).toBeNull();
    }
    // Flag off ignores even malformed values.
    expect(parseRelayfactsPushConfig({ RELAYFACTS_GITHUB_REPOS: "../../etc", RELAYFACTS_GITHUB_API_BASE: "nope" })).toBeNull();
  });

  test("parses targets with defaults, branch and dir", () => {
    const config = parseRelayfactsPushConfig({
      RELAYFACTS_GITHUB_PUSH_ENABLED: "1",
      RELAYFACTS_GITHUB_REPOS: "acme/widgets, acme/docs@release/v2:notes/relayfacts ,Org.x/re_po-1:facts",
      RELAYFACTS_GITHUB_TOKEN: TOKEN,
    });
    expect(config).toEqual({
      apiBase: "https://api.github.com",
      targets: [
        { owner: "acme", repo: "widgets", branch: "main", dir: "relayfacts" },
        { owner: "acme", repo: "docs", branch: "release/v2", dir: "notes/relayfacts" },
        { owner: "Org.x", repo: "re_po-1", branch: "main", dir: "facts" },
      ],
    });
    expect(JSON.stringify(config)).not.toContain(TOKEN);
  });

  test("api base override is normalized; non-https or credentialed rejected", () => {
    const base = (v: string) => parseRelayfactsPushConfig({ RELAYFACTS_GITHUB_PUSH_ENABLED: "1", RELAYFACTS_GITHUB_REPOS: "a/b", RELAYFACTS_GITHUB_API_BASE: v });
    expect(base("https://ghe.example.test/api/v3/")?.apiBase).toBe("https://ghe.example.test/api/v3");
    expect(() => base("http://api.github.com")).toThrow("RELAYFACTS_GITHUB_API_BASE");
    expect(() => base(`https://x:${TOKEN}@api.github.com`)).toThrow("RELAYFACTS_GITHUB_API_BASE");
    expect(() => base("not a url")).toThrow("RELAYFACTS_GITHUB_API_BASE");
  });

  const bad = [
    "", "acme", "acme/", "/widgets", "acme/widgets/extra", "ac me/widgets", "acme/wid$gets", "../widgets", "acme/..",
    "acme/widgets:../up", "acme/widgets:/abs", "acme/widgets:a/../b", "acme/widgets:", "acme/widgets@", "acme/widgets@../x",
    "acme/widgets@-x", "acme/widgets:dir/", "a/b,,c/d", "a/b,a/b", `acme/${TOKEN}$`,
  ];
  for (const repos of bad) {
    test(`rejects malformed RELAYFACTS_GITHUB_REPOS ${JSON.stringify(repos.slice(0, 24))}`, () => {
      let message = "";
      try {
        parseRelayfactsPushConfig({ RELAYFACTS_GITHUB_PUSH_ENABLED: "1", RELAYFACTS_GITHUB_REPOS: repos, RELAYFACTS_GITHUB_TOKEN: TOKEN });
      } catch (err) {
        message = (err as Error).message;
      }
      expect(message).toContain("RELAYFACTS_GITHUB_REPOS");
      expect(message).not.toContain(TOKEN);
    });
  }
});

describe("relayfactsDocumentPath", () => {
  const h = (t: string) => createHash("sha256").update(t, "utf8").digest("hex").slice(0, 12);

  test("an already-clean topic is used as is", () => {
    expect(relayfactsDocumentPath("relayfacts", "src-session-read-api")).toBe("relayfacts/src-session-read-api.md");
    expect(relayfactsDocumentPath("a/b", "general")).toBe("a/b/general.md");
  });

  test("a lossy slug gets the short hash, so distinct topics never collide", () => {
    const a = relayfactsDocumentPath("relayfacts", "Src Session/Read API");
    const b = relayfactsDocumentPath("relayfacts", "src-session-read-api");
    const c = relayfactsDocumentPath("relayfacts", "src_session_read_api");
    expect(a).toBe(`relayfacts/src-session-read-api-${h("Src Session/Read API")}.md`);
    expect(new Set([a, b, c]).size).toBe(3);
  });

  test("empty slug falls back to topic-<hash>", () => {
    for (const t of ["", "!!!", "../..", "日本語"]) expect(relayfactsDocumentPath("relayfacts", t)).toBe(`relayfacts/topic-${h(t)}.md`);
  });

  test("long topics are capped at 80 characters with the hash", () => {
    const long = "a".repeat(200);
    const path = relayfactsDocumentPath("relayfacts", long);
    const name = path.slice("relayfacts/".length, -3);
    expect(name.length).toBeLessThanOrEqual(80);
    expect(name.endsWith(`-${h(long)}`)).toBe(true);
    expect(relayfactsDocumentPath("relayfacts", "b".repeat(80))).toBe(`relayfacts/${"b".repeat(80)}.md`);
  });

  test("traversal and absolute dirs throw", () => {
    for (const dir of ["", "/abs", "../x", "a/../b", "a//b", "a/", ".", "a\\b"]) expect(() => relayfactsDocumentPath(dir, "t")).toThrow();
  });
});

describe("pushRelayfactsDocument", () => {
  test("404 creates the file with the expected request shape", async () => {
    const { d, requests, events } = deps([json(404, { message: "Not Found" }), json(201, { content: {} })]);
    expect(await pushRelayfactsDocument(d, input)).toEqual({ outcome: "created" });
    expect(requests.map((r) => [r.method, r.url])).toEqual([["GET", `${URL_BASE}?ref=main`], ["PUT", URL_BASE]]);
    for (const r of requests) {
      expect(r.headers.authorization).toBe(`Bearer ${TOKEN}`);
      expect(r.headers.accept).toBe("application/vnd.github+json");
      expect(r.headers["x-github-api-version"]).toBe("2022-11-28");
      expect(r.headers["user-agent"]).toBe("relayhistory-pipeline-relayfacts");
    }
    const body = requests[1].body as Record<string, unknown>;
    expect(body).toEqual({ message: "relayfacts: update src-session-read-api", content: Buffer.from(MARKDOWN).toString("base64"), branch: "main" });
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ outcome: "created", status: 201, repository: "acme/widgets", path: PATH, branch: "main" });
    assertNoLeak(events);
  });

  test("200 with different content updates with the sha", async () => {
    const { d, requests, events } = deps([json(200, fileBody("old content\n", "abc123")), json(200, {})]);
    expect(await pushRelayfactsDocument(d, input)).toEqual({ outcome: "updated" });
    expect(requests[1].body).toMatchObject({ sha: "abc123", branch: "main", message: "relayfacts: update src-session-read-api" });
    assertNoLeak(events);
  });

  test("identical content returns unchanged and never PUTs", async () => {
    const { d, requests, events } = deps([json(200, fileBody(MARKDOWN))]);
    expect(await pushRelayfactsDocument(d, input)).toEqual({ outcome: "unchanged" });
    expect(requests.map((r) => r.method)).toEqual(["GET"]);
    expect(events[0]).toMatchObject({ outcome: "unchanged", status: 200 });
  });

  test("commit message carries the slug, never the raw topic or workspace", async () => {
    const topic = "Customer Thing / Secret Plan";
    const path = relayfactsDocumentPath("relayfacts", topic);
    const { d, requests } = deps([json(404, {}), json(201, {})]);
    await pushRelayfactsDocument(d, { ...input, topic, path });
    const message = (requests[1].body as { message: string }).message;
    expect(message).toMatch(/^relayfacts: update customer-thing-secret-plan-[0-9a-f]{12}$/);
    expect(message).not.toContain(topic);
    expect(JSON.stringify(requests)).not.toContain(WORKSPACE);
  });

  test("409 on PUT re-reads once and retries", async () => {
    const { d, requests } = deps([json(200, fileBody("old", "s1")), json(409, {}), json(200, fileBody("older", "s2")), json(200, {})]);
    expect(await pushRelayfactsDocument(d, input)).toEqual({ outcome: "updated" });
    expect(requests.map((r) => r.method)).toEqual(["GET", "PUT", "GET", "PUT"]);
    expect(requests[3].body).toMatchObject({ sha: "s2" });
  });

  test("409 twice fails without a third attempt", async () => {
    const { d, requests, events } = deps([json(200, fileBody("old", "s1")), json(409, {}), json(200, fileBody("old", "s2")), json(409, {})]);
    expect(await pushRelayfactsDocument(d, input)).toEqual({ outcome: "failed", status: 409, reason: "conflict" });
    expect(requests).toHaveLength(4);
    expect(events).toHaveLength(1);
    assertNoLeak(events);
  });

  test("403 rate limit and 429 fail as rate_limited with no retry", async () => {
    const a = deps([json(403, { message: "API rate limit exceeded" }, { "x-ratelimit-remaining": "0" })]);
    expect(await pushRelayfactsDocument(a.d, input)).toEqual({ outcome: "failed", status: 403, reason: "rate_limited" });
    expect(a.requests).toHaveLength(1);
    const b = deps([json(404, {}), json(429, {}, { "retry-after": "60" })]);
    expect(await pushRelayfactsDocument(b.d, input)).toEqual({ outcome: "failed", status: 429, reason: "rate_limited" });
    expect(b.requests).toHaveLength(2);
    const c = deps([json(403, { message: "secondary rate limit" }, { "retry-after": "30" })]);
    expect(await pushRelayfactsDocument(c.d, input)).toMatchObject({ reason: "rate_limited" });
    assertNoLeak([...a.events, ...b.events, ...c.events]);
  });

  test("403 without rate-limit headers is forbidden; 500 is failed", async () => {
    const a = deps([json(403, { message: "Resource not accessible" })]);
    expect(await pushRelayfactsDocument(a.d, input)).toEqual({ outcome: "failed", status: 403, reason: "forbidden" });
    const b = deps([json(404, {}), json(500, { message: `echo ${TOKEN}` })]);
    expect(await pushRelayfactsDocument(b.d, input)).toEqual({ outcome: "failed", status: 500, reason: "put_failed" });
    assertNoLeak([...a.events, ...b.events]);
  });

  test("network errors become failed, never thrown, and never leak the error text", async () => {
    const a = deps([new Error(`connect failed Bearer ${TOKEN}`)]);
    expect(await pushRelayfactsDocument(a.d, input)).toEqual({ outcome: "failed", reason: "network" });
    const b = deps([json(404, {}), new TypeError("socket hang up")]);
    expect(await pushRelayfactsDocument(b.d, input)).toEqual({ outcome: "failed", reason: "network" });
    // A PUT that gets no response logs no status, not the status of the GET before it.
    const c = deps([json(200, fileBody("old")), new TypeError("socket hang up")]);
    expect(await pushRelayfactsDocument(c.d, input)).toEqual({ outcome: "failed", reason: "network" });
    for (const events of [a.events, b.events, c.events]) {
      expect(events).toHaveLength(1);
      expect(events[0]).toMatchObject({ outcome: "failed", reason: "network", status: null });
    }
    assertNoLeak([...a.events, ...b.events, ...c.events]);
    expect(JSON.stringify(a.events)).not.toContain("connect failed");
  });

  test("unparseable or non-file GET responses fail cleanly", async () => {
    const a = deps([new Response("not json", { status: 200 })]);
    expect(await pushRelayfactsDocument(a.d, input)).toEqual({ outcome: "failed", status: 200, reason: "bad_response" });
    const b = deps([json(200, [{ type: "file" }])]);
    expect(await pushRelayfactsDocument(b.d, input)).toEqual({ outcome: "failed", status: 200, reason: "not_a_file" });
  });

  test("no token skips without any request", async () => {
    for (const token of [undefined, "", "   "]) {
      const { d, requests, events } = deps([], token);
      expect(await pushRelayfactsDocument(d, input)).toEqual({ outcome: "skipped", reason: "no_token" });
      expect(requests).toHaveLength(0);
      expect(events[0]).toMatchObject({ outcome: "skipped", status: null });
    }
    const throwing = deps([]);
    throwing.d.token = () => { throw new Error(TOKEN); };
    expect(await pushRelayfactsDocument(throwing.d, input)).toEqual({ outcome: "skipped", reason: "no_token" });
  });

  test("a throwing logger never changes the outcome", async () => {
    const { d } = deps([json(404, {}), json(201, {})]);
    d.log = () => { throw new Error("log sink down"); };
    expect(await pushRelayfactsDocument(d, input)).toEqual({ outcome: "created" });
  });

  test("invalid path or target is a programming error and throws without the token", async () => {
    const cases = [
      { ...input, path: "relayfacts/../secrets.md" },
      { ...input, path: "/etc/passwd" },
      { ...input, path: "other/src-session-read-api.md" },
      { ...input, path: "relayfacts/UPPER.md" },
      { ...input, path: "relayfacts/x.txt" },
      { ...input, target: { ...target, owner: "../x" } },
      { ...input, target: { ...target, dir: "../relayfacts" }, path: "../relayfacts/x.md" },
    ];
    for (const c of cases) {
      const { d, requests } = deps([]);
      let message = "";
      try {
        await pushRelayfactsDocument(d, c);
      } catch (err) {
        message = (err as Error).message;
      }
      expect(message).toContain("pushRelayfactsDocument");
      expect(message).not.toContain(TOKEN);
      expect(requests).toHaveLength(0);
    }
  });
});

describe("pushRelayfactsTopic", () => {
  test("one result per target; a failing target never stops the others", async () => {
    const config = parseRelayfactsPushConfig({
      RELAYFACTS_GITHUB_PUSH_ENABLED: "1",
      RELAYFACTS_GITHUB_REPOS: "acme/one,acme/two@dev:docs/facts,acme/three",
      RELAYFACTS_GITHUB_API_BASE: "https://ghe.example.test/api/v3",
    })!;
    const { d, requests, events } = deps([
      new Error("boom"),
      json(404, {}), json(201, {}),
      json(200, fileBody(MARKDOWN)),
    ]);
    const results = await pushRelayfactsTopic(d, config, { workspace: WORKSPACE, topic: TOPIC, markdown: MARKDOWN });
    expect(results.map((r) => [r.target.repo, r.result])).toEqual([
      ["one", { outcome: "failed", reason: "network" }],
      ["two", { outcome: "created" }],
      ["three", { outcome: "unchanged" }],
    ]);
    expect(requests.map((r) => `${r.method} ${r.url}`)).toEqual([
      "GET https://ghe.example.test/api/v3/repos/acme/one/contents/relayfacts/src-session-read-api.md?ref=main",
      "GET https://ghe.example.test/api/v3/repos/acme/two/contents/docs/facts/src-session-read-api.md?ref=dev",
      "PUT https://ghe.example.test/api/v3/repos/acme/two/contents/docs/facts/src-session-read-api.md",
      "GET https://ghe.example.test/api/v3/repos/acme/three/contents/relayfacts/src-session-read-api.md?ref=main",
    ]);
    expect(requests[2].body).toMatchObject({ branch: "dev" });
    expect(events).toHaveLength(3);
    for (const e of events) {
      const text = JSON.stringify(e);
      expect(text).not.toContain(TOKEN);
      expect(text).not.toContain(WORKSPACE);
    }
  });

  test("a hand-built invalid target throws before any request", async () => {
    const { d, requests } = deps([]);
    const config = { apiBase: "https://api.github.com", targets: [target, { ...target, dir: "../x" }] };
    await expect(pushRelayfactsTopic(d, config, { workspace: WORKSPACE, topic: TOPIC, markdown: MARKDOWN })).rejects.toThrow("invalid target");
    expect(requests).toHaveLength(0);
  });
});
