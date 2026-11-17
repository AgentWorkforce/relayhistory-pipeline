/**
 * Shared SearchIndexAdapter conformance suite. Every adapter (OSS postgres;
 * cloud clickhouse/opensearch) runs exactly these cases, so "supports the
 * read API" means the same thing for each backend.
 *
 * Usage in a vitest file:
 *
 *   searchIndexConformance("postgres (pglite)", async () => ({ adapter, dispose }));
 *
 * The factory may return one shared index for the whole suite: every case
 * uses its own random org id, so cases never see each other's rows. Adapters
 * with refresh-based visibility must be configured to wait for refresh.
 * Only synthetic data is used.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  CONTENT_PREFIX_CHARS,
  MAX_EXCLUDED_SESSIONS,
  MAX_SCAN_PAGE,
  SearchIndexError,
  comparePositions,
  matchesQuery,
  positionOf,
  type SearchEventDocument,
  type SearchFingerprint,
  type SearchHit,
  type SearchIndexAdapter,
  type SearchPosition,
  type SearchQuery,
  type SearchScope,
} from "./contracts.js";
import { planScopeReconcile } from "./reconcile.js";

export interface ConformanceTarget {
  adapter: SearchIndexAdapter;
  dispose?: () => Promise<void>;
}

let seq = 0;
const rand = () => `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}${seq++}`;

export function syntheticDoc(over: Partial<SearchEventDocument> & Pick<SearchEventDocument, "orgId">): SearchEventDocument {
  return {
    workspaceId: "ws-a",
    tsMs: 1_767_225_600_000,
    sessionId: "sess-1",
    source: "claude",
    machineId: "m-1",
    kind: "message",
    eventId: `ev-${rand()}`,
    type: "assistant",
    userId: "u-1",
    actorName: "agent",
    actorRole: "assistant",
    subagentId: "",
    projectId: "proj-1",
    taskTitle: "",
    taskRef: "{}",
    taskRefId: "",
    tags: [],
    filesTouched: [],
    toolName: "",
    toolStatus: "",
    deliveryRecordId: "",
    content: "synthetic content",
    fingerprint: "fp-1",
    version: 1,
    ...over,
  };
}

async function allPages(adapter: SearchIndexAdapter, query: Omit<SearchQuery, "cursor">): Promise<SearchHit[]> {
  const out: SearchHit[] = [];
  let cursor = undefined as SearchQuery["cursor"];
  for (let i = 0; i < 1000; i++) {
    const page = await adapter.query({ ...query, cursor });
    out.push(...page.hits);
    if (!page.next) return out;
    cursor = page.next;
  }
  throw new Error("pagination did not terminate");
}

const key = (h: { eventId: string; tsMs: number }) => `${h.tsMs}:${h.eventId}`;

/**
 * Walks scanFingerprintsPage to the end, `between` awaited after each page that has a
 * `next`. Returns the rows and the number of calls.
 */
async function walkPages(
  adapter: SearchIndexAdapter,
  scope: SearchScope,
  fromMs: number,
  limit: number,
  between: (page: number) => Promise<void> = async () => {},
): Promise<{ rows: SearchFingerprint[]; calls: number }> {
  const rows: SearchFingerprint[] = [];
  let after: SearchPosition | undefined;
  for (let calls = 1; calls <= 10_000; calls++) {
    const page = await adapter.scanFingerprintsPage!(scope, { fromMs, limit, ...(after ? { after } : {}) });
    expect(page.rows.length).toBeLessThanOrEqual(limit);
    rows.push(...page.rows);
    if (!page.next) return { rows, calls };
    // `next` is the last row served, so the walk always advances.
    expect(page.rows.length).toBe(limit);
    expect(page.next).toEqual(positionOf(page.rows[page.rows.length - 1]!));
    after = page.next;
    await between(calls);
  }
  throw new Error("page walk did not terminate");
}

export function searchIndexConformance(name: string, factory: () => Promise<ConformanceTarget>): void {
  describe(`SearchIndexAdapter conformance: ${name}`, () => {
    let target: ConformanceTarget;
    let a: SearchIndexAdapter;
    beforeAll(async () => {
      target = await factory();
      a = target.adapter;
      await a.ensureSchema();
    }, 120_000);
    afterAll(async () => {
      await target?.dispose?.();
    });

    it("declares capabilities, is healthy after ensureSchema, and ensureSchema is idempotent", async () => {
      const caps = a.capabilities();
      expect(caps.backend).toBe(a.backend);
      expect(caps.filters).toEqual(expect.arrayContaining(["session", "source", "kind", "project", "since", "until"]));
      await a.ensureSchema();
      expect(await a.health()).toEqual({ ok: true, code: "ok" });
    });

    it("isolates tenants by org and workspace", async () => {
      const org = `org-${rand()}`;
      const other = `org-${rand()}`;
      await a.upsert([
        syntheticDoc({ orgId: org, content: "needle shared" }),
        syntheticDoc({ orgId: org, workspaceId: "ws-b", content: "needle shared" }),
        syntheticDoc({ orgId: other, content: "needle shared" }),
      ]);
      const page = await a.query({ tenant: { orgId: org, workspaceId: "ws-a" }, q: "needle", order: "desc", limit: 10 });
      expect(page.hits).toHaveLength(1);
      expect(page.hits[0]!.orgId).toBe(org);
      expect(page.hits[0]!.workspaceId).toBe("ws-a");
    });

    it("matches q as a case-insensitive literal substring of the content prefix or task title", async () => {
      const org = `org-${rand()}`;
      const docs = [
        syntheticDoc({ orgId: org, eventId: "e-upper", content: "The QUICK brown fox" }),
        syntheticDoc({ orgId: org, eventId: "e-title", content: "nothing here", taskTitle: "Quick task" }),
        syntheticDoc({ orgId: org, eventId: "e-spaced", content: "q u i c k" }),
        syntheticDoc({ orgId: org, eventId: "e-late", content: `${"x".repeat(CONTENT_PREFIX_CHARS)}quick` }),
        syntheticDoc({ orgId: org, eventId: "e-edge", content: `${"x".repeat(CONTENT_PREFIX_CHARS - 5)}quick` }),
        syntheticDoc({ orgId: org, eventId: "e-pct", content: "rate 100% done_now" }),
        syntheticDoc({ orgId: org, eventId: "e-wild", content: "rate 1000 doneXnow" }),
        syntheticDoc({ orgId: org, eventId: "e-uni", content: "Ünïcode café résumé" }),
      ];
      await a.upsert(docs);
      const ids = async (q: string) =>
        (await a.query({ tenant: { orgId: org, workspaceId: "ws-a" }, q, order: "asc", limit: 50 })).hits
          .map((h) => h.eventId)
          .sort();
      const oracle = (q: string) => docs.filter((d) => matchesQuery(d, q)).map((d) => d.eventId).sort();
      for (const q of ["quick", "QUICK", "k b", "100%", "done_now", "café", "ab", "x".repeat(3) + "quick"]) {
        expect(await ids(q), `q=${JSON.stringify(q)}`).toEqual(oracle(q));
      }
      expect(await ids("quick")).toEqual(["e-edge", "e-title", "e-upper"]);
      expect(await ids("done_now")).toEqual(["e-pct"]);
    });

    it("applies the session, source, kind, project and inclusive time filters", async () => {
      const org = `org-${rand()}`;
      const t = 1_767_225_600_000;
      await a.upsert([
        syntheticDoc({ orgId: org, eventId: "a", tsMs: t, sessionId: "s1", source: "claude", kind: "message", projectId: "p1" }),
        syntheticDoc({ orgId: org, eventId: "b", tsMs: t + 1, sessionId: "s2", source: "claude", kind: "tool", projectId: "p1" }),
        syntheticDoc({ orgId: org, eventId: "c", tsMs: t + 2, sessionId: "s1", source: "codex", kind: "message", projectId: "p2" }),
        syntheticDoc({ orgId: org, eventId: "d", tsMs: t + 3, sessionId: "s1", source: "claude", kind: "message", projectId: "p2" }),
      ]);
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const ids = async (f: Partial<SearchQuery>) =>
        (await a.query({ tenant, order: "asc", limit: 50, ...f })).hits.map((h) => h.eventId);
      expect(await ids({ sessionId: "s1" })).toEqual(["a", "c", "d"]);
      expect(await ids({ source: "codex" })).toEqual(["c"]);
      expect(await ids({ kind: "tool" })).toEqual(["b"]);
      expect(await ids({ projectId: "p2" })).toEqual(["c", "d"]);
      expect(await ids({ sinceMs: t + 1, untilMs: t + 2 })).toEqual(["b", "c"]);
      expect(await ids({ sessionId: "s1", source: "claude", projectId: "p2" })).toEqual(["d"]);
    });

    it("actorName keeps only that author's events (a human-only search) and composes with q", async () => {
      // Optional filter: the read API refuses `origin=` for an adapter without it.
      if (!a.capabilities().filters.includes("actor")) return;
      const org = `org-${rand()}`;
      const t = 1_767_225_600_000;
      await a.upsert([
        syntheticDoc({ orgId: org, eventId: "h1", tsMs: t, actorName: "human", actorRole: "user", content: "yes, ship it" }),
        syntheticDoc({ orgId: org, eventId: "p1", tsMs: t + 1, actorName: "peer", actorRole: "user", content: "yes, ship it" }),
        syntheticDoc({ orgId: org, eventId: "t1", tsMs: t + 2, actorName: "task-notification", actorRole: "user", content: "yes, ship it" }),
        syntheticDoc({ orgId: org, eventId: "s1", tsMs: t + 3, actorName: "subagent", actorRole: "user", content: "yes, ship it" }),
        syntheticDoc({ orgId: org, eventId: "u1", tsMs: t + 4, actorName: "", actorRole: "user", content: "yes, ship it" }),
        syntheticDoc({ orgId: org, eventId: "h2", tsMs: t + 5, actorName: "human", actorRole: "user", content: "no, wait" }),
      ]);
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const ids = async (f: Partial<SearchQuery>) =>
        (await a.query({ tenant, order: "asc", limit: 50, ...f })).hits.map((h) => h.eventId);
      expect(await ids({ actorName: "human" })).toEqual(["h1", "h2"]);
      expect(await ids({ actorName: "human", q: "YES" })).toEqual(["h1"]);
      expect(await ids({ actorName: "peer" })).toEqual(["p1"]);
      expect(await ids({ q: "yes" })).toEqual(["h1", "p1", "t1", "s1", "u1"]);
      await expect(a.query({ tenant, order: "asc", limit: 5, actorName: "" })).rejects.toMatchObject({ code: "invalid_argument" });
    });

    it("excludeSessionIds removes those sessions on every page and composes with the session filter", async () => {
      // Optional filter: an adapter that does not declare it may ignore the field
      // (and the read API refuses READ_API_SEARCH_PARTIAL_SESSIONS=skip for it).
      if (!a.capabilities().filters.includes("exclude_session")) return;
      const org = `org-${rand()}`;
      const t = 1_767_225_600_000;
      const docs = Array.from({ length: 9 }, (_, i) =>
        syntheticDoc({ orgId: org, eventId: `x${i}`, tsMs: t + i, sessionId: `s${i % 3}`, content: "needle" }));
      await a.upsert(docs);
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const ids = (hits: readonly SearchHit[]) => hits.map((h) => h.eventId);
      const kept = docs.filter((d) => d.sessionId !== "s1").map((d) => d.eventId);
      // An id that would end an interpolated literal is only a value.
      for (const limit of [1, 2, 50]) {
        expect(ids(await allPages(a, { tenant, order: "asc", limit, q: "needle", excludeSessionIds: ["s1", "s'2"] }))).toEqual(kept);
      }
      expect(ids(await allPages(a, { tenant, order: "asc", limit: 2, excludeSessionIds: ["s1", "s2"] }))).toEqual(["x0", "x3", "x6"]);
      // A query for an excluded session answers no rows, not an error.
      expect((await a.query({ tenant, order: "asc", limit: 50, sessionId: "s1", excludeSessionIds: ["s1"] })).hits).toEqual([]);
      expect(ids((await a.query({ tenant, order: "asc", limit: 50, sessionId: "s0", excludeSessionIds: ["s1"] })).hits)).toEqual(["x0", "x3", "x6"]);
      // Empty excludes nothing; the cap is accepted.
      expect(await allPages(a, { tenant, order: "asc", limit: 50, excludeSessionIds: [] })).toHaveLength(9);
      const atCap = Array.from({ length: MAX_EXCLUDED_SESSIONS }, (_, i) => (i === 0 ? "s0" : `none-${i}`));
      expect(ids(await allPages(a, { tenant, order: "asc", limit: 50, excludeSessionIds: atCap })).sort()).toEqual(docs.filter((d) => d.sessionId !== "s0").map((d) => d.eventId).sort());
    });

    it("pages in exact (ts, session, source, machine, kind, event) order with no gaps or repeats", async () => {
      const org = `org-${rand()}`;
      const t = 1_767_225_600_000;
      const docs: SearchEventDocument[] = [];
      for (let i = 0; i < 23; i++) {
        docs.push(
          syntheticDoc({
            orgId: org,
            tsMs: t + (i % 4), // many ties on ts
            sessionId: `s${i % 3}`,
            machineId: `m${i % 2}`,
            eventId: `e${String(i).padStart(2, "0")}`,
            content: i % 2 ? "match odd" : "match even",
          }),
        );
      }
      await a.upsert(docs);
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const expected = [...docs].sort(comparePositions).map(key);
      for (const limit of [1, 5, 7, 23, 100]) {
        expect((await allPages(a, { tenant, order: "asc", limit })).map(key)).toEqual(expected);
        expect((await allPages(a, { tenant, order: "desc", limit })).map(key)).toEqual([...expected].reverse());
      }
      const odd = await allPages(a, { tenant, q: "odd", order: "desc", limit: 4 });
      expect(odd.map(key)).toEqual(docs.filter((d) => d.content.includes("odd")).sort(comparePositions).reverse().map(key));
    });

    it("matches the reference case folding for Unicode content and titles", async () => {
      const org = `org-${rand()}`;
      const docs = [
        syntheticDoc({ orgId: org, eventId: "unicode-i", content: "i" }),
        syntheticDoc({ orgId: org, eventId: "unicode-dot", content: "İ" }),
        syntheticDoc({ orgId: org, eventId: "unicode-sigma", content: "", taskTitle: "ΟΣ" }),
      ];
      await a.upsert(docs);
      for (const q of ["İ", "i\u0307", "οσ", "ος"]) {
        const page = await a.query({ tenant: { orgId: org, workspaceId: "ws-a" }, order: "asc", limit: 10, q });
        expect(page.hits.map((hit) => hit.eventId).sort()).toEqual(
          docs.filter((doc) => matchesQuery(doc, q)).map((doc) => doc.eventId).sort(),
        );
      }
    });

    it("upserts idempotently and never lets an older version replace a newer row", async () => {
      const org = `org-${rand()}`;
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const base = syntheticDoc({ orgId: org, eventId: "v", content: "version two", fingerprint: "fp-2", version: 2 });
      await a.upsert([base]);
      await a.upsert([base]);
      await a.upsert([{ ...base, content: "version one", fingerprint: "fp-1", version: 1 }]);
      let hits = (await a.query({ tenant, order: "asc", limit: 10 })).hits;
      expect(hits.map((h) => h.content)).toEqual(["version two"]);
      await a.upsert([{ ...base, content: "version three", fingerprint: "fp-3", version: 3 }]);
      hits = (await a.query({ tenant, order: "asc", limit: 10 })).hits;
      expect(hits.map((h) => h.content)).toEqual(["version three"]);
      const scan = await a.scanFingerprints({ ...tenant, source: base.source, sessionIds: [base.sessionId] }, { fromMs: 0, limit: 10 });
      expect(scan.rows.map((r) => r.fingerprint)).toEqual(["fp-3"]);
    });

    it("returns stored fields intact and truncates content to maxContentChars", async () => {
      const org = `org-${rand()}`;
      const doc = syntheticDoc({
        orgId: org,
        content: "héllo wörld",
        tags: ["t1", "t2"],
        filesTouched: ["a.ts"],
        toolName: "Edit",
        toolStatus: "ok",
        taskTitle: "Title",
        taskRef: '{"id":"T-1"}',
        taskRefId: "T-1",
        deliveryRecordId: "rec-1",
      });
      await a.upsert([doc]);
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const full = (await a.query({ tenant, order: "asc", limit: 1 })).hits[0]!;
      const { fingerprint: _f, version: _v, taskRefId: _t, deliveryRecordId: _d, ...expected } = doc;
      expect(full).toEqual(expected);
      const cut = (await a.query({ tenant, order: "asc", limit: 1, maxContentChars: 4 })).hits[0]!;
      expect(cut.content).toBe("héll");
    });

    it("deletes exact rows and erases a whole scope at any ts, leaving other scopes", async () => {
      const org = `org-${rand()}`;
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const keep = syntheticDoc({ orgId: org, sessionId: "keep", content: "erasable text" });
      const s1 = syntheticDoc({ orgId: org, sessionId: "gone", tsMs: 1, content: "erasable text" });
      const s2 = syntheticDoc({ orgId: org, sessionId: "gone-alias", tsMs: 9_999_999_999_999, content: "erasable text" });
      const one = syntheticDoc({ orgId: org, sessionId: "keep", eventId: "drop-me", content: "erasable text" });
      await a.upsert([keep, s1, s2, one]);
      expect(await a.deleteEvents(tenant, [positionOf(one)])).toEqual({ deleted: 1 });
      await a.eraseScope({ ...tenant, source: "claude", sessionIds: ["gone", "gone-alias"] });
      const left = await allPages(a, { tenant, q: "erasable", order: "asc", limit: 10 });
      expect(left.map((h) => h.sessionId)).toEqual(["keep"]);
      const scan = await a.scanFingerprints({ ...tenant, source: "claude", sessionIds: ["gone", "gone-alias"] }, { fromMs: 0, limit: 10 });
      expect(scan.rows).toEqual([]);
    });

    it("scans fingerprints from a ts bound and flags truncation", async () => {
      const org = `org-${rand()}`;
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const docs = [0, 1, 2, 3].map((i) => syntheticDoc({ orgId: org, tsMs: 1000 + i, eventId: `f${i}`, fingerprint: `fp${i}` }));
      await a.upsert(docs);
      const scope = { ...tenant, source: "claude", sessionIds: ["sess-1"] };
      const all = await a.scanFingerprints(scope, { fromMs: 1001, limit: 10 });
      expect(all.truncated).toBe(false);
      expect(all.rows.map((r) => [r.eventId, r.fingerprint, r.workspaceId])).toEqual([
        ["f1", "fp1", "ws-a"],
        ["f2", "fp2", "ws-a"],
        ["f3", "fp3", "ws-a"],
      ]);
      const cut = await a.scanFingerprints(scope, { fromMs: 0, limit: 2 });
      expect(cut.truncated).toBe(true);
      expect(cut.rows).toHaveLength(2);
    });

    it("pages a fingerprint scan in scanFingerprints order: ts ties, alias session ids, the fromMs floor", async () => {
      // Optional method: the staged reconcile requires it (SEARCH_INDEX_STAGED_RECONCILE_ENABLED).
      if (!a.scanFingerprintsPage) return;
      const org = `org-${rand()}`;
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const t = 1_767_225_600_000;
      const docs: SearchEventDocument[] = [];
      for (let i = 0; i < 17; i++)
        docs.push(syntheticDoc({
          orgId: org,
          tsMs: t + (i % 3), // ties on ts across both session ids
          sessionId: i % 2 ? "sess-alias" : "sess-a",
          machineId: `m${i % 4}`,
          kind: i % 5 ? "message" : "tool_call",
          eventId: `p${String(i).padStart(2, "0")}`,
          fingerprint: `fp${i}`,
        }));
      // Outside the scope (another session, source, workspace) and below the floor.
      docs.push(syntheticDoc({ orgId: org, tsMs: t, sessionId: "other", eventId: "x-session" }));
      docs.push(syntheticDoc({ orgId: org, tsMs: t, sessionId: "sess-a", source: "codex", eventId: "x-source" }));
      docs.push(syntheticDoc({ orgId: org, tsMs: t, sessionId: "sess-a", workspaceId: "ws-b", eventId: "x-workspace" }));
      docs.push(syntheticDoc({ orgId: org, tsMs: t - 1, sessionId: "sess-a", eventId: "x-floor" }));
      await a.upsert(docs);
      const scope = { ...tenant, source: "claude", sessionIds: ["sess-a", "sess-alias"] };
      const whole = await a.scanFingerprints(scope, { fromMs: t, limit: 1000 });
      expect(whole.truncated).toBe(false);
      expect(whole.rows.map((r) => r.eventId).sort()).toEqual(docs.slice(0, 17).map((d) => d.eventId).sort());
      expect([...whole.rows].sort(comparePositions)).toEqual(whole.rows);
      for (const limit of [1, 2, 3, 5, 17, 100]) {
        const walk = await walkPages(a, scope, t, limit);
        expect(walk.rows, `limit=${limit}`).toEqual(whole.rows);
        expect(walk.calls, `limit=${limit}`).toBe(Math.max(1, Math.ceil(17 / limit)));
      }
      // The floor is inclusive and applies on every page.
      const floored = await walkPages(a, scope, t + 2, 2);
      expect(floored.rows).toEqual(whole.rows.filter((r) => r.tsMs >= t + 2));
      // Exhausted: a page after the last row is empty and has no next.
      expect(await a.scanFingerprintsPage(scope, { fromMs: t, limit: 5, after: positionOf(whole.rows[whole.rows.length - 1]!) })).toEqual({ rows: [] });
      // An empty scope is one empty page.
      expect(await a.scanFingerprintsPage({ ...scope, sessionIds: ["none"] }, { fromMs: 0, limit: 5 })).toEqual({ rows: [] });
    });

    it("a fingerprint page walk skips no row present throughout when rows are deleted between pages", async () => {
      if (!a.scanFingerprintsPage) return;
      const org = `org-${rand()}`;
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const t = 1_767_225_600_000;
      const docs = Array.from({ length: 12 }, (_, i) =>
        syntheticDoc({ orgId: org, tsMs: t + Math.floor(i / 3), machineId: `m${i % 3}`, eventId: `d${String(i).padStart(2, "0")}` }));
      await a.upsert(docs);
      const scope = { ...tenant, source: "claude", sessionIds: ["sess-1"] };
      const before = (await a.scanFingerprints(scope, { fromMs: 0, limit: 100 })).rows;
      expect(before).toHaveLength(12);
      // After the first page (rows 0..3): delete its last row (the walk's `after`) and a row two pages on.
      const cursor = before[3]!;
      const ahead = before[8]!;
      const walk = await walkPages(a, scope, 0, 4, async (page) => {
        if (page === 1) expect(await a.deleteEvents(tenant, [positionOf(cursor), positionOf(ahead)])).toEqual({ deleted: 2 });
      });
      const seen = walk.rows.map(key);
      expect(new Set(seen).size).toBe(seen.length); // nothing twice
      expect(seen).toEqual(before.filter((r) => r !== ahead).map(key)); // the cursor row was served before it went
      expect((await a.scanFingerprints(scope, { fromMs: 0, limit: 100 })).rows).toHaveLength(10);
    });

    it("refuses an invalid fingerprint page request before touching the backend", async () => {
      if (!a.scanFingerprintsPage) return;
      const scope = { orgId: "org-x", workspaceId: "ws-a", source: "claude", sessionIds: ["s"] };
      const bad = async (p: Promise<unknown>) => {
        const error = await p.then(() => null, (e: unknown) => e);
        expect(error).toBeInstanceOf(SearchIndexError);
        expect((error as SearchIndexError).code).toBe("invalid_argument");
      };
      const at = { tsMs: 1, sessionId: "s", source: "claude", machineId: "m", kind: "message", eventId: "e" };
      await bad(a.scanFingerprintsPage(scope, { fromMs: 0, limit: 0 }));
      await bad(a.scanFingerprintsPage(scope, { fromMs: 0, limit: MAX_SCAN_PAGE + 1 }));
      await bad(a.scanFingerprintsPage(scope, { fromMs: 0.5, limit: 1 }));
      await bad(a.scanFingerprintsPage(scope, { fromMs: 0, limit: 1, after: { ...at, tsMs: 1.5 } }));
      await bad(a.scanFingerprintsPage(scope, { fromMs: 0, limit: 1, after: { ...at, eventId: "" } }));
      await bad(a.scanFingerprintsPage({ ...scope, sessionIds: [] }, { fromMs: 0, limit: 1 }));
    });

    it("converges a scope through planScopeReconcile (insert, change, move, retract)", async () => {
      const org = `org-${rand()}`;
      const tenant = { orgId: org, workspaceId: "ws-a" };
      const scope = { ...tenant, source: "claude", sessionIds: ["sess-1"] };
      const v1 = [
        syntheticDoc({ orgId: org, eventId: "r1", tsMs: 10, fingerprint: "a", content: "reconcile one" }),
        syntheticDoc({ orgId: org, eventId: "r2", tsMs: 20, fingerprint: "a", content: "reconcile two" }),
        syntheticDoc({ orgId: org, eventId: "r3", tsMs: 30, fingerprint: "a", content: "reconcile three" }),
      ];
      const apply = async (desired: SearchEventDocument[], complete = true) => {
        const scan = await a.scanFingerprints(scope, { fromMs: 0, limit: 100 });
        const plan = planScopeReconcile(desired, scan.rows, { complete: complete && !scan.truncated });
        if (plan.upserts.length) await a.upsert(plan.upserts);
        if (plan.deletes.length) await a.deleteEvents(tenant, plan.deletes);
        return plan;
      };
      expect((await apply(v1)).upserts).toHaveLength(3);
      expect(await apply(v1)).toEqual({ upserts: [], deletes: [] });
      const v2 = [
        { ...v1[0]!, fingerprint: "b", content: "reconcile one edited", version: 2 },
        { ...v1[1]!, tsMs: 25, version: 2 },
      ];
      // An incomplete read inserts but never deletes.
      expect((await apply(v2, false)).deletes).toEqual([]);
      const plan = await apply(v2);
      expect(plan.deletes.map((d) => [d.eventId, d.tsMs]).sort()).toEqual([
        ["r2", 20],
        ["r3", 30],
      ]);
      const rows = await allPages(a, { tenant, order: "asc", limit: 10 });
      expect(rows.map((r) => [r.eventId, r.tsMs, r.content])).toEqual([
        ["r1", 10, "reconcile one edited"],
        ["r2", 25, "reconcile two"],
      ]);
    });

    it("rejects invalid input before touching the backend", async () => {
      const bad = async (p: Promise<unknown>) => {
        const error = await p.then(() => null, (e: unknown) => e);
        expect(error).toBeInstanceOf(SearchIndexError);
        expect((error as SearchIndexError).code).toBe("invalid_argument");
      };
      const tenant = { orgId: "org-x", workspaceId: "ws-a" };
      await bad(a.query({ tenant: { orgId: "", workspaceId: "w" }, order: "asc", limit: 1 }));
      await bad(a.query({ tenant, order: "asc", limit: 0 }));
      await bad(a.query({ tenant, order: "asc", limit: 1, q: "" }));
      await bad(a.query({ tenant, order: "sideways" as "asc", limit: 1 }));
      await bad(a.query({ tenant, order: "asc", limit: 1, excludeSessionIds: Array.from({ length: MAX_EXCLUDED_SESSIONS + 1 }, (_, i) => `s-${i}`) }));
      await bad(a.query({ tenant, order: "asc", limit: 1, excludeSessionIds: ["s-1", ""] }));
      await bad(a.query({ tenant, order: "asc", limit: 1, excludeSessionIds: "s-1" as unknown as string[] }));
      await bad(a.upsert([{ ...syntheticDoc({ orgId: "org-x" }), tsMs: 1.5 }]));
      await bad(a.eraseScope({ ...tenant, source: "claude", sessionIds: [] }));
    });
  });
}
