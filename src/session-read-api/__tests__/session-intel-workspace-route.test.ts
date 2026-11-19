/**
 * GET /v1/session-intel/workspace (docs/session-intel-workspace-api.md): the exact flag gate (501
 * session_intel_disabled), auth and workspace attestation, the coordination canary and allowlist, no other
 * parameter, tenant isolation, the bounds and caps, no statement or transcript text, the class mapping (pinned to
 * src/session-intel/replay/classes.ts), the coverage object, and a failed claims read degrading the view only.
 */
import { describe, expect, test } from "bun:test";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import type { Directory, DirectoryListArgs, DirectoryListRow } from "../directory.ts";
import {
  alertClassOf, SESSION_INTEL_CONTRACT, SESSION_INTEL_NOTES, sqlSessionIntel,
  type SessionIntelAlertRow, type SessionIntelClaimRow, type SessionIntelSource, type SessionRef,
} from "../session-intel-workspace.ts";
import { TENANT_SETTINGS_SQL, type TenantRunner } from "../liveness.ts";
import { classOfKind } from "../../session-intel/replay/classes.ts";
import { memoryLiveness } from "./support.ts";
import { parseAdvisoryNotifyRows } from "../advisory-priority.ts";

const ID: VerifiedIdentity = { userId: "u", orgId: "org", workspaceId: "ws", scopes: ["rth:read"] };
const auth = { authorization: "Bearer good" };
const verify = async (token: string) => {
  if (token !== "good") throw new AuthError(401, "invalid_token", "invalid");
  return ID;
};
const PATH = "/v1/session-intel/workspace?workspace=ws";
const NOW = Date.now();
const minutesAgo = (m: number) => new Date(NOW - m * 60_000);

function dirRow(i: number, over: Partial<DirectoryListRow> = {}): DirectoryListRow {
  return {
    org_id: "org", workspace_id: "ws", harness: "claude", session_id: `s-${i}`, created_at: minutesAgo(600), last_activity_at: minutesAgo(i),
    title: `Session ${i}`, task_title: null, agent_names: null, project_id: "proj", project_key: null, project_key_method: null,
    git_repo: null, git_branch: null, initial_commit: null, originator: null, agent_version: null, models: null, state: "active",
    event_count: 1, input_tokens: 0, output_tokens: 0, cost_usd_micros: null, machine_ids: null, user_ids: null, parent_session_ids: null,
    child_session_count: 0, short_summary: `scrubbed summary ${i}`, short_summary_generated_at: minutesAgo(i + 1), catalog_projected_at: null,
    projected_at: null, ...over,
  };
}

/** A directory over fixed rows with the list function's semantics: bounds, newest first, limit + 1. */
function memoryDirectory(rows: DirectoryListRow[]) {
  const calls: DirectoryListArgs[] = [];
  const directory: Directory = {
    async list(orgId, workspaceId, a) {
      calls.push(a);
      const t = (r: DirectoryListRow) => new Date(r.last_activity_at).getTime();
      return rows
        .filter((r) => r.org_id === orgId && r.workspace_id === workspaceId || r.session_id.startsWith("leak"))
        .filter((r) => (!a.lastTsAfter || t(r) >= a.lastTsAfter.getTime()) && (!a.lastTsBefore || t(r) < a.lastTsBefore.getTime()))
        .sort((x, y) => t(y) - t(x))
        .slice(0, a.limit + 1);
    },
    async activity() { return []; },
    async query() { return []; },
    async queryActivity() { return []; },
  };
  return { directory, calls };
}

function alertRow(i: number, over: Partial<SessionIntelAlertRow> = {}): SessionIntelAlertRow {
  return {
    advisoryId: i.toString(16).padStart(16, "0"), urgency: "high", kind: "duplicate_work", artifact: "supabase/migrations/062130_x.sql",
    pair: [{ source: "claude", sessionId: "s-1" }, { source: "codex", sessionId: "s-2" }],
    recipients: [{ source: "claude", sessionId: "s-1", acked: false }, { source: "codex", sessionId: "s-2", acked: true }],
    createdAt: minutesAgo(i).toISOString(), deliveredAt: null,
    evidence: [{ sessionId: "s-1", eventId: "e-1", ts: "2026-11-12T09:58:12.000Z", kind: "tool_result" }], ...over,
  };
}

function memorySource(init: { alerts?: SessionIntelAlertRow[]; claims?: SessionIntelClaimRow[]; claimsFail?: unknown; claimsNull?: boolean; alertsNull?: boolean } = {}) {
  const calls: { op: string; orgId: string; workspaceId: string; keys?: SessionRef[]; limit: number }[] = [];
  const source: SessionIntelSource = {
    async alerts(orgId, workspaceId, limit) {
      calls.push({ op: "alerts", orgId, workspaceId, limit });
      return init.alertsNull ? null : (init.alerts ?? []).slice(0, limit);
    },
    async claims(orgId, workspaceId, keys, limit) {
      calls.push({ op: "claims", orgId, workspaceId, keys, limit });
      if (init.claimsFail) throw init.claimsFail;
      if (init.claimsNull) return null;
      const wanted = new Set(keys.map((k) => `${k.source}:${k.sessionId}`));
      return (init.claims ?? []).filter((c) => wanted.has(`${c.source}:${c.sessionId}`)).slice(0, limit);
    },
  };
  return { source, calls };
}

const api = (opts: {
  enabled?: boolean; source?: SessionIntelSource; directory?: Directory | null; canary?: ReadonlySet<string> | "*"; logs?: Record<string, unknown>[];
  notifyRows?: ReadonlySet<string>;
} = {}) => createReadApi({
  ...(opts.notifyRows ? { advisoryNotifyRows: opts.notifyRows } : {}),
  verify, adapter: {} as never, liveness: memoryLiveness().liveness, completeSinceMs: 0,
  directoryWorkspaces: opts.canary ?? new Set([ID.workspaceId]),
  ...(opts.directory === null ? {} : { directory: opts.directory ?? memoryDirectory([dirRow(1)]).directory }),
  ...(opts.enabled === undefined ? {} : { sessionIntel: { enabled: opts.enabled, source: opts.source ?? memorySource().source } }),
  ...(opts.logs ? { log: (f: Record<string, unknown>) => { opts.logs!.push(f); } } : {}),
});
const get = async (app: ReturnType<typeof api>, path = PATH, headers: Record<string, string> = auth) => {
  const res = await app.request(path, { headers });
  return { status: res.status, body: await res.json() as any };
};

describe("GET /v1/session-intel/workspace: gate, auth and scope", () => {
  test("off (absent or not enabled) is the standard 501 envelope with reason session_intel_disabled, and nothing is read", async () => {
    for (const enabled of [undefined, false]) {
      const mem = memorySource();
      const dir = memoryDirectory([dirRow(1)]);
      const { status, body } = await get(api({ enabled, source: mem.source, directory: dir.directory }));
      expect(status).toBe(501);
      expect(body.error).toEqual({ code: "unsupported_by_backend", message: "not served by this backend: session_intel_disabled", reason: "session_intel_disabled" });
      expect(typeof body.correlationId).toBe("string");
      expect(mem.calls).toEqual([]);
      expect(dir.calls).toEqual([]);
    }
    expect((await api({ enabled: false }).request(PATH)).status).toBe(401);
  });

  test("bearer, workspace attestation, the coordination canary and the allowlist come before any read", async () => {
    const mem = memorySource();
    const app = api({ enabled: true, source: mem.source });
    expect((await app.request(PATH)).status).toBe(401);
    expect((await app.request(PATH, { headers: { authorization: "Bearer bad" } })).status).toBe(401);
    expect((await get(app, "/v1/session-intel/workspace?workspace=other")).status).toBe(403);
    expect((await get(app, "/v1/session-intel/workspace")).body.error.reason).toBe("org_scope");
    expect((await get(app, `${PATH}&workspace=ws`)).status).toBe(400);
    expect((await get(app, `${PATH}&tag=x`)).body.error.reason).toBe("param_tag");
    for (const p of ["cursor", "q", "limit", "source"]) {
      const r = await get(app, `${PATH}&${p}=x`);
      expect([p, r.status, r.body.error.reason]).toEqual([p, 501, `param_${p}`]);
    }
    for (const canary of [new Set<string>(), new Set(["other"])]) {
      const r = await get(api({ enabled: true, source: mem.source, canary }));
      expect([r.status, r.body.error.reason]).toEqual([501, "coordination_workspace_not_enabled"]);
    }
    expect(mem.calls).toEqual([]);
    expect((await get(api({ enabled: true, source: mem.source, canary: "*" }))).status).toBe(200);
  });

  test("tenant: every read is the token's org and workspace; a directory row of another tenant refuses the response", async () => {
    const mem = memorySource();
    const dir = memoryDirectory([dirRow(1), dirRow(2, { org_id: "org-b" }), dirRow(3, { workspace_id: "ws-b" })]);
    const ok = await get(api({ enabled: true, source: mem.source, directory: dir.directory }));
    expect(ok.status).toBe(200);
    expect(ok.body.status.map((s: any) => s.sessionId)).toEqual(["s-1"]);
    expect(mem.calls.every((c) => c.orgId === "org" && c.workspaceId === "ws")).toBe(true);
    const leak = memoryDirectory([dirRow(1), dirRow(2, { session_id: "leak-1", org_id: "org-b" })]);
    const logs: Record<string, unknown>[] = [];
    const refused = await get(api({ enabled: true, source: memorySource().source, directory: leak.directory, logs }));
    expect(refused.status).toBe(500);
    expect(refused.body.error.code).toBe("internal");
    expect(logs.some((l) => l.reason === "cross_tenant_rows")).toBe(true);
  });

  test("not configured or not migrated is a 501 with its reason; a failing alerts read is a 503", async () => {
    expect((await get(api({ enabled: true, directory: null }))).body.error.reason).toBe("directory_not_configured");
    expect((await get(api({ enabled: true, source: memorySource({ alertsNull: true }).source }))).body.error.reason).toBe("session_intel_not_migrated");
    const nullDirectory: Directory = { ...memoryDirectory([]).directory, list: async () => null };
    expect((await get(api({ enabled: true, directory: nullDirectory }))).body.error.reason).toBe("directory_not_migrated");
    const failing: SessionIntelSource = { ...memorySource().source, alerts: async () => { throw Object.assign(new Error("boom"), { code: "42501" }); } };
    const r = await get(api({ enabled: true, source: failing }));
    expect([r.status, r.body.error.code]).toEqual([503, "backend_unavailable"]);
  });
});

describe("GET /v1/session-intel/workspace: the view", () => {
  const rows = [
    dirRow(5),
    dirRow(10, { state: "done", short_summary: null, short_summary_generated_at: null }),
    dirRow(20, { harness: "codex", session_id: "s-20", project_id: null, state: "unknown" }),
    dirRow(60 * 3, { state: "abandoned" }),
    dirRow(60 * 24 * 3, { state: "active" }),
    dirRow(60 * 24 * 8),
  ];
  const claims: SessionIntelClaimRow[] = [
    { source: "claude", sessionId: "s-5", kind: "intent", title: "Add the workspace route", updatedAt: minutesAgo(4).toISOString() },
    { source: "claude", sessionId: "s-5", kind: "decision", title: "Use 501 when off", updatedAt: minutesAgo(3).toISOString() },
    { source: "claude", sessionId: "s-180", kind: "decision", title: "Slot 062170 is provisional", updatedAt: minutesAgo(200).toISOString() },
  ];

  test("contract, alerts, status, handoffs and coverage; field sets exact; class mapped", async () => {
    const mem = memorySource({ alerts: [alertRow(1), alertRow(2, { urgency: "low", kind: "concurrent_edit", deliveredAt: minutesAgo(1).toISOString() })], claims });
    const dir = memoryDirectory(rows);
    const { status, body } = await get(api({ enabled: true, source: mem.source, directory: dir.directory }));
    expect(status).toBe(200);
    expect(Object.keys(body).sort()).toEqual(["alerts", "contract", "coverage", "handoffs", "nextCursor", "status"]);
    expect(body.contract).toBe(SESSION_INTEL_CONTRACT);
    expect(body.nextCursor).toBeNull();
    expect(body.alerts).toEqual([
      { advisoryId: "0000000000000001", urgency: "high", kind: "duplicate_work", class: "collision", priority: 1, notifyEligible: false, artifact: "supabase/migrations/062130_x.sql",
        sessions: [{ source: "claude", sessionId: "s-1" }, { source: "codex", sessionId: "s-2" }], createdAt: minutesAgo(1).toISOString(), deliveredAt: null,
        acked: [{ source: "claude", sessionId: "s-1", acked: false }, { source: "codex", sessionId: "s-2", acked: true }],
        evidence: [{ sessionId: "s-1", eventId: "e-1", ts: "2026-11-12T09:58:12.000Z", kind: "tool_result" }] },
      expect.objectContaining({ advisoryId: "0000000000000002", urgency: "low", kind: "concurrent_edit", class: "overlap", priority: 3 }),
    ]);
    // Status: within 2 hours and not ended, newest first.
    expect(body.status.map((s: any) => s.sessionId)).toEqual(["s-5", "s-20"]);
    expect(body.status[0]).toEqual({
      source: "claude", sessionId: "s-5", projectId: "proj", title: "Session 5", shortSummary: "scrubbed summary 5",
      summaryAt: minutesAgo(6).toISOString(), state: "active", lastActivityAt: minutesAgo(5).toISOString(),
      openIntents: ["Add the workspace route"], hasClaims: true,
    });
    expect(body.status[1]).toMatchObject({ source: "codex", projectId: null, state: "unknown", openIntents: [], hasClaims: false });
    // Handoffs: ended within 2 hours, and anything 2 hours to 7 days old; older than 7 days is not read.
    expect(body.handoffs.map((s: any) => s.sessionId)).toEqual(["s-10", "s-180", `s-${60 * 24 * 3}`]);
    expect(Object.keys(body.handoffs[0]).sort()).toEqual(["decisions", "hasClaims", "lastActivityAt", "openIntents", "sessionId", "shortSummary", "source", "state", "summaryAt", "title"]);
    expect(body.handoffs[0]).toMatchObject({ shortSummary: null, summaryAt: null, state: "done", decisions: [], hasClaims: false });
    expect(body.handoffs[1]).toMatchObject({ decisions: ["Slot 062170 is provisional"], openIntents: [], hasClaims: true });
    expect(body.coverage).toEqual({
      sessionsActive: 2, sessionsWithSummary: 2, sessionsWithClaims: 1,
      claimsNewestAt: minutesAgo(3).toISOString(), summariesNewestAt: minutesAgo(6).toISOString(),
      claimsExtraction: "unknown", notes: [SESSION_INTEL_NOTES.searchNotServed],
    });
    // The directory reads: the active page (200) and the older page (100), with their windows.
    expect(dir.calls.map((c) => [c.limit, c.lastTsBefore === undefined])).toEqual([[200, true], [100, false]]);
    expect(NOW - dir.calls[0]!.lastTsAfter!.getTime()).toBeLessThan(2 * 3600_000 + 60_000);
    expect(dir.calls[1]!.lastTsBefore!.getTime()).toBe(dir.calls[0]!.lastTsAfter!.getTime());
    // The claims read: every listed session once, 8 titles each.
    const claimCall = mem.calls.find((c) => c.op === "claims")!;
    expect(claimCall.keys!.length).toBe(5);
    expect(claimCall.limit).toBe(40);
  });

  test("a failed claims read (an error, 42883 or not migrated) degrades the view: intents empty, hasClaims and the claims coverage null, a note", async () => {
    for (const init of [{ claimsFail: Object.assign(new Error("no fn"), { code: "42883" }) }, { claimsFail: new Error("timeout") }, { claimsNull: true }]) {
      const logs: Record<string, unknown>[] = [];
      const mem = memorySource({ alerts: [alertRow(1)], claims, ...init });
      const { status, body } = await get(api({ enabled: true, source: mem.source, directory: memoryDirectory(rows).directory, logs }));
      expect(status).toBe(200);
      expect(body.alerts.length).toBe(1);
      expect(body.status.map((s: any) => [s.sessionId, s.openIntents, s.hasClaims])).toEqual([["s-5", [], null], ["s-20", [], null]]);
      expect(body.handoffs.every((s: any) => s.decisions.length === 0 && s.openIntents.length === 0 && s.hasClaims === null)).toBe(true);
      expect(body.coverage).toMatchObject({ sessionsActive: 2, sessionsWithSummary: 2, sessionsWithClaims: null, claimsNewestAt: null, claimsExtraction: "unknown" });
      expect(body.coverage.notes).toContain(SESSION_INTEL_NOTES.claimsUnavailable);
      expect(logs.some((l) => l.reason === "session_intel_claims_unavailable")).toBe(true);
      expect(JSON.stringify(logs)).not.toMatch(/s-5|Session 5|no fn|timeout/);
    }
  });

  test("bounds: alerts 100, status 200, handoffs 100, each with its fixed note; titles cut at 120", async () => {
    const many = [
      ...Array.from({ length: 230 }, (_, i) => dirRow(i % 100, { session_id: `a-${i}`, state: i < 30 ? "done" : "active" })),
      ...Array.from({ length: 150 }, (_, i) => dirRow(200 + i, { session_id: `o-${i}` })),
    ];
    const longTitle = "x".repeat(300);
    const mem = memorySource({
      alerts: Array.from({ length: 150 }, (_, i) => alertRow(i + 1)),
      claims: Array.from({ length: 10 }, (_, i) => ({ source: "claude", sessionId: "a-40", kind: "intent" as const, title: `${longTitle}${i}`, updatedAt: minutesAgo(i).toISOString() })),
    });
    const { body } = await get(api({ enabled: true, source: mem.source, directory: memoryDirectory(many).directory }));
    expect(body.alerts.length).toBe(100);
    expect(body.status.length).toBeLessThanOrEqual(200);
    expect(body.handoffs.length).toBe(100);
    expect(body.coverage.notes).toEqual(expect.arrayContaining([SESSION_INTEL_NOTES.alertsCapped, SESSION_INTEL_NOTES.statusCapped, SESSION_INTEL_NOTES.handoffsCapped]));
    for (const note of body.coverage.notes) expect(Object.values(SESSION_INTEL_NOTES)).toContain(note);
    const s40 = body.status.find((s: any) => s.sessionId === "a-40");
    expect(s40.openIntents.length).toBe(3);
    const claimCall = mem.calls.find((c) => c.op === "claims")!;
    expect(claimCall.keys!.length).toBeLessThanOrEqual(300);
    expect(claimCall.limit).toBeLessThanOrEqual(2400);
  });

  test("no statement, evidence claim text or transcript text: only the fields the contract names", async () => {
    const secret = "TRANSCRIPT-BODY";
    const row = dirRow(1, { task_title: secret, git_branch: secret, agent_names: [secret], models: [secret] });
    const mem = memorySource({ alerts: [alertRow(1)], claims: [{ source: "claude", sessionId: "s-1", kind: "intent", title: "t", updatedAt: minutesAgo(1).toISOString() }] });
    const { body } = await get(api({ enabled: true, source: mem.source, directory: memoryDirectory([row]).directory }));
    expect(JSON.stringify(body)).not.toContain(secret);
    expect(JSON.stringify(body)).not.toMatch(/statement|"claim"/);
  });

  test("a session whose key no claim evidence can hold is not sent to the claims reader (which would refuse the batch) and has no claims", async () => {
    const mem = memorySource({ claims: [{ source: "claude", sessionId: "s-1", kind: "intent", title: "t", updatedAt: minutesAgo(1).toISOString() }] });
    const odd = [dirRow(1), dirRow(2, { harness: "Odd-Harness" }), dirRow(3, { session_id: "x".repeat(513) })];
    const { body } = await get(api({ enabled: true, source: mem.source, directory: memoryDirectory(odd).directory }));
    expect(mem.calls.find((c) => c.op === "claims")!.keys).toEqual([{ source: "claude", sessionId: "s-1" }]);
    expect(body.status.map((s: any) => s.hasClaims)).toEqual([true, false, false]);
    expect(body.coverage.sessionsWithClaims).toBe(1);
  });

  test("alert priority comes from the shared advisoryPriority (the same ranking as the advisories GET)", async () => {
    const kinds: Array<[string, "high" | "low", number]> = [
      ["constraint_violation", "high", 1], ["intent_conflict", "high", 1], ["superseded_work", "high", 1],
      ["concurrent_edit", "high", 2], ["stale_base", "high", 2], ["planned_fleet", "high", 3], ["duplicate_work", "low", 3],
    ];
    const mem = memorySource({ alerts: kinds.map(([kind, urgency], i) => alertRow(i + 1, { kind, urgency })) });
    const { body } = await get(api({ enabled: true, source: mem.source }));
    expect(body.alerts.map((a: any) => [a.kind, a.urgency, a.priority])).toEqual(kinds);
  });

  test("notifyEligible on alerts: all false by default, true only for an allowed (kind, urgency); flag off unchanged", async () => {
    const alerts = [alertRow(1), alertRow(2, { urgency: "low" }), alertRow(3, { kind: "superseded_work" })];
    const eligible = async (notifyRows?: ReadonlySet<string>) =>
      (await get(api({ enabled: true, source: memorySource({ alerts }).source, ...(notifyRows ? { notifyRows } : {}) }))).body.alerts.map((a: any) => a.notifyEligible);
    expect(await eligible()).toEqual([false, false, false]);
    expect(await eligible(parseAdvisoryNotifyRows("duplicate_work:high"))).toEqual([true, false, false]);
    const off = await get(api({ enabled: false, notifyRows: parseAdvisoryNotifyRows("duplicate_work:high") }));
    expect([off.status, off.body.error.reason]).toEqual([501, "session_intel_disabled"]);
  });

  test("an unmapped alert kind has class null and a note, never a silent none", async () => {
    const mem = memorySource({ alerts: [alertRow(1, { kind: "stale_base" })] });
    const { body } = await get(api({ enabled: true, source: mem.source }));
    expect(body.alerts[0].class).toBeNull();
    expect(body.coverage.notes).toContain(SESSION_INTEL_NOTES.alertClassUnmapped);
  });
});

describe("class mapping and the SQL source", () => {
  test("alertClassOf mirrors classes.ts classOfKind for every mapped kind and urgency, and is null where it throws", () => {
    const kinds = ["duplicate_work", "superseded_work", "concurrent_edit", "nearby_work", "similar_summary", "constraint_violation",
      "intent_conflict", "collision", "overlap", "planned_fleet", "none", "repeated_investigation", "stale_base", "made_up"];
    for (const kind of kinds) for (const urgency of ["high", "low"] as const) {
      let expected: string | null;
      try { expected = classOfKind(kind, urgency); } catch { expected = null; }
      expect([kind, urgency, alertClassOf(kind, urgency)]).toEqual([kind, urgency, expected]);
    }
  });

  test("sqlSessionIntel: READ ONLY tenant transactions, the installed check, the 062170 calls and wire parsing; null when not installed", async () => {
    const sql: { text: string; params?: unknown[] }[] = [];
    let installed = true;
    const run: TenantRunner = async (orgId, workspaceId, work) => {
      sql.push({ text: "BEGIN READ ONLY" }, { text: TENANT_SETTINGS_SQL, params: [orgId, workspaceId] });
      const out = await work({
        async query<R>(text: string, params?: unknown[]) {
          sql.push({ text, params });
          if (text.includes("to_regprocedure")) return { rows: [{ installed } as R] };
          if (text.includes("alerts_v1")) return { rows: [{
            advisory_id: "0000000000000001", urgency: "high", kind: "duplicate_work", artifact: "a",
            pair: JSON.stringify([{ source: "claude", sessionId: "s-1" }, { source: "codex", sessionId: "s-2" }]),
            recipients: [{ source: "claude", sessionId: "s-1", acked: true }], created_at: new Date(0), delivered_at: null,
            evidence: [{ sessionId: "s-1", eventId: "e", ts: "t", kind: "k", extra: "dropped" }],
          } as R] };
          return { rows: [{ source: "claude", session_id: "s:1", kind: "decision", title: "y".repeat(200), updated_at: new Date(0) } as R] };
        },
      });
      sql.push({ text: "COMMIT" });
      return out;
    };
    const src = sqlSessionIntel(run);
    expect(await src.alerts("org", "ws", 100)).toEqual([{
      advisoryId: "0000000000000001", urgency: "high", kind: "duplicate_work", artifact: "a",
      pair: [{ source: "claude", sessionId: "s-1" }, { source: "codex", sessionId: "s-2" }], recipients: [{ source: "claude", sessionId: "s-1", acked: true }],
      createdAt: new Date(0).toISOString(), deliveredAt: null, evidence: [{ sessionId: "s-1", eventId: "e", ts: "t", kind: "k" }],
    }]);
    expect(sql.find((s) => s.text.includes("session_intel_workspace_alerts_v1("))!.params).toEqual(["org", "ws", 100]);
    const claimRows = await src.claims("org", "ws", [{ source: "claude", sessionId: "s:1" }], 8);
    expect(claimRows).toEqual([{ source: "claude", sessionId: "s:1", kind: "decision", title: "y".repeat(120), updatedAt: new Date(0).toISOString() }]);
    expect(sql.find((s) => s.text.includes("session_intel_workspace_claims_v1("))!.params).toEqual(["org", "ws", ["claude:s:1"], 8]);
    expect(sql.filter((s) => s.text === "BEGIN READ ONLY").length).toBe(2);
    installed = false;
    expect(await src.alerts("org", "ws", 100)).toBeNull();
    expect(await src.claims("org", "ws", [{ source: "claude", sessionId: "s" }], 8)).toBeNull();
  });
});
