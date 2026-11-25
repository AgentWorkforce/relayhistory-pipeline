/**
 * GET /v1/session-intel/synthesis (docs/session-intel-workspace-synthesis.md, "Read route"): the exact flag gate (501
 * workspace_synthesis_disabled, nothing read), auth and workspace attestation, the coordination canary, the window
 * parameters, the newest complete artifact, and erased-source invalidation through the pointer tombstones.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { memoryLiveness } from "./support.ts";
import {
  SYNTHESIS_ALIAS_ROWS_MAX, storeWorkspaceSynthesis, synthesisKeyRoot, synthesisWindow, type SessionAliasRow, type SessionAliases,
  type WorkspaceSynthesisSource,
} from "../workspace-synthesis.ts";
import { memoryArtifactStore, runWorkspaceSynthesisJob } from "../../session-analysis/workspace-synthesis.ts";
import { workspaceSynthesisDeps } from "../../../packages/session-read-worker/src/index.ts";
import { withChild, withRelated } from "../../session-analysis/__tests__/workspace-synthesis-fixtures.ts";
import { keyOf, type SessionKey } from "../page.ts";

const FIXTURE = new URL("../../session-intel/__tests__/fixtures/synthesis/", import.meta.url);
const cohortText = readFileSync(new URL("cohort.json", FIXTURE), "utf8");
const golden = JSON.parse(readFileSync(new URL("expected-synthesis.json", FIXTURE), "utf8"));
const ID: VerifiedIdentity = { userId: "u", orgId: "fx-org", workspaceId: "fx-ws-synthesis", scopes: ["rth:read"] };
const auth = { authorization: "Bearer good" };
const verify = async (token: string) => {
  if (token !== "good") throw new AuthError(401, "invalid_token", "invalid");
  return ID;
};
const SINCE = "2026-11-01T00:00:00.000Z", UNTIL = "2026-11-08T00:00:00.000Z";
const PATH = `/v1/session-intel/synthesis?workspace=fx-ws-synthesis&since=${SINCE}&until=${UNTIL}`;

async function storedSource(opts: { cohort?: unknown } = {}) {
  const store = memoryArtifactStore();
  await runWorkspaceSynthesisJob({ store, keyRoot: "fxroot", orgId: "fx-org", cohort: opts.cohort ?? JSON.parse(cohortText), options: { now: "2026-11-08T00:00:00.000Z" } });
  const calls: unknown[] = [];
  const inner = storeWorkspaceSynthesis(store, "fxroot");
  const source: WorkspaceSynthesisSource = { async latest(...args) { calls.push(args); return inner.latest(...args); } };
  return { source, calls, store };
}

const api = (opts: { enabled?: boolean; source?: WorkspaceSynthesisSource | null; erased?: SessionKey[]; canary?: ReadonlySet<string> | "*" } = {}) => {
  const live = memoryLiveness({ erased: opts.erased ?? [] });
  const app = createReadApi({
    verify, adapter: {} as never, liveness: live.liveness, completeSinceMs: 0,
    directoryWorkspaces: opts.canary ?? new Set([ID.workspaceId]),
    ...(opts.enabled === undefined ? {} : { workspaceSynthesis: { enabled: opts.enabled, ...(opts.source ? { source: opts.source } : {}) } }),
  });
  return { app, live };
};
const get = async (app: ReturnType<typeof api>["app"], path = PATH, headers: Record<string, string> = auth) => {
  const res = await app.request(path, { headers });
  return { status: res.status, body: await res.json() as any };
};

describe("GET /v1/session-intel/synthesis: gate, auth and scope", () => {
  test("flag off (absent or not enabled) is the standard 501 envelope, and nothing is read", async () => {
    for (const enabled of [undefined, false]) {
      const s = await storedSource();
      const { app, live } = api({ enabled, source: s.source });
      const { status, body } = await get(app);
      expect(status).toBe(501);
      expect(body.error).toEqual({ code: "unsupported_by_backend", message: "not served by this backend: workspace_synthesis_disabled", reason: "workspace_synthesis_disabled" });
      expect(typeof body.correlationId).toBe("string");
      expect(s.calls).toEqual([]);
      expect(live.calls.erased).toEqual([]);
    }
  });

  test("enabled without a source is 501 workspace_synthesis_not_configured", async () => {
    const { app } = api({ enabled: true, source: null });
    expect((await get(app)).body.error.reason).toBe("workspace_synthesis_not_configured");
  });

  test("bearer, workspace attestation, the canary and the parameters come before any read", async () => {
    const s = await storedSource();
    const { app } = api({ enabled: true, source: s.source });
    expect((await app.request(PATH)).status).toBe(401);
    expect((await get(app, PATH.replace("workspace=fx-ws-synthesis", "workspace=fx-ws-other"))).status).toBe(403);
    expect((await get(app, `/v1/session-intel/synthesis?since=${SINCE}&until=${UNTIL}`)).body.error.reason).toBe("org_scope");
    expect((await get(app, `${PATH}&limit=1`)).body.error.reason).toBe("param_limit");
    expect((await get(app, `${PATH}&since=${SINCE}`)).status).toBe(400);
    expect((await get(app, `/v1/session-intel/synthesis?workspace=fx-ws-synthesis&since=${SINCE}`)).status).toBe(400);
    expect((await get(app, `/v1/session-intel/synthesis?workspace=fx-ws-synthesis&since=2026-10-01T00:00:00.000Z&until=${UNTIL}`)).body.error.message).toBe("the window is longer than 30 days");
    expect((await get(app, `/v1/session-intel/synthesis?workspace=fx-ws-synthesis&since=yesterday&until=${UNTIL}`)).status).toBe(400);
    const { app: noCanary } = api({ enabled: true, source: s.source, canary: new Set(["fx-ws-other"]) });
    expect((await get(noCanary)).status).toBe(501);
    expect(s.calls).toEqual([]);
  });
});

describe("GET /v1/session-intel/synthesis: serving and invalidation", () => {
  test("serves the newest complete artifact's synthesis, exactly the golden, after the erase check", async () => {
    const s = await storedSource();
    const { app, live } = api({ enabled: true, source: s.source });
    const { status, body } = await get(app);
    expect(status).toBe(200);
    expect(body).toEqual(golden);
    expect(body.orgId).toBeUndefined();
    expect(live.calls.erased.length).toBe(1);
    expect(live.calls.erased[0]!.length).toBe(golden.inputs.considered);
    // The same instant in another zone names the same window.
    expect((await get(app, `/v1/session-intel/synthesis?workspace=fx-ws-synthesis&since=2026-11-01T01:00:00%2B01:00&until=${UNTIL}`)).status).toBe(200);
  });

  test("no artifact for the window is a 404", async () => {
    const s = await storedSource();
    const { app } = api({ enabled: true, source: s.source });
    const r = await get(app, `/v1/session-intel/synthesis?workspace=fx-ws-synthesis&since=${SINCE}&until=2026-11-07T00:00:00.000Z`);
    expect([r.status, r.body.error.code]).toEqual([404, "not_found"]);
  });

  test("an artifact citing an erased session is never served: a drilldown session, an issue session or a skipped one", async () => {
    for (const erased of [
      { source: "fx-claude", sessionId: "fx-s01" }, // in an issue and a category
      { source: "fx-codex", sessionId: "fx-s40" }, // a category only
      { source: "fx-claude", sessionId: "fx-skip-not-analyzed" }, // counted as skipped only
    ]) {
      const s = await storedSource();
      const { app } = api({ enabled: true, source: s.source, erased: [erased] });
      const r = await get(app);
      expect([erased.sessionId, r.status, r.body.error.reason]).toEqual([erased.sessionId, 404, "synthesis_invalidated"]);
      expect(JSON.stringify(r.body)).not.toContain(erased.sessionId);
    }
    // A child session only the evidence belongs to (not in the cohort): erasing just the child invalidates.
    for (const child of ["fx-s01-child", "fx-s01-spawned"]) {
      const s = await storedSource({ cohort: withChild(JSON.parse(cohortText)) });
      const ok = await get(api({ enabled: true, source: s.source }).app);
      expect(ok.status).toBe(200);
      expect(ok.body).toEqual(golden); // the served document is the unchanged workspace-synthesis/1 output
      const r = await get(api({ enabled: true, source: s.source, erased: [{ source: "fx-claude", sessionId: child }] }).app);
      expect([child, r.status, r.body.error.reason]).toEqual([child, 404, "synthesis_invalidated"]);
    }
    // An erased session the artifact does not consider leaves it servable.
    const s = await storedSource();
    expect((await get(api({ enabled: true, source: s.source, erased: [{ source: "fx-claude", sessionId: "fx-unrelated" }] }).app)).status).toBe(200);
  });

  test("more than one erase batch: only the LAST recorded session erased is still never served", async () => {
    const s = await storedSource({ cohort: withRelated(JSON.parse(cohortText), 600) });
    const ok = api({ enabled: true, source: s.source });
    expect((await get(ok.app)).status).toBe(200);
    const total = ok.live.calls.erased.flat().length;
    expect(total).toBe(golden.inputs.considered + 600);
    expect(ok.live.calls.erased.length).toBe(Math.ceil(total / 500));
    const last = { source: "fx-claude", sessionId: "fx-zz-rel-00599" };
    expect(ok.live.calls.erased.at(-1)!.at(-1)).toEqual(last);
    const r = await get(api({ enabled: true, source: s.source, erased: [last] }).app);
    expect([r.status, r.body.error.reason]).toEqual([404, "synthesis_invalidated"]);
  });

  test("a failing erase check is a 503, never a served artifact", async () => {
    const s = await storedSource();
    const app = createReadApi({
      verify, adapter: {} as never, completeSinceMs: 0, directoryWorkspaces: new Set([ID.workspaceId]),
      liveness: { ...memoryLiveness().liveness, async erased() { throw new Error("tombstones unavailable"); } },
      workspaceSynthesis: { enabled: true, source: s.source },
    });
    const r = await get(app);
    expect([r.status, r.body.error.code]).toEqual([503, "backend_unavailable"]);
    expect(JSON.stringify(r.body)).not.toContain("fx-s01");
  });

  test("an artifact of another workspace handed back by the source is a 500 refusal", async () => {
    const s = await storedSource();
    const artifact = await s.source.latest("fx-org", "fx-ws-synthesis", { since: SINCE, until: UNTIL });
    const source: WorkspaceSynthesisSource = { async latest() { return { ...artifact!, workspace: "fx_ws-synthesis" }; } };
    const r = await get(api({ enabled: true, source }).app);
    expect([r.status, r.body.error.code]).toEqual([500, "internal"]);
  });

  test("an artifact of another org is a 500 refusal, never served", async () => {
    const s = await storedSource();
    // The token's org reads its own prefix; plant this org's artifact bytes there under another org id.
    const [key, bytes] = [...s.store.objects][0]!;
    const artifact = JSON.parse(new TextDecoder().decode(bytes));
    const forged = new TextEncoder().encode(`${JSON.stringify({ ...artifact, orgId: "fx-org-other" })}\n`);
    const digest = new Bun.CryptoHasher("sha256").update(forged).digest("hex");
    s.store.objects.set(key.replace(/-[0-9a-f]{64}\.json$/u, `-${digest}.json`), forged);
    s.store.objects.delete(key);
    const r = await get(api({ enabled: true, source: s.source }).app);
    expect([r.status, r.body.error.code]).toEqual([500, "internal"]);
  });

  test("a failing store is a 503", async () => {
    const source: WorkspaceSynthesisSource = { async latest() { throw new Error("store down"); } };
    expect((await get(api({ enabled: true, source }).app)).status).toBe(503);
  });

  test("the window parser normalizes to UTC ms and bounds the span", () => {
    expect(synthesisWindow("2026-11-01T01:00:00+01:00", UNTIL)).toEqual({ since: SINCE, until: UNTIL });
    expect(typeof synthesisWindow(UNTIL, SINCE)).toBe("string");
    expect(typeof synthesisWindow(undefined, UNTIL)).toBe("string");
  });
});

/** The pointer rows of the alias SQL, in memory: rows whose physical or source id is a key under the key's source. */
function memoryAliases(rows: Array<{ harness: string; session_id: string; canonical_session_id: string | null }>) {
  const calls: SessionKey[][] = [];
  const aliases: SessionAliases = async (orgId, workspaceId, keys) => {
    calls.push(keys);
    const asked = new Set(keys.map(keyOf));
    return rows
      .filter((r) => asked.has(keyOf({ source: r.harness, sessionId: r.session_id })) ||
        (r.canonical_session_id !== null && asked.has(keyOf({ source: r.harness, sessionId: r.canonical_session_id }))))
      .map((r): SessionAliasRow => ({ org_id: orgId, workspace_id: workspaceId, ...r }));
  };
  return { aliases, calls };
}

/**
 * The operator erase fence (self-host SESSION_ERASE_FENCE_ENFORCED=1), bound as the Lambda binds it (operatorErased
 * and operatorErasedBatch) or per key only. Ordinary liveness has no tombstone in any of these cases: an operator erase
 * is in progress.
 */
const fenced = (opts: {
  source: WorkspaceSynthesisSource;
  fence?: SessionKey[];
  mode?: "batch" | "perKey" | "outage" | "perKeyOutage";
  aliasRows?: Array<{ harness: string; session_id: string; canonical_session_id: string | null }>;
  aliases?: SessionAliases | null;
}) => {
  const live = memoryLiveness();
  const fence = new Set((opts.fence ?? []).map(keyOf));
  const calls = { batch: [] as SessionKey[][], perKey: [] as SessionKey[] };
  const mode = opts.mode ?? "batch";
  const alias = memoryAliases(opts.aliasRows ?? []);
  const aliases = opts.aliases === null ? undefined : opts.aliases ?? alias.aliases;
  const app = createReadApi({
    verify, adapter: {} as never, liveness: live.liveness, completeSinceMs: 0, directoryWorkspaces: new Set([ID.workspaceId]),
    operatorErased: async (key) => {
      calls.perKey.push({ source: key.source, sessionId: key.sessionId });
      if (key.orgId !== ID.orgId || key.workspaceId !== ID.workspaceId) throw new Error("tenant");
      if (mode === "perKeyOutage") throw new Error("operator_fence_unavailable fx-s01");
      return fence.has(keyOf(key));
    },
    ...(mode === "batch" || mode === "outage" ? {
      operatorErasedBatch: async (orgId: string, workspaceId: string, keys: SessionKey[]) => {
        calls.batch.push(keys);
        if (orgId !== ID.orgId || workspaceId !== ID.workspaceId) throw new Error("tenant");
        if (mode === "outage") throw new Error("operator_fence_unavailable fx-s01");
        return new Set(keys.map(keyOf).filter((k) => fence.has(k)));
      },
    } : {}),
    workspaceSynthesis: { enabled: true, source: opts.source, ...(aliases ? { aliases } : {}) },
  });
  return { app, live, calls, alias };
};

describe("GET /v1/session-intel/synthesis: the operator erase fence", () => {
  const invalidated = (r: { status: number; body: any }, id: string) => {
    expect([id, r.status, r.body.error?.reason]).toEqual([id, 404, "synthesis_invalidated"]);
    expect(JSON.stringify(r.body)).not.toContain(id);
  };

  test("nothing fenced still serves exactly the golden, every recorded session checked against the fence", async () => {
    const s = await storedSource();
    const f = fenced({ source: s.source });
    const r = await get(f.app);
    expect(r.status).toBe(200);
    expect(r.body).toEqual(golden);
    expect(f.calls.batch.length).toBe(1);
    expect(f.calls.batch[0]!.length).toBe(golden.inputs.considered);
    expect(f.calls.perKey).toEqual([]);
    expect(f.alias.calls.length).toBe(1);
  });

  test("a fenced considered session (no tombstone yet) is never served: the lander's fx-s01 case and a skipped one", async () => {
    for (const key of [{ source: "fx-claude", sessionId: "fx-s01" }, { source: "fx-claude", sessionId: "fx-skip-not-analyzed" }]) {
      const s = await storedSource();
      const f = fenced({ source: s.source, fence: [key] });
      invalidated(await get(f.app), key.sessionId);
      expect(f.live.calls.erased.flat()).toContainEqual(key);
    }
    // The same id fenced under another source is another session: still served.
    const s = await storedSource();
    expect((await get(fenced({ source: s.source, fence: [{ source: "fx-codex", sessionId: "fx-s01" }] }).app)).status).toBe(200);
  });

  test("a fenced RELATED-only session (a child the evidence belongs to, not in the cohort) is never served", async () => {
    for (const child of ["fx-s01-child", "fx-s01-spawned"]) {
      const s = await storedSource({ cohort: withChild(JSON.parse(cohortText)) });
      expect((await get(fenced({ source: s.source }).app)).body).toEqual(golden);
      invalidated(await get(fenced({ source: s.source, fence: [{ source: "fx-claude", sessionId: child }] }).app), child);
    }
  });

  test("a fence under a canonical or physical alias of a recorded session is never served, both directions", async () => {
    // fx-s01 recorded as its physical id; the fence written under its source (canonical) id.
    const s = await storedSource();
    const toCanonical = [{ harness: "fx-claude", session_id: "fx-s01", canonical_session_id: "fx-canon-s01" }];
    invalidated(await get(fenced({ source: s.source, aliasRows: toCanonical, fence: [{ source: "fx-claude", sessionId: "fx-canon-s01" }] }).app), "fx-s01");
    // fx-s01 recorded as a source id whose pointer's physical id differs; the fence written under the physical id.
    const toPhysical = [{ harness: "fx-claude", session_id: "fx-phys-s01", canonical_session_id: "fx-s01" }];
    const f = fenced({ source: s.source, aliasRows: toPhysical, fence: [{ source: "fx-claude", sessionId: "fx-phys-s01" }] });
    invalidated(await get(f.app), "fx-s01");
    expect(f.calls.batch.flat()).toContainEqual({ source: "fx-claude", sessionId: "fx-phys-s01" });
    // The per-key fallback (operatorErased alone) sees the alias as well.
    invalidated(await get(fenced({ source: s.source, mode: "perKey", aliasRows: toCanonical, fence: [{ source: "fx-claude", sessionId: "fx-canon-s01" }] }).app), "fx-s01");
    // An alias is per source: the fence on the same id under another source does not invalidate.
    expect((await get(fenced({ source: s.source, aliasRows: toCanonical, fence: [{ source: "fx-codex", sessionId: "fx-canon-s01" }] }).app)).status).toBe(200);
    // The alias forms reach ordinary liveness too: a tombstoned physical alias invalidates.
    const live = memoryLiveness({ erased: [{ source: "fx-claude", sessionId: "fx-phys-s01" }] });
    const app = createReadApi({
      verify, adapter: {} as never, liveness: live.liveness, completeSinceMs: 0, directoryWorkspaces: new Set([ID.workspaceId]),
      workspaceSynthesis: { enabled: true, source: s.source, aliases: memoryAliases(toPhysical).aliases },
    });
    invalidated(await get(app), "fx-s01");
  });

  test("the per-key fence alone (no batch bound) covers the set too", async () => {
    const s = await storedSource();
    const ok = fenced({ source: s.source, mode: "perKey" });
    expect((await get(ok.app)).body).toEqual(golden);
    expect(ok.calls.perKey.length).toBe(golden.inputs.considered);
    invalidated(await get(fenced({ source: s.source, mode: "perKey", fence: [{ source: "fx-codex", sessionId: "fx-s40" }] }).app), "fx-s40");
  });

  test("more than one fence batch: only the LAST recorded session fenced is still never served", async () => {
    const s = await storedSource({ cohort: withRelated(JSON.parse(cohortText), 600) });
    const ok = fenced({ source: s.source });
    expect((await get(ok.app)).status).toBe(200);
    const total = golden.inputs.considered + 600;
    expect(ok.calls.batch.flat().length).toBe(total);
    expect(ok.calls.batch.length).toBe(Math.ceil(total / 500));
    expect(ok.alias.calls.length).toBe(Math.ceil(total / 500));
    expect(ok.calls.batch.every((b) => b.length <= 500)).toBe(true);
    const last = { source: "fx-claude", sessionId: "fx-zz-rel-00599" };
    invalidated(await get(fenced({ source: s.source, fence: [last] }).app), last.sessionId);
  });

  test("fails closed: the fence unavailable or throwing, or the alias reader missing, failing or out of contract, is a 503", async () => {
    const s = await storedSource();
    const closed = async (app: ReturnType<typeof fenced>["app"]) => {
      const r = await get(app);
      expect([r.status, r.body.error.code]).toEqual([503, "backend_unavailable"]);
      expect(JSON.stringify(r.body)).not.toContain("fx-s01");
    };
    await closed(fenced({ source: s.source, mode: "outage" }).app);
    await closed(fenced({ source: s.source, mode: "perKeyOutage" }).app);
    // The fence bound without an alias reader would see one form only: refused.
    const noAliases = fenced({ source: s.source, aliases: null });
    await closed(noAliases.app);
    expect(noAliases.calls.batch).toEqual([]);
    await closed(fenced({ source: s.source, aliases: async () => { throw new Error("pointers down fx-s01"); } }).app);
    const tooMany = Array.from({ length: SYNTHESIS_ALIAS_ROWS_MAX + 1 }, (_, i) => ({ org_id: ID.orgId, workspace_id: ID.workspaceId, harness: "fx-claude", session_id: "fx-s01", canonical_session_id: `fx-c-${i}` }));
    await closed(fenced({ source: s.source, aliases: async () => tooMany }).app);
    // A row for a session not asked about, or of another tenant, is out of contract.
    await closed(fenced({ source: s.source, aliases: async () => [{ org_id: ID.orgId, workspace_id: ID.workspaceId, harness: "fx-claude", session_id: "fx-other", canonical_session_id: null }] }).app);
    const other = fenced({ source: s.source, aliases: async () => [{ org_id: "fx-org-other", workspace_id: ID.workspaceId, harness: "fx-claude", session_id: "fx-s01", canonical_session_id: null }] });
    expect((await get(other.app)).status).toBe(500);
  });
});

describe("Worker wiring", () => {
  test("without raw-store secrets the route is enabled without a source (501 not_configured)", () => {
    expect(workspaceSynthesisDeps({} as never)).toEqual({ enabled: true });
    expect(synthesisKeyRoot(undefined)).toBeUndefined();
    expect(synthesisKeyRoot("/fxroot/")).toBe("fxroot");
    expect(synthesisKeyRoot("fx/root")).toBeUndefined();
  });
});
