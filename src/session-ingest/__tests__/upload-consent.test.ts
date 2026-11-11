/**
 * Server-owned upload consent (upload-consent.ts). The brief's required cases, through the real request
 * app, the real deliver path and the async consumer, against the in-memory store (the SQL functions are
 * proven in upload-consent.pglite.test.ts):
 *
 *   - an old client sending everything under a server "selected" policy lands zero unselected records;
 *   - unknown mode, missing allowlist, unverifiable age, an old retry, a forged version header, a fresh
 *     origin and another tenant's allowlist cannot bypass it;
 *   - a mixed allowed/denied batch persists nothing and receives no receipt (503, Retry-After), while a
 *     session the deletion fence drops keeps the legacy acknowledge-not-store answer;
 *   - a narrowing that lands mid-flight (between the pre-check and the commit, or between enqueue and
 *     admission) still refuses;
 *   - trusted transitions, explicit denies kept across "all", restart, origin isolation, owner-only writes;
 *   - a refused synthetic upload produces an operator signal with no id or content.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { zstdDecompressSync } from "node:zlib";
import { objectKey } from "../../shared/vendor.ts";
import { ALLOWED_LOG_FIELDS } from "../../shared/log.ts";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, parseConsentWrite, recordSelectionDeny, SYNC_SCOPE, UPLOAD_CONSENT_PATH } from "../app.ts";
import { admitMessage } from "../admission-consumer.ts";
import { ADMISSION_ACCEPT_VALUE, enqueueAdmission, type AdmissionMessage } from "../async-admission.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { deliveryAccount } from "../vendor/delivery/prepare.ts";
import {
  CONSENT_SCOPE, ConsentWriteRefused, MemoryConsentStore, UPLOAD_CONSENT_METRIC, UPLOAD_CONSENT_REQUIRED, UPLOAD_CONSENT_RETRY_AFTER_SECONDS,
  batchSessions, consentRefuses, decideUploadConsent, sessionKey, uploadConsentEnforcement, writeBroadens,
  type ConsentSnapshot, type UploadConsentEnforcement, type UploadConsentStore,
} from "../upload-consent.ts";
import { MemoryLedger, SIGNING_KEY, TENANT, batch, record, scratchStore, signedHeaders } from "./support.ts";
import { RECOVERY_CONTENT_TYPE, RECOVERY_PROTOCOL } from "../recovery-protocol.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const T0 = 1_800_000_000_000;
const OTHER = { ...TENANT, userId: "user-2" };
const zstd = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b));

function store() {
  const s = scratchStore();
  cleanups.push(s.done);
  return s.store;
}

function harness(options: { enforcement?: UploadConsentEnforcement; consent?: UploadConsentStore; ledger?: MemoryLedger; now?: () => number } = {}) {
  const ledger = options.ledger ?? new MemoryLedger();
  const consent = options.consent ?? new MemoryConsentStore();
  const lines: Record<string, unknown>[] = [];
  const objects = store();
  const app = createIngestApp({
    identify: (r) => bindingIdentity(r),
    log: (f) => void lines.push(f),
    deliver: { ledger, target: { store: objects, keyRoot: "96bulls", sleep: async () => {} } } as never,
    uploadConsent: { enforcement: options.enforcement ?? "enforce", store: consent, nowMs: options.now ?? (() => T0) },
  });
  const as = (identity: Record<string, unknown>) => ({ [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE], ...identity } as never) });
  const post = async (b: unknown, headers: Record<string, string> = as({})) =>
    app.request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json", ...headers }, body: JSON.stringify({ protocolVersion: 1, batch: b }) });
  const write = async (body: unknown, identity: Record<string, unknown> = {}) =>
    app.request(UPLOAD_CONSENT_PATH, { method: "POST", headers: { "content-type": "application/json", ...as(identity) }, body: JSON.stringify(body) });
  const confirm = (body: unknown, identity: Record<string, unknown> = {}) => write(body, { scopes: [CONSENT_SCOPE], ...identity });
  return { app, ledger, consent, lines, objects, post, write, confirm, as };
}

const sess = (id: string, over: Record<string, unknown> = {}) => record({ record_id: `${id}-r`, session_id: id, ...over });
const sessionRecord = (id: string, firstActivityMs: number) => record({ record_id: `${id}-s`, session_id: id, kind: "session", payload: { first_activity_ms: firstActivityMs } });
const errorCode = async (res: Response) => ((await res.json()) as { error: { code: string } }).error.code;

describe("INGEST_UPLOAD_CONSENT", () => {
  test("off by default; exact values only; anything else refuses startup", () => {
    expect(uploadConsentEnforcement(undefined)).toBe("off");
    expect(uploadConsentEnforcement(" Enforce ")).toBe("enforce");
    expect(uploadConsentEnforcement("hold-only")).toBe("hold-only");
    expect(() => uploadConsentEnforcement("on")).toThrow();
    expect(() => uploadConsentEnforcement("true")).toThrow();
  });

  test("off reads nothing and changes nothing", async () => {
    const consent = new MemoryConsentStore();
    let reads = 0;
    const spy: UploadConsentStore = { snapshot: async (...a) => { reads++; return consent.snapshot(...a); }, apply: consent.apply.bind(consent) };
    const h = harness({ enforcement: "off", consent: spy });
    expect((await h.post(await batch([sess("a")]))).status).toBe(200);
    expect(reads).toBe(0);
  });
});

describe("decision", () => {
  const snap = (over: Partial<ConsentSnapshot> = {}): ConsentSnapshot => ({ held: false, policy: { mode: "selected", revision: 1, newSessionBoundaryMs: null }, originConfirmed: true, sessions: new Map(), ...over });
  test("precedence: hold > no policy > unconfirmed origin > per-session", () => {
    const records = [sess("a")];
    expect(decideUploadConsent(snap({ held: true, policy: null }), records).outcome).toBe("held");
    expect(decideUploadConsent(snap({ policy: null }), records).outcome).toBe("no_policy");
    expect(decideUploadConsent(snap({ originConfirmed: false }), records).outcome).toBe("origin_unconfirmed");
    expect(decideUploadConsent(snap(), records).outcome).toBe("not_selected");
  });
  test("an explicit deny refuses in every mode, including all", () => {
    const sessions = new Map([[sessionKey("claude", "a"), { decision: "deny" as const }]]);
    for (const mode of ["selected", "new", "all"] as const)
      expect(decideUploadConsent(snap({ policy: { mode, revision: 1, newSessionBoundaryMs: mode === "new" ? 0 : null }, sessions }), [sess("a")])).toMatchObject({ outcome: "denied", refuse: true });
  });
  test("a session the deletion fence drops never refuses; a deferred deny waits for the post-claim check", () => {
    const sessions = new Map([[sessionKey("claude", "a"), { decision: "deny" as const }]]);
    const s = snap({ policy: { mode: "all", revision: 1, newSessionBoundaryMs: null }, sessions });
    expect(decideUploadConsent(s, [sess("a"), sess("b")], { dropped: new Set([sessionKey("claude", "a")]) }).refuse).toBe(false);
    expect(decideUploadConsent(s, [sess("a")], { deferDenied: true }).refuse).toBe(false);
  });
  test("new: both server first-seen and claimed first activity must be at or after the boundary", () => {
    const policy = { mode: "new" as const, revision: 1, newSessionBoundaryMs: 1000 };
    const at = (firstSeenMs: number, firstActivityMs?: number) => snap({ policy, sessions: new Map([[sessionKey("claude", "a"), { firstSeenMs, ...(firstActivityMs !== undefined ? { firstActivityMs } : {}) }]]) });
    expect(decideUploadConsent(at(2000, 1500), [sess("a")]).outcome).toBe("allowed");
    expect(decideUploadConsent(at(2000), [sess("a")]).outcome).toBe("age_unknown");
    expect(decideUploadConsent(at(2000, 500), [sess("a")]).outcome).toBe("age_unknown");
    expect(decideUploadConsent(at(500, 1500), [sess("a")]).outcome).toBe("age_unknown");
    // A claim in this batch that is earlier than the stored one wins (the earliest claim counts).
    expect(decideUploadConsent(at(2000, 1500), [sessionRecord("a", 10)]).outcome).toBe("age_unknown");
  });
  test("hold-only refuses holds only; report never refuses", () => {
    const held = decideUploadConsent(snap({ held: true }), [sess("a")]);
    const unselected = decideUploadConsent(snap(), [sess("a")]);
    expect([consentRefuses("hold-only", held), consentRefuses("hold-only", unselected)]).toEqual([true, false]);
    expect(consentRefuses("report", held)).toBe(false);
    expect(consentRefuses("enforce", unselected)).toBe(true);
  });
  test("batchSessions keys sessions only and keeps the earliest claimed activity", () => {
    const named = batchSessions([sessionRecord("a", 50), sessionRecord("a", 20), record({ record_id: "x", session_id: null })]);
    expect([...named.values()]).toEqual([{ source: "claude", sessionId: "a", firstActivityMs: 20 }]);
  });
  test("broadening: allow, origin confirmation, hold release and any widening mode", () => {
    const cur = { mode: "new" as const, revision: 1, newSessionBoundaryMs: 1 };
    expect(writeBroadens(cur, { mode: "all" })).toBe(true);
    expect(writeBroadens(cur, { mode: "selected" })).toBe(false);
    expect(writeBroadens(null, { mode: "selected" })).toBe(false);
    expect(writeBroadens(null, { mode: "new" })).toBe(true);
    expect(writeBroadens(cur, { deny: [{ source: "c", sessionId: "s" }] })).toBe(false);
    expect(writeBroadens(cur, { allow: [{ source: "c", sessionId: "s" }] })).toBe(true);
    expect(writeBroadens(cur, { confirmOrigins: ["o"] })).toBe(true);
    expect(writeBroadens(cur, { releaseHold: true })).toBe(true);
  });
});

describe("request path", () => {
  test("an old client sending everything under server 'selected' lands zero unselected records", async () => {
    const h = harness();
    expect((await h.confirm({ mode: "selected", allow: [{ source: "claude", session_id: "picked" }], confirm_origins: ["origin-1"] })).status).toBe(200);
    // The legacy client knows nothing of the policy: it sends every session, as `all_sources: true` would.
    const res = await h.post(await batch([sess("picked"), sess("other-1"), sess("other-2")]));
    expect(res.status).toBe(503);
    expect(res.headers.get("retry-after")).toBe(String(UPLOAD_CONSENT_RETRY_AFTER_SECONDS));
    expect(await errorCode(res)).toBe(UPLOAD_CONSENT_REQUIRED);
    expect(h.ledger.claims).toHaveLength(0);
    expect(h.ledger.commits).toHaveLength(0);
    expect((await h.objects.list(objectKey("96bulls"))).items).toHaveLength(0);
    // The selected session alone is accepted.
    const ok = await h.post(await batch([sess("picked")], { batch_id: "batch-2" }));
    expect(ok.status).toBe(200);
    expect(h.ledger.commits).toHaveLength(1);
    expect(h.ledger.commits[0]!.sessions).toEqual([["claude", "picked"]]);
  });

  test("no policy holds (never falls back to all), and so does a policy with no allowlist", async () => {
    const h = harness();
    expect((await h.post(await batch([sess("a")]))).status).toBe(503);
    await h.write({ mode: "selected" }); // narrowing: a sync token may create the strictest policy
    await h.confirm({ confirm_origins: ["origin-1"] });
    expect((await h.post(await batch([sess("a")]))).status).toBe(503);
    expect(h.ledger.claims).toHaveLength(0);
  });

  test("new: unverifiable or earlier age holds; a session started after the choice is accepted", async () => {
    let now = T0;
    const h = harness({ now: () => now });
    await h.confirm({ mode: "new", confirm_origins: ["origin-1"] });
    now = T0 + 10_000;
    // No session record (no claimed start): ambiguous.
    expect((await h.post(await batch([sess("fresh")]))).status).toBe(503);
    // Claims to have started before the choice.
    expect((await h.post(await batch([sessionRecord("old", T0 - 1)], { batch_id: "b2" }))).status).toBe(503);
    // Started after the choice and first seen after it.
    expect((await h.post(await batch([sessionRecord("new-1", T0 + 5000), sess("new-1")], { batch_id: "b3" }))).status).toBe(200);
  });

  test("an old retry, a forged version header and a loose consent header cannot bypass the hold", async () => {
    const h = harness();
    const b = await batch([sess("a")]);
    for (const extra of [{}, { "user-agent": "AgentRelayDesktop/9999.0.0" }, { "x-agent-relay-capture-policy-revision": "999" }, { "x-relay-upload-consent": "all" }]) {
      const res = await h.post(b, { ...h.as({}), ...extra });
      expect(res.status).toBe(503);
    }
    expect(h.ledger.claims).toHaveLength(0);
  });

  test("a fresh or rotated origin under a confirmed principal is held until confirmed", async () => {
    const h = harness();
    await h.confirm({ mode: "all", confirm_origins: ["origin-1"] });
    expect((await h.post(await batch([sess("a")]))).status).toBe(200);
    const rotated = await batch([{ ...sess("a"), origin_id: "origin-new", record_id: "a-r2" }], { origin_id: "origin-new", batch_id: "b2" });
    const res = await h.post(rotated);
    expect(res.status).toBe(503);
    expect(h.lines.find((l) => l.route === UPLOAD_CONSENT_METRIC)?.reason).toBe("origin_unconfirmed");
  });

  test("another user's allowlist and policy never apply (cross-tenant)", async () => {
    const h = harness();
    await h.confirm({ mode: "all", confirm_origins: ["origin-1"] }, { userId: OTHER.userId });
    expect((await h.post(await batch([sess("a")]))).status).toBe(503);
    // Nothing in the body names the principal: the policy key comes from the signed identity only.
    const theirs = await batch([sess("a")], { account_id: await deliveryAccount(OTHER), batch_id: "b2" });
    expect((await h.post(theirs)).status).not.toBe(200);
    expect(h.ledger.claims).toHaveLength(0);
  });

  test("a mixed allowed/denied batch persists nothing and gets no receipt", async () => {
    const h = harness();
    await h.confirm({ mode: "all", confirm_origins: ["origin-1"] });
    await h.write({ deny: [{ source: "claude", session_id: "secret" }] });
    const res = await h.post(await batch([sess("ok"), sess("secret", { payload: { text: "DENIED-CONTENT" } })]));
    expect(res.status).toBe(503);
    expect(h.ledger.commits).toHaveLength(0);
    for (const item of (await h.objects.list(objectKey("96bulls"))).items) expect(String(item.key)).not.toContain("secret");
  });

  test("'Don't upload' through the deletion fence keeps the legacy answer: acknowledged, not stored", async () => {
    const h = harness();
    await h.confirm({ mode: "all", confirm_origins: ["origin-1"] });
    const account = await deliveryAccount(TENANT);
    const sel = await h.app.request("/v1/delivery/sessions/selection", { method: "POST", headers: { "content-type": "application/json", ...h.as({}) },
      body: JSON.stringify({ account_id: account, origin_id: "origin-1", sessions: [{ source: "claude", session_id: "secret" }], enabled: false }) });
    expect(sel.status).toBe(200);
    // The disable also became the principal's explicit deny (every origin).
    expect(h.consent instanceof MemoryConsentStore && h.consent.decisions.get(JSON.stringify([TENANT.orgId, TENANT.workspaceId, TENANT.userId]))?.get(sessionKey("claude", "secret"))).toBe("deny");
    const res = await h.post(await batch([sess("ok"), sess("secret")]));
    expect(res.status).toBe(200);
    expect(h.ledger.commits[0]!.sessions).toEqual([["claude", "ok"]]);
    // A re-enable never broadens consent on its own.
    await h.app.request("/v1/delivery/sessions/selection", { method: "POST", headers: { "content-type": "application/json", ...h.as({}) },
      body: JSON.stringify({ account_id: account, origin_id: "origin-1", sessions: [{ source: "claude", session_id: "secret" }], enabled: true }) });
    expect((await h.post(await batch([sess("secret")], { batch_id: "b2" }))).status).toBe(503);
  });

  test("a narrowing that lands between the pre-check and the commit still refuses", async () => {
    const inner = new MemoryConsentStore();
    let snapshots = 0;
    const racing: UploadConsentStore = {
      snapshot: async (...a) => {
        snapshots++;
        // The person switches to "selected" while the batch is between its admit check and the commit.
        if (snapshots === 3) await inner.apply(TENANT, { mode: "selected" }, { trusted: false, nowMs: T0 });
        return inner.snapshot(...a);
      },
      apply: inner.apply.bind(inner),
    };
    const h = harness({ consent: racing });
    await h.confirm({ mode: "all", confirm_origins: ["origin-1"] });
    const res = await h.post(await batch([sess("a")]));
    expect(res.status).toBe(503);
    expect(snapshots).toBe(3);
    expect(h.ledger.commits).toHaveLength(0);
  });

  test("a store failure holds (503), never admits", async () => {
    const broken: UploadConsentStore = { snapshot: async () => { throw new Error("db down"); }, apply: async () => { throw new Error("db down"); } };
    const h = harness({ consent: broken });
    expect((await h.post(await batch([sess("a")]))).status).toBe(503);
    expect(h.ledger.claims).toHaveLength(0);
  });

  test("hold-only: refuses the containment cohort only; report accepts and signals", async () => {
    const consent = new MemoryConsentStore();
    consent.hold(TENANT);
    const held = harness({ enforcement: "hold-only", consent });
    expect((await held.post(await batch([sess("a")]))).status).toBe(503);
    const other = harness({ enforcement: "hold-only", consent });
    expect((await other.post(await batch([sess("a")]), other.as({ userId: OTHER.userId }))).status).toBe(200);
    const report = harness({ enforcement: "report", consent });
    expect((await report.post(await batch([sess("a")]))).status).toBe(200);
    expect(report.lines.find((l) => l.route === UPLOAD_CONSENT_METRIC)).toMatchObject({ reason: "held", state: "reported" });
  });

  test("an org-wide hold covers every user and every origin; a person cannot release it", async () => {
    const consent = new MemoryConsentStore();
    consent.hold(TENANT, true);
    const h = harness({ consent });
    await h.confirm({ mode: "all", confirm_origins: ["origin-1"], release_hold: true }, { userId: OTHER.userId });
    expect((await h.post(await batch([sess("a")]), h.as({ userId: OTHER.userId }))).status).toBe(503);
  });

  test("the operator signal: counts and classes only, all of it survives the log allowlist", async () => {
    const h = harness();
    await h.post(await batch([sess("SESSION-PLANTED", { payload: { text: "CONTENT-PLANTED" } })]));
    const line = h.lines.find((l) => l.route === UPLOAD_CONSENT_METRIC)!;
    expect(line).toMatchObject({ reason: "no_policy", state: "refused", error_code: UPLOAD_CONSENT_REQUIRED, count: 1, org_id: TENANT.orgId });
    const text = JSON.stringify(h.lines);
    for (const planted of ["SESSION-PLANTED", "CONTENT-PLANTED", "origin-1", TENANT.userId]) expect(text).not.toContain(planted);
    for (const key of Object.keys(line)) expect(ALLOWED_LOG_FIELDS.has(key)).toBe(true);
  });
});

describe("consent writes", () => {
  test("a sync token can narrow but never widen; the consent scope can widen", async () => {
    const h = harness();
    expect(await errorCode(await h.write({ mode: "all" }))).toBe("upload_consent_untrusted");
    expect(await errorCode(await h.write({ allow: [{ source: "claude", session_id: "a" }] }))).toBe("upload_consent_untrusted");
    expect(await errorCode(await h.write({ confirm_origins: ["origin-1"] }))).toBe("upload_consent_untrusted");
    expect(await errorCode(await h.write({ release_hold: true }))).toBe("upload_consent_untrusted");
    expect((await h.write({ mode: "selected" })).status).toBe(200);
    expect((await h.confirm({ mode: "all" })).status).toBe(200);
    expect((await h.write({ mode: "new" })).status).toBe(200); // all -> new narrows
    expect(await errorCode(await h.write({ mode: "all" }))).toBe("upload_consent_untrusted");
  });

  test("explicit denies survive a change to all; a restart keeps the choice", async () => {
    const consent = new MemoryConsentStore();
    const a = harness({ consent });
    await a.confirm({ mode: "selected", allow: [{ source: "claude", session_id: "keep" }], confirm_origins: ["origin-1"] });
    await a.write({ deny: [{ source: "claude", session_id: "never" }] });
    await a.confirm({ mode: "all" });
    const b = harness({ consent }); // a new process over the same store
    expect((await b.post(await batch([sess("never")]))).status).toBe(503);
    expect((await b.post(await batch([sess("keep"), sess("anything")], { batch_id: "b2" }))).status).toBe(200);
  });

  test("optimistic revision and invalid bodies", async () => {
    const h = harness();
    expect(await (await h.confirm({ mode: "all", expected_revision: 0 })).json()).toEqual({ revision: 1 });
    expect(await errorCode(await h.confirm({ mode: "new", expected_revision: 0 }))).toBe("upload_consent_revision");
    expect((await h.confirm({ allow: [{ source: "c", session_id: "s" }], deny: [{ source: "c", session_id: "s" }] })).status).toBe(400);
    expect((await h.confirm({ mode: "everything" })).status).toBe(400);
    expect((await h.confirm({ org_id: "org-b" })).status).toBe(400);
    expect(() => parseConsentWrite({ user_id: "someone-else" })).toThrow();
  });

  test("the route needs a sync or consent scope", async () => {
    const h = harness();
    expect((await h.write({ mode: "selected" }, { scopes: ["rth:read"] })).status).toBe(403);
  });
});

describe("async admission and direct uploads", () => {
  const T1 = T0 + 5000;
  async function queued(s: ReturnType<typeof store>, records: Parameters<typeof batch>[0]) {
    const prepared = await prepareDelivery(TENANT, await batch(records));
    const key = await enqueueAdmission({ store: s, keyRoot: "96bulls", enqueue: async () => {} }, TENANT, prepared, T0, 0);
    return { v: 1, kind: "ingest-admission", key } as AdmissionMessage;
  }
  const consumer = (s: ReturnType<typeof store>, ledger: MemoryLedger, consent: UploadConsentStore) => ({
    mode: "on" as const, deliver: { ledger, target: { store: s, keyRoot: "96bulls" } } as never, custodySource: "relayhistory" as const,
    decompress: zstd, nowMs: () => T1, uploadConsent: { enforcement: "enforce" as const, store: consent, nowMs: () => T1 },
  });

  test("a narrowing after enqueue (or after a presigned upload) refuses at admission: retry, inbox kept, nothing written", async () => {
    const s = store();
    const consent = new MemoryConsentStore();
    await consent.apply(TENANT, { mode: "all", confirmOrigins: ["origin-1"] }, { trusted: true, nowMs: T0 });
    const message = await queued(s, [sess("a")]);
    await consent.apply(TENANT, { mode: "selected" }, { trusted: false, nowMs: T0 + 1 });
    const ledger = new MemoryLedger();
    const r = await admitMessage(consumer(s, ledger, consent), message, 1);
    expect(r).toMatchObject({ action: "retry", code: UPLOAD_CONSENT_REQUIRED, stage: "consent" });
    expect(ledger.commits).toHaveLength(0);
    expect((await s.list(objectKey("96bulls/.admission-inbox"))).items.filter((i) => String(i.key).includes("outcome-"))).toHaveLength(0);
    // Once the person confirms, a redrive admits it.
    await consent.apply(TENANT, { allow: [{ source: "claude", sessionId: "a" }] }, { trusted: true, nowMs: T0 + 2 });
    expect(await admitMessage(consumer(s, ledger, consent), message, 2)).toMatchObject({ action: "ack", outcome: "admitted" });
  });

  test("the request path refuses before the inbox PUT: no queued ACK under a hold", async () => {
    const s = store();
    const sent: AdmissionMessage[] = [];
    const app = createIngestApp({
      identify: (req) => bindingIdentity(req), log: () => {},
      deliver: { ledger: new MemoryLedger(), acceptanceMode: "buffered", target: { store: s, keyRoot: "96bulls" } },
      asyncAdmission: { mode: "on", enqueue: async (m) => void sent.push(m), nowMs: () => T0 },
      uploadConsent: { enforcement: "enforce", store: new MemoryConsentStore(), nowMs: () => T0 },
    });
    const res = await app.request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json", [IDENTITY_HEADER]: encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE], admission: ADMISSION_ACCEPT_VALUE }) },
      body: JSON.stringify({ protocolVersion: 1, batch: await batch([sess("a")]) }) });
    expect(res.status).toBe(503);
    expect(sent).toHaveLength(0);
    expect((await s.list(objectKey("96bulls/.admission-inbox"))).items).toHaveLength(0);
  });
});

describe("decoded paths", () => {
  test("a percent-encoded consent route is the same route: same scopes, same trust rule", async () => {
    const h = harness();
    const enc = (body: unknown, identity: Record<string, unknown>) =>
      h.app.request("/v1/upload-%63onsent", { method: "POST", headers: { "content-type": "application/json", ...h.as(identity) }, body: JSON.stringify(body) });
    expect(await errorCode(await enc({ mode: "all" }, {}))).toBe("upload_consent_untrusted");
    expect((await enc({ mode: "all" }, { scopes: [CONSENT_SCOPE] })).status).toBe(200);
    expect((await enc({ mode: "selected" }, { scopes: ["rth:read"] })).status).toBe(403);
  });

  test("a percent-encoded batch route is held exactly like the plain one", async () => {
    const h = harness();
    const res = await h.app.request("/v1/delivery/%62atches", { method: "POST", headers: { "content-type": "application/json", ...h.as({}) },
      body: JSON.stringify({ protocolVersion: 1, batch: await batch([sess("a")]) }) });
    expect(res.status).toBe(503);
    expect(h.ledger.claims).toHaveLength(0);
  });
});

describe("archive recovery (/v1/delivery/recovery)", () => {
  // The claim-timeout-busy.test.ts recovery harness: one signed buffered-mode archive replay.
  async function recover(consent: MemoryConsentStore) {
    const s = store();
    const ledger = new MemoryLedger();
    const raw = await batch([record({ record_id: "recovered", session_id: "archived" })], { batch_id: "recovered" });
    const prepared = await prepareDelivery(TENANT, raw);
    const sourceDigest = "d".repeat(64);
    const custody = {
      sourceDigest,
      witness: {
        version: 1 as const, issuer: "relayhistory-cloud" as const, sourceIdentitySha256: "c".repeat(64), controlEpoch: "e".repeat(64), privacyBaselineSha256: "a".repeat(64),
        tenant: TENANT, originId: prepared.originId, batchId: prepared.batchId, batchDigest: prepared.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null,
        privacyVersion: "0", originalRecordCount: raw.records.length, candidateCount: prepared.records.length,
        records: prepared.records.map((r, preparedOrd) => ({ preparedOrd, processingRank: preparedOrd, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
        privacy: { complete: true as const, deletions: [], resets: [] },
      },
    };
    ledger.prepareBufferStage = async () => ({ stageToken: crypto.randomUUID(), expiresAt: new Date(Date.now() + 60_000).toISOString() });
    ledger.commitBuffered = async () => ({ version: 1, sourceDigest, sourceControlVersion: "0", privacySealed: true });
    const app = createIngestApp({
      deliver: { ledger, target: { store: s, keyRoot: "96bulls", sleep: async () => {} }, acceptanceMode: "buffered" } as never,
      log: () => {}, signingKey: SIGNING_KEY,
      uploadConsent: { enforcement: "enforce", store: consent, nowMs: () => T0 },
    });
    const body = JSON.stringify({ protocol: RECOVERY_PROTOCOL, prepared, custody });
    const path = "/v1/delivery/recovery";
    const res = await app.fetch(new Request(`https://x${path}`, { method: "POST", body, headers: { "content-type": RECOVERY_CONTENT_TYPE, ...signedHeaders({ ...TENANT, scopes: [SYNC_SCOPE] }, "POST", path, new TextEncoder().encode(body)) } }));
    return { res, ledger };
  }

  test("an archive replay re-publishes: held without a confirmed choice, nothing claimed", async () => {
    const { res, ledger } = await recover(new MemoryConsentStore());
    expect(res.status).toBe(503);
    expect(await errorCode(res)).toBe(UPLOAD_CONSENT_REQUIRED);
    expect(ledger.claims).toHaveLength(0);
  });

  test("with the person's confirmed choice, the same replay is admitted", async () => {
    const consent = new MemoryConsentStore();
    await consent.apply(TENANT, { mode: "all", confirmOrigins: ["origin-1"] }, { trusted: true, nowMs: T0 });
    const { res, ledger } = await recover(consent);
    expect(res.status).toBe(200);
    expect(ledger.claims).toHaveLength(1);
  });
});

describe("selection disables vs a confirmed allow (ordered controls)", () => {
  const control = { epoch: "e".repeat(64), version: "5", digest: "d".repeat(64), kind: "selection" as const };
  async function disable(outcome: "applied" | "replay" | "stale") {
    const consent = new MemoryConsentStore();
    await consent.apply(TENANT, { mode: "all", confirmOrigins: ["origin-1"], allow: [{ source: "claude", sessionId: "kept" }] }, { trusted: true, nowMs: T0 });
    const ledger = new MemoryLedger() as MemoryLedger & { applyOrderedControl?: unknown };
    ledger.applyOrderedControl = async () => ({ control: { ...control, outcome }, updated: 1 });
    const app = createIngestApp({
      identify: () => ({ ...TENANT, scopes: [SYNC_SCOPE], control }) as never, log: () => {},
      deliver: { ledger, target: { store: store(), keyRoot: "96bulls", sleep: async () => {} } } as never,
      uploadConsent: { enforcement: "enforce", store: consent, nowMs: () => T0 + 1 },
    });
    const res = await app.request("/v1/delivery/sessions/selection", { method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ account_id: await deliveryAccount(TENANT), origin_id: "origin-1", sessions: [{ source: "claude", session_id: "kept" }, { source: "claude", session_id: "other" }], enabled: false }) });
    expect(res.status).toBe(200);
    return consent.decisions.get(JSON.stringify([TENANT.orgId, TENANT.workspaceId, TENANT.userId]))!;
  }

  test("a stale disable never overwrites the newer confirmed allow, but records the deny for undecided sessions", async () => {
    const d = await disable("stale");
    expect([d.get(sessionKey("claude", "kept")), d.get(sessionKey("claude", "other"))]).toEqual(["allow", "deny"]);
  });

  test("a replayed disable never overwrites an allow, but still records the deny the first attempt may have missed", async () => {
    const d = await disable("replay");
    expect([d.get(sessionKey("claude", "kept")), d.get(sessionKey("claude", "other"))]).toEqual(["allow", "deny"]);
  });

  test("an applied disable is the newest intent: it denies, even over an earlier allow", async () => {
    const d = await disable("applied");
    expect([d.get(sessionKey("claude", "kept")), d.get(sessionKey("claude", "other"))]).toEqual(["deny", "deny"]);
  });
});

describe("the operator cap rides the admission inbox", () => {
  const T1 = T0 + 5000;
  test("the request path writes the cap; the consumer enforces it even after the person's allow is withdrawn", async () => {
    const s = store();
    const consent = new MemoryConsentStore();
    await consent.apply(TENANT, { mode: "all", confirmOrigins: ["origin-1"], allow: [{ source: "claude", sessionId: "a" }] }, { trusted: true, nowMs: T0 });
    const sent: AdmissionMessage[] = [];
    const app = createIngestApp({
      identify: () => ({ ...TENANT, scopes: [SYNC_SCOPE], admission: ADMISSION_ACCEPT_VALUE, role: "member", capturePolicy: { revision: 7, settings: { sharing_mode: "selected" } } }) as never,
      log: () => {}, capturePolicyUploads: "enforce",
      deliver: { ledger: new MemoryLedger(), acceptanceMode: "buffered", target: { store: s, keyRoot: "96bulls" } } as never,
      asyncAdmission: { mode: "on", enqueue: async (m) => void sent.push(m), nowMs: () => T0, selfhost: { identityOf: () => ({ ...TENANT, scopes: [SYNC_SCOPE], admission: ADMISSION_ACCEPT_VALUE }) as never } },
      uploadConsent: { enforcement: "enforce", store: consent, nowMs: () => T0 },
    });
    const res = await app.request("/v1/delivery/batches", { method: "POST", headers: { "content-type": "application/json", "x-agent-relay-capture-policy-revision": "7" },
      body: JSON.stringify({ protocolVersion: 1, batch: await batch([sess("a")]) }) });
    expect(res.status).toBe(200);
    expect(sent).toHaveLength(1);
    const { decodeInbox } = await import("../async-admission.ts");
    const { readAll } = await import("../../shared/vendor.ts");
    const decoded = await decodeInbox("96bulls", sent[0]!.key, zstd(await readAll((await s.getObject(objectKey(sent[0]!.key))).body)));
    expect(decoded?.consentCap).toEqual({ mode: "selected" });
    // The person's allow is withdrawn after the upload was queued (policy stays "all"): uncapped, "all"
    // would admit it; the operator's "selected" cap still refuses it.
    consent.decisions.get(JSON.stringify([TENANT.orgId, TENANT.workspaceId, TENANT.userId]))!.delete(sessionKey("claude", "a"));
    const ledger = new MemoryLedger();
    const r = await admitMessage({ mode: "on", deliver: { ledger, target: { store: s, keyRoot: "96bulls" } } as never, custodySource: "relayhistory",
      decompress: zstd, nowMs: () => T1, uploadConsent: { enforcement: "enforce", store: consent, nowMs: () => T1 } }, sent[0]!, 1);
    expect(r).toMatchObject({ action: "retry", code: UPLOAD_CONSENT_REQUIRED });
    expect(ledger.commits).toHaveLength(0);
  });

  test("a malformed stored cap is a malformed inbox object, never ignored", async () => {
    const { encodeInbox, decodeInbox, inboxKey } = await import("../async-admission.ts");
    const prepared = await prepareDelivery(TENANT, await batch([sess("a")]));
    const key = inboxKey("96bulls", TENANT, prepared.originId, prepared.receipt.receiptId, T0, "0123456789abcdef");
    for (const cap of [{ mode: "all" }, { mode: "selected", extra: 1 }, { mode: "new", sinceMs: -1 }]) {
      const raw = zstd(encodeInbox(TENANT, prepared, T0, 0, cap as never));
      await expect(decodeInbox("96bulls", key, raw)).rejects.toThrow();
    }
    expect((await decodeInbox("96bulls", key, zstd(encodeInbox(TENANT, prepared, T0, 0, { mode: "new", sinceMs: 5 }))))?.consentCap).toEqual({ mode: "new", sinceMs: 5 });
  });
});

describe("recordSelectionDeny (replay/stale path)", () => {
  const sessions = [{ source: "claude", sessionId: "a" }, { source: "claude", sessionId: "b" }];
  test("an allow that lands between the read and the write wins: CAS loss, re-read, the allowed session is skipped", async () => {
    const inner = new MemoryConsentStore();
    await inner.apply(TENANT, { mode: "all", confirmOrigins: ["origin-1"] }, { trusted: true, nowMs: T0 });
    let raced = false;
    const racing: UploadConsentStore = {
      snapshot: inner.snapshot.bind(inner),
      apply: async (t, w, o) => {
        if (!raced && w.deny) { raced = true; await inner.apply(TENANT, { allow: [{ source: "claude", sessionId: "a" }] }, { trusted: true, nowMs: T0 + 1 }); }
        return inner.apply(t, w, o);
      },
    };
    await recordSelectionDeny({ enforcement: "enforce", store: racing, nowMs: () => T0 + 2 }, TENANT, "origin-1", sessions, true);
    const d = inner.decisions.get(JSON.stringify([TENANT.orgId, TENANT.workspaceId, TENANT.userId]))!;
    expect([d.get(sessionKey("claude", "a")), d.get(sessionKey("claude", "b"))]).toEqual(["allow", "deny"]);
  });

  test("an existing deny is left as is (no revision bump); three CAS losses in a row answer 503", async () => {
    const inner = new MemoryConsentStore();
    await inner.apply(TENANT, { mode: "all", deny: [{ source: "claude", sessionId: "a" }, { source: "claude", sessionId: "b" }] }, { trusted: true, nowMs: T0 });
    const before = inner.policies.get(JSON.stringify([TENANT.orgId, TENANT.workspaceId, TENANT.userId]))!.revision;
    await recordSelectionDeny({ enforcement: "enforce", store: inner, nowMs: () => T0 }, TENANT, "origin-1", sessions, true);
    expect(inner.policies.get(JSON.stringify([TENANT.orgId, TENANT.workspaceId, TENANT.userId]))!.revision).toBe(before);
    let applies = 0;
    const contended: UploadConsentStore = {
      snapshot: async () => ({ held: false, policy: { mode: "all", revision: 1, newSessionBoundaryMs: null }, originConfirmed: true, sessions: new Map() }),
      apply: async () => { applies++; throw new ConsentWriteRefused("upload_consent_revision", 409, "changed"); },
    };
    await expect(recordSelectionDeny({ enforcement: "enforce", store: contended }, TENANT, "origin-1", sessions, true)).rejects.toMatchObject({ status: 503 });
    expect(applies).toBe(3);
  });
});


describe("report mode never refuses, even with the consent store unreadable", () => {
  const broken: UploadConsentStore = { snapshot: async () => { throw new Error("relation does not exist"); }, apply: async () => { throw new Error("relation does not exist"); } };
  test("report: an upload is accepted and the store outage is logged (e.g. before migration 057010 is applied)", async () => {
    const h = harness({ enforcement: "report", consent: broken });
    expect((await h.post(await batch([sess("a")]))).status).toBe(200);
    expect(h.ledger.commits).toHaveLength(1);
    expect(h.lines.find((l) => l.error_code === "upload_consent_store_unavailable")).toMatchObject({ state: "reported", org_id: TENANT.orgId });
  });
  test("report: a selection disable whose deny cannot be recorded is a retryable 503, never a silent success", async () => {
    const h = harness({ enforcement: "report", consent: broken });
    const body = JSON.stringify({ account_id: await deliveryAccount(TENANT), origin_id: "origin-1", sessions: [{ source: "claude", session_id: "x" }], enabled: false });
    const res = await h.app.request("/v1/delivery/sessions/selection", { method: "POST", headers: { "content-type": "application/json", ...h.as({}) }, body });
    expect(res.status).toBe(503);
    // The fence applied and is idempotent: the Desktop's retry, once the store answers, records the deny.
    expect(h.ledger.deleted.has(sessionKey("claude", "x"))).toBe(true);
    const good = new MemoryConsentStore();
    const retry = harness({ enforcement: "report", consent: good, ledger: h.ledger });
    expect((await retry.app.request("/v1/delivery/sessions/selection", { method: "POST", headers: { "content-type": "application/json", ...retry.as({}) }, body })).status).toBe(200);
    expect(good.decisions.get(JSON.stringify([TENANT.orgId, TENANT.workspaceId, TENANT.userId]))?.get(sessionKey("claude", "x"))).toBe("deny");
  });
  test("hold-only and enforce still fail closed on a store outage", async () => {
    for (const enforcement of ["hold-only", "enforce"] as const) {
      const h = harness({ enforcement, consent: broken });
      expect((await h.post(await batch([sess("a")]))).status).toBe(503);
      expect(h.ledger.claims).toHaveLength(0);
    }
  });
});
