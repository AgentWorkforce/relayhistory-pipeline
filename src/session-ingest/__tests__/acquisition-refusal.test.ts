/**
 * The writer-facing acquisition refusal (docs/factory-acquisition-owner-design.md §5.6): a database
 * `acquisition: <code>` refusal becomes HTTP 409 `{error:{code:'acquisition_refused'},refused:[...]}`,
 * whatever stage raised it; anything else keeps today's status. No database: a pg-shaped error.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { bindingIdentity, createIngestApp, encodeIdentity, IDENTITY_HEADER, SYNC_SCOPE } from "../app.ts";
import { ACQUISITION_ENROLLED_SOURCES, ACQUISITION_REFUSAL_CODES, AcquisitionRefusedError, acquisitionRefusalFrom } from "../acquisition-refusal.ts";
import { sqlLedger } from "../ledger-sql.ts";
import type { ClaimInput } from "../ledger.ts";
import { DeliveryError } from "../vendor/delivery/delivery-contracts.ts";
import { MemoryLedger, TENANT, batch, record, scratchStore } from "./support.ts";

const STATE: Record<string, string> = { not_owner: "42501", session_required: "22023" };
const pgError = (code: string, refused: unknown[], sqlstate = STATE[code] ?? "55000", detail = JSON.stringify(refused)) =>
  Object.assign(new Error(`acquisition: ${code}`), { code: sqlstate, detail });
const one = (code: string, source = "droid", sessionId = code === "session_required" ? "" : "sess-1") => ({ source, sessionId, code });

describe("acquisitionRefusalFrom: closed parse", () => {
  test("every refusal code with its SQLSTATE becomes the typed 409", () => {
    expect([...ACQUISITION_REFUSAL_CODES].sort()).toEqual(["grant_expired", "not_owner", "owner_changed", "owner_draining",
      "owner_other_principal", "owner_released", "owner_unknown", "session_required"]);
    for (const code of ACQUISITION_REFUSAL_CODES) {
      const r = acquisitionRefusalFrom(pgError(code, [one(code)]));
      expect(r).toBeInstanceOf(AcquisitionRefusedError);
      expect(r).toBeInstanceOf(DeliveryError);
      expect([r!.status, r!.code]).toEqual([409, "acquisition_refused"]);
      expect(r!.refused).toEqual([one(code)]);
    }
  });

  test("a mixed batch names every refused session in DETAIL order", () => {
    const list = [one("not_owner", "droid", "a"), one("owner_draining", "droid", "b"), one("session_required")];
    expect(acquisitionRefusalFrom(pgError("not_owner", list))!.refused).toEqual(list);
  });

  test("anything not exactly our refusal is not a 409 (the ledger keeps its 503)", () => {
    const cases: unknown[] = [
      pgError("not_owner", [one("not_owner")], "55000"), // wrong SQLSTATE
      pgError("owner_unknown", [one("owner_unknown")], "40001"),
      Object.assign(new Error("acquisition: session_busy"), { code: "40001", detail: "[]" }), // retryable, 503
      Object.assign(new Error("acquisition: acquisition_unexpected"), { code: "22023" }),
      Object.assign(new Error("acquisition: invalid_request"), { code: "22023" }),
      pgError("owner_unknown", []), // empty list
      pgError("owner_unknown", [one("owner_draining")]), // first entry disagrees with the message
      pgError("owner_unknown", [{ ...one("owner_unknown"), extra: 1 }]),
      pgError("owner_unknown", [{ source: "droid", sessionId: 1, code: "owner_unknown" }]),
      pgError("owner_unknown", [one("owner_unknown"), one("session_busy")]), // unknown code inside
      pgError("owner_unknown", [{ source: "droid", sessionId: "", code: "owner_unknown" }]), // only session_required has no session
      pgError("session_required", [{ source: "droid", sessionId: "x", code: "session_required" }]),
      pgError("owner_unknown", [], "55000", "not json"),
      pgError("owner_unknown", [], "55000", JSON.stringify({ source: "droid" })),
      pgError("owner_unknown", [], "55000", "[" + "1,".repeat(600_000) + "1]"), // over the detail bound
      Object.assign(new Error("acquisition: owner_unknown "), { code: "55000", detail: JSON.stringify([one("owner_unknown")]) }),
      Object.assign(new Error("ACQUISITION: owner_unknown"), { code: "55000", detail: JSON.stringify([one("owner_unknown")]) }),
      new Error("connection terminated"), null, undefined, "acquisition: owner_unknown",
      new Proxy({}, { get() { throw new Error("trap"); } }),
    ];
    for (const c of cases) expect(acquisitionRefusalFrom(c)).toBeUndefined();
  });
});

describe("ledger-sql: the refusal crosses the ledger boundary as the 409", () => {
  const claim: ClaimInput = { tenant: TENANT, originId: "o", batchId: "b", batchDigest: "d".repeat(64), receiptId: "rhr_" + "0".repeat(64), sessions: [["droid", "sess-1"]], records: [] };
  test("claim: refusal is typed; other database errors stay LedgerUnavailableError", async () => {
    const sent: string[] = [];
    const refusing = sqlLedger(async (_t, params) => { sent.push(String(params[0])); throw pgError("owner_unknown", [one("owner_unknown")]); });
    const e = await refusing.claim(claim).catch((x) => x);
    expect(e).toBeInstanceOf(AcquisitionRefusedError);
    // Unchanged payload when no acquisition entries are presented: no key at all.
    expect(sent[0]).not.toContain("acquisition");
    const busy = sqlLedger(async () => { throw Object.assign(new Error("acquisition: session_busy"), { code: "40001" }); });
    const b = await busy.claim(claim).catch((x) => x);
    expect(b).not.toBeInstanceOf(DeliveryError);
    expect(b.code).toBe("ledger_sqlstate_40001");
  });
  test("claim: presented entries pass through verbatim", async () => {
    const sent: string[] = [];
    const ledger = sqlLedger(async (_t, params) => { sent.push(String(params[0])); throw pgError("not_owner", [one("not_owner")]); });
    const acquisition = [{ source: "droid", sessionId: "sess-1", grantId: "00000000-0000-4000-8000-000000000001", ownerEpoch: "9223372036854775807", grant: "rhg_" + "0".repeat(64) }];
    await ledger.claim({ ...claim, acquisition }).catch(() => {});
    expect(JSON.parse(sent[0]!).acquisition).toEqual(acquisition);
  });
});

describe("HTTP: 409 acquisition_refused at claim and at commit; limits advertise enrolled sources", () => {
  const cleanups: (() => void)[] = [];
  afterEach(() => cleanups.splice(0).forEach((f) => f()));
  const good = encodeIdentity({ ...TENANT, scopes: [SYNC_SCOPE] });
  function app(ledger: MemoryLedger) {
    const s = scratchStore();
    cleanups.push(s.done);
    return createIngestApp({ deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} } }, log: () => {}, identify: (r) => bindingIdentity(r) });
  }
  const post = async (ledger: MemoryLedger) => {
    const body = JSON.stringify({ protocolVersion: 1, batch: await batch([record({ record_id: "e1" })]) });
    return app(ledger).fetch(new Request("https://x/v1/delivery/batches", { method: "POST", body, headers: { [IDENTITY_HEADER]: good, "x-correlation-id": "corr-a" } }));
  };
  const refused = [one("not_owner", "claude", "a"), one("grant_expired", "claude", "b")];

  test("claim refusal: exact body, nothing committed", async () => {
    const ledger = new MemoryLedger();
    ledger.claim = async () => { throw acquisitionRefusalFrom(pgError("not_owner", refused))!; };
    const res = await post(ledger);
    expect(res.status).toBe(409);
    expect(await res.json()).toEqual({ error: { code: "acquisition_refused", message: "Delivery refused until this machine owns the named sessions" }, correlationId: "corr-a", refused });
    expect(ledger.commits).toHaveLength(0);
  });

  test("commit refusal (owner changed after admission) is the same 409, never a 2xx", async () => {
    const ledger = new MemoryLedger();
    ledger.commit = async () => { throw acquisitionRefusalFrom(pgError("owner_changed", [one("owner_changed")]))!; };
    const res = await post(ledger);
    expect(res.status).toBe(409);
    const json = (await res.json()) as { error: { code: string }; refused: unknown[] };
    expect([json.error.code, json.refused]).toEqual(["acquisition_refused", [one("owner_changed")]]);
  });

  test("session_busy and every non-refusal database error remain 503", async () => {
    const ledger = new MemoryLedger();
    // Exactly what the SQL ledger throws for a 40001 (a LedgerUnavailableError, not a DeliveryError).
    ledger.claim = (input) => sqlLedger(async () => { throw Object.assign(new Error("acquisition: session_busy"), { code: "40001" }); }).claim(input);
    const res = await post(ledger);
    expect(res.status).toBe(503);
    expect(((await res.json()) as { error: { code: string } }).error.code).toBe("delivery_unavailable");
  });

  test("GET /v1/delivery/limits advertises the enrolled sources: none (dormant)", async () => {
    expect(ACQUISITION_ENROLLED_SOURCES).toEqual([]);
    const res = await app(new MemoryLedger()).fetch(new Request("https://x/v1/delivery/limits", { headers: { [IDENTITY_HEADER]: good } }));
    expect(((await res.json()) as { acquisitionEnrolledSources: unknown }).acquisitionEnrolledSources).toEqual([]);
  });
});

