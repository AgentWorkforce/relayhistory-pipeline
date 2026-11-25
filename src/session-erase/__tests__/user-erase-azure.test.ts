/**
 * The operator per-user erase on LOCKER_PROVIDER=azure (user-erase-main.ts, azure-target.ts) over a fake owner
 * connection and the in-memory Azure container: the writer-store match and the data-plane preflight come before any
 * SQL; the bytes phase erases every version, snapshot and soft-deleted copy under the user's machine prefixes and a
 * fresh strict pass proves it; with SUMMARY_DECISION_RECORDS_ENABLED=true the relayfacts step runs first, before begin
 * (the proof the runner's azure_decision_records_unsupported refusal waits on); Azure errors a rerun cannot fix are
 * refusals, classified by operation.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAzureBlobEraser } from "../../migration/azure-blob-erasure/eraser.ts";
import { FAKE_ENDPOINT, FakeAzureBlob, type FakeAzureOptions } from "../../migration/__tests__/fake-azure-blob.ts";
import { liveMachineId } from "../../migration/neon-s3-erasure/live.ts";
import { CONFIRMATION, UserEraseRefused } from "../user-erase.ts";
import { SessionEraseRefused } from "../selfhost-eraser.ts";
import { errorEvent, exitCodeForError, exitCodeForResult, runUserEraseMain } from "../user-erase-main.ts";

const ROOT = "96bulls", ORG = "fx-org", WS = "fx-ws", USER = "fx-user", ORIGIN = "fx-origin";
const M = liveMachineId(WS, ORIGIN, USER);
const OTHER = liveMachineId(WS, ORIGIN, "fx-other-user");
const MINE = [
  `${ROOT}/${ORG}/buffered-batches/v1/ws=${WS}/m=${M}/b=fx-1.ndjson.zst`,
  `${ROOT}/${ORG}/live-batches/v1/ws=${WS}/m=${M}/b=fx-2.staged.ndjson.zst`,
  `${ROOT}/${ORG}/v1/ws=${WS}/m=${M}/h=claude-code/d=2026/11/01/s=fx-s/manifest.json`,
  `${ROOT}/${ORG}/live-sessions/v1/ws=${WS}/m=${M}/h=claude-code/s=fx-s.json`,
];
const THEIRS = [`${ROOT}/${ORG}/v1/ws=${WS}/m=${OTHER}/h=claude-code/d=2026/11/01/s=fx-t/manifest.json`];
const good = { request_id: "fx-request", org_id: ORG, workspace_id: WS, user_id: USER, confirmation: CONFIRMATION };
const COUNTS = { sessions: 1, fencesAdded: 1, evidenceRows: 2, recordsDeleted: 1, recordsRewritten: 0 };
const PREFLIGHT = "SELECT to_regprocedure($1) IS NOT NULL AS r";

let dir = "", KEY = "", keyFile = "";
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "user-erase-azure-"));
  KEY = Buffer.from(crypto.getRandomValues(new Uint8Array(64))).toString("base64");
  keyFile = join(dir, "account-key");
  writeFileSync(keyFile, KEY);
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));

function env(extra: Record<string, string | undefined> = {}): NodeJS.ProcessEnv {
  const e: Record<string, string | undefined> = {
    OPERATOR_USER_ERASE_ENABLED: "1", LOCKER_PROVIDER: "azure",
    LOCKER_AZURE_ACCOUNT: "fxacct", LOCKER_BUCKET: "fx-container", LOCKER_ENDPOINT: FAKE_ENDPOINT,
    SESSION_ERASE_AZURE_ACCOUNT: "fxacct", SESSION_ERASE_AZURE_CONTAINER: "fx-container", SESSION_ERASE_AZURE_ENDPOINT: FAKE_ENDPOINT,
    SESSION_ERASE_AZURE_ACCOUNT_KEY_FILE: keyFile, SESSION_ERASE_KEY_ROOT: ROOT, ...extra,
  };
  for (const k of Object.keys(e)) if (e[k] === undefined) delete e[k];
  return e as NodeJS.ProcessEnv;
}

/** The owner connection: begin purges to metadata_purged, authorize answers metadata_purged, complete verifies. */
function fakeDb() {
  const seen: string[] = [];
  const view = (state: string) => ({ state, origins: [ORIGIN], tokens_revoked: 0, deleted: {}, done: true });
  return {
    seen,
    query: async (text: string) => {
      seen.push(text);
      if (text === PREFLIGHT) return { rows: [{ r: true }] };
      if (text.startsWith("SELECT sessions.relayfacts_user_erase_v1")) return { rows: [{ r: COUNTS }] };
      if (text.startsWith("SELECT sessions.operator_user_erase_begin")) return { rows: [{ r: view("metadata_purged") }] };
      if (text.startsWith("SELECT sessions.operator_user_erase_complete")) return { rows: [{ r: view("bytes_verified") }] };
      if (text.startsWith("SELECT state FROM sessions.operator_user_erasures")) return { rows: [{ state: "metadata_purged" }] };
      return { rows: [] };
    },
  };
}

function seed(opts: FakeAzureOptions = {}) {
  const fake = new FakeAzureBlob({ account: "fxacct", container: "fx-container", accountKey: KEY, ...opts });
  for (const k of MINE) { fake.put(k, "a"); fake.put(k, "b"); }
  fake.snapshot(MINE[2]!);
  for (const k of THEIRS) fake.put(k, "keep");
  return fake;
}
const ports = (fake: FakeAzureBlob, db = fakeDb()) => ({
  db, sleep: async () => {}, log: () => {},
  azureEraser: (c: Parameters<typeof createAzureBlobEraser>[0]) => createAzureBlobEraser({ ...c, fetch: fake.fetch }),
  preflight: { fetch: fake.fetch },
  eraser: () => { throw new Error("no S3 eraser on azure"); },
});
const mine = (fake: FakeAzureBlob) => fake.records.filter((r) => !THEIRS.includes(r.key)).map((r) => r.key);
const codeOf = (e: unknown) => (e instanceof UserEraseRefused || e instanceof SessionEraseRefused ? e.code : String(e));

describe("per-user erase on azure", () => {
  test("preflight before any SQL; every copy under the user's prefixes erased and verified; another user's bytes survive", async () => {
    const fake = seed();
    const db = fakeDb();
    let sqlBeforePreflight = -1;
    fake.hook = (req) => { if (req.method === "PUT" && sqlBeforePreflight < 0) sqlBeforePreflight = db.seen.length; return undefined; };
    const r = await runUserEraseMain(env(), () => JSON.stringify(good), ports(fake, db));
    expect(r).toMatchObject({ enabled: true, verified: true, state: "bytes_verified" });
    expect(exitCodeForResult(r)).toBe(0);
    expect(sqlBeforePreflight).toBe(0);
    expect(mine(fake)).toEqual([]);
    expect(fake.records.map((x) => x.key)).toEqual(THEIRS);
    expect(fake.requests.every((q) => q.redirect === "manual")).toBe(true);
  });

  test("relayfacts on (SUMMARY_DECISION_RECORDS_ENABLED=true): the relayfacts step runs before begin, then the bytes are erased and verified", async () => {
    const fake = seed();
    const db = fakeDb();
    const r = await runUserEraseMain(env({ SUMMARY_DECISION_RECORDS_ENABLED: "true" }), () => JSON.stringify(good), ports(fake, db));
    expect(r).toMatchObject({ verified: true, relayfacts: COUNTS });
    const at = (prefix: string) => db.seen.findIndex((t) => t.startsWith(prefix));
    expect(at("SELECT sessions.relayfacts_user_erase_v1")).toBeGreaterThan(-1);
    expect(at("SELECT sessions.relayfacts_user_erase_v1")).toBeLessThan(at("SELECT sessions.operator_user_erase_begin"));
    expect(mine(fake)).toEqual([]);
  });

  test("the writer-store match and the preflight refuse before any SQL or session delete (exit 2)", async () => {
    const cases: [Record<string, string | undefined>, FakeAzureOptions, string][] = [
      [{ LOCKER_BUCKET: "fx-other-container" }, {}, "azure_target_not_writer_store"],
      [{ SESSION_ERASE_S3_BUCKET: "fx-bucket" }, {}, "azure_s3_settings_conflict"],
      [{}, { versioning: false }, "azure_blob_not_versioned"],
      [{}, { softDelete: true, permanentDelete: false }, "azure_blob_soft_delete_retained"],
    ];
    for (const [extra, opts, want] of cases) {
      const fake = seed(opts);
      const db = fakeDb();
      const e = await runUserEraseMain(env(extra), () => JSON.stringify(good), ports(fake, db)).catch((x) => x);
      expect([want, codeOf(e), exitCodeForError(e)]).toEqual([want, want, 2]);
      expect([want, db.seen]).toEqual([want, []]);
      expect([want, fake.requests.filter((q) => q.method === "DELETE" && !decodeURIComponent(q.url.pathname).includes(".runner-probe")).length]).toEqual([want, 0]);
    }
  });

  test("mid-run: a list 403 refuses (exit 2); a delete 403 fails (exit 4) under its own printed code", async () => {
    for (const [method, want, exit] of [["GET", "azure_forbidden", 2], ["DELETE", "erase_azure_delete_forbidden", 4]] as const) {
      const fake = seed();
      fake.hook = (req) => (req.method === method && !decodeURIComponent(req.url.pathname + req.url.search).includes(".runner-probe")
        ? new Response(null, { status: 403 }) : undefined);
      const e = await runUserEraseMain(env(), () => JSON.stringify(good), ports(fake)).catch((x) => x);
      expect([method, exit === 2 ? codeOf(e) : errorEvent(e).error, exitCodeForError(e)]).toEqual([method, want, exit]);
    }
  });

  test("a refusal AFTER begin (user already revoked and fenced): fix the account, rerun the SAME request, it resumes and verifies", async () => {
    const fake = seed();
    const db = fakeDb();
    let broken = true;
    fake.hook = (req) => (broken && req.method === "GET" && !decodeURIComponent(req.url.pathname + req.url.search).includes(".runner-probe")
      ? new Response(null, { status: 403 }) : undefined);
    const first = await runUserEraseMain(env(), () => JSON.stringify(good), ports(fake, db)).catch((x) => x);
    expect([codeOf(first), exitCodeForError(first)]).toEqual(["azure_forbidden", 2]);
    expect(db.seen.some((t) => t.startsWith("SELECT sessions.operator_user_erase_begin"))).toBe(true);
    expect(mine(fake).length).toBeGreaterThan(0);
    broken = false;   // the operator fixes the account (here: the key's list permission)
    const second = await runUserEraseMain(env(), () => JSON.stringify(good), ports(fake, db));
    expect(second).toMatchObject({ verified: true });
    expect(mine(fake)).toEqual([]);
  });

  test("unset LOCKER_PROVIDER keeps the S3 path: no Azure eraser, no preflight", async () => {
    let azure = 0;
    const e = await runUserEraseMain(env({ LOCKER_PROVIDER: undefined, SESSION_ERASE_S3_BUCKET: "fx-bucket", SESSION_ERASE_S3_REGION: "us-east-1",
      SESSION_ERASE_S3_ACCESS_KEY_ID: "AKIDFXEXAMPLE", SESSION_ERASE_S3_SECRET_ACCESS_KEY: "fxsecretkey0123456789" }), () => JSON.stringify(good), {
      db: fakeDb(), sleep: async () => {}, log: () => {},
      azureEraser: () => { azure++; throw new Error("no Azure eraser on S3"); },
      preflight: { probe: async () => { azure++; return "Enabled"; } },
      eraser: () => { throw new Error("s3-eraser-built"); },
    }).catch((x) => x as Error);
    expect((e as Error).message).toBe("s3-eraser-built");
    expect(azure).toBe(0);
  });
});
