/**
 * The export's seat sidecar (ExportOptions.seatSidecar, v3) against PGlite with the migrations through 20261105047020:
 * SEAT_PAGE agrees with the real `sessions.ingest_lazy_stage_seats` (seats when it returns, a refusal when it raises),
 * the opt-in off leaves the export exactly as before, and the drain (DrainOptions.acceptSeatSidecar) writes the seats.
 */
import { afterAll, afterEach, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { SEAT_PAGE, exportAcceptedStages, provenSeats, type ExportOptions, type ExportSql } from "../accepted-stage-export.ts";
import { ACCEPTED_SUFFIX, drainBufferedBatches, parseAcceptedMarker, parseAcceptedMarkerV3 } from "../buffered-drain.ts";
import { orgFenceAuthorityKey, s3LedgerKeys, s3LedgerObjects } from "../ledger-s3.ts";
import { BUFFER_STAGE_MAX_BYTES, prepareBufferedStage, putBufferedStage, stagedKey } from "../staging.ts";
import type { Group } from "../writer.ts";
import { applySupabaseSchema } from "./pglite-schema.ts";
import { scratchStore } from "./support.ts";

setDefaultTimeout(120_000);
const ROOT = "96bulls";
// Synthetic tenants, hashes and times only (the repo is public).
const A = { orgId: "org-a", workspaceId: "ws-a", userId: "user-1" };
const ORIGIN = "origin-1";
const hex = (c: string) => c.repeat(64);
const sha = (v: string | Uint8Array) => createHash("sha256").update(v).digest("hex");
// The clock carries the policy's identity and epoch, as the lazy guard requires.
const IDENTITY = hex("1"), EPOCH = hex("2"), GEN = hex("5");
const HIGH = 1000, LAST = 1_000_000;
const dec = new TextDecoder();
const enc = new TextEncoder();
const decompress = (b: Uint8Array) => new Uint8Array(zstdDecompressSync(b, { maxOutputLength: BUFFER_STAGE_MAX_BYTES }));

let db: PGlite;
/** The custody controls in their consistent state: policy, clock, PASS generation and the org's admission. */
async function resetControls() {
  await db.exec(`SET session_replication_role = replica;
    DELETE FROM sessions.ingest_lazy_policy; DELETE FROM sessions.ingest_receipt_seat_clock; DELETE FROM sessions.ingest_buffer_admission;
    DELETE FROM sessions.__96bulls_role_validation;`);
  await db.query(`INSERT INTO sessions.ingest_lazy_policy (singleton, baseline_mode, generation, source_identity_sha256, control_epoch, privacy_baseline_sha256,
      retention_contract_sha256, source_proof_sha256, issuer, valid_until) VALUES (true, 'lazy-current', $1, $2, $3, $1, $1, $1, 'relayhistory-cloud', now() + interval '1 day')`,
    [GEN, IDENTITY, EPOCH]);
  await db.query(`INSERT INTO sessions.ingest_receipt_seat_clock (singleton, control_epoch, source_identity_sha256, allocated_high_water, last_seat, baseline, initial_generation)
      VALUES (true, $1, $2, $3, $4, jsonb_build_object('sourceProofSha256', $5::text), $5)`, [EPOCH, IDENTITY, HIGH, LAST, GEN]);
  await db.query(`INSERT INTO sessions.ingest_buffer_admission (org_id, privacy_baseline_sha256, retention_contract_sha256, schema_generation, valid_until, control_epoch)
      VALUES ($1, $2, $2, $2, now() + interval '1 day', $3)`, [A.orgId, GEN, EPOCH]);
  await db.query("INSERT INTO sessions.__96bulls_role_validation VALUES ('session_directory_search_worker', $1, 'pass')", [GEN]);
  await db.exec("SET session_replication_role = origin");
}
beforeAll(async () => {
  db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  await applySupabaseSchema(db, { until: "20261105047020_buffered_session_stages_paged.sql" });
  await db.exec("CREATE TABLE sessions.__96bulls_role_validation(name text PRIMARY KEY, generation text, status text)");
  await db.exec(`SET session_replication_role = replica;
    UPDATE sessions.ingest_claim_sessions_policy SET write_enabled = true, backfill_complete_at = now(), read_enabled = true;
    INSERT INTO sessions.ingest_privacy_authority (singleton, state, control_epoch, neon_fingerprint_sha256, activated_at) VALUES (true, 'active', '${EPOCH}', '${hex("6")}', now());
    SET session_replication_role = origin;`);
  await resetControls();
});
afterAll(async () => { await db.close(); });
let cleanup: (() => void)[] = [];
afterEach(async () => {
  cleanup.forEach((c) => c());
  cleanup = [];
  await db.exec(`SET session_replication_role = replica;
    DELETE FROM sessions.ingest_buffer_outbox; DELETE FROM sessions.ingest_lazy_ack_decisions; DELETE FROM sessions.ingest_lazy_native_slots;
    DELETE FROM sessions.ingest_receipt_seats; DELETE FROM sessions.ingest_stage_intents; DELETE FROM sessions.ingest_claim_sessions;
    DELETE FROM sessions.ingest_claim_attempts; DELETE FROM sessions.ingest_batches; DELETE FROM sessions.ingest_fence_versions;
    DELETE FROM sessions.owner_buffered_parity;
    SET session_replication_role = origin;`);
  await resetControls();
});
async function scratch(): Promise<ObjectStore> {
  const s = scratchStore();
  cleanup.push(s.done);
  await s3LedgerObjects(s.store, ROOT).editFence(s3LedgerKeys(ROOT, A).fence, (f) => ({ next: f, result: undefined }));
  await s.store.put(objectKey(orgFenceAuthorityKey(ROOT, A.orgId)), enc.encode(JSON.stringify({ v: 1, fenceBackfill: "complete", revisionBackfill: "complete",
    ownershipBackfill: "complete", controlMirror: "on", rearmSweeper: "live", revisionLayout: "4096", workerHttpAdmission: "off", writtenAtMs: 1, orgId: A.orgId })));
  return s.store;
}
/** The owner's parity proof with no control head (as accepted-stage-export.pglite.test.ts proves it). */
async function prove() {
  await db.exec("SET session_replication_role = replica");
  await db.query("DELETE FROM sessions.owner_buffered_parity WHERE org_id = $1 AND workspace_id = $2 AND user_id = $3", [A.orgId, A.workspaceId, A.userId]);
  await db.query(`INSERT INTO sessions.owner_buffered_parity (org_id, workspace_id, user_id, cutover_epoch, head_epoch, head_version, head_digest, head_kind, no_head,
      privacy_baseline_sha256, deletion_set_digest, reset_set_digest, target_lineage_digest, target_generation)
    SELECT $1, $2, $3, $4, NULL, NULL, NULL, NULL, true, $5, e.deletion_set_digest, e.reset_set_digest, e.target_lineage_digest, e.target_generation
      FROM sessions.owner_buffered_parity_expected($1, $2, $3, $4, NULL, NULL, NULL) AS e`, [A.orgId, A.workspaceId, A.userId, EPOCH, GEN]);
  await db.exec("SET session_replication_role = origin");
}

type SeedGroup = { source: string; session: string; records: { id: string; rev: number; accepted?: boolean }[] };
interface Seeded { key: string; sha256: string; token: string; groups: SeedGroup[]; seats: string[][] }
let nextSeat = HIGH;
/**
 * One buffered batch as lazy admission leaves it, consistent with the seat clock: the claim (schema generation, witness
 * and identity of the custody policy), its acknowledged live stage and outbox row, and per line a receipt seat whose
 * key material is the reader's exact candidate key array (built in SQL), its native slot and its ACK decision. A line
 * with `accepted: false` lost to an earlier body (another first digest).
 */
async function seed(groups: SeedGroup[], o: { key?: string; sha256?: string; digest?: string; claim?: string; epochs?: unknown[] } = {}): Promise<Seeded> {
  const batchId = randomUUID(), digest = o.digest ?? sha(batchId), claim = o.claim ?? randomUUID(), token = randomUUID();
  const sha256 = o.sha256 ?? sha(token);
  const key = o.key ?? stagedKey(ROOT, { org_id: A.orgId, workspace_id: A.workspaceId, origin_id: ORIGIN, user_id: A.userId, batch_digest: digest, claim_token: claim }, sha256, "buffered-batches");
  let ord = 0;
  const groupOrdinals = groups.map((g) => g.records.map(() => ord++));
  const lines = groups.flatMap((g, gi) => g.records.map((r, j) => ({ ord: groupOrdinals[gi]![j]!, g, r, digest: sha(`${r.id}:${r.rev}`) })));
  const records = lines.map((l) => ({ ord: l.ord, source: l.g.source, session_id: l.g.session, record_id: l.r.id, revision: l.r.rev, digest: l.digest }));
  const sessions = [...new Map(groups.map((g) => [JSON.stringify([g.source, g.session]), g])).values()];
  const epochs = o.epochs ?? sessions.map((g) => ({ source: g.source, session_id: g.session, epoch: 0, reset_epoch: 0 }));
  await db.exec("SET session_replication_role = replica");
  await db.query(`INSERT INTO sessions.ingest_batches (org_id, workspace_id, origin_id, batch_id, user_id, digest, receipt_id, state, committed_at, publication_state)
    VALUES ($1,$2,$3,$4,$5,$6,$7,'committed',now(),'buffered')`, [A.orgId, A.workspaceId, ORIGIN, batchId, A.userId, digest, `rhr_${hex("c")}`]);
  await db.query(`INSERT INTO sessions.ingest_claim_attempts (claim_token, org_id, workspace_id, user_id, origin_id, batch_id, digest, epochs, records, reset_epoch,
      source_control_version, source_identity_sha256, schema_generation, source_witness_sha256)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,0,0,$10,$11,$11)`, [claim, A.orgId, A.workspaceId, A.userId, ORIGIN, batchId, digest, JSON.stringify(epochs), JSON.stringify(records), IDENTITY, GEN]);
  for (const g of sessions)
    await db.query(`INSERT INTO sessions.ingest_claim_sessions (claim_token, source, session_id, org_id, workspace_id, user_id, origin_id, batch_id, digest, reset_epoch, accepted_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,now())`, [claim, g.source, g.session, A.orgId, A.workspaceId, A.userId, ORIGIN, batchId, digest]);
  await db.query(`INSERT INTO sessions.ingest_stage_intents (stage_token, claim_token, org_id, workspace_id, user_id, origin_id, batch_id, digest, stage_key, stage_sha256, stage_bytes, group_ordinals, reset_epoch, receipt_state, cleanup_state, expires_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,100,$11::jsonb,0,'acknowledged','live',now() + interval '1 day')`,
    [token, claim, A.orgId, A.workspaceId, A.userId, ORIGIN, batchId, digest, key, sha256, JSON.stringify(groupOrdinals)]);
  await db.query(`INSERT INTO sessions.ingest_buffer_outbox (org_id, workspace_id, origin_id, batch_id, user_id, digest, active_stage_token, state) VALUES ($1,$2,$3,$4,$5,$6,$7,'buffered')`,
    [A.orgId, A.workspaceId, ORIGIN, batchId, A.userId, digest, token]);
  const seats: string[][] = groupOrdinals.map((g) => g.map(() => ""));
  for (const l of lines) {
    const seat = ++nextSeat, slotKey = sha(`slot:${claim}:${l.ord}`), accepted = l.r.accepted !== false;
    const first = accepted ? l.digest : sha(`earlier:${l.r.id}`);
    const gi = groups.indexOf(l.g);
    seats[gi]![l.g.records.indexOf(l.r)] = String(seat);
    await db.query(`WITH m AS (SELECT jsonb_build_array('buffer-first-ack-seat-v1', $1::text, $2::text, $3::text, $4::text, $5::text, $6::text,
               $7::text, $8::text, '0'::text, $9::text, $10::text, '0'::text) AS m)
      INSERT INTO sessions.ingest_receipt_seats (key_sha256, key_material, seat, first_digest, source, session_id, fence_epoch)
      SELECT encode(sha256(convert_to(m.m::text, 'UTF8')), 'hex'), m.m, $11, $12, $9, $10, 0 FROM m`,
      [EPOCH, IDENTITY, A.orgId, A.workspaceId, A.userId, ORIGIN, l.r.id, String(l.r.rev), l.g.source, l.g.session, seat, first]);
    const seatKey = (await db.query<{ k: string }>("SELECT key_sha256 AS k FROM sessions.ingest_receipt_seats WHERE seat = $1", [seat])).rows[0]!.k;
    await db.query(`INSERT INTO sessions.ingest_lazy_native_slots (key_sha256, key_material, org_id, workspace_id, origin_id, record_id, source_identity_sha256, control_epoch,
        initialized_witness_sha256, current_owner, current_source, current_session, current_revision, current_digest, current_reset_epoch, current_fence_epoch, current_seat_key)
      VALUES ($1,'[]',$2,$3,$4,$5,$6,$7,$6,$8,$9,$10,$11,$12,0,0,$13)`,
      [slotKey, A.orgId, A.workspaceId, ORIGIN, l.r.id, IDENTITY, EPOCH, A.userId, l.g.source, l.g.session, l.r.rev, first, seatKey]);
    await db.query(`INSERT INTO sessions.ingest_lazy_ack_decisions (claim_token, ord, native_key_sha256, native_lineage, candidate_key_sha256, candidate_digest, first_digest,
        body_accepted, seat_key_sha256, reset_epoch, fence_epoch, generation, source_control_version, source_witness_sha256)
      VALUES ($1, $2, $3, 0, $4, $5, $6, $7, $4, 0, 0, $8, 0, $8)`,
      [claim, l.ord, slotKey, seatKey, l.digest, first, accepted, GEN]);
  }
  await db.exec("SET session_replication_role = origin");
  await prove();
  return { key, sha256, token, groups, seats };
}
const objectLine = (id: string) => JSON.stringify({ kind: "session_event", origin_id: ORIGIN, record_id: id, revision: "1", operation: "upsert", received_at: "2030-01-02T00:00:00.000Z", payload: { text: id, ts_ms: 1_893_542_400_000 } });
/** A real staged object in S3 and the same batch in SQL (records at revision 1, as objectLine writes them). */
async function stageInBoth(store: ObjectStore, groups: SeedGroup[]) {
  const claim = randomUUID(), digest = sha(randomUUID());
  const objectGroups: Group[] = groups.map((g) => ({ source: g.source, sessionId: g.session, lines: g.records.map((r) => objectLine(r.id)), meta: g.records.map((r) => [`${r.id}:r${r.rev}`, sha(`${r.id}:${r.rev}`), g.source, g.session]) }));
  let ord = 0;
  const prepared = prepareBufferedStage(ROOT, { claim_token: claim, batch_digest: digest, received_at: "2030-01-02T00:00:00.000Z", org_id: A.orgId, workspace_id: A.workspaceId, origin_id: ORIGIN, user_id: A.userId },
    objectGroups, groups.map((g) => g.records.map(() => ord++)));
  await putBufferedStage(store, prepared);
  return seed(groups, { key: prepared.spec.key, sha256: prepared.spec.sha256, digest, claim });
}

function recordingSql(log: string[]): ExportSql {
  return { async query(text, params) { log.push(text.trim()); return { rows: (await db.query(text, params as unknown[])).rows as Record<string, unknown>[] }; } };
}
const run = (store: ObjectStore, over: Partial<ExportOptions> = {}, log: string[] = []) =>
  exportAcceptedStages({ sql: recordingSql(log), store, keyRoot: ROOT, dryRun: false, nowMs: () => 1_893_542_400_000, ...over });
const readText = async (store: ObjectStore, key: string) => dec.decode(await readAll(await store.get(objectKey(key))));
const exists = async (store: ObjectStore, key: string) => (await store.head(objectKey(key))) !== undefined;
const drain = (store: ObjectStore, acceptSeatSidecar?: boolean) => drainBufferedBatches({ store, keyRoot: ROOT, orgs: [A.orgId], dryRun: false, sleep: async () => {},
  decompress, ...(acceptSeatSidecar === undefined ? {} : { acceptSeatSidecar }) });
async function rawLines(store: ObjectStore): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(`${ROOT}/${A.orgId}/v1`), cursor);
    for (const i of page.items)
      if (String(i.key).includes("/raw/")) out.push(...dec.decode(decompress(await readAll(await store.get(objectKey(String(i.key)))))).split("\n").filter(Boolean));
    cursor = page.nextCursor;
  } while (cursor);
  return out;
}

const TWO: SeedGroup[] = [
  { source: "claude", session: "sess-1", records: [{ id: "a", rev: 1 }, { id: "b", rev: 1, accepted: false }, { id: "c", rev: 2 }] },
  { source: "codex", session: "sess-2", records: [{ id: "d", rev: 1 }] },
];
/** SEAT_PAGE's row for one stage, and the real function's answer for it (its return value, or that it raised). */
async function both(s: Seeded) {
  const row = (await db.query(SEAT_PAGE, [[s.key]])).rows[0] as Record<string, unknown>;
  let real: unknown, raised: string | undefined;
  try {
    real = (await db.query<{ s: unknown }>("SELECT sessions.ingest_lazy_stage_seats($1::uuid) AS s", [s.token])).rows[0]!.s;
  } catch (error) {
    raised = String((error as Error).message);
  }
  return { row, real, raised };
}
/** provenSeats over a SEAT_PAGE row, every accepted line kept. */
function seatsOf(s: Seeded, row: Record<string, unknown>) {
  const r = { stage_key: s.key, stage_sha256: s.sha256, claim_token: row.claim_token, group_ordinals: row.group_ordinals };
  const lines = s.groups.map((g) => g.records.flatMap((x, j) => (x.accepted === false ? [] : [j])));
  return provenSeats(r, [row], lines, s.groups.map((g, group) => ({ group, source: g.source, session: g.session })));
}
const expected = (s: Seeded) => s.seats.map((g, gi) => g.flatMap((seat, j) => (s.groups[gi]!.records[j]!.accepted === false ? [] : [{ seat, digest: sha(`${s.groups[gi]!.records[j]!.id}:${s.groups[gi]!.records[j]!.rev}`) }])));
const failCode = (f: () => unknown) => { try { f(); } catch (e) { return (e as Error).message; } return "none"; };

describe("SEAT_PAGE against sessions.ingest_lazy_stage_seats", () => {
  test("consistent state: the same snapshot as the real function, and provenSeats gives each accepted line its seat", async () => {
    const s = await seed(TWO);
    const { row, real, raised } = await both(s);
    expect(raised).toBeUndefined();
    expect(row).toMatchObject({ lazy: true, policy_ok: true, pre_ok: true, ords_n: 4, wanted_n: 4, seated_n: 4 });
    expect(row.snapshot).toEqual(real);
    expect(seatsOf(s, row)).toEqual(expected(s));
    // The losing line (ordinal 1) is in the snapshot as not accepted: it never gets a seat.
    expect((real as { records: { ord: number; bodyAccepted: boolean }[] }).records.find((x) => x.ord === 1)!.bodyAccepted).toBe(false);
    const withLoser = { ...s, groups: s.groups.map((g) => ({ ...g, records: g.records.map((r) => ({ ...r, accepted: true })) })) };
    expect(failCode(() => seatsOf(withLoser, row))).toBe("publication_seats_losing_body");
  });

  // Each change makes the real function raise; SEAT_PAGE must then never yield seats.
  const changes: [string, string, string][] = [
    ["policy expired", "UPDATE sessions.ingest_lazy_policy SET valid_until = now() - interval '1 second'", "seat_precondition"],
    ["PASS generation differs from the policy's", `UPDATE sessions.__96bulls_role_validation SET generation = '${hex("7")}'`, "seat_precondition"],
    ["no PASS generation", "UPDATE sessions.__96bulls_role_validation SET status = 'pending'", "seat_precondition"],
    ["clock epoch differs from the policy's", `UPDATE sessions.ingest_receipt_seat_clock SET control_epoch = '${hex("8")}'`, "seat_precondition"],
    ["clock source proof differs", `UPDATE sessions.ingest_receipt_seat_clock SET baseline = '{"sourceProofSha256":"${hex("9")}"}'`, "seat_precondition"],
    ["clock source proof missing", "UPDATE sessions.ingest_receipt_seat_clock SET baseline = '{}'", "seat_precondition"],
    ["admission missing", "DELETE FROM sessions.ingest_buffer_admission", "seat_precondition"],
    ["admission expired", "UPDATE sessions.ingest_buffer_admission SET valid_until = now() - interval '1 second'", "seat_precondition"],
    ["admission at another generation", `UPDATE sessions.ingest_buffer_admission SET schema_generation = '${hex("7")}'`, "seat_precondition"],
    ["stage not acknowledged", "UPDATE sessions.ingest_stage_intents SET receipt_state = 'preparing'", "seat_precondition"],
    ["no outbox row", "DELETE FROM sessions.ingest_buffer_outbox", "seat_precondition"],
    ["claim identity differs from the clock's", `UPDATE sessions.ingest_claim_attempts SET source_identity_sha256 = '${hex("9")}'`, "seat_precondition"],
    ["one decision missing", "DELETE FROM sessions.ingest_lazy_ack_decisions WHERE ord = 2", "seat_missing"],
    ["decision at another generation", `UPDATE sessions.ingest_lazy_ack_decisions SET generation = '${hex("7")}' WHERE ord = 0`, "seat_missing"],
    ["decision with another witness", `UPDATE sessions.ingest_lazy_ack_decisions SET source_witness_sha256 = '${hex("7")}' WHERE ord = 0`, "seat_missing"],
    ["decision at another fence epoch", "UPDATE sessions.ingest_lazy_ack_decisions SET fence_epoch = 1 WHERE ord = 0", "seat_missing"],
    ["seat's first digest changed", `UPDATE sessions.ingest_receipt_seats SET first_digest = '${hex("d")}' WHERE seat = (SELECT min(seat) FROM sessions.ingest_receipt_seats)`, "seat_missing"],
    ["seat key material of another revision", "UPDATE sessions.ingest_receipt_seats SET key_material = jsonb_set(key_material, '{8}', '\"9\"') WHERE seat = (SELECT min(seat) FROM sessions.ingest_receipt_seats)", "seat_missing"],
    ["seat at or below the clock's high water", "UPDATE sessions.ingest_receipt_seat_clock SET allocated_high_water = (SELECT min(seat) FROM sessions.ingest_receipt_seats)", "seat_missing"],
    ["native slot under another epoch", `UPDATE sessions.ingest_lazy_native_slots SET control_epoch = '${hex("8")}' WHERE record_id = 'a'`, "seat_missing"],
    ["seat above the clock's last seat", `UPDATE sessions.ingest_receipt_seat_clock SET last_seat = ${HIGH + 1}`, "publication_seats_seat"],
    ["a session listed twice in the claim's epochs", `UPDATE sessions.ingest_claim_attempts SET epochs = epochs || '[{"source":"codex","session_id":"sess-2","epoch":1,"reset_epoch":0}]'`, "seat_missing"],
  ];
  for (const [name, change, code] of changes)
    test(`refused when the real function raises: ${name} (${code})`, async () => {
      const s = await seed(TWO);
      expect((await both(s)).raised).toBeUndefined();
      await db.exec(`BEGIN; SET LOCAL session_replication_role = replica; ${change}; SET LOCAL session_replication_role = origin;`);
      try {
        const { row, raised } = await both(s);
        expect(raised).toBeDefined();
        const got = failCode(() => seatsOf(s, row));
        expect(got).toBe(code);
      } finally {
        await db.exec("ROLLBACK");
      }
    });

  test("no custody policy (not lazy): seat_not_lazy", async () => {
    const s = await seed(TWO);
    await db.exec("SET session_replication_role = replica; DELETE FROM sessions.ingest_lazy_policy; SET session_replication_role = origin;");
    const { row, raised } = await both(s);
    expect(raised).toBeDefined();
    expect(row).toMatchObject({ lazy: false, policy_ok: false });
    expect(failCode(() => seatsOf(s, row))).toBe("seat_not_lazy");
  });
});

describe("export: ExportOptions.seatSidecar", () => {
  test("off (default) and false: the same queries and the same v2 bytes, and SEAT_PAGE is never read", async () => {
    const out: { log: string[]; text: string }[] = [];
    let s: Seeded | undefined;
    for (const over of [{}, { seatSidecar: false }] as Partial<ExportOptions>[]) {
      const store = await scratch();
      s ??= await seed(TWO);
      const log: string[] = [];
      expect(await run(store, over, log)).toMatchObject({ live: 1, written: 1, failed: 0 });
      out.push({ log, text: await readText(store, s.key + ACCEPTED_SUFFIX) });
    }
    // Each scratch store has its own fence instance id; everything else must match byte for byte.
    const sameFence = (t: string) => t.replace(/"instance":"[0-9a-f-]{36}"/, '"instance":"x"');
    expect(out[1]!.log).toEqual(out[0]!.log);
    expect(sameFence(out[1]!.text)).toBe(sameFence(out[0]!.text));
    expect(sameFence(out[0]!.text)).not.toBe(out[0]!.text);
    expect(out[0]!.log.some((l) => l === SEAT_PAGE.trim())).toBe(false);
    expect(parseAcceptedMarker(out[0]!.text)).toMatchObject({ v: 2, live: true, kept: [[0, 2], [0]] });
    expect(out[0]!.text).not.toContain("seats");
    // On: the same queries plus exactly one SEAT_PAGE read per verdict page, inside the same read-only transaction.
    const store = await scratch();
    const log: string[] = [];
    await run(store, { seatSidecar: true }, log);
    expect(log.filter((l) => l !== SEAT_PAGE.trim())).toEqual(out[0]!.log);
    const at = log.indexOf(SEAT_PAGE.trim());
    expect(at).toBeGreaterThan(0);
    expect(log[at - 1]!.startsWith("WITH pol")).toBe(true);
    expect(log[at + 1]).toBe("ROLLBACK");
  });

  test("a non-boolean opt-in throws export_seat_sidecar_invalid", async () => {
    const store = await scratch();
    await expect(run(store, { seatSidecar: "yes" as unknown as boolean })).rejects.toThrow("export_seat_sidecar_invalid");
  });

  test("on: v3 with one seat per kept line; the not-accepted line is neither kept nor seated", async () => {
    const store = await scratch();
    const s = await seed(TWO);
    expect(await run(store, { seatSidecar: true })).toMatchObject({ live: 1, written: 1, failed: 0, orgs: { complete: 1 } });
    const v = parseAcceptedMarkerV3(await readText(store, s.key + ACCEPTED_SUFFIX))!;
    expect(v).toMatchObject({ v: 3, live: true, kept: [[0, 2], [0]], stage_sha256: s.sha256 });
    expect(v.seats).toEqual(expected(s));
    expect(v.seats.flat().map((x) => x.seat)).not.toContain(s.seats[0]![1]);
  });

  test("on, stage not live: v3 with no seats", async () => {
    const store = await scratch();
    const s = await seed(TWO);
    await db.query("INSERT INTO sessions.ingest_fence_versions (org_id, workspace_id, user_id, origin_id, source, session_id, epoch) VALUES ($1,$2,$3,$4,'claude','sess-1',1), ($1,$2,$3,$4,'codex','sess-2',1)",
      [A.orgId, A.workspaceId, A.userId, ORIGIN]);
    expect(await run(store, { seatSidecar: true })).toMatchObject({ live: 0, not_live: 1, written: 1 });
    expect(parseAcceptedMarkerV3(await readText(store, s.key + ACCEPTED_SUFFIX))).toMatchObject({ v: 3, live: false, kept: [], seats: [] });
  });

  // A live stage whose seats SQL does not prove gets no sidecar, a failed_codes count, and an incomplete org.
  const refusals: [string, string, string][] = [
    ["admission missing", "DELETE FROM sessions.ingest_buffer_admission", "seat_precondition"],
    ["clock source proof differs", `UPDATE sessions.ingest_receipt_seat_clock SET baseline = '{"sourceProofSha256":"${hex("9")}"}'`, "seat_precondition"],
    ["seat's first digest changed", `UPDATE sessions.ingest_receipt_seats SET first_digest = '${hex("d")}' WHERE seat = (SELECT min(seat) FROM sessions.ingest_receipt_seats)`, "seat_missing"],
    ["seat key material of another revision", "UPDATE sessions.ingest_receipt_seats SET key_material = jsonb_set(key_material, '{8}', '\"9\"') WHERE seat = (SELECT min(seat) FROM sessions.ingest_receipt_seats)", "seat_missing"],
    ["seat above the clock's last seat", `UPDATE sessions.ingest_receipt_seat_clock SET last_seat = ${HIGH + 1}`, "publication_seats_seat"],
  ];
  for (const [name, change, code] of refusals)
    test(`on, refused: ${name} (${code})`, async () => {
      const store = await scratch();
      const s = await seed(TWO);
      await db.exec(`SET session_replication_role = replica; ${change}; SET session_replication_role = origin;`);
      const summary = await run(store, { seatSidecar: true });
      expect(summary).toMatchObject({ failed: 1, written: 0, failed_codes: { [code]: 1 }, orgs: { complete: 0 } });
      expect(await exists(store, s.key + ACCEPTED_SUFFIX)).toBe(false);
      // The same stage with the opt-in off still gets its v2 verdict (the seats are not read).
      expect(await run(store)).toMatchObject({ failed: 0, written: 1 });
    });
});

describe("export then drain", () => {
  test("seated lines with DrainOptions.acceptSeatSidecar; refused (accepted_mismatch) without it", async () => {
    const store = await scratch();
    const s = await stageInBoth(store, [{ source: "claude", session: "sess-1", records: [{ id: "a", rev: 1 }, { id: "b", rev: 1, accepted: false }] }]);
    expect(await run(store, { seatSidecar: true })).toMatchObject({ live: 1, written: 1, failed: 0 });
    const off = await drain(store);
    expect(off.objects).toMatchObject({ drained: 0, accepted_mismatch: 1 });
    expect(await rawLines(store)).toEqual([]);
    const on = await drain(store, true);
    expect(on.objects).toMatchObject({ drained: 1, accepted_mismatch: 0 });
    expect(on.lines).toMatchObject({ written: 1, not_kept: 1 });
    const lines = await rawLines(store);
    expect(lines.length).toBe(1);
    expect(lines[0]).toBe(objectLine("a").slice(0, -1) + `,"accepted_seq":"${s.seats[0]![0]}"}`);
  });
});
