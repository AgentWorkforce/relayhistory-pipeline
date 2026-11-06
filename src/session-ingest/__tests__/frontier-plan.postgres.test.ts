import { describe, expect, test } from "bun:test";
import pg from "pg";
import { createS3Store, objectStoreManifestReader } from "../../shared/vendor.ts";
import { signRequest } from "../../../packages/object-store/src/sigv4.ts";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import { xmlElements, xmlText } from "../../../packages/object-store/src/xml.ts";
import { writeBatchObjects } from "../writer.ts";
import { prepareDelivery, deliveryAccount } from "../vendor/delivery/prepare.ts";
import { DELIVERY_MAPPING_VERSION, type HistoryExportBatch } from "../vendor/delivery/delivery-contracts.ts";
import {
  FRONTIER_CAPTURE_PROTOCOL,
  evaluateCanonicalFrontier,
  frontierCaptureId,
  frontierExportRootFromSeedPointer,
  frontierExportRootSha256,
  frontierScanItemsSha256,
  frontierSha256,
  type CanonicalFrontierCapture,
  type FrontierAuthority,
  type FrontierOriginCapture,
  type FrontierScanEvidence,
  type FrontierScanKind,
} from "../frontier-plan.ts";
import type { SeedPointer } from "../seed-proof.ts";

const url = process.env.FRONTIER_TEST_PG_URL;
const endpoint = process.env.FRONTIER_TEST_S3_ENDPOINT;
const bucket = process.env.FRONTIER_TEST_S3_BUCKET;
const accessKeyId = process.env.FRONTIER_TEST_S3_ACCESS_KEY;
const secretAccessKey = process.env.FRONTIER_TEST_S3_SECRET_KEY;
const enabled = !!(url && endpoint && bucket && accessKeyId && secretAccessKey);
const H = (value: string) => frontierSha256(value);
const adapters: Record<FrontierScanKind, string> = {
  "origin-inventory": "legacy-origin-inventory-v1", "record-maxima": "legacy-record-maxima-v1",
  "export-roots": "ingest-seed-s3-export-roots-v1", "privacy-controls": "ordered-privacy-controls-v1",
  "receipt-decisions": "first-ack-seat-decisions-v1",
};

describe.skipIf(!enabled)("frontier capture on real PostgreSQL 17 and versioned S3 fixture", () => {
  test("bounded read-only scan, actual manifest version, closure race and idempotency", async () => {
    const pool = new pg.Pool({ connectionString: url!, max: 3, application_name: "frontier-plan-proof" });
    const suffix = crypto.randomUUID().replaceAll("-", "");
    const schema = `frontier_${suffix}`;
    const tenant = { orgId: `frontier-${suffix.slice(0, 12)}`, workspaceId: "workspace-fixture", userId: "user-fixture", originId: "origin-fixture" };
    const keyRoot = `frontierproof${suffix.slice(0, 8)}`;
    const store = createS3Store({ endpoint: endpoint!, bucket: bucket!, region: "us-east-1", credentials: { accessKeyId: accessKeyId!, secretAccessKey: secretAccessKey! } });
    const latestVersion = async (key: string): Promise<string> => {
      const target = new URL(`${endpoint}/${bucket}?versions=&prefix=${encodeURIComponent(key)}`);
      const headers = new Headers();
      await signRequest({ method: "GET", url: target, headers, payloadSha256: await sha256Hex(new Uint8Array()), region: "us-east-1", credentials: { accessKeyId: accessKeyId!, secretAccessKey: secretAccessKey! } });
      const response = await fetch(target, { headers, signal: AbortSignal.timeout(5000) });
      if (!response.ok) throw new Error(`fixture_versions_${response.status}`);
      const xml = await response.text();
      const version = xmlElements(xml, "Version").find(item => xmlText(item, "Key") === key && xmlText(item, "IsLatest") === "true");
      const value = version ? xmlText(version, "VersionId") : undefined;
      if (!value || value === "null") throw new Error("fixture_version_missing");
      return value;
    };
    try {
      await pool.query(`CREATE SCHEMA ${schema}`);
      await pool.query(`CREATE TABLE ${schema}.authority(source_identity text NOT NULL,source_generation text NOT NULL,source_head text NOT NULL,control_epoch text NOT NULL,privacy_baseline text NOT NULL,privacy_version bigint NOT NULL)`);
      await pool.query(`CREATE TABLE ${schema}.records(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,record_id text NOT NULL,revision bigint NOT NULL,revision_id text NOT NULL,digest text NOT NULL,source text NOT NULL,session_id text,operation text NOT NULL,PRIMARY KEY(org_id,workspace_id,origin_id,record_id))`);
      const baseAuthority: FrontierAuthority = { sourceIdentitySha256: H("pg-source-identity"), sourceGenerationSha256: H("pg-source-generation"), sourceHead: "b".repeat(40), controlEpoch: H("pg-control"), privacyBaselineSha256: H("pg-privacy"), privacyVersion: "19" };
      await pool.query(`INSERT INTO ${schema}.authority VALUES($1,$2,$3,$4,$5,19)`, [baseAuthority.sourceIdentitySha256, baseAuthority.sourceGenerationSha256, baseAuthority.sourceHead, baseAuthority.controlEpoch, baseAuthority.privacyBaselineSha256]);

      const batch: HistoryExportBatch = {
        schema_version: 1, origin_id: tenant.originId, batch_id: "fixture-batch", job_id: "fixture-job", generation: 1,
        destination_id: "fixture", instance_id: "fixture", account_id: await deliveryAccount(tenant), mapping_version: DELIVERY_MAPPING_VERSION,
        records: [{ schema_version: 1, origin_id: tenant.originId, revision_id: "revision-one", record_id: "record-one", revision: 1,
          kind: "session_event", source: "claude", session_id: "session-one", operation: "upsert", payload: { text: "synthetic private fixture", ts_ms: 1_762_406_400_000 } }],
      };
      const prepared = await prepareDelivery(tenant, batch);
      const written = await writeBatchObjects({ store, keyRoot, now: () => new Date("2026-11-06T04:00:00.000Z"), sleep: async () => {}, random: () => 0 }, { ...tenant, originId: tenant.originId, batchDigest: prepared.batchDigest, records: prepared.records });
      const manifest = written.manifests[0]!;
      const manifestVersion = await latestVersion(manifest.key);
      const read = await objectStoreManifestReader(store, { provider: "s3", bucket }).read(manifest.key);
      expect(read?.manifest.raw.content_sha256).toBe(manifest.manifest.raw.content_sha256);
      const pointer: SeedPointer = {
        org_id: tenant.orgId, workspace_id: tenant.workspaceId, user_id: tenant.userId, harness: "claude", session_id: "session-one",
        machine_id: written.machineId, bucket: bucket!, storage_provider: "s3", manifest_key: manifest.key,
        revision: String(manifest.manifest.revision), content_sha256: manifest.manifest.raw.content_sha256, raw_format: manifest.manifest.raw.format,
        capture: { pointer_id: H("fixture-pointer"), object_version: manifestVersion },
      };
      const root = frontierExportRootFromSeedPointer(pointer, { originId: tenant.originId, keyRoot: `${keyRoot}/${tenant.orgId}`, versionId: manifestVersion, recordCount: 1, inventoryComplete: true, versioning: "Enabled" });
      const record = prepared.records[0]!;
      await pool.query(`INSERT INTO ${schema}.records VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, [tenant.orgId, tenant.workspaceId, tenant.userId, tenant.originId, record.record_id, record.revision, record.revision_id, record.digest, record.source, record.session_id, record.operation]);

      const readAuthority = async (): Promise<FrontierAuthority> => {
        const row = (await pool.query(`SELECT source_identity,source_generation,source_head,control_epoch,privacy_baseline,privacy_version::text FROM ${schema}.authority`)).rows[0];
        return { sourceIdentitySha256: row.source_identity, sourceGenerationSha256: row.source_generation, sourceHead: row.source_head, controlEpoch: row.control_epoch, privacyBaselineSha256: row.privacy_baseline, privacyVersion: row.privacy_version };
      };
      const makeScan = (kind: FrontierScanKind, items: unknown, opening: FrontierAuthority, closing: FrontierAuthority, snapshot: string, pagesRead = 2): FrontierScanEvidence => ({
        kind, adapter: adapters[kind], querySha256: H(`reviewed-query:${kind}`), snapshotSha256: H(snapshot),
        startedAt: "2026-11-06T04:00:00.000Z", finishedAt: "2026-11-06T04:00:03.000Z",
        pageLimit: 1, maxPages: 100, pagesRead, itemCount: Array.isArray(items) ? items.length : 1,
        byteCount: Buffer.byteLength(JSON.stringify(items)), checkpointCount: Math.max(0, pagesRead - 1),
        pageChainSha256: frontierSha256({ kind, pagesRead, items }), itemsSha256: frontierSha256(items),
        openingStateSha256: frontierSha256(opening), closingStateSha256: frontierSha256(closing), terminalCursor: null, complete: true,
      });
      const capture = async (between?: () => Promise<void>): Promise<CanonicalFrontierCapture> => {
        const opening = await readAuthority();
        const client = await pool.connect();
        let rows: Array<Record<string, unknown>> = [], snapshot = "";
        try {
          await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
          snapshot = (await client.query("SELECT txid_current_snapshot()::text snapshot")).rows[0].snapshot;
          let after = "", pages = 0;
          for (;;) {
            const page = (await client.query(`SELECT org_id,workspace_id,user_id,origin_id,record_id,revision::text,revision_id,digest,source,session_id,operation FROM ${schema}.records WHERE record_id>$1 ORDER BY record_id COLLATE \"C\" LIMIT 1`, [after])).rows;
            pages++;
            if (!page.length) break;
            rows.push(...page); after = page.at(-1)!.record_id;
            if (pages >= 100) throw new Error("fixture_scan_bound");
          }
          expect(pages).toBe(2);
          await client.query("COMMIT");
        } catch (error) { await client.query("ROLLBACK").catch(() => {}); throw error; }
        finally { client.release(); }
        if (between) await between();
        const closing = await readAuthority();
        const maxima = rows.map(row => ({
          tenant: { ...tenant }, recordId: String(row.record_id), revision: String(row.revision), revisionId: String(row.revision_id), digest: String(row.digest),
          source: String(row.source), sessionId: row.session_id === null ? null : String(row.session_id), operation: row.operation as "upsert" | "delete",
          authority: "seed-proof" as const, exportRootSha256: frontierExportRootSha256(root),
        }));
        const origin: FrontierOriginCapture = { tenant: { ...tenant }, opening, closing, sourceRecordCount: rows.length, scans: [], exportRoots: [root], recordMaxima: maxima, receiptDecisions: [] };
        origin.scans = [
          makeScan("record-maxima", [...maxima], opening, closing, snapshot),
          makeScan("export-roots", [root], opening, closing, `s3:${manifestVersion}`),
          makeScan("privacy-controls", { opening, closing }, opening, closing, snapshot, 2),
          makeScan("receipt-decisions", [], opening, closing, snapshot, 1),
        ];
        const value: CanonicalFrontierCapture = {
          protocol: FRONTIER_CAPTURE_PROTOCOL, captureId: "0".repeat(64), startedAt: "2026-11-06T04:00:00.000Z", capturedAt: "2026-11-06T04:00:04.000Z",
          scope: { orgId: tenant.orgId, workspaceId: tenant.workspaceId }, target: { bucket: "canonical-target", region: "us-east-1", keyRoot: "96bulls", schemaGeneration: H("fixture-generation") },
          expectedOrigins: [{ userId: tenant.userId, originId: tenant.originId }], inventoryScan: {} as FrontierScanEvidence, origins: [origin],
        };
        const inventoryOpening = frontierSha256(value.origins.map(item => ({ tenant: item.tenant, authority: item.opening })));
        const inventoryClosing = frontierSha256(value.origins.map(item => ({ tenant: item.tenant, authority: item.closing })));
        value.inventoryScan = makeScan("origin-inventory", value.expectedOrigins, opening, closing, snapshot, 2);
        value.inventoryScan.openingStateSha256 = inventoryOpening; value.inventoryScan.closingStateSha256 = inventoryClosing;
        // Recompute using the public protocol helpers, not the fixture's incidental array order.
        value.inventoryScan.itemsSha256 = frontierScanItemsSha256("origin-inventory", value);
        for (const scan of origin.scans) scan.itemsSha256 = frontierScanItemsSha256(scan.kind, origin);
        value.captureId = frontierCaptureId(value);
        return value;
      };

      const full = await capture();
      const now = new Date("2026-11-06T04:00:05.000Z");
      const first = evaluateCanonicalFrontier(full, { now });
      const retry = evaluateCanonicalFrontier(structuredClone(full), { now });
      expect(first).toEqual(retry);
      expect(first).toMatchObject({ status: "ready", frontiers: [{ org_id: tenant.orgId, origin_id: tenant.originId }] });
      expect(evaluateCanonicalFrontier(full, { now: new Date("2026-11-06T04:20:00.000Z") })).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["capture_stale"]) });

      const incomplete = structuredClone(full);
      incomplete.origins[0]!.scans[0]!.complete = false; incomplete.origins[0]!.scans[0]!.terminalCursor = "bounded-stop";
      incomplete.captureId = frontierCaptureId(incomplete);
      expect(evaluateCanonicalFrontier(incomplete, { now })).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["scan_incomplete"]) });

      const unknown = structuredClone(full);
      unknown.origins[0]!.recordMaxima[0]!.revision = null;
      unknown.origins[0]!.scans.find(scan => scan.kind === "record-maxima")!.itemsSha256 = frontierScanItemsSha256("record-maxima", unknown.origins[0]!);
      unknown.captureId = frontierCaptureId(unknown);
      expect(evaluateCanonicalFrontier(unknown, { now })).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["unknown_legacy_fact"]) });

      const overlap = structuredClone(full);
      const maximum = overlap.origins[0]!.recordMaxima[0]!; maximum.authority = "receipt-seat";
      overlap.origins[0]!.receiptDecisions = [
        { tenant: { ...tenant }, recordId: maximum.recordId, revision: maximum.revision!, revisionId: maximum.revisionId!, source: maximum.source!, sessionId: maximum.sessionId, seat: "41", bodyAccepted: true, candidateDigest: maximum.digest!, firstDigest: maximum.digest! },
        { tenant: { ...tenant }, recordId: maximum.recordId, revision: maximum.revision!, revisionId: "losing-revision", source: "codex", sessionId: "overlap-session", seat: "41", bodyAccepted: false, candidateDigest: H("losing-body"), firstDigest: maximum.digest! },
      ];
      const receiptScan = overlap.origins[0]!.scans.find(scan => scan.kind === "receipt-decisions")!;
      overlap.origins[0]!.scans.find(scan => scan.kind === "record-maxima")!.itemsSha256 = frontierScanItemsSha256("record-maxima", overlap.origins[0]!);
      receiptScan.itemCount = 2; receiptScan.pagesRead = 3; receiptScan.checkpointCount = 2;
      receiptScan.itemsSha256 = frontierScanItemsSha256("receipt-decisions", overlap.origins[0]!);
      overlap.captureId = frontierCaptureId(overlap);
      expect(evaluateCanonicalFrontier(overlap, { now })).toMatchObject({ status: "ready" });
      overlap.origins[0]!.receiptDecisions[1]!.bodyAccepted = true;
      overlap.origins[0]!.receiptDecisions[1]!.firstDigest = overlap.origins[0]!.receiptDecisions[1]!.candidateDigest;
      receiptScan.itemsSha256 = frontierScanItemsSha256("receipt-decisions", overlap.origins[0]!);
      overlap.captureId = frontierCaptureId(overlap);
      expect(evaluateCanonicalFrontier(overlap, { now })).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["receipt_decision_conflict"]) });

      const raced = await capture(async () => { await pool.query(`UPDATE ${schema}.authority SET privacy_version=20`); });
      expect(evaluateCanonicalFrontier(raced, { now })).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["source_race", "privacy_changed"]) });
      await pool.query(`UPDATE ${schema}.authority SET privacy_version=19`);

      const foreign = await capture();
      foreign.origins[0]!.exportRoots[0]!.tenant.workspaceId = "foreign-workspace";
      foreign.origins[0]!.scans.find(scan => scan.kind === "export-roots")!.itemsSha256 = frontierScanItemsSha256("export-roots", foreign.origins[0]!);
      foreign.captureId = frontierCaptureId(foreign);
      expect(evaluateCanonicalFrontier(foreign, { now })).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["tenant_mismatch"]) });
    } finally {
      await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).catch(() => {});
      await pool.end();
    }
  }, 30_000);
});
