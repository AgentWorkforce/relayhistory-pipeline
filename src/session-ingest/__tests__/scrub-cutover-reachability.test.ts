/**
 * Negative reachability proof for the relayscrubber 0.5.0-alpha.9 cutover
 * (docs/scrub-engine.md, "Cutover: pinned relayscrubber 0.5.0-alpha.9").
 *
 * Every TypeScript scrub/seal entry point (lib/scrub.ts and lib/pipeline/raw-chunks.ts)
 * is replaced in place by a counting spy. Then each in-scope ingress path (ingest
 * writer, publication image, routes/pipeline HTTP, neon-s3-sync export) is driven end
 * to end against scratch stores with the sealer the production constructors install
 * (`routedRawChunkSealerDeps()`):
 *
 * - cutover = the DEFAULT: SCRUB_ENGINE, SCRUB_ENGINE_REQUIRED and READER_MAX_RULESET
 *   all unset, no TS capability granted (the reset clears the test preload's grant), with
 *   the REAL vendored engine: no TS entry point runs and every sealed object carries scrub
 *   version 10 and the crate profile ("Default flip" in docs/scrub-engine.md);
 * - SCRUB_ENGINE=ts in a process without a TS capability (a Node self-host entry): every
 *   driver refuses with scrub_engine_ts_not_allowed before sealing, zero TS calls, no raw
 *   object;
 * - positive control: a process holding the hosted TS capability with the knob unset
 *   (what the hosted Worker/Lambda entries declare) seals through the TS path (the spies
 *   count) with scrub version 8 and no profile, exactly as before the flip;
 * - the production call sites that do NOT install the routed sealer (publication canary,
 *   the default `pipelineRoutes` mounted by packages/pipeline/src/app.ts, the ingest
 *   Worker's `deliverDepsFor`) are driven under the default too: each refuses with
 *   `seal_path_not_routed` at the seam, makes zero TS scrub/seal calls and writes no raw
 *   object ("unwired, refuses" in docs/scrub-engine.md);
 * - a source scan proves the lists are complete: every `?? sealRawChunk` fallback and
 *   every constructor of DeliverDeps / PipelineRouteDeps / PublicationJobDeps / a write
 *   target is classified as wired (spreads routedRawChunkSealerDeps()) or unwired-refuses.
 *
 * With SCRUB_REACHABILITY_REPORT=<absolute path> it writes a JSON report
 * (scripts/scrub-cutover-reachability.ts runs this file and checks that report).
 */
import { afterAll, afterEach, beforeAll, describe, expect, mock, spyOn, test } from 'bun:test';
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { Hono } from 'hono';
import { zstdDecompressSync } from 'node:zlib';
import * as rawChunks from '../../../packages/pipeline/src/lib/pipeline/raw-chunks.ts';
import * as tsScrub from '../../../packages/pipeline/src/lib/scrub.ts';
import { createPipelineRoutes } from '../../../packages/pipeline/src/routes/pipeline.ts';
import { createApp } from '../../../packages/pipeline/src/app.ts';
import * as scrubEngineSeam from '../../../packages/pipeline/src/lib/scrub-engine/index.ts';
import type { AppEnv } from '../../../packages/pipeline/src/env.ts';
import { readAll, objectKey, type ObjectStore } from '../../../packages/object-store/src/index.ts';
import {
  activeScrubEngine, initScrubEngine, resetScrubEngineForTests, routedRawChunkSealerDeps, sealRawChunk as seam, scrubEngineReady,
} from '../../../packages/pipeline/src/lib/scrub-engine/index.ts';
import { declareScrubTsCapability, scrubTsCapabilities } from '../../../packages/pipeline/src/lib/scrub-engine/ts-capability.ts';
import { writeGroups } from '../writer.ts';
import { preparePublicationImage, writePublicationImage } from '../publication-image.ts';
import { parseReceiptSeatSnapshot } from '../publication-seats.ts';
import { publishBufferedStage } from '../publication-job.ts';
import { prepareBufferedStage, putBufferedStage } from '../staging.ts';
import { deliverDepsFor } from '../worker.ts';
import { exportUnit } from '../../migration/neon-s3-sync/export-session.ts';
import { scratchStore } from './support.ts';

const KEYS = ['SCRUB_ENGINE', 'SCRUB_ENGINE_REQUIRED', 'READER_MAX_RULESET', 'SCRUB_BINDGEN_DIR', 'S3X_OBLIGATIONS'] as const;
/** The default: no scrub env at all and no TS capability (resetScrubEngineForTests clears grants). */
function unsetDefault() {
  for (const k of KEYS) delete process.env[k];
  resetScrubEngineForTests();
  expect(scrubTsCapabilities()).toStrictEqual([]);
}
const PROFILE = 'relayhistory-secrets-v10';
const SECRET = 'AKIAIOSFODNN7EXAMPLE';
const enc = new TextEncoder(), HEX = 'a'.repeat(64), OTHER = 'b'.repeat(64);

// ---- spies: every TS scrub / seal entry point, replaced in place (live bindings).
const TS_ENTRY_POINTS = {
  'raw-chunks.ts': ['sealRawChunk', 'scrubRecords', 'scrubLong', 'scrubStringToken'],
  'scrub.ts': ['scrubText', 'scrubJson', 'scrubRecord', 'scrubNumber'],
} as const;
const counts: Record<string, number> = {};
function spied<M extends Record<string, unknown>>(file: string, real: M, names: readonly string[]): M {
  const out: Record<string, unknown> = { ...real };
  for (const name of names) {
    const fn = real[name] as (...a: unknown[]) => unknown;
    counts[`${file}#${name}`] = 0;
    out[name] = (...a: unknown[]) => { counts[`${file}#${name}`]!++; return fn(...a); };
  }
  return out as M;
}
// Plain snapshots of the real exports, taken before any mock, so afterAll can put them back
// (module mocks are process-wide in a multi-file `bun test` run).
const RAW_CHUNKS = { ...rawChunks }, TS_SCRUB = { ...tsScrub };
mock.module('../../../packages/pipeline/src/lib/pipeline/raw-chunks.ts', () => spied('raw-chunks.ts', RAW_CHUNKS, TS_ENTRY_POINTS['raw-chunks.ts']));
mock.module('../../../packages/pipeline/src/lib/scrub.ts', () => spied('scrub.ts', TS_SCRUB, TS_ENTRY_POINTS['scrub.ts']));
const tsCalls = () => Object.values(counts).reduce((a, b) => a + b, 0);
const resetCounts = () => { for (const k of Object.keys(counts)) counts[k] = 0; };

let saved: Record<string, string | undefined>;
const cleanup: (() => void)[] = [];
beforeAll(() => { saved = Object.fromEntries(KEYS.map((k) => [k, process.env[k]])); });
afterEach(() => {
  for (const k of KEYS) if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
  resetScrubEngineForTests();
  for (const f of cleanup.splice(0)) f();
});
function scratch() { const s = scratchStore(); cleanup.push(s.done); return s.store; }

const line = JSON.stringify({ kind: 'session_event', origin_id: 'origin-a', record_id: 'native-r', revision: '1', operation: 'upsert', received_at: '2026-11-04T01:00:00Z', payload: { text: `VISIBLE key ${SECRET}`, ts_ms: 1762218000000 } });
function publication(stage: { key: string; sha256: string; bytes: number; groupOrdinals: number[][] } = { key: 'unused', sha256: HEX, bytes: 1, groupOrdinals: [[0]] }) {
  const record = { ord: 0, source: 'claude', session_id: 'session:1', record_id: 'native-r', revision_id: 'revision-1', revision: 1, digest: HEX };
  const receiptSeats = parseReceiptSeatSnapshot({ controlEpoch: HEX, sourceIdentitySha256: OTHER, allocatedHighWater: '9007199254740993', lastAssignedSeat: '9007199254740999', records: [{ ord: 0, seat: '9007199254740994', bodyAccepted: true, candidateDigest: HEX, firstDigest: HEX, resetEpoch: '0', fenceEpoch: '2' }] }, [record], stage);
  const generation = '12345678-1234-1234-1234-123456789abc', attemptToken = 'abcdefab-1234-1234-1234-123456789abc';
  const head = { groupIndex: 0, ordinals: [0], generation, prefix: `96bulls/org-a/upload-generations/g=${generation}`, headVersion: '1', revision: '1', previousKey: null, previousRawSha256: null, basePointer: null, source: 'claude', sessionId: 'session:1', physicalWorkspace: 'ws-a', physicalSource: 'claude', physicalSession: 'session_1', machineId: 'srv_fixture', fenceEpoch: '2', resetEpoch: '0' };
  const ticket = { schemaGeneration: HEX, attemptToken, leaseToken: '11111111-1111-1111-1111-111111111111', leaseUntil: '2026-11-05T00:05:00.000000Z', stageToken: '22222222-2222-2222-2222-222222222222', stageVersion: '1', outboxVersion: '1', tenant: { orgId: 'org-a', workspaceId: 'ws-a', userId: 'user-a' }, originId: 'origin-a', batchId: 'batch-a', batchDigest: HEX, claimToken: '33333333-3333-3333-3333-333333333333', stage, records: [record], receiptSeats, acceptedOrdinals: [0], heads: [head] };
  const decoded = { header: { v: 1 as const, publication_state: 'buffered' as const, claim_token: ticket.claimToken, batch_digest: HEX, received_at: '2026-11-04T01:00:00.000Z', org_id: 'org-a', workspace_id: 'ws-a', origin_id: 'origin-a', user_id: 'user-a', groups: [['claude', 'session:1'] as [string, string]] }, groups: [{ source: 'claude', sessionId: 'session:1', lines: [line] }], spec: stage };
  return { ticket, head, decoded, target: { bucket: 'fixture', region: 'us-east-1', keyRoot: '96bulls', generation: HEX, provider: 's3' as const } };
}
async function packed(s: ObjectStore, key: string) {
  const o = await s.getObject(objectKey(key));
  return { text: new TextDecoder().decode(zstdDecompressSync(await readAll(o.body))), meta: o.meta.metadata };
}

type Caller = 'writer' | 'publication-image' | 'routes/pipeline' | 'export';
const CALLERS: Caller[] = ['writer', 'publication-image', 'routes/pipeline', 'export'];
/** Drive one ingress path end to end with the deps a production constructor installs; returns the sealed raw key. */
async function drive(caller: Caller, s: ObjectStore): Promise<string> {
  const deps = routedRawChunkSealerDeps();
  if (caller === 'writer') {
    const r = await writeGroups({ store: s, keyRoot: '96bulls', ...deps }, { orgId: 'org-a', workspaceId: 'ws-a', userId: 'user-a', originId: 'origin-a', batchDigest: HEX }, [{ source: 'claude', sessionId: 'session:1', lines: [line] }], new Date('2026-11-04T01:00:00Z'));
    return r.parts[0]!.chunkKey;
  }
  if (caller === 'publication-image') {
    const p = publication(), signal = new AbortController().signal;
    const image = await preparePublicationImage(p.ticket as never, p.head as never, p.decoded as never, p.target, s, signal, undefined, deps.rawChunkSealer);
    await writePublicationImage(p.ticket as never, image, { prepare: async () => {}, register: async () => {} } as never, s, signal, p.target);
    return image.newChunk.spec.key;
  }
  if (caller === 'routes/pipeline') {
    const app = new Hono<AppEnv>();
    app.use('*', async (c, next) => { c.set('auth' as never, { orgId: 'org-a', workspaceId: 'ws-a', scopes: ['rth:sync'] } as never); c.set('correlationId', 'corr'); await next(); });
    app.route('/v1', createPipelineRoutes({ ...deps, rawStore: () => ({ store: s, provider: 's3', bucket: 'fixture' }), sink: () => ({ record: async () => {} }) as never, deadLetters: () => ({}) as never, send: async () => {} }));
    const response = await app.request('/v1/pipeline/chunks?machine=m&harness=claude&session=s&date=2026-11-04&seq=0&epoch=0&offset=0&format=ndjson', { method: 'POST', body: line + '\n' }, { PIPELINE_ORG_ID: 'org-a' });
    expect(response.status).toBe(201);
    return ((await response.json()) as { key: string }).key;
  }
  process.env.S3X_OBLIGATIONS = 'off';
  const r = await exportUnit({ store: s, keyRoot: '96bulls', grantId: 'test-grant', ...deps }, { orgId: 'org-a', workspaceId: 'ws-a', source: 'claude', sessionId: 'session:1', firstMs: Date.parse('2026-11-04T01:00:00Z'), lastMs: Date.parse('2026-11-04T01:01:00Z'), machineIds: [], totals: { event_count: '1', models: [] } } as never, { fenced: async () => false, async *lines() { yield { machine: 'machine', bytes: enc.encode(line + '\n'), userId: 'user-a' }; } } as never);
  return r.manifests[0]!.referenced.find((k: string) => k.includes('/raw/'))!;
}

interface Row { caller: Caller | Unwired; mode: 'cutover' | 'control' | 'unwired' | 'ts-refused'; routedSealer: boolean; tsCalls: Record<string, number>; scrubVersion?: string | undefined; scrubProfile?: string | undefined; secretInSealedBytes?: boolean; seamRefusals?: string[]; outcome?: string; rawObjectsWritten?: number }
const rows: Row[] = [];

async function run(caller: Caller, mode: Row['mode']) {
  const s = scratch();
  resetCounts();
  const routedSealer = routedRawChunkSealerDeps().rawChunkSealer !== undefined;
  const key = await drive(caller, s);
  const o = await packed(s, key);
  const row: Row = { caller, mode, routedSealer, tsCalls: { ...counts }, scrubVersion: o.meta['scrub-version'], scrubProfile: o.meta['scrub-profile'], secretInSealedBytes: o.text.includes(SECRET) };
  rows.push(row);
  return { row, text: o.text };
}

describe('default (scrub env unset, no TS capability; real vendored alpha.9 engine)', () => {
  for (const caller of CALLERS)
    test(`${caller}: the TS scrub/sealer is never invoked; the sealed object is the crate's (version 10)`, async () => {
      unsetDefault();
      await initScrubEngine(); // production path: process.env, default vendored dir, default glue importer
      expect(activeScrubEngine().kind).toBe('wasm-bindgen');
      expect(scrubEngineReady()).toBe(true);
      const { row, text } = await run(caller, 'cutover');
      expect(row.routedSealer).toBe(true);
      expect(row.tsCalls).toStrictEqual(Object.fromEntries(Object.keys(counts).map((k) => [k, 0])));
      expect(row.scrubVersion).toBe('10');
      expect(row.scrubProfile).toBe(PROFILE);
      expect(text).toContain('[REDACTED]');
      expect(row.secretInSealedBytes).toBe(false);
    });

  test('an unwired call site (no rawChunkSealer) refuses seal_path_not_routed and still never reaches TS', async () => {
    unsetDefault();
    await initScrubEngine();
    resetCounts();
    const s = scratch();
    await expect(seam(s, { location: { prefix: '96bulls/o', workspaceId: 'w', machineId: 'm', harness: 'h', sessionId: 's', startDate: '2026-11-10' }, format: 'ndjson', seq: 0, epoch: 0, startOffset: 0, source: enc.encode(line + '\n') })).rejects.toMatchObject({ reason: 'seal_path_not_routed' });
    expect(tsCalls()).toBe(0);
    expect((await s.list(objectKey('96bulls'))).items).toHaveLength(0);
  });
});

// ---- unwired production call sites: driven under the default, each must refuse at the seam.
type Unwired = 'publication-canary' | 'routes/pipeline-default' | 'worker-deliverDepsFor';
const UNWIRED_CALLERS: Unwired[] = ['publication-canary', 'routes/pipeline-default', 'worker-deliverDepsFor'];
const rawKeys = async (s: ObjectStore) => (await s.list(objectKey('96bulls'))).items.map((i) => String(i.key)).filter((k) => k.includes('/raw/'));
/** Drive one unwired site end to end; returns the outward outcome (status / error code / reason). */
async function driveUnwired(caller: Unwired, s: ObjectStore): Promise<string> {
  if (caller === 'publication-canary') {
    // publication-canary.ts calls publishBufferedStage({ target, rpc, store }, ...): no rawChunkSealer.
    const prepared = prepareBufferedStage('96bulls', { claim_token: '33333333-3333-3333-3333-333333333333', batch_digest: HEX, received_at: '2026-11-04T01:00:00.000Z', org_id: 'org-a', workspace_id: 'ws-a', origin_id: 'origin-a', user_id: 'user-a' }, [{ source: 'claude', sessionId: 'session:1', lines: [line], meta: [['revision-1', HEX, 'claude', 'session:1']] }], [[0]]);
    await putBufferedStage(s, prepared);
    const p = publication(prepared.spec);
    resetCounts();
    const rpc = { claim: async () => p.ticket, heartbeat: async () => {}, abandon: async () => {}, prepare: async () => {}, register: async () => {}, publish: async () => { throw new Error('unreachable: publish'); } };
    const error = await publishBufferedStage({ target: p.target, rpc: rpc as never, store: () => s }, p.ticket.stageToken, { enabled: true, maxJobMs: 10_000 }).then(() => undefined, (e: unknown) => e as { code?: string });
    return `rejected ${error?.code}`;
  }
  if (caller === 'routes/pipeline-default') {
    // createApp() mounts the module-level `pipelineRoutes = createPipelineRoutes()` (defaultDeps).
    const app = createApp({ auth: async (c, next) => { c.set('auth' as never, { orgId: 'org-a', workspaceId: 'ws-a', scopes: ['rth:sync'] } as never); await next(); } });
    const env = { PIPELINE_ORG_ID: 'org-a', PIPELINE_RAW_S3_BUCKET: 'fixture', PIPELINE_RAW_S3_REGION: 'us-east-1', PIPELINE_RAW_S3_ENDPOINT: 'http://127.0.0.1:9', PIPELINE_RAW_S3_ACCESS_KEY_ID: 'fixture-id', PIPELINE_RAW_S3_SECRET_ACCESS_KEY: 'fixture-key' };
    const realFetch = globalThis.fetch;
    let fetches = 0;
    globalThis.fetch = (async () => { fetches++; throw new Error('no network in this test'); }) as unknown as typeof fetch;
    try {
      const response = await app.request('/v1/pipeline/chunks?machine=m&harness=claude&session=s&date=2026-11-04&seq=0&epoch=0&offset=0&format=ndjson', { method: 'POST', body: line + '\n' }, env);
      const body = (await response.json()) as { error?: { code?: string } };
      return `${response.status} ${body.error?.code} fetches=${fetches}`;
    } finally {
      globalThis.fetch = realFetch;
    }
  }
  // worker.ts builds its write target as { store, keyRoot } and passes it through deliverDepsFor.
  const deps = deliverDepsFor({ acceptanceMode: 'canonical', maxPerUser: 1, maxPerOrg: 1, inlineMaxSessions: 1 } as never, (async () => { throw new Error('no database in this test'); }) as never, { store: s, keyRoot: '96bulls' });
  const error = await writeGroups(deps.target, { orgId: 'org-a', workspaceId: 'ws-a', userId: 'user-a', originId: 'origin-a', batchDigest: HEX }, [{ source: 'claude', sessionId: 'session:1', lines: [line] }], new Date('2026-11-04T01:00:00Z')).then(() => undefined, (e: unknown) => e as { reason?: string });
  return `rejected ${error?.reason}`;
}

describe('unwired call sites refuse under the default', () => {
  const EXPECTED_OUTCOME: Record<Unwired, string> = {
    // publishBufferedStage maps the seam's PipelineError code; the seam spy records the reason.
    'publication-canary': 'rejected scrub_engine_unavailable',
    'routes/pipeline-default': '503 scrub_unavailable fetches=0',
    'worker-deliverDepsFor': 'rejected seal_path_not_routed',
  };
  for (const caller of UNWIRED_CALLERS)
    test(`${caller}: refuses seal_path_not_routed, zero TS scrub/seal calls, no raw object`, async () => {
      unsetDefault();
      await initScrubEngine();
      expect(activeScrubEngine().kind).toBe('wasm-bindgen');
      const s = scratch();
      const seamRefusals: string[] = [];
      const realSeam = scrubEngineSeam.sealRawChunk;
      const seamSpy = spyOn(scrubEngineSeam, 'sealRawChunk').mockImplementation(async (...a: Parameters<typeof realSeam>) => {
        try { return await realSeam(...a); } catch (error) { seamRefusals.push(String((error as { reason?: unknown }).reason)); throw error; }
      });
      try {
        resetCounts();
        const outcome = await driveUnwired(caller, s);
        const row: Row = { caller, mode: 'unwired', routedSealer: false, tsCalls: { ...counts }, seamRefusals, outcome, rawObjectsWritten: (await rawKeys(s)).length };
        rows.push(row);
        expect(outcome).toBe(EXPECTED_OUTCOME[caller]);
        expect(seamRefusals).toEqual(['seal_path_not_routed']);
        expect(row.tsCalls).toStrictEqual(Object.fromEntries(Object.keys(counts).map((k) => [k, 0])));
        expect(row.rawObjectsWritten).toBe(0);
      } finally {
        seamSpy.mockRestore();
      }
    });
});

describe('SCRUB_ENGINE=ts without a TS capability (Node self-host) refuses', () => {
  for (const caller of CALLERS)
    test(`${caller}: scrub_engine_ts_not_allowed before any seal, zero TS scrub/seal calls, no raw object`, async () => {
      unsetDefault();
      process.env.SCRUB_ENGINE = 'ts';
      await expect(initScrubEngine()).rejects.toMatchObject({ code: 'scrub_engine_invalid', reason: 'scrub_engine_ts_not_allowed' });
      const s = scratch();
      resetCounts();
      const outcome = await drive(caller, s).then(() => 'sealed', (e: unknown) => String((e as { reason?: unknown }).reason));
      const row: Row = { caller, mode: 'ts-refused', routedSealer: false, tsCalls: { ...counts }, outcome, rawObjectsWritten: (await rawKeys(s)).length };
      rows.push(row);
      expect(outcome).toBe('scrub_engine_ts_not_allowed');
      expect(row.tsCalls).toStrictEqual(Object.fromEntries(Object.keys(counts).map((k) => [k, 0])));
      expect(row.rawObjectsWritten).toBe(0);
    });
});

describe('positive control: hosted TS capability, knob unset (the hosted entries)', () => {
  for (const caller of CALLERS)
    test(`${caller}: no routed sealer; the TS sealer runs (version 8, no profile)`, async () => {
      unsetDefault();
      declareScrubTsCapability('test'); // stands in for the hosted-worker / hosted-lambda declaration
      expect(await initScrubEngine()).toBe(activeScrubEngine());
      expect(activeScrubEngine().kind).toBe('ts');
      const { row } = await run(caller, 'control');
      expect(row.routedSealer).toBe(false);
      expect(row.tsCalls['raw-chunks.ts#sealRawChunk']).toBeGreaterThanOrEqual(1);
      expect(row.tsCalls['raw-chunks.ts#scrubRecords'] + row.tsCalls['raw-chunks.ts#sealRawChunk']).toBeGreaterThanOrEqual(1);
      expect(row.scrubVersion).toBe('8');
      expect(row.scrubProfile).toBeUndefined();
    });
});

describe('production constructors install the routed sealer', () => {
  // Static evidence for the wiring the drivers above stand in for (see docs/scrub-engine.md).
  const root = new URL('../../../', import.meta.url);
  const WIRED: [string, RegExp][] = [
    ['src/session-ingest/selfhost-server.ts', /\.\.\.routedRawChunkSealerDeps\(\)/],
    ['src/session-ingest/upload-admission-hosted.ts', /\.\.\.routedRawChunkSealerDeps\(\)/],
    ['src/alleyoop/ingest.ts', /\.\.\.routedRawChunkSealerDeps\(\)/],
    ['src/session-ingest/publication-runtime.ts', /\.\.\.routedRawChunkSealerDeps\(\)/],
    ['src/session-ingest/publication-portable.ts', /\.\.\.routedRawChunkSealerDeps\(\)/],
    ['src/migration/neon-s3-sync/main.ts', /\.\.\.routedRawChunkSealerDeps\(\)/],
    ['src/session-ingest/buffered-drain.ts', /\.\.\.routedRawChunkSealerDeps\(\)/],
  ];
  for (const [file, pattern] of WIRED)
    test(`${file} spreads routedRawChunkSealerDeps()`, () => {
      expect(readFileSync(new URL(file, root), 'utf8')).toMatch(pattern);
    });
});

describe('wiring inventory is complete (source scan)', () => {
  // Every site that can reach the seam's sealRawChunk, by the shape that installs a sealer. Each
  // must be classified; a new site fails here until it is wired or added to UNWIRED (with a row
  // above proving it refuses). WIRED regions must spread routedRawChunkSealerDeps(); UNWIRED and
  // NOT_SEAL regions must not.
  const root = new URL('../../../', import.meta.url);
  const SITE_PATTERNS: Record<string, RegExp> = {
    'seal-fallback': /\?\?\s*sealRawChunk\b/g,
    DeliverDeps: /:\s*DeliverDeps\s*=\s*\{|\)\s*:\s*DeliverDeps\s*\{/g,
    PipelineRouteDeps: /:\s*PipelineRouteDeps\s*=\s*\{|(?<!function\s)\bcreatePipeline(?:Ops)?Routes\(/g,
    PublicationJobDeps: /\b(?:publishBufferedStage|canonicalPublicationTick)\(\s*\{/g,
    'write/export target': /\{\s*store\s*(?::[^,{}]*)?,\s*keyRoot\s*:/g,
  };
  // [kind, file, n-th match in that file] -> classification
  const WIRED_SITES = [
    ['DeliverDeps', 'src/session-ingest/selfhost-server.ts', 1],
    ['write/export target', 'src/session-ingest/selfhost-server.ts', 1],
    // Decision 0005: the hosted upload-admission Lambda admits (and so seals) itself, inline, TS-only by config.
    ['DeliverDeps', 'src/session-ingest/upload-admission-hosted.ts', 1],
    ['write/export target', 'src/session-ingest/upload-admission-hosted.ts', 1],
    ['PipelineRouteDeps', 'src/alleyoop/ingest.ts', 1],
    ['PublicationJobDeps', 'src/session-ingest/publication-runtime.ts', 1],
    ['PublicationJobDeps', 'src/session-ingest/publication-portable.ts', 1],
    ['write/export target', 'src/migration/neon-s3-sync/main.ts', 1],
    ['write/export target', 'src/session-ingest/buffered-drain.ts', 1], // buffered backlog drain (0005)
  ] as const;
  const UNWIRED_SITES = [
    ['PublicationJobDeps', 'src/session-ingest/publication-canary.ts', 1], // row publication-canary
    ['PipelineRouteDeps', 'packages/pipeline/src/routes/pipeline.ts', 1], // defaultDeps; row routes/pipeline-default
    ['PipelineRouteDeps', 'packages/pipeline/src/routes/pipeline.ts', 2], // pipelineRoutes = createPipelineRoutes()
    ['PipelineRouteDeps', 'packages/pipeline/src/routes/pipeline.ts', 3], // pipelineOpsRoutes (ops routes never seal)
    ['DeliverDeps', 'src/session-ingest/worker.ts', 1], // deliverDepsFor; row worker-deliverDepsFor
    ['write/export target', 'src/session-ingest/worker.ts', 1], // ingestApp target
    ['write/export target', 'src/session-ingest/worker.ts', 2], // admitQueueBatch target
    ['write/export target', 'src/session-ingest/worker.ts', 3], // fanout target
  ] as const;
  // The seam fallbacks themselves: each consumer is driven by a cutover/control row above.
  const FALLBACK_SITES = [
    ['seal-fallback', 'src/session-ingest/writer.ts', 1], // row writer
    ['seal-fallback', 'src/session-ingest/publication-image.ts', 1], // row publication-image
    ['seal-fallback', 'packages/pipeline/src/routes/pipeline.ts', 1], // row routes/pipeline
    ['seal-fallback', 'src/migration/neon-s3-sync/export-session.ts', 1], // row export
  ] as const;
  // Matches the target shape but never seals a raw chunk.
  const NOT_SEAL_SITES = [
    ['write/export target', 'src/session-ingest/app.ts', 1], // enqueueAdmission: inbox object only
  ] as const;

  function sources(): string[] {
    const out: string[] = [];
    const walk = (rel: string) => {
      for (const e of readdirSync(new URL(rel, root), { withFileTypes: true })) {
        const child = `${rel}${e.name}`;
        if (e.isDirectory()) { if (e.name !== 'node_modules' && e.name !== '__tests__' && e.name !== 'test') walk(`${child}/`); }
        else if (e.name.endsWith('.ts') && !e.name.endsWith('.test.ts') && !e.name.endsWith('.d.ts')) out.push(child);
      }
    };
    walk('src/');
    for (const pkg of readdirSync(new URL('packages/', root), { withFileTypes: true })) if (pkg.isDirectory()) { try { walk(`packages/${pkg.name}/src/`); } catch { /* no src/ */ } }
    return out.sort();
  }
  /** The balanced (...) or {...} region starting at the first bracket at or after `from`. */
  function region(text: string, from: number): string {
    const start = text.slice(from).search(/[({]/) + from;
    let depth = 0;
    for (let i = start; i < text.length; i++) {
      const ch = text[i]!;
      if (ch === '(' || ch === '{') depth++;
      else if (ch === ')' || ch === '}') if (--depth === 0) return text.slice(from, i + 1);
    }
    return text.slice(from);
  }
  const found = new Map<string, string>();
  for (const file of sources()) {
    const text = readFileSync(new URL(file, root), 'utf8');
    for (const [kind, pattern] of Object.entries(SITE_PATTERNS)) {
      let n = 0;
      for (const m of text.matchAll(pattern)) found.set(`${kind} ${file} #${++n}`, region(text, m.index!));
    }
  }
  const key = ([kind, file, n]: readonly [string, string, number]) => `${kind} ${file} #${n}`;

  test('every site is classified exactly once (wired, unwired-refuses, seam fallback, not a seal)', () => {
    const classified = [...WIRED_SITES, ...UNWIRED_SITES, ...FALLBACK_SITES, ...NOT_SEAL_SITES].map(key).sort();
    expect(new Set(classified).size).toBe(classified.length);
    expect([...found.keys()].sort()).toStrictEqual(classified);
  });
  test('wired sites spread routedRawChunkSealerDeps(); unwired and non-seal sites do not', () => {
    for (const site of WIRED_SITES) expect(found.get(key(site))).toMatch(/\.\.\.routedRawChunkSealerDeps\(\)/);
    for (const site of [...UNWIRED_SITES, ...NOT_SEAL_SITES]) expect(found.get(key(site))).not.toMatch(/routedRawChunkSealerDeps|rawChunkSealer/);
  });
});

afterAll(() => {
  mock.module('../../../packages/pipeline/src/lib/pipeline/raw-chunks.ts', () => RAW_CHUNKS);
  mock.module('../../../packages/pipeline/src/lib/scrub.ts', () => TS_SCRUB);
  const out = process.env.SCRUB_REACHABILITY_REPORT;
  if (!out) return;
  writeFileSync(out, JSON.stringify({ schema: 2, cutoverEnv: 'SCRUB_ENGINE, SCRUB_ENGINE_REQUIRED, READER_MAX_RULESET unset; no TS capability', tsEntryPoints: TS_ENTRY_POINTS, rows }, null, 2) + '\n');
});
