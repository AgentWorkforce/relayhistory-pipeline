/**
 * Negative reachability proof for the relayscrubber 0.5.0-alpha.4 cutover
 * (docs/scrub-engine.md, "Cutover: pinned relayscrubber 0.5.0-alpha.4").
 *
 * Every TypeScript scrub/seal entry point (lib/scrub.ts and lib/pipeline/raw-chunks.ts)
 * is replaced in place by a counting spy. Then each in-scope ingress path (ingest
 * writer, publication image, routes/pipeline HTTP, neon-s3-sync export) is driven end
 * to end against scratch stores with the sealer the production constructors install
 * (`routedRawChunkSealerDeps()`):
 *
 * - under SCRUB_ENGINE=wasm-bindgen + SCRUB_ENGINE_REQUIRED=wasm-bindgen +
 *   READER_MAX_RULESET=10, with the REAL vendored engine, no TS entry point runs and
 *   every sealed object carries scrub version 10 and the crate profile;
 * - with the knob unset (positive control), the same drivers seal through the TS path
 *   (the spies count) with scrub version 8 and no profile, exactly as today.
 *
 * With SCRUB_REACHABILITY_REPORT=<absolute path> it writes a JSON report
 * (scripts/scrub-cutover-reachability.ts runs this file and checks that report).
 */
import { afterAll, afterEach, beforeAll, describe, expect, mock, test } from 'bun:test';
import { writeFileSync, readFileSync } from 'node:fs';
import { Hono } from 'hono';
import { zstdDecompressSync } from 'node:zlib';
import * as rawChunks from '../../../packages/pipeline/src/lib/pipeline/raw-chunks.ts';
import * as tsScrub from '../../../packages/pipeline/src/lib/scrub.ts';
import { createPipelineRoutes } from '../../../packages/pipeline/src/routes/pipeline.ts';
import type { AppEnv } from '../../../packages/pipeline/src/env.ts';
import { readAll, objectKey, type ObjectStore } from '../../../packages/object-store/src/index.ts';
import {
  activeScrubEngine, initScrubEngine, resetScrubEngineForTests, routedRawChunkSealerDeps, sealRawChunk as seam, scrubEngineReady,
} from '../../../packages/pipeline/src/lib/scrub-engine/index.ts';
import { writeGroups } from '../writer.ts';
import { preparePublicationImage, writePublicationImage } from '../publication-image.ts';
import { parseReceiptSeatSnapshot } from '../publication-seats.ts';
import { exportUnit } from '../../migration/neon-s3-sync/export-session.ts';
import { scratchStore } from './support.ts';

const TRIPLE = { SCRUB_ENGINE: 'wasm-bindgen', SCRUB_ENGINE_REQUIRED: 'wasm-bindgen', READER_MAX_RULESET: '10' } as const;
const KEYS = [...Object.keys(TRIPLE), 'SCRUB_BINDGEN_DIR', 'S3X_OBLIGATIONS'] as const;
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
function publication() {
  const record = { ord: 0, source: 'claude', session_id: 'session:1', record_id: 'native-r', revision_id: 'revision-1', revision: 1, digest: HEX };
  const stage = { key: 'unused', sha256: HEX, bytes: 1, groupOrdinals: [[0]] };
  const receiptSeats = parseReceiptSeatSnapshot({ controlEpoch: HEX, sourceIdentitySha256: OTHER, allocatedHighWater: '9007199254740993', lastAssignedSeat: '9007199254740999', records: [{ ord: 0, seat: '9007199254740994', bodyAccepted: true, candidateDigest: HEX, firstDigest: HEX, resetEpoch: '0', fenceEpoch: '2' }] }, [record], stage);
  const generation = '12345678-1234-1234-1234-123456789abc', attemptToken = 'abcdefab-1234-1234-1234-123456789abc';
  const head = { groupIndex: 0, ordinals: [0], generation, prefix: `96bulls/org-a/upload-generations/g=${generation}`, headVersion: '1', revision: '1', previousKey: null, previousRawSha256: null, basePointer: null, source: 'claude', sessionId: 'session:1', physicalWorkspace: 'ws-a', physicalSource: 'claude', physicalSession: 'session_1', machineId: 'srv_fixture', fenceEpoch: '2', resetEpoch: '0' };
  const ticket = { schemaGeneration: HEX, attemptToken, leaseToken: '11111111-1111-1111-1111-111111111111', leaseUntil: '2026-11-05T00:05:00.000000Z', stageToken: '22222222-2222-2222-2222-222222222222', stageVersion: '1', outboxVersion: '1', tenant: { orgId: 'org-a', workspaceId: 'ws-a', userId: 'user-a' }, originId: 'origin-a', batchId: 'batch-a', batchDigest: HEX, claimToken: '33333333-3333-3333-3333-333333333333', stage, records: [record], receiptSeats, acceptedOrdinals: [0], heads: [head] };
  const decoded = { header: { v: 1 as const, publication_state: 'buffered' as const, claim_token: ticket.claimToken, batch_digest: HEX, received_at: '2026-11-04T01:00:00.000Z', org_id: 'org-a', workspace_id: 'ws-a', origin_id: 'origin-a', user_id: 'user-a', groups: [['claude', 'session:1'] as [string, string]] }, groups: [{ source: 'claude', sessionId: 'session:1', lines: [line] }], spec: stage };
  return { ticket, head, decoded, target: { bucket: 'fixture', region: 'us-east-1', keyRoot: '96bulls', generation: HEX } };
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

interface Row { caller: Caller; mode: 'cutover' | 'control'; routedSealer: boolean; tsCalls: Record<string, number>; scrubVersion: string | undefined; scrubProfile: string | undefined; secretInSealedBytes: boolean }
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

describe('under the env triple (real vendored alpha.4 engine)', () => {
  for (const caller of CALLERS)
    test(`${caller}: the TS scrub/sealer is never invoked; the sealed object is the crate's (version 10)`, async () => {
      Object.assign(process.env, TRIPLE);
      delete process.env.SCRUB_BINDGEN_DIR;
      resetScrubEngineForTests();
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
    Object.assign(process.env, TRIPLE);
    resetScrubEngineForTests();
    await initScrubEngine();
    resetCounts();
    const s = scratch();
    await expect(seam(s, { location: { prefix: '96bulls/o', workspaceId: 'w', machineId: 'm', harness: 'h', sessionId: 's', startDate: '2026-11-10' }, format: 'ndjson', seq: 0, epoch: 0, startOffset: 0, source: enc.encode(line + '\n') })).rejects.toMatchObject({ reason: 'seal_path_not_routed' });
    expect(tsCalls()).toBe(0);
    expect((await s.list(objectKey('96bulls'))).items).toHaveLength(0);
  });
});

describe('positive control: knob unset (today\'s behaviour)', () => {
  for (const caller of CALLERS)
    test(`${caller}: no routed sealer; the TS sealer runs (version 8, no profile)`, async () => {
      for (const k of KEYS) delete process.env[k];
      resetScrubEngineForTests();
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
    ['src/alleyoop/ingest.ts', /\.\.\.routedRawChunkSealerDeps\(\)/],
    ['src/session-ingest/publication-runtime.ts', /\.\.\.routedRawChunkSealerDeps\(\)/],
    ['src/session-ingest/publication-portable.ts', /\.\.\.routedRawChunkSealerDeps\(\)/],
    ['src/migration/neon-s3-sync/main.ts', /\.\.\.routedRawChunkSealerDeps\(\)/],
  ];
  for (const [file, pattern] of WIRED)
    test(`${file} spreads routedRawChunkSealerDeps()`, () => {
      expect(readFileSync(new URL(file, root), 'utf8')).toMatch(pattern);
    });
});

afterAll(() => {
  mock.module('../../../packages/pipeline/src/lib/pipeline/raw-chunks.ts', () => RAW_CHUNKS);
  mock.module('../../../packages/pipeline/src/lib/scrub.ts', () => TS_SCRUB);
  const out = process.env.SCRUB_REACHABILITY_REPORT;
  if (!out) return;
  writeFileSync(out, JSON.stringify({ schema: 1, triple: TRIPLE, tsEntryPoints: TS_ENTRY_POINTS, rows }, null, 2) + '\n');
});
