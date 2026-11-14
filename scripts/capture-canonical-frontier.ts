#!/usr/bin/env bun
/** Reviewed live-capable READ ONLY capture runner. Configuration is stdin-only. */
import pg from "pg";
import { lstat, mkdir, open } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { createS3Store, objectStoreManifestReader } from "../src/shared/vendor.ts";
import type { ObjectStore } from "../packages/object-store/src/types.ts";
import { sessionReadOnly } from "../src/migration/neon-s3-sync/neon-reader.ts";
import { captureLegacyFrontierSource, composeCanonicalFrontierCapture, reconcileLegacyFrontierClosure } from "../src/session-ingest/frontier-capture-source.ts";
import { captureSeedScopeExportEvidence, createS3FrontierVersioningReader, type SeedScopeExportEvidence } from "../src/session-ingest/frontier-plan-source.ts";
import { evaluateCanonicalFrontier, frontierCanonicalJson, frontierSha256 } from "../src/session-ingest/frontier-plan.ts";
import type { SeedSql } from "../src/session-ingest/seed.ts";
import { readerMaxRuleset } from "../packages/pipeline/src/lib/scrub-engine/reader-policy.ts";
import { initScrubEngine } from "../packages/pipeline/src/lib/scrub-engine/index.ts";

const PROTOCOL = "canonical-frontier-readonly-capture-runner-v1";
const MAX_CONFIG = 1024 * 1024;
const CONTROL = /[\u0000-\u001f\u007f]/u;
const HEX64 = /^[a-f0-9]{64}$/;
const IDENT = /^[a-zA-Z0-9._:/-]{1,1024}$/;
process.umask(0o077);

function refuse(code: string): never { throw Object.assign(new Error(code), { code }); }
function object(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === "object" && !Array.isArray(value); }
function text(value: unknown, max = 1024): value is string { return typeof value === "string" && value.length > 0 && Buffer.byteLength(value) <= max && !CONTROL.test(value); }
function exactKeys(value: Record<string, unknown>, expected: string[]): boolean { return Object.keys(value).sort().join(",") === [...expected].sort().join(","); }
async function stdinCapped(): Promise<string> {
  const chunks: Uint8Array[] = []; let size = 0;
  for await (const chunk of process.stdin) {
    const bytes = typeof chunk === "string" ? new TextEncoder().encode(chunk) : chunk;
    size += bytes.byteLength; if (size > MAX_CONFIG) refuse("frontier_capture_config_bound"); chunks.push(bytes);
  }
  const out = new Uint8Array(size); let at = 0;
  for (const chunk of chunks) { out.set(chunk, at); at += chunk.byteLength; }
  try { return new TextDecoder("utf-8", { fatal: true }).decode(out); }
  catch { refuse("frontier_capture_config_encoding"); }
}

interface Config {
  protocol: typeof PROTOCOL;
  sourceDatabaseUrl: string;
  seedDatabaseUrl: string;
  scope: { orgId: string; workspaceId: string };
  target: { bucket: string; region: string; keyRoot: string; schemaGeneration: string };
  sourceObjects: { bucket: string; region: string; keyRoot: string; endpoint?: string; accessKeyId: string; secretAccessKey: string };
  bounds: { pageLimit: number; maxPages: number; maxScopes: number; maxOrigins: number; maxRecords: number; maxBytes: number; maxDurationMs: number };
}

function config(value: unknown): Config {
  if (!object(value) || Object.keys(value).sort().join(",") !== "bounds,protocol,scope,seedDatabaseUrl,sourceDatabaseUrl,sourceObjects,target" || value.protocol !== PROTOCOL ||
      !object(value.scope) || !object(value.target) || !object(value.sourceObjects) || !object(value.bounds)) refuse("frontier_capture_config_shape");
  if (!exactKeys(value.scope, ["orgId", "workspaceId"]) || !exactKeys(value.target, ["bucket", "region", "keyRoot", "schemaGeneration"]) ||
      ![["bucket", "region", "keyRoot", "accessKeyId", "secretAccessKey"], ["bucket", "region", "keyRoot", "endpoint", "accessKeyId", "secretAccessKey"]].some(keys => exactKeys(value.sourceObjects as Record<string, unknown>, keys)) ||
      !exactKeys(value.bounds, ["pageLimit", "maxPages", "maxScopes", "maxOrigins", "maxRecords", "maxBytes", "maxDurationMs"])) refuse("frontier_capture_config_shape");
  const v = value as unknown as Config;
  if (![v.sourceDatabaseUrl, v.seedDatabaseUrl].every(item => text(item, 8192)) || !text(v.scope.orgId, 512) || typeof v.scope.workspaceId !== "string" || Buffer.byteLength(v.scope.workspaceId) > 512 || CONTROL.test(v.scope.workspaceId) ||
      !text(v.target.bucket, 255) || !text(v.target.region, 63) || !text(v.target.keyRoot, 63) || !HEX64.test(v.target.schemaGeneration) ||
      !text(v.sourceObjects.bucket, 255) || !text(v.sourceObjects.region, 63) || !text(v.sourceObjects.keyRoot, 1024) || !text(v.sourceObjects.accessKeyId, 1024) || !text(v.sourceObjects.secretAccessKey, 4096) ||
      (v.sourceObjects.endpoint !== undefined && !text(v.sourceObjects.endpoint, 2048)) || !IDENT.test(v.sourceObjects.keyRoot) || v.sourceObjects.keyRoot.startsWith("/") || v.sourceObjects.keyRoot.endsWith("/") || v.sourceObjects.keyRoot.split("/").some(part => part === "" || part === "." || part === "..") ||
      !Number.isSafeInteger(v.bounds.pageLimit) || v.bounds.pageLimit < 1 || v.bounds.pageLimit > 1000 || !Number.isSafeInteger(v.bounds.maxPages) || v.bounds.maxPages < 1 || v.bounds.maxPages > 1_000_000 ||
      !Number.isSafeInteger(v.bounds.maxScopes) || v.bounds.maxScopes < 1 || v.bounds.maxScopes > 100_000 || !Number.isSafeInteger(v.bounds.maxOrigins) || v.bounds.maxOrigins < 1 || v.bounds.maxOrigins > 100_000 ||
      !Number.isSafeInteger(v.bounds.maxRecords) || v.bounds.maxRecords < 1 || v.bounds.maxRecords > 10_000_000 || !Number.isSafeInteger(v.bounds.maxBytes) || v.bounds.maxBytes < 1024 || v.bounds.maxBytes > 1024 * 1024 * 1024 ||
      !Number.isSafeInteger(v.bounds.maxDurationMs) || v.bounds.maxDurationMs < 1 || v.bounds.maxDurationMs > 300_000) refuse("frontier_capture_config_value");
  for (const url of [v.sourceDatabaseUrl, v.seedDatabaseUrl]) {
    const parsed = new URL(url); if (!["postgres:", "postgresql:"].includes(parsed.protocol) || !parsed.hostname || parsed.hash) refuse("frontier_capture_database_url");
  }
  if (v.sourceObjects.endpoint !== undefined) {
    const endpoint = new URL(v.sourceObjects.endpoint);
    if (!["http:", "https:"].includes(endpoint.protocol) || !["127.0.0.1", "[::1]", "localhost"].includes(endpoint.hostname) || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) refuse("frontier_capture_object_endpoint");
  }
  if (v.sourceObjects.keyRoot !== v.target.keyRoot) refuse("frontier_capture_key_root_mismatch");
  return v;
}

function args(argv: string[]): { output: string; cacheDir: string } {
  let output = "", cacheDir = "";
  const seen = new Set<string>();
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index], value = argv[index + 1];
    if (!value || Buffer.byteLength(value) > 4096 || CONTROL.test(value) || !isAbsolute(value) || (flag !== "--output" && flag !== "--cache-dir") || seen.has(flag)) refuse("frontier_capture_usage");
    seen.add(flag);
    if (flag === "--output") output = resolve(value); else cacheDir = resolve(value);
  }
  if (!output || !cacheDir || !isAbsolute(output) || !isAbsolute(cacheDir) || output === cacheDir || output.startsWith(`${cacheDir}/`)) refuse("frontier_capture_usage");
  return { output, cacheDir };
}

async function writeExclusive(path: string, value: unknown): Promise<void> {
  const file = await open(path, "wx", 0o600);
  try { await file.writeFile(`${frontierCanonicalJson(value)}\n`); await file.sync(); }
  finally { await file.close(); }
}

async function requirePrivateDirectory(path: string): Promise<void> {
  const value = await lstat(path);
  if (!value.isDirectory() || value.isSymbolicLink() || (value.mode & 0o077) !== 0 || (typeof process.getuid === "function" && value.uid !== process.getuid())) refuse("frontier_capture_directory_permissions");
}

async function requireSeedReader(db: SeedSql): Promise<void> {
  const rows = (await db.query<{ name: string; superuser: boolean; inherit: boolean; create_role: boolean; create_db: boolean; replication: boolean; bypass_rls: boolean; read_only: string }>(
    `SELECT r.rolname AS name,r.rolsuper AS superuser,r.rolinherit AS inherit,r.rolcreaterole AS create_role,
            r.rolcreatedb AS create_db,r.rolreplication AS replication,r.rolbypassrls AS bypass_rls,
            current_setting('transaction_read_only') AS read_only
       FROM pg_roles r WHERE r.rolname=current_user`,
  )).rows;
  const role = rows[0];
  if (rows.length !== 1 || role?.name !== "bulls96_session_ingest_seeder" || role.read_only !== "on" || role.superuser || role.inherit || role.create_role || role.create_db || role.replication || role.bypass_rls) refuse("frontier_capture_seed_role");
}

function readOnlyObjectStore(store: ObjectStore): ObjectStore {
  return {
    backend: store.backend,
    head: store.head.bind(store), get: store.get.bind(store), getObject: store.getObject.bind(store),
    list: store.list.bind(store), presignGet: store.presignGet.bind(store),
    async put() { refuse("frontier_capture_object_write"); },
    async delete() { refuse("frontier_capture_object_write"); },
  };
}

let sourceClient: pg.Client | undefined, seedClient: pg.Client | undefined;
try {
  // Scrub engine (docs/scrub-engine.md, "Default flip"): the pinned wasm-bindgen engine, read from the repo checkout.
  await initScrubEngine();
  const paths = args(process.argv.slice(2));
  const input = config(JSON.parse(await stdinCapped()));
  await mkdir(paths.cacheDir, { recursive: true, mode: 0o700 });
  await requirePrivateDirectory(paths.cacheDir);
  await requirePrivateDirectory(dirname(paths.output));
  sourceClient = new pg.Client({ connectionString: input.sourceDatabaseUrl, application_name: "canonical-frontier-readonly-source", statement_timeout: 20_000, query_timeout: 25_000 });
  seedClient = new pg.Client({ connectionString: input.seedDatabaseUrl, application_name: "canonical-frontier-readonly-seed", statement_timeout: 20_000, query_timeout: 25_000 });
  await sourceClient.connect(); await seedClient.connect();
  const sourceSql = await sessionReadOnly(sourceClient);
  const seedSql = await sessionReadOnly(seedClient) as SeedSql;
  await requireSeedReader(seedSql);
  const sourceOptions = { pageLimit: input.bounds.pageLimit, maxPages: input.bounds.maxPages, maxOrigins: input.bounds.maxOrigins, maxBytes: input.bounds.maxBytes };
  const sourceOpening = await captureLegacyFrontierSource(sourceSql, input.scope, sourceOptions);

  const scopes = new Map<string, { source: string; sessionId: string }>();
  const scopeKeys = new Set<string>();
  let after: string[] | null = null, scopeCount = 0;
  for (;;) {
    const page = (await seedSql.query<{ r: { rows: Array<{ workspace_id: string; source: string; session_id: string }>; next: string[] | null } }>("SELECT sessions.ingest_seed_scope_page($1::jsonb) AS r", [JSON.stringify({ org_id: input.scope.orgId, generation: input.target.schemaGeneration, after, limit: 200 })])).rows[0]?.r;
    if (!page || !Array.isArray(page.rows) || page.rows.length > 200 || (page.next && (!Array.isArray(page.next) || page.next.length !== 3 || !page.next.every(value => text(value, 512)) || !page.rows.length || frontierCanonicalJson(page.next) === frontierCanonicalJson(after)))) refuse("frontier_capture_scope_page");
    for (const row of page.rows) {
      if (!row || typeof row.workspace_id !== "string" || Buffer.byteLength(row.workspace_id) > 512 || CONTROL.test(row.workspace_id) || !text(row.source, 512) || !text(row.session_id, 512)) refuse("frontier_capture_scope_page");
      scopeCount++; if (scopeCount > input.bounds.maxScopes) refuse("frontier_capture_scope_bound");
      const scopeKey = frontierCanonicalJson([row.workspace_id, row.source, row.session_id]);
      if (scopeKeys.has(scopeKey)) refuse("frontier_capture_scope_page");
      scopeKeys.add(scopeKey);
      if (row.workspace_id === input.scope.workspaceId) scopes.set(frontierCanonicalJson([row.source, row.session_id]), { source: row.source, sessionId: row.session_id });
    }
    if (page.next && frontierCanonicalJson(page.next) !== frontierCanonicalJson([page.rows.at(-1)!.workspace_id, page.rows.at(-1)!.source, page.rows.at(-1)!.session_id])) refuse("frontier_capture_scope_page");
    if (!page.next) break; after = page.next;
  }
  const store = readOnlyObjectStore(createS3Store({ endpoint: input.sourceObjects.endpoint, bucket: input.sourceObjects.bucket, region: input.sourceObjects.region,
    credentials: { accessKeyId: input.sourceObjects.accessKeyId, secretAccessKey: input.sourceObjects.secretAccessKey } }));
  const reader = objectStoreManifestReader(store, { provider: "s3", bucket: input.sourceObjects.bucket });
  const maximum = readerMaxRuleset(process.env); // unset: legacy reads
  const versioning = createS3FrontierVersioningReader({ endpoint: input.sourceObjects.endpoint, region: input.sourceObjects.region,
    ...(input.sourceObjects.endpoint ? { allowFixtureEndpoint: true } : {}), credentials: { accessKeyId: input.sourceObjects.accessKeyId, secretAccessKey: input.sourceObjects.secretAccessKey } });
  const requests: Array<{ origin: (typeof sourceOpening.origins)[number]; group: { source: string; sessionId: string }; cachePath: string }> = [];
  for (const origin of sourceOpening.origins) {
    const required = new Map(origin.records.map(record => [frontierCanonicalJson([record.source, record.sessionId]), { source: record.source, sessionId: record.sessionId }]));
    for (const group of required.values()) {
      if (group.sessionId === null || !scopes.has(frontierCanonicalJson([group.source, group.sessionId]))) refuse("frontier_capture_scope_missing");
      const key = frontierSha256([origin.tenant, group]);
      requests.push({ origin, group: { source: group.source, sessionId: group.sessionId }, cachePath: join(paths.cacheDir, `${key}.sqlite`) });
    }
  }
  const captureExports = async (): Promise<SeedScopeExportEvidence[]> => {
    const captured: SeedScopeExportEvidence[] = [];
    for (const request of requests) captured.push(await captureSeedScopeExportEvidence(seedSql, { store, reader, readerMaxRuleset: maximum, bucket: input.sourceObjects.bucket, provider: "s3",
        prefixFor: org => `${input.sourceObjects.keyRoot}/${org}`, decompress: (bytes, max) => new Uint8Array(zstdDecompressSync(bytes, { maxOutputLength: max })) }, versioning,
      { tenant: request.origin.tenant, generation: input.target.schemaGeneration, sourceHead: request.origin.opening.sourceHead!, source: request.group.source, sessionId: request.group.sessionId,
        keyRoot: `${input.sourceObjects.keyRoot}/${input.scope.orgId}`, cachePath: request.cachePath,
        maxRecords: input.bounds.maxRecords, maxEvidenceBytes: input.bounds.maxBytes, maxDurationMs: input.bounds.maxDurationMs }));
    return captured;
  };
  const exportOpening = await captureExports();
  const exportClosing = await captureExports();
  const exports = exportOpening.map((first, index): SeedScopeExportEvidence => {
    const second = exportClosing[index];
    if (!second || frontierCanonicalJson(first) !== frontierCanonicalJson(second)) refuse("frontier_capture_export_changed");
    return { ...first, snapshotSha256: frontierSha256({ opening: first.snapshotSha256, closing: second.snapshotSha256 }) };
  });
  if (exportClosing.length !== exports.length) refuse("frontier_capture_export_changed");
  const sourceClosing = await captureLegacyFrontierSource(sourceSql, input.scope, sourceOptions);
  const source = reconcileLegacyFrontierClosure(sourceOpening, sourceClosing);
  const capture = composeCanonicalFrontierCapture(source, input.target, exports);
  const exportBindings = exports.flatMap(evidence => evidence.bindings);
  const plan = evaluateCanonicalFrontier(capture, { now: new Date(capture.capturedAt), exportBindings });
  await writeExclusive(paths.output, { protocol: PROTOCOL, capturedAt: capture.capturedAt, capture, exportBindings, plan });
  process.stdout.write(`${JSON.stringify({ protocol: PROTOCOL, status: plan.status, captureId: capture.captureId, output: paths.output })}\n`);
  process.exitCode = plan.status === "ready" ? 0 : 2;
} catch (error) {
  const candidate = object(error) && typeof error.code === "string" ? error.code : "";
  const code = /^[A-Za-z0-9_]{1,80}$/.test(candidate) ? candidate : error instanceof SyntaxError ? "frontier_capture_config_json" : "frontier_capture_refused";
  process.stderr.write(`${JSON.stringify({ protocol: PROTOCOL, status: "error", code })}\n`); process.exitCode = 1;
} finally {
  await Promise.allSettled([sourceClient?.end(), seedClient?.end()].filter(Boolean) as Promise<unknown>[]);
}
