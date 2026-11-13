import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { createReadStream } from "node:fs";
import { lstat, mkdir, readFile, realpath, writeFile } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, resolve, sep } from "node:path";
import { outcomeFacts, sharedWrite60m, type DetectionInput, type DetectorOutput, type Finding, type LaneFacts } from "../../packages/coordination-detect/src/index.ts";
import { projectRelayhistorySession } from "../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import type { RelayhistoryStreamRecord } from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";

type Json = Record<string, unknown>;
type Projection = "snapshot" | "current";
type Split = "dev" | "heldout";

interface ManifestFile { sha256: string; bytes: number }
interface SnapshotManifest {
  snapshot_id: string;
  version: number;
  window_start: string;
  window_end: string;
  payload_total_bytes: number;
  files: Record<string, ManifestFile>;
}

interface SessionRow {
  source: string;
  session_id: string;
  started_at: string;
  last_action_at: string;
  project_id_raw: string | null;
  repo: string | null;
  git_branch: string | null;
  task_title: string | null;
  member_user_ids: string[];
  parent_delegation_id: string | null;
  child_delegation_ids: string[];
}

interface TimedValue { timestamp: string; value: string }
interface FeatureRow {
  source: string;
  session_id: string;
  repo: string | null;
  files_written: TimedValue[];
  commit_shas: TimedValue[];
  git_branches_seen: TimedValue[];
  pull_requests: Array<{ timestamp: string; repo?: string; number?: number; value?: string }>;
}

interface SnapshotEvent extends Json {
  type: string;
  timestamp?: string;
  event_id?: string;
  tool_name?: string | null;
  status?: string | null;
  content_truncated?: boolean;
  args_json?: string;
  tool_input?: Json;
  file_path?: string;
}

interface TruthIncident {
  incident_id: string;
  split: Split;
  status?: string;
  sessions: Array<{ source: string; session_id: string }>;
}

interface NegativeRow {
  negative_id: string;
  sessions: Array<{ source: string; session_id: string }>;
}

interface PrIncident {
  id: string;
  split: Split;
  status?: string;
  repo: string;
  harm_at: string;
  side_a: { prs?: number[]; branches?: string[]; shas?: string[] };
  side_b: { prs?: number[]; branches?: string[]; shas?: string[] };
}

interface SplitFile {
  incidents: Record<Split, string[]>;
  negatives: Record<Split, string[]>;
}

interface LoadedTruth {
  split: SplitFile;
  incidents: TruthIncident[];
  negatives: NegativeRow[];
  prIncidents: PrIncident[];
  hashes: Record<string, string>;
}

interface ProjectionGap {
  code: "raw_tool_input_unavailable";
  message: string;
  relevantToolCalls: number;
  missingArgsJson: number;
  truncatedCompactInputs: number;
  sampledSessions: number;
}

interface LoadedSnapshot {
  manifest: SnapshotManifest;
  manifestHash: string;
  sessions: SessionRow[];
  features: FeatureRow[];
  lanes?: LaneFacts[];
  projectionGap?: ProjectionGap;
  work: { writes: number; commits: number; outcomes: number };
}

interface ScoredBlock {
  alerts: number;
  alertBudgetPer100Sessions: number;
  adjudicatedPairs: {
    confirmedTrue: number;
    confirmedFalse: number;
    denominator: number;
    precision: number | null;
    unlabeledAlerts: number;
  };
  incidents: {
    found: number;
    total: number;
    recall: number | null;
    foundIds: string[];
    beforeHarmIds: string[];
  };
}

interface DetectorReport {
  name: string;
  version: string;
  paramsHash: string;
  runtime: {
    measurement: "external_wall_clock";
    note: string;
    lanes: number;
    writes: number;
    findings: number;
  };
  tiers: Record<string, { dev: ScoredBlock; heldout: ScoredBlock }>;
}

export interface EvaluationReport {
  contract: "coordination-eval/1";
  status: "complete" | "projection_unavailable";
  projection: Projection;
  inputs: {
    snapshotId: string;
    snapshotManifestSha256: string;
    truthSha256: Record<string, string>;
    sessionCount: number;
  };
  provenance: {
    gitSha: string;
    reportDeterminism: string;
  };
  coverage: {
    status: "complete" | "unavailable";
    notes: string[];
    projectionGap?: ProjectionGap;
  };
  detectors: DetectorReport[];
}

const key = (source: string, sessionId: string) => `${source}\u0000${sessionId}`;
const pairKey = (left: string, right: string) => [left, right].sort().join("\u0001");
const round = (value: number) => Number(value.toFixed(6));
const divide = (n: number, d: number) => d === 0 ? null : round(n / d);
const asObject = (value: unknown, label: string): Json => {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as Json;
};

async function sha256File(path: string): Promise<string> {
  const hash = createHash("sha256");
  await new Promise<void>((resolvePromise, reject) => {
    const stream = createReadStream(path);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("error", reject);
    stream.on("end", resolvePromise);
  });
  return hash.digest("hex");
}

async function readJson(path: string): Promise<Json> {
  return asObject(JSON.parse(await readFile(path, "utf8")), path);
}

async function readNdjson<T extends object>(path: string): Promise<T[]> {
  const rows: T[] = [];
  const lines = (await readFile(path, "utf8")).split(/\r?\n/);
  let line = 0;
  for (const raw of lines) {
    line += 1;
    if (!raw.trim()) continue;
    rows.push(asObject(JSON.parse(raw), `${path}:${line}`) as T);
  }
  return rows;
}

async function verifyManifest(snapshotDir: string): Promise<{ manifest: SnapshotManifest; manifestHash: string }> {
  const root = await realpath(snapshotDir);
  const manifestPath = join(root, "MANIFEST.json");
  const manifest = await readJson(manifestPath) as unknown as SnapshotManifest;
  if (!manifest.snapshot_id || !manifest.files || typeof manifest.files !== "object") throw new Error("snapshot MANIFEST.json is invalid");
  // The snapshot builder hashes the manifest's insertion order. That order is
  // produced by recursively sorted directory reads and is frozen in JSON.
  const entries = Object.entries(manifest.files);
  let payloadBytes = 0;
  const verified: Array<[string, ManifestFile]> = [];
  const workers = Array.from({ length: Math.min(16, Math.max(1, entries.length)) }, async (_, worker) => {
    for (let index = worker; index < entries.length; index += 16) {
      const [name, expected] = entries[index]!;
      if (isAbsolute(name) || name.split(/[\\/]/).includes("..")) throw new Error(`manifest path escapes snapshot at entry ${index}`);
      const path = resolve(root, name);
      let info: Awaited<ReturnType<typeof lstat>>;
      try {
        info = await lstat(path);
      } catch {
        throw new Error(`manifest payload is unreadable at entry ${index}`);
      }
      if (!info.isFile() || info.isSymbolicLink()) throw new Error(`manifest payload is not a regular file at entry ${index}`);
      let canonical: string;
      try {
        canonical = await realpath(path);
      } catch {
        throw new Error(`manifest payload is unreadable at entry ${index}`);
      }
      if (canonical !== root && !canonical.startsWith(`${root}${sep}`)) throw new Error(`manifest payload escapes snapshot at entry ${index}`);
      if (info.size !== expected.bytes) throw new Error(`manifest byte mismatch at entry ${index}`);
      let actual: string;
      try {
        actual = await sha256File(path);
      } catch {
        throw new Error(`manifest payload is unreadable at entry ${index}`);
      }
      if (actual !== expected.sha256) throw new Error(`manifest hash mismatch at entry ${index}`);
      payloadBytes += info.size;
      verified.push([name, expected]);
    }
  });
  await Promise.all(workers);
  if (payloadBytes !== manifest.payload_total_bytes) throw new Error("manifest payload_total_bytes mismatch");
  const verifiedByName = new Map(verified);
  const snapshotId = createHash("sha256")
    .update(entries.map(([name]) => {
      const value = verifiedByName.get(name)!;
      return `${name}\0${value.sha256}\n`;
    }).join(""))
    .digest("hex");
  if (snapshotId !== manifest.snapshot_id) throw new Error("manifest snapshot_id mismatch");
  return { manifest, manifestHash: await sha256File(manifestPath) };
}

function parentMap(sessions: SessionRow[]): Map<string, string> {
  const owner = new Map<string, string>();
  for (const session of sessions) for (const delegation of session.child_delegation_ids ?? []) {
    if (delegation) owner.set(`${session.source}\u0000${delegation}`, session.session_id);
  }
  const out = new Map<string, string>();
  for (const session of sessions) {
    const delegation = session.parent_delegation_id;
    if (!delegation) continue;
    const parent = owner.get(`${session.source}\u0000${delegation}`);
    if (parent) out.set(key(session.source, session.session_id), parent);
  }
  return out;
}

function latest(values: TimedValue[] | undefined, fallback: string | null): string | null {
  return [...(values ?? [])].sort((a, b) => a.timestamp.localeCompare(b.timestamp)).at(-1)?.value ?? fallback;
}

function baseLane(session: SessionRow, feature: FeatureRow, parents: Map<string, string>): Omit<LaneFacts, "writes"> {
  return {
    sessionId: session.session_id,
    source: session.source,
    userId: session.member_user_ids?.length === 1 ? session.member_user_ids[0]! : null,
    projectId: session.project_id_raw,
    repo: session.repo ?? feature.repo,
    branch: latest(feature.git_branches_seen, session.git_branch),
    taskRef: null,
    title: session.task_title,
    parentSessionId: parents.get(key(session.source, session.session_id)) ?? null,
    startedAt: session.started_at,
    lastActionAt: session.last_action_at,
    commits: (feature.commit_shas ?? []).map((row, index) => ({
      sha: row.value, ts: row.timestamp, eventId: `snapshot-commit:${index}`, reverted: null,
    })),
    // The shared-session snapshot retained neither legacy session outcomes nor
    // successful tool-result bodies, so no read-API outcome fact can be rebuilt.
    outcomes: [],
    usage: [],
  };
}

function snapshotLanes(sessions: SessionRow[], features: FeatureRow[]): LaneFacts[] {
  const byFeature = new Map(features.map((row) => [key(row.source, row.session_id), row]));
  const parents = parentMap(sessions);
  return sessions.map((session, sessionIndex) => {
    const feature = byFeature.get(key(session.source, session.session_id));
    if (!feature) throw new Error(`missing feature row at session ordinal ${sessionIndex}`);
    const lane = baseLane(session, feature, parents);
    return {
      ...lane,
      writes: (feature.files_written ?? []).map((row, index) => ({
        path: row.value,
        ts: row.timestamp,
        eventId: `snapshot-write:${index}`,
        kind: "snapshot_feature",
        branch: lane.branch,
      })),
    };
  }).sort((a, b) => key(a.source, a.sessionId).localeCompare(key(b.source, b.sessionId)));
}

const normalizedToolName = (name: string) => (name.split(/[.:/]/).at(-1) ?? "").replaceAll(/[_-]/g, "").toLowerCase();

function eventFile(snapshotDir: string, session: SessionRow): string {
  const safe = (value: string) => value.replace(/[^A-Za-z0-9_.-]+/g, "_");
  return join(snapshotDir, "events", `${safe(session.source)}__${safe(session.session_id)}.ndjson`);
}

async function currentLanes(snapshotDir: string, sessions: SessionRow[], features: FeatureRow[]): Promise<{ lanes?: LaneFacts[]; gap?: ProjectionGap }> {
  const byFeature = new Map(features.map((row) => [key(row.source, row.session_id), row]));
  const parents = parentMap(sessions);
  const lanes: LaneFacts[] = [];
  let relevantToolCalls = 0;
  let missingArgsJson = 0;
  let truncatedCompactInputs = 0;
  let sampledSessions = 0;
  for (let sessionIndex = 0; sessionIndex < sessions.length; sessionIndex += 1) {
    const session = sessions[sessionIndex]!;
    const feature = byFeature.get(key(session.source, session.session_id));
    if (!feature) throw new Error(`missing feature row at session ordinal ${sessionIndex}`);
    let events: SnapshotEvent[];
    try {
      events = await readNdjson<SnapshotEvent>(eventFile(snapshotDir, session));
    } catch {
      throw new Error(`current projection could not parse manifest-verified event file at session ordinal ${sessionIndex}`);
    }
    const meta = events.find((event) => event.type === "session_meta");
    if (meta?.bounded_sample === true) sampledSessions += 1;
    const records: RelayhistoryStreamRecord[] = [];
    for (const event of events) {
      const name = typeof event.tool_name === "string" ? event.tool_name : "";
      const relevantExec = session.source === "codex" && name === "exec";
      const relevantMultiEdit = session.source === "claude" && normalizedToolName(name) === "multiedit";
      const relevantFileEdit = event.type === "file_edit";
      if (!relevantExec && !relevantMultiEdit && !relevantFileEdit) continue;
      relevantToolCalls += 1;
      const argsJson = typeof event.args_json === "string"
        ? event.args_json
        : event.tool_input && typeof event.tool_input === "object" ? JSON.stringify(event.tool_input) : undefined;
      if (!relevantFileEdit && !argsJson) {
        missingArgsJson += 1;
        if (event.content_truncated === true) truncatedCompactInputs += 1;
        continue;
      }
      if (argsJson) {
        try { asObject(JSON.parse(argsJson), "tool args_json"); }
        catch { missingArgsJson += 1; continue; }
      }
      const timestamp = String(event.timestamp ?? "");
      const eventId = String(event.event_id ?? "");
      if (!timestamp || !eventId || !Number.isFinite(Date.parse(timestamp))) throw new Error("current projection event lacks timestamp or event_id");
      const payload: Json = {
        ts_ms: Date.parse(timestamp),
        name,
        args_json: argsJson ?? null,
        is_error: event.status === "error",
        ...(relevantFileEdit && typeof event.file_path === "string" ? { file_path: event.file_path } : {}),
      };
      records.push({
        kind: relevantFileEdit ? "file_edit" : "tool_call",
        origin_id: `snapshot:${session.source}:${session.session_id}`,
        record_id: eventId,
        revision: "1",
        operation: "upsert",
        received_at: timestamp,
        payload,
      });
    }
    const base = baseLane(session, feature, parents);
    let projected: ReturnType<typeof projectRelayhistorySession>;
    try {
      projected = projectRelayhistorySession(records, {
        workspaceId: "offline-evaluation",
        source: session.source,
        sessionId: session.session_id,
        machineId: session.session_id,
        userId: base.userId ?? "unknown",
      });
    } catch {
      throw new Error(`current projection failed for source ${session.source} at session ordinal ${sessionIndex}`);
    }
    const writes = projected.rows
      .filter((item) => item.table === "convergence_events")
      .flatMap((item) => {
        const row = item.row;
        const paths = Array.isArray(row.files_touched) ? row.files_touched : [];
        return paths.filter((path): path is string => typeof path === "string" && path.length > 0).map((path) => ({
          path,
          ts: String(row.ts),
          eventId: String(row.event_id),
          kind: String(row.kind),
          // The compact snapshot omitted task_ref. The directory branch is the
          // only retained branch evidence and is the same fallback lane metadata
          // the read API exposes.
          branch: base.branch,
        }));
      });
    lanes.push({ ...base, writes });
  }
  if (missingArgsJson > 0) {
    return { gap: {
      code: "raw_tool_input_unavailable",
      message: "Current projection requires complete tool-call args_json (Codex exec input and Claude MultiEdit arguments), but this frozen snapshot retained only command_or_target text capped at 600 characters; stored files_written was not used.",
      relevantToolCalls,
      missingArgsJson,
      truncatedCompactInputs,
      sampledSessions,
    } };
  }
  lanes.sort((a, b) => key(a.source, a.sessionId).localeCompare(key(b.source, b.sessionId)));
  return { lanes };
}

async function loadSnapshot(snapshotDir: string, projection: Projection): Promise<LoadedSnapshot> {
  const root = await realpath(snapshotDir);
  const { manifest, manifestHash } = await verifyManifest(root);
  const sessions = await readNdjson<SessionRow>(join(root, "sessions.ndjson"));
  const features = await readNdjson<FeatureRow>(join(root, "features.ndjson"));
  if (sessions.length !== features.length) throw new Error("snapshot sessions/features row count mismatch");
  if (projection === "snapshot") {
    const lanes = snapshotLanes(sessions, features);
    return {
      manifest, manifestHash, sessions, features, lanes,
      work: {
        writes: lanes.reduce((sum, lane) => sum + lane.writes.length, 0),
        commits: lanes.reduce((sum, lane) => sum + lane.commits.length, 0),
        outcomes: lanes.reduce((sum, lane) => sum + lane.outcomes.length, 0),
      },
    };
  }
  const current = await currentLanes(root, sessions, features);
  const lanes = current.lanes;
  return {
    manifest, manifestHash, sessions, features, lanes, projectionGap: current.gap,
    work: {
      writes: lanes?.reduce((sum, lane) => sum + lane.writes.length, 0) ?? 0,
      commits: lanes?.reduce((sum, lane) => sum + lane.commits.length, 0) ?? 0,
      outcomes: lanes?.reduce((sum, lane) => sum + lane.outcomes.length, 0) ?? 0,
    },
  };
}

function truthPaths(truthFile: string): { split: string; incidents: string; negatives: string; prIncidents: string } {
  const absolute = resolve(truthFile);
  if (basename(absolute) !== "split.json") throw new Error("--truth must name the truth bundle's split.json");
  const root = dirname(absolute);
  return {
    split: absolute,
    incidents: join(root, "incidents.ndjson"),
    negatives: join(root, "negatives.ndjson"),
    prIncidents: join(root, "pr_incidents.ndjson"),
  };
}

async function loadTruth(truthFile: string): Promise<LoadedTruth> {
  const paths = truthPaths(truthFile);
  const [split, incidents, negatives, prIncidents] = await Promise.all([
    readJson(paths.split),
    readNdjson<TruthIncident>(paths.incidents),
    readNdjson<NegativeRow>(paths.negatives),
    readNdjson<PrIncident>(paths.prIncidents),
  ]);
  const hashes = await hashTruthPaths(paths);
  return { split: split as unknown as SplitFile, incidents, negatives, prIncidents, hashes };
}

async function hashTruthPaths(paths: ReturnType<typeof truthPaths>): Promise<Record<string, string>> {
  const hashes: Record<string, string> = {};
  for (const [name, path] of Object.entries(paths).sort(([a], [b]) => a.localeCompare(b))) hashes[name] = await sha256File(path);
  return hashes;
}

function shaMatches(observed: string, expected: string): boolean {
  const left = observed.toLowerCase(), right = expected.toLowerCase();
  return Math.min(left.length, right.length) >= 7 && (left.startsWith(right) || right.startsWith(left));
}

function sideReference(feature: FeatureRow | undefined, side: PrIncident["side_a"], repo: string): boolean {
  if (!feature) return false;
  const prs = new Set(side.prs ?? []);
  if (feature.pull_requests?.some((ref) => ref.repo === repo && typeof ref.number === "number" && prs.has(ref.number))) return true;
  const branches = new Set(side.branches ?? []);
  if (feature.git_branches_seen?.some((ref) => branches.has(ref.value))) return true;
  const shas = side.shas ?? [];
  return feature.commit_shas?.some((ref) => shas.some((sha) => shaMatches(ref.value, sha))) ?? false;
}

function findingPair(finding: Finding): string {
  return pairKey(...finding.lanes.map((lane) => key(lane.source, lane.sessionId)) as [string, string]);
}

function matchingIncidents(finding: Finding, truth: LoadedTruth, features: Map<string, FeatureRow>): PrIncident[] {
  const [left, right] = finding.lanes;
  if (!left || !right) return [];
  const leftFeature = features.get(key(left.source, left.sessionId));
  const rightFeature = features.get(key(right.source, right.sessionId));
  return truth.prIncidents.filter((incident) => incident.status === undefined || incident.status === "scored")
    .filter((incident) =>
      (sideReference(leftFeature, incident.side_a, incident.repo) && sideReference(rightFeature, incident.side_b, incident.repo)) ||
      (sideReference(rightFeature, incident.side_a, incident.repo) && sideReference(leftFeature, incident.side_b, incident.repo)));
}

function scored(findings: Finding[], split: Split, truth: LoadedTruth, featureRows: FeatureRow[], sessionCount: number): ScoredBlock {
  const features = new Map(featureRows.map((row) => [key(row.source, row.session_id), row]));
  const incidentIds = new Set(truth.split.incidents[split] ?? []);
  const negativeIds = new Set(truth.split.negatives[split] ?? []);
  const positivePairs = new Map<string, string>();
  for (const incident of truth.incidents) {
    if (!incidentIds.has(incident.incident_id) || (incident.status !== undefined && incident.status !== "scored") || incident.sessions.length !== 2) continue;
    positivePairs.set(pairKey(...incident.sessions.map((row) => key(row.source, row.session_id)) as [string, string]), incident.incident_id);
  }
  const negativePairs = new Set(truth.negatives
    .filter((row) => negativeIds.has(row.negative_id) && row.sessions.length === 2)
    .map((row) => pairKey(...row.sessions.map((ref) => key(ref.source, ref.session_id)) as [string, string])));
  const found = new Set<string>();
  const beforeHarm = new Set<string>();
  let confirmedTrue = 0, confirmedFalse = 0, unlabeled = 0;
  for (const finding of findings) {
    const pair = findingPair(finding);
    const matches = matchingIncidents(finding, truth, features).filter((incident) => incident.split === split && incidentIds.has(incident.id));
    for (const incident of matches) {
      found.add(incident.id);
      if (finding.alertAt < incident.harm_at) beforeHarm.add(incident.id);
    }
    const exactIncident = positivePairs.get(pair);
    if (exactIncident) found.add(exactIncident);
    if (exactIncident || matches.length > 0) confirmedTrue += 1;
    else if (negativePairs.has(pair)) confirmedFalse += 1;
    else unlabeled += 1;
  }
  const denominator = confirmedTrue + confirmedFalse;
  const total = incidentIds.size;
  return {
    alerts: findings.length,
    alertBudgetPer100Sessions: round(findings.length * 100 / Math.max(1, sessionCount)),
    adjudicatedPairs: {
      confirmedTrue,
      confirmedFalse,
      denominator,
      precision: divide(confirmedTrue, denominator),
      unlabeledAlerts: unlabeled,
    },
    incidents: {
      found: found.size,
      total,
      recall: divide(found.size, total),
      foundIds: [...found].sort(),
      beforeHarmIds: [...beforeHarm].sort(),
    },
  };
}

function detectorReport(output: DetectorOutput, tiers: Record<string, Finding[]>, truth: LoadedTruth, features: FeatureRow[], sessions: number, lanes: LaneFacts[], writes: number): DetectorReport {
  return {
    name: output.detector.name,
    version: output.detector.version,
    paramsHash: output.detector.paramsHash,
    runtime: {
      measurement: "external_wall_clock",
      note: "Wall-clock milliseconds are printed by the CLI, not embedded, so report artifacts remain byte-identical.",
      lanes: lanes.length,
      writes,
      findings: output.findings.length,
    },
    tiers: Object.fromEntries(Object.entries(tiers).map(([name, findings]) => [name, {
      dev: scored(findings, "dev", truth, features, sessions),
      heldout: scored(findings, "heldout", truth, features, sessions),
    }])),
  };
}

function gitSha(): string {
  return execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim();
}

function tiered(output: DetectorOutput): Record<string, Finding[]> {
  const byTier: Record<string, Finding[]> = { overall: output.findings };
  if (output.detector.name === "outcome-facts") {
    byTier.T1 = output.findings.filter((finding) => finding.kind === "duplicate_work");
    byTier.T2 = output.findings.filter((finding) => finding.kind === "superseded_work");
    byTier.T3 = output.findings.filter((finding) => finding.kind === "concurrent_edit");
  } else {
    byTier.baseline = output.findings;
  }
  return byTier;
}

export async function evaluate(options: { snapshotDir: string; truthFile: string; projection: Projection }): Promise<{ report: EvaluationReport; wallClockMs: number }> {
  const started = performance.now();
  // Detection and input construction intentionally happen before truth is read.
  // This keeps held-out labels scorer-only.
  const snapshot = await loadSnapshot(options.snapshotDir, options.projection);
  const outputs: DetectorOutput[] = [];
  if (snapshot.lanes) {
    const input: DetectionInput = {
      workspaceId: `offline:${snapshot.manifest.snapshot_id}`,
      window: { since: snapshot.manifest.window_start, until: snapshot.manifest.window_end },
      lanes: snapshot.lanes,
    };
    outputs.push(outcomeFacts.detect(input, {}, 200));
    outputs.push(sharedWrite60m.detect(input, {}, 200));
  }
  const unavailable = snapshot.projectionGap !== undefined;
  // Hashing an unavailable run's truth inputs does not parse or expose labels.
  // Held-out labels are parsed only when findings are ready to score.
  const truth = snapshot.lanes ? await loadTruth(options.truthFile) : undefined;
  const truthHashes = truth?.hashes ?? await hashTruthPaths(truthPaths(options.truthFile));
  const report: EvaluationReport = {
    contract: "coordination-eval/1",
    status: unavailable ? "projection_unavailable" : "complete",
    projection: options.projection,
    inputs: {
      snapshotId: snapshot.manifest.snapshot_id,
      snapshotManifestSha256: snapshot.manifestHash,
      truthSha256: truthHashes,
      sessionCount: snapshot.sessions.length,
    },
    provenance: {
      gitSha: gitSha(),
      reportDeterminism: "No timestamps or measured durations are embedded; sorted inputs and stable detector outputs produce byte-identical artifacts.",
    },
    coverage: {
      status: unavailable ? "unavailable" : "complete",
      notes: [
        "Snapshot manifest hashes were verified before evaluation.",
        "The snapshot retained no legacy outcome records or successful tool-result bodies; T2 outcome facts are unavailable.",
        "Held-out labels were loaded only after all detector runs completed.",
        ...(options.projection === "current" ? ["Stored snapshot files_written was not used for current projection."] : []),
      ],
      ...(snapshot.projectionGap ? { projectionGap: snapshot.projectionGap } : {}),
    },
    detectors: outputs.map((output) => detectorReport(
      output,
      tiered(output),
      truth!,
      snapshot.features,
      snapshot.sessions.length,
      snapshot.lanes!,
      snapshot.work.writes,
    )),
  };
  return { report, wallClockMs: performance.now() - started };
}

export function markdown(report: EvaluationReport): string {
  const lines = [
    "# Coordination overlap evaluation",
    "",
    `- Status: \`${report.status}\``,
    `- Projection: \`${report.projection}\``,
    `- Snapshot: \`${report.inputs.snapshotId}\` (${report.inputs.sessionCount} sessions)`,
    `- Git SHA: \`${report.provenance.gitSha}\``,
    "",
  ];
  if (report.coverage.projectionGap) {
    const gap = report.coverage.projectionGap;
    lines.push("## Projection unavailable", "", gap.message, "", `Relevant tool calls: ${gap.relevantToolCalls}; missing raw arguments: ${gap.missingArgsJson}; capped inputs: ${gap.truncatedCompactInputs}; sampled sessions: ${gap.sampledSessions}.`, "");
  }
  for (const detector of report.detectors) {
    lines.push(`## ${detector.name} ${detector.version}`, "", `Parameters hash: \`${detector.paramsHash}\``, "", "| Tier | Split | Precision (adjudicated) | Recall | Found incidents | Alerts / 100 sessions |", "|---|---|---:|---:|---|---:|");
    for (const [tier, splits] of Object.entries(detector.tiers)) for (const split of ["dev", "heldout"] as const) {
      const block = splits[split];
      const precision = block.adjudicatedPairs.precision === null ? "n/a" : block.adjudicatedPairs.precision.toFixed(3);
      const recall = block.incidents.recall === null ? "n/a" : `${block.incidents.found}/${block.incidents.total}`;
      lines.push(`| ${tier} | ${split} | ${precision} (${block.adjudicatedPairs.denominator}) | ${recall} | ${block.incidents.foundIds.join(", ") || "—"} | ${block.alertBudgetPer100Sessions} |`);
    }
    lines.push("");
  }
  lines.push("## Coverage notes", "", ...report.coverage.notes.map((note) => `- ${note}`), "");
  return `${lines.join("\n")}\n`;
}

export async function writeReports(outDir: string, report: EvaluationReport): Promise<void> {
  await mkdir(outDir, { recursive: true });
  await writeFile(join(outDir, "report.json"), `${JSON.stringify(report, null, 2)}\n`, "utf8");
  await writeFile(join(outDir, "report.md"), markdown(report), "utf8");
}

export async function reportsAreByteIdentical(left: string, right: string): Promise<boolean> {
  for (const name of ["report.json", "report.md"]) {
    const [a, b] = await Promise.all([readFile(join(left, name)), readFile(join(right, name))]);
    if (!a.equals(b)) return false;
  }
  return true;
}
