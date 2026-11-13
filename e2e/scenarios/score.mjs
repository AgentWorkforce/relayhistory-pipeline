#!/usr/bin/env node
/** Score real-session ground truth against the kind read API and its fact store. */
import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const manifestDefault = fileURLToPath(new URL("./manifest.json", import.meta.url));
const args = new Map();
for (let i = 2; i < process.argv.length;) {
  const key = process.argv[i++];
  if (key === "--in-process") { args.set("in-process", true); continue; }
  if (!key?.startsWith("--") || i >= process.argv.length) {
    throw new Error("usage: score.mjs [--in-process] [--detector-module path] [--manifest path] [--response path] [--evidence path] [--ledger-response path] [--workspace id] [--since ISO] [--until ISO]");
  }
  args.set(key.slice(2), process.argv[i++]);
}

function requiredId(value, name) {
  if (!value || !/^[A-Za-z0-9._:-]{1,255}$/.test(value)) throw new Error(`invalid ${name}`);
  return value;
}

async function run(command, argv, input) {
  const child = spawn(command, argv, { stdio: ["pipe", "pipe", "pipe"] });
  const timeout = setTimeout(() => child.kill("SIGTERM"), 15_000);
  const stdout = [], stderr = [];
  child.stdout.on("data", (chunk) => stdout.push(chunk));
  child.stderr.on("data", (chunk) => stderr.push(chunk));
  child.stdin.end(input);
  const code = await new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("close", resolve);
  });
  clearTimeout(timeout);
  if (code !== 0) throw new Error(`${command} exited ${code}: ${Buffer.concat(stderr).toString("utf8").trim()}`);
  return Buffer.concat(stdout).toString("utf8").trim();
}

const kube = ["exec", "-i", "agent-relay-local-control-plane", "kubectl", "--kubeconfig", "/etc/kubernetes/admin.conf", "-n", "agent-relay", "exec", "-i"];

async function fetchKindOverlaps(workspace, since, until) {
  const token = process.env.SCENARIO_READ_API_TOKEN;
  if (!token) throw new Error("SCENARIO_READ_API_TOKEN is required for a live read; keep it in the environment only");
  const url = new URL("http://127.0.0.1:8080/v1/coordination/overlaps");
  url.searchParams.set("workspace", workspace);
  url.searchParams.set("since", since);
  url.searchParams.set("until", until);
  url.searchParams.set("limit", "200");
  const script = "const reader=Bun.stdin.stream().getReader();let line='';while(!line.includes('\\n')){const part=await reader.read();if(part.done)break;line+=new TextDecoder().decode(part.value)}const input=JSON.parse(line.trim());const r=await fetch(input.url,{headers:{authorization:`Bearer ${input.token}`},signal:AbortSignal.timeout(10000)});console.log(JSON.stringify({status:r.status,body:await r.json()}));";
  const raw = await run("docker", [...kube, "deploy/session-read-api", "--", "bun", "-e", script], JSON.stringify({ url: url.toString(), token }) + "\n");
  const response = JSON.parse(raw);
  if (response.status !== 200) throw new Error(`read API returned ${response.status}: ${JSON.stringify(response.body)}`);
  return response.body;
}

async function probeKindEvidence(workspace, sessions, since, until) {
  if (!sessions.length) return { sessions: [] };
  const ids = [...new Set(sessions.map((session) => requiredId(session.session_id, "session ID")))];
  const sqlIds = ids.map((id) => `'${id}'`).join(",");
  const start = new Date(Date.parse(since) - 7 * 24 * 60 * 60 * 1000).toISOString();
  const end = new Date(until).toISOString();
  const sql = `WITH ids AS (SELECT unnest(ARRAY[${sqlIds}]::text[]) AS session_id)
    SELECT coalesce(json_agg(row_to_json(x)), '[]'::json) FROM (
      SELECT ids.session_id,
        (SELECT count(*) FROM sessions.session_directory d WHERE d.workspace_id='${workspace}' AND d.session_id=ids.session_id) AS directory_count,
        (SELECT max(d.git_repo) FROM sessions.session_directory d WHERE d.workspace_id='${workspace}' AND d.session_id=ids.session_id) AS git_repo,
        (SELECT count(*) FROM sessions.session_event_facts e WHERE e.workspace_id='${workspace}' AND e.session_id=ids.session_id) AS event_count,
        (SELECT count(*) FROM sessions.session_event_facts e WHERE e.workspace_id='${workspace}' AND e.session_id=ids.session_id AND e.ts >= '${start}' AND e.ts < '${end}' AND cardinality(e.files_touched)>0) AS write_count,
        (SELECT count(*) FROM sessions.session_event_facts e WHERE e.workspace_id='${workspace}' AND e.session_id=ids.session_id AND e.ts < '2000-01-01') AS ancient_event_count,
        (SELECT array_agg(DISTINCT e.project_id) FROM sessions.session_event_facts e WHERE e.workspace_id='${workspace}' AND e.session_id=ids.session_id AND e.project_id IS NOT NULL) AS fact_project_ids,
        (SELECT count(*) FROM sessions.session_output_facts o WHERE o.workspace_id='${workspace}' AND o.session_id=ids.session_id) AS output_count,
        (SELECT count(*) FROM sessions.session_output_facts o WHERE o.workspace_id='${workspace}' AND o.session_id=ids.session_id AND o.ts < '2000-01-01') AS ancient_output_count,
        (SELECT count(*) FROM sessions.session_output_facts o WHERE o.workspace_id='${workspace}' AND o.session_id=ids.session_id AND o.ts >= '${start}' AND o.ts < '${end}' AND o.fact_kind='commit') AS commit_count,
        (SELECT count(*) FROM sessions.session_output_facts o WHERE o.workspace_id='${workspace}' AND o.session_id=ids.session_id AND o.ts >= '${start}' AND o.ts < '${end}' AND o.fact_kind='outcome') AS outcome_count
      FROM ids ORDER BY ids.session_id
    ) x`;
  const raw = await run("docker", [...kube, "postgres-0", "--", "psql", "-U", "postgres", "-d", "hoopdb", "-X", "-At", "-v", "ON_ERROR_STOP=1", "-c", sql], "");
  return { sessions: JSON.parse(raw) };
}

async function readKindFacts(workspace, sessions, since, until) {
  if (!sessions.length) return { events: [], outputs: [], directory: [], relationships: [] };
  const sqlIds = [...new Set(sessions.map((session) => requiredId(session.session_id, "session ID")))].map((id) => `'${id}'`).join(",");
  const start = new Date(Date.parse(since) - 7 * 24 * 60 * 60 * 1000).toISOString();
  const end = new Date(until).toISOString();
  const sql = `SELECT json_build_object(
    'events', (SELECT coalesce(json_agg(json_build_object('source',e.source,'session_id',e.session_id,'ts',e.ts,'event_id',e.event_id,'kind',e.kind,'project_id',e.project_id,'user_id',e.user_id,'branch',e.branch,'files_touched',e.files_touched,'input_tokens',e.input_tokens,'output_tokens',e.output_tokens,'reasoning_tokens',e.reasoning_tokens,'cost_usd_micros',e.cost_usd_micros)), '[]'::json)
      FROM sessions.session_event_facts e WHERE e.workspace_id='${workspace}' AND e.session_id IN (${sqlIds}) AND e.ts >= '${start}' AND e.ts < '${end}'),
    'outputs', (SELECT coalesce(json_agg(json_build_object('source',o.scope_source,'session_id',o.session_id,'ts',o.ts,'row_id',o.row_id,'fact_kind',o.fact_kind,'fact_key',o.fact_key,'reverted',o.reverted)), '[]'::json)
      FROM sessions.session_output_facts o WHERE o.workspace_id='${workspace}' AND o.session_id IN (${sqlIds}) AND o.ts >= '${start}' AND o.ts < '${end}'),
    'directory', (SELECT coalesce(json_agg(json_build_object('harness',d.harness,'session_id',d.session_id,'created_at',d.created_at,'last_activity_at',d.last_activity_at,'git_repo',d.git_repo,'git_branch',d.git_branch,'title',d.title)), '[]'::json)
      FROM sessions.session_directory d WHERE d.workspace_id='${workspace}' AND d.session_id IN (${sqlIds})),
    'relationships', (SELECT coalesce(json_agg(json_build_object('harness',r.harness,'child_session_id',r.child_session_id,'parent_session_id',r.parent_session_id)), '[]'::json)
      FROM sessions.session_directory_relationships r WHERE r.workspace_id='${workspace}' AND r.child_session_id IN (${sqlIds})))`;
  const raw = await run("docker", [...kube, "postgres-0", "--", "psql", "-U", "postgres", "-d", "hoopdb", "-X", "-At", "-v", "ON_ERROR_STOP=1", "-c", sql], "");
  return JSON.parse(raw);
}

async function detectInProcess(workspace, sessions, since, until) {
  const detectorModule = args.get("detector-module") ?? fileURLToPath(new URL("../../packages/coordination-detect/src/index.ts", import.meta.url));
  const { detectCoordination } = await import(pathToFileURL(resolve(detectorModule)).href);
  const facts = await readKindFacts(workspace, sessions, since, until);
  const key = (source, sessionId) => `${source}\u0000${sessionId}`;
  const eventsByKey = new Map(), outputsByKey = new Map();
  for (const event of facts.events) {
    const k = key(event.source, event.session_id);
    eventsByKey.set(k, [...(eventsByKey.get(k) ?? []), event]);
  }
  for (const output of facts.outputs) {
    const k = key(output.source, output.session_id);
    outputsByKey.set(k, [...(outputsByKey.get(k) ?? []), output]);
  }
  const directoryByKey = new Map(facts.directory.map((row) => [key(row.harness, row.session_id), row]));
  const parentSets = new Map();
  for (const row of facts.relationships) {
    const k = key(row.harness, row.child_session_id);
    const parents = parentSets.get(k) ?? new Set();
    parents.add(row.parent_session_id);
    parentSets.set(k, parents);
  }
  const identities = new Map(sessions.map((session) => [key(session.harness, session.session_id), session]));
  const lanes = [...identities].flatMap(([k, session]) => {
    const events = eventsByKey.get(k) ?? [], outputs = outputsByKey.get(k) ?? [];
    if (!events.length && !outputs.length) return [];
    const directory = directoryByKey.get(k);
    const lastEvent = [...events].sort((a, b) => a.ts.localeCompare(b.ts)).at(-1);
    const latest = (...values) => values.find((value) => value != null) ?? null;
    const iso = (value) => new Date(value).toISOString();
    return [{
      sessionId: session.session_id, source: session.harness,
      userId: lastEvent?.user_id ?? null, projectId: lastEvent?.project_id ?? null,
      repo: directory?.git_repo ?? null, branch: latest(lastEvent?.branch, directory?.git_branch),
      taskRef: null, title: directory?.title ?? null, parentSessionId: parentSets.get(k)?.size === 1 ? [...parentSets.get(k)][0] : null,
      startedAt: iso(directory?.created_at ?? events[0]?.ts ?? outputs[0].ts),
      lastActionAt: iso(directory?.last_activity_at ?? lastEvent?.ts ?? outputs.at(-1).ts),
      writes: events.flatMap((event) => (event.files_touched ?? []).filter(Boolean).map((path) => ({ path, ts: iso(event.ts), eventId: event.event_id, kind: event.kind, branch: event.branch }))),
      commits: outputs.filter((output) => output.fact_kind === "commit").map((output) => ({ sha: output.fact_key, ts: iso(output.ts), eventId: output.row_id, reverted: null })),
      outcomes: outputs.filter((output) => output.fact_kind === "outcome").map((output) => ({ key: output.fact_key, ts: iso(output.ts), eventId: output.row_id, reverted: Number(output.reverted) !== 0 })),
      usage: events.map((event) => ({ ts: iso(event.ts), eventId: event.event_id,
        tokens: Number(event.input_tokens) + Number(event.output_tokens) + Number(event.reasoning_tokens),
        costUsdMicros: event.cost_usd_micros == null ? null : Number(event.cost_usd_micros) })),
    }];
  });
  const detected = detectCoordination({ workspaceId: workspace, window: { since, until }, lanes }, { limit: 200 });
  return {
    contract: "coordination-overlaps/1", window: { since, until }, detector: detected.detector,
    coverage: { status: "local_fact_replay", sessionsConsidered: lanes.length, notes: ["Detector run in process against current kind facts; no read API request used."] },
    summary: detected.summary, findings: detected.findings,
  };
}

function validateManifest(manifest) {
  if (manifest.schemaVersion !== 1 || !Array.isArray(manifest.scenarios)) throw new Error("unsupported manifest");
  for (const scenario of manifest.scenarios) {
    if (!["pending", "complete"].includes(scenario.status)) throw new Error(`invalid status in ${scenario.id}`);
    const roles = new Set(scenario.sessions?.map((session) => session.role));
    if (roles.size !== scenario.sessions?.length || !Array.isArray(scenario.pairs)) throw new Error(`invalid scenario ${scenario.id}`);
    if (scenario.pairs.length === 0 && !scenario.expected_episodes?.length) throw new Error(`scenario ${scenario.id} has no expectations`);
    for (const session of scenario.sessions) {
      if (!manifest.projects[session.expected_project]) throw new Error(`unknown project in ${scenario.id}`);
      if (session.session_id !== null) requiredId(session.session_id, "session ID");
      if ((session.started_at === null) !== (session.ended_at === null)) throw new Error(`incomplete times in ${scenario.id}`);
      if (scenario.status === "complete" && (session.session_id === null && session.harness !== "human" || session.started_at === null)) throw new Error(`missing completed session in ${scenario.id}`);
      if (session.started_at && Date.parse(session.started_at) > Date.parse(session.ended_at)) throw new Error(`reversed times in ${scenario.id}`);
    }
    for (const pair of scenario.pairs) if (!roles.has(pair.left) || !roles.has(pair.right)) throw new Error(`unknown pair role in ${scenario.id}`);
  }
}

const manifest = JSON.parse(await readFile(args.get("manifest") ?? manifestDefault, "utf8"));
validateManifest(manifest);
const sessions = manifest.scenarios.flatMap((scenario) => scenario.sessions).filter((session) => session.session_id);
const workspace = args.get("workspace") ?? process.env.SCENARIO_WORKSPACE_ID;
const now = Date.now();
const since = args.get("since") ?? new Date(now - 24 * 60 * 60 * 1000).toISOString();
const until = args.get("until") ?? new Date(now + 60 * 1000).toISOString();
if (!(Date.parse(since) < Date.parse(until))) throw new Error("since must precede until");
if (!args.has("response") && !workspace) throw new Error("--workspace or SCENARIO_WORKSPACE_ID is required for a live read");
const overlaps = args.has("response") ? JSON.parse(await readFile(args.get("response"), "utf8"))
  : args.has("in-process") ? await detectInProcess(requiredId(workspace, "workspace ID"), sessions, since, until)
  : await fetchKindOverlaps(requiredId(workspace, "workspace ID"), since, until);
if (overlaps.contract !== "coordination-overlaps/1" || !Array.isArray(overlaps.findings)) throw new Error("unexpected overlaps contract");
const evidence = args.has("evidence") ? JSON.parse(await readFile(args.get("evidence"), "utf8"))
  : workspace ? await probeKindEvidence(requiredId(workspace, "workspace ID"), sessions, since, until) : null;
const evidenceById = new Map(evidence?.sessions?.map((row) => [row.session_id, row]) ?? []);
const ledger = args.has("ledger-response") ? JSON.parse(await readFile(args.get("ledger-response"), "utf8")) : null;
const ledgerEntries = ledger ? (ledger.entries ?? ledger.ledger?.entries) : null;
if (ledger && !Array.isArray(ledgerEntries)) throw new Error("ledger response has no entries array");

const suppressing = new Set(["none", "planned_parallel", "continuation"]);
const normalizedRepo = (value) => value?.trim().replace(/^git@github\.com:/i, "").replace(/^https?:\/\/github\.com\//i, "").replace(/\.git$/i, "").toLowerCase() || null;
function requiredSignalMissing(pair, counts) {
  if (!counts.every(Boolean)) return false;
  if (pair.required_evidence === "writes_both" && counts.some((row) => Number(row.write_count) === 0)) return true;
  const label = pair.expected_label;
  if (label === "duplicate_work" || label === "concurrent_edit") return counts.some((row) => Number(row.write_count) === 0);
  if (label === "superseded_work") return Number(counts[0].commit_count) === 0 || Number(counts[1].outcome_count) === 0;
  return false;
}
const results = [];
for (const scenario of manifest.scenarios) {
  const byRole = new Map(scenario.sessions.map((session) => [session.role, session]));
  for (const pair of scenario.pairs) {
    const left = byRole.get(pair.left), right = byRole.get(pair.right);
    const base = { scenario: scenario.id, pair: `${pair.left}:${pair.right}`, expected: pair.expected_label,
      ...(pair.expected_actor_kinds ? { expected_actor_kinds: pair.expected_actor_kinds } : {}) };
    if (scenario.status !== "complete" || (!left.session_id && left.harness !== "human") || (!right.session_id && right.harness !== "human")) {
      results.push({ ...base, state: "pending" });
      continue;
    }
    if (!left.session_id || !right.session_id) {
      const captured = left.session_id ? left : right;
      results.push({ ...base, state: "no_evidence", reason: "human-authored commit outside a captured session has no session facts", captured_evidence: evidenceById.get(captured.session_id) ?? null });
      continue;
    }
    const laneMatch = (lane, session) => lane.sessionId === session.session_id && lane.source === session.harness;
    const findings = overlaps.findings.filter((finding) => Array.isArray(finding.lanes) && finding.lanes.some((lane) => laneMatch(lane, left)) && finding.lanes.some((lane) => laneMatch(lane, right)));
    const counts = [left, right].map((session) => evidenceById.get(session.session_id));
    const lacksEvidence = counts.some((row) => row && Number(row.event_count) + Number(row.output_count) === 0);
    const projectTagState = pair.required_project_tags && evidence ? [left, right].map((session, index) => {
      const observed = normalizedRepo(counts[index]?.git_repo);
      return observed === manifest.projects[session.expected_project] ? null : observed ? "wrong_project" : "missing_project_tag";
    }).find(Boolean) : null;
    const state = evidence && counts.some((row) => !row) ? "unverified_evidence"
      : lacksEvidence ? "no_evidence"
      : requiredSignalMissing(pair, counts) ? "missing_signal"
      : projectTagState ? projectTagState
      : pair.expected_label === "planned_parallel" && findings.length > 0 && findings.every((finding) => finding.score < 0.3) ? "correct_suppression"
      : findings.some((finding) => finding.kind === pair.expected_label) ? "hit"
      : suppressing.has(pair.expected_label) && findings.length === 0 ? "correct_absence"
      : findings.length ? "wrong_relation"
      : evidence ? "evidence_no_finding" : "unverified_no_finding";
    const defects = counts.flatMap((row) => row && Number(row.ancient_event_count) + Number(row.ancient_output_count) > 0
      ? [{ session_id: row.session_id, kind: "evidence_defect", reason: "fact timestamp before 2000-01-01", count: Number(row.ancient_event_count) + Number(row.ancient_output_count) }]
      : []);
    results.push({ ...base, state, observed: findings.map((finding) => ({ kind: finding.kind, id: finding.id, confidence: finding.confidence })), evidence: counts, defects });
  }
}

const summary = Object.fromEntries([...new Set(results.map((row) => row.state))].sort().map((state) => [state, results.filter((row) => row.state === state).length]));
const evidenceDefects = (evidence?.sessions ?? []).flatMap((row) => Number(row.ancient_event_count) + Number(row.ancient_output_count) > 0
  ? [{ session_id: row.session_id, kind: "evidence_defect", reason: "fact timestamp before 2000-01-01", count: Number(row.ancient_event_count) + Number(row.ancient_output_count) }]
  : []);
const projectResults = [...new Map(manifest.scenarios.flatMap((scenario) => scenario.sessions)
  .filter((session) => session.session_id)
  .map((session) => [`${session.harness}:${session.session_id}`, session])).values()].map((session) => {
    const expected = manifest.projects[session.expected_project];
    const row = evidenceById.get(session.session_id);
    if (!row) return { session_id: session.session_id, harness: session.harness, expected, state: "unverified" };
    const observed = normalizedRepo(row.git_repo);
    return {
      session_id: session.session_id, harness: session.harness, expected, observed,
      state: observed === expected ? "hit" : observed ? "wrong_project" : "missing_project_tag",
      fact_project_ids: row.fact_project_ids ?? [],
    };
  });
const episodes = manifest.scenarios.filter((scenario) => scenario.expected_episodes).map((scenario) => {
  const session = scenario.sessions[0];
  const expected = scenario.expected_episodes.map((episode) => episode.category);
  if (!session.session_id || scenario.status !== "complete") return { scenario: scenario.id, state: "pending", expected };
  const durationMinutes = (Date.parse(session.ended_at) - Date.parse(session.started_at)) / 60_000;
  const duration = { observed_minutes: Number(durationMinutes.toFixed(2)), minimum_minutes: scenario.minimum_duration_minutes ?? 0,
    state: durationMinutes >= (scenario.minimum_duration_minutes ?? 0) ? "met" : "short" };
  if (!ledgerEntries) return { scenario: scenario.id, state: "ledger_unavailable", expected, duration };
  const actual = ledgerEntries.filter((entry) => entry.session_id === session.session_id && (entry.harness ?? entry.source) === session.harness)
    .sort((a, b) => Number(a.episode_seq) - Number(b.episode_seq)).map((entry) => entry.category);
  return { scenario: scenario.id, state: JSON.stringify(actual) === JSON.stringify(expected) ? "hit" : "episode_mismatch", expected, actual, duration };
});
const scenarioResults = manifest.scenarios.map((scenario) => {
  const pairs = results.filter((row) => row.scenario === scenario.id);
  const episode = episodes.find((row) => row.scenario === scenario.id);
  const states = [...pairs.map((row) => row.state), ...(episode ? [episode.state, episode.duration?.state === "short" ? "duration_short" : null].filter(Boolean) : [])];
  const outcome = states.includes("pending") ? "pending"
    : states.some((state) => ["wrong_relation", "evidence_no_finding", "episode_mismatch"].includes(state)) ? "miss"
    : states.some((state) => ["no_evidence", "missing_signal", "missing_project_tag", "wrong_project", "unverified_evidence", "unverified_no_finding", "ledger_unavailable", "duration_short"].includes(state)) ? "inconclusive"
    : "hit";
  return { scenario: scenario.id, outcome, pair_states: pairs.map((row) => row.state), ...(episode ? { episode_state: episode.state, duration_state: episode.duration?.state ?? null } : {}) };
});
console.log(JSON.stringify({ scored_at: new Date().toISOString(), window: overlaps.window, coverage: overlaps.coverage, quality_gate: evidenceDefects.length ? "evidence_defect" : "clear", evidenceDefects, summary, scenarioResults, results, projectResults, episodes }, null, 2));
