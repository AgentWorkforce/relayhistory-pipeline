/**
 * M3-5 foundation: real worker + migrated Postgres + authenticated HTTP pull/ack.
 * Synthetic directory and tool facts are inserted atomically at the facts boundary;
 * this does not claim to prove extraction, conflict classification or Relay DMs.
 */
import { createHash } from "node:crypto";
import { TENANT_A, TENANT_B } from "../selfhost/stack.ts";
import type { PipelineStack } from "./stack.ts";

const prefix = "e2e-coordination-";
const q = (s: string) => "'" + s.replaceAll("'", "''") + "'";
const check: (ok: unknown, detail: string) => asserts ok = (ok, detail) => {
  if (!ok) throw new Error(`coordination: ${detail}`);
};
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function waitFor<T>(detail: string, probe: () => Promise<T | undefined>): Promise<T> {
  const deadline = Date.now() + 90_000;
  do {
    const result = await probe();
    if (result !== undefined) return result;
    await sleep(1_000);
  } while (Date.now() < deadline);
  throw new Error(`coordination: timed out waiting for ${detail}`);
}

export async function proveCoordinationAdvisories(stack: PipelineStack): Promise<string> {
  const a = TENANT_A;
  const session = (id: string) => prefix + id;
  const where = `org_id=${q(a.org)} AND workspace_id=${q(a.workspace)} AND session_id LIKE '${prefix}%'`;
  const rows = () => stack.sql(`SELECT session_id, peer_session_id, urgency, kind, artifact, status
    FROM sessions.coordination_advisories WHERE ${where} ORDER BY session_id, peer_session_id, artifact`);
  const scanLines = () => stack.logs("coordination-advisor").split("\n").filter((l) => l.includes("coordination advisor scan:"));
  const scanAfter = async (before: number, signal?: RegExp) => waitFor("a completed advisor scan", async () => {
    const lines = scanLines().slice(before);
    check(!lines.some((l) => /failed=[1-9]/u.test(l)), "worker scan failed");
    return lines.some((l) => !signal || signal.test(l)) ? true : undefined;
  });
  async function request(path: string, token = stack.tokens.a, body?: unknown) {
    const response = await fetch(`${stack.readUrl}${path}`, {
      method: body === undefined ? "GET" : "POST",
      headers: { authorization: `Bearer ${token}`, ...(body === undefined ? {} : { "content-type": "application/json" }) },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(10_000),
    });
    const text = await response.text();
    return { status: response.status, body: text ? JSON.parse(text) : null };
  }
  const path = (id: string, workspace = a.workspace) => `/v1/coordination/advisories?workspace=${encodeURIComponent(workspace)}&source=claude&sessionId=${encodeURIComponent(session(id))}`;
  function directory(id: string, project: string, org = a.org, workspace = a.workspace) {
    return `INSERT INTO sessions.session_directory
      (org_id,workspace_id,harness,session_id,created_at,last_activity_at,project_id,git_repo,git_branch)
      VALUES (${q(org)},${q(workspace)},'claude',${q(session(id))},now()-interval '1 hour',now(),${q(session(project))},'fixture/repo',${q('branch/' + id)});`;
  }
  function fact(id: string, files: string[], suffix = "first", org = a.org, workspace = a.workspace) {
    const event = session(id + "-" + suffix);
    const hash = createHash("sha256").update(org + workspace + event).digest("hex");
    return `INSERT INTO sessions.session_event_facts
      (org_id,workspace_id,ts,session_id,source,machine_id,kind,event_id,project_id,branch,
       input_tokens,output_tokens,reasoning_tokens,cache_read_tokens,cache_create_tokens,files_touched,row_id,fingerprint,source_version)
      VALUES (${q(org)},${q(workspace)},now()-interval '1 minute',${q(session(id))},'claude','fixture','tool',${q(event)},
        'fixture',${q('branch/' + id)},0,0,0,0,0,ARRAY[${files.map(q).join(",")}],${q(hash)},${q(hash)},1);`;
  }
  // Each pair is in its own project, avoiding accidental cross-scenario LOWs.
  const firstScan = scanLines().length;
  stack.sql(`BEGIN;
    INSERT INTO sessions.session_intel_controls(scope,feature,enabled,updated_by)
      VALUES ('global','advisor',true,'fixture');
    INSERT INTO sessions.session_intel_controls(scope,org_id,workspace_id,feature,enabled,updated_by)
      VALUES ('workspace',${q(a.org)},${q(a.workspace)},'advisor',true,'fixture');
    ${directory("high-a", "high")}${directory("high-b", "high")}
    ${fact("high-a", ["src/collision/shared.ts"])}${fact("high-b", ["src/collision/shared.ts"])}
    ${directory("low-a", "low")}${directory("low-b", "low")}
    ${fact("low-a", ["src/nearby/a.ts"])}${fact("low-b", ["src/nearby/b.ts"])}
    ${directory("fleet-parent", "parent")}${directory("fleet-a", "fleet")}${directory("fleet-b", "fleet")}
    ${fact("fleet-a", ["src/fleet/shared.ts"])}${fact("fleet-b", ["src/fleet/shared.ts"])}
    INSERT INTO sessions.session_directory_relationships
      (org_id,workspace_id,harness,parent_session_id,relationship_uid,child_session_id)
      VALUES (${q(a.org)},${q(a.workspace)},'claude',${q(session("fleet-parent"))},'fixture-a',${q(session("fleet-a"))}),
             (${q(a.org)},${q(a.workspace)},'claude',${q(session("fleet-parent"))},'fixture-b',${q(session("fleet-b"))});
    ${directory("foreign-a", "foreign", TENANT_B.org, TENANT_B.workspace)}
    ${directory("foreign-b", "foreign", TENANT_B.org, TENANT_B.workspace)}
    ${fact("foreign-a", ["src/foreign/shared.ts"], "first", TENANT_B.org, TENANT_B.workspace)}
    ${fact("foreign-b", ["src/foreign/shared.ts"], "first", TENANT_B.org, TENANT_B.workspace)}
    COMMIT;`);
  await waitFor("two HIGH and two LOW recipient rows", async () => {
    const got = rows();
    return got.length >= 4 ? got : undefined;
  });
  await scanAfter(firstScan, /findings=[1-9]/u);
  const initial = rows();
  check(initial.length === 4, `unexpected initial rows (${initial.length})`);
  check(initial.filter((r) => r[2] === "high" && r[3] === "concurrent_edit").length === 2, "collision was not HIGH for both recipients");
  check(initial.filter((r) => r[2] === "low" && r[3] === "nearby_work").length === 2, "nearby work was not LOW for both recipients");
  check(initial.every((r) => r[5] === "pending"), "DM-disabled row was marked delivered");
  check(stack.sql(`SELECT count(*) FROM sessions.coordination_advisories WHERE workspace_id=${q(TENANT_B.workspace)}`)[0]?.[0] === "0", "worker scanned a workspace outside its allowlist");

  const pull = await request(path("high-a"));
  check(pull.status === 200 && pull.body?.contract === "coordination-advisories/1", `pull status=${pull.status}`);
  const items = pull.body.advisories;
  check(items?.length === 1 && items[0].urgency === "high" && items[0].peer.sessionId === session("high-b"), "pull returned the wrong recipient or peer");
  check(items[0].evidence.length === 2, "pull lost the two write citations");
  const low = await request(path("low-a"));
  check(low.status === 200 && low.body.advisories.length === 1 && low.body.advisories[0].urgency === "low", "LOW was not available on pull");
  const fleet = await request(path("fleet-a"));
  check(fleet.status === 200 && fleet.body.advisories.length === 0, "planned fleet was interrupted");
  const foreign = await request(path("high-a", TENANT_B.workspace), stack.tokens.b);
  check(foreign.status === 200 && foreign.body.advisories.length === 0, "another tenant read the advisory");
  const mismatch = await request(path("high-a", TENANT_B.workspace));
  // Real Control introspection rejects the mismatched workspace before the
  // route's own workspace attestation (which separately returns 403).
  check(mismatch.status === 401, `workspace mismatch status=${mismatch.status}`);
  const ackPath = `/v1/coordination/advisories/${items[0].advisoryId}/ack?workspace=${encodeURIComponent(a.workspace)}`;
  const otherAck = await request(ackPath.replace(`workspace=${a.workspace}`, `workspace=${TENANT_B.workspace}`), stack.tokens.b, { source: "claude", sessionId: session("high-a") });
  check(otherAck.status === 404, `foreign ack status=${otherAck.status}`);
  for (let i = 0; i < 2; i++) {
    const ack = await request(ackPath, stack.tokens.a, { source: "claude", sessionId: session("high-a") });
    check(ack.status === 204, `ack ${i + 1} status=${ack.status}`);
  }
  check((await request(path("high-a"))).body.advisories.length === 0, "acked row stayed on pull");
  check((await request(path("high-b"))).body.advisories.length === 1, "acking one recipient hid the peer's row");

  // A distinct artifact would make a new finding. Cooldown must suppress it,
  // including after the real worker process loses all of its in-memory state.
  stack.compose(["stop", "coordination-advisor"], { quiet: true });
  stack.sql(`BEGIN;
    DELETE FROM sessions.session_event_facts WHERE org_id=${q(a.org)} AND workspace_id=${q(a.workspace)}
      AND event_id IN (${q(session("high-a-first"))},${q(session("high-b-first"))});
    ${fact("high-a", ["src/collision/second.ts"], "second")}${fact("high-b", ["src/collision/second.ts"], "second")} COMMIT;`);
  let before = scanLines().length;
  stack.compose(["start", "coordination-advisor"], { quiet: true });
  await scanAfter(before, /cooldown=[1-9]/u);
  check(rows().length === 4, "restart lost the pair cooldown");
  check((await request(path("high-a"))).body.advisories.length === 0, "restart resurrected an acked row");

  // Four peers generate more than the configured three advisories for the hub.
  // Different top-level directories avoid spurious LOW pairs between the peers.
  before = scanLines().length;
  stack.sql(`BEGIN; ${directory("cap-hub", "cap")}
    ${[1, 2, 3, 4].map((i) => directory(`cap-${i}`, "cap") + fact(`cap-${i}`, [`cap${i}/shared.ts`])).join("\n")}
    ${fact("cap-hub", [1, 2, 3, 4].map((i) => `cap${i}/shared.ts`))} COMMIT;`);
  await scanAfter(before, /capped=[1-9]/u);
  const hubCount = () => rows().filter((r) => r[0] === session("cap-hub")).length;
  check(hubCount() === 3, `hourly cap: hub has ${hubCount()} rows`);
  stack.compose(["stop", "coordination-advisor"], { quiet: true });
  // A fresh peer has no pair cooldown. Only the durable recipient cap can
  // prevent a fourth hub row after this restart.
  stack.sql(`BEGIN; ${directory("cap-5", "cap")}${fact("cap-5", ["cap5/shared.ts"])}
    ${fact("cap-hub", ["cap5/shared.ts"], "fifth")} COMMIT;`);
  before = scanLines().length;
  stack.compose(["start", "coordination-advisor"], { quiet: true });
  await scanAfter(before, /capped=[1-9]/u);
  check(hubCount() === 3, "restart reset the hourly cap");
  check(rows().every((r) => r[5] !== "delivered"), "DM-disabled proof delivered a message");
  stack.compose(["stop", "coordination-advisor"], { quiet: true });
  stack.sql(`BEGIN;
    UPDATE sessions.session_intel_controls SET enabled=false,updated_at=now(),updated_by='fixture'
      WHERE scope='workspace' AND org_id=${q(a.org)} AND workspace_id=${q(a.workspace)} AND feature='advisor';
    ${directory("switch-a", "switch")}${directory("switch-b", "switch")}
    ${fact("switch-a", ["src/switch/shared.ts"])}${fact("switch-b", ["src/switch/shared.ts"])} COMMIT;`);
  before = scanLines().length;
  stack.compose(["start", "coordination-advisor"], { quiet: true });
  await scanAfter(before, /workspaces=0/u);
  const switchRows = () => rows().filter((r) => r[0] === session("switch-a") || r[0] === session("switch-b"));
  check(switchRows().length === 0, "disabled workspace still wrote advisories");
  // No restart: the next completed tick must observe the operator's flip.
  before = scanLines().length;
  stack.sql(`UPDATE sessions.session_intel_controls SET enabled=true,updated_at=now(),updated_by='fixture'
    WHERE scope='workspace' AND org_id=${q(a.org)} AND workspace_id=${q(a.workspace)} AND feature='advisor'`);
  await scanAfter(before, /written=2\s/u);
  check(switchRows().length === 2, "control flip was not applied by the next scan");
  return "HIGH collision + LOW overlap; fleet silent; allowlist and tenant isolation; pull citations; idempotent recipient ack; cooldown and cap survive restarts; control flip on the next tick; no DMs";
}
