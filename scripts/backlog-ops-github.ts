/**
 * Read-only GitHub gate for .github/workflows/backlog-ops.yml (the 0005 backlog tools against production; contract in
 * src/session-ingest/backlog-ops.ts). The same gates as the guarded schema release and its operators
 * (scripts/schema-release-github.ts): repository id, main ref, the exact live main head, first attempt, the pinned
 * human dispatcher on both actor and triggering_actor, the mode's protected environment (session-schema-production for
 * a dry run, session-backlog-execute for an execute; main only), green push CI and pg-proof at the SHA, and, inside
 * the protected job, the recorded approval for that environment.
 * An execute also binds earlier runs of this workflow at the same SHA: each completed/success, first attempt, by the
 * pinned dispatcher, of the expected mode (read back from its run-name), under 24 h old, holding exactly one
 * unexpired receipt artifact; the drain's backfill under BACKLOG_OPS_FRESH_BACKFILL_MS old and its export created
 * after that backfill. The dormant switch and the inputs are checked first, before any GitHub read. Errors carry
 * fixed codes only.
 *
 * Refresh (drain-execute with FULL): the drain's BACKFILL is then a backfill-refresh-execute (fences and ownership
 * sections only) under BACKLOG_OPS_FRESH_BACKFILL_MS old, and FULL a backfill-execute at the same sha under
 * BACKLOG_OPS_BINDING_MAX_AGE_MS old, created before the refresh; the EXPORT must be created after the refresh (it
 * records the fence views it read), which is stricter than after FULL. The refresh's mode is in its run-name, so its
 * display title never equals a full run's: a refresh can never satisfy a full-run binding, nor a full run a refresh
 * binding. Why that is safe: after the scope's Worker HTTP admission stop no new upload reaches SQL for those orgs,
 * so SQL's revisions, ownership rows and owned lazy slots should not advance, and the full run's revision shards and
 * owner stubs stay valid for its 24 h window; only the fences (deletions and resets) and ownership can still move,
 * and the refresh re-runs exactly those. A control also deletes revision/ownership rows, so one landing between FULL
 * and the refresh makes the fingerprints differ and the drain refuses stale (a new full run is then needed). The credential step proves the rest unchanged before any credential is read
 * (a writer that still moved one of those rows changes it): the two receipts' input fingerprints must be equal
 * (checkRefreshFingerprint, backlog_ops_backfill_stale).
 *
 * Catch-up (backfill-catchup-dry/-execute, and a drain whose BACKFILL with FULL is a backfill-catchup-execute): FULL is
 * a backfill-execute at the same sha under BACKLOG_OPS_BINDING_MAX_AGE_MS old (created before the catch-up execute's
 * DRY too), and the gate computes t = FULL's created_at - BACKLOG_OPS_CATCHUP_MARGIN_MS from this API, never from an
 * input, and writes it as the step output `revisions_since_ms` (the credential step passes it as
 * --revisions-since-ms and checks the bound catch-up receipts recorded the same t). A drain's BACKFILL with FULL may be
 * a refresh or a catch-up (its run-name says which); both obey the refresh's age and order rules.
 */
import { appendFileSync } from "node:fs";
import { PRIVATE_INGEST_RELEASE } from "../src/session-ingest/private-ingest-release.ts";
import { SCHEMA_RELEASE as S } from "../src/hoopdb/schema-release-contract.ts";
import {
  BACKLOG_OPS_BINDING_MAX_AGE_MS,
  BACKLOG_OPS_CATCHUP_MARGIN_MS,
  BACKLOG_OPS_AUTHORITY_EXECUTE_ENABLED,
  BACKLOG_OPS_BRIDGE_EXECUTE_ENABLED,
  BACKLOG_OPS_EXECUTE_ENABLED,
  BACKLOG_OPS_MARKER_EXECUTE_ENABLED,
  BACKLOG_OPS_HOLD_EXECUTE_ENABLED,
  BACKLOG_OPS_FRESH_BACKFILL_MS,
  BACKLOG_OPS_RECEIPT_ARTIFACT,
  BACKLOG_OPS_WORKFLOW,
  BACKLOG_OPS_PREVIOUS_MAX_AGE_MS,
  backlogOpsBindingMaxAgeMs,
  backlogOpsBindings,
  backlogOpsDispatch,
  backlogOpsEnvironment,
  backlogOpsRunName,
  isCatchup,
  isMarker,
  refuseBacklogOps as refuse,
} from "../src/session-ingest/backlog-ops.ts";
import {
  githubGet,
  isPinnedDispatcher,
  requireGreenPushRuns,
  requireHumanApproval,
  requireProtectedEnvironment,
  type Get,
} from "./schema-release-github.ts";

type Obj = Record<string, any>;

export async function backlogOpsGithubGate(
  env: Record<string, string | undefined>,
  get: Get,
  approved = false,
  options: { enabled?: boolean; authorityEnabled?: boolean; bridgeEnabled?: boolean; markerEnabled?: boolean; holdEnabled?: boolean; now?: number } = {},
) {
  const input = backlogOpsDispatch(env, options.enabled ?? BACKLOG_OPS_EXECUTE_ENABLED, options.authorityEnabled ?? BACKLOG_OPS_AUTHORITY_EXECUTE_ENABLED,
    options.bridgeEnabled ?? BACKLOG_OPS_BRIDGE_EXECUTE_ENABLED, options.markerEnabled ?? BACKLOG_OPS_MARKER_EXECUTE_ENABLED,
    options.holdEnabled ?? BACKLOG_OPS_HOLD_EXECUTE_ENABLED);
  const now = options.now ?? Date.now();
  if (env.GITHUB_REPOSITORY !== S.repository || env.GITHUB_REPOSITORY_ID !== PRIVATE_INGEST_RELEASE.repositoryId ||
      env.GITHUB_EVENT_NAME !== "workflow_dispatch" || env.GITHUB_REF !== "refs/heads/main" ||
      env.GITHUB_SHA !== input.sha || env.GITHUB_RUN_ATTEMPT !== "1" ||
      !/^[1-9][0-9]*$/u.test(env.GITHUB_RUN_ID ?? ""))
    refuse("backlog_ops_gate_context");
  const base = `/repos/${S.repository}`;
  if ((await get(base + "/git/ref/heads/main")).object?.sha !== input.sha) refuse("backlog_ops_gate_main_moved");
  const run = await get(base + "/actions/runs/" + env.GITHUB_RUN_ID);
  if (run.head_sha !== input.sha || run.event !== "workflow_dispatch" || run.head_branch !== "main" ||
      run.path !== BACKLOG_OPS_WORKFLOW || run.run_attempt !== 1 || !isPinnedDispatcher(run) ||
      run.display_title !== backlogOpsRunName(input.mode, input.sha))
    refuse("backlog_ops_gate_run");
  // Dry modes: the schema release environment. Executes: their own environment, which alone the write role trusts.
  const environment = backlogOpsEnvironment(input.mode);
  const gate = await requireProtectedEnvironment(get, base, environment, true, {
    environment: "backlog_ops_gate_environment",
    branch: "backlog_ops_gate_environment_branch",
  });
  await requireGreenPushRuns(get, base, input.sha, { runs: "backlog_ops_gate_ci", jobs: "backlog_ops_gate_ci_jobs" });
  const created: Record<string, number> = {};
  for (const b of backlogOpsBindings(input)) {
    const bound = await get(base + "/actions/runs/" + b.run);
    const at = Date.parse(bound.created_at);
    // The drain's BACKFILL and authority-execute's catch-up must be fresh (3 h); everything else under 24 h.
    const maxAge = backlogOpsBindingMaxAgeMs(input.mode, b.kind);
    if (String(bound.id) !== b.run || String(bound.id) === env.GITHUB_RUN_ID || bound.path !== BACKLOG_OPS_WORKFLOW ||
        bound.head_sha !== input.sha || bound.head_branch !== "main" || bound.event !== "workflow_dispatch" ||
        bound.run_attempt !== 1 || bound.status !== "completed" || bound.conclusion !== "success" ||
        !isPinnedDispatcher(bound) ||
        (bound.display_title !== backlogOpsRunName(b.mode, input.sha) && (b.altMode === undefined || bound.display_title !== backlogOpsRunName(b.altMode, input.sha))) ||
        !Number.isFinite(at) || at > now || now - at >= maxAge)
      refuse(`backlog_ops_gate_${b.kind}_run`);
    const artifacts = await get(base + "/actions/runs/" + b.run + "/artifacts?per_page=100");
    const named = (artifacts.artifacts ?? []).filter((a: Obj) => a.name === BACKLOG_OPS_RECEIPT_ARTIFACT);
    if (artifacts.total_count !== artifacts.artifacts?.length || named.length !== 1 || named[0].expired !== false)
      refuse(`backlog_ops_gate_${b.kind}_artifact`);
    created[b.kind] = at;
  }
  // The drain's export ran after its backfill (the export reads the fences the backfill wrote).
  if (input.mode === "drain-execute" && !(created.export! > created.backfill!)) refuse("backlog_ops_gate_order");
  // With FULL, the refresh refreshes it: FULL was created first.
  if (input.mode === "drain-execute" && input.fullBackfillRun !== null && !(created.full! < created.backfill!))
    refuse("backlog_ops_gate_full_order");
  // A catch-up execute's FULL came before its DRY (which bound the same FULL: the credential step checks its t).
  if (input.mode === "backfill-catchup-execute" && !(created.full! < created.dry!)) refuse("backlog_ops_gate_full_order");
  // authority-execute: the evidence module's order. Its DRY (the unbound evidence the baseline took its stops from) and
  // the catch-up's FULL were both created before the catch-up that ran the baseline.
  if (input.mode === "authority-execute" && !(created.full! < created.backfill!)) refuse("backlog_ops_gate_full_order");
  if (input.mode === "authority-execute" && !(created.dry! < created.backfill!)) refuse("backlog_ops_gate_order");
  // The reshard verify ran after the catch-up (it verifies the state the marker would cover).
  if (input.mode === "authority-execute" && !(created.reshard! > created.backfill!)) refuse("backlog_ops_gate_reshard_order");
  // A marker mode: the evidence module's order (authority-dry and FULL before the catch-up that ran the baseline), the
  // reshard verify after that catch-up, and marker-execute's marker-dry after the reshard verify (it read the state the
  // markers cover, last).
  if (isMarker(input.mode) && !(created.full! < created.backfill!)) refuse("backlog_ops_gate_full_order");
  if (isMarker(input.mode) && !(created.authority! < created.backfill!)) refuse("backlog_ops_gate_order");
  if (isMarker(input.mode) && !(created.reshard! > created.backfill!)) refuse("backlog_ops_gate_reshard_order");
  if (input.mode === "marker-execute" && !(created.dry! > created.reshard!)) refuse("backlog_ops_gate_order");
  // A catch-up's AUTHORITY: created before the catch-up's own DRY when it is an execute (the evidence it hands the baseline
  // existed before either catch-up run started).
  if (input.mode === "backfill-catchup-execute" && created.authority !== undefined && !(created.authority < created.dry!)) refuse("backlog_ops_gate_order");
  // t: FULL's created_at minus the margin, from this API only. Every run binding FULL gets it (a drain's BACKFILL may be
  // a catch-up; the credential step uses it only then).
  let revisionsSinceMs: number | null = null;
  if (input.fullBackfillRun !== null) {
    revisionsSinceMs = created.full! - BACKLOG_OPS_CATCHUP_MARGIN_MS;
    if (!Number.isSafeInteger(revisionsSinceMs) || revisionsSinceMs < 1) refuse("backlog_ops_gate_full_run");
  } else if (isCatchup(input.mode)) refuse("backlog_ops_full_backfill_run");
  // hold-seal-execute: its DRY (which recorded the epochs it seals) ran after the FULL it seals and binds, so the hold
  // was already installed when that FULL was dispatched only if the operator followed the order; 062340 checks it.
  if (input.mode === "hold-seal-execute" && !(created.full! < created.dry!)) refuse("backlog_ops_gate_full_order");
  // A per-org retry's PREVIOUS: the same execute at this sha by the same dispatcher, first attempt, completed and
  // FAILED (an incomplete per-org execute exits nonzero), with one unexpired receipt, created after every run this
  // one binds (it bound the same ones; the credential step checks that) and under BACKLOG_OPS_PREVIOUS_MAX_AGE_MS old.
  let previousCreatedMs: number | null = null;
  if (input.previousRun) {
    const prev = await get(base + "/actions/runs/" + input.previousRun);
    const at = Date.parse(prev.created_at);
    if (String(prev.id) !== input.previousRun || String(prev.id) === env.GITHUB_RUN_ID || prev.path !== BACKLOG_OPS_WORKFLOW || prev.head_sha !== input.sha ||
        prev.head_branch !== "main" || prev.event !== "workflow_dispatch" || prev.run_attempt !== 1 || prev.status !== "completed" || prev.conclusion !== "failure" ||
        !isPinnedDispatcher(prev) || prev.display_title !== backlogOpsRunName(input.mode, input.sha) || !Number.isFinite(at) || at > now ||
        now - at >= BACKLOG_OPS_PREVIOUS_MAX_AGE_MS || Object.values(created).some((c) => !(c < at)))
      refuse("backlog_ops_gate_previous_run");
    const artifacts = await get(base + "/actions/runs/" + input.previousRun + "/artifacts?per_page=100");
    const named = (artifacts.artifacts ?? []).filter((a: Obj) => a.name === BACKLOG_OPS_RECEIPT_ARTIFACT);
    if (artifacts.total_count !== artifacts.artifacts?.length || named.length !== 1 || named[0].expired !== false) refuse("backlog_ops_gate_previous_artifact");
    previousCreatedMs = at;
  }
  if (approved)
    await requireHumanApproval(get, base, env.GITHUB_RUN_ID!, environment, gate, true, {
      approval: "backlog_ops_gate_human_approval",
      rejected: "backlog_ops_gate_human_rejected",
      dispatch: "backlog_ops_gate_human_dispatch",
    });
  return { ...input, environment, revisionsSinceMs, previousCreatedMs };
}

if (import.meta.main)
  backlogOpsGithubGate(process.env, githubGet(), process.argv.includes("--approved"))
    .then((r) => {
      console.log("backlog_ops_github_gate_ok");
      // The protected environment the operate job must run in, derived from the mode (never an input).
      if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `environment=${r.environment}\n` +
        // The catch-up's t (an epoch ms computed from FULL's created_at), for the credential step; never an input.
        (r.revisionsSinceMs !== null ? `revisions_since_ms=${r.revisionsSinceMs}\n` : "") +
        // A per-org retry's previous run's created_at (epoch ms), from this API only; never an input.
        (r.previousCreatedMs !== null ? `previous_created_ms=${r.previousCreatedMs}\n` : ""));
    })
    .catch((error) => {
      const code = (error as { code?: unknown })?.code;
      console.error("backlog_ops_github_gate_refused " +
        (typeof code === "string" && /^[a-z0-9_]{1,64}$/u.test(code) ? code : "failed"));
      process.exitCode = 1;
    });
