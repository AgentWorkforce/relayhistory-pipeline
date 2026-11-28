/**
 * The synthetic per-org cutover qualification (e2e/cutover) on the CI fixture (PGlite + fs store): the harness
 * itself, and that it FAILS CLOSED while the release-path heads are not on main. Pins what main does today, step by
 * step, so a head landing (or a regression) changes this file on purpose:
 *   - S3 admission is open from the marker until (and while) bridge_complete commits: every S3 probe after the marker
 *     (post-marker verify, drain, before the replays, and an upload already in flight at the release commit) is
 *     accepted. With a named S3 hold it must be refused with that code and leave no durable write.
 *   - the drain leaves the backlog unmaterialized (no canonical manifest for any old session);
 *   - injected erasures: release BLOCKED on the generation proof, no release commit, no resumption;
 *   - empty window: release commits through the stand-in bridge; publication is not exercised; old sessions are not
 *     readable exactly;
 *   - an erasure the replay never saw refuses the release (erasure_unaccounted), whatever its kind;
 *   - direct org (already accepting direct uploads, layered 252344394069577728): the existing marker conflicts (kept);
 *     a writer authorized before the install completes during the hold; the mirrored and S3-only resets leave no
 *     062340 row, so the empty-window release would commit: refused (direct_erasure_unrecorded).
 * Never QUALIFIED here: the qualification backend is PG17 + MinIO (synthetic-cutover.postgres.test.ts).
 */
import { describe, expect, setDefaultTimeout, test } from "bun:test";
import { SIZE_CI } from "../../../e2e/cutover/fixture.ts";
import { QUALIFICATION_BACKEND, reportMarkdown, runCutoverQualification, type CutoverReport } from "../../../e2e/cutover/harness.ts";
import { allLanded, defaultHeads } from "../../../e2e/cutover/heads.ts";

setDefaultTimeout(240_000);
const statuses = (r: CutoverReport) => Object.fromEntries(r.steps.map((s) => [s.id, s.code ? `${s.status} ${s.code}` : s.status]));
const S3_OPEN = "FAIL s3_admits_before_bridge_complete";
const THROUGH_MARKER = { seed: "PASS", "rearm.list": "PASS", "hold.install": "PASS", full: "PASS", "seal.bind": "PASS", catchup: "PASS",
  "reshard.verify": "PASS", marker: "PASS", "marker.verify": S3_OPEN, drain: "FAIL drain_not_materialized+s3_admits_before_bridge_complete" };

describe("synthetic per-org cutover qualification (PGlite)", () => {
  test("injected erasures: S3 open from the marker, backlog unmaterialized, release blocked on the generation proof", async () => {
    const r = await runCutoverQualification({ size: SIZE_CI });
    expect(statuses(r)).toEqual({ ...THROUGH_MARKER, release: "BLOCKED erasure_generation_unproven", admission: "SKIP", publication: "SKIP", reads: "SKIP" });
    expect(r.verdict).toBe("NOT_QUALIFIED");
    expect(r.first_failure).toMatchObject({ step: "marker.verify", code: "s3_admits_before_bridge_complete" });
    expect(r.uncovered.some((u) => u.includes("HTTP serving"))).toBe(true);
    expect(r.reasons).toContain(`backend: pglite+fs is not the qualification backend (${QUALIFICATION_BACKEND})`);
    // A blocked release has no release commit and no resumption; the S3 admission bounds the observed downtime.
    expect(r.timeline.release_committed_ms).toBeNull();
    expect(r.timeline.first_admit_after_release_started_ms).toBeNull();
    expect(r.downtime.through_release_ms).toBeNull();
    expect(r.uncovered.some((u) => u.includes("post-authority/pre-PUT race"))).toBe(true);
    expect(r.downtime.observed_upper_bound_ms).toBeGreaterThan(0);
    expect(r.timeline.first_s3_admit_started_ms!).toBeGreaterThan(r.timeline.install_committed_ms!);
    // Every SQL upload during the hold refused RH042; every S3 probe after the marker accepted (today).
    expect(r.uploads.filter((u) => u.path === "sql" && u.step !== "seed").every((u) => u.outcome === "RH042")).toBe(true);
    expect(r.uploads.filter((u) => u.path === "s3").every((u) => u.outcome === "accepted")).toBe(true);
    expect(r.uploads.some((u) => u.step === "release.pre")).toBe(true);
    expect(Object.values(r.erasures.before_hold).reduce((a, b) => a + b, 0)).toBe(3);
    expect(Object.values(r.erasures.during_hold).reduce((a, b) => a + b, 0)).toBe(3);
    expect(Object.keys(r.erasures.history_since_install ?? {}).sort()).toEqual(["operator_user_erasures", "session_erasure_fences", "upload_deletions"]);
    expect(r.size.batches).toBe(SIZE_CI.users * SIZE_CI.sessionsPerUser * SIZE_CI.batchesPerSession);
    expect(reportMarkdown(r)).toContain("**Verdict: NOT_QUALIFIED**");
  });

  test("empty window: release commits through the stand-in bridge with an upload in flight still admitted", async () => {
    const r = await runCutoverQualification({ size: SIZE_CI, erasures: "empty" });
    expect(statuses(r)).toEqual({ ...THROUGH_MARKER, release: S3_OPEN, admission: "PASS", publication: "FAIL publication_not_exercised", reads: "FAIL old_events_not_canonical" });
    expect(r.erasures.history_since_install).toEqual({});
    // Deterministic ordering: the in-flight writer was parked inside admission before the release committed.
    expect(r.inflight_barrier!.reached_ms).not.toBeNull();
    expect(r.inflight_barrier!.reached_ms!).toBeLessThanOrEqual(r.inflight_barrier!.release_committed_ms!);
    expect(r.uploads.find((u) => u.step === "release.inflight")!.outcome).toBe("accepted");
    expect(r.inflight_barrier!.store_ops_after_barrier_open).toBeGreaterThan(0);
    // The post-authority/pre-PUT writer: it read the org's fence-authority marker, parked at its first durable write,
    // the release committed, then it wrote and was accepted (no S3 hold on main).
    expect(r.inflight_barrier!.post_authority).toMatchObject({ authority_read_before_park: true, outcome: "accepted" });
    expect(r.inflight_barrier!.post_authority!.parked_ms).not.toBeNull();
    expect(r.timeline.first_s3_admit_accepted_ms!).toBeLessThan(r.timeline.release_committed_ms!);
    expect(r.downtime.observed_upper_bound_ms!).toBeLessThan(r.downtime.through_release_ms!);
    expect(r.verdict).toBe("NOT_QUALIFIED");
  });

  test("simulated pending heads: timings only, still NOT_QUALIFIED", async () => {
    const r = await runCutoverQualification({ size: SIZE_CI, simulatePending: true });
    expect(statuses(r)).toMatchObject({ release: S3_OPEN, admission: "PASS", publication: "FAIL publication_not_exercised", reads: "FAIL old_events_not_canonical" });
    expect(r.simulated).toBe(true);
    expect(r.verdict).toBe("NOT_QUALIFIED");
    expect(r.uploads.filter((u) => u.step === "admission.retry").every((u) => u.outcome === "accepted")).toBe(true);
  });

  test("an erasure the replay never saw refuses the release, though its kind is replayed (erasure_unaccounted)", async () => {
    const r = await runCutoverQualification({ size: SIZE_CI, simulatePending: true, unreplayedErasure: true });
    expect(statuses(r)).toMatchObject({ release: "FAIL erasure_unaccounted", admission: "SKIP" });
    expect(r.timeline.release_committed_ms).toBeNull();
    expect(r.inflight_barrier).toBeNull(); // the coverage check refuses before any in-flight probe
  });

  test("direct org: pre-install direct upload, writer parked before install completes in the hold, unrecorded direct controls", async () => {
    const r = await runCutoverQualification({ size: SIZE_CI, erasures: "direct" });
    expect(statuses(r)).toEqual({ seed: "PASS", "direct.enroll": "PASS", "rearm.list": "PASS", "hold.install": S3_OPEN, full: "PASS", "seal.bind": "PASS",
      catchup: "PASS", "reshard.verify": "PASS", marker: "FAIL marker_conflict", "marker.verify": S3_OPEN,
      drain: "FAIL drain_reset_margin_pending+drain_not_materialized+s3_admits_before_bridge_complete",
      release: "FAIL direct_erasure_unrecorded", admission: "SKIP", publication: "SKIP", reads: "SKIP" });
    expect(r.direct).toMatchObject({
      preinstall_upload: "accepted",
      // Parked after its authority read before the install; let go after the cutover: it wrote while the org was held.
      parked_writer: { parked_before_install: true, authority_read_before_park: true, outcome: "accepted", readable: "manifest_present" },
      // The real mirrored control applies the S3 reset and its SQL half is refused: no 062340 row. Same for S3-only.
      mirrored_reset: { s3_applied: true, history_rows: 0 }, s3_only_reset: { s3_applied: true, history_rows: 0 },
      // So the empty-window release would commit (proved inside a rolled-back transaction); the harness refuses.
      empty_window_release_would_commit: true,
    });
    expect(r.direct!.parked_writer.effects).toEqual(["batch_record", "owner_stub", "canonical"]);
    expect(r.timeline.release_committed_ms).toBeNull();
    expect(r.verdict).toBe("NOT_QUALIFIED");
  });

  test("QUALIFIED needs every adapter landed: the defaults are stand-ins and pending heads", () => {
    const h = defaultHeads();
    expect(allLanded(h)).toBe(false);
    for (const k of ["userEraseReplay", "sessionEraseReplay", "s3HoldRefusal", "publication"] as const) expect(h[k].integration).toBe("pending");
    expect(defaultHeads({ simulatePending: true }).userEraseReplay.integration).toBe("simulated");
    expect(defaultHeads({ simulatePending: true }).s3HoldRefusal.integration).toBe("pending");
  });
});
