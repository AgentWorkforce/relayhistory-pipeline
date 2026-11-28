/**
 * The synthetic per-org cutover qualification (e2e/cutover) on real PostgreSQL and MinIO: a scratch database migrated
 * by the real migrator as the owner role, and a versioned scratch bucket. Run under the hold-harness runner, which
 * starts disposable containers and fails on a skip:
 *
 *   bun src/session-ingest/__tests__/hold-harness/run.ts --pg 17 -- src/session-ingest/__tests__/synthetic-cutover.postgres.test.ts
 *   CUTOVER_SIZE=representative CUTOVER_REPORT_DIR=$HOME/.agent-tmp/cutover-pg bun ...   # the reported run, with reports
 *
 * Both erasure scenarios. Pins what main does today, as the PGlite suite does: NOT_QUALIFIED; S3 admission reopens at
 * the marker; the injected scenario stops at release on the generation proof; the empty scenario releases through the
 * stand-in bridge and then the backlog is not in the canonical layout. Wave-1 acceptance (layered-sessions-pipeline)
 * needs this file green with the real adapters, S3 refused through bridge_complete, and old sessions canonical.
 */
import { afterAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fixturePgUrl } from "./hold-harness/pg.ts";
import { fixtureS3 } from "./hold-harness/s3.ts";
import { postgresBackend } from "../../../e2e/cutover/backend.ts";
import { SIZE_CI, SIZE_REPRESENTATIVE } from "../../../e2e/cutover/fixture.ts";
import { reportMarkdown, runCutoverQualification, type CutoverReport } from "../../../e2e/cutover/harness.ts";

setDefaultTimeout(3_600_000);
const pgUrl = fixturePgUrl(), s3 = fixtureS3();
const major = process.env.SESSION_INGEST_TEST_PG_MAJOR ? Number(process.env.SESSION_INGEST_TEST_PG_MAJOR) : undefined;
const size = process.env.CUTOVER_SIZE === "representative" ? SIZE_REPRESENTATIVE : SIZE_CI;
const statuses = (r: CutoverReport) => Object.fromEntries(r.steps.map((s) => [s.id, s.code ? `${s.status} ${s.code}` : s.status]));
const reports: CutoverReport[] = [];
afterAll(() => {
  const dir = process.env.CUTOVER_REPORT_DIR;
  if (!dir || !reports.length) return;
  mkdirSync(dir, { recursive: true });
  for (const r of reports) {
    const name = `${r.backend}-${r.scenario}${r.simulated ? "-simulated" : ""}`;
    writeFileSync(join(dir, `${name}.json`), JSON.stringify(r, null, 2) + "\n");
    writeFileSync(join(dir, `${name}.md`), reportMarkdown(r));
  }
});
const run = async (o: { erasures: "injected" | "empty" | "direct"; simulatePending?: boolean }) => {
  const r = await runCutoverQualification({ size, ...o, backend: () => postgresBackend({ pgUrl: pgUrl!, s3: s3!, ...(major ? { expectMajor: major } : {}) }) });
  reports.push(r);
  return r;
};

const S3_OPEN = "FAIL s3_admits_before_bridge_complete";
const THROUGH_MARKER = { seed: "PASS", "rearm.list": "PASS", "hold.install": "PASS", full: "PASS", "seal.bind": "PASS", catchup: "PASS",
  "reshard.verify": "PASS", marker: "PASS", "marker.verify": S3_OPEN, drain: "FAIL drain_not_materialized+s3_admits_before_bridge_complete" };

describe.skipIf(!pgUrl || !s3)("synthetic per-org cutover on PostgreSQL + MinIO", () => {
  test("injected erasures: S3 open from the marker, backlog unmaterialized, release blocked on the generation proof", async () => {
    const r = await run({ erasures: "injected" });
    expect(r.backend).toMatch(/^pg1[5-9]\+minio$/);
    expect(statuses(r)).toEqual({ ...THROUGH_MARKER, release: "BLOCKED erasure_generation_unproven", admission: "SKIP", publication: "SKIP", reads: "SKIP" });
    expect(r.verdict).toBe("NOT_QUALIFIED");
    expect(r.first_failure).toMatchObject({ step: "marker.verify", code: "s3_admits_before_bridge_complete" });
    expect(r.uncovered.some((u) => u.includes("HTTP serving"))).toBe(true);
    expect(r.timeline.release_committed_ms).toBeNull();
    expect(r.timeline.first_admit_after_release_started_ms).toBeNull();
    expect(r.downtime.observed_upper_bound_ms).toBeGreaterThan(0);
  });

  test("empty window: release commits through the stand-in bridge with an upload in flight still admitted", async () => {
    const r = await run({ erasures: "empty" });
    expect(statuses(r)).toEqual({ ...THROUGH_MARKER, release: S3_OPEN, admission: "PASS", publication: "FAIL publication_not_exercised", reads: "FAIL old_events_not_canonical" });
    expect(r.erasures.history_since_install).toEqual({});
    // Deterministic ordering: the in-flight writer was parked inside admission before the release committed.
    expect(r.inflight_barrier!.reached_ms).not.toBeNull();
    expect(r.inflight_barrier!.reached_ms!).toBeLessThanOrEqual(r.inflight_barrier!.release_committed_ms!);
    expect(r.uploads.find((u) => u.step === "release.inflight")!.outcome).toBe("accepted");
    expect(r.downtime.observed_upper_bound_ms!).toBeLessThan(r.downtime.through_release_ms!);
    expect(r.verdict).toBe("NOT_QUALIFIED");
  });

  test("direct org: pre-install upload, parked writer, unrecorded direct controls refuse the release", async () => {
    const r = await run({ erasures: "direct" });
    expect(statuses(r)).toMatchObject({ "direct.enroll": "PASS", "hold.install": S3_OPEN, marker: "FAIL marker_conflict", release: "FAIL direct_erasure_unrecorded" });
    expect(r.direct).toMatchObject({ preinstall_upload: "accepted", parked_writer: { authority_read_before_park: true, outcome: "accepted" },
      mirrored_reset: { s3_applied: true, history_rows: 0 }, empty_window_release_would_commit: true });
    expect(r.verdict).toBe("NOT_QUALIFIED");
  });

  test("injected erasures, simulated pending heads: timings only, NOT_QUALIFIED", async () => {
    const r = await run({ erasures: "injected", simulatePending: true });
    expect(statuses(r)).toMatchObject({ release: S3_OPEN, admission: "PASS", publication: "FAIL publication_not_exercised", reads: "FAIL old_events_not_canonical" });
    expect(r.verdict).toBe("NOT_QUALIFIED");
  });
});
