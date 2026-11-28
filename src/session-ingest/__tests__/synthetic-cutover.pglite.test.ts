/**
 * The synthetic per-org cutover qualification (e2e/cutover) on the CI fixture: the harness itself, and that it FAILS
 * CLOSED while the release-path heads are not on main. Pins what main does today, step by step, so a head landing
 * (or a regression) changes this file on purpose:
 *   - default: every step through the drain passes on main's real code (the hold, seal and marker through stand-ins
 *     over the 062320/062340 SQL), except that S3 admission reopens at the marker before bridge_complete; release is
 *     BLOCKED on the generation proof; nothing after it runs; no downtime is reported.
 *   - simulated pending heads: release and resumed admission run and the downtime is measured, the backlog is not
 *     in the canonical layout after the drain (old_events_not_canonical), and the verdict stays NOT_QUALIFIED.
 */
import { describe, expect, setDefaultTimeout, test } from "bun:test";
import { SIZE_CI } from "../../../e2e/cutover/fixture.ts";
import { reportMarkdown, runCutoverQualification, type CutoverReport } from "../../../e2e/cutover/harness.ts";
import { allLanded, defaultHeads } from "../../../e2e/cutover/heads.ts";

setDefaultTimeout(240_000);
const statuses = (r: CutoverReport) => Object.fromEntries(r.steps.map((s) => [s.id, s.code ? `${s.status} ${s.code}` : s.status]));

describe("synthetic per-org cutover qualification", () => {
  test("fails closed on main: release blocked on the generation proof, no resumed admission, NOT_QUALIFIED", async () => {
    const r = await runCutoverQualification({ size: SIZE_CI });
    expect(statuses(r)).toEqual({
      seed: "PASS", "rearm.list": "PASS", "hold.install": "PASS", full: "PASS", "seal.bind": "PASS", catchup: "PASS",
      "reshard.verify": "PASS", marker: "PASS", "marker.verify": "FAIL s3_admits_before_bridge_complete", drain: "PASS",
      release: "BLOCKED erasure_generation_unproven", admission: "SKIP", publication: "SKIP", reads: "SKIP",
    });
    expect(r.verdict).toBe("NOT_QUALIFIED");
    expect(r.downtime.downtime_ms).toBeNull();
    // Every SQL upload during the hold was refused RH042; the S3 path admitted once the marker existed.
    expect(r.uploads.filter((u) => u.path === "sql" && u.step !== "seed").every((u) => u.outcome === "RH042")).toBe(true);
    expect(r.uploads.filter((u) => u.path === "s3").every((u) => u.outcome === "accepted")).toBe(true);
    // Nonempty erasures before and during the hold, and the history recorded the in-hold ones.
    expect(Object.values(r.erasures.before_hold).reduce((a, b) => a + b, 0)).toBe(3);
    expect(Object.values(r.erasures.during_hold).reduce((a, b) => a + b, 0)).toBe(3);
    expect(Object.keys(r.erasures.history_since_install ?? {}).sort()).toEqual(["operator_user_erasures", "session_erasure_fences", "upload_deletions"]);
    expect(r.size.batches).toBe(SIZE_CI.users * SIZE_CI.sessionsPerUser * SIZE_CI.batchesPerSession);
    expect(reportMarkdown(r)).toContain("**Verdict: NOT_QUALIFIED**");
  });

  test("simulated pending heads: downtime measured, still NOT_QUALIFIED", async () => {
    const r = await runCutoverQualification({ size: SIZE_CI, simulatePending: true });
    expect(statuses(r)).toMatchObject({ release: "PASS", admission: "PASS", publication: "PASS", reads: "FAIL old_events_not_canonical" });
    expect(r.simulated).toBe(true);
    expect(r.verdict).toBe("NOT_QUALIFIED");
    expect(r.reasons.some((x) => x.startsWith("simulated"))).toBe(true);
    expect(r.downtime.downtime_ms).toBeGreaterThan(0);
    // Every upload refused during the hold was admitted on retry after the release.
    expect(r.uploads.filter((u) => u.step === "admission.retry").every((u) => u.outcome === "accepted")).toBe(true);
  });

  test("QUALIFIED needs every adapter landed: the defaults are stand-ins and pending heads", () => {
    const h = defaultHeads();
    expect(allLanded(h)).toBe(false);
    expect(h.userEraseReplay.integration).toBe("pending");
    expect(h.sessionEraseReplay.integration).toBe("pending");
    expect(defaultHeads({ simulatePending: true }).userEraseReplay.integration).toBe("simulated");
  });
});
