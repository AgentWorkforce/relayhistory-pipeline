/**
 * T124 (Pipeline 220fe2274d20 / ec758b1fab8e): a below-threshold scope B whose S3-event delivery is lost, with no later
 * append, recovers only through a durable obligation written with its pointer. The candidate is the managed search
 * wake: hoopDbSessionIndex's publication transaction writes the `search-index` job (pipeline_jobs) in the SAME
 * authority transaction as the pointer when publication.enqueueSearchWake is exactly true (lambda.ts:
 * SEARCH_WAKE_ENQUEUE=1; consumer-service.ts / indexer.ts: their own flags), and the managed search-jobs drain
 * (SEARCH_JOBS_ENABLED) claims and runs it.
 *
 * Enabled test flags: publication.enqueueSearchWake = true (publishedHead's default). Everything else default.
 * Real: migrations on PGlite, the ingest delivery, the portable publisher against a MinIO-shaped fake, object-landed and
 * the managed session-indexed authority path (search-jobs-publication.ts publishedHead), claimSearchJobs,
 * runSearchJob with the real indexSearchScope and a recording ClickHouse, settleSearchJob.
 * Suppressed: B's S3-event delivery to the session pipeline consumer (processBatch is never called) and every later
 * append. Not covered: an actual SQS loss, the hosted schedule that drains held scopes, and any read endpoint past the
 * search state rows (no read-API authorized read; T139).
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { claimSearchJobs, runSearchJob, settleSearchJob } from "../search-jobs.ts";
import { objectStoreManifestReader } from "../../shared/vendor.ts";
import type { SearchIndexDeps } from "../search-index.ts";
import { publishedHead, type PublishedHead } from "./search-jobs-publication.ts";
import { recordingReplayClickHouse } from "./replay-clickhouse-fixture.ts";

describe("below-threshold B, delivery lost, no later append: the managed wake committed with the pointer recovers it", () => {
  let head: PublishedHead;
  beforeAll(async () => { head = await publishedHead({ enqueueSearchWake: true }); }, 180_000);
  afterAll(() => head?.close());

  test("the wake commits with the pointer; with B's delivery suppressed, the drain claims it, runs B's managed pass and settles done", async () => {
    // The pointer and its wake committed together in the authority transaction (no consumer ran for B).
    expect(head.authority).toMatchObject({ acked: 1 });
    const pointer = (await head.db.query<{ n: number }>("SELECT count(*)::int n FROM sessions.session_index WHERE manifest_key=$1 AND deleted_at IS NULL", [head.key])).rows[0]!.n;
    expect(pointer).toBe(1);
    const wakes = head.jobsAfter.filter((j) => j.stage === "search-index");
    expect(wakes).toHaveLength(1);
    // Below the large-read discovery thresholds: discovery would never pick B up.
    const size = (await head.db.query<{ b: string; n: string }>(
      "SELECT coalesce(sum(raw_bytes),0)::text b, coalesce(sum(event_count),0)::text n FROM sessions.session_index WHERE manifest_key=$1", [head.key])).rows[0]!;
    expect(BigInt(size.b)).toBeLessThanOrEqual(67108864n);
    expect(BigInt(size.n)).toBeLessThanOrEqual(100000n);
    // The drain: claim, run B's managed pass, settle.
    const [claimed] = await claimSearchJobs(head.db, 1);
    expect(claimed).toBeDefined();
    const recorded = recordingReplayClickHouse();
    const prefix = head.key.slice(0, head.key.indexOf("/", head.key.indexOf("/") + 1));
    const search = {
      transact: (work) => head.db.transaction((tx) => work(tx)),
      reader: objectStoreManifestReader(head.store, { provider: "s3", bucket: "selfhost-sessions" }),
      store: head.store, ch: recorded.ch, prefixFor: () => prefix,
    } as SearchIndexDeps;
    const out = await runSearchJob(claimed!.claimedMessage, { search });
    expect(out.outcome).toBe("indexed");
    expect(await settleSearchJob(head.db, claimed!, "done")).toMatchObject({ applied: true });
    expect((await head.db.query("SELECT 1 FROM sessions.pipeline_jobs WHERE stage='search-index' AND dlq=false")).rows).toEqual([]);
    // Furthest endpoint verified here: B's search state written by its managed pass (recording ClickHouse).
    const states = Object.values(recorded.stats().states) as { session_id: string; needs_full: number }[];
    expect(states).toEqual([expect.objectContaining({ session_id: head.scope.sessionId, needs_full: 0 })]);
  }, 180_000);
});
