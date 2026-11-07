/**
 * --verify's coverage and shape rules, on a scripted query result (no database).
 *
 * VERIFY_SQL returns exactly one row per requested key. Anything else is not evidence about
 * pointers, so it must fail the run (malformed, exit 2):
 *   - an empty response;
 *   - a row lacking `live` (or with a non-boolean `live`);
 *   - a duplicate row;
 *   - a row for an unrequested key;
 *   - a served title on a row that says there is no live pointer;
 *   - a served title VERIFY_SQL cannot return: empty, blank, or not btrim()-ed.
 * no_pointer is counted only from a present, well-formed row with live === false.
 * These are causal: at 9419893d, verifyBackfill treated an absent row, or one lacking `live`,
 * as no_pointer and passed; at 677ed8b3 it accepted an empty or blank served title (e.g. as
 * native) and passed.
 */
import { describe, expect, test } from "bun:test";
import { verifyBackfill, verifyPassed, type NeonBriefRow } from "../neon-brief-titles/backfill.ts";

const neon = (over: Partial<NeonBriefRow> = {}): NeonBriefRow => ({
  org_id: "org-a", workspace_id: "ws-a", source_provider: "claude", session_id: "s-1", job_id: "j-1",
  title: "Fix login", description: "d", state: "completed", prompt_version: "session-summary-v3",
  summary_version: "session-summary-v3", model: "m", summary_model_version: "m", generated_at: "2026-10-01T00:00:00Z",
  completed_at: null, message_count: null, shortened_messages: null, input_tokens: null, output_tokens: null, ...over,
});
const key = { org_id: "org-a", workspace_id: "ws-a", harness: "claude", session_id: "s-1" };
const scripted = (rows: unknown[]) => ({ query: async () => ({ rows: rows as never[] }) });

describe("--verify coverage and row shape", () => {
  test("an empty response fails (malformed), never no_pointer", async () => {
    const r = await verifyBackfill(scripted([]), [neon()]);
    expect(r).toMatchObject({ compared: 1, malformed: 1, no_pointer: 0, equal: 0 });
    expect(verifyPassed(r)).toBe(false);
  });

  test("a row lacking live, or with a non-boolean live, fails (malformed)", async () => {
    for (const live of [undefined, "false", 0, null]) {
      const row: Record<string, unknown> = { ...key, title: null, origin: null };
      if (live !== undefined) row.live = live;
      const r = await verifyBackfill(scripted([row]), [neon()]);
      expect(r).toMatchObject({ malformed: 1, no_pointer: 0 });
      expect(verifyPassed(r)).toBe(false);
    }
  });

  test("a duplicate row, an unrequested row, or a title with no live pointer fails (malformed)", async () => {
    const ok = { ...key, title: "Fix login", origin: "neon_backfill", live: true };
    for (const rows of [
      [ok, ok],
      [ok, { ...key, session_id: "s-other", title: null, origin: null, live: false }],
      [{ ...key, title: "Fix login", origin: "neon_backfill", live: false }],
      [{ ...key, title: "Fix login", origin: "bogus", live: true }],
    ]) {
      const r = await verifyBackfill(scripted(rows), [neon()]);
      expect(r.malformed).toBeGreaterThanOrEqual(1);
      expect(verifyPassed(r)).toBe(false);
    }
  });

  test("a served title VERIFY_SQL cannot return (empty, blank, or untrimmed) fails (malformed)", async () => {
    // VERIFY_SQL serves btrim(title) WHERE btrim(title) <> '': none of these can come back.
    for (const title of ["", "   ", " Fix login", "Fix login "]) {
      for (const origin of ["native", "neon_backfill"]) {
        const r = await verifyBackfill(scripted([{ ...key, title, origin, live: true }]), [neon()]);
        expect(r).toMatchObject({ malformed: 1, native: 0, equal: 0, different: 0 });
        expect(verifyPassed(r)).toBe(false);
      }
    }
  });

  test("an all-whitespace served title the HTTP reader trims to empty (tab-only) fails (malformed)", async () => {
    // btrim keeps "\t", but the reader's JS trim() drops it, so the key would serve no title.
    for (const title of ["\t", "\t\t", "\u00a0"]) {
      for (const origin of ["native", "neon_backfill"]) {
        const r = await verifyBackfill(scripted([{ ...key, title, origin, live: true }]), [neon()]);
        expect(r).toMatchObject({ malformed: 1, native: 0, equal: 0, different: 0 });
        expect(verifyPassed(r)).toBe(false);
      }
    }
  });

  test("only spaces count as trim (PostgreSQL btrim): a tab- or newline-edged title is servable", async () => {
    // btrim(title) strips spaces only, so VERIFY_SQL can return these; they are well formed.
    for (const title of ["\tFix login", "Fix login\n"]) {
      const r = await verifyBackfill(scripted([{ ...key, title, origin: "native", live: true }]), [neon()]);
      expect(r).toMatchObject({ malformed: 0, native: 1 });
    }
  });

  test("well-formed rows keep their meaning: no_pointer excluded, live-but-missing fails, equal passes", async () => {
    const none = await verifyBackfill(scripted([{ ...key, title: null, origin: null, live: false }]), [neon()]);
    expect(none).toMatchObject({ no_pointer: 1, malformed: 0, missing: 0 });
    expect(verifyPassed(none)).toBe(true);
    const missing = await verifyBackfill(scripted([{ ...key, title: null, origin: null, live: true }]), [neon()]);
    expect(missing).toMatchObject({ missing: 1, malformed: 0 });
    expect(verifyPassed(missing)).toBe(false);
    const equal = await verifyBackfill(scripted([{ ...key, title: "Fix login", origin: "neon_backfill", live: true }]), [neon()]);
    expect(equal).toMatchObject({ equal: 1, malformed: 0 });
    expect(verifyPassed(equal)).toBe(true);
    const native = await verifyBackfill(scripted([{ ...key, title: "Other", origin: "native", live: true }]), [neon()]);
    expect(native).toMatchObject({ native: 1, malformed: 0 });
    expect(verifyPassed(native)).toBe(true);
  });

  test("a source that repeats a key is refused", async () => {
    await expect(verifyBackfill(scripted([]), [neon(), neon({ job_id: "j-2" })])).rejects.toThrow(/duplicate session key/);
  });
});
