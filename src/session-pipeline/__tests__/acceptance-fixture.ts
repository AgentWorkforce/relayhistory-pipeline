/** Synthetic acceptance-missing fixtures shared by the forward-port tests. */
import { activity, generate, type Line } from "../../session-read-api/__tests__/stream-fixture.ts";

/**
 * generate(8) (one machine, so no record is copied across manifests) plus two manifests of their own (machines
 * contend-a and contend-b), each carrying one record id
 * from its own origin without an acceptance seat: the whole fold refuses with relayhistory_acceptance_missing
 * alone, and each manifest still folds on its own (the outcome SEARCH_INDEX_SKIP_ACCEPTANCE_MISSING_ENABLED stamps).
 */
export function crossManifestAcceptanceLines(): Line[] {
  const lines: Line[] = generate(8);
  lines.push(
    activity("machine:contend-a", "capacity:contend-a", "session_event", "unseated", Date.parse("2026-11-01T10:00:00.600Z"), { role: "user", text: "a" }),
    activity("machine:contend-b", "capacity:contend-b", "session_event", "unseated", Date.parse("2026-11-01T10:00:00.700Z"), { role: "user", text: "b" }),
  );
  return lines;
}
