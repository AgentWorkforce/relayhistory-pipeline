/**
 * Claim excerpts for the conflict DM (docs/coordination-live-advisories.md, "Conflict rows"). The decision-records
 * rule: a claim's text goes into a message only when the scrubber leaves it exactly as it is (and does not
 * refuse it), cut to 200 characters first. Otherwise the message carries ids only. No scrub logic lives here:
 * this only asks the vendored scrubber.
 */
import { scrubText } from "../shared/vendor.ts";
import type { ConflictAdvisory } from "./conflicts.ts";
import type { MessageExcerpt } from "./template.ts";

export const EXCERPT_MAX_CHARS = 200;

/** The excerpt, or null when the scrubber would change or refuse it. */
export function scrubCleanExcerpt(text: string, scrub: (t: string) => string = scrubText): string | null {
  const cut = Array.from(text).slice(0, EXCERPT_MAX_CHARS).join("");
  if (!cut.trim()) return null;
  try {
    return scrub(cut) === cut ? cut : null;
  } catch {
    return null;
  }
}

/** The scrub-clean excerpts of a conflict advisory's cited claims, in citation order. */
export function conflictExcerpts(a: Pick<ConflictAdvisory, "claimTexts">, scrub?: (t: string) => string): MessageExcerpt[] {
  return a.claimTexts.flatMap((c) => {
    const text = scrubCleanExcerpt(c.text, scrub);
    return text === null ? [] : [{ label: c.kind, text }];
  });
}
