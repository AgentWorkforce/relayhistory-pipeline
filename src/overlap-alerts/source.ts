import { scrubText } from "../../packages/pipeline/src/lib/scrub.ts";
import { type Dataset, parseDataset } from "./contracts.ts";
import { requireUnexpired } from "./artifacts.ts";

/** Offline intake of evidence obtained through the owner's signed-in session tools.
 * This module has no network or storage-reader credentials. The source adapter must
 * normalize only authorized tool results to Dataset; it must not invent visibility,
 * edit operations, arrival timestamps, or completeness missing from those results.
 */
export function importEvidence(input: unknown, now = new Date()): Dataset {
  const dataset = parseDataset(input);
  if (dataset.provenance.kind !== "production") throw new Error("production_source_required");
  requireUnexpired(dataset, now);
  if (Date.parse(dataset.provenance.capturedAt) > now.getTime()) throw new Error("future_capture");
  return parseDataset({ ...dataset,
    provenance: { ...dataset.provenance, reader: scrubText(dataset.provenance.reader), selection: scrubText(dataset.provenance.selection) },
    sessions: dataset.sessions.map(s => ({ ...s, title: scrubText(s.title),
      repository: s.repository === null ? null : scrubText(s.repository),
      evidence: s.evidence.map(e => ({ ...e, text: scrubText(e.text),
        files: e.files.map(f => ({ ...f, path: scrubText(f.path) })), taskRefs: e.taskRefs.map(scrubText) })) })),
  });
}
