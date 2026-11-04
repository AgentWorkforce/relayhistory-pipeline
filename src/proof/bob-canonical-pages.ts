/** Read-only capacity proof against existing canonical-artifact fixture objects.
 * Usage: bun run src/proof/bob-canonical-pages.ts <proof-dir> <foundation-checkout>
 * The explicit foundation checkout is a test oracle until source composition.
 * No service credentials, SQL mutations, new fold, or source transcript logging.
 */
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { resolve, join } from "node:path";
import { tmpdir } from "node:os";
import { randomBytes, randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";
import { createFsStore } from "../../packages/object-store/src/fs.ts";
import { objectKey } from "../../packages/object-store/src/index.ts";
import { pageDigest } from "../bob-costas/page-digest.ts";
import { extractiveSummarizer } from "../bob-costas/summarizer.ts";
import { prepareSummaryObject, readSummaryObject, type SummaryKind } from "../bob-costas/summary-objects.ts";
import { masterKeyring, parseMasterKeys } from "../shared/keyring.ts";

const [fixtureArg, foundationArg] = process.argv.slice(2);
if (!fixtureArg || !foundationArg) throw new Error("proof and foundation paths required");
const fixtureRoot = resolve(fixtureArg), foundationRoot = resolve(foundationArg);
const claim = JSON.parse(await readFile(join(fixtureRoot, "source/claim.json"), "utf8"));
const fixture = JSON.parse(await readFile(join(fixtureRoot, "source/fixture.json"), "utf8"));
const { canonicalArtifactPage } = await import(pathToFileURL(join(foundationRoot, "src/session-read-api/canonical-artifact-reader.ts")).href);
const store = createFsStore({ root: join(fixtureRoot, "source/objects") });
const signal = AbortSignal.timeout(10 * 60_000);
let pages = 0, publicationReads = 0, objectsRead = 0;
const start = performance.now();
const digest = await pageDigest({
  identity: claim.identity, generation: claim.generation, signal,
  readPage: async (query, signal) => {
    pages++;
    return canonicalArtifactPage({
      publication: async () => { publicationReads++; return claim; },
      read: async (key: string) => { objectsRead++; return store.get(objectKey(key)); }, signal,
    }, claim.identity, query);
  },
});
if (digest.records !== fixture.expectedEvents) throw new Error("canonical event count mismatch");
if (digest.userTurns.length > 10 || digest.models.length > 64 || Object.keys(digest.toolCalls).length > 128)
  throw new Error("retained digest exceeded bounds");
const summaries = await Promise.all([extractiveSummarizer.summarize("short", digest), extractiveSummarizer.summarize("long", digest)]);
if (summaries.some(s => !s.text)) throw new Error("empty summary");
const outputRoot = await mkdtemp(join(tmpdir(), "bob-canonical-summary-proof-"));
let encryptedObjectsVerified = 0;
try {
  const outputStore = createFsStore({ root: outputRoot });
  const keyring = await masterKeyring(parseMasterKeys(`fixture:${randomBytes(32).toString("hex")}`));
  const ids = { orgId: claim.identity.orgId, workspaceId: claim.identity.workspaceId };
  for (const [i, kind] of (["short", "long"] as SummaryKind[]).entries()) {
    // Fixture-only paths: Native owns production publication-qualified keys.
    const key = `fixture/${randomUUID()}/${kind}.json.enc`;
    const prepared = await prepareSummaryObject({ keyring, ids, key, record: {
      v: 1, kind, revision: 1, status: "ready", summary: summaries[i]!.text,
      model: extractiveSummarizer.id, built_at: new Date().toISOString(),
      source_content_sha256: claim.source.fingerprint, trace_id: randomUUID(),
    } });
    await outputStore.put(objectKey(key), prepared.body, { ifNoneMatch: true, contentSha256: prepared.sha256 });
    const read = await readSummaryObject({ store: outputStore, keyring, ids, key, sha256: prepared.sha256, kind, revision: 1 });
    if (read?.summary !== summaries[i]!.text) throw new Error("large-page summary decrypt mismatch");
    encryptedObjectsVerified++;
  }
} finally { await rm(outputRoot, { recursive: true, force: true }); }
// Exercise the actual authority reader's final-page recheck, not only a stub.
let authorityCalls = 0, refused = false;
try {
  await canonicalArtifactPage({ publication: async () => ++authorityCalls === 1 ? claim : { ...claim, current: false },
    read: (key: string) => store.get(objectKey(key)), signal }, claim.identity,
    { table: "convergence_events", view: "replay", order: "asc", limit: 1, maxContent: 2000, generation: claim.generation });
} catch { refused = true; }
if (!refused || authorityCalls !== 2) throw new Error("publication freshness negative control failed");
console.log(JSON.stringify({ proof: "bob-canonical-pages", sourceRawBytes: fixture.sourceRawBytes,
  records: digest.records, pages, publicationReads, objectsRead,
  retainedPromptSamples: digest.userTurns.length, retainedPromptChars: digest.userTurns.reduce((n, s) => n + s.length, 0),
  models: digest.models.length, tools: Object.keys(digest.toolCalls).length,
  summaryChars: summaries.map(s => s.text.length), encryptedObjectsVerified, publicationChangeRefused: refused,
  seconds: (performance.now() - start) / 1000, maxRssKiB: process.resourceUsage().maxRSS,
  activated: false, managedAuthorityComposed: false, productionReady: false }, null, 2));
