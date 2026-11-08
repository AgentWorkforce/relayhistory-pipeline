import type { LaneManifest } from "../types.ts";
import { briefDocuments } from "./brief-documents.ts";
import { briefTitles } from "./brief-titles.ts";
import { ownerBufferedRead } from "./owner-buffered-read.ts";
import { bufferedSearchGate } from "./buffered-search-gate.ts";
import { bufferedSearchFeed } from "./buffered-search-feed.ts";
import { bufferedGcSingleCandidate } from "./buffered-gc-single-candidate.ts";
import { acquisitionOwner } from "./acquisition-owner.ts";
import { babysitterRetention } from "./babysitter-retention.ts";
import { canonicalFeatureConsumer } from "./canonical-feature-consumer.ts";
import { lazySourceTables } from "./lazy-source-tables.ts";
import { lazyClaimClockNarrowing } from "./lazy-claim-clock-narrowing.ts";
import { lazyAckClockNoKeyUpdate } from "./lazy-ack-clock-no-key-update.ts";
import { privacyAuthority } from "./privacy-authority.ts";
import { sessionDirectoryDocumentPool } from "./session-directory-document-pool.ts";
import { claimRecordsRetirement } from "./claim-records-retirement.ts";
import { canonicalProjectEvents } from "./canonical-project-events.ts";
import { desktopCatalog } from "./desktop-catalog.ts";
import { ingestHistory } from "./ingest-history.ts";
import { readApi } from "./read-api.ts";
import { semanticCopy } from "./semantic-copy.ts";
import { ingestBookkeepingCleanup } from "./ingest-bookkeeping-cleanup.ts";
import { bufferedSearchGateRetiredWitness } from "./buffered-search-gate-retired-witness.ts";

/** Every lane the unified generator composes. Order does not affect the output. */
export const LANES: readonly LaneManifest[] = [briefDocuments, ingestHistory, readApi, canonicalProjectEvents, canonicalFeatureConsumer, sessionDirectoryDocumentPool, desktopCatalog, semanticCopy, claimRecordsRetirement, bufferedGcSingleCandidate, lazySourceTables, privacyAuthority, babysitterRetention, briefTitles, lazyClaimClockNarrowing, ownerBufferedRead, lazyAckClockNoKeyUpdate, acquisitionOwner, bufferedSearchGate, bufferedSearchFeed, ingestBookkeepingCleanup, bufferedSearchGateRetiredWitness];
