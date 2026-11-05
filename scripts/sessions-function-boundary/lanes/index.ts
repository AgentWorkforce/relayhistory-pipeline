import type { LaneManifest } from "../types.ts";
import { canonicalProjectEvents } from "./canonical-project-events.ts";
import { ingestHistory } from "./ingest-history.ts";
import { readApi } from "./read-api.ts";

/** Every lane the unified generator composes. Order does not affect the output. */
export const LANES: readonly LaneManifest[] = [ingestHistory, readApi, canonicalProjectEvents];
