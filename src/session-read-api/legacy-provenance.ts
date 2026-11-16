/**
 * Shared provenance gate for legacy evidence in canonical whole and streamed
 * reads. Legacy records bypass delivery acceptance and owner binding, so they
 * are admitted only from one exporter-owned, user-scoped manifest containing
 * nothing but in-scope legacy records.
 */
import type { ManifestV1 } from "../../packages/object-store/src/index.ts";
import {
  isLegacyRelayhistoryRecord,
  type RelayhistoryStreamRecord,
} from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import { legacyMachineId } from "../shared/legacy-export-machine.ts";
import { canonicalManifestIdentity } from "../shared/manifest-identity.ts";

export const LEGACY_PROVENANCE_INVALID = "legacy_provenance_invalid";

export interface LegacyProvenanceScope {
  orgId: string;
  workspaceId: string;
  harness: string;
  sessionId: string;
}

export interface LegacyProvenancePart {
  manifest: ManifestV1;
  userId: string;
}

/**
 * Observe each record exactly once, in any order, then read `invalid`. The
 * state is deliberately payload-free: the streaming reader can share this
 * gate without retaining records or customer content.
 */
export function legacyProvenanceGate(
  scope: LegacyProvenanceScope,
  parts: readonly LegacyProvenancePart[],
) {
  const legacyParts = new Set<number>();
  const liveParts = new Set<number>();
  let invalidRecord = false;

  const owned = (part: number) => {
    const { manifest, userId } = parts[part]!;
    const marker = manifest.extensions?.["client.relayhistory_export"] as
      | { grant_id?: unknown }
      | undefined;
    return (
      typeof marker?.grant_id === "string" &&
      manifest.raw.owner === `migration:${marker.grant_id}` &&
      manifest.raw.epochs.length === 1 &&
      manifest.raw.epochs[0]!.reason === "initial" &&
      canonicalManifestIdentity(manifest).machineId ===
        legacyMachineId(manifest.session.workspace_id, userId)
    );
  };

  const inScope = (record: RelayhistoryStreamRecord) => {
    const payload = record.payload;
    return (
      payload?.org_id === scope.orgId &&
      payload.workspace_id === scope.workspaceId &&
      payload.source === scope.harness &&
      payload.session_id === scope.sessionId
    );
  };

  return {
    observe(part: number, record: RelayhistoryStreamRecord): void {
      if (!isLegacyRelayhistoryRecord(record)) {
        liveParts.add(part);
        if (legacyParts.has(part)) invalidRecord = true;
        return;
      }
      legacyParts.add(part);
      if (liveParts.has(part) || !owned(part) || !inScope(record))
        invalidRecord = true;
    },
    get invalid(): boolean {
      return invalidRecord || legacyParts.size > 1;
    },
    get hasLegacy(): boolean {
      return legacyParts.size > 0;
    },
  };
}
