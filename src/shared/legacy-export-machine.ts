import { createHash } from "node:crypto";

/** Synthetic origin used only to derive a user-scoped legacy export machine. */
export const LEGACY_EXPORT_ORIGIN_ID = "legacy:tables:v1";

/** Matches the live writer's machine derivation without importing migration code. */
export function legacyMachineId(workspaceId: string, userId: string): string {
  return `srv_${createHash("sha256")
    .update(`${workspaceId}\u001f${LEGACY_EXPORT_ORIGIN_ID}\u001f${userId}`)
    .digest("hex")
    .slice(0, 32)}`;
}
