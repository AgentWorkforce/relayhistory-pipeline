export const ERASE_FENCE_FLAG = "SESSION_ERASE_FENCE_ENFORCED";
export const ERASE_BYTES_FLAG = "SESSION_ERASE_BYTES_ENABLED";
export function eraseRuntimeFlag(env: Record<string, string | undefined>, name: string): boolean {
  const raw = env[name];
  if (raw === undefined || raw === "" || raw === "0") return false;
  if (raw === "1") return true;
  throw new Error(`96bulls: ${name} must be unset, "0" or "1"`);
}
export function eraseRuntimeSettings(env: Record<string, string | undefined>, api: { enabled: boolean }, admission?: unknown) {
  const enforced = eraseRuntimeFlag(env, ERASE_FENCE_FLAG), bytes = eraseRuntimeFlag(env, ERASE_BYTES_FLAG);
  if ((api.enabled || bytes) && !enforced) throw Error(`96bulls: operator erase needs ${ERASE_FENCE_FLAG}=1`);
  if (enforced && admission) throw Error("96bulls: operator erase requires synchronous self-host admission");
  if (enforced && env.HISTORY_UPLOAD_DIRECT && env.HISTORY_UPLOAD_DIRECT !== "off")
    throw Error("96bulls: operator erase requires direct desktop uploads to remain off");
  return { ...(enforced ? { eraseFenceEnforced: true as const } : {}), ...(bytes ? { eraseBytesEnabled: true as const } : {}) };
}
