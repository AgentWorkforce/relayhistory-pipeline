/**
 * Writes the selfhost-control tokens file (src/selfhost-control/server.ts,
 * SELFHOST_CONTROL_TOKENS_FILE) for the local single-user stack, run by the
 * selfhost-control container in compose.yaml just before the service starts.
 *
 * Reads RH_USER_SUB, RH_USER_ORG, RH_WORKSPACE and RH_CONTROL_TOKEN from the
 * environment and writes {"sub","org","workspace","tokens":[one rth:read +
 * rth:sync token]} at SELFHOST_CONTROL_TOKENS_FILE with mode 0600. compose.yaml
 * points that at a tmpfs, so the token is never written to disk. The service
 * itself validates every field; this only refuses a missing value. Errors name
 * the variable, never a value. Standalone (node:fs only): the container mounts
 * this one file.
 */
import { chmodSync, writeFileSync } from "node:fs";

export const TOKEN_ENV = ["RH_USER_SUB", "RH_USER_ORG", "RH_WORKSPACE", "RH_CONTROL_TOKEN"] as const;

export interface LocalTokensDocument {
  sub: string;
  org: string;
  workspace: string;
  tokens: { token: string; token_id: string; scopes: string[] }[];
}

export function tokensDocument(env: NodeJS.ProcessEnv): LocalTokensDocument {
  for (const name of TOKEN_ENV) if (!env[name]?.trim()) throw new Error(`control-tokens: ${name} is required`);
  return {
    sub: env.RH_USER_SUB!.trim(),
    org: env.RH_USER_ORG!.trim(),
    workspace: env.RH_WORKSPACE!.trim(),
    tokens: [{ token: env.RH_CONTROL_TOKEN!.trim(), token_id: "local-1", scopes: ["rth:read", "rth:sync"] }],
  };
}

export function writeTokensFile(env: NodeJS.ProcessEnv): void {
  const file = env.SELFHOST_CONTROL_TOKENS_FILE?.trim();
  if (!file) throw new Error("control-tokens: SELFHOST_CONTROL_TOKENS_FILE is required");
  writeFileSync(file, JSON.stringify(tokensDocument(env)), { mode: 0o600 });
  chmodSync(file, 0o600); // mode applies only on create; a restart rewrites an existing file
}

if (import.meta.main) {
  try {
    writeTokensFile(process.env);
  } catch (error) {
    console.error(error instanceof Error && error.message.startsWith("control-tokens:") ? error.message : "control-tokens: cannot write the tokens file");
    process.exit(2);
  }
}
