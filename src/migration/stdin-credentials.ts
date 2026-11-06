/**
 * Credentials for operator jobs come only through stdin, held in RAM: never argv, env, or a file path
 * (AGENTS.md "nothing credential-like on disk"; 1Password is the system of record). The caller pipes one
 * JSON object, e.g. `op read ... | jq ... | bun run <job>`; nothing is echoed or logged.
 */
import { readFileSync, fstatSync } from "node:fs";

/** Environment names that would put a credential, or a path to one, outside RAM. Any of them refuses. */
export const FORBIDDEN_CREDENTIAL_ENV = [
  "NEON_URL",
  "NEON_URL_FILE",
  "SUPABASE_URL_FILE",
  "DATABASE_URL",
  "S3X_CREDS_FILE",
  "S3X_ERASER_CREDS_FILE",
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "AWS_SESSION_TOKEN",
  "AWS_SHARED_CREDENTIALS_FILE",
  "AWS_PROFILE",
] as const;

export const MAX_STDIN_CREDENTIAL_BYTES = 64 * 1024;

export class CredentialRefusal extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "CredentialRefusal";
  }
}

/** Refuse when any credential-bearing environment name is set (even empty), naming only the variable. */
export function refuseCredentialEnv(env: Record<string, string | undefined> = process.env): void {
  for (const k of FORBIDDEN_CREDENTIAL_ENV) if (env[k] !== undefined) throw new CredentialRefusal(`credential_env_refused.${k}`);
}

/** Parse one bounded JSON object of exactly `allowed` keys (all of `required` present). Values never leave RAM. */
export function parseCredentialDocument(text: string, required: readonly string[], allowed: readonly string[] = required): Record<string, unknown> {
  if (Buffer.byteLength(text) > MAX_STDIN_CREDENTIAL_BYTES) throw new CredentialRefusal("credentials_too_large");
  let v: unknown;
  try {
    v = JSON.parse(text);
  } catch {
    throw new CredentialRefusal("credentials_not_json");
  }
  if (!v || typeof v !== "object" || Array.isArray(v)) throw new CredentialRefusal("credentials_not_object");
  const doc = v as Record<string, unknown>;
  for (const k of Object.keys(doc)) if (!allowed.includes(k)) throw new CredentialRefusal(`credentials_unknown_key.${k}`);
  for (const k of required) if (doc[k] === undefined) throw new CredentialRefusal(`credentials_missing_key.${k}`);
  return doc;
}

/** Read fd 0 once. Refuses a terminal (a typed secret would echo) and a regular file (that is a file on disk). */
export function readStdinCredentials(required: readonly string[], allowed: readonly string[] = required, fd = 0): Record<string, unknown> {
  const st = fstatSync(fd);
  if (st.isCharacterDevice()) throw new CredentialRefusal("credentials_stdin_is_a_terminal");
  if (st.isFile()) throw new CredentialRefusal("credentials_stdin_is_a_file");
  return parseCredentialDocument(readFileSync(fd, "utf8"), required, allowed);
}

export function postgresUrl(v: unknown, name: string): string {
  if (typeof v !== "string") throw new CredentialRefusal(`credentials_invalid.${name}`);
  let u: URL;
  try {
    u = new URL(v);
  } catch {
    throw new CredentialRefusal(`credentials_invalid.${name}`);
  }
  if (u.protocol !== "postgres:" && u.protocol !== "postgresql:") throw new CredentialRefusal(`credentials_invalid.${name}`);
  return v;
}

export interface S3Credentials {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
}

export function s3Credentials(v: unknown, name: string): S3Credentials {
  const c = v as Record<string, unknown> | null;
  if (!c || typeof c !== "object" || Array.isArray(c)) throw new CredentialRefusal(`credentials_invalid.${name}`);
  for (const k of Object.keys(c)) if (!["accessKeyId", "secretAccessKey", "sessionToken"].includes(k)) throw new CredentialRefusal(`credentials_invalid.${name}`);
  if (typeof c.accessKeyId !== "string" || !c.accessKeyId || typeof c.secretAccessKey !== "string" || !c.secretAccessKey) throw new CredentialRefusal(`credentials_invalid.${name}`);
  if (c.sessionToken !== undefined && (typeof c.sessionToken !== "string" || !c.sessionToken)) throw new CredentialRefusal(`credentials_invalid.${name}`);
  return { accessKeyId: c.accessKeyId, secretAccessKey: c.secretAccessKey, ...(c.sessionToken ? { sessionToken: c.sessionToken as string } : {}) };
}
