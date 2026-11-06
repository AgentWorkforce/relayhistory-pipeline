import { createHash, X509Certificate } from "node:crypto";
import { readFileSync, statSync } from "node:fs";
import type { ConnectionOptions } from "node:tls";
import { SUPABASE_PROD_CA_2021 } from "../session-pipeline/supabase-ca.ts";

export interface PostgresTlsEnv {
  SESSION_POSTGRES_TLS_PROFILE?: string;
  SESSION_POSTGRES_CA_FILE?: string;
  SESSION_POSTGRES_CA_SHA256?: string;
}

export class PostgresTlsConfigError extends Error {
  constructor() { super("PostgreSQL TLS configuration is invalid"); this.name = "PostgresTlsConfigError"; }
}

/** Explicit trust only. CA files contain public certificates, never credentials.
 * No provider lookup, system-root fallback, URL TLS overrides or hostname bypass.
 * An unset profile retains the existing pinned Supabase trust anchor. */
export function postgresTlsOptions(env: PostgresTlsEnv = process.env): ConnectionOptions & { ca: string; rejectUnauthorized: true } {
  const profile = env.SESSION_POSTGRES_TLS_PROFILE ?? "supabase";
  if (profile === "supabase") {
    if (env.SESSION_POSTGRES_CA_FILE !== undefined || env.SESSION_POSTGRES_CA_SHA256 !== undefined) throw new PostgresTlsConfigError();
    return { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true };
  }
  if (profile !== "custom-ca" || !env.SESSION_POSTGRES_CA_FILE || !/^[a-f0-9]{64}$/.test(env.SESSION_POSTGRES_CA_SHA256 ?? "")) throw new PostgresTlsConfigError();
  try {
    const path = env.SESSION_POSTGRES_CA_FILE;
    const stat = statSync(path);
    if (!stat.isFile() || stat.size < 1 || stat.size > 131_072) throw new PostgresTlsConfigError();
    const bytes = readFileSync(path);
    if (bytes.length > 131_072 || createHash("sha256").update(bytes).digest("hex") !== env.SESSION_POSTGRES_CA_SHA256) throw new PostgresTlsConfigError();
    const text = bytes.toString("utf8");
    const pattern = /-----BEGIN CERTIFICATE-----\r?\n[A-Za-z0-9+/=\r\n]+\r?\n-----END CERTIFICATE-----/g;
    const certificates = [...text.matchAll(pattern)].map(m => m[0]);
    if (!certificates.length || certificates.length > 16 || text.replace(pattern, "").trim()) throw new PostgresTlsConfigError();
    for (const pem of certificates) {
      const cert = new X509Certificate(pem);
      if (!cert.ca || Date.parse(cert.validFrom) > Date.now() || Date.parse(cert.validTo) <= Date.now()) throw new PostgresTlsConfigError();
    }
    return { ca: text, rejectUnauthorized: true };
  } catch { throw new PostgresTlsConfigError(); }
}

/** pg-connection-string otherwise lets URL query options replace the explicit
 * ssl object. Remove all TLS spellings before the pool sees the URL. */
export function postgresConnectionString(value: string): string {
  try {
    const url = new URL(value);
    if (!["postgres:", "postgresql:"].includes(url.protocol) || !url.hostname) throw new PostgresTlsConfigError();
    for (const key of [...url.searchParams.keys()]) if (/^ssl/i.test(key) || key.toLowerCase() === "options") url.searchParams.delete(key);
    // Query-string host overrides are not a second target selection mechanism.
    for (const key of url.searchParams.keys()) if (["host", "hostaddr", "port", "user", "password", "database"].includes(key.toLowerCase())) throw new PostgresTlsConfigError();
    return url.toString();
  } catch { throw new PostgresTlsConfigError(); }
}
