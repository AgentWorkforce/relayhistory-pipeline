import { X509Certificate } from "node:crypto";
import { readFileSync, statSync } from "node:fs";
import { AnalyticsConfigError, type AnalyticsConfig } from "../shared/analytics-config.ts";
import type { Fetcher } from "../shared/analytics-export.ts";

const CA_FILE = "ANALYTICS_COLLECTOR_CA_FILE";
const MAX_CA_BYTES = 128 * 1024;

/** Extend trust only for the configured collector request. The brief worker's
 * Postgres and model clients keep their existing TLS configuration. */
export function briefCollectorFetch(
  config: AnalyticsConfig,
  file = process.env[CA_FILE],
): Fetcher | undefined {
  if (!file) return;
  if (config.sink !== "collector" || !config.url.startsWith("https://")) {
    throw new AnalyticsConfigError(CA_FILE, "requires an HTTPS collector destination");
  }
  if (!process.versions.bun) {
    throw new AnalyticsConfigError(CA_FILE, "requires the Bun service runtime");
  }
  let ca: string;
  try {
    const stat = statSync(file);
    if (!stat.isFile() || stat.size > MAX_CA_BYTES) throw new Error();
    ca = readFileSync(file, "utf8");
    const pem = /-----BEGIN CERTIFICATE-----\r?\n[A-Za-z0-9+/=\r\n]+\r?\n-----END CERTIFICATE-----/g;
    const certificates = ca.match(pem);
    if (Buffer.byteLength(ca) > MAX_CA_BYTES || !certificates?.length || ca.replace(pem, "").trim()) throw new Error();
    if (certificates.some((certificate) => !new X509Certificate(certificate).ca)) throw new Error();
  } catch {
    throw new AnalyticsConfigError(CA_FILE, "must reference a readable public CA bundle");
  }
  return (url, init) => {
    if (url !== config.url) return Promise.reject(new Error("unexpected analytics collector URL"));
    const options = { ...init, redirect: "manual" as const, tls: { ca, rejectUnauthorized: true } };
    return fetch(url, options);
  };
}
