// Synthetic direct-owner URL for tests: built from the pinned schema-release constants plus a fake password, so it
// passes the format checks without any real credential appearing in a test file.
import { SCHEMA_RELEASE } from "../../../hoopdb/schema-release-contract.ts";
export const FROZEN_COPY_TEST_OWNER_PASSWORD = "synthetic-owner-password-not-real";
export function syntheticOwnerUrl(o: { host?: string; port?: number; database?: string; user?: string; password?: string; query?: string } = {}) {
  const user = o.user ?? `${SCHEMA_RELEASE.owner}.${SCHEMA_RELEASE.project}`;
  return `postgresql://${encodeURIComponent(user)}:${encodeURIComponent(o.password ?? FROZEN_COPY_TEST_OWNER_PASSWORD)}@${o.host ?? SCHEMA_RELEASE.host}:${o.port ?? SCHEMA_RELEASE.port}/${o.database ?? SCHEMA_RELEASE.database}${o.query ?? ""}`;
}
/** Passes the driver's existing Neon production-owner pattern; the host is .invalid and the password fake. */
export const SYNTHETIC_NEON_URL = "postgresql://neondb_owner:synthetic-not-authorized@ep-floral-pond-apkvat38.synthetic.invalid/neondb";
