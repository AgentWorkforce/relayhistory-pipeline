// Held components only. No registration in the migration scanner or release selector.
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

function component(name: string) {
  const text = readFileSync(new URL(name, import.meta.url), "utf8");
  // Older components own a transaction when run independently. The migration
  // runner owns the whole transaction for a composed up or a guarded down.
  return text.replace(/^BEGIN;\n/m, "").replace(/\nCOMMIT;\s*$/, "\n");
}

function slot(file: string, parts: string[], downParts: string[], sha256: string) {
  const name = "upstream:" + file;
  const up = "SET LOCAL lock_timeout='5s';\n" + parts.map(component).join("\n");
  const actual = createHash("sha256").update(up).digest("hex");
  if (actual !== sha256) throw Error("Held publication slot checksum changed: "+name+" "+actual);
  const down = `BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL row_security=off;
SELECT pg_advisory_xact_lock(961996,1);
LOCK TABLE sessions.__96bulls_migrations IN SHARE ROW EXCLUSIVE MODE;
DO $guard$ BEGIN
 IF EXISTS (SELECT FROM sessions.__96bulls_migrations WHERE name LIKE 'upstream:%' AND name COLLATE "C">'${name}')
    OR NOT EXISTS (SELECT FROM sessions.__96bulls_migrations WHERE name='${name}' AND sha256='${sha256}')
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='publication rollback requires the exact latest upstream entry'; END IF;
END $guard$;
${downParts.map(component).join("\n")}
DELETE FROM sessions.__96bulls_migrations WHERE name='${name}';
COMMIT;
`;
  return {name, sha256, up, down};
}

export const publicationKernelSlot = slot("20261105062280_publication_scoped_kernels.sql",
  ["062280-scoped-kernels.up.sql"], ["062280-scoped-kernels.down.sql"], "02453c5d633fc6657591f0bf883e4014f2ffe19949201f90f3102772558471ae");
export const publicationRecoverySlot = slot("20261105062290_publication_fleet_recovery.sql",
  ["062290-scoped-recover.up.sql","062290-fleet-storage.up.sql"],
  ["062290-fleet-storage.down.sql","062290-scoped-recover.down.sql"], "56fc45e66eafb05721edb050bf5ba0c81a3aa815b0e3a85bee0ad697c354ba81");

// Registration candidates for the production migrator's sentinel map. These
// structural checks complement the applied-byte checksum and role tests.
export const heldPublicationSentinels = {
  [publicationKernelSlot.name]: {
    evidence: "private stage admissions and all seven scoped publication wrappers",
    sql: `SELECT to_regclass('publication_private.admission') IS NOT NULL
      AND NOT EXISTS (SELECT FROM unnest(ARRAY['candidates','claim','heartbeat','prepare','register_objects','publish','abandon']) op
      WHERE to_regprocedure('sessions.ingest_publication_scoped_'||op||'_v1(jsonb)') IS NULL) AS applied`,
  },
  [publicationRecoverySlot.name]: {
    evidence: "expired ticket history, scoped recovery, fleet authorization and durable checkpoint/retry state",
    sql: `SELECT to_regprocedure('sessions.ingest_publication_scoped_recover_v1(jsonb)') IS NOT NULL
      AND to_regclass('publication_private.expired_tickets') IS NOT NULL
      AND NOT EXISTS (SELECT FROM unnest(ARRAY['publication_scheduler_fleets','publication_scheduler_scopes',
      'publication_fleet_cursors','publication_retry_checkpoints','publication_receipt_retries','publication_receipt_retries_due']) rel
      WHERE to_regclass('sessions.'||rel) IS NULL) AS applied`,
  },
};
