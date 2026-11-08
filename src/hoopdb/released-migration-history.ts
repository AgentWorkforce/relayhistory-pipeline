/**
 * Released migration names are immutable history too. The two original files
 * below shipped in this image and match its installed ledger byte for byte:
 * ghcr.io/agentworkforce/96bulls-services@sha256:070fe6fd46ecd1e1f9db32ff22379f6df9b9e6c4d48ede5312c8fb986a95f3a9
 * (source f1c2672581b6f88d26d0549b4d1cb78fce0d5f16).
 *
 * The later release renamed them as if they had never shipped. Keep physical
 * ledger rows/checksums untouched; only interpret these exact historical pairs
 * as the corresponding current steps. This is not a generic import mechanism.
 * The ingest rename also removed two unsafe seed grants. Its exact forward
 * repair must be in the migration plan, and all pending steps/phase 2 must pass
 * before this generation can become ready.
 */
export const RELEASED_MIGRATION_NAME_ALIASES = [
  {
    original: "upstream:20261103010000_session_directory_event_metadata.sql",
    originalSha256: "fe07b2ef2b3e18634b024f35f4272bee9100ea39d0383ce33e8c6d969b77a476",
    current: "upstream:20261104021000_session_directory_event_metadata.sql",
    currentSha256: "4fb1337524c8335dd5bd507f60ea896a41a031ace0c7a2da8a8f7dedc1245e01",
  },
  {
    original: "upstream:20261102010000_session_ingest_ledger.sql",
    originalSha256: "21a0be7dc69afa7d3b1d1b27e31fee9778c1a6a0c3b20246e3029fa2f1356fa6",
    current: "upstream:20261104022000_session_ingest_ledger.sql",
    currentSha256: "69f4157ea3a615344aa1b383fc6af7405c1ff9f1568e7c8ab97e58fbc78fe1b5",
    repair: "upstream:20261104040000_ingest_s3_proof_seed.sql",
    repairSha256: "18ea54ed1aa50e5ea44bab01a65177e2fc9c92919ac23b973f820e8af8b75b86",
  },
] as const;

const refuse = (code: string, message: string): never => {
  throw Object.assign(new Error(`96bulls: ${message}`), { code });
};

/** Pure effective view, never a ledger UPDATE/INSERT or a plan-binding hash. */
export function effectiveMigrationLedger(physical: ReadonlyMap<string, string>): Map<string, string> {
  const effective = new Map(physical);
  for (const alias of RELEASED_MIGRATION_NAME_ALIASES) {
    if (!physical.has(alias.original)) continue;
    if (physical.get(alias.original) !== alias.originalSha256)
      refuse("migration_changed", `released migration ${alias.original} has an unrecognized checksum`);
    if (physical.has(alias.current))
      refuse("ledger_alias_collision", `both released and renamed migration ${alias.current} are recorded`);
    effective.delete(alias.original);
    effective.set(alias.current, alias.currentSha256);
  }
  return effective;
}
