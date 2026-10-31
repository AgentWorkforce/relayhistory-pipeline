-- The legacy machine-claim window is read-only, so claimless_uses records reads
-- alone. A forward change: 20261031000100 is checksum-locked. Applying this to a
-- real database needs Khaliq's explicit OK; see runbooks/manifest-activation.md.
delete from sessions.claimless_uses where surface = 'write';
alter table sessions.claimless_uses drop constraint claimless_uses_pkey;
alter table sessions.claimless_uses drop column surface;
alter table sessions.claimless_uses
  add primary key (day, org_id, caller, client_version);
