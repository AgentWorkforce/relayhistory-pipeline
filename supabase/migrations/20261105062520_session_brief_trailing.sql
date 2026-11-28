-- Slot 20261105062520 (reserved, registry receipt 252413542110740480; migration-registry-v3.md line 366).
-- T144 (under T130) session-brief trigger: durable trailing state on sessions.session_brief_wakeups.
-- Code only: applying it to any database needs Khaliq's separate explicit OK.
--
-- Why. A run claims a wakeup's generation; committed evidence armed while that lease is outstanding (expired or
-- not: finishWakeup fences by lease_id) is TRAILING work. Before this column, finishWakeup restarted the trailing
-- window at now(), so B's own uncovered time (B, then C, during A's lease) was lost and did not survive a failed
-- run and its retry.
--
-- What changes. One nullable column, no data change:
--   trailing_since  the first committed evidence armed while a lease was outstanding. Set by armWakeup when
--                   lease_id IS NOT NULL and it is NULL; cleared only by a claim (claimWakeups: a new generation
--                   boundary); kept by a failure (rescheduleWakeup); on a generation mismatch finishWakeup sets
--                   pending_since := coalesce(trailing_since, pending_since) (a legacy NULL keeps the conservative
--                   bound, never now()).
-- Existing rows get NULL. Readers that do not know the column are unaffected (store.ts names its columns).
ALTER TABLE sessions.session_brief_wakeups ADD COLUMN trailing_since timestamptz;
COMMENT ON COLUMN sessions.session_brief_wakeups.trailing_since IS
  'T144: first committed evidence armed while a lease was outstanding (the trailing work''s own oldest uncovered time); cleared by the next claim.';
