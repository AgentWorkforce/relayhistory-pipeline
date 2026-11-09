-- rds-activation/v1 fragment 60: remove the executor, as the provisioner (ADMIN on it
-- through its creator edge).
--
-- DROP ROLE removes every pg_auth_members row in which the executor is the role, the
-- member or the grantor. Those are: its two edges to the provisioner, its creator edge
-- on each role from 30, and the SET-only edge on hoop from 40. Measured on PostgreSQL
-- 16.11 and 17.11: zero rows remain. The creator edges themselves cannot be revoked
-- by a non-superuser beforehand (their grantor is the bootstrap superuser), and no
-- edge is re-granted under another grantor, because the target graph is empty: no
-- HoopDb role keeps a member or a membership. Fragment 70 proves the result.
-- The executor owns no objects (creating a role records no ownership) and holds no
-- ACL entry, so nothing else blocks the drop.
DO $rds_activation_60$
BEGIN
  IF current_user::text <> current_setting('hoopdb_activation.provisioner')
     OR session_user::text <> current_user::text THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'rds-activation fragment 60 must run as the provisioner with no role set';
  END IF;
  DROP ROLE hoopdb_activation_executor;
END
$rds_activation_60$;
