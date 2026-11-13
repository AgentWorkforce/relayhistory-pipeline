// Old Cloud/Neon account access. Every profile, identity, credential and
// membership mutation is keyed to the confirmed user. Uploaded content is
// handled exclusively by the History-owned per-user reset functions.
export const scrubAccount = `WITH revoked_memberships AS (
    UPDATE organization_memberships SET status='revoked', updated_at=now()
    WHERE user_id=$1 AND status<>'revoked'
  ), revoked_tokens AS (
    UPDATE api_token_sessions SET revoked_at=now(),
      revoked_reason=COALESCE(revoked_reason,'admin_user_reset'),updated_at=now()
    WHERE user_id=$1 AND revoked_at IS NULL
  ), removed_identities AS (
    DELETE FROM auth_identities WHERE user_id=$1
  )
  UPDATE users SET primary_email=NULL,name=NULL,avatar_url=NULL,
    last_organization_id=NULL,last_workspace_id=NULL,cloud_agent_spawn_quota_override=NULL,updated_at=now()
  WHERE id=$1 AND primary_email=$2 RETURNING id`;

export const privateOrganizations = `SELECT m.organization_id::text AS org_id
  FROM organization_memberships m JOIN organizations o ON o.id=m.organization_id
  WHERE m.user_id=$1 AND m.status='active' AND o.created_by_user_id=$1
    AND m.organization_id::text=ANY($2::text[])
    AND NOT EXISTS (SELECT 1 FROM organization_memberships other
      WHERE other.organization_id=m.organization_id AND other.user_id<>m.user_id)
  ORDER BY m.organization_id`;

export const relayTargets = `SELECT rw.id,rw.relaycast_workspace_id AS provider_id
  FROM relay_workspaces rw WHERE rw.owner_user_id=$1 AND NOT EXISTS (
    SELECT 1 FROM workspaces w WHERE w.relay_workspace_id=rw.id
    AND NOT (w.organization_id::text=ANY($2::text[])))
  ORDER BY rw.id`;
