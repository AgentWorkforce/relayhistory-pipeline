BEGIN;
-- Apply to Cloud Neon. The pipeline runner uses a direct, session-mode connection.
CREATE TABLE public.admin_account_resets (
  user_id uuid PRIMARY KEY REFERENCES public.users(id),
  requested_by text NOT NULL,
  confirmation text,
  scope_orgs text[] NOT NULL,
  legacy_relay_only boolean NOT NULL DEFAULT false,
  phase text NOT NULL DEFAULT 'account' CHECK (phase IN ('account','history','relaycast','complete')),
  history_targets jsonb NOT NULL DEFAULT '[]',
  relay_targets jsonb NOT NULL DEFAULT '[]',
  attempts integer NOT NULL DEFAULT 0,
  error_code text,
  deferred_error text,
  stalled boolean NOT NULL DEFAULT false,
  run_after timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX admin_account_resets_ready ON public.admin_account_resets(run_after)
  WHERE NOT stalled AND phase <> 'complete';

-- A retired registry row stays as the audit record. Its provider ID and owner
-- cannot be reassigned between the Cloud commit and an ambiguous HTTP retry.
CREATE TABLE public.admin_reset_retired_workspaces (
  registry_id text PRIMARY KEY REFERENCES public.relay_workspaces(id),
  provider_id text NOT NULL UNIQUE,
  user_id uuid NOT NULL REFERENCES public.users(id)
);
CREATE FUNCTION public.admin_reset_guard_binding() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF NEW.relay_workspace_id IS NULL THEN RETURN NEW; END IF;
  IF TG_OP='UPDATE' AND NEW.relay_workspace_id IS NOT DISTINCT FROM OLD.relay_workspace_id THEN RETURN NEW; END IF;
  -- Retirement updates this same row, so a transaction with an old
  -- REPEATABLE READ snapshot gets a serialization failure on the locking read.
  PERFORM 1 FROM public.relay_workspaces WHERE id=NEW.relay_workspace_id FOR SHARE;
  IF EXISTS (SELECT FROM public.admin_reset_retired_workspaces WHERE registry_id=NEW.relay_workspace_id) THEN
    RAISE EXCEPTION 'Relay workspace retired by account reset' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END
$$;
CREATE TRIGGER admin_reset_guard_binding BEFORE INSERT OR UPDATE OF relay_workspace_id ON public.workspaces
  FOR EACH ROW EXECUTE FUNCTION public.admin_reset_guard_binding();
CREATE FUNCTION public.admin_reset_guard_registry() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF EXISTS (SELECT FROM public.admin_reset_retired_workspaces WHERE registry_id=OLD.id) THEN
    RAISE EXCEPTION 'Retired Relay workspace identity is immutable' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END
$$;
CREATE TRIGGER admin_reset_guard_registry BEFORE UPDATE OF owner_user_id,relaycast_workspace_id ON public.relay_workspaces
  FOR EACH ROW WHEN (OLD.owner_user_id IS DISTINCT FROM NEW.owner_user_id OR OLD.relaycast_workspace_id IS DISTINCT FROM NEW.relaycast_workspace_id)
  EXECUTE FUNCTION public.admin_reset_guard_registry();
REVOKE ALL ON FUNCTION public.admin_reset_guard_binding(), public.admin_reset_guard_registry() FROM PUBLIC;

-- Fixed SQL only. The dashboard may submit a confirmed request but cannot
-- supply or edit any target, actor credential, verified receipt or phase.
CREATE FUNCTION public.admin_enqueue_account_reset(uuid,text,text) RETURNS SETOF uuid
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
  WITH target AS (
    SELECT u.id,u.primary_email,ARRAY(
      SELECT m.organization_id::text FROM public.organization_memberships m
      JOIN public.organizations o ON o.id=m.organization_id
      WHERE m.user_id=u.id AND m.status='active' AND o.created_by_user_id=u.id
        AND NOT EXISTS (SELECT FROM public.organization_memberships other
          WHERE other.organization_id=m.organization_id AND other.user_id<>u.id)
    ) AS orgs FROM public.users u WHERE u.id=$1 AND u.primary_email=$2
  )
  INSERT INTO public.admin_account_resets AS j(user_id,confirmation,requested_by,scope_orgs,relay_targets)
  SELECT t.id,t.primary_email,$3,t.orgs,COALESCE((
    SELECT jsonb_agg(jsonb_build_object('id',rw.id,'provider_id',rw.relaycast_workspace_id) ORDER BY rw.id)
    FROM public.relay_workspaces rw WHERE rw.owner_user_id=t.id AND NOT EXISTS (
      SELECT FROM public.workspaces w WHERE w.relay_workspace_id=rw.id AND NOT(w.organization_id::text=ANY(t.orgs)))
  ),'[]'::jsonb) FROM target t
  ON CONFLICT (user_id) DO UPDATE SET confirmation=EXCLUDED.confirmation,
    requested_by=CASE WHEN j.stalled THEN EXCLUDED.requested_by ELSE j.requested_by END,
    stalled=false, attempts=CASE WHEN j.stalled THEN 0 ELSE j.attempts END,
    error_code=CASE WHEN j.stalled THEN NULL ELSE j.error_code END,
    run_after=CASE WHEN j.stalled THEN now() ELSE j.run_after END
  WHERE j.phase='account'
  RETURNING user_id;
$$;

CREATE FUNCTION public.admin_enqueue_legacy_relay_cleanup(uuid,text) RETURNS SETOF uuid
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
  WITH old AS (
    SELECT id,updated_at FROM public.users WHERE id=$1 AND primary_email IS NULL
  ), scope AS (
    SELECT m.organization_id FROM public.organization_memberships m JOIN old ON old.id=m.user_id
    JOIN public.organizations o ON o.id=m.organization_id AND o.created_by_user_id=old.id
    WHERE m.status='revoked' AND m.updated_at=old.updated_at AND NOT EXISTS(
      SELECT FROM public.organization_memberships other WHERE other.organization_id=m.organization_id AND other.user_id<>old.id)
  ), targets AS (
    SELECT rw.id,rw.relaycast_workspace_id AS provider_id
    FROM public.relay_workspaces rw JOIN old ON rw.owner_user_id=old.id
    WHERE NOT EXISTS(SELECT FROM public.workspaces w WHERE w.relay_workspace_id=rw.id
      AND w.organization_id NOT IN(SELECT organization_id FROM scope))
  )
  INSERT INTO public.admin_account_resets(user_id,requested_by,scope_orgs,phase,relay_targets,legacy_relay_only)
  SELECT id,$2,ARRAY(SELECT organization_id::text FROM scope),'relaycast',
    COALESCE((SELECT jsonb_agg(to_jsonb(targets) ORDER BY id) FROM targets),'[]'::jsonb),true FROM old
  ON CONFLICT(user_id) DO UPDATE SET user_id=EXCLUDED.user_id RETURNING user_id;
$$;

CREATE FUNCTION public.admin_resume_account_reset(uuid) RETURNS void
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path=pg_catalog AS $$
  UPDATE public.admin_account_resets SET stalled=false,attempts=0,error_code=NULL,deferred_error=NULL,run_after=now()
  WHERE user_id=$1 AND stalled AND phase IN ('history','relaycast');
$$;
REVOKE ALL ON FUNCTION public.admin_enqueue_account_reset(uuid,text,text),
  public.admin_enqueue_legacy_relay_cleanup(uuid,text), public.admin_resume_account_reset(uuid) FROM PUBLIC;
COMMIT;
