-- When each deployment first opened the legacy machine-claim window. The 72-hour
-- cap counts from here, so restating the window's dates cannot extend it: rows
-- are written once and never change. Applying this to a real database needs
-- Khaliq's explicit OK; see runbooks/manifest-activation.md.
create table sessions.claimless_window_activations (
  deployment_id text primary key,
  first_activation timestamptz not null,
  recorded_at timestamptz not null default now()
);

create function sessions.claimless_window_activations_append_only()
returns trigger language plpgsql as $$
begin
  raise exception 'claimless_window_activations is append-only';
end;
$$;

create trigger claimless_window_activations_append_only
  before update or delete on sessions.claimless_window_activations
  for each row execute function sessions.claimless_window_activations_append_only();

create trigger claimless_window_activations_no_truncate
  before truncate on sessions.claimless_window_activations
  for each statement execute function sessions.claimless_window_activations_append_only();

alter table sessions.claimless_window_activations enable row level security;
revoke all on sessions.claimless_window_activations from public, anon, authenticated;
grant select, insert on sessions.claimless_window_activations to service_role;
