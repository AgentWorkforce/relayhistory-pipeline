/**
 * `Supabase.Project`: a custom Alchemy v2 resource over the Supabase Management
 * API, because Alchemy has no Supabase provider. CODE ONLY until Khaliq creates
 * the provisioner token: the token is read from `SUPABASE_ACCESS_TOKEN` in the
 * process environment at call time (injected by `scripts/sb` via `op run`),
 * never from a file, never written to state (state keeps project refs only).
 *
 * Covers: create a project in the org, read it, delete it. Branches and
 * per-sandbox roles are the provisioner's job (Pippin Tools), not IaC.
 */
import { Resource } from "alchemy";
import * as Provider from "alchemy/Provider";
import * as Effect from "effect/Effect";

export interface ProjectProps {
  organizationId: string;
  name: string;
  region: string;
  plan?: "free" | "pro";
  /** Set by the provisioner at create time and stored ONLY in 1Password, never in state. */
  dbPassRef: string;
}

export interface ProjectAttrs {
  id: string;
  ref: string;
  name: string;
  region: string;
  status: string;
  restUrl: string;
}

export type Project = Resource<"Supabase.Project", ProjectProps, ProjectAttrs>;
export const Project = Resource<Project>("Supabase.Project");

const API = "https://api.supabase.com/v1";

function token(): string {
  const t = process.env.SUPABASE_ACCESS_TOKEN;
  if (!t) throw new Error("96bulls: SUPABASE_ACCESS_TOKEN is not in the environment; run through scripts/sb (op run). Nothing was created.");
  return t;
}

async function api<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${API}${path}`, {
    method,
    headers: { authorization: `Bearer ${token()}`, "content-type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (res.status === 404) return undefined as T;
  if (!res.ok) throw Object.assign(new Error(`supabase management api ${method} ${path}: http ${res.status}`), { code: `http_${res.status}` });
  return (await res.json()) as T;
}

interface ApiProject { id: string; ref?: string; name: string; region: string; status: string; organization_id: string }

const toAttrs = (p: ApiProject): ProjectAttrs => ({
  id: p.id, ref: p.ref ?? p.id, name: p.name, region: p.region, status: p.status, restUrl: `https://${p.ref ?? p.id}.supabase.co`,
});

export const SupabaseProvider = () =>
  Provider.succeed(
    Project,
    Project.Provider.of({
      reconcile: Effect.fn(function* ({ news, output }) {
        // Observe: an existing project with this ref or name in the org wins (adopt-by-name).
        const existing = output?.id
          ? yield* Effect.promise(() => api<ApiProject | undefined>("GET", `/projects/${output.id}`))
          : (yield* Effect.promise(() => api<ApiProject[]>("GET", "/projects"))).find((p) => p.organization_id === news.organizationId && p.name === news.name);
        if (existing) return toAttrs(existing);
        // The database password is resolved by the provisioner from 1Password at call time (op:// reference), never literal.
        const dbPass = process.env.SUPABASE_DB_PASS;
        if (!dbPass) throw new Error("96bulls: SUPABASE_DB_PASS must be injected (op run) to create a project; nothing was created");
        const created = yield* Effect.promise(() =>
          api<ApiProject>("POST", "/projects", { organization_id: news.organizationId, name: news.name, region: news.region, plan: news.plan ?? "free", db_pass: dbPass }),
        );
        return toAttrs(created);
      }),
      delete: Effect.fn(function* ({ output }) {
        if (!output?.id) return;
        yield* Effect.promise(() => api<unknown>("DELETE", `/projects/${output.id}`));
      }),
      list: () => Effect.succeed([]),
    }),
  );

/** Merge into a Stack's `providers`: `Layer.mergeAll(AWS.providers(), SupabaseProvider())`. */
