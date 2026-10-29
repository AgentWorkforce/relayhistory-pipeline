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
import { execFileSync } from "node:child_process";
import { Resource } from "alchemy";
import * as Provider from "alchemy/Provider";
import * as Effect from "effect/Effect";

export interface ProjectProps {
  organizationId: string;
  name: string;
  region: string;
  plan?: "free" | "pro";
  /** An `op://` reference to the database password item. Resolved with `op read` at create time only; never literal, never in state. */
  dbPassRef: string;
}

export interface ProjectAttrs {
  id: string;
  ref: string;
  name: string;
  region: string;
  status: string;
  restUrl: string;
  /** True only for a project this stack created. Only managed projects are ever deleted. */
  managed: boolean;
  organizationId: string;
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
  // 204 and other empty successes carry no body.
  if (res.status === 204 || res.headers.get("content-length") === "0") return undefined as T;
  const text = await res.text();
  return (text ? JSON.parse(text) : undefined) as T;
}

/** The database password, read from 1Password at the moment of creation; never cached, logged or stored. */
function readDbPass(ref: string): string {
  if (!ref.startsWith("op://")) throw new Error("96bulls: dbPassRef must be an op:// reference, never a literal password");
  const out = execFileSync("op", ["read", "--no-newline", ref], { encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] });
  if (!out) throw new Error("96bulls: op read returned nothing for the database password reference");
  return out;
}

interface ApiProject { id: string; ref?: string; name: string; region: string; status: string; organization_id: string }

const toAttrs = (p: ApiProject, managed: boolean): ProjectAttrs => ({
  id: p.id, ref: p.ref ?? p.id, name: p.name, region: p.region, status: p.status, restUrl: `https://${p.ref ?? p.id}.supabase.co`,
  managed, organizationId: p.organization_id,
});

export const SupabaseProvider = () =>
  Provider.succeed(
    Project,
    Project.Provider.of({
      reconcile: Effect.fn(function* ({ news, output }) {
        if (output?.id) {
          // Tracked by id only; never adopt a project by name (an unrelated project could share it).
          const current = yield* Effect.promise(() => api<ApiProject | undefined>("GET", `/projects/${output.id}`));
          if (!current) throw new Error(`96bulls: Supabase project ${output.id} no longer exists; remove it from the stack or recreate it explicitly`);
          // Region and organisation are immutable on Supabase, and a rename is not applied here:
          // refuse loudly instead of reporting success for settings that were not applied.
          const drift = [
            current.region !== news.region ? `region ${current.region} -> ${news.region}` : "",
            current.organization_id !== news.organizationId ? `organization ${current.organization_id} -> ${news.organizationId}` : "",
            current.name !== news.name ? `name ${current.name} -> ${news.name}` : "",
          ].filter(Boolean);
          if (drift.length) throw new Error(`96bulls: Supabase.Project ${output.id} cannot change ${drift.join(", ")}; create a new project instead`);
          return toAttrs(current, output.managed);
        }
        const created = yield* Effect.promise(() =>
          api<ApiProject>("POST", "/projects", {
            organization_id: news.organizationId, name: news.name, region: news.region, plan: news.plan ?? "free",
            db_pass: readDbPass(news.dbPassRef),
          }),
        );
        return toAttrs(created, true);
      }),
      delete: Effect.fn(function* ({ output }) {
        // Only a project this stack created is ever deleted.
        if (!output?.id || !output.managed) return;
        yield* Effect.promise(() => api<unknown>("DELETE", `/projects/${output.id}`));
      }),
      list: () => Effect.succeed([]),
    }),
  );

/** Merge into a Stack's `providers`: `Layer.mergeAll(AWS.providers(), SupabaseProvider())`. */
