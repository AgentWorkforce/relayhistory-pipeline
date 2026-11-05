/** Create-only named control-state prerequisite; never deploys the ingest app.
 * Official exported Alchemy resources, retained snapshot, RAM state/credentials.
 */
import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import Api, {
  STATE_STORE_VERSION,
} from "@alchemy.run/cloudflare/StateStore/Api";
import {
  TokenValue,
  AuthToken,
  StateStoreName,
  stateStoreSecretName,
  AuthTokenSecretName,
  EncryptionKeySecretName,
} from "@alchemy.run/cloudflare/StateStore/Token";
import { RandomProvider } from "alchemy/Random";
import { inMemoryState } from "alchemy/State/InMemoryState";
import { makeHttpStateStore } from "alchemy/State/HttpStateStore";
import type { StateService } from "alchemy/State/State";
import { encodeState } from "alchemy/State/StateEncoding";
import * as Stacks from "alchemy/Alchemist/routes/stack";
import { routeCacheLayer, StackModuleLoader } from "alchemy/Alchemist/Session";
import { AlchemyContext } from "alchemy/AlchemyContext";
import { ProfileStore } from "alchemy/Auth/Profile";
import { CredentialsStore } from "alchemy/Auth/Credentials";
import { PlatformServices } from "alchemy/Util/PlatformServices";
import { ArtifactStore, createArtifactStore } from "alchemy/Artifacts";
import { layerNonInteractive } from "alchemy/Interaction";
import { formatPlanLines } from "alchemy/Cli/LoggingCli";
import * as Output from "alchemy/Output";
import * as Redacted from "effect/Redacted";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ConfigProvider from "effect/ConfigProvider";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as References from "effect/References";
import * as Schedule from "effect/Schedule";
import { createHash } from "node:crypto";
import { hostname } from "node:os";
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import {
  APPROVER,
  APPROVAL_MAX_AGE_DAYS,
  parseCalendarDate,
  assertApprovalBinding,
  inputsDigest,
  findPlaceholders,
} from "../stacks/aws/deploy-guard.ts";
import { sanitizePlan } from "../src/cli/plan-provenance.ts";
export const STATE_SCRIPT = "alchemy-state-store-96bulls";
export const PROTOCOL = "private-state-bootstrap-v1";
export const EXECUTION = "ssh-sf-frame-state-bootstrap-only";
export const SECRET_NAMES = [
  stateStoreSecretName(AuthTokenSecretName, STATE_SCRIPT),
  stateStoreSecretName(EncryptionKeySecretName, STATE_SCRIPT),
] as const;
const hash = (text: string) => createHash("sha256").update(text).digest("hex");
const obj = (v: unknown): v is Record<string, any> =>
  v !== null && typeof v === "object" && !Array.isArray(v);
const hex32 = (v: unknown): v is string =>
  typeof v === "string" && /^[0-9a-f]{32}$/u.test(v) && !/^0+$/u.test(v);
function no(code: string): never {
  throw new BootstrapRefusal(code);
}
export class BootstrapRefusal extends Error {
  constructor(readonly code: string) {
    super("state bootstrap refused: " + code);
  }
}
export interface BootstrapSpec {
  accountId: string;
  secretsStoreId: string;
  onePasswordVaultId: string;
  approval?: {
    decision: string;
    by: string;
    date: string;
    quote: string;
    execution: string;
    capture: any;
  };
}
export interface BootstrapSource {
  commit: string;
  host: string;
  clean: boolean;
  tools: { bun: string; node: string; alchemy: string; cloudflare: string };
}
export interface Plans {
  planText: string;
  detailedText: string;
}
export interface Admission {
  accountId: string;
  storeId: string;
  subdomain: string;
  workerAbsent: true;
  secretsAbsent: true;
  namespaceAbsent: true;
}
export interface Receipt {
  stored: true;
  vaultId: string;
  itemId: string;
}
export interface CredentialEnvelope {
  protocol: typeof PROTOCOL;
  status: "created" | "partial";
  generation: string;
  accountId: string;
  url: string;
  authToken: string | null;
  encodedBootstrapState: unknown;
}
export type CredentialSink = (value: CredentialEnvelope) => Promise<Receipt>;

/** A dedicated item is reserved before any Cloudflare mutation. Only JSON stdin
 * carries secrets; neither CLI arguments, files nor returned receipts contain them.
 * The injectable port is for offline tests, never a shell command string. */
export type OnePasswordPort = (
  args: string[],
  input?: string,
) => Promise<unknown>;
const runOnePassword: OnePasswordPort = async (args, input) => {
  const result = spawnSync("op", args, {
    input,
    encoding: "utf8",
    timeout: 30000,
    maxBuffer: 16 * 1024 * 1024,
    stdio: ["pipe", "pipe", "pipe"],
  });
  if (result.status !== 0 || result.error) no("one_password_operation");
  try {
    return JSON.parse(result.stdout);
  } catch {
    return no("one_password_response");
  }
};
export async function prepareOnePasswordSink(
  spec: BootstrapSpec,
  generation: string,
  port: OnePasswordPort = runOnePassword,
): Promise<{ itemId: string; sink: CredentialSink }> {
  bootstrapSpec(spec);
  if (!/^[0-9a-f]{64}$/u.test(generation)) no("sink_generation");
  const title = STATE_SCRIPT + " bootstrap " + generation;
  const template = {
    title,
    category: "SECURE_NOTE",
    vault: { id: spec.onePasswordVaultId },
    fields: [
      {
        id: "bootstrap_recovery",
        label: "bootstrap_recovery",
        type: "CONCEALED",
        value: "pending",
      },
    ],
  };
  const validate = (v: unknown): Record<string, any> => {
    if (
      !obj(v) ||
      typeof v.id !== "string" ||
      !/^[a-z0-9]{26}$/u.test(v.id) ||
      v.vault?.id !== spec.onePasswordVaultId ||
      v.title !== title ||
      v.category !== "SECURE_NOTE" ||
      !Array.isArray(v.fields)
    )
      no("one_password_identity");
    return v;
  };
  const created = validate(
    await port(
      [
        "item",
        "create",
        "--vault",
        spec.onePasswordVaultId,
        "--format=json",
        "-",
      ],
      JSON.stringify(template),
    ),
  );
  const itemId = created.id;
  // Verify the dedicated slot is readable and editable before provisioning.
  const pending = validate(
    await port([
      "item",
      "get",
      itemId,
      "--vault",
      spec.onePasswordVaultId,
      "--format=json",
      "--reveal",
    ]),
  );
  if (pending.id !== itemId) no("one_password_identity");
  const ready = validate(
    await port(
      [
        "item",
        "edit",
        itemId,
        "--vault",
        spec.onePasswordVaultId,
        "--format=json",
      ],
      JSON.stringify(pending),
    ),
  );
  if (ready.id !== itemId) no("one_password_identity");
  return {
    itemId,
    sink: async (envelope) => {
      if (
        envelope.protocol !== PROTOCOL ||
        envelope.accountId !== spec.accountId ||
        envelope.generation !== generation
      )
        no("sink_envelope_identity");
      const value = JSON.stringify(envelope);
      if (Buffer.byteLength(value) > 8 * 1024 * 1024) no("sink_envelope_bound");
      const current = validate(
        await port([
          "item",
          "get",
          itemId,
          "--vault",
          spec.onePasswordVaultId,
          "--format=json",
          "--reveal",
        ]),
      );
      if (current.id !== itemId) no("one_password_identity");
      const next = {
        ...current,
        fields: [
          {
            id: "bootstrap_recovery",
            label: "bootstrap_recovery",
            type: "CONCEALED",
            value,
          },
        ],
      };
      const edited = validate(
        await port(
          [
            "item",
            "edit",
            itemId,
            "--vault",
            spec.onePasswordVaultId,
            "--format=json",
          ],
          JSON.stringify(next),
        ),
      );
      if (edited.id !== itemId) no("one_password_identity");
      const readback = validate(
        await port([
          "item",
          "get",
          itemId,
          "--vault",
          spec.onePasswordVaultId,
          "--format=json",
          "--reveal",
        ]),
      );
      if (
        readback.id !== itemId ||
        readback.fields.filter(
          (f: any) =>
            f.id === "bootstrap_recovery" &&
            f.type === "CONCEALED" &&
            f.value === value,
        ).length !== 1
      )
        no("one_password_readback");
      return { stored: true, vaultId: spec.onePasswordVaultId, itemId };
    },
  };
}

export const FILE_PREFIX = "docs/plans/cf-state-bootstrap-96bulls";
export function bootstrapSpec(value: unknown): BootstrapSpec {
  if (
    !obj(value) ||
    Object.keys(value).some(
      (key) =>
        ![
          "accountId",
          "secretsStoreId",
          "onePasswordVaultId",
          "approval",
        ].includes(key),
    ) ||
    !hex32(value.accountId) ||
    !hex32(value.secretsStoreId) ||
    typeof value.onePasswordVaultId !== "string" ||
    !/^[a-z0-9]{26}$/u.test(value.onePasswordVaultId) ||
    findPlaceholders(value).length
  )
    no("inputs");
  return value as BootstrapSpec;
}
export function safeBootstrapPlans(plans: Plans, token: string): Plans {
  const clean = (s: string) => {
    const v = sanitizePlan(s.replace(/\x1b\[[0-9;]*m/gu, ""), []).trim() + "\n";
    if (
      Buffer.byteLength(v) > 16 * 1024 * 1024 ||
      (token.length >= 8 && v.includes(token)) ||
      /secret_text|AKIA[0-9A-Z]{16}|-----BEGIN [A-Z ]*PRIVATE KEY|postgres(?:ql)?:\/\/[^\s"]*:[^\s"@]*@/u.test(
        v,
      )
    )
      no("unsafe_plan");
    return v;
  };
  return {
    planText: clean(plans.planText),
    detailedText: clean(plans.detailedText),
  };
}
const expectedTypes: Record<string, string> = {
  StateStoreAuthTokenValue: "Alchemy.Random",
  Api: "Cloudflare.Worker",
  StateStoreSecrets: "Cloudflare.SecretsStore",
  AlchemyStateStoreToken: "Cloudflare.SecretsStore.Secret",
  StateStoreEncryptionKeyValue: "Alchemy.Random",
  StateStoreEncryptionKey: "Cloudflare.SecretsStore.Secret",
};
export function assertBootstrapSnapshot(
  snapshot: any,
  spec: BootstrapSpec,
): void {
  const resources = Object.values(snapshot.native.resources ?? {}) as any[],
    keys = new Set<string>();
  if (
    resources.length !== 6 ||
    Object.keys(snapshot.native.deletions ?? {}).length ||
    Object.keys(snapshot.native.actions ?? {}).length ||
    Object.keys(snapshot.native.actionDeletions ?? {}).length
  )
    no("resource_inventory");
  for (const row of resources) {
    const id = row.resource.LogicalId;
    if (keys.has(id) || row.resource.Type !== expectedTypes[id])
      no("resource_inventory");
    keys.add(id);
    if (id === "StateStoreSecrets") {
      if (
        row.action !== "adopted" ||
        row.state?.attr?.storeId !== spec.secretsStoreId ||
        row.state?.attr?.accountId !== spec.accountId
      )
        no("shared_store_identity");
    } else if (row.action !== "create") no("create_only");
    if (id === "Api") {
      if (
        row.props.name !== STATE_SCRIPT ||
        row.props.workersDev !== true ||
        row.props.routes !== undefined ||
        row.props.domain !== undefined ||
        row.props.crons !== undefined
      )
        no("control_worker_identity");
      const bindings = [...row.bindings]
        .map((b: any) => [b.sid, b.action])
        .sort();
      if (
        JSON.stringify(bindings) !==
        JSON.stringify([
          ["AlchemyStateStoreToken", "create"],
          ["StateStoreEncryptionKey", "create"],
          ["Store", "create"],
        ])
      )
        no("control_bindings");
    }
    if (
      (id === "AlchemyStateStoreToken" && row.props.name !== SECRET_NAMES[0]) ||
      (id === "StateStoreEncryptionKey" && row.props.name !== SECRET_NAMES[1])
    )
      no("secret_identity");
    if (
      row.resource.Type === "Cloudflare.SecretsStore.Secret" &&
      typeof row.props.value === "string"
    )
      no("literal_secret");
  }
  if (
    [...keys].sort().join(",") !== Object.keys(expectedTypes).sort().join(",")
  )
    no("resource_inventory");
  const s = snapshot.summary;
  if (
    s.create !== 5 ||
    s.adopted !== 1 ||
    s.update ||
    s.replace ||
    s.delete ||
    s.orphaned ||
    s.noop
  )
    no("create_only");
}
export function captureBootstrap(
  source: BootstrapSource,
  spec: BootstrapSpec,
  admission: Admission,
  plans: Plans,
) {
  const body = {
    source: { commit: source.commit, host: source.host, clean: true },
    target: {
      accountId: spec.accountId,
      secretsStoreId: spec.secretsStoreId,
      stateScript: STATE_SCRIPT,
      stateVersion: STATE_STORE_VERSION,
      onePasswordVaultId: spec.onePasswordVaultId,
    },
    inputs: { sha256: inputsDigest(JSON.stringify(spec)) },
    outputs: {
      plan: { sha256: hash(plans.planText) },
      detailed: { sha256: hash(plans.detailedText) },
    },
    tools: source.tools,
    admission,
    execution: {
      state: "memory-only",
      credentials: "memory-only",
      edgePreview: false,
      ingestApply: false,
    },
  };
  return {
    schema: 1,
    kind: PROTOCOL,
    stage: STATE_SCRIPT,
    generatedAt: new Date().toISOString(),
    generation: hash(JSON.stringify([PROTOCOL, body])),
    ...body,
  };
}
export function verifyBootstrap(
  source: BootstrapSource,
  spec: BootstrapSpec,
  admission: Admission,
  plans: Plans,
  provenanceText: string,
  expectedDigest: string,
) {
  if (
    !/^[0-9a-f]{64}$/u.test(expectedDigest) ||
    hash(provenanceText) !== expectedDigest
  )
    no("capture_digest");
  let captured: any;
  try {
    captured = JSON.parse(provenanceText);
  } catch {
    no("capture_json");
  }
  const fresh = captureBootstrap(source, spec, admission, plans);
  const bound = (v: any) =>
    inputsDigest(JSON.stringify({ bound: { ...v, generatedAt: null } }));
  if (!obj(captured) || bound(captured) !== bound(fresh))
    no("capture_generation");
  const a = spec.approval,
    day = parseCalendarDate(a?.date),
    today = new Date();
  today.setUTCHours(0, 0, 0, 0);
  if (
    !a ||
    a.decision !== "approved" ||
    a.by !== APPROVER ||
    a.execution !== EXECUTION ||
    typeof a.quote !== "string" ||
    a.quote.trim().length < 8 ||
    !day ||
    day.getTime() > today.getTime() ||
    today.getTime() - day.getTime() > APPROVAL_MAX_AGE_DAYS * 86400000
  )
    no("explicit_bootstrap_approval");
  try {
    assertApprovalBinding(a.capture, {
      provenanceText,
      planText: plans.planText,
      inputsText: JSON.stringify(spec),
    });
  } catch {
    no("approval_binding");
  }
  return captured as ReturnType<typeof captureBootstrap>;
}
async function json(response: Response) {
  const reader = response.body?.getReader();
  if (!reader) no("metadata_body");
  const controller = new AbortController(),
    timer = setTimeout(() => controller.abort(), 20000);
  let abort: () => void = () => {};
  const deadline = new Promise<never>((_, reject) => {
    abort = () => {
      reject(new BootstrapRefusal("metadata_deadline"));
      void reader!.cancel().catch(() => {});
    };
  });
  controller.signal.addEventListener("abort", abort, { once: true });
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const v = await Promise.race([reader.read(), deadline]);
      if (v.done) break;
      size += v.value.byteLength;
      if (size > 2 * 1024 * 1024) no("metadata_bound");
      chunks.push(v.value);
    }
    const all = new Uint8Array(size);
    let at = 0;
    for (const c of chunks) {
      all.set(c, at);
      at += c.length;
    }
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(all));
  } finally {
    clearTimeout(timer);
    controller.signal.removeEventListener("abort", abort);
    void reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
export function bootstrapMetadata(
  spec: BootstrapSpec,
  token: string,
  fetcher: typeof fetch,
) {
  const account =
    "https://api.cloudflare.com/client/v4/accounts/" + spec.accountId + "/";
  const get = async (path: string) => {
    const r = await fetcher(account + path, {
      headers: { authorization: "Bearer " + token },
      redirect: "error",
      signal: AbortSignal.timeout(20000),
    });
    const body: any = await json(r);
    if (!r.ok || body.success !== true) no("metadata_readback");
    return body;
  };
  const list = async (path: string) => {
    const values: any[] = [];
    let total: number | undefined;
    for (let page = 1; page <= 100; page++) {
      const b = await get(path + "?per_page=100&page=" + page),
        p = b.result_info;
      if (
        !Array.isArray(b.result) ||
        !obj(p) ||
        p.page !== page ||
        p.per_page !== 100 ||
        !Number.isSafeInteger(p.total_count) ||
        p.total_count < 0 ||
        !Number.isSafeInteger(p.count) ||
        p.count !== b.result.length ||
        p.count > 100
      )
        no("metadata_pagination");
      const lastPage = Math.max(1, Math.ceil(p.total_count / p.per_page));
      if (
        lastPage > 100 ||
        page > lastPage ||
        p.count !==
          Math.min(100, Math.max(0, p.total_count - (page - 1) * 100)) ||
        (p.total_pages !== undefined && p.total_pages !== lastPage)
      )
        no("metadata_pagination");
      if (total !== undefined && total !== p.total_count)
        no("metadata_changed");
      total = p.total_count;
      values.push(...b.result);
      if (page === lastPage) {
        if (values.length !== total) no("metadata_pagination");
        return values;
      }
    }
    return no("metadata_pagination");
  };
  const absent = async () => {
    const r = await fetcher(
      account + "workers/scripts/" + STATE_SCRIPT + "/settings",
      {
        headers: { authorization: "Bearer " + token },
        redirect: "error",
        signal: AbortSignal.timeout(20000),
      },
    );
    const b: any = await json(r);
    if (
      r.status !== 404 ||
      b.success !== false ||
      !Array.isArray(b.errors) ||
      !b.errors.some((e: any) => e.code === 10007)
    )
      no("state_worker_exists_or_unproven");
  };
  const secretAbsent = async (name?: string) => {
    const secrets = await list(
      "secrets_store/stores/" + spec.secretsStoreId + "/secrets",
    );
    if (secrets.some((s) => typeof s.name !== "string")) no("secret_metadata");
    if (
      secrets.some((s) =>
        name ? s.name === name : SECRET_NAMES.includes(s.name),
      )
    )
      no("state_secret_exists");
  };
  const admission = async (): Promise<Admission> => {
    await absent();
    const stores = await list("secrets_store/stores");
    if (stores.length !== 1 || stores[0].id !== spec.secretsStoreId)
      no("shared_store_identity");
    await secretAbsent();
    const namespaces = await list("workers/durable_objects/namespaces");
    if (
      namespaces.some(
        (n) => typeof n.script !== "string" || typeof n.name !== "string",
      )
    )
      no("namespace_metadata");
    if (
      namespaces.some(
        (n) => n.script === STATE_SCRIPT || n.name.includes(STATE_SCRIPT),
      )
    )
      no("state_namespace_exists");
    const worker = await get("workers/subdomain"),
      subdomain = worker.result?.subdomain;
    if (
      typeof subdomain !== "string" ||
      !/^[a-z0-9][a-z0-9-]{0,62}$/u.test(subdomain)
    )
      no("worker_subdomain");
    return {
      accountId: spec.accountId,
      storeId: spec.secretsStoreId,
      subdomain,
      workerAbsent: true,
      secretsAbsent: true,
      namespaceAbsent: true,
    };
  };
  return { admission, absent, secretAbsent };
}
export function bootstrapFetch(
  spec: BootstrapSpec,
  token: string,
  mode: "plan" | "create",
  subdomain: string,
  base: typeof fetch,
): typeof fetch {
  const prefix = "/client/v4/accounts/" + spec.accountId + "/",
    stateOrigin = "https://" + STATE_SCRIPT + "." + subdomain + ".workers.dev",
    metadata = bootstrapMetadata(spec, token, base);
  let uploaded = false;
  const names = new Set<string>();
  const guarded = async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(
        typeof input === "string" || input instanceof URL
          ? String(input)
          : input.url,
      ),
      method = (
        init?.method ?? (input instanceof Request ? input.method : "GET")
      ).toUpperCase();
    if (url.username || url.password || url.hash || url.protocol !== "https:")
      no("network_scope");
    if (url.origin === stateOrigin) {
      if (mode !== "create" || !uploaded || url.search) no("state_scope");
      const root =
        "/state/stacks/CloudflareStateStore/stages/" +
        STATE_SCRIPT +
        "/resources/";
      const key = url.pathname.startsWith(root)
        ? url.pathname.slice(root.length)
        : "";
      const allowedRead =
        method === "GET" &&
        (url.pathname === "/version" || url.pathname === "/state/stacks");
      if (
        !allowedRead &&
        !(
          (method === "GET" || method === "PUT") &&
          Object.hasOwn(expectedTypes, key)
        )
      )
        no("state_scope");
      return base(input, {
        ...init,
        redirect: "error",
        signal: AbortSignal.timeout(20000),
      });
    }
    if (
      url.origin !== "https://api.cloudflare.com" ||
      !url.pathname.startsWith(prefix) ||
      /preview|%2f|%5c|%2e/iu.test(url.pathname)
    )
      no("network_scope");
    if (method !== "GET") {
      if (mode !== "create") no("plan_write");
      const path = url.pathname.slice(prefix.length);
      if (path === "workers/scripts/" + STATE_SCRIPT && method === "PUT") {
        if (uploaded) no("worker_second_upload");
        await metadata.absent();
        const response = await base(input, { ...init, redirect: "error" });
        if (!response.ok) {
          void response.body?.cancel().catch(() => {});
          no("worker_create_failed");
        }
        uploaded = true;
        return response;
      } else if (
        path === "workers/scripts/" + STATE_SCRIPT + "/settings" &&
        method === "PATCH"
      ) {
        if (!uploaded) no("worker_not_created_here");
      } else if (
        path === "workers/scripts/" + STATE_SCRIPT + "/subdomain" &&
        method === "POST"
      ) {
        if (!uploaded) no("worker_not_created_here");
        const v = await new Response(init?.body).json();
        if (!obj(v) || v.enabled !== true || v.previews_enabled !== true)
          no("worker_url_properties");
      } else if (
        path === "secrets_store/stores/" + spec.secretsStoreId + "/secrets" &&
        method === "POST"
      ) {
        const body: any = await new Response(init?.body).json(),
          v = Array.isArray(body) && body.length === 1 ? body[0] : null;
        if (
          !obj(v) ||
          !SECRET_NAMES.includes(v.name) ||
          names.has(v.name) ||
          !Array.isArray(v.scopes) ||
          v.scopes.length !== 1 ||
          v.scopes[0] !== "workers" ||
          typeof v.value !== "string" ||
          !/^[0-9a-f]{64}$/u.test(v.value)
        )
          no("secret_create_scope");
        await metadata.secretAbsent(v.name);
        names.add(v.name);
        const response = await base(input, { ...init, redirect: "error" });
        if (!response.ok) {
          void response.body?.cancel().catch(() => {});
          no("secret_create_failed");
        }
        return response;
      } else no("unplanned_write");
    }
    return base(input, { ...init, redirect: "error" });
  };
  return Object.assign(guarded, {
    preconnect: () => no("preconnect_forbidden"),
  }) as typeof fetch;
}
function runtime(
  spec: BootstrapSpec,
  token: string,
  directory: string,
  fetcher: typeof fetch,
  definition: any,
) {
  const deny = () =>
    Effect.die(new BootstrapRefusal("profile_or_credential_io"));
  const profiles: any = {
    current: Effect.succeed({
      name: "bootstrap-memory",
      source: "configuration",
    }),
    readManifest: deny(),
    getProfile: deny,
    ensureProfile: deny,
    createProfile: deny,
    renameProfile: deny,
    setProviderConfig: deny,
    deleteProviderConfig: deny,
    deleteProfile: deny,
    loadProviderConfig: deny,
  };
  const credentials: any = {
    read: deny,
    write: deny,
    delete: deny,
    deleteProfile: deny,
  };
  credentials.scoped = () => credentials;
  return Layer.mergeAll(
    PlatformServices,
    FetchHttpClient.layer,
    Layer.succeed(FetchHttpClient.Fetch, fetcher),
    ConfigProvider.layer(
      ConfigProvider.fromEnvRecord({
        CI: "true",
        CLOUDFLARE_ACCOUNT_ID: spec.accountId,
        CLOUDFLARE_API_TOKEN: token,
      }),
    ),
    Layer.succeed(AlchemyContext, {
      dotAlchemy: directory,
      dev: false,
      adopt: false,
      updateStateStore: false,
    }),
    Layer.succeed(ProfileStore, profiles),
    Layer.succeed(CredentialsStore, credentials),
    Layer.succeed(ArtifactStore, createArtifactStore()),
    routeCacheLayer,
    layerNonInteractive(),
    Layer.succeed(StackModuleLoader, {
      import: async (url: string) => {
        if (url !== pathToFileURL(import.meta.filename).href) no("entrypoint");
        return { default: definition };
      },
    }),
  );
}
export function checkpointThenHoist<R, E>(
  spec: BootstrapSpec,
  envelope: CredentialEnvelope,
  records: Record<string, any>,
  sink: CredentialSink,
  remoteFactory: () => Effect.Effect<StateService, E, R>,
) {
  return Effect.gen(function* () {
    const receipt = yield* Effect.promise(() => sink(envelope));
    if (
      receipt?.stored !== true ||
      receipt.vaultId !== spec.onePasswordVaultId ||
      typeof receipt.itemId !== "string" ||
      !receipt.itemId
    )
      no("credential_sink_ack");
    const remote = yield* remoteFactory();
    const retry = {
      while: (e: any) =>
        e.http?.status === undefined ||
        e.http.status === 401 ||
        e.http.status === 404 ||
        e.http.status >= 500,
      schedule: Schedule.max([Schedule.fixed(500), Schedule.recurs(60)]),
    };
    const version = yield* remote.getVersion().pipe(Effect.retry(retry));
    if (version !== STATE_STORE_VERSION) no("state_version");
    const existing = yield* remote.listStacks().pipe(Effect.retry(retry));
    if (existing.length) no("remote_state_not_empty");
    if (
      !records ||
      Object.keys(records).sort().join(",") !==
        Object.keys(expectedTypes).sort().join(",")
    )
      no("bootstrap_state_inventory");
    for (const [fqn, value] of Object.entries(records)) {
      yield* remote
        .set({
          stack: "CloudflareStateStore",
          stage: STATE_SCRIPT,
          fqn,
          value: value as any,
        })
        .pipe(Effect.retry(retry));
      const read = yield* remote.get({
        stack: "CloudflareStateStore",
        stage: STATE_SCRIPT,
        fqn,
      });
      if (
        inputsDigest(JSON.stringify({ row: encodeState(read) })) !==
        inputsDigest(JSON.stringify({ row: encodeState(value) }))
      )
        no("bootstrap_state_readback");
    }
    return receipt;
  });
}
export async function runStateBootstrap(options: {
  mode: "plan" | "create";
  spec: BootstrapSpec;
  token: string;
  source: BootstrapSource;
  directory: string;
  fetcher?: typeof fetch;
  reviewed?: { plans: Plans; provenanceText: string; provenanceSha256: string };
  credentialSink?: CredentialSink;
  recheckSource?: () => void;
}) {
  const { mode, spec, token, source, directory } = options;
  bootstrapSpec(spec);
  if (
    !source.clean ||
    !/^[0-9a-f]{40}$/u.test(source.commit) ||
    !source.host ||
    token.length < 16
  )
    no("source_or_credentials");
  if (
    mode === "create" &&
    (!options.reviewed ||
      typeof options.credentialSink !== "function" ||
      typeof options.recheckSource !== "function")
  )
    no("review_and_memory_sink_required");
  if (
    process.env.CI !== "true" ||
    process.env.CLOUDFLARE_ACCOUNT_ID !== spec.accountId ||
    process.env.CLOUDFLARE_API_TOKEN !== token
  )
    no("environment_credential_binding");
  const base = options.fetcher ?? fetch,
    metadata = bootstrapMetadata(spec, token, base),
    admission = await metadata.admission();
  let reviewed: ReturnType<typeof captureBootstrap> | undefined;
  if (options.reviewed) {
    safeBootstrapPlans(options.reviewed.plans, token);
    reviewed = verifyBootstrap(
      source,
      spec,
      admission,
      options.reviewed.plans,
      options.reviewed.provenanceText,
      options.reviewed.provenanceSha256,
    );
  }
  const states: Record<string, any> = {},
    outputs: Record<string, any> = {};
  const definition = Alchemy.Stack(
    "CloudflareStateStore",
    {
      providers: Layer.mergeAll(Cloudflare.providers(), RandomProvider()),
      state: inMemoryState(states, outputs),
    },
    Effect.gen(function* () {
      const value = yield* TokenValue;
      const api = yield* Api;
      yield* AuthToken;
      return {
        url: api.url.as<string>(),
        authToken: value.text.pipe(Output.map(Redacted.value)),
      };
    }).pipe(Effect.provideService(StateStoreName, STATE_SCRIPT)),
  );
  const guarded = bootstrapFetch(spec, token, mode, admission.subdomain, base);
  const expectedUrl =
    "https://" + STATE_SCRIPT + "." + admission.subdomain + ".workers.dev";
  const recovery = (): CredentialEnvelope => {
    const records = states.CloudflareStateStore?.[STATE_SCRIPT] ?? {},
      value = records.StateStoreAuthTokenValue?.attr?.text;
    return {
      protocol: PROTOCOL,
      status: "partial",
      generation: reviewed?.generation ?? "",
      accountId: spec.accountId,
      url: expectedUrl,
      authToken: value ? (Redacted.value(value) as string) : null,
      encodedBootstrapState: encodeState(records),
    };
  };
  process.env.ALCHEMY_HOME = resolve(directory, "auth");
  const empty = resolve(directory, "empty-config.txt");
  mkdirSync(directory, { recursive: true });
  writeFileSync(empty, "", { mode: 0o600 });
  const program = Effect.gen(function* () {
    const snapshot = yield* Stacks.plan({
      target: {
        entrypoint: import.meta.filename,
        stage: STATE_SCRIPT,
        envFile: empty,
      },
      operation: "deploy",
      updateStateStore: false,
    });
    assertBootstrapSnapshot(snapshot, spec);
    const plans = safeBootstrapPlans(
      {
        planText: formatPlanLines(snapshot.native).join("\n"),
        detailedText: formatPlanLines(snapshot.native, { detailed: true }).join(
          "\n",
        ),
      },
      token,
    );
    options.recheckSource?.();
    const capture = captureBootstrap(source, spec, admission, plans);
    if (mode === "plan") return { operation: "captured-only", capture, plans };
    if (!reviewed || capture.generation !== reviewed.generation)
      no("fresh_plan_drift");
    options.recheckSource?.();
    yield* Effect.promise(() => metadata.admission());
    const result: any = yield* Stacks.apply(snapshot);
    if (
      result.url?.replace(/\/$/u, "") !== expectedUrl ||
      typeof result.authToken !== "string" ||
      !/^[0-9a-f]{64}$/u.test(result.authToken)
    )
      no("credential_output");
    const envelope = {
      ...recovery(),
      status: "created" as const,
      authToken: result.authToken,
    };
    const receipt = yield* checkpointThenHoist(
      spec,
      envelope,
      states.CloudflareStateStore?.[STATE_SCRIPT],
      options.credentialSink!,
      () =>
        makeHttpStateStore({
          url: expectedUrl,
          authToken: result.authToken,
          id: "cloudflare-http",
        }),
    );
    return {
      operation: "created-control-store",
      generation: capture.generation,
      url: expectedUrl,
      credentialItemId: receipt.itemId,
      controlResources: 6,
      ingestApplied: false,
      edgePreview: false,
    };
  });
  try {
    return await Effect.runPromise(
      program.pipe(
        Effect.provide(runtime(spec, token, directory, guarded, definition)),
        Effect.provideService(References.MinimumLogLevel, "None"),
        Effect.scoped,
      ),
    );
  } catch (error) {
    if (
      mode === "create" &&
      states.CloudflareStateStore?.[STATE_SCRIPT]?.StateStoreAuthTokenValue
        ?.attr?.text
    ) {
      try {
        await options.credentialSink!(recovery());
      } catch {
        no("partial_create_recovery_sink_failed");
      }
    }
    throw error;
  }
}
const PREFIX = "docs/plans/cf-state-bootstrap-96bulls";
export async function captureStateBootstrap(
  argv = process.argv.slice(2),
  onePasswordPort: OnePasswordPort = runOnePassword,
) {
  const mode = argv[0];
  if (
    argv[1] !== "--expected-sha" ||
    !(
      (mode === "plan" && argv.length === 4 && argv[3] === "--capture") ||
      (mode === "create" &&
        argv.length === 5 &&
        argv[3] === "--reviewed-provenance-sha")
    )
  )
    no("capture_command");
  if (process.env.CI !== "true") no("ci_safety_required");
  const git = (args: string[]) => {
    const r = spawnSync("git", args, {
      encoding: "utf8",
      env: { PATH: process.env.PATH },
    });
    if (r.status !== 0) no("git");
    return r.stdout;
  };
  const commit = git(["rev-parse", "HEAD"]).trim();
  if (commit !== argv[2] || !/^[0-9a-f]{40}$/u.test(commit))
    no("source_revision");
  const allowed = new Set(
    [
      "plan.txt",
      "plan.detailed.txt",
      "inputs.sanitized.json",
      "provenance.json",
    ].map((s) => PREFIX + "." + s),
  );
  const changed = [
    ...git(["diff", "HEAD", "--name-only", "-z"]).split("\0"),
    ...git(["ls-files", "--others", "--exclude-standard", "-z"]).split("\0"),
  ].filter(Boolean);
  if (changed.some((p) => !allowed.has(p))) no("dirty_source");
  const spec = bootstrapSpec(
    JSON.parse(readFileSync(".96bulls/state-bootstrap/inputs.json", "utf8")),
  );
  const token = process.env.CLOUDFLARE_API_TOKEN ?? "";
  if (process.env.CLOUDFLARE_ACCOUNT_ID !== spec.accountId) no("account");
  const node = spawnSync("node", ["--version"], {
    encoding: "utf8",
    env: { PATH: process.env.PATH },
  });
  if (node.status !== 0) no("node");
  const source = {
    commit,
    host: hostname(),
    clean: true,
    tools: {
      bun: Bun.version,
      node: node.stdout.trim(),
      alchemy: JSON.parse(
        readFileSync("node_modules/alchemy/package.json", "utf8"),
      ).version,
      cloudflare: JSON.parse(
        readFileSync(
          "node_modules/@alchemy.run/cloudflare/package.json",
          "utf8",
        ),
      ).version,
    },
  };
  const directory = resolve(".96bulls/state-bootstrap/runtime");
  process.env.ALCHEMY_HOME = resolve(directory, "auth");
  const recheckSource = () => {
    if (git(["rev-parse", "HEAD"]).trim() !== commit) no("source_revision");
    const files = [
      ...git(["diff", "HEAD", "--name-only", "-z"]).split("\0"),
      ...git(["ls-files", "--others", "--exclude-standard", "-z"]).split("\0"),
    ].filter(Boolean);
    if (files.some((path) => !allowed.has(path))) no("dirty_source");
  };
  if (mode === "create") {
    const plans = {
        planText: readFileSync(PREFIX + ".plan.txt", "utf8"),
        detailedText: readFileSync(PREFIX + ".plan.detailed.txt", "utf8"),
      },
      provenanceText = readFileSync(PREFIX + ".provenance.json", "utf8");
    const captured = JSON.parse(provenanceText);
    // Validate the reviewed local capture and explicit approval before reserving 1P.
    verifyBootstrap(
      source,
      spec,
      captured.admission,
      plans,
      provenanceText,
      argv[4],
    );
    recheckSource();
    const reserved = await prepareOnePasswordSink(
      spec,
      captured.generation,
      onePasswordPort,
    );
    return await runStateBootstrap({
      mode: "create",
      spec,
      token,
      source,
      directory,
      recheckSource,
      credentialSink: reserved.sink,
      reviewed: { plans, provenanceText, provenanceSha256: argv[4] },
    });
  }
  const result = await runStateBootstrap({
    mode: "plan",
    spec,
    token,
    source,
    directory,
    recheckSource,
  });
  if (result.operation !== "captured-only") no("capture_result");
  mkdirSync(dirname(PREFIX), { recursive: true });
  const provenanceText = JSON.stringify(result.capture, null, 2) + "\n";
  for (const [suffix, text] of [
    ["plan.txt", result.plans!.planText],
    ["plan.detailed.txt", result.plans!.detailedText],
    [
      "inputs.sanitized.json",
      JSON.stringify(
        {
          accountId: spec.accountId,
          secretsStoreId: spec.secretsStoreId,
          onePasswordVaultId: spec.onePasswordVaultId,
        },
        null,
        2,
      ) + "\n",
    ],
    ["provenance.json", provenanceText],
  ])
    writeFileSync(PREFIX + "." + suffix, text!, { mode: 0o600 });
  return {
    operation: "captured-only",
    sourceCommit: commit,
    generation: result.capture!.generation,
    capture: {
      provenanceSha256: hash(provenanceText),
      planSha256: hash(result.plans!.planText),
      inputsSha256: inputsDigest(JSON.stringify(spec)),
      sourceCommit: commit,
    },
  };
}
if (import.meta.main)
  captureStateBootstrap()
    .then((value) => console.log(JSON.stringify(value)))
    .catch((error) => {
      console.error(
        "state bootstrap refused: " +
          (error instanceof BootstrapRefusal ? error.code : "metadata_or_plan"),
      );
      process.exitCode = 1;
    });
