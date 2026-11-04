/** Source-only operator adapter. No operation happens on import.
 * Real execution requires a separately reviewed operator exception; no CI bypass.
 */
import { createHash } from "node:crypto";
import { hostname } from "node:os";
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { spawnSync } from "node:child_process";
import * as Effect from "effect/Effect";
import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import { bufferedQueueProviders } from "../stacks/cloudflare/buffered-queue-provider.ts";
import { State, type StateService } from "alchemy/State/State";
import { makeHttpStateStore } from "alchemy/State/HttpStateStore";
import { encodeState } from "alchemy/State/StateEncoding";
import { STATE_STORE_VERSION } from "@alchemy.run/cloudflare/StateStore/Api";
import { pathToFileURL } from "node:url";
import * as Layer from "effect/Layer";
import * as ConfigProvider from "effect/ConfigProvider";
import * as References from "effect/References";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Stacks from "alchemy/Alchemist/routes/stack";
import { routeCacheLayer, StackModuleLoader } from "alchemy/Alchemist/Session";
import { PlatformServices } from "alchemy/Util/PlatformServices";
import { AlchemyContext } from "alchemy/AlchemyContext";
import { ProfileStore } from "alchemy/Auth/Profile";
import { CredentialsStore } from "alchemy/Auth/Credentials";
import { ArtifactStore, createArtifactStore } from "alchemy/Artifacts";
import { layerNonInteractive } from "alchemy/Interaction";
import { formatPlanLines } from "alchemy/Cli/LoggingCli";
import {
  APPROVER,
  APPROVAL_MAX_AGE_DAYS,
  parseCalendarDate,
  inputsDigest,
  assertApprovalBinding,
} from "../stacks/aws/deploy-guard.ts";
import {
  assertCloudflareStage,
  type CloudflareStageInputs,
} from "../stacks/cloudflare/alchemy.cloudflare.run.ts";
import {
  assertSessionIngestPlanContract,
  assertPrivateWorker,
  assertNoSecretLiterals,
  SESSION_INGEST_RESOURCES,
  sessionIngestVars,
  sessionIngestStage,
  SESSION_INGEST_STAGE,
  SESSION_INGEST_WORKER,
  FANOUT_QUEUE,
  FANOUT_DLQ,
  S3_ACCESS_KEY_SECRET,
  S3_SECRET_KEY_SECRET,
  SIGNING_KEY_SECRET,
} from "../stacks/cloudflare/session-ingest.ts";
import { sanitizePlan, captureFiles } from "../src/cli/plan-provenance.ts";
export const OPERATOR_PROTOCOL = "private-ingest-operator-v1";
export const STATE_SCRIPT = "alchemy-state-store-96bulls";
export const EXECUTION_EXCEPTION = "ssh-sf-frame-retained-plan";
const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const isHash = (s: unknown): s is string =>
  typeof s === "string" && /^[0-9a-f]{64}$/u.test(s);
const isCommit = (s: unknown): s is string =>
  typeof s === "string" && /^[0-9a-f]{40}$/u.test(s);
const object = (s: unknown): s is Record<string, any> =>
  s !== null && typeof s === "object" && !Array.isArray(s);
export class OperatorRefusal extends Error {
  constructor(readonly code: string) {
    super("private ingest operator refused: " + code);
  }
}
function refuse(code: string): never {
  throw new OperatorRefusal(code);
}
export interface OperatorSecrets {
  accountId: string;
  apiToken: string;
  stateUrl: string;
  stateToken: string;
}
export interface OperatorOptions {
  mode: "plan" | "deploy";
  sourceSha: string;
  inputsSha256: string;
  provenanceSha256?: string;
}
export interface OperatorSource {
  commit: string;
  changedFiles: string[];
  host: string;
}
export interface OperatorPlans {
  planText: string;
  detailedText: string;
}
export interface OperatorCapture {
  schema: 1;
  kind: typeof OPERATOR_PROTOCOL;
  stage: string;
  generatedAt: string;
  generation: string;
  source: { commit: string; host: string };
  target: {
    accountId: string;
    stateScript: string;
    stateUrl: string;
    stateVersion: number;
    controlStateSha256: string;
    nativeBindingsSha256: string;
  };
  inputs: { sha256: string };
  outputs: { plan: { sha256: string }; detailed: { sha256: string } };
  tools: { bun: string; node: string; alchemy: string; cloudflare: string };
  execution: {
    updateStateStore: false;
    profile: "memory-only";
    stateCredentials: "memory-only";
    refresh: "forbidden";
  };
}
export function assertOperatorSource(
  options: OperatorOptions,
  source: OperatorSource,
): void {
  if (
    !isCommit(options.sourceSha) ||
    source.commit !== options.sourceSha ||
    !isHash(options.inputsSha256)
  )
    refuse("immutable_source_or_inputs");
  const allowed = new Set(captureFiles(SESSION_INGEST_STAGE));
  if (source.changedFiles.some((path) => !allowed.has(path)))
    refuse("dirty_source");
  if (!source.host) refuse("host");
}
export function operatorStateUrl(text: string): string {
  let url: URL;
  try {
    url = new URL(text);
  } catch {
    refuse("state_url");
  }
  if (
    url.protocol !== "https:" ||
    !new RegExp(
      "^" + STATE_SCRIPT + "\\.[a-z0-9][a-z0-9-]{0,62}\\.workers\\.dev$",
      "u",
    ).test(url.hostname) ||
    url.pathname !== "/" ||
    url.search ||
    url.hash ||
    url.username ||
    url.password
  )
    refuse("state_url");
  return url.origin;
}
export function assertOperatorInputs(
  text: string,
  options: OperatorOptions,
  secrets: OperatorSecrets,
): CloudflareStageInputs {
  if (
    !/^[0-9a-f]{32}$/u.test(secrets.accountId) ||
    /^0+$/u.test(secrets.accountId) ||
    !secrets.apiToken ||
    !secrets.stateToken ||
    secrets.apiToken.length < 16 ||
    secrets.stateToken.length < 16
  )
    refuse("memory_credentials_required");
  operatorStateUrl(secrets.stateUrl);
  if (inputsDigest(text) !== options.inputsSha256) refuse("input_digest");
  let input: unknown;
  try {
    input = JSON.parse(text);
  } catch {
    refuse("input_json");
  }
  try {
    assertCloudflareStage(SESSION_INGEST_STAGE, options.mode, input, {
      CLOUDFLARE_ACCOUNT_ID: secrets.accountId,
    });
  } catch {
    refuse("original_stage_guard");
  }
  if (
    (input as CloudflareStageInputs).sessionIngest.accountId !==
    secrets.accountId
  )
    refuse("account");
  return input as CloudflareStageInputs;
}
export function operatorSafePlans(
  plans: OperatorPlans,
  secrets: OperatorSecrets,
  bindings?: BindingInventory,
): OperatorPlans {
  const safe = (text: string) => {
    const clean =
      sanitizePlan(text.replace(/\x1b\[[0-9;]*m/gu, ""), []).trim() + "\n";
    if (
      Buffer.byteLength(clean) > 16 * 1024 * 1024 ||
      [secrets.apiToken, secrets.stateToken].some(
        (s) => s.length >= 8 && clean.includes(s),
      )
    )
      refuse("unsafe_plan");
    return clean;
  };
  const result = {
    planText: safe(plans.planText),
    detailedText: safe(plans.detailedText),
  };
  if (bindings) {
    try {
      assertSessionIngestPlanContract(
        resourcePlanView(result.planText, bindings),
        resourcePlanView(result.detailedText, bindings),
      );
    } catch {
      refuse("original_plan_contract");
    }
  }
  return result;
}
export interface StateAuthority {
  stateVersion: number;
  controlStateSha256: string;
  nativeBindingsSha256?: string;
}
export function operatorCapture(
  source: OperatorSource,
  inputText: string,
  secrets: OperatorSecrets,
  plans: OperatorPlans,
  tools: OperatorCapture["tools"],
  authority: StateAuthority,
  now = new Date(),
): OperatorCapture {
  if (
    authority.stateVersion !== STATE_STORE_VERSION ||
    !isHash(authority.controlStateSha256) ||
    !isHash(authority.nativeBindingsSha256)
  )
    refuse("capture_native_authority");
  const body = {
    source: { commit: source.commit, host: source.host },
    target: {
      accountId: secrets.accountId,
      stateScript: STATE_SCRIPT,
      stateUrl: operatorStateUrl(secrets.stateUrl),
      stateVersion: authority.stateVersion,
      controlStateSha256: authority.controlStateSha256,
      nativeBindingsSha256: authority.nativeBindingsSha256,
    },
    inputs: { sha256: inputsDigest(inputText) },
    outputs: {
      plan: { sha256: sha(plans.planText) },
      detailed: { sha256: sha(plans.detailedText) },
    },
    tools,
    execution: {
      updateStateStore: false as const,
      profile: "memory-only" as const,
      stateCredentials: "memory-only" as const,
      refresh: "forbidden" as const,
    },
  };
  const generation = sha(JSON.stringify([OPERATOR_PROTOCOL, body]));
  return {
    schema: 1,
    kind: OPERATOR_PROTOCOL,
    stage: SESSION_INGEST_STAGE,
    generatedAt: now.toISOString(),
    generation,
    ...body,
  };
}
export function verifyOperatorCapture(
  options: OperatorOptions,
  source: OperatorSource,
  inputText: string,
  secrets: OperatorSecrets,
  plans: OperatorPlans,
  provenanceText: string,
  tools: OperatorCapture["tools"],
  authority: StateAuthority,
  now = new Date(),
): OperatorCapture {
  if (
    !isHash(options.provenanceSha256) ||
    sha(provenanceText) !== options.provenanceSha256
  )
    refuse("provenance_digest");
  let capture: any;
  try {
    capture = JSON.parse(provenanceText);
  } catch {
    refuse("provenance_json");
  }
  const fresh = operatorCapture(
    source,
    inputText,
    secrets,
    plans,
    tools,
    authority,
    now,
  );
  if (
    !object(capture) ||
    capture.schema !== 1 ||
    capture.kind !== OPERATOR_PROTOCOL ||
    capture.stage !== SESSION_INGEST_STAGE ||
    capture.generation !== fresh.generation ||
    inputsDigest(
      JSON.stringify({ bound: { ...capture, generatedAt: null } }),
    ) !==
      inputsDigest(JSON.stringify({ bound: { ...fresh, generatedAt: null } }))
  )
    refuse("capture_generation");
  const input = assertOperatorInputs(inputText, options, secrets),
    approval = input.approval as any;
  const day = parseCalendarDate(approval?.date),
    today = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  if (
    !approval ||
    approval.decision !== "approved" ||
    approval.by !== APPROVER ||
    approval.execution !== EXECUTION_EXCEPTION ||
    typeof approval.quote !== "string" ||
    approval.quote.trim().length < 8 ||
    !day ||
    day.getTime() > today ||
    today - day.getTime() > APPROVAL_MAX_AGE_DAYS * 86400000
  )
    refuse("explicit_manual_exception_required");
  try {
    assertApprovalBinding(approval.capture, {
      provenanceText,
      planText: plans.planText,
      inputsText: inputText,
    });
  } catch {
    refuse("original_approval_binding");
  }
  return capture as OperatorCapture;
}
/** Reject refresh/preview/state-store mutations BEFORE any network call. */
export function guardOperatorFetch(
  secrets: OperatorSecrets,
  mode: "plan" | "deploy",
  base: typeof fetch,
  acceptanceMode: "canonical" | "buffered" = "canonical",
): typeof fetch {
  const state = operatorStateUrl(secrets.stateUrl),
    queueIds = new Map<string, string>();
  const observe = async (response: Response) => {
    if (!response.ok) return response;
    const b = await operatorMetadata(response.clone()),
      rows = Array.isArray(b.result) ? b.result : [b.result];
    for (const r of rows)
      if (
        r &&
        [FANOUT_QUEUE, FANOUT_DLQ].includes(r.queue_name) &&
        typeof r.queue_id === "string" &&
        /^[0-9a-f]{32}$/u.test(r.queue_id)
      )
        queueIds.set(r.queue_id, r.queue_name);
    return response;
  };
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
      refuse("network_target");
    const send = () =>
      base(input, {
        ...init,
        redirect: "error",
        signal: AbortSignal.timeout(30000),
      });
    if (url.origin === state) {
      if (url.search) refuse("state_scope");
      const root =
        "/state/stacks/96bulls-cloudflare/stages/" + SESSION_INGEST_STAGE;
      const key = url.pathname.startsWith(root + "/resources/")
        ? url.pathname.slice((root + "/resources/").length)
        : "";
      const read =
        method === "GET" &&
        (url.pathname === "/version" ||
          url.pathname === "/state/stacks" ||
          url.pathname ===
            "/state/stacks/CloudflareStateStore/stages/" +
              STATE_SCRIPT +
              "/resources/Api" ||
          url.pathname === "/state/stacks/96bulls-cloudflare/stages" ||
          [
            root + "/resources",
            root + "/replaced-resources",
            root + "/output",
          ].includes(url.pathname) ||
          SESSION_INGEST_RESOURCES.includes(key as never));
      const write =
        mode === "deploy" &&
        method === "PUT" &&
        (url.pathname === root + "/output" ||
          SESSION_INGEST_RESOURCES.includes(key as never));
      if (!read && !write) refuse("state_scope");
      return send();
    }
    const prefix = "/client/v4/accounts/" + secrets.accountId + "/";
    if (
      url.origin !== "https://api.cloudflare.com" ||
      !url.pathname.startsWith(prefix) ||
      /preview|secrets_store|secrets-store|%2f|%5c|%2e/iu.test(url.pathname)
    )
      refuse("network_scope");
    const path = url.pathname.slice(prefix.length);
    if (method === "GET") {
      const response = await send();
      return path === "queues" || /^queues\/[0-9a-f]{32}$/u.test(path)
        ? observe(response)
        : response;
    }
    if (mode !== "deploy") refuse("plan_cloudflare_write");
    if (path === "workers/scripts/" + SESSION_INGEST_WORKER && method === "PUT")
      return send();
    if (
      path === "workers/scripts/" + SESSION_INGEST_WORKER + "/settings" &&
      method === "PATCH"
    ) {
      const b = await new Response(init?.body).json();
      if (!object(b) || Object.keys(b).some((k) => k !== "observability"))
        refuse("worker_settings_scope");
      return send();
    }
    if (
      path === "workers/scripts/" + SESSION_INGEST_WORKER + "/subdomain" &&
      method === "POST"
    ) {
      const b = await new Response(init?.body).json();
      if (!object(b) || b.enabled !== false || b.previews_enabled !== false)
        refuse("private_worker_url");
      return send();
    }
    if (path === "queues" && method === "POST") {
      const b = await new Response(init?.body).json();
      if (!object(b) || ![FANOUT_QUEUE, FANOUT_DLQ].includes(b.queue_name))
        refuse("queue_scope");
      return observe(await send());
    }
    const q = /^queues\/([0-9a-f]{32})(?:\/consumers(?:\/[^/]+)?)?$/u.exec(
      path,
    );
    if (q && queueIds.has(q[1]!) && ["PUT", "POST", "PATCH"].includes(method)) {
      const b = await new Response(init?.body).json();
      if (!object(b)) refuse("queue_scope");
      if (path.includes("/consumers")) {
        if (
          queueIds.get(q[1]!) !== FANOUT_QUEUE ||
          b.script_name !== SESSION_INGEST_WORKER ||
          b.dead_letter_queue !== FANOUT_DLQ
        )
          refuse("consumer_scope");
      } else if (method === "PATCH") {
        if (acceptanceMode !== "buffered") refuse("queue_pause_scope");
        if (
          Object.keys(b).some((k) => k !== "settings") ||
          !object(b.settings) ||
          Object.keys(b.settings).length !== 1 ||
          b.settings.delivery_paused !== true
        )
          refuse("queue_pause_scope");
      } else if (b.queue_name !== queueIds.get(q[1]!)) refuse("queue_scope");
      return send();
    }
    refuse("unplanned_cloudflare_write");
  };
  return Object.assign(guarded, {
    preconnect: () => refuse("preconnect_forbidden"),
  }) as typeof fetch;
}
/** No ~/.alchemy profile or credential service is instantiated. State cache lives only in memory. */
export function memoryOperatorServices(
  secrets: OperatorSecrets,
  runDirectory: string,
  fetcher: typeof fetch,
  mode: "plan" | "deploy",
) {
  const denied = () => Effect.die(new OperatorRefusal("profile_io_forbidden"));
  const profiles: any = {
    current: Effect.succeed({
      name: "private-ingest-memory",
      source: "configuration",
    }),
    readManifest: denied(),
    getProfile: denied,
    ensureProfile: denied,
    createProfile: denied,
    renameProfile: denied,
    setProviderConfig: denied,
    deleteProviderConfig: denied,
    deleteProfile: denied,
    loadProviderConfig: denied,
  };
  const credentials: any = {
    read: denied,
    write: denied,
    delete: denied,
    deleteProfile: denied,
  };
  credentials.scoped = () => credentials;
  return Layer.mergeAll(
    PlatformServices,
    FetchHttpClient.layer,
    Layer.succeed(FetchHttpClient.Fetch, fetcher),
    ConfigProvider.layer(
      ConfigProvider.fromEnvRecord({
        CI: "true",
        CLOUDFLARE_ACCOUNT_ID: secrets.accountId,
        CLOUDFLARE_API_TOKEN: secrets.apiToken,
      }),
    ),
    Layer.succeed(AlchemyContext, {
      dotAlchemy: runDirectory,
      dev: false,
      adopt: false,
      updateStateStore: false,
    }),
    Layer.succeed(ProfileStore, profiles),
    Layer.succeed(CredentialsStore, credentials),
    Layer.succeed(ArtifactStore, createArtifactStore()),
    routeCacheLayer,
    layerNonInteractive(),
  );
}
export interface RetainedPlanResult {
  plans: OperatorPlans;
  summary: unknown;
  authority: StateAuthority;
}
export type BindingInventory = Array<{
  sid: string;
  action: string;
  target: Record<string, unknown>;
}>;
export function resourcePlanView(
  text: string,
  inventory: BindingInventory,
): string {
  const summary = text.split("\n").filter((l) => l.startsWith("Plan:"));
  const bindingCount = inventory.filter((b) => b.action !== "noop").length;
  const stated = summary[0]?.match(/(?:, )?(\d+) binding changes/u);
  if (
    summary.length !== 1 ||
    (bindingCount
      ? Number(stated?.[1]) !== bindingCount
      : stated !== null && stated !== undefined)
  )
    refuse("binding_render_summary");
  const expected = new Set(
      inventory.map((b) => "[session-ingest/" + b.sid + "] " + b.action),
    ),
    seen = new Set<string>();
  const lines = text.split("\n").filter((line) => {
    if (!/^\s*\[session-ingest\//u.test(line)) return true;
    if (!expected.has(line.trim()) || seen.has(line.trim()))
      refuse("binding_render_inventory");
    seen.add(line.trim());
    return false;
  });
  if (seen.size !== expected.size) refuse("binding_render_inventory");
  return lines.join("\n");
}
export function assertOperatorSnapshot(
  snapshot: any,
  input: CloudflareStageInputs,
): BindingInventory {
  const rows = Object.values(snapshot.native.resources ?? {}) as any[];
  const expected: Record<string, string> = {
    "session-ingest-fanout-dlq": "Cloudflare.Queues.Queue",
    "session-ingest-fanout": "Cloudflare.Queues.Queue",
    "session-ingest": "Cloudflare.Worker",
    "session-ingest-fanout-consumer": "Cloudflare.Queues.Consumer",
  };
  if (
    rows.length !== 4 ||
    Object.keys(snapshot.native.deletions ?? {}).length ||
    Object.keys(snapshot.native.actions ?? {}).length ||
    Object.keys(snapshot.native.actionDeletions ?? {}).length
  )
    refuse("native_resource_inventory");
  const seen = new Set<string>();
  let inventory: BindingInventory = [];
  for (const row of rows) {
    const id = row.resource.LogicalId;
    if (
      seen.has(id) ||
      expected[id] !== row.resource.Type ||
      !["create", "update", "noop"].includes(row.action)
    )
      refuse("native_resource_inventory");
    seen.add(id);
    if (id === "session-ingest") {
      assertPrivateWorker(row.props, () => refuse("native_private_worker"));
      if (row.props.main !== "src/session-ingest/worker.ts")
        refuse("native_worker_entrypoint");
      assertNoSecretLiterals(row.props.env, () =>
        refuse("native_secret_literal"),
      );
      const vars = sessionIngestVars(input.sessionIngest),
        expected: Record<string, any> = {};
      for (const [k, text] of Object.entries(vars))
        expected[k] = { type: "plain_text", name: k, text };
      expected.HYPERDRIVE = {
        type: "hyperdrive",
        name: "HYPERDRIVE",
        id: input.sessionIngest.hyperdriveId,
      };
      expected.INGEST_FANOUT = { type: "queue", name: "INGEST_FANOUT" };
      for (const [name, secretName] of Object.entries({
        S3_ACCESS_KEY_ID: S3_ACCESS_KEY_SECRET,
        S3_SECRET_ACCESS_KEY: S3_SECRET_KEY_SECRET,
        INGEST_SIGNING_KEY: SIGNING_KEY_SECRET,
      })) {
        const value = row.props.env[name];
        if (
          value.Type !== "Cloudflare.SecretsStore.Secret" ||
          value.secretName !== secretName ||
          value.storeId !== input.sessionIngest.secretsStoreId
        )
          refuse("native_secret_target");
        expected[name] = {
          type: "secrets_store_secret",
          name,
          secretName,
          storeId: input.sessionIngest.secretsStoreId,
        };
      }
      if (input.sessionIngest.replicaMode === "queue")
        expected.NEON_REPLICA = {
          type: "queue",
          name: "NEON_REPLICA",
          queue_name: input.sessionIngest.neonReplicaQueue,
        };
      if (row.bindings.length !== Object.keys(expected).length)
        refuse("native_binding_inventory");
      const found = new Set<string>();
      for (const b of row.bindings) {
        const wanted = expected[b.sid],
          actual = b.data?.bindings;
        if (
          !wanted ||
          found.has(b.sid) ||
          !["create", "update", "noop"].includes(b.action) ||
          !Array.isArray(actual) ||
          actual.length !== 1 ||
          Object.entries(b.data).some(
            ([k, v]) => k !== "bindings" && v !== undefined,
          )
        )
          refuse("native_binding_inventory");
        found.add(b.sid);
        const value = { ...actual[0] };
        if (b.sid === "INGEST_FANOUT") {
          const ref = row.resource.Props.env.INGEST_FANOUT;
          if (
            ref.Type !== "Cloudflare.Queues.Queue" ||
            ref.LogicalId !== "session-ingest-fanout" ||
            (value.queue_name !== undefined &&
              value.queue_name !== FANOUT_QUEUE)
          )
            refuse("native_binding_target");
          delete value.queue_name;
        }
        if (
          inputsDigest(JSON.stringify({ value })) !==
          inputsDigest(JSON.stringify({ value: wanted }))
        )
          refuse("native_binding_target");
        inventory.push({
          sid: b.sid,
          action: b.action,
          target:
            b.sid === "INGEST_FANOUT"
              ? {
                  ...wanted,
                  queue_name: FANOUT_QUEUE,
                  logicalId: "session-ingest-fanout",
                }
              : wanted,
        });
      }
    }
    if (
      ["session-ingest-fanout", "session-ingest-fanout-dlq"].includes(id) &&
      row.props.deliveryPaused !==
        (input.sessionIngest.acceptanceMode === "buffered" ? true : undefined)
    )
      refuse("native_queue_pause");
    if (id !== "session-ingest" && row.bindings.length)
      refuse("native_binding_inventory");
    if (
      (id === "session-ingest-fanout" && row.props.name !== FANOUT_QUEUE) ||
      (id === "session-ingest-fanout-dlq" && row.props.name !== FANOUT_DLQ)
    )
      refuse("native_queue_identity");
  }
  if (
    [...seen].sort().join(",") !==
    [...SESSION_INGEST_RESOURCES].sort().join(",")
  )
    refuse("native_resource_inventory");
  return inventory.sort((a, b) => a.sid.localeCompare(b.sid));
}
export function requireStateAuthority(
  remote: StateService,
  secrets: OperatorSecrets,
) {
  return Effect.gen(function* () {
    const stateVersion = yield* remote.getVersion();
    if (stateVersion !== STATE_STORE_VERSION)
      refuse("state_version_prerequisite");
    const control = yield* remote.get({
      stack: "CloudflareStateStore",
      stage: STATE_SCRIPT,
      fqn: "Api",
    });
    if (
      !control ||
      !("resourceType" in control) ||
      control.resourceType !== "Cloudflare.Worker" ||
      !["created", "updated"].includes(control.status) ||
      control.attr?.workerName !== STATE_SCRIPT ||
      control.attr?.accountId !== secrets.accountId
    )
      refuse("control_state_prerequisite");
    return {
      stateVersion,
      controlStateSha256: sha(JSON.stringify(encodeState(control))),
    };
  });
}
export async function retainedOperatorPlan(
  secrets: OperatorSecrets,
  mode: "plan" | "deploy",
  input: CloudflareStageInputs,
  review: (plans: OperatorPlans, authority: StateAuthority) => void,
  runtime: {
    directory: string;
    emptyEnvPath: string;
    fetcher: typeof fetch;
    reviewed?: {
      options: OperatorOptions;
      source: OperatorSource;
      inputText: string;
      provenanceText: string;
      tools: OperatorCapture["tools"];
      recheckSource: () => void;
    };
  },
): Promise<RetainedPlanResult> {
  if (
    mode === "deploy" &&
    (!runtime.reviewed || typeof runtime.reviewed.recheckSource !== "function")
  )
    refuse("retained_deploy_approval_required");
  if (
    process.env.CI !== "true" ||
    process.env.CLOUDFLARE_ACCOUNT_ID !== secrets.accountId ||
    process.env.CLOUDFLARE_API_TOKEN !== secrets.apiToken
  )
    refuse("environment_credential_binding");
  let authority: StateAuthority | undefined;
  let remoteState: StateService | undefined;
  const initializeState = Effect.gen(function* () {
    const remote = yield* makeHttpStateStore({
      url: operatorStateUrl(secrets.stateUrl),
      authToken: secrets.stateToken,
      id: "cloudflare-http",
    });
    authority = yield* requireStateAuthority(remote, secrets);
    remoteState = remote;
    return remote;
  });
  const state = Layer.effect(
    State,
    Effect.gen(function* () {
      const context =
        yield* Effect.context<Effect.Services<typeof initializeState>>();
      return yield* Effect.cached(
        initializeState.pipe(Effect.provideContext(context), Effect.orDie),
      );
    }),
  );
  // Reuse the authoritative resource declaration and original guard. The custom
  // state port deliberately does not instantiate Cloudflare.state/bootstrap/login.
  const definition = Alchemy.Stack(
    "96bulls-cloudflare",
    {
      providers:
        input.sessionIngest.acceptanceMode === "buffered"
          ? bufferedQueueProviders()
          : Cloudflare.providers(),
      state,
    },
    Effect.gen(function* () {
      assertCloudflareStage(SESSION_INGEST_STAGE, mode, input, {
        CLOUDFLARE_ACCOUNT_ID: secrets.accountId,
      });
      const ingest = yield* sessionIngestStage(input.sessionIngest);
      return {
        stage: SESSION_INGEST_STAGE,
        accountId: input.sessionIngest.accountId,
        sessionIngest: ingest,
      };
    }),
  );
  const entrypoint = resolve("stacks/cloudflare/alchemy.cloudflare.run.ts");
  const loader = Layer.succeed(StackModuleLoader, {
    import: async (url: string) => {
      if (url !== pathToFileURL(entrypoint).href) refuse("entrypoint");
      return { default: definition };
    },
  });
  const program = Effect.gen(function* () {
    const snapshot = yield* Stacks.plan({
      target: {
        entrypoint,
        stage: SESSION_INGEST_STAGE,
        envFile: runtime.emptyEnvPath,
      },
      operation: "deploy",
      updateStateStore: false,
    });
    const bindings = assertOperatorSnapshot(snapshot, input);
    if (!authority) refuse("state_authority");
    authority.nativeBindingsSha256 = sha(JSON.stringify(bindings));
    const plans = operatorSafePlans(
      {
        planText: formatPlanLines(snapshot.native).join("\n"),
        detailedText: formatPlanLines(snapshot.native, { detailed: true }).join(
          "\n",
        ),
      },
      secrets,
      bindings,
    );
    if (!authority) refuse("state_authority");
    review(plans, authority);
    if (mode === "deploy") {
      const r = runtime.reviewed!;
      if (
        r.options.mode !== "deploy" ||
        inputsDigest(JSON.stringify(input)) !== inputsDigest(r.inputText)
      )
        refuse("retained_deploy_inputs");
      assertOperatorSource(r.options, r.source);
      verifyOperatorCapture(
        r.options,
        r.source,
        r.inputText,
        secrets,
        plans,
        r.provenanceText,
        r.tools,
        authority,
      );
      r.recheckSource();
      const current = yield* requireStateAuthority(remoteState!, secrets);
      if (
        current.controlStateSha256 !== authority.controlStateSha256 ||
        current.stateVersion !== authority.stateVersion
      )
        refuse("state_authority_drift");
      yield* Stacks.apply(snapshot);
    }
    return { plans, summary: snapshot.summary, authority };
  });
  return Effect.runPromise(
    program.pipe(
      Effect.provide(
        Layer.mergeAll(
          memoryOperatorServices(
            secrets,
            runtime.directory,
            guardOperatorFetch(
              secrets,
              mode,
              runtime.fetcher,
              input.sessionIngest.acceptanceMode,
            ),
            mode,
          ),
          loader,
        ),
      ),
      Effect.provideService(References.MinimumLogLevel, "None"),
      Effect.scoped,
    ),
  );
}
async function operatorMetadata(response: Response): Promise<any> {
  const reader = response.body?.getReader();
  if (!reader) refuse("metadata_body");
  const abort = new AbortController();
  const timer = setTimeout(() => abort.abort(), 20000);
  let onAbort: () => void = () => {};
  const cancelled = new Promise<never>((_, reject) => {
    onAbort = () => {
      reject(new OperatorRefusal("metadata_deadline"));
      void reader!.cancel().catch(() => {});
    };
  });
  abort.signal.addEventListener("abort", onAbort, { once: true });
  const parts: Uint8Array[] = [];
  let bytes = 0;
  try {
    for (;;) {
      const part = await Promise.race([reader.read(), cancelled]);
      if (part.done) break;
      bytes += part.value.byteLength;
      if (bytes > 65536) refuse("metadata_bound");
      parts.push(part.value);
    }
    const all = new Uint8Array(bytes);
    let at = 0;
    for (const part of parts) {
      all.set(part, at);
      at += part.length;
    }
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(all));
  } finally {
    clearTimeout(timer);
    abort.signal.removeEventListener("abort", onAbort);
    void reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
function parseOptions(argv: string[]): OperatorOptions {
  const mode = argv[0];
  if (mode !== "plan" && mode !== "deploy") refuse("command");
  const keys = ["--expected-sha", "--inputs-sha256", "--provenance-sha256"],
    values = new Map<string, string>();
  for (let i = 1; i < argv.length; i += 2) {
    const key = argv[i]!,
      value = argv[i + 1];
    if (!keys.includes(key) || !value || values.has(key)) refuse("arguments");
    values.set(key, value);
  }
  return {
    mode,
    sourceSha: values.get("--expected-sha") ?? "",
    inputsSha256: values.get("--inputs-sha256") ?? "",
    provenanceSha256: values.get("--provenance-sha256"),
  };
}
const PREFIX = "docs/plans/" + SESSION_INGEST_STAGE;
export async function operatorMain(argv = process.argv.slice(2)) {
  const options = parseOptions(argv);
  if (process.env.CI !== "true") refuse("ci_state_safety_required");
  const git = (args: string[]) => {
    const result = spawnSync("git", args, {
      encoding: "utf8",
      env: { PATH: process.env.PATH },
    });
    if (result.status !== 0) refuse("source_command");
    return result.stdout;
  };
  const sourceNow = () => ({
    commit: git(["rev-parse", "HEAD"]).trim(),
    changedFiles: [
      ...new Set(
        [
          ...git(["diff", "HEAD", "--name-only", "-z"]).split("\0"),
          ...git(["ls-files", "--others", "--exclude-standard", "-z"]).split(
            "\0",
          ),
        ].filter(Boolean),
      ),
    ],
    host: hostname(),
  });
  const source = sourceNow();
  assertOperatorSource(options, source);
  const secrets = {
    accountId: process.env.CLOUDFLARE_ACCOUNT_ID ?? "",
    apiToken: process.env.CLOUDFLARE_API_TOKEN ?? "",
    stateUrl: process.env.PRIVATE_INGEST_STATE_URL ?? "",
    stateToken: process.env.PRIVATE_INGEST_STATE_TOKEN ?? "",
  };
  const inputPath =
      ".96bulls/" + SESSION_INGEST_STAGE + "/cloudflare-inputs.json",
    inputText = readFileSync(inputPath, "utf8");
  assertOperatorInputs(inputText, options, secrets);
  const node = spawnSync("node", ["--version"], {
    encoding: "utf8",
    env: { PATH: process.env.PATH },
  });
  if (node.status !== 0) refuse("node_version");
  const tools = {
    bun: Bun.version,
    node: node.stdout.trim(),
    alchemy: JSON.parse(
      readFileSync("node_modules/alchemy/package.json", "utf8"),
    ).version,
    cloudflare: JSON.parse(
      readFileSync("node_modules/@alchemy.run/cloudflare/package.json", "utf8"),
    ).version,
  };
  const readPlans = () => ({
    planText: readFileSync(PREFIX + ".plan.txt", "utf8"),
    detailedText: readFileSync(PREFIX + ".plan.detailed.txt", "utf8"),
  });
  let captured: OperatorCapture | undefined,
    reviewed: OperatorPlans | undefined;
  if (options.mode === "deploy") {
    reviewed = readPlans();
    operatorSafePlans(reviewed, secrets);
    captured = verifyOperatorCapture(
      options,
      source,
      inputText,
      secrets,
      reviewed,
      readFileSync(PREFIX + ".provenance.json", "utf8"),
      tools,
      JSON.parse(readFileSync(PREFIX + ".provenance.json", "utf8")).target,
    );
  }
  const directory = resolve(".96bulls/private-ingest-operator/runtime");
  mkdirSync(directory, { recursive: true });
  process.env.ALCHEMY_HOME = resolve(directory, "auth"); // Only local locks; profile/credential IO remains denied.
  const emptyEnvPath = resolve(directory, "empty-config.txt");
  writeFileSync(emptyEnvPath, "", { mode: 0o600 });
  const guarded = guardOperatorFetch(secrets, options.mode, fetch);
  // Bind the memory credential URL to the actual account's public subdomain.
  const response = await guarded(
    "https://api.cloudflare.com/client/v4/accounts/" +
      secrets.accountId +
      "/workers/subdomain",
    {
      headers: { authorization: "Bearer " + secrets.apiToken },
      signal: AbortSignal.timeout(20000),
    },
  );
  if (!response.ok) refuse("state_account_readback");
  const metadata: any = await operatorMetadata(response);
  if (
    metadata.success !== true ||
    typeof metadata.result?.subdomain !== "string" ||
    operatorStateUrl(secrets.stateUrl) !==
      "https://" +
        STATE_SCRIPT +
        "." +
        metadata.result.subdomain +
        ".workers.dev"
  )
    refuse("state_account_binding");
  const result = await retainedOperatorPlan(
    secrets,
    options.mode,
    JSON.parse(inputText),
    (plans, authority) => {
      assertOperatorSource(options, sourceNow());
      if (
        captured &&
        JSON.stringify(authority) !==
          JSON.stringify({
            stateVersion: captured.target.stateVersion,
            controlStateSha256: captured.target.controlStateSha256,
            nativeBindingsSha256: captured.target.nativeBindingsSha256,
          })
      )
        refuse("state_authority_drift");
      if (
        reviewed &&
        (sha(plans.planText) !== sha(reviewed.planText) ||
          sha(plans.detailedText) !== sha(reviewed.detailedText))
      )
        refuse("fresh_plan_drift");
    },
    {
      directory,
      emptyEnvPath,
      fetcher: fetch,
      reviewed:
        options.mode === "deploy"
          ? {
              options,
              source,
              inputText,
              provenanceText: readFileSync(PREFIX + ".provenance.json", "utf8"),
              tools,
              recheckSource: () => assertOperatorSource(options, sourceNow()),
            }
          : undefined,
    },
  );
  if (options.mode === "plan") {
    const capture = operatorCapture(
        source,
        inputText,
        secrets,
        result.plans,
        tools,
        result.authority,
      ),
      provenanceText = JSON.stringify(capture, null, 2) + "\n";
    mkdirSync(dirname(PREFIX), { recursive: true });
    for (const [suffix, text] of [
      ["plan.txt", result.plans.planText],
      ["plan.detailed.txt", result.plans.detailedText],
      [
        "inputs.sanitized.json",
        JSON.stringify(
          { sessionIngest: JSON.parse(inputText).sessionIngest },
          null,
          2,
        ) + "\n",
      ],
      ["provenance.json", provenanceText],
    ])
      writeFileSync(PREFIX + "." + suffix, text!, { mode: 0o600 });
    return {
      operation: "captured-only",
      sourceCommit: source.commit,
      generation: capture.generation,
      capture: {
        provenanceSha256: sha(provenanceText),
        planSha256: sha(result.plans.planText),
        inputsSha256: options.inputsSha256,
        sourceCommit: source.commit,
      },
    };
  }
  return {
    operation: "applied-retained-plan",
    sourceCommit: source.commit,
    generation: captured!.generation,
    summary: result.summary,
  };
}
if (import.meta.main)
  operatorMain()
    .then((result) => console.log(JSON.stringify(result)))
    .catch((error) => {
      console.error(
        "private ingest operator refused: " +
          (error instanceof OperatorRefusal
            ? error.code
            : "state_or_plan_failed"),
      );
      process.exitCode = 1;
    });
