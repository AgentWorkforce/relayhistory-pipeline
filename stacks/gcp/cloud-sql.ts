/**
 * Private Cloud SQL body and Auth Proxy route.
 *
 * `@alchemy.run/gcp@2.0.0-beta.84` `GCP.SQL.Instance` writes only
 * `ipConfiguration.ipv4Enabled`. Extra fields are dropped, so this stack must
 * not call that resource. The body below is the SQL Admin insert shape a
 * later poster has to send: public IPv4 off, the VPC link, the Private
 * Service Access range, and no authorized networks. `posted` stays false.
 * `sqlAdminInsertRequest` builds the SQL Admin POST and does not send it.
 * `postPrivateCloudSqlInsert` sends one insert through an injected transport.
 * `ensurePrivateCloudSql` gets the instance, inserts only when it is absent,
 * polls the operation, and passes the bearer credential only to that transport.
 * The stack calls the builder and does not call either poster or `GCP.SQL.Instance`.
 */

export interface SqlProviderCapabilities {
  /** True only when `GCP.SQL.Instance` accepts `privateNetwork` and passes it to Cloud SQL. */
  privateNetwork: boolean;
}

/** Capability of the pin used by this lane. Do not flip this until the provider passes the field through. */
export const ALCHEMY_GCP_BETA_84_SQL: SqlProviderCapabilities = { privateNetwork: false };

export const CLOUD_SQL_PROXY_IMAGE = "gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.26.3";

export interface CloudSqlIpConfiguration {
  ipv4Enabled: false;
  /** `/projects/{project}/global/networks/{name}`. */
  privateNetwork: string;
  /** Private Service Access range name, not a CIDR. */
  allocatedIpRange: string;
}

export interface PrivateCloudSqlSpec {
  /** This module does not call Cloud SQL. A later resource must post `insertBody` unchanged. */
  posted: false;
  instanceName: string;
  connectionName: string;
  databaseVersion: "POSTGRES_17";
  ipv4Enabled: false;
  privateNetwork: string;
  allocatedIpRange: string;
  insertBody: {
    name: string;
    region: string;
    databaseVersion: "POSTGRES_17";
    settings: {
      tier: string;
      availabilityType: "ZONAL" | "REGIONAL";
      ipConfiguration: CloudSqlIpConfiguration;
    };
  };
  /** Positional connection name is last. `--private-ip` is required. */
  proxyArgs: readonly ["--private-ip", "--address=0.0.0.0", "--port=5432", string];
}

const PROJECT_ID = /^[a-z][a-z0-9-]{4,28}[a-z0-9]$/;
const REGION = /^[a-z]+(?:-[a-z0-9]+)+[0-9]$/;
const SHORT_NAME = /^[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const TIER = /^[a-z][a-z0-9-]{1,62}$/;

function refuse(message: string): never {
  throw new Error(`96bulls: ${message} Nothing was created.`);
}

function shortName(label: string, value: unknown): string {
  if (typeof value !== "string" || !SHORT_NAME.test(value))
    refuse(`Cloud SQL private attachment needs a ${label} name.`);
  return value;
}

export function privateCloudSqlSpec(input: {
  projectId: string;
  region: string;
  instanceName: string;
  networkName: string;
  allocatedIpRange: string;
  tier?: string;
  highAvailability?: boolean;
}): PrivateCloudSqlSpec {
  if (!PROJECT_ID.test(input.projectId)) refuse("Cloud SQL private attachment needs a project id.");
  if (!REGION.test(input.region)) refuse("Cloud SQL private attachment needs a region.");
  const instanceName = shortName("instance", input.instanceName);
  const networkName = shortName("VPC network", input.networkName);
  const allocatedIpRange = shortName("Private Service Access range", input.allocatedIpRange);
  const tier = input.tier ?? "db-custom-1-3840";
  if (!TIER.test(tier)) refuse("Cloud SQL tier is not a Cloud SQL tier name.");
  const privateNetwork = `/projects/${input.projectId}/global/networks/${networkName}`;
  const connectionName = `${input.projectId}:${input.region}:${instanceName}`;
  const ipConfiguration: CloudSqlIpConfiguration = {
    ipv4Enabled: false,
    privateNetwork,
    allocatedIpRange,
  };
  if ("authorizedNetworks" in ipConfiguration || ipConfiguration.ipv4Enabled !== false)
    refuse("Cloud SQL ipConfiguration drifted off the private-only body.");
  const proxyArgs = ["--private-ip", "--address=0.0.0.0", "--port=5432", connectionName] as const;
  if (proxyArgs[0] !== "--private-ip") refuse("Cloud SQL proxy lost --private-ip.");
  return {
    posted: false,
    instanceName,
    connectionName,
    databaseVersion: "POSTGRES_17",
    ipv4Enabled: false,
    privateNetwork,
    allocatedIpRange,
    insertBody: {
      name: instanceName,
      region: input.region,
      databaseVersion: "POSTGRES_17",
      settings: {
        tier,
        availabilityType: input.highAvailability ? "REGIONAL" : "ZONAL",
        ipConfiguration,
      },
    },
    proxyArgs,
  };
}

export interface CloudSqlProxyWorkload {
  serviceAccount: {
    apiVersion: "v1";
    kind: "ServiceAccount";
    metadata: {
      name: "cloud-sql-proxy";
      namespace: "96bulls";
      annotations: { "iam.gke.io/gcp-service-account": string };
    };
  };
  service: {
    apiVersion: "v1";
    kind: "Service";
    metadata: { name: "cloud-sql-proxy"; namespace: "96bulls" };
    spec: { selector: { app: "cloud-sql-proxy" }; ports: [{ name: "postgres"; port: 5432; targetPort: 5432 }] };
  };
  deployment: {
    apiVersion: "apps/v1";
    kind: "Deployment";
    metadata: { name: "cloud-sql-proxy"; namespace: "96bulls" };
    spec: {
      replicas: 1;
      selector: { matchLabels: { app: "cloud-sql-proxy" } };
      template: {
        metadata: { labels: { app: "cloud-sql-proxy" } };
        spec: {
          serviceAccountName: "cloud-sql-proxy";
          containers: [
            {
              name: "cloud-sql-proxy";
              image: typeof CLOUD_SQL_PROXY_IMAGE;
              args: PrivateCloudSqlSpec["proxyArgs"];
              ports: [{ name: "postgres"; containerPort: 5432 }];
              readinessProbe: { tcpSocket: { port: 5432 }; periodSeconds: 10 };
              livenessProbe: { tcpSocket: { port: 5432 }; periodSeconds: 20 };
              securityContext: { runAsNonRoot: true; allowPrivilegeEscalation: false; readOnlyRootFilesystem: true };
            },
          ];
        };
      };
    };
  };
}

const GSA_EMAIL = /^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com$/;

/**
 * Apply the proxy only after a resource posts `insertBody`. The pinned
 * provider never does, so this stays false and the stack must not create
 * a proxy for an instance that does not exist.
 */
export function cloudSqlProxyApplies(spec: PrivateCloudSqlSpec): boolean {
  return spec.posted !== false && spec.ipv4Enabled === false && spec.proxyArgs[0] === "--private-ip";
}

const INSERT_BODY_KEYS = new Set(["name", "region", "databaseVersion", "settings"]);
const INSERT_SETTINGS_KEYS = new Set(["tier", "availabilityType", "ipConfiguration"]);
const INSERT_IP_KEYS = new Set(["ipv4Enabled", "privateNetwork", "allocatedIpRange"]);
const PRIVATE_NETWORK = /^\/projects\/([a-z][a-z0-9-]{4,28}[a-z0-9])\/global\/networks\/([a-z](?:[a-z0-9-]{0,61}[a-z0-9])?)$/;
const RETURNED_PRIVATE_NETWORK = /^\/?projects\/([a-z][a-z0-9-]{4,28}[a-z0-9])\/global\/networks\/([a-z](?:[a-z0-9-]{0,61}[a-z0-9])?)$/;
const OPERATION_ID = /^[A-Za-z0-9_-]{1,128}$/;

export interface SqlAdminInsertRequest {
  method: "POST";
  url: string;
  /** Authentication belongs exclusively to the injected transport. */
  headers: { "content-type": "application/json" };
  body: PrivateCloudSqlSpec["insertBody"];
  /** Building a request does not send it. */
  sent: false;
}

export interface SqlAdminTransport {
  /** Send `request` as the body. Keep the credential in the transport, not the request. */
  post(request: SqlAdminInsertRequest): Promise<{ name: string }>;
}

function exactKeys(value: object, allowed: ReadonlySet<string>, label: string): void {
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) refuse(`Cloud SQL ${label} has unexpected field ${key}.`);
  }
}

/** SQL Admin `instances.insert` for the private-only body. This function does not call the network. */
export function sqlAdminInsertRequest(spec: PrivateCloudSqlSpec): SqlAdminInsertRequest {
  if (spec.posted !== false || spec.ipv4Enabled !== false || spec.databaseVersion !== "POSTGRES_17")
    refuse("Cloud SQL insert seam requires the unposted private-only spec.");
  if (spec.proxyArgs[0] !== "--private-ip" || spec.proxyArgs.at(-1) !== spec.connectionName)
    refuse("Cloud SQL insert seam lost --private-ip.");
  const body = spec.insertBody;
  exactKeys(body, INSERT_BODY_KEYS, "insert body");
  exactKeys(body.settings, INSERT_SETTINGS_KEYS, "settings");
  const ip = body.settings.ipConfiguration as object;
  exactKeys(ip, INSERT_IP_KEYS, "ipConfiguration");
  const network = PRIVATE_NETWORK.exec(body.settings.ipConfiguration.privateNetwork);
  if (!network || body.settings.ipConfiguration.privateNetwork !== spec.privateNetwork)
    refuse("Cloud SQL insert seam lost the VPC link.");
  const projectId = network[1]!;
  if (body.settings.ipConfiguration.ipv4Enabled !== false)
    refuse("Cloud SQL insert seam would enable public IPv4.");
  if (
    body.settings.ipConfiguration.allocatedIpRange !== spec.allocatedIpRange ||
    !SHORT_NAME.test(body.settings.ipConfiguration.allocatedIpRange)
  )
    refuse("Cloud SQL insert seam lost the Private Service Access range.");
  if (body.name !== spec.instanceName || body.region !== spec.connectionName.split(":")[1])
    refuse("Cloud SQL insert seam drifted off the instance name.");
  if (body.databaseVersion !== "POSTGRES_17" || !TIER.test(body.settings.tier))
    refuse("Cloud SQL insert seam drifted off PostgreSQL 17.");
  if (body.settings.availabilityType !== "ZONAL" && body.settings.availabilityType !== "REGIONAL")
    refuse("Cloud SQL insert seam drifted off the availability type.");
  if (spec.connectionName !== `${projectId}:${body.region}:${body.name}`)
    refuse("Cloud SQL insert seam drifted off the connection name.");
  const rendered = JSON.stringify(body);
  if (rendered.includes("authorizedNetworks") || rendered.includes("rootPassword") || rendered.includes("://") || rendered.includes("BEGIN "))
    refuse("Cloud SQL insert body carries a public network or secret.");
  return {
    method: "POST",
    url: `https://sqladmin.googleapis.com/v1/projects/${projectId}/instances`,
    headers: { "content-type": "application/json" },
    body: structuredClone(body),
    sent: false,
  };
}

/**
 * Send the private-only insert through an injected transport.
 * There is no default transport, and this function is not called by the stack.
 */
export async function postPrivateCloudSqlInsert(
  spec: PrivateCloudSqlSpec,
  transport: SqlAdminTransport,
): Promise<{ operation: string; sent: true }> {
  const request = sqlAdminInsertRequest(spec);
  if (request.sent !== false || Object.keys(request.headers).some((name) => name.toLowerCase() === "authorization"))
    refuse("Cloud SQL insert seam tried to carry a credential.");
  const projectId = PRIVATE_NETWORK.exec(request.body.settings.ipConfiguration.privateNetwork)?.[1];
  let result: { name: string };
  try {
    result = await transport.post(request);
  } catch {
    throw new Error(
      `96bulls: Cloud SQL Admin insert outcome is uncertain for ${projectId}/${spec.instanceName}; reconcile that instance before retry or teardown.`,
    );
  }
  if (!projectId || !OPERATION_ID.test(result?.name ?? ""))
    throw new Error(
      `96bulls: Cloud SQL Admin insert outcome is uncertain for ${projectId}/${spec.instanceName}; reconcile that instance before retry or teardown.`,
    );
  return { operation: result.name, sent: true };
}

/** Held by the caller for one call. The request object never carries it. */
export interface SqlAdminCredential {
  readonly bearer: string;
}

export interface SqlAdminEnsureRequest {
  method: "GET" | "POST";
  url: string;
  body?: PrivateCloudSqlSpec["insertBody"];
}

export interface SqlAdminEnsureResponse {
  status: number;
  body?: unknown;
}

export interface SqlAdminEnsureTransport {
  /**
   * Set `Authorization` from `credential.bearer` for this call only.
   * Do not log the credential, the response body, or this request together.
   */
  request(call: SqlAdminEnsureRequest, credential: SqlAdminCredential): Promise<SqlAdminEnsureResponse>;
}

export interface SqlAdminPollControl {
  now(): number;
  sleep(ms: number): Promise<void>;
  deadlineMs?: number;
  intervalMs?: number;
  maxPolls?: number;
}

export interface PrivateCloudSqlEnsureResult {
  /** False when a matching private instance already existed. */
  created: boolean;
  /** Operation id after a finished create. Null when the existing instance matched. */
  operation: string | null;
}

interface ResolvedPoll {
  now(): number;
  sleep(ms: number): Promise<void>;
  deadlineMs: number;
  intervalMs: number;
  maxPolls: number;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return;
  return value as Record<string, unknown>;
}

function assertCredential(credential: SqlAdminCredential): void {
  const bearer = credential?.bearer;
  if (typeof bearer !== "string" || bearer.length < 16 || bearer.length > 8192 || /\s/.test(bearer))
    refuse("Cloud SQL Admin credential is missing.");
}

function uncertain(projectId: string, instanceName: string): never {
  throw new Error(
    `96bulls: Cloud SQL Admin insert outcome is uncertain for ${projectId}/${instanceName}; reconcile that instance before retry or teardown.`,
  );
}

function timedOut(projectId: string, instanceName: string): never {
  throw new Error(
    `96bulls: Cloud SQL Admin operation timed out for ${projectId}/${instanceName}; reconcile that instance before retry or teardown.`,
  );
}

function failedOperation(projectId: string, instanceName: string): never {
  throw new Error(
    `96bulls: Cloud SQL Admin operation failed for ${projectId}/${instanceName}; reconcile that instance before retry or teardown.`,
  );
}

function rejectPublic(projectId: string, instanceName: string): never {
  throw new Error(`96bulls: Cloud SQL Admin instance ${projectId}/${instanceName} is public. Nothing was changed.`);
}

function rejectMismatch(projectId: string, instanceName: string): never {
  throw new Error(
    `96bulls: Cloud SQL Admin instance ${projectId}/${instanceName} does not match the private body. Nothing was changed.`,
  );
}

function rejectNotReady(projectId: string, instanceName: string): never {
  throw new Error(
    `96bulls: Cloud SQL Admin instance ${projectId}/${instanceName} is not runnable. Reconcile before retry or teardown.`,
  );
}

function rejectAdoption(projectId: string, instanceName: string): never {
  throw new Error(
    `96bulls: Cloud SQL Admin refused to adopt ${projectId}/${instanceName}. Reconcile before retry or teardown.`,
  );
}

function createdMismatch(projectId: string, instanceName: string): never {
  throw new Error(
    `96bulls: Cloud SQL Admin created ${projectId}/${instanceName}, but the instance does not match the private body. Reconcile before retry or teardown.`,
  );
}

function samePrivateNetwork(expected: string, actual: unknown): boolean {
  if (typeof actual !== "string") return false;
  const wanted = RETURNED_PRIVATE_NETWORK.exec(expected);
  const found = RETURNED_PRIVATE_NETWORK.exec(actual);
  return Boolean(wanted && found && wanted[1] === found[1] && wanted[2] === found[2]);
}

function classifyExisting(spec: PrivateCloudSqlSpec, body: unknown): "match" | "public" | "not-ready" | "mismatch" {
  const record = asRecord(body);
  const settings = asRecord(record?.settings);
  const ip = asRecord(settings?.ipConfiguration);
  const networks = ip?.authorizedNetworks;
  const addresses = record?.ipAddresses;
  if (ip?.ipv4Enabled === true) return "public";
  if (Array.isArray(networks) && networks.length > 0) return "public";
  if (Array.isArray(addresses) && addresses.some((entry) => asRecord(entry)?.type === "PRIMARY")) return "public";
  const networksEmpty = networks === undefined || (Array.isArray(networks) && networks.length === 0);
  const addressesPrivate =
    addresses === undefined ||
    (Array.isArray(addresses) && addresses.every((entry) => asRecord(entry)?.type === "PRIVATE"));
  const privateMatch =
    record?.name === spec.instanceName &&
    record.region === spec.insertBody.region &&
    record.databaseVersion === "POSTGRES_17" &&
    settings?.tier === spec.insertBody.settings.tier &&
    settings.availabilityType === spec.insertBody.settings.availabilityType &&
    ip?.ipv4Enabled === false &&
    samePrivateNetwork(spec.privateNetwork, ip.privateNetwork) &&
    ip.allocatedIpRange === spec.allocatedIpRange &&
    networksEmpty &&
    addressesPrivate;
  if (!privateMatch) return "mismatch";
  if (record?.state !== "RUNNABLE") return "not-ready";
  return "match";
}

function operationIdOf(body: unknown): string | undefined {
  const name = asRecord(body)?.name;
  if (typeof name !== "string" || name.includes("://") || name.includes("@") || name.includes("?") || name.includes("#"))
    return;
  const slash = name.lastIndexOf("/");
  const id = slash === -1 ? name : name.slice(slash + 1);
  if (!OPERATION_ID.test(id)) return;
  return id;
}

function operationState(body: unknown): "done" | "failed" | "pending" | "invalid" {
  const record = asRecord(body);
  if (!record) return "invalid";
  if (record.status === undefined || record.status === "PENDING" || record.status === "RUNNING") return "pending";
  if (record.status === "DONE") return record.error == null ? "done" : "failed";
  return "invalid";
}

function resolvePoll(poll?: SqlAdminPollControl): ResolvedPoll {
  const deadlineMs = poll?.deadlineMs ?? 10 * 60 * 1000;
  const intervalMs = poll?.intervalMs ?? 2000;
  const maxPolls = poll?.maxPolls ?? 300;
  if (!Number.isFinite(deadlineMs) || deadlineMs < 1 || deadlineMs > 60 * 60 * 1000)
    refuse("Cloud SQL Admin poll deadline is invalid.");
  if (!Number.isInteger(intervalMs) || intervalMs < 1 || intervalMs > 60_000)
    refuse("Cloud SQL Admin poll interval is invalid.");
  if (!Number.isInteger(maxPolls) || maxPolls < 1 || maxPolls > 10_000)
    refuse("Cloud SQL Admin poll limit is invalid.");
  return {
    now: poll?.now ?? (() => Date.now()),
    sleep: poll?.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms))),
    deadlineMs,
    intervalMs,
    maxPolls,
  };
}

async function ask(
  transport: SqlAdminEnsureTransport,
  call: SqlAdminEnsureRequest,
  credential: SqlAdminCredential,
): Promise<SqlAdminEnsureResponse | "thrown"> {
  try {
    const response = await transport.request(call, credential);
    if (!response || typeof response.status !== "number" || !Number.isInteger(response.status)) return "thrown";
    return response;
  } catch {
    return "thrown";
  }
}

/**
 * Get the private instance or create it, then poll the insert operation.
 * There is no default transport. A thrown or unreadable insert does not claim
 * the instance is absent. This function is not called by the stack.
 */
export async function ensurePrivateCloudSql(
  spec: PrivateCloudSqlSpec,
  credential: SqlAdminCredential,
  transport: SqlAdminEnsureTransport,
  poll?: SqlAdminPollControl,
): Promise<PrivateCloudSqlEnsureResult> {
  const request = sqlAdminInsertRequest(spec);
  if (request.sent !== false || Object.keys(request.headers).some((name) => name.toLowerCase() === "authorization"))
    refuse("Cloud SQL insert seam tried to carry a credential.");
  assertCredential(credential);
  const clock = resolvePoll(poll);
  const projectId = PRIVATE_NETWORK.exec(request.body.settings.ipConfiguration.privateNetwork)?.[1];
  if (!projectId) refuse("Cloud SQL insert seam lost the VPC link.");
  const instanceName = spec.instanceName;
  const instanceUrl = `https://sqladmin.googleapis.com/v1/projects/${projectId}/instances/${instanceName}`;
  const getCall: SqlAdminEnsureRequest = { method: "GET", url: instanceUrl };
  const existing = await ask(transport, getCall, credential);
  if (existing === "thrown") refuse("Cloud SQL Admin instance lookup was not accepted.");
  if (existing.status === 200) {
    const kind = classifyExisting(spec, existing.body);
    if (kind === "match") return { created: false, operation: null };
    if (kind === "public") rejectPublic(projectId, instanceName);
    if (kind === "not-ready") rejectNotReady(projectId, instanceName);
    rejectMismatch(projectId, instanceName);
  }
  if (existing.status !== 404) refuse("Cloud SQL Admin instance lookup was not accepted.");

  const inserted = await ask(transport, { method: "POST", url: request.url, body: request.body }, credential);
  if (inserted === "thrown") uncertain(projectId, instanceName);
  if (inserted.status === 409) {
    const started = clock.now();
    for (let attempt = 0; attempt < clock.maxPolls; attempt += 1) {
      if (clock.now() - started >= clock.deadlineMs) timedOut(projectId, instanceName);
      const again = await ask(transport, getCall, credential);
      if (again === "thrown") uncertain(projectId, instanceName);
      if (again.status === 200) {
        const kind = classifyExisting(spec, again.body);
        if (kind === "match") return { created: false, operation: null };
        if (kind !== "not-ready") rejectAdoption(projectId, instanceName);
      } else if (again.status !== 404) {
        uncertain(projectId, instanceName);
      }
      await clock.sleep(clock.intervalMs);
    }
    timedOut(projectId, instanceName);
  }
  if (inserted.status !== 200) uncertain(projectId, instanceName);
  const operationId = operationIdOf(inserted.body);
  if (!operationId) uncertain(projectId, instanceName);
  const initial = operationState(inserted.body);
  if (initial === "failed") failedOperation(projectId, instanceName);
  if (initial === "invalid") uncertain(projectId, instanceName);
  if (initial !== "done") {
    const operationUrl = `https://sqladmin.googleapis.com/v1/projects/${projectId}/operations/${operationId}`;
    const started = clock.now();
    let finished = false;
    for (let attempt = 0; attempt < clock.maxPolls; attempt += 1) {
      if (clock.now() - started >= clock.deadlineMs) timedOut(projectId, instanceName);
      const polled = await ask(transport, { method: "GET", url: operationUrl }, credential);
      if (polled === "thrown" || polled.status !== 200) uncertain(projectId, instanceName);
      const state = operationState(polled.body);
      if (state === "done") {
        finished = true;
        break;
      }
      if (state === "failed") failedOperation(projectId, instanceName);
      if (state !== "pending") uncertain(projectId, instanceName);
      await clock.sleep(clock.intervalMs);
    }
    if (!finished) timedOut(projectId, instanceName);
  }
  const confirmed = await ask(transport, getCall, credential);
  if (confirmed === "thrown" || confirmed.status !== 200) uncertain(projectId, instanceName);
  if (classifyExisting(spec, confirmed.body) !== "match") createdMismatch(projectId, instanceName);
  return { created: true, operation: operationId };
}

/** In-cluster proxy. The instance connection name is a positional arg, and `--private-ip` is the first flag. */
export function cloudSqlProxyWorkload(spec: PrivateCloudSqlSpec, workloadIdentityEmail: string): CloudSqlProxyWorkload {
  if (spec.posted !== false || spec.proxyArgs[0] !== "--private-ip" || spec.ipv4Enabled !== false)
    refuse("Cloud SQL proxy requires the private-only spec.");
  if (!GSA_EMAIL.test(workloadIdentityEmail))
    refuse("Cloud SQL proxy needs the runtime Workload Identity email, not a key.");
  return {
    serviceAccount: {
      apiVersion: "v1",
      kind: "ServiceAccount",
      metadata: {
        name: "cloud-sql-proxy",
        namespace: "96bulls",
        annotations: { "iam.gke.io/gcp-service-account": workloadIdentityEmail },
      },
    },
    service: {
      apiVersion: "v1",
      kind: "Service",
      metadata: { name: "cloud-sql-proxy", namespace: "96bulls" },
      spec: { selector: { app: "cloud-sql-proxy" }, ports: [{ name: "postgres", port: 5432, targetPort: 5432 }] },
    },
    deployment: {
      apiVersion: "apps/v1",
      kind: "Deployment",
      metadata: { name: "cloud-sql-proxy", namespace: "96bulls" },
      spec: {
        replicas: 1,
        selector: { matchLabels: { app: "cloud-sql-proxy" } },
        template: {
          metadata: { labels: { app: "cloud-sql-proxy" } },
          spec: {
            serviceAccountName: "cloud-sql-proxy",
            containers: [
              {
                name: "cloud-sql-proxy",
                image: CLOUD_SQL_PROXY_IMAGE,
                args: spec.proxyArgs,
                ports: [{ name: "postgres", containerPort: 5432 }],
                readinessProbe: { tcpSocket: { port: 5432 }, periodSeconds: 10 },
                livenessProbe: { tcpSocket: { port: 5432 }, periodSeconds: 20 },
                securityContext: { runAsNonRoot: true, allowPrivilegeEscalation: false, readOnlyRootFilesystem: true },
              },
            ],
          },
        },
      },
    },
  };
}
