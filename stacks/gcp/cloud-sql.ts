/**
 * Private Cloud SQL body and Auth Proxy route.
 *
 * `@alchemy.run/gcp@2.0.0-beta.84` `GCP.SQL.Instance` writes only
 * `ipConfiguration.ipv4Enabled`. Extra fields are dropped, so this stack must
 * not call that resource. The body below is the SQL Admin insert shape a
 * later poster has to send: public IPv4 off, the VPC link, the Private
 * Service Access range, and no authorized networks. `posted` stays false.
 * `sqlAdminInsertRequest` builds the SQL Admin POST and does not send it.
 * `ensurePrivateCloudSql` gets the instance, inserts only when it is absent,
 * polls the operation, and passes the bearer credential only to that transport.
 * The stack calls it from an apply-time Alchemy Action; it never calls
 * `GCP.SQL.Instance` because the pinned provider drops `privateNetwork`.
 */

import type { Input } from "alchemy/Input";
import { isIP } from "node:net";
import { performance } from "node:perf_hooks";

export interface SqlProviderCapabilities {
  /** True only when `GCP.SQL.Instance` accepts `privateNetwork` and passes it to Cloud SQL. */
  privateNetwork: boolean;
}

/** Capability of the pin used by this lane. Do not flip this until the provider passes the field through. */
export const ALCHEMY_GCP_BETA_84_SQL: SqlProviderCapabilities = { privateNetwork: false };

export const CLOUD_SQL_PROXY_IMAGE = "gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.26.3";

export interface CloudSqlIpConfiguration {
  ipv4Enabled: false;
  /** `projects/{project}/global/networks/{name}`. */
  privateNetwork: string;
  /** Private Service Access range name, not a CIDR. */
  allocatedIpRange: string;
}

export interface PrivateCloudSqlSpec {
  /** The spec is inert until the apply-time Action posts `insertBody` unchanged. */
  posted: false;
  instanceName: string;
  connectionName: string;
  databaseVersion: "POSTGRES_17";
  ipv4Enabled: false;
  privateNetwork: string;
  allocatedIpRange: string;
  allocatedIpCidr: string;
  insertBody: {
    name: string;
    region: string;
    databaseVersion: "POSTGRES_17";
    settings: {
      tier: string;
      edition: "ENTERPRISE";
      availabilityType: "ZONAL" | "REGIONAL";
      backupConfiguration: {
        enabled: true;
        pointInTimeRecoveryEnabled: true;
      };
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
  allocatedIpCidr: string;
  tier?: string;
  highAvailability?: boolean;
}): PrivateCloudSqlSpec {
  if (!PROJECT_ID.test(input.projectId)) refuse("Cloud SQL private attachment needs a project id.");
  if (!REGION.test(input.region)) refuse("Cloud SQL private attachment needs a region.");
  const instanceName = shortName("instance", input.instanceName);
  const networkName = shortName("VPC network", input.networkName);
  const allocatedIpRange = shortName("Private Service Access range", input.allocatedIpRange);
  if (!isRfc1918Cidr(input.allocatedIpCidr))
    refuse("Cloud SQL private attachment needs an RFC1918 Private Service Access CIDR.");
  const tier = input.tier ?? "db-custom-1-3840";
  if (!TIER.test(tier)) refuse("Cloud SQL tier is not a Cloud SQL tier name.");
  const privateNetwork = `projects/${input.projectId}/global/networks/${networkName}`;
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
    allocatedIpCidr: input.allocatedIpCidr,
    insertBody: {
      name: instanceName,
      region: input.region,
      databaseVersion: "POSTGRES_17",
      settings: {
        tier,
        edition: "ENTERPRISE",
        availabilityType: input.highAvailability ? "REGIONAL" : "ZONAL",
        backupConfiguration: {
          enabled: true,
          pointInTimeRecoveryEnabled: true,
        },
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
              args: readonly ["--private-ip", "--address=0.0.0.0", "--port=5432", Input<string>];
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

const INSERT_BODY_KEYS = new Set(["name", "region", "databaseVersion", "settings"]);
const INSERT_SETTINGS_KEYS = new Set(["tier", "edition", "availabilityType", "backupConfiguration", "ipConfiguration"]);
const INSERT_BACKUP_KEYS = new Set(["enabled", "pointInTimeRecoveryEnabled"]);
const INSERT_IP_KEYS = new Set(["ipv4Enabled", "privateNetwork", "allocatedIpRange"]);
const PRIVATE_NETWORK = /^projects\/([a-z][a-z0-9-]{4,28}[a-z0-9])\/global\/networks\/([a-z](?:[a-z0-9-]{0,61}[a-z0-9])?)$/;
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

function exactKeys(value: object, allowed: ReadonlySet<string>, label: string): void {
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) refuse(`Cloud SQL ${label} has unexpected field ${key}.`);
  }
}

/** SQL Admin `instances.insert` for the private-only body. This function does not call the network. */
export function sqlAdminInsertRequest(spec: PrivateCloudSqlSpec): SqlAdminInsertRequest {
  if (spec.posted !== false || spec.ipv4Enabled !== false || spec.databaseVersion !== "POSTGRES_17")
    refuse("Cloud SQL insert seam requires the unposted private-only spec.");
  if (!isRfc1918Cidr(spec.allocatedIpCidr))
    refuse("Cloud SQL insert seam lost the RFC1918 Private Service Access CIDR.");
  if (spec.proxyArgs[0] !== "--private-ip" || spec.proxyArgs.at(-1) !== spec.connectionName)
    refuse("Cloud SQL insert seam lost --private-ip.");
  const body = spec.insertBody;
  exactKeys(body, INSERT_BODY_KEYS, "insert body");
  exactKeys(body.settings, INSERT_SETTINGS_KEYS, "settings");
  exactKeys(body.settings.backupConfiguration, INSERT_BACKUP_KEYS, "backupConfiguration");
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
  if (
    body.databaseVersion !== "POSTGRES_17" ||
    body.settings.edition !== "ENTERPRISE" ||
    !TIER.test(body.settings.tier)
  )
    refuse("Cloud SQL insert seam drifted off PostgreSQL 17.");
  if (body.settings.availabilityType !== "ZONAL" && body.settings.availabilityType !== "REGIONAL")
    refuse("Cloud SQL insert seam drifted off the availability type.");
  if (
    body.settings.backupConfiguration.enabled !== true ||
    body.settings.backupConfiguration.pointInTimeRecoveryEnabled !== true
  )
    refuse("Cloud SQL insert seam lost automated backups or point-in-time recovery.");
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
  request(
    call: SqlAdminEnsureRequest,
    credential: SqlAdminCredential,
    signal?: AbortSignal,
  ): Promise<SqlAdminEnsureResponse>;
}

type SqlAdminFetch = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

const SQL_ADMIN_ORIGIN = "https://sqladmin.googleapis.com";
const SQL_ADMIN_INSERT_PATH =
  /^\/v1\/projects\/[a-z][a-z0-9-]{4,28}[a-z0-9]\/instances$/;
const SQL_ADMIN_GET_PATH =
  /^\/v1\/projects\/[a-z][a-z0-9-]{4,28}[a-z0-9]\/(?:instances\/[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?|operations\/[A-Za-z0-9_-]{1,128})$/;

/**
 * Live SQL Admin transport for the apply-time poster. The bearer is attached
 * only after the URL passes the exact HTTPS host/path allowlist; neither the
 * request object nor the returned action output contains it.
 */
export function sqlAdminFetchTransport(fetcher: SqlAdminFetch = globalThis.fetch): SqlAdminEnsureTransport {
  return {
    async request(call, credential, signal) {
      let url: URL;
      try {
        url = new URL(call.url);
      } catch {
        refuse("Cloud SQL Admin target is invalid.");
      }
      if (
        url.origin !== SQL_ADMIN_ORIGIN ||
        url.username ||
        url.password ||
        url.search ||
        url.hash ||
        !(
          (call.method === "GET" && call.body === undefined && SQL_ADMIN_GET_PATH.test(url.pathname)) ||
          (call.method === "POST" && call.body !== undefined && SQL_ADMIN_INSERT_PATH.test(url.pathname))
        )
      )
        refuse("Cloud SQL Admin target is not approved.");
      const response = await fetcher(url, {
        method: call.method,
        headers: {
          accept: "application/json",
          authorization: `Bearer ${credential.bearer}`,
          ...(call.body === undefined ? {} : { "content-type": "application/json" }),
        },
        body: call.body === undefined ? undefined : JSON.stringify(call.body),
        signal,
      });
      const text = await response.text();
      let body: unknown;
      if (text.length > 0) {
        try {
          body = JSON.parse(text);
        } catch {
          body = undefined;
        }
      }
      return { status: response.status, body };
    },
  };
}

export interface SqlAdminPollControl {
  now(): number;
  sleep(ms: number): Promise<void>;
  deadlineMs?: number;
  intervalMs?: number;
  maxPolls?: number;
  /** Bounds one lookup, insert, or operation GET. The poll deadline does not cancel a hung request. */
  requestTimeoutMs?: number;
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
  requestTimeoutMs: number;
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

function malformed(projectId: string, instanceName: string): never {
  throw new Error(
    `96bulls: Cloud SQL Admin operation is malformed for ${projectId}/${instanceName}; reconcile that instance before retry or teardown.`,
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

function ipv4Number(value: string): number | undefined {
  if (isIP(value) !== 4 || value !== value.trim()) return;
  const octets = value.split(".").map(Number);
  return (((octets[0]! << 24) >>> 0) + (octets[1]! << 16) + (octets[2]! << 8) + octets[3]!) >>> 0;
}

function isRfc1918Address(value: string): boolean {
  const ip = ipv4Number(value);
  if (ip === undefined) return false;
  return (
    (ip >= 0x0a000000 && ip <= 0x0affffff) ||
    (ip >= 0xac100000 && ip <= 0xac1fffff) ||
    (ip >= 0xc0a80000 && ip <= 0xc0a8ffff)
  );
}

function isRfc1918Cidr(value: string): boolean {
  if (typeof value !== "string" || value !== value.trim()) return false;
  const match = /^(\d{1,3}(?:\.\d{1,3}){3})\/(\d{1,2})$/.exec(value);
  if (!match) return false;
  const address = ipv4Number(match[1]!);
  const prefix = Number(match[2]);
  if (address === undefined || prefix > 30 || !isRfc1918Address(match[1]!)) return false;
  const mask = prefix === 0 ? 0 : (0xffffffff << (32 - prefix)) >>> 0;
  if ((address & mask) !== address) return false;
  const minimumPrivatePrefix = address >= 0x0a000000 && address <= 0x0affffff
    ? 8
    : address >= 0xac100000 && address <= 0xac1fffff
      ? 12
      : 16;
  return prefix >= minimumPrivatePrefix;
}

function addressInsideCidr(value: string, cidr: string): boolean {
  const address = ipv4Number(value);
  const match = /^(\d{1,3}(?:\.\d{1,3}){3})\/(\d{1,2})$/.exec(cidr);
  if (address === undefined || !match || !isRfc1918Address(value)) return false;
  const network = ipv4Number(match[1]!);
  const prefix = Number(match[2]);
  if (network === undefined || prefix < 0 || prefix > 32) return false;
  const mask = prefix === 0 ? 0 : (0xffffffff << (32 - prefix)) >>> 0;
  return (address & mask) === (network & mask);
}

function classifyExisting(spec: PrivateCloudSqlSpec, body: unknown): "match" | "public" | "not-ready" | "mismatch" {
  const record = asRecord(body);
  const settings = asRecord(record?.settings);
  const backup = asRecord(settings?.backupConfiguration);
  const ip = asRecord(settings?.ipConfiguration);
  const networks = ip?.authorizedNetworks;
  const addresses = record?.ipAddresses;
  if (ip?.ipv4Enabled === true) return "public";
  if (Array.isArray(networks) && networks.length > 0) return "public";
  if (Array.isArray(addresses) && addresses.some((entry) => asRecord(entry)?.type === "PRIMARY")) return "public";
  const networksEmpty = networks === undefined || (Array.isArray(networks) && networks.length === 0);
  const privateMatch =
    record?.name === spec.instanceName &&
    record.region === spec.insertBody.region &&
    record.databaseVersion === "POSTGRES_17" &&
    settings?.tier === spec.insertBody.settings.tier &&
    settings.edition === spec.insertBody.settings.edition &&
    settings.availabilityType === spec.insertBody.settings.availabilityType &&
    backup?.enabled === true &&
    backup.pointInTimeRecoveryEnabled === true &&
    ip?.ipv4Enabled === false &&
    samePrivateNetwork(spec.privateNetwork, ip.privateNetwork) &&
    ip.allocatedIpRange === spec.allocatedIpRange &&
    networksEmpty;
  if (!privateMatch) return "mismatch";
  if (record?.state !== "RUNNABLE") return "not-ready";
  const privateAddresses =
    Array.isArray(addresses) &&
    addresses.length > 0 &&
    addresses.every((entry) => {
      const address = asRecord(entry);
      const ipAddress = address?.ipAddress;
      return (
        address?.type === "PRIVATE" &&
        typeof ipAddress === "string" &&
        ipAddress === ipAddress.trim() &&
        addressInsideCidr(ipAddress, spec.allocatedIpCidr)
      );
    });
  if (!privateAddresses) return "mismatch";
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
  if (record.status === "PENDING" || record.status === "RUNNING") return "pending";
  if (record.status === "DONE") return record.error == null ? "done" : "failed";
  return "invalid";
}

function resolvePoll(poll?: SqlAdminPollControl): ResolvedPoll {
  const deadlineMs = poll?.deadlineMs ?? 10 * 60 * 1000;
  const intervalMs = poll?.intervalMs ?? 2000;
  const maxPolls = poll?.maxPolls ?? 300;
  const requestTimeoutMs = poll?.requestTimeoutMs ?? 30_000;
  if (!Number.isFinite(deadlineMs) || deadlineMs < 1 || deadlineMs > 60 * 60 * 1000)
    refuse("Cloud SQL Admin poll deadline is invalid.");
  if (!Number.isInteger(intervalMs) || intervalMs < 1 || intervalMs > 60_000)
    refuse("Cloud SQL Admin poll interval is invalid.");
  if (!Number.isInteger(maxPolls) || maxPolls < 1 || maxPolls > 10_000)
    refuse("Cloud SQL Admin poll limit is invalid.");
  if (!Number.isInteger(requestTimeoutMs) || requestTimeoutMs < 1 || requestTimeoutMs > 120_000)
    refuse("Cloud SQL Admin request timeout is invalid.");
  return {
    now: poll?.now ?? (() => Date.now()),
    sleep: poll?.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms))),
    deadlineMs,
    intervalMs,
    maxPolls,
    requestTimeoutMs,
  };
}

async function ask(
  transport: SqlAdminEnsureTransport,
  call: SqlAdminEnsureRequest,
  credential: SqlAdminCredential,
  timeoutMs: number,
): Promise<SqlAdminEnsureResponse | "thrown" | "timeout"> {
  const controller = new AbortController();
  let timedOut = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<"timeout">((resolve) => {
    timer = setTimeout(() => {
      timedOut = true;
      controller.abort();
      resolve("timeout");
    }, timeoutMs);
  });
  try {
    const response = await Promise.race([
      transport.request(call, credential, controller.signal).then(
        (value) => value,
        (): "timeout" | "thrown" => (timedOut ? "timeout" : "thrown"),
      ),
      timeout,
    ]);
    if (response === "timeout" || response === "thrown") return response;
    if (!response || typeof response.status !== "number" || !Number.isInteger(response.status)) return "thrown";
    return response;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

interface OuterDeadline {
  injectedAt: number;
  monotonicAt: number;
}

function remainingMs(clock: ResolvedPoll, deadline: OuterDeadline): number {
  return Math.min(deadline.injectedAt - clock.now(), deadline.monotonicAt - performance.now());
}

function requestBudget(clock: ResolvedPoll, deadline: OuterDeadline): number | undefined {
  const remaining = remainingMs(clock, deadline);
  if (remaining <= 0) return;
  return Math.min(clock.requestTimeoutMs, remaining);
}

function deadlineAlive(clock: ResolvedPoll, deadline: OuterDeadline): boolean {
  return remainingMs(clock, deadline) > 0;
}

async function askWithinDeadline(
  clock: ResolvedPoll,
  deadline: OuterDeadline,
  transport: SqlAdminEnsureTransport,
  call: SqlAdminEnsureRequest,
  credential: SqlAdminCredential,
): Promise<SqlAdminEnsureResponse | "thrown" | "timeout"> {
  const budget = requestBudget(clock, deadline);
  if (budget === undefined) return "timeout";
  const response = await ask(transport, call, credential, budget);
  // setTimeout cannot preempt synchronous event-loop blocking. The absolute
  // clocks are therefore checked again after every transport continuation.
  if (!deadlineAlive(clock, deadline)) return "timeout";
  return response;
}

async function sleepWithinDeadline(clock: ResolvedPoll, deadline: OuterDeadline): Promise<boolean> {
  if (!deadlineAlive(clock, deadline)) return false;
  const remaining = remainingMs(clock, deadline);
  if (remaining <= 0) return false;
  const duration = Math.min(clock.intervalMs, remaining);
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const completed = await Promise.race([
      clock.sleep(duration).then(() => remainingMs(clock, deadline) > 0),
      new Promise<false>((resolve) => {
        timer = setTimeout(() => resolve(false), remaining);
      }),
    ]);
    return completed && deadlineAlive(clock, deadline);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/**
 * Get the private instance or create it, then poll the insert operation.
 * There is no default transport. A thrown or unreadable insert does not claim
 * the instance is absent. The stack calls this only from an apply-time Action.
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
  // One absolute deadline covers lookup, insert, 409 adoption, operation
  // polling, every sleep, and final confirmation. The real clock prevents a
  // faulty injected test clock from weakening the production wall bound.
  const deadline = {
    injectedAt: clock.now() + clock.deadlineMs,
    monotonicAt: performance.now() + clock.deadlineMs,
  };
  const projectId = PRIVATE_NETWORK.exec(request.body.settings.ipConfiguration.privateNetwork)?.[1];
  if (!projectId) refuse("Cloud SQL insert seam lost the VPC link.");
  const instanceName = spec.instanceName;
  const instanceUrl = `https://sqladmin.googleapis.com/v1/projects/${projectId}/instances/${instanceName}`;
  const getCall: SqlAdminEnsureRequest = { method: "GET", url: instanceUrl };
  const existing = await askWithinDeadline(clock, deadline, transport, getCall, credential);
  if (existing === "timeout") refuse("Cloud SQL Admin instance lookup timed out.");
  if (existing === "thrown") refuse("Cloud SQL Admin instance lookup was not accepted.");
  if (existing.status === 200) {
    const kind = classifyExisting(spec, existing.body);
    if (kind === "match") {
      if (!deadlineAlive(clock, deadline)) refuse("Cloud SQL Admin instance lookup timed out.");
      return { created: false, operation: null };
    }
    if (kind === "public") rejectPublic(projectId, instanceName);
    if (kind === "not-ready") rejectNotReady(projectId, instanceName);
    rejectMismatch(projectId, instanceName);
  }
  if (existing.status !== 404) refuse("Cloud SQL Admin instance lookup was not accepted.");

  const inserted = await askWithinDeadline(
    clock,
    deadline,
    transport,
    { method: "POST", url: request.url, body: request.body },
    credential,
  );
  if (inserted === "thrown" || inserted === "timeout") uncertain(projectId, instanceName);
  if (inserted.status === 409) {
    for (let attempt = 0; attempt < clock.maxPolls; attempt += 1) {
      const again = await askWithinDeadline(clock, deadline, transport, getCall, credential);
      if (again === "thrown" || again === "timeout") uncertain(projectId, instanceName);
      if (again.status === 200) {
        const kind = classifyExisting(spec, again.body);
        if (kind === "match") {
          if (!deadlineAlive(clock, deadline)) timedOut(projectId, instanceName);
          return { created: false, operation: null };
        }
        // Only the normal create window is worth another GET. FAILED and any
        // other non-RUNNABLE state will not become a private match.
        if (kind === "not-ready" && asRecord(again.body)?.state === "PENDING_CREATE") {
          if (!(await sleepWithinDeadline(clock, deadline))) timedOut(projectId, instanceName);
          continue;
        }
        if (kind === "not-ready") rejectNotReady(projectId, instanceName);
        rejectAdoption(projectId, instanceName);
      } else if (again.status !== 404) {
        uncertain(projectId, instanceName);
      }
      if (!(await sleepWithinDeadline(clock, deadline))) timedOut(projectId, instanceName);
    }
    timedOut(projectId, instanceName);
  }
  if (inserted.status !== 200) uncertain(projectId, instanceName);
  const operationId = operationIdOf(inserted.body);
  if (!operationId) uncertain(projectId, instanceName);
  const initial = operationState(inserted.body);
  if (initial === "failed") failedOperation(projectId, instanceName);
  if (initial === "invalid") malformed(projectId, instanceName);
  if (initial !== "done") {
    const operationUrl = `https://sqladmin.googleapis.com/v1/projects/${projectId}/operations/${operationId}`;
    let finished = false;
    for (let attempt = 0; attempt < clock.maxPolls; attempt += 1) {
      const polled = await askWithinDeadline(
        clock,
        deadline,
        transport,
        { method: "GET", url: operationUrl },
        credential,
      );
      if (polled === "thrown" || polled === "timeout" || polled.status !== 200) uncertain(projectId, instanceName);
      const state = operationState(polled.body);
      if (state === "done") {
        finished = true;
        break;
      }
      if (state === "failed") failedOperation(projectId, instanceName);
      if (state === "invalid") malformed(projectId, instanceName);
      if (state !== "pending") uncertain(projectId, instanceName);
      if (!(await sleepWithinDeadline(clock, deadline))) timedOut(projectId, instanceName);
    }
    if (!finished) timedOut(projectId, instanceName);
  }
  const confirmed = await askWithinDeadline(clock, deadline, transport, getCall, credential);
  if (confirmed === "thrown" || confirmed === "timeout" || confirmed.status !== 200) uncertain(projectId, instanceName);
  if (classifyExisting(spec, confirmed.body) !== "match") createdMismatch(projectId, instanceName);
  if (!deadlineAlive(clock, deadline)) timedOut(projectId, instanceName);
  return { created: true, operation: operationId };
}

/** In-cluster proxy. The instance connection name is a positional arg, and `--private-ip` is the first flag. */
export function cloudSqlProxyWorkload(
  spec: PrivateCloudSqlSpec,
  workloadIdentityEmail: string,
  postedConnectionName: Input<string>,
): CloudSqlProxyWorkload {
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
                args: ["--private-ip", "--address=0.0.0.0", "--port=5432", postedConnectionName],
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
