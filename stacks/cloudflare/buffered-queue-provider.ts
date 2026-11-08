/** Paused-only live Queue lifecycle for the reviewed buffered ingest mode. */
import * as Cloudflare from "@alchemy.run/cloudflare";
import { CloudflareEnvironment } from "@alchemy.run/cloudflare/CloudflareEnvironment";
import * as Provider from "alchemy/Provider";
import { Queue } from "@alchemy.run/cloudflare/Queues/Queue";
import * as queues from "@distilled.cloud/cloudflare/queues";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { ADMISSION_DLQ, ADMISSION_QUEUE, ADMISSION_RETENTION_SECONDS, FANOUT_QUEUE, FANOUT_DLQ } from "./session-ingest.ts";
export class BufferedQueueRefusal extends Error {
  constructor(readonly code: string) {
    super("buffered queue refused: " + code);
  }
}
const no = (code: string): never => {
  throw new BufferedQueueRefusal(code);
};
/** Narrow view derived from the pinned SDK's authoritative response type. */
export type QueueObserved = Pick<
  queues.GetQueueResponse,
  | "queueId"
  | "queueName"
  | "settings"
  | "consumersTotalCount"
  | "producersTotalCount"
>;
export interface BufferedQueueApi {
  list(accountId: string): Effect.Effect<QueueObserved[], any, any>;
  get(
    accountId: string,
    queueId: string,
  ): Effect.Effect<QueueObserved, any, any>;
  pause(accountId: string, queueId: string): Effect.Effect<unknown, any, any>;
  /** Admission queues only: exactly `{ settings: { message_retention_period: ADMISSION_RETENTION_SECONDS } }`, never a pause. */
  retain(accountId: string, queueId: string): Effect.Effect<unknown, any, any>;
}
const id = (v: unknown): v is string =>
  typeof v === "string" && /^[0-9a-f]{32}$/u.test(v);
const names = new Map([
  ["session-ingest-fanout", FANOUT_QUEUE],
  ["session-ingest-fanout-dlq", FANOUT_DLQ],
]);
function props(input: any): string {
  const name = names.get(input.id);
  if (!name || input.news?.name !== name || input.news?.deliveryPaused !== true)
    no("captured_paused_props");
  return name!;
}
/**
 * The async admission queue + DLQ (declared only with sessionIngest.asyncAdmission): the official
 * lifecycle, never paused and never PATCHed here, with the same ownership/identity refusals as the
 * paused queues. The fanout queues above stay paused-only; no path here can unpause them.
 */
const admissionNames = new Map([
  ["session-ingest-admission", ADMISSION_QUEUE],
  ["session-ingest-admission-dlq", ADMISSION_DLQ],
]);
function admissionProps(input: any): string {
  const name = admissionNames.get(input.id);
  if (!name || input.news?.name !== name || input.news?.deliveryPaused !== undefined)
    no("captured_admission_props");
  return name!;
}
function exact(row: QueueObserved, queueId: string, name: string) {
  if (row.queueId !== queueId || row.queueName !== name) no("queue_identity");
  return row;
}
export function pausedQueueService(
  base: any,
  api: BufferedQueueApi,
  expectedAccountId: string,
  deadlineMs = 30000,
) {
  const bounded = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(Effect.timeout(deadlineMs));
  const paused = pausedOnly(base, api, expectedAccountId, bounded);
  const admission = {
    diff: Effect.fn(function* (input: any) {
      admissionProps(input);
      return yield* base.diff(input);
    }),
    read: Effect.fn(function* (input: any) {
      const result = yield* base.read(input);
      if (!result) return undefined;
      if (!id(result.accountId) || result.accountId !== expectedAccountId || !id(result.queueId) || result.queueName !== admissionNames.get(input.id))
        no("queue_identity");
      return result;
    }),
    reconcile: Effect.fn(function* (input: any) {
      const name = admissionProps(input);
      const accountId = input.output?.accountId;
      if (accountId !== undefined && !id(accountId)) no("queue_identity");
      if (!id(expectedAccountId) || (accountId !== undefined && accountId !== expectedAccountId)) no("queue_identity");
      // Never adopt an unowned queue just because its physical name matches.
      const existing = yield* bounded(api.list(expectedAccountId));
      const matches = existing.filter((q) => q.queueName === name);
      if (matches.length > 1 || (input.output && matches.length !== 1) || (!input.output && matches.length) ||
          (input.output && matches.some((q) => q.queueId !== input.output.queueId)))
        no("unowned_queue_prerequisite");
      const result = yield* base.reconcile(input);
      if (!id(result.accountId) || result.accountId !== expectedAccountId || !id(result.queueId) || result.queueName !== name) no("queue_identity");
      if (input.output && input.output.queueId !== result.queueId) no("owned_queue_changed");
      const observed = exact(yield* bounded(api.get(result.accountId, result.queueId)), result.queueId, name);
      if (!input.output && (observed.consumersTotalCount !== 0 || observed.producersTotalCount !== 0)) no("initial_queue_not_empty");
      // Never paused: an observed pause refuses (nothing here can resume it, and nothing here pauses).
      if (observed.settings?.deliveryPaused === true) no("admission_queue_paused");
      // Retention: the Cloudflare maximum, pinned and read back. Idempotent: no PATCH when it already holds.
      if (observed.settings?.messageRetentionPeriod !== ADMISSION_RETENTION_SECONDS) {
        yield* bounded(api.retain(result.accountId, result.queueId));
        const final = exact(yield* bounded(api.get(result.accountId, result.queueId)), result.queueId, name);
        if (final.settings?.messageRetentionPeriod !== ADMISSION_RETENTION_SECONDS) no("retention_not_observed");
        if (final.settings?.deliveryPaused === true) no("admission_queue_paused");
        if (!input.output && (final.consumersTotalCount !== 0 || final.producersTotalCount !== 0)) no("initial_queue_not_empty");
      }
      return result;
    }),
  };
  const route = (method: "diff" | "read" | "reconcile") => (input: any) =>
    admissionNames.has(input?.id) ? admission[method](input) : paused[method](input);
  return { ...paused, diff: route("diff"), read: route("read"), reconcile: route("reconcile") };
}

function pausedOnly(base: any, api: BufferedQueueApi, expectedAccountId: string, bounded: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, any, R>) {
  return {
    ...base,
    version: (base.version ?? 0) + 1,
    mode: "live",
    modes: undefined,
    diff: Effect.fn(function* (input: any) {
      props(input);
      const ordinary = yield* base.diff(input);
      if (ordinary?.action === "replace") return ordinary;
      if (input.output && input.output.deliveryPaused !== true)
        return { action: "update" };
      return ordinary;
    }),
    read: Effect.fn(function* (input: any) {
      const result = yield* base.read(input);
      if (!result) return undefined;
      if (
        !id(result.accountId) ||
        result.accountId !== expectedAccountId ||
        !id(result.queueId) ||
        !names.has(input.id) ||
        result.queueName !== names.get(input.id)
      )
        no("queue_identity");
      const observed = exact(
        yield* bounded(api.get(result.accountId, result.queueId)),
        result.queueId,
        result.queueName,
      );
      return {
        ...result,
        deliveryPaused: observed.settings?.deliveryPaused === true,
      };
    }),
    reconcile: Effect.fn(function* (input: any) {
      const name = props(input);
      // Never adopt an unowned queue just because its physical name matches.
      const accountId = input.output?.accountId;
      if (accountId !== undefined && !id(accountId)) no("queue_identity");
      if (
        !id(expectedAccountId) ||
        (accountId !== undefined && accountId !== expectedAccountId)
      )
        no("queue_identity");
      const existing = yield* bounded(api.list(expectedAccountId));
      const matches = existing.filter((q) => q.queueName === name);
      if (
        matches.length > 1 ||
        (input.output && matches.length !== 1) ||
        (!input.output && matches.length) ||
        (input.output &&
          matches.some((q) => q.queueId !== input.output.queueId))
      )
        no("unowned_queue_prerequisite");
      const result = yield* base.reconcile(input);
      if (
        !id(result.accountId) ||
        result.accountId !== expectedAccountId ||
        !id(result.queueId) ||
        result.queueName !== name
      )
        no("queue_identity");
      if (input.output && input.output.queueId !== result.queueId)
        no("owned_queue_changed");
      const observed = exact(
        yield* bounded(api.get(result.accountId, result.queueId)),
        result.queueId,
        name,
      );
      if (
        !input.output &&
        (observed.consumersTotalCount !== 0 ||
          observed.producersTotalCount !== 0)
      )
        no("initial_queue_not_empty");
      yield* bounded(api.pause(result.accountId, result.queueId));
      const final = exact(
        yield* bounded(api.get(result.accountId, result.queueId)),
        result.queueId,
        name,
      );
      if (final.settings?.deliveryPaused !== true) no("pause_not_observed");
      if (
        !input.output &&
        (final.consumersTotalCount !== 0 || final.producersTotalCount !== 0)
      )
        no("initial_queue_not_empty");
      return { ...result, deliveryPaused: true };
    }),
    delete: () =>
      Effect.die(new BufferedQueueRefusal("queue_delete_forbidden")),
  };
}
export const liveBufferedQueueApi: BufferedQueueApi = {
  list: (accountId) =>
    queues.listQueues.items({ accountId }).pipe(
      Stream.take(10001),
      Stream.runCollect,
      Effect.map((rows) => {
        if (rows.length > 10000) no("queue_inventory_bound");
        return [...rows];
      }),
    ),
  get: (accountId, queueId) => queues.getQueue({ accountId, queueId }),
  pause: (accountId, queueId) =>
    queues.patchQueue({
      accountId,
      queueId,
      settings: { deliveryPaused: true },
    }),
  retain: (accountId, queueId) =>
    queues.patchQueue({
      accountId,
      queueId,
      settings: { messageRetentionPeriod: ADMISSION_RETENTION_SECONDS },
    }),
};
/** Overrides only the live Queue registration; Worker/Consumer stay official. */
export function bufferedQueueProviders() {
  return Provider.effect(
    Queue,
    Effect.gen(function* () {
      const live = yield* Provider.findProvider(Queue, "live");
      const { accountId } = yield* yield* CloudflareEnvironment;
      return pausedQueueService(
        live,
        liveBufferedQueueApi,
        accountId,
      ) as Provider.ProviderService<Queue>;
    }),
  ).pipe(Layer.provideMerge(Cloudflare.providers()));
}
