/**
 * INGEST_ADVERTISED_MAX_RECORDS: the records-per-batch limit this Worker ADVERTISES to uploaders, separately from the
 * limit it ACCEPTS. Default off: unset publishes DELIVERY_LIMITS exactly as before (500).
 *
 * Why: every staged buffered commit holds the global receipt seat clock for a time roughly linear in its record
 * count, and a waiting commit fails 55P03 once the queue ahead of it outlasts the 2 s lock_timeout in one lock stage.
 * Smaller batches shorten each hold. Uploaders that follow the published limit (desktop-core's
 * DeliveryLimits::with_published for default jobs, relayhistory sdk-ts exportLimitsFromPublished) prepare their NEXT
 * batches at min(advertised, 500); batches they already persisted keep their body and are retried unchanged.
 *
 * What it never changes: the accepted limit. MAX_DELIVERY_RECORDS (500, vendor/delivery) still gates prepare,
 * async admission, recovery and turns, so a lower advertised value never refuses or strands a batch.
 * The four publish sites read only the resolved value: GET /v1/delivery/limits, the queued and legacy-replay
 * receipts in app.ts, and deliver.ts's receipt.
 *
 * Parsing: a strict decimal integer 1..MAX_DELIVERY_RECORDS (no sign, no leading zero, no spaces). Anything else is
 * treated as unset and reported once (`invalid`), never a refusal: a bad advertisement must not take uploads down.
 */
import { DELIVERY_LIMITS, MAX_DELIVERY_RECORDS, type DeliveryLimits } from "./vendor/delivery/delivery-contracts.ts";

export type AdvertisedMaxRecords = { value?: number; invalid?: true };

export function parseAdvertisedMaxRecords(raw: string | undefined): AdvertisedMaxRecords {
  if (raw === undefined || raw === "") return {};
  if (!/^[1-9][0-9]{0,2}$/.test(raw)) return { invalid: true };
  const value = Number(raw);
  return value >= 1 && value <= MAX_DELIVERY_RECORDS ? { value } : { invalid: true };
}

/** The limits object to publish. Unset (or equal to the accepted limit) returns DELIVERY_LIMITS itself. */
export function advertisedDeliveryLimits(maxRecords: number | undefined): Readonly<DeliveryLimits> {
  if (maxRecords === undefined || maxRecords === DELIVERY_LIMITS.maxRecords) return DELIVERY_LIMITS;
  return Object.freeze({ ...DELIVERY_LIMITS, maxRecords });
}
