/**
 * The run's cost bound, from the request budget rather than from how fast the targets happen to answer.
 * Every lane refuses its (budget+1)-th HTTP request (aws.ts RequestMeter), so pricing every budgeted request
 * at the dearest S3 request type gives a ceiling the run cannot exceed. DynamoDB is bounded by the operation
 * caps of its cells (caps.ts), since one transaction can consume up to 200 write units. Prices are public
 * list prices in USD, rounded up.
 */
import { GROUP_SIZES, logOpCap, STREAM_COUNTS, TRANSACT_ITEMS, TRANSACT_WRITERS, HOT_HEAD_LEVELS, transactOpCap } from "./caps.ts";
import { LANES, type Lane, type Profile } from "./config.ts";

/** Lanes whose budgeted requests are S3 requests; the dynamo lane's are priced by write units instead. */
const S3_LANES = LANES.filter((lane) => lane !== "dynamo").length;

/** USD per unit: public list prices, rounded up. */
export const PRICE = {
  /** S3 Standard PUT/COPY/POST/LIST: $0.005 per 1,000. */
  s3Write: 0.005 / 1000,
  /** S3 Standard GET/HEAD: $0.0004 per 1,000. Named for the reader; the bound prices every request as a write. */
  s3Read: 0.0004 / 1000,
  /** One KMS call per request on a bucket without Bucket Keys: $0.03 per 10,000. */
  kmsCall: 0.03 / 10_000,
  /** DynamoDB on-demand write request unit, at the older $1.25 per million. */
  dynamoWriteUnit: 1.25 / 1_000_000,
} as const;

/** USD per GiB, public list prices. */
export const PRICE_GIB = {
  /** NAT gateway data processing (billed to the cluster's account): $0.045. */
  natProcessing: 0.045,
  /** Transfer between us-east-1 and us-west-2 (R1 only): $0.02. */
  interRegion: 0.02,
  /** S3 Express One Zone upload $0.0032 and retrieval $0.0006 (R4, R5 only), taken as the larger. */
  expressData: 0.0032,
} as const;
/**
 * What one budgeted GiB can cost at most. No lane pays all three (inter-region is R1's, Express is R4/R5's),
 * so their sum is an upper bound for every lane, rounded up to the cent.
 */
export const PRICE_PER_GIB = Math.ceil((PRICE_GIB.natProcessing + PRICE_GIB.interRegion + PRICE_GIB.expressData) * 100) / 100;

/**
 * What neither budget prices, rounded up: KMS key-months, a few GiB-days of storage, DynamoDB reads, the
 * requests of the credential hop, and each lane's one results copy (written outside the meter).
 */
export const FIXED_USD = 15;
/** The approved ceiling for the whole benchmark, every run included. */
export const APPROVED_CAP_USD = 300;

export interface CostBound {
  /** Lanes that make S3 requests. */
  s3_lanes: number;
  repeats: number;
  requests_max: number;
  /** Every budgeted request priced as an SSE-KMS PUT without Bucket Keys, plus every budgeted byte. */
  s3_and_kms_usd: number;
  dynamo_write_units_max: number;
  dynamo_usd: number;
  fixed_usd: number;
  total_usd: number;
}

/** The most write units the DynamoDB cells of one repeat can consume (a transactional write is 2 units per item). */
export function dynamoWriteUnitsMax(profile: Profile): number {
  let units = 0;
  for (const items of TRANSACT_ITEMS) units += TRANSACT_WRITERS.length * transactOpCap(profile, items) * items * 2;
  // PDDB: the head, the entry and one item per batch id.
  for (const group of GROUP_SIZES) units += STREAM_COUNTS.length * logOpCap(profile, group) * (group + 2) * 2;
  units += HOT_HEAD_LEVELS.length * profile.maxOpsPerCell;
  return units;
}

/** The most one run of one lane can cost: what `ops.ts run` adds to the spend ledger before it starts the lane. */
export function laneCostBound(profile: Profile, lane: Lane): number {
  const s3 = lane === "dynamo" ? 0 : profile.requestBudgetPerLane * (PRICE.s3Write + PRICE.kmsCall) + (profile.byteBudgetPerLane / 1024 ** 3) * PRICE_PER_GIB;
  let units = 0;
  if (lane === "dynamo") {
    for (const items of TRANSACT_ITEMS) units += TRANSACT_WRITERS.length * transactOpCap(profile, items) * items * 2;
    units += HOT_HEAD_LEVELS.length * profile.maxOpsPerCell;
  }
  if (lane === "r2") for (const group of GROUP_SIZES) units += STREAM_COUNTS.length * logOpCap(profile, group) * (group + 2) * 2;
  return Math.ceil((s3 + units * PRICE.dynamoWriteUnit) * 100) / 100;
}

export function costBound(profile: Profile, repeats: number, lanes: number = S3_LANES): CostBound {
  const requests = profile.requestBudgetPerLane * lanes * repeats;
  const bytes = (profile.byteBudgetPerLane / 1024 ** 3) * PRICE_PER_GIB * lanes * repeats;
  const s3 = requests * (PRICE.s3Write + PRICE.kmsCall) + bytes;
  const units = dynamoWriteUnitsMax(profile) * repeats;
  const dynamo = units * PRICE.dynamoWriteUnit;
  const round = (v: number) => Math.ceil(v * 100) / 100;
  return { s3_lanes: lanes, repeats, requests_max: requests, s3_and_kms_usd: round(s3), dynamo_write_units_max: units, dynamo_usd: round(dynamo), fixed_usd: FIXED_USD, total_usd: round(s3 + dynamo + FIXED_USD) };
}
