/** The matrix dimensions and per-cell operation caps that both the steps and the cost bound (cost.ts) use. */
import type { Profile } from "./config.ts";

/** Batches per group for the log-commit protocols. */
export const GROUP_SIZES = [1, 4, 16, 30];
/** 1 stream is a hot user; 64 is many users. */
export const STREAM_COUNTS = [1, 64];
export const TRANSACT_ITEMS = [2, 25, 100];
export const TRANSACT_WRITERS = [1, 8];
export const HOT_HEAD_LEVELS = [64, 128, 256, 512];

/** A group of 30 is a 1.9 MiB PUT; the cap keeps a cell under about 0.75 GiB written. */
export const logOpCap = (profile: Profile, group: number) => Math.min(profile.maxOpsPerCell, Math.ceil(12_000 / group));

/**
 * A latency cell on objects of `kib` KiB stops after about 0.75 GiB: 12,000 operations at 64 KiB, 3,000 at
 * 256 KiB. A fast backend (S3 Express) otherwise spends the lane's byte budget in step 1 and never reaches
 * its later steps. Small objects keep the profile's cap.
 */
export const objectOpCap = (profile: Profile, kib: number) => Math.min(profile.maxOpsPerCell, kib < 64 ? profile.maxOpsPerCell : Math.ceil((12_000 * 64) / kib));

/** A 100-item transaction is 200 write units; the cap keeps a cell near a million units. */
export const transactOpCap = (profile: Profile, items: number) => Math.min(profile.maxOpsPerCell, Math.ceil(600_000 / items));
