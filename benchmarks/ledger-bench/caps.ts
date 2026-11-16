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

/** The 256 KiB cells: about 2 GiB each at most. */
export const bigObjectOpCap = (profile: Profile) => Math.min(profile.maxOpsPerCell, 8_000);

/** A 100-item transaction is 200 write units; the cap keeps a cell near a million units. */
export const transactOpCap = (profile: Profile, items: number) => Math.min(profile.maxOpsPerCell, Math.ceil(600_000 / items));
