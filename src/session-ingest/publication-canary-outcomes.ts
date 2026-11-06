/** Append-only, fsync-backed canary evidence. Contains metadata only. */
import { createHash } from "node:crypto";
import { constants } from "node:fs";
import {
  lstat,
  open,
  realpath,
  unlink,
  type FileHandle,
} from "node:fs/promises";
import { basename, dirname, isAbsolute, resolve } from "node:path";
import type {
  PublicationCanaryOutcome,
  PublicationCanaryOutcomeStore,
  PublicationCanaryPlan,
  PublicationCanarySummary,
} from "./publication-canary.ts";

interface LedgerPayload {
  kind: "begin" | "outcome" | "finish";
  planDigest: string;
  plan?: PublicationCanaryPlan;
  outcome?: PublicationCanaryOutcome;
  summary?: PublicationCanarySummary;
}
interface LedgerLine {
  version: 1;
  sequence: number;
  previousSha256: string;
  payload: LedgerPayload;
  sha256: string;
}
const HEX = /^[a-f0-9]{64}$/u;
const fail = (code: string) => Object.assign(new Error(code), { code });
const hashLine = (line: Omit<LedgerLine, "sha256">) =>
  createHash("sha256").update(JSON.stringify(line)).digest("hex");
const syncDirectory = async (path: string) => {
  const directory = await open(path, constants.O_RDONLY);
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
};

export interface FilePublicationCanaryOutcomeStore
  extends PublicationCanaryOutcomeStore {
  close(): Promise<void>;
  readonly path: string;
}

/**
 * Acquires `<path>.lock` exclusively for the whole run. A stale lock is an
 * operator-visible hold; this code never guesses that another process is dead.
 */
export async function openPublicationCanaryOutcomeFile(
  requestedPath: string,
): Promise<FilePublicationCanaryOutcomeStore> {
  if (!isAbsolute(requestedPath)) throw fail("publication_canary_outcome_path");
  const parent = await realpath(dirname(requestedPath));
  const path = resolve(parent, basename(requestedPath));
  if (dirname(path) !== parent) throw fail("publication_canary_outcome_path");
  const lockPath = `${path}.lock`;
  let lock: FileHandle;
  try {
    lock = await open(
      lockPath,
      constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY,
      0o600,
    );
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST")
      throw fail("publication_canary_outcome_locked");
    throw fail("publication_canary_outcome_path");
  }
  try {
    await lock.writeFile(
      JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() }) +
        "\n",
    );
    await lock.sync();
  } catch (error) {
    await lock.close().catch(() => {});
    await unlink(lockPath).catch(() => {});
    throw error;
  }
  let file: FileHandle | undefined;
  let closed = false;
  try {
    try {
      const stat = await lstat(path);
      if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o077) !== 0)
        throw fail("publication_canary_outcome_path");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    file = await open(
      path,
      constants.O_CREAT | constants.O_APPEND | constants.O_RDWR,
      0o600,
    );
    await syncDirectory(parent);
    const text = await file.readFile({ encoding: "utf8" });
    const lines: LedgerLine[] = [];
    if (text) {
      if (!text.endsWith("\n")) throw fail("publication_canary_outcome_torn");
      for (const raw of text.slice(0, -1).split("\n")) {
        let line: LedgerLine;
        try {
          line = JSON.parse(raw) as LedgerLine;
        } catch {
          throw fail("publication_canary_outcome_parse");
        }
        const previous = lines.at(-1)?.sha256 ?? "0".repeat(64);
        const unsigned = {
          version: line.version,
          sequence: line.sequence,
          previousSha256: line.previousSha256,
          payload: line.payload,
        };
        if (
          line.version !== 1 ||
          line.sequence !== lines.length ||
          line.previousSha256 !== previous ||
          !HEX.test(line.sha256) ||
          hashLine(unsigned) !== line.sha256
        )
          throw fail("publication_canary_outcome_chain");
        lines.push(line);
      }
    }
    let begun: LedgerPayload | undefined = lines.find(
      (line) => line.payload.kind === "begin",
    )?.payload;
    let finished: PublicationCanarySummary | undefined = lines.find(
      (line) => line.payload.kind === "finish",
    )?.payload.summary;
    const outcomes = new Map<string, PublicationCanaryOutcome>();
    for (const line of lines) {
      const outcome = line.payload.outcome;
      if (line.payload.kind !== "outcome" || !outcome) continue;
      const prior = outcomes.get(outcome.stageToken);
      if (prior && JSON.stringify(prior) !== JSON.stringify(outcome))
        throw fail("publication_canary_outcome_conflict");
      outcomes.set(outcome.stageToken, outcome);
    }
    const append = async (payload: LedgerPayload) => {
      if (!file || closed) throw fail("publication_canary_outcome_closed");
      const unsigned = {
        version: 1 as const,
        sequence: lines.length,
        previousSha256: lines.at(-1)?.sha256 ?? "0".repeat(64),
        payload,
      };
      const line: LedgerLine = { ...unsigned, sha256: hashLine(unsigned) };
      const encoded = Buffer.from(JSON.stringify(line) + "\n", "utf8");
      let offset = 0;
      while (offset < encoded.byteLength) {
        const remaining = encoded.byteLength - offset;
        const { bytesWritten } = await file.write(encoded, offset, remaining);
        if (
          !Number.isSafeInteger(bytesWritten) ||
          bytesWritten <= 0 ||
          bytesWritten > remaining
        )
          throw fail("publication_canary_outcome_write");
        offset += bytesWritten;
      }
      await file.sync();
      lines.push(line);
    };
    return {
      path,
      async begin(planDigest, plan) {
        if (begun) {
          if (
            begun.planDigest !== planDigest ||
            JSON.stringify(begun.plan) !== JSON.stringify(plan)
          )
            throw fail("publication_canary_outcome_plan_conflict");
          return;
        }
        const payload: LedgerPayload = {
          kind: "begin",
          planDigest,
          plan,
        };
        await append(payload);
        begun = payload;
      },
      async existing(planDigest) {
        if (begun?.planDigest !== planDigest)
          throw fail("publication_canary_outcome_plan_conflict");
        return new Map(outcomes);
      },
      async record(planDigest, outcome) {
        if (begun?.planDigest !== planDigest || finished)
          throw fail("publication_canary_outcome_state");
        const prior = outcomes.get(outcome.stageToken);
        if (prior) {
          if (JSON.stringify(prior) !== JSON.stringify(outcome))
            throw fail("publication_canary_outcome_conflict");
          return;
        }
        await append({ kind: "outcome", planDigest, outcome });
        outcomes.set(outcome.stageToken, outcome);
      },
      async finish(planDigest, summary) {
        if (begun?.planDigest !== planDigest)
          throw fail("publication_canary_outcome_plan_conflict");
        if (finished) {
          if (JSON.stringify(finished) !== JSON.stringify(summary))
            throw fail("publication_canary_outcome_conflict");
          return;
        }
        await append({ kind: "finish", planDigest, summary });
        finished = summary;
      },
      async close() {
        if (closed) return;
        closed = true;
        await file?.close();
        await lock.close();
        await unlink(lockPath);
        await syncDirectory(parent);
      },
    };
  } catch (error) {
    await file?.close().catch(() => {});
    await lock.close().catch(() => {});
    await unlink(lockPath).catch(() => {});
    throw error;
  }
}
