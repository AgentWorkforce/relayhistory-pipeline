# Drain seat sidecar (v3)

## What it does

The accepted-stage export can write a v3 sidecar.
A v3 sidecar is the v2 verdict plus one receipt seat for each kept line.
The buffered drain can read a v3 sidecar.
It then writes each kept line with its seat, as `accepted_seq`.
The seated line has the same bytes the publication path writes for that line and seat.

Both parts are code only.
Both are off by default.
Nothing is deployed and no flag is turned on anywhere.
There is no new migration and no new SQL function.

## The two opt-ins

- Export: `ExportOptions.seatSidecar` in `src/session-ingest/accepted-stage-export.ts`.
- Drain: `DrainOptions.acceptSeatSidecar` in `src/session-ingest/buffered-drain.ts`.

Both are library options.
Each must be a boolean when given; anything else throws (`export_seat_sidecar_invalid`, `drain_accept_seat_sidecar_invalid`).
The command-line scripts do not pass them yet.

## With the opt-ins off

The export writes the same v2 bytes as before.
It runs the same queries and never reads the seat query.
The drain refuses a v3 sidecar exactly as it refuses an unknown version.
That outcome is `accepted_mismatch`, and nothing is written.
A v2 sidecar drains to the same unseated lines whether the drain opt-in is on or off.
An export with the opt-in off still reads an existing v3 sidecar, so it can replace it with v2.

## How the export proves a seat

The export reads the seats with `SEAT_PAGE`.
`SEAT_PAGE` runs in the same read-only snapshot as the verdict.
It is a read-only copy of `sessions.ingest_lazy_stage_seats`, its guard `sessions.ingest_lazy_guard`, and `sessions.ingest_receipt_seat_records`.
Those functions take locks and raise, so the export cannot call them.
Each case where they raise becomes a failed flag or a short count.

The export opt-in needs the lazy custody policy (a row in `sessions.ingest_lazy_policy`).
Without one, every live stage is refused with `seat_not_lazy` and gets no sidecar.

A seat is given only under the rules of `acceptedReceiptSeat` in `publication-seats.ts`.
Under the lazy policy, a line whose body was not accepted is not kept and never gets a seat.
The seat's first digest must equal the line's digest.
The reset epoch and fence epoch must equal the publication head's.

Any doubt means refuse.
A refused live stage gets no sidecar.
Its code is counted in `failed_codes`, and its org stays incomplete.
In the summary, such a stage counts in both `live` and `failed`, as a `no_head_parity_unlisted` refusal does.
A sidecar an earlier export wrote for that stage is left as it is.
So an earlier v2 sidecar can still drain, unseated.
The codes are `seat_row`, `seat_not_lazy`, `seat_precondition`, `seat_missing`, `seat_scope`, `seat_duplicate`, and the `publication_seats_*` codes.
A generation admitted only through an open overlap window is refused.
A stage that is not live gets a v3 sidecar with no seats.

## How the drain checks a v3 sidecar

The drain refuses the whole sidecar (`accepted_mismatch`) when any seat is:

- missing or extra;
- for a line that is not kept;
- not a canonical positive integer string (no leading zero, no exponent, not zero, not above 2^63-1);
- used twice;
- given with a digest that is not the line's digest.

It also refuses a sidecar that gives one record revision twice among its kept lines.
It also refuses a line that already carries a seat or names another origin.

The drain fits the line first, then appends the seat with `injectReceiptSeat`.
The revision index keeps the digest of the unseated line, as publication does.

## Limits

There is no retro-seating.
A line already stored at an equal revision stays `already_stored`.
If that stored line has no seat, it keeps no seat.
If it has a seat, that seat is kept, even when the sidecar gives another.
So one revision never gets two seats from the drain.

Lines drained before the opt-ins are on stay unseated.
Other writers still write unseated lines: the inline canonical path, the staging fan-out, and the courier.
Readers fold an unseated copy and a seated copy of one revision to the one seat.
Two copies with different seats are refused by the reader (`relayhistory_acceptance_conflict`).

## Before turning it on

1. Confirm the lazy custody policy is in place where the export runs.
   Without it, the export opt-in refuses every live stage (`seat_not_lazy`).
2. Turn on the drain opt-in first, everywhere the drain runs.
   This applies to every drain build and every host that runs a drain.
   A drain without it refuses every v3 sidecar.
3. Then turn on the export opt-in.
4. Watch `failed_codes` for the `seat_*` codes after the first export.
   Any of them leaves the org incomplete until the cause is fixed.
5. Each step needs the owner's explicit OK.

The export and the drain run from this repository's ingest tooling.
So a build that carries these options also carries every other change on the branch it is built from that the running ingest build does not have yet.
Compare the running build with the build to be shipped before enabling.

## Turning it off

To roll back, turn the export option off first, let one export rewrite v2, then turn the drain option off.
That export replaces the v3 sidecar of each stage it still exports with v2, and the drain reads both.

Do not turn the drain option off while v3 sidecars exist.
A drain without the opt-in, or an older drain build, meets a v3 sidecar as an unknown version.
Each such stage is `accepted_mismatch`, and nothing is written.
That outcome is final for that export generation: a checkpoint may pass it, so a run resumed from that checkpoint does not retry it.
The user's hold entry for that stage is released first, so later batches of that user can drain ahead of it.

## What was not tested

- Real PostgreSQL. The SQL ran only in PGlite, with the migrations through `20261105047020`.
- Real S3. The object store was the local file store.
- The command-line scripts, since they do not pass the opt-ins.
- A deployment without the lazy custody policy, beyond the unit check that gives `seat_not_lazy`.
- The rollback order, and an older drain build meeting a v3 sidecar. These follow from reading the drain code; no test runs them.
