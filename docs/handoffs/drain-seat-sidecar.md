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

## How the export proves a seat

The export reads the seats with `SEAT_PAGE`.
`SEAT_PAGE` runs in the same read-only snapshot as the verdict.
It is a read-only copy of `sessions.ingest_lazy_stage_seats`, its guard `sessions.ingest_lazy_guard`, and `sessions.ingest_receipt_seat_records`.
Those functions take locks and raise, so the export cannot call them.
Each case where they raise becomes a failed flag or a short count.

A seat is given only under the rules of `acceptedReceiptSeat` in `publication-seats.ts`.
A line whose body was not accepted is not kept and never gets a seat.
The seat's first digest must equal the line's digest.
The reset epoch and fence epoch must equal the publication head's.

Any doubt means refuse.
A refused live stage gets no sidecar.
Its code is counted in `failed_codes`, and its org stays incomplete.
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

1. Turn on the drain opt-in first, everywhere the drain runs.
   A drain without it refuses every v3 sidecar.
2. Then turn on the export opt-in.
3. Watch `failed_codes` for the `seat_*` codes after the first export.
   Any of them leaves the org incomplete until the cause is fixed.
4. Each step needs the owner's explicit OK.

Turning the export opt-in off again is safe.
The next export replaces each v3 sidecar with v2, and the drain reads both.

## What was not tested

- Real PostgreSQL. The SQL ran only in PGlite, with the migrations through `20261105047020`.
- Real S3. The object store was the local file store.
- The command-line scripts, since they do not pass the opt-ins.
