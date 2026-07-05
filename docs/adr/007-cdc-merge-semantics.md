# ADR 007: CDC Merge Semantics

## Status
Accepted

## Context
Source events carry an op_type of insert, update, or delete, and a change_ts. Events can arrive out of order, so a late-arriving older event must not overwrite newer state.

## Decision
The Delta MERGE branches on op_type explicitly. A delete closes the current SCD2 row, an update opens a new version, and an unmatched key inserts. Out-of-order events are guarded by a change_ts comparison so an older event cannot overwrite a newer one.

## Consequences
Change data is modeled correctly rather than blind-upserted. The ordering guard is a real correctness property tested with a deliberately late event. The term CDC is used only because the op_type and ordering logic is present, not as a relabel of a plain upsert.