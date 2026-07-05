# ADR 005: Idempotency and Exactly-Once Recovery

## Status
Accepted

## Context
A production pipeline must be replay-safe. A re-run after a failure cannot be allowed to duplicate or drop rows, and a single malformed record cannot be allowed to crash the job.

## Decision
Bronze streaming uses an explicit checkpoint with Kafka auto-commit disabled for exactly-once delivery into Delta. The silver MERGE is keyed on a natural key plus sequence key so re-running a batch is a no-op on already-applied rows. Records that fail validation route to silver_deadletter with error type and detail rather than failing the job.

## Consequences
Correctness and recoverability are favored over raw throughput at this data scale. Replaying a completed batch produces identical row counts with zero duplicates, which was verified by re-running validation and confirming the silver count held. The deadletter table is append-only, so reconciliation is keyed on distinct source offset rather than raw row count.