# ADR 001: Lambda vs Kappa Architecture

## Status
Accepted

## Context
The platform needs both low-latency scoring on incoming transactions and periodic batch work: historical aggregations, peer benchmarks, and scheduled ML retraining. It also maintains SCD2 customer history, which requires retrospective analysis at row level.

## Decision
Use a Lambda architecture with separate streaming and batch paths unified in Delta Lake. Kappa was rejected because a pure streaming model cannot natively support the row-level retrospective analysis that SCD2 behavioral history requires.

## Consequences
Two code paths to maintain, but both write to the same Delta tables so downstream consumers see one source of truth. The batch path owns retraining and benchmarks without competing with the streaming path for latency.