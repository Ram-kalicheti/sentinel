# ADR 003: SCD Type 2 Customer History

## Status
Accepted

## Context
Fraud scoring depends on how a customer's behavioral state changes over time, and regulatory audit requires a full history of that state. SCD Type 1 overwrites prior values and destroys that history.

## Decision
Model the customer dimension as SCD Type 2 with effective_start, effective_end, and is_current. Delta MERGE performs the atomic upsert that closes the prior version and opens the new one.

## Consequences
The dimension grows over time and queries must filter on is_current for the present state, but the full behavioral history needed for retrospective model analysis and audit is preserved.