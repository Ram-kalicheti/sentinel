# ADR 006: Spark Partition Tuning and Delta Z-Order

## Status
Accepted

## Context
The default of 200 shuffle partitions is wasteful at dev data volume, and a join between the large fact and the small customer dimension shuffles both sides unnecessarily.

## Decision
Set spark.sql.shuffle.partitions explicitly to a value suited to dev volume instead of the 200 default. Broadcast the small dimension to convert the shuffle join into a broadcast join. Run OPTIMIZE with ZORDER BY on customer_id on the silver and gold tables for point-lookup scan performance.

## Consequences
The documented value is the mechanism, not just the numbers: the tuning removes an unnecessary shuffle and improves scan locality for point lookups. Before-and-after wall-clock timing was captured as supporting evidence. The mechanism is what transfers to a larger dataset, where the specific partition count would be retuned.