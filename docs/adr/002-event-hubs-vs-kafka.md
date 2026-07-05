# ADR 002: Azure Event Hubs vs Kafka on Docker

## Status
Accepted

## Context
The pipeline needs a durable, partitioned event ingress. Self-hosting Kafka on Docker means managing brokers, ZooKeeper, and partition rebalancing.

## Decision
Use Azure Event Hubs on the Standard tier, which exposes the native Kafka protocol. This removes broker and ZooKeeper management and keeps the stack Azure-native and consistent with the rest of the platform.

## Consequences
The producer and consumer code uses the standard Kafka protocol and concepts, so the skills and design transfer directly to a Confluent or self-hosted Kafka deployment. The tradeoff is a managed-service dependency in place of full broker-level control.