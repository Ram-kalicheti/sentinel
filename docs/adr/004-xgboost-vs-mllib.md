# ADR 004: XGBoost vs Spark MLlib

## Status
Accepted

## Context
The training dataset at this stage fits comfortably in memory on a single node. Distributed training adds coordination overhead that is only justified when the data exceeds single-node memory.

## Decision
Train with scikit-learn XGBoost rather than Spark MLlib. XGBoost integrates natively with MLflow autologging, and distributed MLlib would add overhead without benefit at this data scale.

## Consequences
Training is simple and fast, and experiments are tracked in MLflow. The dev-scale model produced weak results and did not clear the promotion gate, so the champion alias was deliberately left unset. Moving to distributed training would be a revisit point once data volume exceeds single-node memory.