# ADR 008: Unity Catalog Governance on Standard Tier

## Status
Accepted

## Context
The platform needs data governance: column-level classification, restricted access to sensitive fraud data, and data lineage. Automated column-level lineage graphs are a Premium-tier feature, and this workspace runs on the Standard tier.

## Decision
Govern the data with the capabilities available on Standard tier. Apply Unity Catalog column and table tags for PII classification, enforce a row-level security policy on the sensitive mart, and document lineage with a manually authored diagram in docs/lineage.md alongside this record stating production intent.

## Consequences
The governance intent is met without Premium features, and the project does not present the manual lineage diagram as auto-generated. One honest deviation: the row-level security policy allow-lists by current_user() rather than an account group, because the identity tenant used for this build could not create the account-level group. On a production tenant this would be a group-based grant. This is the same honest mitigation pattern used for the Consumption-tier limitation documented in the Meridian project.