-- Staging: a clean, typed, 1:1 pass over silver_transactions.
-- No business logic here (dbt convention). Source is path-based Delta because the
-- Hive metastore is disabled on this workspace, so we read it via  delta.`abfss://...`.
{{ config(materialized='view') }}

with source as (

    select * from delta.`{{ var('silver_transactions_path') }}`

)

select
    transaction_id,
    customer_id,
    cast(amount as decimal(12,2))   as amount,
    currency,
    lower(channel)                  as channel,
    merchant_category,
    txn_ts,
    geo_country,
    is_new_payee,
    op_type,
    change_ts,
    is_fraud,
    _processed_ts
from source
where _dq_passed = true
