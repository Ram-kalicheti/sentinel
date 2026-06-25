-- Intermediate: row-grain enriched transactions.
-- Materialized INCREMENTAL with a MERGE on the idempotent grain (transaction_id, change_ts),
-- so a second run only processes rows newer than the current watermark (_processed_ts).
--
-- Only PER-ROW derived features live here. Rolling-window "trend" aggregation is owned by the
-- Day 5 mart (fct_transaction_health): a window function over an incremental slice cannot see
-- rows outside the slice, so doing it here would be silently wrong. This separation is deliberate.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['transaction_id', 'change_ts'],
    file_format='delta'
) }}

with txns as (

    select * from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    -- only rows processed after the newest row already in this table
    where _processed_ts > (select coalesce(max(_processed_ts), timestamp '1900-01-01') from {{ this }})
    {% endif %}

),

dim as (

    -- current customer version only (is_current). Deleted/closed customers -> null customer_sk.
    -- The rigorous point-in-time (as-of) join on effective dates is the Day 5 refinement.
    select customer_id, customer_sk, risk_segment, home_country
    from delta.`{{ var('dim_customers_path') }}`
    where is_current = true

)

select
    t.transaction_id,
    t.change_ts,
    t.customer_id,
    d.customer_sk,
    d.risk_segment,
    t.amount,
    t.channel,
    t.geo_country,
    t.is_new_payee,
    hour(t.txn_ts)                                            as txn_hour,
    (t.amount >= 10000)                                       as is_high_value,
    (d.home_country is not null and t.geo_country <> d.home_country) as is_cross_border,
    t.is_fraud,
    t.txn_ts,
    t._processed_ts
from txns t
left join dim d
    on t.customer_id = d.customer_id
