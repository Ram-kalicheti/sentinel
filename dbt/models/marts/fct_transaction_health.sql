{{
  config(
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key='transaction_health_sk'
  )
}}

with txns as (
    select
        transaction_id,
        cast(change_ts as timestamp) as change_ts,
        customer_id,
        amount,
        cast(txn_ts as timestamp) as txn_ts,
        channel,
        geo_country,
        is_cross_border,
        txn_hour,
        is_high_value
    from {{ ref('int_transaction_trends') }}
    {% if is_incremental() %}
    -- 30-day lookback so the rolling windows see prior rows, not just new ones
    where cast(txn_ts as timestamp) >= (
        select coalesce(max(txn_ts), timestamp '1900-01-01') - interval 30 days
        from {{ this }}
    )
    {% endif %}
),

dim as (
    -- floor each customer's earliest version to the start of time so a fact that
    -- predates the dimension's first recorded version still resolves to it
    select
        customer_sk,
        customer_id,
        risk_segment,
        case
            when effective_start = min(effective_start) over (partition by customer_id)
            then timestamp '1900-01-01'
            else effective_start
        end as effective_start,
        effective_end
    from delta.`{{ var('dim_customers_path') }}`
),

joined as (
    -- as-of join - dimension version effective at txn_ts, not the current row
    select t.*, d.customer_sk, d.risk_segment
    from txns t
    left join dim d
           on t.customer_id = d.customer_id
          and t.txn_ts >= d.effective_start
          and (t.txn_ts < d.effective_end or d.effective_end is null)
),

windowed as (
    -- cast to long gives epoch seconds so the range frames count by elapsed time
    select
        *,
        count(*) over (
            partition by customer_id order by cast(txn_ts as long)
            range between 3600 preceding and current row
        ) as txn_count_1h,
        avg(amount) over (
            partition by customer_id order by cast(txn_ts as long)
            range between 2592000 preceding and current row
        ) as amount_avg_30d,
        stddev(amount) over (
            partition by customer_id order by cast(txn_ts as long)
            range between 2592000 preceding and current row
        ) as amount_std_30d,
        avg(amount) over (partition by risk_segment) as peer_avg_amount
    from joined
)

select
    md5(concat(transaction_id, '|', cast(change_ts as string))) as transaction_health_sk,
    transaction_id,
    change_ts,
    customer_id,
    customer_sk,
    risk_segment,
    amount,
    txn_ts,
    channel,
    geo_country,
    is_cross_border,
    txn_hour,
    is_high_value,
    txn_count_1h,
    case when amount_std_30d is null or amount_std_30d = 0 then 0.0
         else (amount - amount_avg_30d) / amount_std_30d end as amount_zscore_30d,
    cast(txn_count_1h as double) as velocity_score,
    amount - peer_avg_amount as peer_deviation,
    round(least(1.0, greatest(0.0,
        0.5 * (case when amount_std_30d is null or amount_std_30d = 0 then 0.0
                    else abs((amount - amount_avg_30d) / amount_std_30d) end) / 3.0
      + 0.5 * (case when is_high_value then 1.0 else 0.0 end)
    )), 4) as risk_score,
    -- model_distress_score is written back by the scoring job - null until then
    cast(null as double) as model_distress_score,
    current_timestamp() as scored_ts
from windowed