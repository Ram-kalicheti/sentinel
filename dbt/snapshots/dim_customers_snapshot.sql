{% snapshot dim_customers_snapshot %}
{{
  config(
    target_schema='sentinel',
    unique_key='customer_id',
    strategy='check',
    check_cols=['risk_segment', 'home_country', 'account_open_date'],
    file_format='delta',
    invalidate_hard_deletes=True
  )
}}
select
    customer_id,
    risk_segment,
    home_country,
    account_open_date,
    updated_at
from delta.`{{ var('customer_master_path') }}`
{% endsnapshot %}