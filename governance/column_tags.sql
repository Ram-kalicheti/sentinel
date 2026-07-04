-- unity catalog column tags for pii and model output on the governed marts
-- tags applied to real columns only - the synthetic source has no account_no or ssn_hash
-- so those production pii fields are documented in docs/lineage.md rather than tagged on absent columns

ALTER TABLE sentinel_adb.sentinel.fct_transaction_health
  ALTER COLUMN customer_id SET TAGS ('pii' = 'true', 'classification' = 'identifier');

ALTER TABLE sentinel_adb.sentinel.fct_transaction_health
  ALTER COLUMN model_distress_score SET TAGS ('data_class' = 'model-output', 'model' = 'fraud_classifier');

ALTER TABLE sentinel_adb.sentinel.fct_transaction_health
  ALTER COLUMN risk_score SET TAGS ('data_class' = 'derived-heuristic');

ALTER TABLE sentinel_adb.sentinel.fct_transaction_health
  ALTER COLUMN risk_segment SET TAGS ('data_class' = 'sensitive');

ALTER TABLE sentinel_adb.sentinel.dim_customers_snapshot
  ALTER COLUMN customer_id SET TAGS ('pii' = 'true', 'classification' = 'identifier');

ALTER TABLE sentinel_adb.sentinel.dim_customers_snapshot
  ALTER COLUMN home_country SET TAGS ('pii' = 'true', 'classification' = 'location');

ALTER TABLE sentinel_adb.sentinel.dim_customers_snapshot
  ALTER COLUMN account_open_date SET TAGS ('pii' = 'true', 'classification' = 'account-metadata');