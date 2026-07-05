-- promote fraud labels from immutable bronze into a governed table
-- filtered to the scored population so model quality is measured on exactly the mart transactions
-- run on the all purpose cluster which holds the adls key that the sql warehouse cannot use to read raw bronze
CREATE OR REPLACE TABLE sentinel_adb.sentinel.transaction_labels AS
SELECT
  b.transaction_id,
  max(cast(b.is_fraud AS int)) AS actual_fraud
FROM delta.`abfss://bronze@sentinelstgrk1.dfs.core.windows.net/bronze_transactions` b
JOIN (SELECT DISTINCT transaction_id FROM sentinel_adb.sentinel.fct_transaction_health) m
  ON b.transaction_id = m.transaction_id
WHERE b.transaction_id IS NOT NULL
GROUP BY b.transaction_id