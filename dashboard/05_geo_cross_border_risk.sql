-- geographic and cross border risk exposure by country and corridor is the aml cut banks track
SELECT
  geo_country,
  is_cross_border,
  count(*) AS txn_count,
  sum(amount) AS total_amount,
  round(avg(model_distress_score), 4) AS avg_fraud_score,
  sum(CASE WHEN model_distress_score >= 0.5 THEN 1 ELSE 0 END) AS flagged_count
FROM sentinel_adb.sentinel.fct_transaction_health
GROUP BY geo_country, is_cross_border
ORDER BY flagged_count DESC, txn_count DESC