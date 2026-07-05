-- fraud alert queue highest model distress first so review works the riskiest transactions
SELECT
  transaction_id,
  customer_id,
  amount,
  channel,
  geo_country,
  risk_segment,
  is_cross_border,
  round(model_distress_score, 4) AS fraud_score,
  scored_ts
FROM sentinel_adb.sentinel.fct_transaction_health
ORDER BY model_distress_score DESC
LIMIT 20