-- sar candidates are transactions the model scores over the review threshold
-- rule triggers for high value cross border and velocity bursts do not fire at this data scale so the list is model driven
SELECT
  transaction_id,
  customer_id,
  amount,
  channel,
  geo_country,
  is_cross_border,
  txn_count_1h,
  round(velocity_score, 4) AS velocity_score,
  round(amount_zscore_30d, 4) AS amount_zscore_30d,
  round(model_distress_score, 4) AS fraud_score
FROM sentinel_adb.sentinel.fct_transaction_health
WHERE model_distress_score >= 0.5
ORDER BY model_distress_score DESC, amount DESC