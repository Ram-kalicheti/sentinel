-- score distribution buckets show the shape a drift monitor tracks formal psi runs in the ml eval job
SELECT
  CASE
    WHEN model_distress_score < 0.1 THEN '0.0-0.1'
    WHEN model_distress_score < 0.2 THEN '0.1-0.2'
    WHEN model_distress_score < 0.3 THEN '0.2-0.3'
    WHEN model_distress_score < 0.4 THEN '0.3-0.4'
    WHEN model_distress_score < 0.5 THEN '0.4-0.5'
    WHEN model_distress_score < 0.6 THEN '0.5-0.6'
    WHEN model_distress_score < 0.7 THEN '0.6-0.7'
    WHEN model_distress_score < 0.8 THEN '0.7-0.8'
    WHEN model_distress_score < 0.9 THEN '0.8-0.9'
    ELSE '0.9-1.0'
  END AS score_band,
  count(*) AS txn_count
FROM sentinel_adb.sentinel.fct_transaction_health
GROUP BY score_band
ORDER BY score_band