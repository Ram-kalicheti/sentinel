-- model quality confusion matrix at the review threshold
-- labels come from the governed transaction_labels table promoted from bronze see 00_transaction_labels
-- precision and recall are dev scale the promotion gate refused the champion alias on these results
WITH scored AS (
  SELECT
    f.transaction_id,
    CASE WHEN f.model_distress_score >= 0.5 THEN 1 ELSE 0 END AS predicted_fraud,
    l.actual_fraud
  FROM sentinel_adb.sentinel.fct_transaction_health f
  JOIN sentinel_adb.sentinel.transaction_labels l
    ON f.transaction_id = l.transaction_id
)
SELECT
  sum(CASE WHEN predicted_fraud = 1 AND actual_fraud = 1 THEN 1 ELSE 0 END) AS true_positive,
  sum(CASE WHEN predicted_fraud = 1 AND actual_fraud = 0 THEN 1 ELSE 0 END) AS false_positive,
  sum(CASE WHEN predicted_fraud = 0 AND actual_fraud = 1 THEN 1 ELSE 0 END) AS false_negative,
  sum(CASE WHEN predicted_fraud = 0 AND actual_fraud = 0 THEN 1 ELSE 0 END) AS true_negative,
  round(sum(CASE WHEN predicted_fraud = 1 AND actual_fraud = 1 THEN 1 ELSE 0 END) / nullif(sum(predicted_fraud), 0), 4) AS precision,
  round(sum(CASE WHEN predicted_fraud = 1 AND actual_fraud = 1 THEN 1 ELSE 0 END) / nullif(sum(actual_fraud), 0), 4) AS recall,
  count(*) AS rows_evaluated
FROM scored