-- row level security on the fraud mart
-- an authorised analyst allow-listed by principal sees every row including sar candidates
-- all other users are row-filtered away from high risk transactions to limit exposure

CREATE OR REPLACE FUNCTION sentinel_adb.sentinel.rls_high_risk(risk_segment STRING)
  RETURN current_user() = 'sitharamkalicheti@zohomail.com' OR risk_segment <> 'high';

ALTER TABLE sentinel_adb.sentinel.fct_transaction_health
  SET ROW FILTER sentinel_adb.sentinel.rls_high_risk ON (risk_segment);