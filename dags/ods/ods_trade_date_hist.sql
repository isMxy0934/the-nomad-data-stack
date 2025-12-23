SELECT
  CAST(STRPTIME(CAST(trade_date AS VARCHAR), '%Y-%m-%d') AS DATE) AS trade_date,
  '${PARTITION_DATE}' AS dt
FROM tmp_ods_trade_date_hist;
