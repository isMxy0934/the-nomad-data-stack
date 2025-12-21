SELECT
  CAST(STRPTIME(CAST(trade_date AS VARCHAR), '%Y-%m-%d') AS DATE) AS trade_date,
  '${PARTITION_DATE}' AS dt
FROM tmp_ods_tool_trade_date_hist_sina_akshare;