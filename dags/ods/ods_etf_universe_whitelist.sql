SELECT
  CAST(symbol AS VARCHAR) AS symbol,
  CAST(name AS VARCHAR) AS name,
  CAST(enabled AS INTEGER) AS enabled,
  CAST(STRPTIME(CAST(trade_date AS VARCHAR), '%Y%m%d') AS DATE) AS trade_date,
  '${PARTITION_DATE}' AS dt
FROM tmp_ods_etf_universe_whitelist;