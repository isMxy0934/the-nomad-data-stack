SELECT
  symbol,
  name,
  enabled,
  'WHITELIST' AS flag,
  trade_date,
  '${PARTITION_DATE}' AS dt
FROM ods.ods_etf_universe_whitelist where dt = '${PARTITION_DATE}';
