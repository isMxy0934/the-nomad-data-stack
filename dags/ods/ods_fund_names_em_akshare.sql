SELECT
  symbol,
  name,
  fund_type,
  pinyin_full,
  pinyin_short,
  CAST(STRPTIME(CAST(trade_date AS VARCHAR), '%Y%m%d') AS DATE) AS trade_date,
  '${PARTITION_DATE}' AS dt
FROM tmp_ods_fund_names_em_akshare;
