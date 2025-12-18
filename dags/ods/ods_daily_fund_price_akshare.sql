SELECT
  symbol,
  name,
  close,
  high,
  low,
  vol,
  amount,
  pre_close,
  pct_chg,
  CAST(STRPTIME(trade_date, '%Y%m%d') AS DATE) AS trade_date,
  '${PARTITION_DATE}' AS dt
FROM tmp_ods_daily_fund_price_akshare;
