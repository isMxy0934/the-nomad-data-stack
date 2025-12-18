SELECT
  date AS trade_date,
  code AS symbol,
  open,
  close,
  volume,
  '${PARTITION_DATE}' AS dt
FROM tmp_ods_daily_stock_price_akshare;
