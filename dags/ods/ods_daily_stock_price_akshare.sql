SELECT
  *,
  '${PARTITION_DATE}' AS dt
FROM tmp_ods_daily_stock_price_akshare;
