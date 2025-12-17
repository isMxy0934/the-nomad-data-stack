SELECT
  *,
  '${PARTITION_DATE}' AS dt
FROM tmp_ods_daily_fund_price_akshare;
