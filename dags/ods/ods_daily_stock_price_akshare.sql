SELECT
  CAST(symbol AS VARCHAR) AS symbol,
  CAST(name   AS VARCHAR) AS name,
  CAST(open   AS DOUBLE)  AS open,
  CAST(high   AS DOUBLE)  AS high,
  CAST(low    AS DOUBLE)  AS low,
  CAST(close  AS DOUBLE)  AS close,
  CAST(vol    AS BIGINT)  AS vol,
  '${PARTITION_DATE}' AS dt
FROM tmp_ods_daily_stock_price_akshare;
