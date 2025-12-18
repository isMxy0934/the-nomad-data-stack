WITH source AS (
    SELECT
        symbol,
        trade_date,
        open,
        close
    FROM ods.ods_daily_stock_price_akshare_dt('${PARTITION_DATE}')
)
SELECT
    symbol,
    trade_date,
    AVG(open) AS open_avg,
    AVG(close) AS close_avg,
    '${PARTITION_DATE}' AS dt
FROM source
GROUP BY symbol, trade_date;
