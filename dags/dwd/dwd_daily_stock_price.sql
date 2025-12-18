WITH source AS (
    SELECT
        symbol,
        data as trade_date,
        high,
        low,
        close,
        '${PARTITION_DATE}' AS dt
    FROM ods.ods_daily_fund_price_akshare_dt('${PARTITION_DATE}')
)
SELECT * FROM source;
