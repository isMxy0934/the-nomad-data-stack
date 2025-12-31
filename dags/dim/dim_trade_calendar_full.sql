SELECT
    trade_date,
    CAST(YEAR(trade_date) AS INTEGER) AS trade_year,
    CAST(MONTH(trade_date) AS INTEGER) AS trade_month,
    CAST(QUARTER(trade_date) AS INTEGER) AS trade_quarter,
    CAST(DAYOFWEEK(trade_date) AS INTEGER) AS trade_day_of_week,
    CAST(DAY(trade_date) AS INTEGER) AS trade_day_of_month,
    CAST(DAYOFYEAR(trade_date) AS INTEGER) AS trade_day_of_year,
    CAST(WEEKOFYEAR(trade_date) AS INTEGER) AS trade_week_of_year,
    DAYOFWEEK(trade_date) NOT IN (1, 7) AS is_weekday,
    (trade_date == (DATE_TRUNC('MONTH', trade_date + INTERVAL '1 MONTH') - INTERVAL '1 DAY')) AS is_month_end,
    (trade_date == (DATE_TRUNC('QUARTER', trade_date + INTERVAL '3 MONTH') - INTERVAL '1 DAY')) AS is_quarter_end,
    (MONTH(trade_date) == 12 AND DAY(trade_date) == 31) AS is_year_end,
    '${PARTITION_DATE}' AS dt
FROM (
    SELECT * FROM ods.ods_trade_date_hist where dt = '${PARTITION_DATE}'
) AS ods_data;