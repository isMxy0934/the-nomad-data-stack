SELECT
    trade_date,
    YEAR(trade_date) AS trade_year,
    MONTH(trade_date) AS trade_month,
    QUARTER(trade_date) AS trade_quarter,
    DAYOFWEEK(trade_date) AS trade_day_of_week,
    DAY(trade_date) AS trade_day_of_month,
    DAYOFYEAR(trade_date) AS trade_day_of_year,
    WEEKOFYEAR(trade_date) AS trade_week_of_year,
    DAYOFWEEK(trade_date) NOT IN (1, 7) AS is_weekday,
    (trade_date == (DATE_TRUNC('MONTH', trade_date + INTERVAL '1 MONTH') - INTERVAL '1 DAY')) AS is_month_end,
    (trade_date == (DATE_TRUNC('QUARTER', trade_date + INTERVAL '3 MONTH') - INTERVAL '1 DAY')) AS is_quarter_end,
    (MONTH(trade_date) == 12 AND DAY(trade_date) == 31) AS is_year_end,
    '${PARTITION_DATE}' AS dt
FROM (
    SELECT * FROM ods.ods_trade_date_hist_dt('${PARTITION_DATE}')
) AS ods_data;
