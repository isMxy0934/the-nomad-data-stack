-- DWD Layer Table Definitions
-- Using the same DRY standard as ODS for consistency.

-------------------------------------------------------------------------------
-- 1. dwd_daily_stock_price
-------------------------------------------------------------------------------
CREATE OR REPLACE MACRO dwd._schema_daily_stock_price() AS
MAP {
    0: {name: 'symbol', type: 'VARCHAR', default_value: NULL},
    1: {name: 'trade_date', type: 'DATE', default_value: NULL},
    2: {name: 'high', type: 'DOUBLE', default_value: NULL},
    3: {name: 'low', type: 'DOUBLE', default_value: NULL},
    4: {name: 'close', type: 'DOUBLE', default_value: NULL}
};

COPY (
    SELECT
        CAST(NULL AS VARCHAR) AS symbol,
        CAST(NULL AS DATE) AS trade_date,
        CAST(NULL AS DOUBLE) AS high,
        CAST(NULL AS DOUBLE) AS low,
        CAST(NULL AS DOUBLE) AS close
    WHERE 1=0
) TO 's3://stock-data/lake/dwd/dwd_daily_stock_price/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

CREATE OR REPLACE VIEW dwd.dwd_daily_stock_price AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/dwd/dwd_daily_stock_price/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

CREATE OR REPLACE MACRO dwd.dwd_daily_stock_price_dt(p_date) AS TABLE 
SELECT * FROM read_parquet(
    's3://stock-data/lake/dwd/dwd_daily_stock_price/dt=' || p_date || '/**/*.parquet', 
    hive_partitioning=true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
);
