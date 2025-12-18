CREATE OR REPLACE MACRO ods._schema_fund_akshare() AS
MAP {
    0: {name: 'symbol', type: 'VARCHAR', default_value: NULL},
    1: {name: 'name', type: 'VARCHAR', default_value: NULL},
    2: {name: 'close', type: 'DOUBLE', default_value: NULL},
    3: {name: 'high', type: 'DOUBLE', default_value: NULL},
    4: {name: 'low', type: 'DOUBLE', default_value: NULL},
    5: {name: 'vol', type: 'DOUBLE', default_value: NULL},
    6: {name: 'amount', type: 'DOUBLE', default_value: NULL},
    7: {name: 'pre_close', type: 'DOUBLE', default_value: NULL},
    8: {name: 'pct_chg', type: 'DOUBLE', default_value: NULL},
    9: {name: 'trade_date', type: 'DATE', default_value: NULL}
};

COPY (
    SELECT
        CAST(NULL AS VARCHAR) AS symbol,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS DOUBLE) AS close,
        CAST(NULL AS DOUBLE) AS high,
        CAST(NULL AS DOUBLE) AS low,
        CAST(NULL AS DOUBLE) AS vol,
        CAST(NULL AS DOUBLE) AS amount,
        CAST(NULL AS DOUBLE) AS pre_close,
        CAST(NULL AS DOUBLE) AS pct_chg,
        CAST(NULL AS DATE) AS trade_date
    WHERE 1=0
) TO 's3://stock-data/lake/ods/ods_daily_fund_price_akshare/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

CREATE OR REPLACE VIEW ods.ods_daily_fund_price_akshare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_fund_price_akshare/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

CREATE OR REPLACE MACRO ods.ods_daily_fund_price_akshare_dt(p_date) AS TABLE 
SELECT * FROM ods.ods_daily_fund_price_akshare
WHERE dt = p_date;

-- DWD Daily Stock Price
CREATE OR REPLACE MACRO dwd._schema_daily_stock_price() AS
MAP {
    0: {name: 'symbol', type: 'VARCHAR', default_value: NULL},
    1: {name: 'high', type: 'DOUBLE', default_value: NULL},
    2: {name: 'low', type: 'DOUBLE', default_value: NULL},
    3: {name: 'close', type: 'DOUBLE', default_value: NULL},
    5: {name: 'trade_date', type: 'DATE', default_value: NULL}
};

COPY (
    SELECT
        CAST(NULL AS VARCHAR) AS symbol,
        CAST(NULL AS DOUBLE) AS high,
        CAST(NULL AS DOUBLE) AS low,
        CAST(NULL AS DOUBLE) AS close,
        CAST(NULL AS DATE) AS trade_date
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
SELECT * FROM dwd.dwd_daily_stock_price
WHERE dt = p_date;
