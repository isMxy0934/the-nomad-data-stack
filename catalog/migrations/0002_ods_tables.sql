-- ODS Layer Table Definitions (v1.3 Smart Inference Standard)
-- Schema is inferred from S3 files (seeded during migration if empty).
-- 'union_by_name' is enabled to handle potential schema evolutions.

-- Bootstrap note:
-- DuckDB fails to CREATE VIEW over read_parquet(...) when the glob matches zero files.
-- To allow migrations to run on a fresh/empty lake, we create a single 0-row parquet
-- file under a reserved partition `dt=1900-01-01`, and filter that partition out in
-- the views so queries still return empty results until real data arrives.

-------------------------------------------------------------------------------
-- 1. ods_daily_stock_price_akshare
-------------------------------------------------------------------------------
CREATE OR REPLACE MACRO ods._schema_stock_akshare() AS
MAP {
    0: {name: 'trade_date', type: 'DATE', default_value: NULL},
    1: {name: 'symbol', type: 'VARCHAR', default_value: NULL},
    2: {name: 'open', type: 'DOUBLE', default_value: NULL},
    3: {name: 'close', type: 'DOUBLE', default_value: NULL},
    4: {name: 'low', type: 'DOUBLE', default_value: NULL},
    5: {name: 'high', type: 'DOUBLE', default_value: NULL},
    6: {name: 'volume', type: 'DOUBLE', default_value: NULL},
    7: {name: 'amount', type: 'DOUBLE', default_value: NULL},
    8: {name: 'outstanding_share', type: 'DOUBLE', default_value: NULL},
    9: {name: 'turnover', type: 'DOUBLE', default_value: NULL}
};

COPY (
    SELECT
        CAST(NULL AS DATE) AS trade_date,
        CAST(NULL AS VARCHAR) AS symbol,
        CAST(NULL AS DOUBLE) AS open,
        CAST(NULL AS DOUBLE) AS close,
        CAST(NULL AS DOUBLE) AS low,
        CAST(NULL AS DOUBLE) AS high,
        CAST(NULL AS DOUBLE) AS volume,
        CAST(NULL AS DOUBLE) AS amount,
        CAST(NULL AS DOUBLE) AS outstanding_share,
        CAST(NULL AS DOUBLE) AS turnover
    WHERE 1=0
) TO 's3://stock-data/lake/ods/ods_daily_stock_price_akshare/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

CREATE OR REPLACE VIEW ods.ods_daily_stock_price_akshare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_stock_price_akshare/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

CREATE OR REPLACE MACRO ods.ods_daily_stock_price_akshare_dt(p_date) AS TABLE 
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_stock_price_akshare/dt=' || p_date || '/**/*.parquet', 
    hive_partitioning=true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
);

-------------------------------------------------------------------------------
-- 2. ods_daily_stock_price_tushare
-------------------------------------------------------------------------------
CREATE OR REPLACE MACRO ods._schema_stock_tushare() AS
MAP {
    0: {name: 'ts_code', type: 'VARCHAR', default_value: NULL},
    1: {name: 'trade_date', type: 'DATE', default_value: NULL},
    2: {name: 'open', type: 'DOUBLE', default_value: NULL},
    3: {name: 'high', type: 'DOUBLE', default_value: NULL},
    4: {name: 'low', type: 'DOUBLE', default_value: NULL},
    5: {name: 'close', type: 'DOUBLE', default_value: NULL},
    6: {name: 'pre_close', type: 'DOUBLE', default_value: NULL},
    7: {name: 'change', type: 'DOUBLE', default_value: NULL},
    8: {name: 'pct_chg', type: 'DOUBLE', default_value: NULL},
    9: {name: 'vol', type: 'DOUBLE', default_value: NULL},
    10: {name: 'amount', type: 'DOUBLE', default_value: NULL}
};

COPY (
    SELECT
        CAST(NULL AS VARCHAR) AS ts_code,
        CAST(NULL AS DATE) AS trade_date,
        CAST(NULL AS DOUBLE) AS open,
        CAST(NULL AS DOUBLE) AS high,
        CAST(NULL AS DOUBLE) AS low,
        CAST(NULL AS DOUBLE) AS close,
        CAST(NULL AS DOUBLE) AS pre_close,
        CAST(NULL AS DOUBLE) AS change,
        CAST(NULL AS DOUBLE) AS pct_chg,
        CAST(NULL AS DOUBLE) AS vol,
        CAST(NULL AS DOUBLE) AS amount
    WHERE 1=0
) TO 's3://stock-data/lake/ods/ods_daily_stock_price_tushare/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

CREATE OR REPLACE VIEW ods.ods_daily_stock_price_tushare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_stock_price_tushare/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

CREATE OR REPLACE MACRO ods.ods_daily_stock_price_tushare_dt(p_date) AS TABLE 
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_stock_price_tushare/dt=' || p_date || '/**/*.parquet', 
    hive_partitioning=true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
);

-------------------------------------------------------------------------------
-- 3. ods_daily_fund_price_akshare
-------------------------------------------------------------------------------
CREATE OR REPLACE MACRO ods._schema_fund_akshare() AS
MAP {
    0: {name: 'date', type: 'DATE', default_value: NULL},
    1: {name: 'symbol', type: 'VARCHAR', default_value: NULL},
    2: {name: 'open', type: 'DOUBLE', default_value: NULL},
    3: {name: 'close', type: 'DOUBLE', default_value: NULL},
    4: {name: 'low', type: 'DOUBLE', default_value: NULL},
    5: {name: 'high', type: 'DOUBLE', default_value: NULL},
    6: {name: 'volume', type: 'DOUBLE', default_value: NULL},
    7: {name: 'amount', type: 'DOUBLE', default_value: NULL}
};

COPY (
    SELECT
        CAST(NULL AS DATE) AS date,
        CAST(NULL AS VARCHAR) AS symbol,
        CAST(NULL AS DOUBLE) AS open,
        CAST(NULL AS DOUBLE) AS close,
        CAST(NULL AS DOUBLE) AS low,
        CAST(NULL AS DOUBLE) AS high,
        CAST(NULL AS DOUBLE) AS volume,
        CAST(NULL AS DOUBLE) AS amount
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
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_fund_price_akshare/dt=' || p_date || '/**/*.parquet', 
    hive_partitioning=true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
);

-------------------------------------------------------------------------------
-- 4. ods_daily_fund_price_tushare
-------------------------------------------------------------------------------
CREATE OR REPLACE MACRO ods._schema_fund_tushare() AS
MAP {
    0: {name: 'ts_code', type: 'VARCHAR', default_value: NULL},
    1: {name: 'trade_date', type: 'DATE', default_value: NULL},
    2: {name: 'open', type: 'DOUBLE', default_value: NULL},
    3: {name: 'high', type: 'DOUBLE', default_value: NULL},
    4: {name: 'low', type: 'DOUBLE', default_value: NULL},
    5: {name: 'close', type: 'DOUBLE', default_value: NULL},
    6: {name: 'pre_close', type: 'DOUBLE', default_value: NULL},
    7: {name: 'change', type: 'DOUBLE', default_value: NULL},
    8: {name: 'pct_chg', type: 'DOUBLE', default_value: NULL},
    9: {name: 'vol', type: 'DOUBLE', default_value: NULL},
    10: {name: 'amount', type: 'DOUBLE', default_value: NULL}
};

COPY (
    SELECT
        CAST(NULL AS VARCHAR) AS ts_code,
        CAST(NULL AS DATE) AS trade_date,
        CAST(NULL AS DOUBLE) AS open,
        CAST(NULL AS DOUBLE) AS high,
        CAST(NULL AS DOUBLE) AS low,
        CAST(NULL AS DOUBLE) AS close,
        CAST(NULL AS DOUBLE) AS pre_close,
        CAST(NULL AS DOUBLE) AS change,
        CAST(NULL AS DOUBLE) AS pct_chg,
        CAST(NULL AS DOUBLE) AS vol,
        CAST(NULL AS DOUBLE) AS amount
    WHERE 1=0
) TO 's3://stock-data/lake/ods/ods_daily_fund_price_tushare/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

CREATE OR REPLACE VIEW ods.ods_daily_fund_price_tushare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_fund_price_tushare/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

CREATE OR REPLACE MACRO ods.ods_daily_fund_price_tushare_dt(p_date) AS TABLE 
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_fund_price_tushare/dt=' || p_date || '/**/*.parquet', 
    hive_partitioning=true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
);
