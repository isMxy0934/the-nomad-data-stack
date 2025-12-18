-- ODS Layer Table Definitions
-- This file defines views that map S3 parquet files to DuckDB tables with explicit schemas.
-- This ensures that the tables exist in the catalog even if no data has been loaded yet.

-- 1. ods_daily_stock_price_akshare
CREATE OR REPLACE VIEW ods.ods_daily_stock_price_akshare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_stock_price_akshare/dt=*/**/*.parquet',
    hive_partitioning = true,
    schema = {
        'trade_date': 'DATE',
        'symbol': 'VARCHAR',
        'open': 'DOUBLE',
        'close': 'DOUBLE',
        'low': 'DOUBLE',
        'high': 'DOUBLE',
        'volume': 'DOUBLE',
        'amount': 'DOUBLE',
        'outstanding_share': 'DOUBLE',
        'turnover': 'DOUBLE',
        'dt': 'VARCHAR'
    }
);

-- 2. ods_daily_stock_price_tushare
CREATE OR REPLACE VIEW ods.ods_daily_stock_price_tushare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_stock_price_tushare/dt=*/**/*.parquet',
    hive_partitioning = true,
    schema = {
        'ts_code': 'VARCHAR',
        'trade_date': 'DATE',
        'open': 'DOUBLE',
        'high': 'DOUBLE',
        'low': 'DOUBLE',
        'close': 'DOUBLE',
        'pre_close': 'DOUBLE',
        'change': 'DOUBLE',
        'pct_chg': 'DOUBLE',
        'vol': 'DOUBLE',
        'amount': 'DOUBLE',
        'dt': 'VARCHAR'
    }
);

-- 3. ods_daily_fund_price_akshare
CREATE OR REPLACE VIEW ods.ods_daily_fund_price_akshare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_fund_price_akshare/dt=*/**/*.parquet',
    hive_partitioning = true,
    schema = {
        'date': 'DATE',
        'symbol': 'VARCHAR',
        'open': 'DOUBLE',
        'close': 'DOUBLE',
        'low': 'DOUBLE',
        'high': 'DOUBLE',
        'volume': 'DOUBLE',
        'amount': 'DOUBLE',
        'dt': 'VARCHAR'
    }
);

-- 4. ods_daily_fund_price_tushare
CREATE OR REPLACE VIEW ods.ods_daily_fund_price_tushare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_fund_price_tushare/dt=*/**/*.parquet',
    hive_partitioning = true,
    schema = {
        'ts_code': 'VARCHAR',
        'trade_date': 'DATE',
        'open': 'DOUBLE',
        'high': 'DOUBLE',
        'low': 'DOUBLE',
        'close': 'DOUBLE',
        'pre_close': 'DOUBLE',
        'change': 'DOUBLE',
        'pct_chg': 'DOUBLE',
        'vol': 'DOUBLE',
        'amount': 'DOUBLE',
        'dt': 'VARCHAR'
    }
);

-- Partitioned Macros for ODS
-- These macros allow efficient access to a single partition by constructing the exact S3 path.
CREATE OR REPLACE MACRO ods.ods_daily_stock_price_akshare_dt(p_date) AS TABLE 
SELECT * FROM read_parquet('s3://stock-data/lake/ods/ods_daily_stock_price_akshare/dt=' || p_date || '/**/*.parquet', hive_partitioning=true);

CREATE OR REPLACE MACRO ods.ods_daily_stock_price_tushare_dt(p_date) AS TABLE 
SELECT * FROM read_parquet('s3://stock-data/lake/ods/ods_daily_stock_price_tushare/dt=' || p_date || '/**/*.parquet', hive_partitioning=true);

CREATE OR REPLACE MACRO ods.ods_daily_fund_price_akshare_dt(p_date) AS TABLE 
SELECT * FROM read_parquet('s3://stock-data/lake/ods/ods_daily_fund_price_akshare/dt=' || p_date || '/**/*.parquet', hive_partitioning=true);

CREATE OR REPLACE MACRO ods.ods_daily_fund_price_tushare_dt(p_date) AS TABLE 
SELECT * FROM read_parquet('s3://stock-data/lake/ods/ods_daily_fund_price_tushare/dt=' || p_date || '/**/*.parquet', hive_partitioning=true);