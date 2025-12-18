-- ODS Layer Table Definitions
-- This file defines views that map S3 parquet files to DuckDB tables with explicit schemas.
-- This ensures that the tables exist in the catalog even if no data has been loaded yet.

-- 1. ods_daily_stock_price_akshare
CREATE OR REPLACE VIEW ods.ods_daily_stock_price_akshare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_stock_price_akshare/dt=*/**/*.parquet',
    hive_partitioning = true,
    columns = {
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
    columns = {
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
    columns = {
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
    columns = {
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
