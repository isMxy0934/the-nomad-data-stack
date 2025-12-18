-- ODS Layer Table Definitions (v1.3 Smart Inference Standard)
-- Schema is inferred from S3 files (seeded during migration if empty).
-- 'union_by_name' is enabled to handle potential schema evolutions.

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

CREATE OR REPLACE VIEW ods.ods_daily_stock_price_akshare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_stock_price_akshare/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
);

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

CREATE OR REPLACE VIEW ods.ods_daily_stock_price_tushare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_stock_price_tushare/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
);

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

CREATE OR REPLACE VIEW ods.ods_daily_fund_price_akshare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_fund_price_akshare/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
);

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

CREATE OR REPLACE VIEW ods.ods_daily_fund_price_tushare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_fund_price_tushare/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
);

CREATE OR REPLACE MACRO ods.ods_daily_fund_price_tushare_dt(p_date) AS TABLE 
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_daily_fund_price_tushare/dt=' || p_date || '/**/*.parquet', 
    hive_partitioning=true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
);