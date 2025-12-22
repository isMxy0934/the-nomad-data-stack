CREATE OR REPLACE MACRO ods._schema_fund_etf_spot() AS
MAP {
    0: {name: 'symbol', type: 'VARCHAR', default_value: NULL},
    1: {name: 'name', type: 'VARCHAR', default_value: NULL},
    2: {name: 'amount', type: 'DOUBLE', default_value: NULL},
    3: {name: 'trade_date', type: 'DATE', default_value: NULL}
};

-- Recreate seed parquet
COPY (
    SELECT
        CAST(NULL AS VARCHAR) AS symbol,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS DOUBLE) AS amount,
        CAST(NULL AS DATE) AS trade_date
    WHERE 1=0
) TO 's3://stock-data/lake/ods/ods_fund_etf_spot/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

-- Recreate view
CREATE OR REPLACE VIEW ods.ods_fund_etf_spot AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_fund_etf_spot/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

-- Recreate date macro
CREATE OR REPLACE MACRO ods.ods_fund_etf_spot_dt(p_date) AS TABLE 
SELECT * FROM ods.ods_fund_etf_spot
WHERE dt = p_date;
