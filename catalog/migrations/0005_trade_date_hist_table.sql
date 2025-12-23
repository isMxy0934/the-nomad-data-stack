-- Schema definition for trade_date_hist
CREATE OR REPLACE MACRO ods._schema_trade_date_hist() AS
MAP {
    0: {name: 'trade_date', type: 'DATE', default_value: NULL}
};

-- Seed parquet for schema initialization
COPY (
    SELECT
        CAST(NULL AS DATE) AS trade_date
    WHERE 1=0
) TO 's3://stock-data/lake/ods/ods_trade_date_hist/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

-- ODS View definition
CREATE OR REPLACE VIEW ods.ods_trade_date_hist AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_trade_date_hist/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

-- Date partition macro for querying specific partitions
CREATE OR REPLACE MACRO ods.ods_trade_date_hist_dt(p_date) AS TABLE
SELECT * FROM ods.ods_trade_date_hist
WHERE dt = p_date;
