-- Schema definition for dim_etf_full
CREATE OR REPLACE MACRO dim._schema_dim_etf_full() AS
MAP {
    0: {name: 'symbol', type: 'VARCHAR', default_value: NULL},
    1: {name: 'name', type: 'VARCHAR', default_value: NULL},
    2: {name: 'enabled', type: 'INTEGER', default_value: NULL},
    3: {name: 'flag', type: 'VARCHAR', default_value: NULL},
    4: {name: 'trade_date', type: 'DATE', default_value: NULL}
};

-- Seed parquet for schema initialization
COPY (
    SELECT
        CAST(NULL AS VARCHAR) AS symbol,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS INTEGER) AS enabled,
        CAST(NULL AS VARCHAR) AS flag,
        CAST(NULL AS DATE) AS trade_date
    WHERE 1=0
) TO 's3://stock-data/lake/dim/dim_etf_full/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

-- DIM View definition
CREATE OR REPLACE VIEW dim.dim_etf_full AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/dim/dim_etf_full/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

-- Date partition macro for querying specific partitions
CREATE OR REPLACE MACRO dim.dim_etf_full_dt(p_date) AS TABLE
SELECT * FROM dim.dim_etf_full
WHERE dt = p_date;
