-- ODS ETF Universe Whitelist
CREATE OR REPLACE MACRO ods._schema_etf_universe_whitelist() AS
MAP {
    0: {name: 'symbol', type: 'VARCHAR', default_value: NULL},
    1: {name: 'name', type: 'VARCHAR', default_value: NULL},
    2: {name: 'enabled', type: 'INTEGER', default_value: NULL},
    3: {name: 'trade_date', type: 'DATE', default_value: NULL}
};

COPY (
    SELECT
        CAST(NULL AS VARCHAR) AS symbol,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS INTEGER) AS enabled,
        CAST(NULL AS DATE) AS trade_date
    WHERE 1=0
) TO 's3://stock-data/lake/ods/ods_etf_universe_whitelist/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

CREATE OR REPLACE VIEW ods.ods_etf_universe_whitelist AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_etf_universe_whitelist/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

CREATE OR REPLACE MACRO ods.ods_etf_universe_whitelist_dt(p_date) AS TABLE 
SELECT * FROM ods.ods_etf_universe_whitelist
WHERE dt = p_date;

