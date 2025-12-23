CREATE OR REPLACE MACRO ods._schema_fund_etf_history() AS
MAP {
    0: {name: 'symbol', type: 'VARCHAR', default_value: NULL},
    1: {name: 'trade_date', type: 'DATE', default_value: NULL},
    2: {name: 'open', type: 'DOUBLE', default_value: NULL},
    3: {name: 'high', type: 'DOUBLE', default_value: NULL},
    4: {name: 'low', type: 'DOUBLE', default_value: NULL},
    5: {name: 'close', type: 'DOUBLE', default_value: NULL},
    6: {name: 'volume', type: 'DOUBLE', default_value: NULL},
    7: {name: 'amount', type: 'DOUBLE', default_value: NULL},
    8: {name: 'amplitude', type: 'DOUBLE', default_value: NULL},
    9: {name: 'change_percent', type: 'DOUBLE', default_value: NULL},
    10: {name: 'change_amount', type: 'DOUBLE', default_value: NULL},
    11: {name: 'turnover_rate', type: 'DOUBLE', default_value: NULL}
};

-- Recreate seed parquet
COPY (
    SELECT
        CAST(NULL AS VARCHAR) AS symbol,
        CAST(NULL AS DATE) AS trade_date,
        CAST(NULL AS DOUBLE) AS open,
        CAST(NULL AS DOUBLE) AS high,
        CAST(NULL AS DOUBLE) AS low,
        CAST(NULL AS DOUBLE) AS close,
        CAST(NULL AS DOUBLE) AS volume,
        CAST(NULL AS DOUBLE) AS amount,
        CAST(NULL AS DOUBLE) AS amplitude,
        CAST(NULL AS DOUBLE) AS change_percent,
        CAST(NULL AS DOUBLE) AS change_amount,
        CAST(NULL AS DOUBLE) AS turnover_rate,
        '${PARTITION_DATE}' AS dt
    WHERE 1=0
) TO 's3://stock-data/lake/ods/ods_fund_etf_history/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

-- Recreate view
CREATE OR REPLACE VIEW ods.ods_fund_etf_history AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_fund_etf_history/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

-- Recreate date macro
CREATE OR REPLACE MACRO ods.ods_fund_etf_history_dt(p_date) AS TABLE 
SELECT * FROM ods.ods_fund_etf_history
WHERE dt = p_date;
