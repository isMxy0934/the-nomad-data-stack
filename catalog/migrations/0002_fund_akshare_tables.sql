-- ODS Fund Names from EM AkShare
CREATE OR REPLACE MACRO ods._schema_fund_names_em_akshare() AS
MAP {
    0: {name: 'symbol', type: 'VARCHAR', default_value: NULL},
    1: {name: 'name', type: 'VARCHAR', default_value: NULL},
    2: {name: 'fund_type', type: 'VARCHAR', default_value: NULL},
    3: {name: 'pinyin_full', type: 'VARCHAR', default_value: NULL},
    4: {name: 'pinyin_short', type: 'VARCHAR', default_value: NULL},
    5: {name: 'trade_date', type: 'DATE', default_value: NULL}
};

COPY (
    SELECT
        CAST(NULL AS VARCHAR) AS symbol,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS VARCHAR) AS fund_type,
        CAST(NULL AS VARCHAR) AS pinyin_full,
        CAST(NULL AS VARCHAR) AS pinyin_short,
        CAST(NULL AS DATE) AS trade_date
    WHERE 1=0
) TO 's3://stock-data/lake/ods/ods_fund_names_em_akshare/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

CREATE OR REPLACE VIEW ods.ods_fund_names_em_akshare AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_fund_names_em_akshare/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

CREATE OR REPLACE MACRO ods.ods_fund_names_em_akshare_dt(p_date) AS TABLE 
SELECT * FROM ods.ods_fund_names_em_akshare
WHERE dt = p_date;